//! Authorization registry shared by the daemon and CLI.
//!
//! The registry is deliberately a small JSON file outside LMDB. It is loaded
//! atomically, stores only token hashes and certificate fingerprints, and is
//! never part of database backups.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use fs2::FileExt;
use rand::rngs::OsRng;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const AUTH_FILE_VERSION: u32 = 1;
pub const TOKEN_PREFIX: &str = "nodb_";
const MAX_NAME: usize = 96;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthFile {
    pub version: u32,
    #[serde(default)]
    pub principals: BTreeMap<String, PrincipalDef>,
    #[serde(default)]
    pub tokens: Vec<TokenEntry>,
    #[serde(default)]
    pub certificates: Vec<CertificateEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalDef {
    #[serde(default)]
    pub scopes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenEntry {
    pub kid: String,
    pub sha256: String,
    pub principal: String,
    #[serde(default)]
    pub disabled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CertificateEntry {
    pub sha256: String,
    pub principal: String,
    #[serde(default)]
    pub disabled: bool,
}

impl AuthFile {
    pub fn empty() -> Self {
        Self {
            version: AUTH_FILE_VERSION,
            principals: BTreeMap::new(),
            tokens: Vec::new(),
            certificates: Vec::new(),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.version != AUTH_FILE_VERSION {
            return Err(format!(
                "unsupported auth file version {} (expected {})",
                self.version, AUTH_FILE_VERSION
            ));
        }
        for (name, principal) in &self.principals {
            validate_name("principal", name)?;
            for scope in &principal.scopes {
                ScopeRule::parse(scope)?;
            }
        }
        let mut kids = HashSet::new();
        for token in &self.tokens {
            validate_kid(&token.kid)?;
            if !kids.insert(token.kid.clone()) {
                return Err(format!("duplicate token kid `{}`", token.kid));
            }
            validate_hash("token sha256", &token.sha256)?;
            if !self.principals.contains_key(&token.principal) {
                return Err(format!(
                    "token `{}` references unknown principal `{}`",
                    token.kid, token.principal
                ));
            }
        }
        let mut certs = HashSet::new();
        for cert in &self.certificates {
            let fp = normalize_fingerprint(&cert.sha256)?;
            if fp != cert.sha256 {
                return Err(format!(
                    "certificate fingerprint must be canonical lowercase hex: {}",
                    cert.sha256
                ));
            }
            if !certs.insert(fp.clone()) {
                return Err(format!("duplicate certificate fingerprint `{fp}`"));
            }
            if !self.principals.contains_key(&cert.principal) {
                return Err(format!(
                    "certificate `{fp}` references unknown principal `{}`",
                    cert.principal
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    name: String,
    scopes: Vec<ScopeRule>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Read,
    Write,
    AdminCollection,
    AdminDatabase,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ScopeRule {
    All,
    Read(Resource),
    Write(Resource),
    AdminCollection(Resource),
    AdminDatabase,
    MetricsRead,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Resource {
    database: Option<String>,
    collection: String,
}

impl ScopeRule {
    fn parse(raw: &str) -> Result<Self, String> {
        let raw = raw.trim();
        if raw == "*:*" {
            return Ok(Self::All);
        }
        let (action, resource) = raw
            .split_once(':')
            .ok_or_else(|| format!("invalid scope `{raw}` (expected action:resource)"))?;
        if action.is_empty() || resource.is_empty() {
            return Err(format!("invalid scope `{raw}`"));
        }
        if resource.contains(':') {
            return Err(format!(
                "invalid scope `{raw}` (resource cannot contain `:`)"
            ));
        }
        if action == "*" {
            return Err(format!(
                "invalid scope `{raw}` (only `*:*` is a full wildcard)"
            ));
        }
        if action == "admin" && resource == "db" {
            return Ok(Self::AdminDatabase);
        }
        if action == "metrics" && resource == "read" {
            return Ok(Self::MetricsRead);
        }
        let resource = parse_resource(resource)?;
        match action {
            "read" => Ok(Self::Read(resource)),
            "write" => Ok(Self::Write(resource)),
            "admin" => Ok(Self::AdminCollection(resource)),
            other => Err(format!("unknown scope action `{other}`")),
        }
    }

    fn matches_collection(&self, action: Action, database: &str, coll: &str) -> bool {
        match (self, action) {
            (Self::All, _) => true,
            (Self::Read(rule), Action::Read)
            | (Self::Write(rule), Action::Write)
            | (Self::AdminCollection(rule), Action::AdminCollection) => {
                rule.matches(database, coll)
            }
            _ => false,
        }
    }
}

impl Resource {
    fn matches(&self, database: &str, coll: &str) -> bool {
        let database_matches = self
            .database
            .as_deref()
            .is_none_or(|rule| rule == "*" || rule == database);
        let collection_matches = self.collection == "*" || self.collection == coll;
        database_matches && collection_matches
    }

    fn display(&self) -> String {
        match &self.database {
            Some(database) => format!("{database}/{}", self.collection),
            None => self.collection.clone(),
        }
    }
}

fn parse_resource(raw: &str) -> Result<Resource, String> {
    let (database, collection) = match raw.split_once('/') {
        Some((database, collection)) => {
            if collection.contains('/') {
                return Err(format!(
                    "invalid scope resource `{raw}` (expected database/collection)"
                ));
            }
            validate_database_name(database)?;
            (Some(database.to_string()), collection)
        }
        None => (None, raw),
    };
    let collection = match collection {
        "*" => "*".to_string(),
        name => {
            validate_collection_name(name)?;
            name.to_string()
        }
    };
    Ok(Resource {
        database,
        collection,
    })
}

impl Principal {
    pub fn new(name: impl Into<String>, scopes: &[String]) -> Result<Self, String> {
        let mut parsed = Vec::with_capacity(scopes.len());
        for scope in scopes {
            parsed.push(ScopeRule::parse(scope)?);
        }
        Ok(Self {
            name: name.into(),
            scopes: parsed,
        })
    }

    pub fn local_admin() -> Self {
        Self {
            name: "local".to_string(),
            scopes: vec![ScopeRule::All],
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn allows(&self, action: Action, database: &str, coll: &str) -> bool {
        self.scopes
            .iter()
            .any(|rule| rule.matches_collection(action, database, coll))
    }

    pub fn allows_admin_database(&self) -> bool {
        self.scopes
            .iter()
            .any(|rule| matches!(rule, ScopeRule::All | ScopeRule::AdminDatabase))
    }

    /// Metrics are instance-wide and reveal cross-collection load. They require
    /// an explicit `metrics:read`, `admin:db`, or `*:*` grant.
    pub fn allows_metrics(&self) -> bool {
        self.scopes.iter().any(|rule| {
            matches!(
                rule,
                ScopeRule::All | ScopeRule::AdminDatabase | ScopeRule::MetricsRead
            )
        })
    }

    /// Collection metadata visibility: any collection scope or full admin.
    pub fn can_see_collection(&self, database: &str, coll: &str) -> bool {
        self.allows(Action::Read, database, coll)
            || self.allows(Action::Write, database, coll)
            || self.allows(Action::AdminCollection, database, coll)
            || self
                .scopes
                .iter()
                .any(|rule| matches!(rule, ScopeRule::All))
    }

    pub fn scopes(&self) -> Vec<String> {
        self.scopes
            .iter()
            .map(|rule| match rule {
                ScopeRule::All => "*:*".to_string(),
                ScopeRule::Read(resource) => format!("read:{}", resource.display()),
                ScopeRule::Write(resource) => format!("write:{}", resource.display()),
                ScopeRule::AdminCollection(resource) => format!("admin:{}", resource.display()),
                ScopeRule::AdminDatabase => "admin:db".to_string(),
                ScopeRule::MetricsRead => "metrics:read".to_string(),
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct Registry {
    file: AuthFile,
    principals: HashMap<String, Principal>,
    tokens: HashMap<String, TokenRecord>,
    certificates: HashMap<[u8; 32], CertificateRecord>,
}

#[derive(Clone, Debug)]
struct TokenRecord {
    principal: Principal,
    hash: [u8; 32],
    disabled: bool,
}

#[derive(Clone, Debug)]
struct CertificateRecord {
    principal: Principal,
    disabled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthError {
    InvalidToken,
    Disabled,
    UnknownCertificate,
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidToken => f.write_str("invalid or unknown token"),
            Self::Disabled => f.write_str("credential is disabled"),
            Self::UnknownCertificate => f.write_str("certificate is not registered"),
        }
    }
}

impl std::error::Error for AuthError {}

impl Registry {
    pub fn from_file(file: AuthFile) -> Result<Self, String> {
        file.validate()?;
        let mut principals = HashMap::new();
        for (name, def) in &file.principals {
            principals.insert(name.clone(), Principal::new(name.clone(), &def.scopes)?);
        }
        let mut tokens = HashMap::new();
        for token in &file.tokens {
            tokens.insert(
                token.kid.clone(),
                TokenRecord {
                    principal: principals[&token.principal].clone(),
                    hash: parse_hash(&token.sha256)?,
                    disabled: token.disabled,
                },
            );
        }
        let mut certificates = HashMap::new();
        for cert in &file.certificates {
            certificates.insert(
                parse_hash(&cert.sha256)?,
                CertificateRecord {
                    principal: principals[&cert.principal].clone(),
                    disabled: cert.disabled,
                },
            );
        }
        Ok(Self {
            file,
            principals,
            tokens,
            certificates,
        })
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        let mut raw = String::new();
        File::open(path)
            .and_then(|mut file| file.read_to_string(&mut raw))
            .map_err(|e| format!("cannot read auth file {}: {e}", path.display()))?;
        let file: AuthFile = serde_json::from_str(&raw)
            .map_err(|e| format!("cannot parse auth file {}: {e}", path.display()))?;
        Self::from_file(file)
    }

    pub fn file(&self) -> &AuthFile {
        &self.file
    }

    pub fn principals(&self) -> impl Iterator<Item = &Principal> {
        self.principals.values()
    }

    pub fn principal(&self, name: &str) -> Option<&Principal> {
        self.principals.get(name)
    }

    pub fn authenticate_token(&self, token: &str) -> Result<Principal, AuthError> {
        let (kid, secret) = parse_token(token).ok_or(AuthError::InvalidToken)?;
        let actual = hash_bytes(secret.as_bytes());
        let Some(record) = self.tokens.get(kid) else {
            constant_time_eq(&actual, &[0u8; 32]);
            return Err(AuthError::InvalidToken);
        };
        if !constant_time_eq(&actual, &record.hash) {
            return Err(AuthError::InvalidToken);
        }
        if record.disabled {
            return Err(AuthError::Disabled);
        }
        Ok(record.principal.clone())
    }

    pub fn authenticate_certificate(&self, fingerprint: &[u8; 32]) -> Result<Principal, AuthError> {
        let Some(record) = self.certificates.get(fingerprint) else {
            return Err(AuthError::UnknownCertificate);
        };
        if record.disabled {
            return Err(AuthError::Disabled);
        }
        Ok(record.principal.clone())
    }
}

pub fn parse_token(token: &str) -> Option<(&str, &str)> {
    let rest = token.strip_prefix(TOKEN_PREFIX)?;
    let (kid, secret) = rest.split_once('_')?;
    if kid.is_empty() || secret.is_empty() {
        return None;
    }
    if !kid.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
        return None;
    }
    Some((kid, secret))
}

pub fn generate_token(kid: Option<&str>) -> Result<(String, String, String), String> {
    let kid = match kid {
        Some(kid) => {
            validate_kid(kid)?;
            kid.to_string()
        }
        None => {
            let mut raw = [0u8; 6];
            OsRng.fill_bytes(&mut raw);
            hex(&raw)
        }
    };
    let mut secret = [0u8; 32];
    OsRng.fill_bytes(&mut secret);
    let secret = URL_SAFE_NO_PAD.encode(secret);
    let token = format!("{TOKEN_PREFIX}{kid}_{secret}");
    let hash = hash_hex(secret.as_bytes());
    Ok((kid, token, hash))
}

pub fn hash_hex(data: &[u8]) -> String {
    hex(&hash_bytes(data))
}

pub fn normalize_fingerprint(raw: &str) -> Result<String, String> {
    let raw = raw.trim();
    let raw = raw.strip_prefix("sha256:").unwrap_or(raw);
    let clean: String = raw.chars().filter(|&c| c != ':').collect();
    if clean.len() != 64 || !clean.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(format!(
            "invalid SHA-256 fingerprint `{raw}` (expected 64 hexadecimal characters)"
        ));
    }
    Ok(clean.to_ascii_lowercase())
}

pub fn save_atomic(path: &Path, file: &AuthFile) -> Result<(), String> {
    let dir = auth_dir(path)?;
    let lock = lock_auth_file(path)?;
    let result = write_atomic_locked(path, dir, file);
    let _ = lock.unlock();
    result
}

/// Applies a mutation while holding the same cross-process lock used by
/// `save_atomic`. The closure receives the freshly loaded file, so GUI and CLI
/// updates cannot silently overwrite each other.
pub fn update_atomic<T, F>(path: &Path, mutate: F) -> Result<T, String>
where
    F: FnOnce(&mut AuthFile) -> Result<T, String>,
{
    let dir = auth_dir(path)?;
    let lock = lock_auth_file(path)?;
    let result = (|| {
        let mut file = read_auth_file(path)?;
        let output = mutate(&mut file)?;
        write_atomic_locked(path, dir, &file)?;
        Ok(output)
    })();
    let _ = lock.unlock();
    result
}

fn auth_dir(path: &Path) -> Result<&Path, String> {
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if !dir.is_dir() {
        return Err(format!("auth directory {} does not exist", dir.display()));
    }
    Ok(dir)
}

fn lock_auth_file(path: &Path) -> Result<File, String> {
    let lock_path = lock_path(path);
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|e| format!("cannot open auth lock {}: {e}", lock_path.display()))?;
    lock.lock_exclusive()
        .map_err(|e| format!("cannot lock auth file: {e}"))?;
    Ok(lock)
}

fn read_auth_file(path: &Path) -> Result<AuthFile, String> {
    let mut raw = String::new();
    File::open(path)
        .and_then(|mut file| file.read_to_string(&mut raw))
        .map_err(|e| format!("cannot read auth file {}: {e}", path.display()))?;
    serde_json::from_str(&raw)
        .map_err(|e| format!("cannot parse auth file {}: {e}", path.display()))
}

fn write_atomic_locked(path: &Path, dir: &Path, file: &AuthFile) -> Result<(), String> {
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("auth.json");
    let tmp = dir.join(format!(".{name}.tmp.{}", std::process::id()));
    let data = serde_json::to_vec_pretty(file).map_err(|e| e.to_string())?;

    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o640);
    }
    let mut out = options
        .open(&tmp)
        .map_err(|e| format!("cannot create {}: {e}", tmp.display()))?;
    let write_result = out
        .write_all(&data)
        .and_then(|_| out.write_all(b"\n"))
        .and_then(|_| out.sync_all());
    if let Err(e) = write_result {
        let _ = fs::remove_file(&tmp);
        return Err(format!("cannot write {}: {e}", tmp.display()));
    }
    drop(out);

    if let Err(e) = fs::rename(&tmp, path) {
        let _ = fs::remove_file(&tmp);
        return Err(format!("cannot replace {}: {e}", path.display()));
    }
    if let Ok(dir_handle) = File::open(dir) {
        let _ = dir_handle.sync_all();
    }
    Ok(())
}

fn lock_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("auth.json")
        .to_string();
    name.push_str(".lock");
    path.with_file_name(name)
}

fn parse_hash(raw: &str) -> Result<[u8; 32], String> {
    validate_hash("sha256", raw)?;
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        let start = i * 2;
        let s = &raw[start..start + 2];
        *byte = u8::from_str_radix(s, 16).map_err(|e| e.to_string())?;
    }
    Ok(out)
}

fn hash_bytes(data: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(data);
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

fn validate_name(kind: &str, name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > MAX_NAME {
        return Err(format!("invalid {kind} name `{name}`"));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':'))
    {
        return Err(format!("invalid {kind} name `{name}`"));
    }
    Ok(())
}

fn validate_database_name(name: &str) -> Result<(), String> {
    if name == "*" {
        return Ok(());
    }
    if name.is_empty() || name.len() > 64 {
        return Err(format!("invalid database name in scope `{name}`"));
    }
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(format!("invalid database name in scope `{name}`"));
    };
    if !first.is_ascii_alphanumeric()
        || !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
    {
        return Err(format!("invalid database name in scope `{name}`"));
    }
    Ok(())
}

fn validate_collection_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 255 || name == "db" {
        return Err(format!("invalid collection name in scope `{name}`"));
    }
    if name.chars().any(char::is_control) {
        return Err(format!("invalid collection name in scope `{name}`"));
    }
    Ok(())
}

fn validate_kid(kid: &str) -> Result<(), String> {
    if kid.is_empty()
        || kid.len() > 64
        || !kid.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err(format!("invalid token kid `{kid}`"));
    }
    Ok(())
}

fn validate_hash(kind: &str, raw: &str) -> Result<(), String> {
    if raw.len() != 64 || !raw.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(format!(
            "invalid {kind}: expected 64 hexadecimal characters"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_authentication_and_scopes() {
        let mut file = AuthFile::empty();
        file.principals.insert(
            "app".into(),
            PrincipalDef {
                scopes: vec!["read:orders".into(), "write:orders".into()],
            },
        );
        let (kid, token, hash) = generate_token(Some("fixed")).unwrap();
        file.tokens.push(TokenEntry {
            kid,
            sha256: hash,
            principal: "app".into(),
            disabled: false,
        });
        let registry = Registry::from_file(file).unwrap();
        assert_eq!(
            registry.authenticate_token("nodb_fixed_unknown"),
            Err(AuthError::InvalidToken)
        );
        let principal = registry.authenticate_token(&token).unwrap();
        assert!(principal.allows(Action::Read, "default", "orders"));
        assert!(!principal.allows(Action::Read, "default", "customers"));
        assert!(principal.allows(Action::Write, "default", "orders"));
        assert!(!principal.allows_admin_database());
        assert!(!principal.allows_metrics());
        assert!(principal.can_see_collection("default", "orders"));
        assert!(!principal.can_see_collection("default", "customers"));
    }

    #[test]
    fn metrics_scope_is_explicit() {
        let mut file = AuthFile::empty();
        file.principals.insert(
            "prometheus".into(),
            PrincipalDef {
                scopes: vec!["metrics:read".into()],
            },
        );
        let (kid, token, hash) = generate_token(Some("metrics")).unwrap();
        file.tokens.push(TokenEntry {
            kid,
            sha256: hash,
            principal: "prometheus".into(),
            disabled: false,
        });
        let registry = Registry::from_file(file).unwrap();
        let principal = registry.authenticate_token(&token).unwrap();
        assert!(principal.allows_metrics());
        assert!(!principal.allows_admin_database());
        assert!(!principal.allows(Action::Read, "default", "orders"));
    }

    #[test]
    fn disabled_token_is_rejected() {
        let mut file = AuthFile::empty();
        file.principals.insert(
            "app".into(),
            PrincipalDef {
                scopes: vec!["read:*".into()],
            },
        );
        let (kid, token, hash) = generate_token(Some("off")).unwrap();
        file.tokens.push(TokenEntry {
            kid,
            sha256: hash,
            principal: "app".into(),
            disabled: true,
        });
        let registry = Registry::from_file(file).unwrap();
        assert_eq!(
            registry.authenticate_token(&token),
            Err(AuthError::Disabled)
        );
    }

    #[test]
    fn scopes_are_strict() {
        assert!(ScopeRule::parse("*:*").is_ok());
        assert!(ScopeRule::parse("read:orders").is_ok());
        assert!(ScopeRule::parse("read:*").is_ok());
        assert!(ScopeRule::parse("read:app1/orders").is_ok());
        assert!(ScopeRule::parse("write:*/orders").is_ok());
        assert!(ScopeRule::parse("read:app1/*").is_ok());
        assert!(ScopeRule::parse("read:app1/orders/extra").is_err());
        assert!(ScopeRule::parse("read:bad db/orders").is_err());
        assert!(ScopeRule::parse("admin:db").is_ok());
        assert!(ScopeRule::parse("metrics:read").is_ok());
        assert!(ScopeRule::parse("read:").is_err());
        assert!(ScopeRule::parse("*:orders").is_err());
        assert!(ScopeRule::parse("read:orders:extra").is_err());
        assert!(ScopeRule::parse("nope:orders").is_err());
    }

    #[test]
    fn certificate_lookup() {
        let mut file = AuthFile::empty();
        file.principals.insert(
            "cert".into(),
            PrincipalDef {
                scopes: vec!["read:*".into()],
            },
        );
        let fp = "aa".repeat(32);
        file.certificates.push(CertificateEntry {
            sha256: fp.clone(),
            principal: "cert".into(),
            disabled: false,
        });
        let registry = Registry::from_file(file).unwrap();
        let mut raw = [0u8; 32];
        raw.fill(0xaa);
        assert_eq!(
            registry.authenticate_certificate(&raw).unwrap().name(),
            "cert"
        );
    }

    #[test]
    fn invalid_registry_is_rejected() {
        let mut file = AuthFile::empty();
        file.principals.insert(
            "app".into(),
            PrincipalDef {
                scopes: vec!["read:orders".into()],
            },
        );
        file.tokens.push(TokenEntry {
            kid: "x".into(),
            sha256: "00".repeat(32),
            principal: "missing".into(),
            disabled: false,
        });
        assert!(file.validate().is_err());
    }
}

#[cfg(test)]
mod update_tests {
    use super::*;

    #[test]
    fn update_atomic_reads_the_latest_file() {
        let path = std::env::temp_dir().join(format!(
            "nonodb-auth-update-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut file = AuthFile::empty();
        file.principals.insert(
            "admin".into(),
            PrincipalDef {
                scopes: vec!["admin:db".into()],
            },
        );
        save_atomic(&path, &file).unwrap();

        update_atomic(&path, |file| {
            file.principals.insert(
                "reader".into(),
                PrincipalDef {
                    scopes: vec!["read:orders".into()],
                },
            );
            Ok(())
        })
        .unwrap();

        let loaded = Registry::load(&path).unwrap();
        assert!(loaded.principal("admin").is_some());
        assert!(loaded.principal("reader").is_some());
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(lock_path(&path));
    }
}
