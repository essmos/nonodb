//! Backup manifests and read-only database verification.
//!
//! Verification streams every document, checks `_id` consistency, validates
//! secondary index entries, and checks full-text/fuzzy posting counts against
//! the documents. It is intended for backups and restore drills.

use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use heed::types::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::history::HISTORY_DB;
use crate::store::{Store, META_DB, SCHEMA_VERSION};
use crate::ttl::TTL_DB;

/// Backup manifest format version.
pub const MANIFEST_VERSION: u32 = 1;
/// Manifest file name inside a backup directory.
pub const MANIFEST_FILE: &str = "manifest.json";
/// Data file name inside a data/backup directory.
pub const DATA_FILE: &str = "data.mdb";

/// One secondary-index verification result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexVerification {
    pub name: String,
    pub fields: Vec<String>,
    pub unique: bool,
    pub include: Vec<String>,
    pub format: u8,
    pub entries: u64,
    /// Fields that contain arrays and therefore produce multiple index entries.
    #[serde(default)]
    pub multikey_fields: Vec<String>,
    /// Whether every document currently has at least one entry in this index.
    #[serde(default)]
    pub multikey_all_docs: bool,
    /// Legacy index formats cannot be reconstructed exactly; entries are
    /// counted but not cross-checked.
    pub checked: bool,
}

/// One collection verification result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionVerification {
    pub documents: u64,
    pub bytes: u64,
    pub indexes: Vec<IndexVerification>,
    pub text_indexes: Vec<String>,
    pub fuzzy_indexes: Vec<String>,
}

/// Full verification report for a data directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationReport {
    pub path: String,
    pub schema_version: u32,
    pub collections: BTreeMap<String, CollectionVerification>,
    pub documents: u64,
    pub bytes: u64,
    pub secondary_index_entries: u64,
    pub text_indexes: u64,
    pub fuzzy_indexes: u64,
    pub history_entries: u64,
    pub ttl_entries: u64,
}

/// Metadata stored next to a compacting backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    pub version: u32,
    pub created_at_ms: u64,
    pub source: String,
    pub data_file: String,
    pub data_size: u64,
    pub sha256: String,
}

/// A verified backup and its database report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupVerification {
    pub manifest: BackupManifest,
    pub report: VerificationReport,
}

impl BackupManifest {
    /// Builds a manifest for an already written backup directory.
    pub fn from_backup(backup_dir: impl AsRef<Path>, source: impl AsRef<Path>) -> Result<Self> {
        let backup_dir = backup_dir.as_ref();
        let path = backup_data_path(backup_dir)?;
        let meta = fs::metadata(&path)?;
        Ok(Self {
            version: MANIFEST_VERSION,
            created_at_ms: now_ms(),
            source: source.as_ref().display().to_string(),
            data_file: DATA_FILE.to_string(),
            data_size: meta.len(),
            sha256: sha256_file(&path)?,
        })
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Returns the `data.mdb` path inside a directory.
pub fn backup_data_path(dir: impl AsRef<Path>) -> Result<PathBuf> {
    let path = dir.as_ref().join(DATA_FILE);
    if !path.is_file() {
        return Err(Error::Query(format!(
            "missing {} in {}",
            DATA_FILE,
            dir.as_ref().display()
        )));
    }
    Ok(path)
}

/// SHA-256 of a file as lowercase hex.
pub fn sha256_file(path: impl AsRef<Path>) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

/// Writes `manifest.json` atomically inside a backup directory.
pub fn write_manifest(backup_dir: impl AsRef<Path>, manifest: &BackupManifest) -> Result<()> {
    let dir = backup_dir.as_ref();
    let tmp = dir.join(".manifest.json.tmp");
    let raw = serde_json::to_vec_pretty(manifest)?;
    fs::write(&tmp, raw)?;
    fs::rename(&tmp, dir.join(MANIFEST_FILE))?;
    Ok(())
}

/// Reads `manifest.json`; returns `None` for backups made by older releases.
pub fn read_manifest(backup_dir: impl AsRef<Path>) -> Result<Option<BackupManifest>> {
    let path = backup_dir.as_ref().join(MANIFEST_FILE);
    let raw = match fs::read(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let manifest = serde_json::from_slice(&raw)
        .map_err(|e| Error::Query(format!("invalid {}: {e}", path.display())))?;
    Ok(Some(manifest))
}

/// Verifies a backup manifest and deep-checks its database.
pub fn verify_backup(backup_dir: impl AsRef<Path>) -> Result<BackupVerification> {
    let dir = backup_dir.as_ref();
    let manifest = read_manifest(dir)?
        .ok_or_else(|| Error::Query(format!("missing {} in {}", MANIFEST_FILE, dir.display())))?;
    if manifest.version != MANIFEST_VERSION {
        return Err(Error::Query(format!(
            "unsupported manifest version {} (expected {MANIFEST_VERSION})",
            manifest.version
        )));
    }
    let data = backup_data_path(dir)?;
    let meta = fs::metadata(&data)?;
    if meta.len() != manifest.data_size {
        return Err(Error::Query(format!(
            "backup size mismatch: manifest {}, actual {}",
            manifest.data_size,
            meta.len()
        )));
    }
    let digest = sha256_file(&data)?;
    if digest != manifest.sha256 {
        return Err(Error::Query(format!(
            "backup checksum mismatch: manifest {}, actual {digest}",
            manifest.sha256
        )));
    }
    let report = verify_data_dir(dir)?;
    Ok(BackupVerification { manifest, report })
}

/// Deep verification of a database or backup directory.
pub fn verify_data_dir(path: impl AsRef<Path>) -> Result<VerificationReport> {
    let path = path.as_ref();
    let store = Store::open_read_only(path)?;
    let rtxn = store.read_txn()?;
    let meta = store
        .env()
        .open_database::<Bytes, Bytes>(&rtxn, Some(META_DB))?
        .ok_or_else(|| Error::Query("meta database missing".into()))?;
    let schema_version = meta
        .get(&rtxn, b"schema_version")?
        .and_then(|raw| std::str::from_utf8(raw).ok()?.parse::<u32>().ok())
        .ok_or_else(|| Error::Query("missing or invalid schema_version".into()))?;
    if schema_version > SCHEMA_VERSION {
        return Err(Error::Query(format!(
            "database schema version {schema_version} is newer than supported {SCHEMA_VERSION}"
        )));
    }

    let mut collections = BTreeMap::new();
    let mut documents = 0u64;
    let mut bytes = 0u64;
    let mut secondary_index_entries = 0u64;
    let mut text_indexes = 0u64;
    let mut fuzzy_indexes = 0u64;

    for name in store.collections_in(&rtxn)? {
        let collection = store.collection_at(&rtxn, &name)?.ok_or_else(|| {
            Error::Query(format!("collection `{name}` is registered but missing"))
        })?;
        let report = collection.verify_in(&rtxn)?;
        documents += report.documents;
        bytes += report.bytes;
        secondary_index_entries += report.indexes.iter().map(|i| i.entries).sum::<u64>();
        text_indexes += report.text_indexes.len() as u64;
        fuzzy_indexes += report.fuzzy_indexes.len() as u64;
        collections.insert(name, report);
    }

    let history_entries = optional_entries(store.env(), &rtxn, HISTORY_DB)?;
    let ttl_entries = optional_entries(store.env(), &rtxn, TTL_DB)?;
    rtxn.commit()?;

    Ok(VerificationReport {
        path: path.display().to_string(),
        schema_version,
        collections,
        documents,
        bytes,
        secondary_index_entries,
        text_indexes,
        fuzzy_indexes,
        history_entries,
        ttl_entries,
    })
}

fn optional_entries(env: &heed::Env, rtxn: &heed::RoTxn<'_>, name: &str) -> Result<u64> {
    let Some(db) = env.open_database::<Bytes, Bytes>(rtxn, Some(name))? else {
        return Ok(0);
    };
    Ok(db.stat(rtxn)?.entries as u64)
}
