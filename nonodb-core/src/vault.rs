//! Optional vault collections: whole-document encryption with a per-request key.
//!
//! A vault collection stores only `_id`, `tenant_id`, and an encrypted envelope.
//! It deliberately supports a much smaller API than a standard collection:
//! CRUD by `_id`, listing/counting by `tenant_id`, and history. Search, fuzzy
//! search, aggregation, secondary indexes, arbitrary filters, and sorting are
//! rejected.

use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine;
use chacha20poly1305::aead::{Aead, Payload};
use chacha20poly1305::{KeyInit, XChaCha20Poly1305, XNonce};
use rand::RngCore;
use serde_json::{Map, Value};
use zeroize::{Zeroize, Zeroizing};

use crate::error::{Error, Result};

/// Collection profile stored in the metadata database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CollectionProfile {
    #[default]
    Standard,
    Vault,
}

impl CollectionProfile {
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "standard" | "" => Ok(Self::Standard),
            "vault" => Ok(Self::Vault),
            other => Err(Error::Vault {
                code: "invalid_collection_profile",
                message: format!("unknown collection profile `{other}`"),
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Vault => "vault",
        }
    }

    pub fn is_vault(self) -> bool {
        matches!(self, Self::Vault)
    }
}

/// Plaintext `tenant_id` field kept outside the encrypted envelope.
pub const TENANT_FIELD: &str = "tenant_id";
/// Vault envelope version.
pub const ENVELOPE_VERSION: u64 = 1;
/// AEAD algorithm name in the envelope.
pub const ALGORITHM: &str = "xchacha20poly1305";
/// Maximum accepted key id length.
pub const MAX_KEY_ID_LEN: usize = 128;
const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 24;

/// 32-byte tenant key. The bytes are zeroized on drop and are deliberately
/// not exposed through `Debug`.
#[derive(Clone)]
pub struct VaultKey {
    bytes: [u8; KEY_LEN],
}

impl VaultKey {
    /// Decodes a 32-byte key from standard or URL-safe base64.
    pub fn from_base64(raw: &str) -> Result<Self> {
        let raw = raw.trim();
        let mut decoded = [STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD]
            .iter()
            .find_map(|engine| engine.decode(raw).ok())
            .ok_or_else(|| invalid_key("key is not valid base64"))?;
        if decoded.len() != KEY_LEN {
            decoded.zeroize();
            return Err(invalid_key(format!(
                "key must decode to 32 bytes (got {})",
                decoded.len()
            )));
        }
        let mut bytes = [0u8; KEY_LEN];
        bytes.copy_from_slice(&decoded);
        decoded.zeroize();
        Ok(Self { bytes })
    }

    /// Builds a key from exactly 32 raw bytes.
    pub fn from_bytes(bytes: [u8; KEY_LEN]) -> Self {
        Self { bytes }
    }

    fn cipher(&self) -> XChaCha20Poly1305 {
        XChaCha20Poly1305::new((&self.bytes).into())
    }
}

impl std::fmt::Debug for VaultKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("VaultKey(<redacted>)")
    }
}

impl Drop for VaultKey {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

fn invalid_key(message: impl Into<String>) -> Error {
    Error::Vault {
        code: "invalid_vault_key",
        message: message.into(),
    }
}

fn decryption_failed(message: impl Into<String>) -> Error {
    Error::Vault {
        code: "decryption_failed",
        message: message.into(),
    }
}

fn invalid_filter(message: impl Into<String>) -> Error {
    Error::Vault {
        code: "invalid_vault_filter",
        message: message.into(),
    }
}

fn tenant_mismatch() -> Error {
    Error::Vault {
        code: "tenant_mismatch",
        message: "the document or filter belongs to a different tenant".into(),
    }
}

/// Validates a tenant id and key id before any crypto work.
pub fn validate_context(tenant: &str, kid: &str) -> Result<()> {
    if tenant.is_empty() || tenant.len() > 200 || tenant.contains('\0') {
        return Err(invalid_key("invalid tenant id"));
    }
    if kid.is_empty() || kid.len() > MAX_KEY_ID_LEN || kid.contains('\0') {
        return Err(invalid_key("invalid key id"));
    }
    Ok(())
}

fn aad(coll: &str, tenant: &str, id: &str, kid: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + coll.len() + tenant.len() + id.len() + kid.len());
    out.extend_from_slice(b"nonodb:vault:v1\n");
    out.extend_from_slice(coll.as_bytes());
    out.push(b'\n');
    out.extend_from_slice(tenant.as_bytes());
    out.push(b'\n');
    out.extend_from_slice(id.as_bytes());
    out.push(b'\n');
    out.extend_from_slice(kid.as_bytes());
    out
}

/// Encrypts a complete document into an envelope value.
pub fn encrypt_document(
    plaintext: &Value,
    key: &VaultKey,
    coll: &str,
    tenant: &str,
    id: &str,
    kid: &str,
) -> Result<Value> {
    validate_context(tenant, kid)?;
    let msg = Zeroizing::new(serde_json::to_vec(plaintext)?);
    let mut nonce = [0u8; NONCE_LEN];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let ciphertext = key
        .cipher()
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: msg.as_slice(),
                aad: &aad(coll, tenant, id, kid),
            },
        )
        .map_err(|_| Error::Vault {
            code: "encryption_failed",
            message: "could not encrypt document".into(),
        })?;
    Ok(serde_json::json!({
        "_enc": {
            "v": ENVELOPE_VERSION,
            "alg": ALGORITHM,
            "kid": kid,
            "n": STANDARD.encode(nonce),
            "ct": STANDARD.encode(ciphertext),
        }
    }))
}

/// Decrypts an envelope and returns the original document.
pub fn decrypt_document(
    stored: &Value,
    key: &VaultKey,
    coll: &str,
    tenant: &str,
    id: &str,
    kid: &str,
) -> Result<Value> {
    validate_context(tenant, kid)?;
    let enc = stored
        .get("_enc")
        .and_then(Value::as_object)
        .ok_or_else(|| decryption_failed("encrypted envelope is missing"))?;
    if enc.get("v").and_then(Value::as_u64) != Some(ENVELOPE_VERSION) {
        return Err(decryption_failed("unsupported envelope version"));
    }
    if enc.get("alg").and_then(Value::as_str) != Some(ALGORITHM) {
        return Err(decryption_failed("unsupported encryption algorithm"));
    }
    if enc.get("kid").and_then(Value::as_str) != Some(kid) {
        return Err(decryption_failed("key id does not match the envelope"));
    }
    let nonce = enc
        .get("n")
        .and_then(Value::as_str)
        .and_then(|s| STANDARD.decode(s).ok())
        .ok_or_else(|| decryption_failed("invalid nonce"))?;
    let ciphertext = enc
        .get("ct")
        .and_then(Value::as_str)
        .and_then(|s| STANDARD.decode(s).ok())
        .ok_or_else(|| decryption_failed("invalid ciphertext"))?;
    if nonce.len() != NONCE_LEN {
        return Err(decryption_failed("invalid nonce length"));
    }
    let plaintext = Zeroizing::new(
        key.cipher()
            .decrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: &ciphertext,
                    aad: &aad(coll, tenant, id, kid),
                },
            )
            .map_err(|_| decryption_failed("wrong key, wrong tenant, or corrupted envelope"))?,
    );
    let value: Value = serde_json::from_slice(plaintext.as_slice())
        .map_err(|_| decryption_failed("decrypted payload is not valid JSON"))?;
    Ok(value)
}

/// Returns the plaintext `tenant_id` from a stored vault document.
pub fn stored_tenant(doc: &Value) -> Option<&str> {
    doc.get(TENANT_FIELD).and_then(Value::as_str)
}

/// Validates a read filter. Only exact `_id` and `tenant_id` matches are allowed.
/// The returned filter always contains `tenant_id` from the authenticated request.
pub fn read_filter(filter: &Value, tenant: &str) -> Result<Value> {
    let source = match filter {
        Value::Null => Map::new(),
        Value::Object(obj) => obj.clone(),
        _ => return Err(invalid_filter("vault filter must be an object or null")),
    };
    let mut out = Map::new();
    for (field, value) in source {
        match field.as_str() {
            "_id" => {
                let id = value
                    .as_str()
                    .ok_or_else(|| invalid_filter("`_id` must be an exact string"))?;
                out.insert("_id".into(), Value::String(id.to_string()));
            }
            TENANT_FIELD => {
                let got = value
                    .as_str()
                    .ok_or_else(|| invalid_filter("`tenant_id` must be an exact string"))?;
                if got != tenant {
                    return Err(tenant_mismatch());
                }
            }
            other => {
                return Err(invalid_filter(format!(
                    "field `{other}` is not queryable in vault collections"
                )))
            }
        }
    }
    out.insert(TENANT_FIELD.into(), Value::String(tenant.to_string()));
    Ok(Value::Object(out))
}

/// Validates and extracts an exact `_id` for update/delete.
pub fn exact_id_filter(filter: &Value, tenant: &str) -> Result<String> {
    let obj = filter
        .as_object()
        .ok_or_else(|| invalid_filter("vault update/delete requires an exact `_id` filter"))?;
    let mut id = None;
    for (field, value) in obj {
        match field.as_str() {
            "_id" => {
                id = Some(
                    value
                        .as_str()
                        .ok_or_else(|| invalid_filter("`_id` must be an exact string"))?
                        .to_string(),
                );
            }
            TENANT_FIELD => {
                let got = value
                    .as_str()
                    .ok_or_else(|| invalid_filter("`tenant_id` must be an exact string"))?;
                if got != tenant {
                    return Err(tenant_mismatch());
                }
            }
            other => {
                return Err(invalid_filter(format!(
                    "vault update/delete cannot filter on `{other}`"
                )))
            }
        }
    }
    id.ok_or_else(|| invalid_filter("vault update/delete requires an exact `_id` filter"))
}

/// Vault sort supports only `_id` (or no sort).
pub fn validate_sort(sort: &[(String, i32)]) -> Result<()> {
    if sort.is_empty() {
        return Ok(());
    }
    if sort.len() == 1 && sort[0].0 == "_id" && matches!(sort[0].1, 1 | -1) {
        return Ok(());
    }
    Err(Error::Vault {
        code: "unsupported_encrypted_operation",
        message: "vault collections can only be sorted by `_id`".into(),
    })
}

/// Vault projections are not supported.
pub fn validate_projection(projection: Option<&Value>) -> Result<()> {
    if projection.is_none() {
        return Ok(());
    }
    Err(Error::Vault {
        code: "unsupported_encrypted_operation",
        message: "projection is not supported for vault collections".into(),
    })
}

/// Error for a standard-only operation attempted on a vault collection.
pub fn unsupported(operation: &str) -> Error {
    Error::Vault {
        code: "unsupported_encrypted_operation",
        message: format!("{operation} is not supported for vault collections"),
    }
}

/// Error for a vault operation attempted without an authenticated key.
pub fn key_required() -> Error {
    Error::Vault {
        code: "vault_key_required",
        message:
            "vault collections require X-NonoDB-Tenant, X-NonoDB-Key-Id, and X-NonoDB-Key headers"
                .into(),
    }
}

/// Error for a key sent to a standard collection.
pub fn key_unexpected() -> Error {
    Error::Vault {
        code: "vault_key_unexpected",
        message: "encryption headers were sent to a standard collection".into(),
    }
}

/// Error when a vault request targets a collection that does not exist or is
/// not a vault collection.
pub fn not_a_vault_collection() -> Error {
    Error::Vault {
        code: "not_a_vault_collection",
        message: "collection does not exist or is not a vault collection".into(),
    }
}

/// Error for a standard write attempted on a vault collection.
pub fn standard_write_forbidden() -> Error {
    Error::Vault {
        code: "unsupported_encrypted_operation",
        message: "use the vault write path with encryption headers".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> VaultKey {
        VaultKey::from_bytes([7u8; 32])
    }

    #[test]
    fn round_trip_and_aad_binding() {
        let doc = serde_json::json!({"_id":"a", "tenant_id":"acme", "secret":"x"});
        let enc = encrypt_document(&doc, &key(), "v", "acme", "a", "acme:v1").unwrap();
        let out = decrypt_document(&enc, &key(), "v", "acme", "a", "acme:v1").unwrap();
        assert_eq!(out, doc);
        assert!(decrypt_document(&enc, &key(), "v", "other", "a", "acme:v1").is_err());
        assert!(decrypt_document(&enc, &key(), "v", "acme", "b", "acme:v1").is_err());
    }

    #[test]
    fn filters_are_narrow() {
        assert_eq!(
            read_filter(&serde_json::json!({"_id":"a"}), "acme").unwrap(),
            serde_json::json!({"_id":"a", "tenant_id":"acme"})
        );
        assert!(read_filter(&serde_json::json!({"name":"a"}), "acme").is_err());
        assert!(read_filter(&serde_json::json!({"tenant_id":"other"}), "acme").is_err());
        assert!(exact_id_filter(&serde_json::json!({"_id":"a"}), "acme").is_ok());
        assert!(exact_id_filter(&serde_json::json!({}), "acme").is_err());
    }
}
