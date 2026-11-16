//! Document primary-key encoding.
//!
//! MongoDB applications normally use ObjectId `_id` values. NonoDB keeps the
//! LMDB key as UTF-8, so ObjectIds use a reserved, high-codepoint prefix. That
//! prefix sorts after ordinary UTF-8 strings, matching BSON's type order where
//! strings precede ObjectIds.

use serde_json::Value;

use crate::bson_types;
use crate::error::{Error, Result};

/// Reserved internal key prefix for ObjectId `_id` values.
pub const OBJECT_ID_KEY_PREFIX: &str = "\u{10ffff}\u{10ffff}oid:";

/// Encodes a document `_id` value as an LMDB key.
pub fn key_from_value(value: &Value) -> Option<String> {
    if let Some(value) = value.as_str() {
        if value.starts_with(OBJECT_ID_KEY_PREFIX) {
            return None;
        }
        return Some(value.to_string());
    }
    let id = bson_types::parse_object_id(value)?;
    let mut key = String::with_capacity(OBJECT_ID_KEY_PREFIX.len() + 24);
    key.push_str(OBJECT_ID_KEY_PREFIX);
    for byte in id.0 {
        use std::fmt::Write;
        let _ = write!(key, "{byte:02x}");
    }
    Some(key)
}

/// Encodes the `_id` field of a document as an LMDB key.
pub fn key_from_document(document: &Value) -> Option<String> {
    key_from_value(document.get("_id")?)
}

/// Returns whether an internal key represents an ObjectId.
pub fn is_object_id_key(key: &str) -> bool {
    key.starts_with(OBJECT_ID_KEY_PREFIX)
}

/// Reconstructs an Extended JSON ObjectId from an internal key.
pub fn object_id_from_key(key: &str) -> Option<Value> {
    let hex = key.strip_prefix(OBJECT_ID_KEY_PREFIX)?;
    if hex.len() != 24 {
        return None;
    }
    let mut bytes = [0u8; 12];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(bson_types::object_id_value(bson_types::ObjectId(bytes)))
}

/// Rejects user string IDs that would collide with the ObjectId key namespace.
pub fn validate_string_key(value: &str) -> Result<()> {
    if value.starts_with(OBJECT_ID_KEY_PREFIX) {
        return Err(Error::InvalidId);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn string_and_object_id_keys_are_distinct_and_ordered() {
        let string = key_from_value(&json!("id")).unwrap();
        let object = key_from_value(&json!({"$oid": "000000000000000000000001"})).unwrap();
        assert_eq!(string, "id");
        assert!(is_object_id_key(&object));
        assert!(string.as_bytes() < object.as_bytes());
        assert_eq!(
            object_id_from_key(&object).unwrap(),
            json!({"$oid": "000000000000000000000001"})
        );
    }

    #[test]
    fn reserved_prefix_cannot_be_used_as_a_string_id() {
        let value = format!("{OBJECT_ID_KEY_PREFIX}000000000000000000000001");
        assert!(key_from_value(&json!(value)).is_none());
        assert!(validate_string_key(&value).is_err());
    }
}
