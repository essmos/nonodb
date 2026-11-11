//! BSON scalar types represented with MongoDB Extended JSON.
//!
//! Supported here:
//!
//! - binary data / UUID: `{"$binary": {"base64": "...", "subType": "00"}}`
//!   and `{"$uuid": "..."}`,
//! - ObjectId: `{"$oid": "..."}`.
//!
//! Canonical storage uses the `$binary` and `$oid` forms. `$type` query
//! matching and index ordering are implemented alongside these values.

use std::cmp::Ordering;

use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};

use crate::date;
use crate::error::{Error, Result};
use crate::numeric;

pub const BINARY_KEY: &str = "$binary";
pub const UUID_KEY: &str = "$uuid";
pub const OBJECT_ID_KEY: &str = "$oid";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Binary {
    pub subtype: u8,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectId(pub [u8; 12]);

/// Parses canonical `$binary` or shorthand `$uuid`.
pub fn parse_binary(value: &Value) -> Option<Binary> {
    let obj = value.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    if let Some(Value::Object(payload)) = obj.get(BINARY_KEY) {
        let base64 = payload.get("base64")?.as_str()?;
        let subtype = parse_subtype(payload.get("subType")?.as_str()?)?;
        let bytes = STANDARD.decode(base64).ok()?;
        return Some(Binary { subtype, bytes });
    }
    if let Some(Value::String(s)) = obj.get(UUID_KEY) {
        let uuid = uuid::Uuid::parse_str(s).ok()?;
        return Some(Binary {
            subtype: 0x04,
            bytes: uuid.as_bytes().to_vec(),
        });
    }
    None
}

/// Parses `{"$oid": "..."}`.
pub fn parse_object_id(value: &Value) -> Option<ObjectId> {
    let obj = value.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    let s = obj.get(OBJECT_ID_KEY)?.as_str()?;
    let mut out = [0u8; 12];
    if s.len() != 24 {
        return None;
    }
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(ObjectId(out))
}

pub fn is_binary(value: &Value) -> bool {
    parse_binary(value).is_some()
}

pub fn is_object_id(value: &Value) -> bool {
    parse_object_id(value).is_some()
}

pub fn is_bson_value(value: &Value) -> bool {
    is_binary(value) || is_object_id(value)
}

/// Whether a filter/document tree contains binary or ObjectId values.
pub fn contains_type(value: &Value) -> bool {
    if is_bson_value(value) {
        return true;
    }
    match value {
        Value::Array(items) => items.iter().any(contains_type),
        Value::Object(obj) => obj.values().any(contains_type),
        _ => false,
    }
}

pub fn binary_value(subtype: u8, bytes: &[u8]) -> Value {
    json!({
        BINARY_KEY: {
            "base64": STANDARD.encode(bytes),
            "subType": format!("{subtype:02x}")
        }
    })
}

pub fn object_id_value(id: ObjectId) -> Value {
    let mut hex = String::with_capacity(24);
    for b in id.0 {
        use std::fmt::Write;
        let _ = write!(hex, "{b:02x}");
    }
    json!({ OBJECT_ID_KEY: hex })
}

/// Equality for binary/ObjectId values. Returns `Some(false)` when either side
/// is one of these types and the other side is not.
pub fn eq_values(a: &Value, b: &Value) -> Option<bool> {
    if let (Some(x), Some(y)) = (parse_binary(a), parse_binary(b)) {
        return Some(x == y);
    }
    if let (Some(x), Some(y)) = (parse_object_id(a), parse_object_id(b)) {
        return Some(x == y);
    }
    if is_bson_value(a) || is_bson_value(b) {
        return Some(false);
    }
    None
}

pub fn cmp_values(a: &Value, b: &Value) -> Option<Ordering> {
    if let (Some(x), Some(y)) = (parse_binary(a), parse_binary(b)) {
        return Some(compare_binary(&x, &y));
    }
    if let (Some(x), Some(y)) = (parse_object_id(a), parse_object_id(b)) {
        return Some(x.0.cmp(&y.0));
    }
    None
}

pub fn encode_binary_ordered(binary: &Binary) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + binary.bytes.len());
    out.push(binary.subtype);
    out.extend_from_slice(&(binary.bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&binary.bytes);
    out
}

pub fn binary_key(value: &Value) -> Option<Vec<u8>> {
    Some(encode_binary_ordered(&parse_binary(value)?))
}

pub fn object_id_key(value: &Value) -> Option<[u8; 12]> {
    Some(parse_object_id(value)?.0)
}

/// Canonicalizes binary/ObjectId Extended JSON values in a document tree.
pub fn normalize_value(value: &mut Value) -> Result<()> {
    if let Some(obj) = value.as_object() {
        if obj.len() == 1 && (obj.contains_key(BINARY_KEY) || obj.contains_key(UUID_KEY)) {
            let parsed = parse_binary(value).ok_or_else(|| {
                Error::InvalidBinary(
                    "`$binary` requires base64+subType, or `$uuid` requires a UUID string".into(),
                )
            })?;
            *value = binary_value(parsed.subtype, &parsed.bytes);
            return Ok(());
        }
        if obj.len() == 1 && obj.contains_key(OBJECT_ID_KEY) {
            let parsed = parse_object_id(value).ok_or_else(|| {
                Error::InvalidObjectId("`$oid` must be a 24-character hexadecimal string".into())
            })?;
            *value = object_id_value(parsed);
            return Ok(());
        }
    }
    if let Some(obj) = value.as_object_mut() {
        for item in obj.values_mut() {
            normalize_value(item)?;
        }
    } else if let Some(items) = value.as_array_mut() {
        for item in items {
            normalize_value(item)?;
        }
    }
    Ok(())
}

pub fn normalize_document(value: &mut Value) -> Result<()> {
    if let Some(obj) = value.as_object_mut() {
        for item in obj.values_mut() {
            normalize_value(item)?;
        }
    }
    Ok(())
}

/// BSON type name used by the `$type` query operator.
pub fn type_name(value: &Value) -> Option<&'static str> {
    if date::is_date(value) {
        return Some("date");
    }
    if let Some(n) = numeric::parse_value(value) {
        return Some(match n.kind() {
            numeric::NumericKind::Int => "int",
            numeric::NumericKind::Long => "long",
            numeric::NumericKind::Double => "double",
            numeric::NumericKind::Decimal => "decimal",
        });
    }
    if is_binary(value) {
        return Some("binData");
    }
    if is_object_id(value) {
        return Some("objectId");
    }
    match value {
        Value::Null => Some("null"),
        Value::Bool(_) => Some("bool"),
        Value::String(_) => Some("string"),
        Value::Array(_) => Some("array"),
        Value::Object(_) => Some("object"),
        Value::Number(_) => None,
    }
}

/// Evaluates a `$type` operand: a name/code or an array of names/codes.
pub fn type_matches(value: &Value, spec: &Value) -> bool {
    if let Some(items) = spec.as_array() {
        return items.iter().any(|item| type_matches_one(value, item));
    }
    type_matches_one(value, spec)
}

fn type_matches_one(value: &Value, spec: &Value) -> bool {
    if direct_type_matches(value, spec) {
        return true;
    }
    if let Value::Array(items) = value {
        return items.iter().any(|item| type_matches_one(item, spec));
    }
    false
}

fn direct_type_matches(value: &Value, spec: &Value) -> bool {
    if let Some(name) = spec.as_str() {
        return match name.to_ascii_lowercase().as_str() {
            "number" => numeric::is_numeric(value),
            other => type_name(value).is_some_and(|actual| actual.eq_ignore_ascii_case(other)),
        };
    }
    let Some(code) = numeric::integer_value(spec) else {
        return false;
    };
    let name = match code {
        1 => "double",
        2 => "string",
        3 => "object",
        4 => "array",
        5 => "binData",
        7 => "objectId",
        8 => "bool",
        9 => "date",
        10 => "null",
        16 => "int",
        18 => "long",
        19 => "decimal",
        -1 => "minKey",
        127 => "maxKey",
        _ => return false,
    };
    if code == 1 && numeric::is_numeric(value) {
        return numeric::parse_value(value)
            .is_some_and(|n| n.kind() == numeric::NumericKind::Double);
    }
    type_name(value).is_some_and(|actual| actual.eq_ignore_ascii_case(name))
}

fn compare_binary(a: &Binary, b: &Binary) -> Ordering {
    a.subtype
        .cmp(&b.subtype)
        .then_with(|| a.bytes.len().cmp(&b.bytes.len()))
        .then_with(|| a.bytes.cmp(&b.bytes))
}

fn parse_subtype(s: &str) -> Option<u8> {
    if s.len() != 2 {
        return None;
    }
    u8::from_str_radix(s, 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_becomes_binary_subtype_four() {
        let mut value = json!({"$uuid": "00112233-4455-6677-8899-aabbccddeeff"});
        normalize_value(&mut value).unwrap();
        let parsed = parse_binary(&value).unwrap();
        assert_eq!(parsed.subtype, 4);
        assert_eq!(parsed.bytes.len(), 16);
        assert_eq!(parsed.bytes[0], 0x00);
        assert_eq!(parsed.bytes[15], 0xff);
    }

    #[test]
    fn binary_roundtrips_and_rejects_bad_base64() {
        let value = binary_value(0x80, b"hello\x00world");
        let parsed = parse_binary(&value).unwrap();
        assert_eq!(parsed.subtype, 0x80);
        assert_eq!(parsed.bytes, b"hello\x00world");
        let mut bad = json!({"$binary": {"base64": "!!!", "subType": "00"}});
        assert!(normalize_value(&mut bad).is_err());
    }

    #[test]
    fn object_id_roundtrips_and_orders_by_bytes() {
        let a = parse_object_id(&json!({"$oid": "000000000000000000000001"})).unwrap();
        let b = parse_object_id(&json!({"$oid": "000000000000000000000002"})).unwrap();
        assert!(a < b);
        assert_eq!(
            object_id_value(a),
            json!({"$oid": "000000000000000000000001"})
        );
        let mut bad = json!({"$oid": "xyz"});
        assert!(normalize_value(&mut bad).is_err());
    }

    #[test]
    fn type_matching_names_and_codes() {
        let binary = binary_value(0, b"x");
        let object_id = object_id_value(ObjectId([1; 12]));
        assert!(type_matches(&binary, &json!("binData")));
        assert!(type_matches(&binary, &json!(5)));
        assert!(type_matches(&object_id, &json!("objectId")));
        assert!(type_matches(&json!({"$numberInt": "1"}), &json!("int")));
        assert!(type_matches(&json!({"$numberLong": "1"}), &json!("long")));
        assert!(type_matches(&json!(1), &json!("number")));
        assert!(!type_matches(&json!("1"), &json!("number")));
        assert!(type_matches(&json!(["a", 1]), &json!("string")));
        assert!(type_matches(&json!(["a", 1]), &json!("array")));
    }
}
