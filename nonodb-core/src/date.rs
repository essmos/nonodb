//! BSON-compatible dates represented as Extended JSON.
//!
//! A date is stored as `{"$date": {"$numberLong": "<milliseconds>"}}`, matching
//! MongoDB canonical Extended JSON. Dates are instants: an RFC 3339 offset is
//! normalized to UTC and no timezone is stored. The input forms accepted by the
//! server are:
//!
//! - `{"$date": "2026-01-01T01:00:00+01:00"}` (relaxed Extended JSON),
//! - `{"$date": {"$numberLong": "1767222000000"}}` (canonical Extended JSON),
//! - `{"$date": 1767222000000}` (convenience numeric form).
//!
//! RFC 3339 strings must include `Z` or a numeric offset. Naive/local datetimes
//! are rejected because their instant is ambiguous.

use std::cmp::Ordering;

use serde_json::{json, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::error::{Error, Result};

pub const DATE_KEY: &str = "$date";
pub const NUMBER_LONG_KEY: &str = "$numberLong";

/// Parses a date value, returning Unix milliseconds.
pub fn parse_value(value: &Value) -> Option<i64> {
    let obj = value.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    parse_payload(obj.get(DATE_KEY)?)
}

/// Whether the value is a valid Extended JSON date object.
pub fn is_date(value: &Value) -> bool {
    parse_value(value).is_some()
}

/// Whether a filter/document tree contains any valid date value.
pub fn contains_date(value: &Value) -> bool {
    if is_date(value) {
        return true;
    }
    match value {
        Value::Array(items) => items.iter().any(contains_date),
        Value::Object(obj) => obj.values().any(contains_date),
        _ => false,
    }
}

/// Returns the canonical Extended JSON representation.
pub fn canonical(ms: i64) -> Value {
    json!({ DATE_KEY: { NUMBER_LONG_KEY: ms.to_string() } })
}

/// Recursively canonicalizes Extended JSON dates in a document or update value.
///
/// An object with a single `$date` key is reserved for dates. Invalid payloads
/// are rejected instead of being silently stored as ordinary objects.
pub fn normalize_value(value: &mut Value) -> Result<()> {
    if let Some(ms) = parse_value(value) {
        *value = canonical(ms);
        return Ok(());
    }
    if let Some(obj) = value.as_object_mut() {
        if obj.len() == 1 && obj.contains_key(DATE_KEY) {
            return Err(Error::InvalidDate(
                "`$date` must be an RFC 3339 string, epoch milliseconds, or `$numberLong`".into(),
            ));
        }
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

/// Canonicalizes date values inside a top-level document.
///
/// The document itself is never interpreted as a date, even if it has a
/// reserved `$date` key, because a document must remain an object.
pub fn normalize_document(value: &mut Value) -> Result<()> {
    if let Some(obj) = value.as_object_mut() {
        for item in obj.values_mut() {
            normalize_value(item)?;
        }
    }
    Ok(())
}

/// Compares two date values by instant.
pub fn cmp(a: &Value, b: &Value) -> Option<Ordering> {
    Some(parse_value(a)?.cmp(&parse_value(b)?))
}

/// Equality by instant (different offsets for the same instant are equal).
pub fn eq(a: &Value, b: &Value) -> Option<bool> {
    Some(parse_value(a)? == parse_value(b)?)
}

/// Encodes a date as an order-preserving 8-byte integer.
pub fn encode_ordered(ms: i64) -> [u8; 8] {
    // Flip the sign bit so lexicographic byte order matches signed i64 order.
    ((ms as u64) ^ (1u64 << 63)).to_be_bytes()
}

fn parse_payload(payload: &Value) -> Option<i64> {
    match payload {
        Value::String(s) => parse_rfc3339_ms(s),
        Value::Number(n) => number_to_i64(n),
        Value::Object(obj) if obj.len() == 1 => {
            let raw = obj.get(NUMBER_LONG_KEY)?;
            match raw {
                Value::String(s) => s.parse::<i64>().ok(),
                Value::Number(n) => number_to_i64(n),
                _ => None,
            }
        }
        _ => None,
    }
}

fn number_to_i64(n: &serde_json::Number) -> Option<i64> {
    n.as_i64()
        .or_else(|| n.as_u64().and_then(|v| i64::try_from(v).ok()))
}

fn parse_rfc3339_ms(raw: &str) -> Option<i64> {
    let dt = OffsetDateTime::parse(raw, &Rfc3339).ok()?;
    let ms = dt.unix_timestamp_nanos().div_euclid(1_000_000);
    i64::try_from(ms).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_offsets_to_the_same_instant() {
        let utc = json!({"$date": "2026-01-01T00:00:00Z"});
        let warsaw = json!({"$date": "2026-01-01T01:00:00+01:00"});
        assert_eq!(parse_value(&utc), Some(1_767_225_600_000));
        assert_eq!(parse_value(&utc), parse_value(&warsaw));
        assert_eq!(eq(&utc, &warsaw), Some(true));
    }

    #[test]
    fn parses_canonical_and_numeric_forms() {
        let canonical = json!({"$date": {"$numberLong": "1767225600000"}});
        let numeric = json!({"$date": 1767225600000i64});
        assert_eq!(parse_value(&canonical), Some(1_767_225_600_000));
        assert_eq!(parse_value(&numeric), Some(1_767_225_600_000));
        assert_eq!(canonical, self::canonical(1_767_225_600_000));
    }

    #[test]
    fn rejects_naive_dates() {
        let mut v = json!({"created_at": {"$date": "2026-01-01T00:00:00"}});
        assert!(normalize_value(&mut v).is_err());
        assert!(!is_date(&json!({"$date": "2026-01-01"})));
    }

    #[test]
    fn normalizes_nested_dates() {
        let mut v = json!({
            "created_at": {"$date": "2026-01-01T01:00:00+01:00"},
            "items": [{"at": {"$date": {"$numberLong": "0"}}}]
        });
        normalize_value(&mut v).unwrap();
        assert_eq!(
            v["created_at"],
            json!({"$date": {"$numberLong": "1767225600000"}})
        );
        assert_eq!(v["items"][0]["at"], canonical(0));
    }

    #[test]
    fn encoding_preserves_signed_order() {
        let values = [-10i64, -1, 0, 1, 10];
        for pair in values.windows(2) {
            assert!(encode_ordered(pair[0]) < encode_ordered(pair[1]));
        }
    }

    #[test]
    fn document_root_is_never_a_date() {
        let mut doc = json!({"$date": "not a document field"});
        normalize_document(&mut doc).unwrap();
        assert_eq!(doc, json!({"$date": "not a document field"}));
    }

    #[test]
    fn detects_dates_inside_filters() {
        assert!(contains_date(&json!({
            "at": {"$gte": {"$date": "2026-01-01T00:00:00Z"}}
        })));
        assert!(contains_date(&json!({
            "at": {"$in": [{"$date": "2026-01-01T00:00:00Z"}]}
        })));
        assert!(!contains_date(&json!({"at": {"$gte": 10}})));
    }

    #[test]
    fn rejects_non_date_payloads() {
        for value in [
            json!({"$date": true}),
            json!({"$date": []}),
            json!({"$date": 1.5}),
            json!({"$date": {"$numberLong": "not-a-number"}}),
            json!({"$date": "2026-01-01"}),
            json!({"$date": "2026-01-01T00:00:00"}),
        ] {
            let mut doc = json!({"at": value});
            assert!(normalize_value(&mut doc).is_err(), "{doc}");
        }
    }
}
