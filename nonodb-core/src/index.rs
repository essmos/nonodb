//! Secondary indexes: key encoding and definitions.

use std::ops::Bound;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::bson_types;
use crate::date;
use crate::numeric;
use crate::regex;

/// Index encoding format. Version 0 means an index created before
/// BSON-compatible dates existed; date values were skipped in those indexes.
/// Version 2 used the old `f64` encoding for all numbers; version 3 uses exact
/// decimal numeric keys and supports Decimal128; version 4 adds Extended JSON
/// binary/UUID and ObjectId keys.
pub const FORMAT_VERSION: u8 = 4;

/// Index definition stored in `meta` under `index:<coll>:<name>`.
///
/// Supports single-field (`fields == ["age"]`) and compound
/// (`fields == ["grp", "age"]`) indexes. `field` exists only for backward
/// compatibility with databases written before compound indexes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    #[serde(default)]
    pub fields: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub field: String,
    #[serde(default)]
    pub unique: bool,
    /// Fields additionally copied into the index value (covering projections).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,
    /// Index key encoding version. Missing in definitions written before date
    /// support and therefore defaults to 0.
    #[serde(default)]
    pub format: u8,
}

impl IndexDef {
    pub fn single(field: &str, unique: bool) -> Self {
        Self {
            fields: vec![field.to_string()],
            field: String::new(),
            unique,
            include: Vec::new(),
            format: FORMAT_VERSION,
        }
    }

    pub fn compound(fields: &[String], unique: bool, include: &[String]) -> Self {
        Self {
            fields: fields.to_vec(),
            field: String::new(),
            unique,
            include: include.to_vec(),
            format: FORMAT_VERSION,
        }
    }

    /// Whether the index was built with the current key encoding.
    pub fn current_format(&self) -> bool {
        self.format >= FORMAT_VERSION
    }

    /// Fields in index order (with support for the legacy `field` format).
    pub fn effective_fields(&self) -> Vec<String> {
        if !self.fields.is_empty() {
            self.fields.clone()
        } else if !self.field.is_empty() {
            vec![self.field.clone()]
        } else {
            Vec::new()
        }
    }

    /// Logical name: `age` or `grp+age`.
    pub fn name(&self) -> String {
        self.effective_fields().join("+")
    }
}

/// Index sub-database name.
pub fn db_name(coll: &str, field: &str) -> String {
    format!("idx:{coll}:{field}")
}

/// Index definition key in `meta`.
pub fn def_key(coll: &str, field: &str) -> String {
    format!("index:{coll}:{field}")
}

/// Document field value (supports `a.b` paths).
pub fn field_value<'a>(doc: &'a Value, field: &str) -> Option<&'a Value> {
    crate::query::path::get(doc, field)
}

/// Encodes a scalar value into bytes whose lexicographic order
/// matches value order. Returns `None` for non-indexable types.
///
/// Layout: `[tag] [data]`.
pub fn encode_value(v: &Value) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(10);
    match v {
        Value::Null => out.push(0x01),
        Value::Number(_) => {
            out.push(0x02);
            out.extend_from_slice(&numeric::encode_ordered(v)?);
        }
        Value::String(s) => {
            out.push(0x03);
            push_escaped_string(&mut out, s);
            out.push(0x00);
            out.push(0x00);
        }
        Value::Bool(b) => {
            out.push(0x04);
            out.push(u8::from(*b));
        }
        _ => {
            if let Some(key) = numeric::encode_ordered(v) {
                out.push(0x02);
                out.extend_from_slice(&key);
            } else if let Some(key) = bson_types::binary_key(v) {
                out.push(0x06);
                out.extend_from_slice(&key);
            } else if let Some(id) = bson_types::object_id_key(v) {
                out.push(0x07);
                out.extend_from_slice(&id);
            } else {
                let ms = date::parse_value(v)?;
                out.push(0x05);
                out.extend_from_slice(&date::encode_ordered(ms));
            }
        }
    }
    Some(out)
}

fn push_escaped_string(out: &mut Vec<u8>, s: &str) {
    // Escape NUL: 0x00 -> 0x00 0x01. The string terminator is appended by
    // `encode_value`; prefix bounds intentionally leave it off.
    for &b in s.as_bytes() {
        if b == 0 {
            out.push(0x00);
            out.push(0x01);
        } else {
            out.push(b);
        }
    }
}

/// Index range for all strings beginning with `prefix` (`^prefix` regex).
pub fn string_prefix_bounds(prefix: &str) -> RangeBytes {
    let mut lower = vec![0x03];
    push_escaped_string(&mut lower, prefix);
    let upper = after_value(&lower);
    (Bound::Included(lower), Bound::Excluded(upper))
}

/// Encodes values of all index fields (compound index).
/// The document is skipped if any field is missing or is an
/// array/object.
pub fn encode_fields(doc: &Value, fields: &[String]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(fields.len() * 10);
    for field in fields {
        out.extend_from_slice(&encode_value(field_value(doc, field)?)?);
    }
    Some(out)
}

/// Index entry value. When the index has `include` fields, the value contains
/// `_id` plus a copy of index and `include` fields (covering projections). In the
/// legacy format the value is just `_id`.
pub fn encode_index_value(def: &IndexDef, doc: &Value, id: &str) -> Vec<u8> {
    if def.include.is_empty() {
        return id.as_bytes().to_vec();
    }
    let mut inc = serde_json::Map::new();
    for field in def.effective_fields().iter().chain(def.include.iter()) {
        if let Some(v) = field_value(doc, field) {
            inc.insert(field.clone(), v.clone());
        }
    }
    let payload = serde_json::json!({ "id": id, "inc": inc });
    match serde_json::to_vec(&payload) {
        Ok(mut bytes) => {
            bytes.insert(0, 0);
            bytes
        }
        Err(_) => id.as_bytes().to_vec(),
    }
}

/// Reads an index entry value: `(id, optional covering fields)`.
pub fn decode_index_value(raw: &[u8]) -> Option<(String, Option<serde_json::Map<String, Value>>)> {
    if raw.first() == Some(&0) {
        if let Ok(Value::Object(mut obj)) = serde_json::from_slice::<Value>(&raw[1..]) {
            let id = match obj.remove("id") {
                Some(Value::String(s)) => s,
                _ => return None,
            };
            let inc = match obj.remove("inc") {
                Some(Value::Object(m)) => Some(m),
                _ => None,
            };
            return Some((id, inc));
        }
    }
    std::str::from_utf8(raw)
        .ok()
        .map(|id| (id.to_string(), None))
}

fn after_value(vb: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(vb.len() + 1);
    out.extend_from_slice(vb);
    out.push(0xFF);
    out
}

/// Index key range for a comparison operator.
///
/// An entry key is `encode_value(value) ++ _id`, so `vb + 0xFF` lies after
/// all entries with value `vb` and before any larger value.
pub type RangeBytes = (Bound<Vec<u8>>, Bound<Vec<u8>>);

pub fn bounds(op: &str, value: &Value) -> Option<RangeBytes> {
    let vb = encode_value(value)?;
    Some(match op {
        "$eq" => (
            Bound::Included(vb.clone()),
            Bound::Excluded(after_value(&vb)),
        ),
        "$gte" => (Bound::Included(vb), Bound::Unbounded),
        "$gt" => (Bound::Included(after_value(&vb)), Bound::Unbounded),
        "$lte" => (Bound::Unbounded, Bound::Excluded(after_value(&vb))),
        "$lt" => (Bound::Unbounded, Bound::Excluded(vb)),
        _ => return None,
    })
}

/// Range for a compound condition, e.g. `{"$gte": 18, "$lt": 30}`.
///
/// Supports one operator (`$eq`, `$gt`, `$gte`, `$lt`, `$lte`) or several
/// range operators on the same field. The range is limited to the same
/// type as the condition value; the matcher does not compare different types, so
/// otherwise `$gt: 2` would also include strings/bools from the index.
pub fn bounds_from_cond(cond: &Value) -> Option<RangeBytes> {
    if date::is_date(cond) || numeric::is_numeric(cond) || bson_types::is_bson_value(cond) {
        return bounds("$eq", cond);
    }
    if let Some(prefix) = regex::condition_prefix(cond) {
        return Some(string_prefix_bounds(&prefix));
    }
    let Some(obj) = cond.as_object() else {
        return bounds("$eq", cond);
    };
    if obj.is_empty() {
        return None;
    }
    if obj.len() == 1 {
        let (op, value) = obj.iter().next()?;
        if op == "$eq" {
            return bounds(op, value);
        }
        if op == "$all" {
            let values = value.as_array()?;
            return if values.len() == 1 {
                bounds("$eq", &values[0])
            } else {
                None
            };
        }
        return range_bounds(std::iter::once((op.as_str(), value)));
    }
    range_bounds(obj.iter().map(|(op, value)| (op.as_str(), value)))
}

fn range_bounds<'a>(ops: impl Iterator<Item = (&'a str, &'a Value)>) -> Option<RangeBytes> {
    let mut tag: Option<u8> = None;
    let mut lower = Bound::Unbounded;
    let mut upper = Bound::Unbounded;
    for (op, value) in ops {
        let vb = encode_value(value)?;
        let t = *vb.first()?;
        if let Some(prev) = tag {
            if prev != t {
                return None; // mixed types -> full scan
            }
        } else {
            tag = Some(t);
        }
        match op {
            "$gt" => lower = max_lower(lower, Bound::Included(after_value(&vb))),
            "$gte" => lower = max_lower(lower, Bound::Included(vb)),
            "$lt" => upper = min_upper(upper, Bound::Excluded(vb)),
            "$lte" => upper = min_upper(upper, Bound::Excluded(after_value(&vb))),
            _ => return None,
        }
    }
    let tag = tag?;
    lower = max_lower(lower, Bound::Included(vec![tag]));
    upper = min_upper(upper, type_upper(tag));
    Some((lower, upper))
}

/// Type upper bound (exclusive): keys with tag `tag` are smaller.
fn type_upper(tag: u8) -> Bound<Vec<u8>> {
    if tag < u8::MAX {
        Bound::Excluded(vec![tag + 1])
    } else {
        Bound::Unbounded
    }
}

fn max_lower(a: Bound<Vec<u8>>, b: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (&a, &b) {
        (Bound::Unbounded, _) => b,
        (_, Bound::Unbounded) => a,
        _ => match bound_key(&a).cmp(bound_key(&b)) {
            std::cmp::Ordering::Greater => a,
            std::cmp::Ordering::Less => b,
            // Same key: `Included` is wider (less restrictive).
            std::cmp::Ordering::Equal => {
                if matches!(a, Bound::Included(_)) {
                    a
                } else {
                    b
                }
            }
        },
    }
}

fn min_upper(a: Bound<Vec<u8>>, b: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (&a, &b) {
        (Bound::Unbounded, _) => b,
        (_, Bound::Unbounded) => a,
        _ => match bound_key(&a).cmp(bound_key(&b)) {
            std::cmp::Ordering::Less => a,
            std::cmp::Ordering::Greater => b,
            // Same key: `Included` is wider.
            std::cmp::Ordering::Equal => {
                if matches!(a, Bound::Included(_)) {
                    a
                } else {
                    b
                }
            }
        },
    }
}

fn bound_key(b: &Bound<Vec<u8>>) -> &[u8] {
    match b {
        Bound::Included(v) | Bound::Excluded(v) => v,
        Bound::Unbounded => &[],
    }
}

/// Prepends the prefix (values of earlier index fields) to range bounds.
pub fn prefix_bounds(prefix: &[u8], (lo, hi): RangeBytes) -> RangeBytes {
    let map = |b: Bound<Vec<u8>>| match b {
        Bound::Included(mut v) => {
            let mut k = prefix.to_vec();
            k.append(&mut v);
            Bound::Included(k)
        }
        Bound::Excluded(mut v) => {
            let mut k = prefix.to_vec();
            k.append(&mut v);
            Bound::Excluded(k)
        }
        Bound::Unbounded => Bound::Included(prefix.to_vec()),
    };
    (map(lo), map(hi))
}

/// Range for a full equality prefix (all components are `=`).
pub fn exact_prefix(prefix: &[u8]) -> RangeBytes {
    (
        Bound::Included(prefix.to_vec()),
        Bound::Excluded(after_value(prefix)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn enc(v: Value) -> Vec<u8> {
        encode_value(&v).unwrap()
    }

    #[test]
    fn numbers_keep_order() {
        let vals = [-1.5, -1.0, 0.0, 1.0, 2.0, 1e9];
        let mut encs: Vec<Vec<u8>> = vals.iter().map(|v| enc(json!(v))).collect();
        let sorted = {
            let mut c = encs.clone();
            c.sort();
            c
        };
        assert_eq!(encs, sorted);
        encs.reverse();
        assert_ne!(encs, sorted);
    }

    #[test]
    fn strings_keep_order() {
        let mut a = enc(json!("abc"));
        let mut b = enc(json!("abd"));
        assert!(a < b);
        b = enc(json!("ab"));
        assert!(b < a);
        a = enc(json!("ab\u{0}"));
        assert!(a > enc(json!("ab")));
    }

    #[test]
    fn range_for_eq_contains_only_equal() {
        let vb = enc(json!(5));
        let (lo, hi) = bounds("$eq", &json!(5)).unwrap();
        let key_eq = {
            let mut k = vb.clone();
            k.extend_from_slice(b"id");
            k
        };
        let key_gt = {
            let mut k = enc(json!(6));
            k.extend_from_slice(b"id");
            k
        };
        assert!(matches!(lo, Bound::Included(ref b) if *b <= key_eq));
        assert!(matches!(hi, Bound::Excluded(ref b) if key_eq < *b && key_gt >= *b));
    }

    #[test]
    fn compound_encoding_keeps_field_order() {
        let a = encode_fields(
            &json!({"g": 1, "age": 30}),
            &["g".to_string(), "age".to_string()],
        )
        .unwrap();
        let b = encode_fields(
            &json!({"g": 1, "age": 31}),
            &["g".to_string(), "age".to_string()],
        )
        .unwrap();
        assert!(a < b);
        assert!(a.starts_with(&enc(json!(1))));
    }

    #[test]
    fn dates_encode_in_chronological_order() {
        let earlier = date::canonical(1_000);
        let later = date::canonical(2_000);
        assert!(encode_value(&earlier) < encode_value(&later));
    }

    #[test]
    fn string_prefix_bounds_cover_only_the_prefix() {
        let (lo, hi) = string_prefix_bounds("Al");
        let ala = enc(json!("Ala"));
        let al = enc(json!("Al"));
        let bob = enc(json!("Bob"));
        let lower = match lo {
            Bound::Included(v) => v,
            _ => panic!("expected included lower bound"),
        };
        let upper = match hi {
            Bound::Excluded(v) => v,
            _ => panic!("expected excluded upper bound"),
        };
        assert!(lower <= al && al < upper);
        assert!(lower <= ala && ala < upper);
        assert!(bob >= upper);
    }

    #[test]
    fn date_ranges_use_date_bounds() {
        let cond = serde_json::json!({
            "$gte": {"$date": "2026-01-01T01:00:00+01:00"},
            "$lt": {"$date": "2026-02-01T00:00:00Z"}
        });
        let (lower, upper) = bounds_from_cond(&cond).unwrap();
        assert!(matches!(lower, Bound::Included(_)));
        assert!(matches!(upper, Bound::Excluded(_)));
    }

    #[test]
    fn large_integers_keep_exact_index_order() {
        let a = enc(json!(9_007_199_254_740_992i64));
        let b = enc(json!(9_007_199_254_740_993i64));
        assert!(a < b);
        assert_ne!(a, b);
    }

    #[test]
    fn decimals_share_numeric_index_bracket() {
        let one = enc(json!(1));
        let decimal_one = enc(json!({"$numberDecimal": "1.00"}));
        let decimal_half = enc(json!({"$numberDecimal": "0.5"}));
        assert_eq!(one, decimal_one);
        assert!(decimal_half < one);
        let (lo, hi) = bounds_from_cond(&json!({
            "$gte": {"$numberDecimal": "0.5"},
            "$lt": 2
        }))
        .unwrap();
        assert!(matches!(lo, Bound::Included(_)));
        assert!(matches!(hi, Bound::Excluded(_)));
    }

    #[test]
    fn binary_and_object_id_keep_index_order() {
        let a = enc(json!({"$oid": "000000000000000000000001"}));
        let b = enc(json!({"$oid": "000000000000000000000002"}));
        assert!(a < b);

        let short = enc(json!({"$binary": {"base64": "eA==", "subType": "00"}}));
        let long = enc(json!({"$binary": {"base64": "eHg=", "subType": "00"}}));
        assert!(short < long);
        assert_eq!(
            enc(json!({"$uuid": "00112233-4455-6677-8899-aabbccddeeff"})),
            enc(json!({"$binary": {"base64": "ABEiM0RVZneImaq7zN3u/w==", "subType": "04"}}))
        );
    }

    #[test]
    fn old_index_defs_default_to_format_zero() {
        let def: IndexDef = serde_json::from_value(serde_json::json!({
            "fields": ["created_at"],
            "unique": false
        }))
        .unwrap();
        assert_eq!(def.format, 0);
        assert!(!def.current_format());
        assert!(IndexDef::single("created_at", false).current_format());
    }
}
