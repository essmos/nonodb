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
/// binary/UUID and ObjectId keys; version 5 indexes array elements as multikey
/// entries.
pub const FORMAT_VERSION: u8 = 5;

/// Number of precision levels stored for each geo point.
const GEO_KEYS_PER_POINT: usize = 4;

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
    /// Explicit MongoDB index name. Empty means the legacy logical name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// Per-field directions (`1` or `-1`). Empty means all ascending.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub directions: Vec<i8>,
    /// Whether `directions` were applied to stored index keys. Older index
    /// definitions keep ascending storage until they are reindexed.
    #[serde(default, skip_serializing_if = "is_false")]
    pub directions_encoded: bool,
    #[serde(default)]
    pub unique: bool,
    /// MongoDB sparse index: documents whose indexed field is absent are not
    /// indexed. For a compound sparse index, documents with at least one
    /// indexed field are indexed; missing fields become null keys.
    #[serde(default, skip_serializing_if = "is_false")]
    pub sparse: bool,
    /// MongoDB partial index predicate. Only matching documents are indexed.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "partialFilterExpression"
    )]
    pub partial_filter_expression: Option<Value>,
    /// Fields additionally copied into the index value (covering projections).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,
    /// Single-field GeoJSON point index. Geo entries use an internal hierarchy
    /// of lon/lat grid cells instead of scalar values.
    #[serde(default)]
    pub geo: bool,
    /// Index key encoding version. Missing in definitions written before date
    /// support and therefore defaults to 0.
    #[serde(default)]
    pub format: u8,
    /// Fields that produced more than one index key for at least one document.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub multikey_fields: Vec<String>,
    /// Whether every document currently has at least one entry in this index.
    /// This is required before a multikey index can provide sort order.
    #[serde(default, skip_serializing_if = "is_false")]
    pub multikey_all_docs: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl IndexDef {
    pub fn single(field: &str, unique: bool) -> Self {
        Self {
            fields: vec![field.to_string()],
            field: String::new(),
            name: String::new(),
            directions: Vec::new(),
            directions_encoded: false,
            unique,
            sparse: false,
            partial_filter_expression: None,
            include: Vec::new(),
            geo: false,
            format: FORMAT_VERSION,
            multikey_fields: Vec::new(),
            multikey_all_docs: false,
        }
    }

    pub fn compound(fields: &[String], unique: bool, include: &[String]) -> Self {
        Self {
            fields: fields.to_vec(),
            field: String::new(),
            name: String::new(),
            directions: Vec::new(),
            directions_encoded: false,
            unique,
            sparse: false,
            partial_filter_expression: None,
            include: include.to_vec(),
            geo: false,
            format: FORMAT_VERSION,
            multikey_fields: Vec::new(),
            multikey_all_docs: false,
        }
    }

    /// Single-field GeoJSON point index.
    pub fn geo(field: &str) -> Self {
        let mut def = Self::single(field, false);
        def.geo = true;
        def
    }

    /// Whether the index was built with the current key encoding.
    pub fn current_format(&self) -> bool {
        self.format >= FORMAT_VERSION
    }

    pub fn is_multikey(&self) -> bool {
        !self.multikey_fields.is_empty()
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

    /// Stored MongoDB name, or the legacy logical name (`age` or `grp+age`).
    pub fn name(&self) -> String {
        if !self.name.is_empty() {
            return self.name.clone();
        }
        self.effective_fields().join("+")
    }

    /// Per-field directions, defaulting to ascending for legacy definitions.
    pub fn directions(&self) -> Vec<i8> {
        let len = self.effective_fields().len();
        if self.directions.len() == len {
            self.directions.clone()
        } else {
            vec![1; len]
        }
    }

    /// Directions actually applied to stored key bytes.
    ///
    /// Older definitions stored mixed-direction metadata but kept every key
    /// ascending. They remain sortable as all-ascending indexes until rebuilt.
    pub fn storage_directions(&self) -> Vec<i8> {
        let len = self.effective_fields().len();
        if self.directions_encoded && self.directions.len() == len {
            self.directions.clone()
        } else {
            vec![1; len]
        }
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
    if bson_types::is_min_key(v) {
        out.push(0x00);
        return Some(out);
    }
    if bson_types::is_max_key(v) {
        out.push(0x09);
        return Some(out);
    }
    if bson_types::is_regex(v) {
        // Stored regex values are intentionally not indexed; queries that
        // target them fall back to a collection scan.
        return None;
    }
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
            } else if let Some(key) = bson_types::timestamp_key(v) {
                out.push(0x08);
                out.extend_from_slice(&key);
            } else {
                let ms = date::parse_value(v)?;
                out.push(0x05);
                out.extend_from_slice(&date::encode_ordered(ms));
            }
        }
    }
    Some(out)
}

/// Encodes a value in an index field's storage direction.
///
/// Descending fields complement every byte of the ascending encoding. This
/// reverses lexicographic order while preserving the prefix boundaries used by
/// ranges and unique-key checks.
pub fn encode_value_direction(v: &Value, direction: i8) -> Option<Vec<u8>> {
    let mut out = encode_value(v)?;
    if direction < 0 {
        complement_bytes(&mut out);
    }
    Some(out)
}

fn complement_bytes(bytes: &mut [u8]) {
    for byte in bytes {
        *byte = !*byte;
    }
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
    string_prefix_bounds_direction(prefix, 1)
}

fn string_prefix_bounds_direction(prefix: &str, direction: i8) -> RangeBytes {
    let mut lower = vec![0x03];
    push_escaped_string(&mut lower, prefix);
    if direction < 0 {
        complement_bytes(&mut lower);
    }
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

/// All index keys produced by a document, plus the fields that needed array
/// expansion. Empty `keys` means the document is not represented in the index.
#[derive(Debug, Clone)]
pub struct IndexKeys {
    pub keys: Vec<Vec<u8>>,
    pub multikey_fields: Vec<String>,
}

fn array_index(part: &str) -> Option<usize> {
    if part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    part.parse().ok()
}

/// Collects all values at a dotted path, expanding arrays encountered before
/// the final path component. This matches MongoDB's implicit traversal for
/// fields such as `items.price`.
fn collect_path_values<'a>(
    value: &'a Value,
    parts: &[&str],
    out: &mut Vec<&'a Value>,
    saw_array: &mut bool,
) {
    let Some((part, rest)) = parts.split_first() else {
        out.push(value);
        return;
    };
    match value {
        Value::Object(obj) => {
            if let Some(child) = obj.get(*part) {
                collect_path_values(child, rest, out, saw_array);
            }
        }
        Value::Array(items) => {
            *saw_array = true;
            if let Some(index) = array_index(part) {
                if let Some(child) = items.get(index) {
                    collect_path_values(child, rest, out, saw_array);
                }
            } else {
                for item in items {
                    if let Value::Object(obj) = item {
                        if let Some(child) = obj.get(*part) {
                            collect_path_values(child, rest, out, saw_array);
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

fn field_values<'a>(doc: &'a Value, field: &str) -> (Vec<&'a Value>, bool) {
    let parts: Vec<&str> = field.split('.').collect();
    let mut out = Vec::new();
    let mut saw_array = false;
    collect_path_values(doc, &parts, &mut out, &mut saw_array);
    (out, saw_array)
}

fn flatten_array<'a>(value: &'a Value, out: &mut Vec<&'a Value>) {
    match value {
        Value::Array(items) => {
            for item in items {
                flatten_array(item, out);
            }
        }
        _ => out.push(value),
    }
}

/// Encodes every multikey entry for a document.
///
/// A scalar field produces one key. An array (or a path that expands through an
/// array) produces one key per flattened scalar element. Compound indexes may
/// contain at most one array field, matching MongoDB's parallel-array rule.
pub fn encode_index_keys(def: &IndexDef, doc: &Value) -> Result<IndexKeys, crate::Error> {
    let fields = def.effective_fields();
    if let Some(filter) = &def.partial_filter_expression {
        if !crate::query::matches(doc, filter) {
            return Ok(IndexKeys {
                keys: Vec::new(),
                multikey_fields: Vec::new(),
            });
        }
    }
    if def.geo {
        if fields.len() != 1 {
            return Err(crate::Error::Query(
                "geo indexes require exactly one field".into(),
            ));
        }
        let field = &fields[0];
        let (values, saw_array) = field_values(doc, field);
        if values.is_empty() && def.sparse {
            return Ok(IndexKeys {
                keys: Vec::new(),
                multikey_fields: Vec::new(),
            });
        }
        let mut keys = Vec::new();
        for value in values {
            if let Some(point) = crate::geo::parse_point(value) {
                keys.extend(crate::geo::encode_point_keys(point));
                continue;
            }
            if let Some(items) = value.as_array() {
                for item in items {
                    if let Some(point) = crate::geo::parse_point(item) {
                        keys.extend(crate::geo::encode_point_keys(point));
                    }
                }
            }
        }
        keys.sort();
        keys.dedup();
        let multikey_fields = if saw_array || keys.len() > GEO_KEYS_PER_POINT {
            vec![field.clone()]
        } else {
            Vec::new()
        };
        return Ok(IndexKeys {
            keys,
            multikey_fields,
        });
    }
    let mut per_field: Vec<Vec<Vec<u8>>> = Vec::with_capacity(fields.len());
    let mut multikey_fields = Vec::new();
    let directions = def.storage_directions();
    let mut present_fields = 0usize;
    for (field_index, field) in fields.iter().enumerate() {
        let direction = directions.get(field_index).copied().unwrap_or(1);
        let (values, saw_array) = field_values(doc, field);
        if values.is_empty() {
            if def.sparse && fields.len() > 1 {
                // MongoDB compound sparse entries store null for absent
                // fields and omit only documents where every key is absent.
                let direction = directions.get(field_index).copied().unwrap_or(1);
                if let Some(key) = encode_value_direction(&Value::Null, direction) {
                    per_field.push(vec![key]);
                }
                continue;
            }
            if !def.sparse {
                // Plain indexes treat an absent field as null, matching
                // MongoDB uniqueness and index ordering. Geo indexes are
                // sparse by construction and handled above.
                let direction = directions.get(field_index).copied().unwrap_or(1);
                let key = encode_value_direction(&Value::Null, direction).ok_or_else(|| {
                    crate::Error::Query("cannot encode a missing index field".into())
                })?;
                per_field.push(vec![key]);
                continue;
            }
        }
        if !values.is_empty() {
            present_fields = present_fields.saturating_add(1);
        }
        let mut encoded = Vec::new();
        let mut field_multikey = saw_array || values.len() > 1;
        for value in &values {
            let mut flattened = Vec::new();
            flatten_array(value, &mut flattened);
            if value.is_array() || flattened.len() > 1 {
                field_multikey = true;
            }
            for value in flattened {
                if let Some(key) = encode_value_direction(value, direction) {
                    encoded.push(key);
                }
            }
        }
        encoded.sort();
        encoded.dedup();
        if field_multikey {
            multikey_fields.push(field.clone());
        }
        if encoded.is_empty() {
            return Ok(IndexKeys {
                keys: Vec::new(),
                multikey_fields,
            });
        }
        per_field.push(encoded);
    }
    if def.sparse && present_fields == 0 {
        return Ok(IndexKeys {
            keys: Vec::new(),
            multikey_fields,
        });
    }
    if multikey_fields.len() > 1 {
        return Err(crate::Error::Query(
            "cannot index parallel arrays in one compound index".into(),
        ));
    }

    let mut keys: Vec<Vec<u8>> = vec![Vec::new()];
    for encodings in per_field {
        let mut next = Vec::with_capacity(keys.len().saturating_mul(encodings.len()));
        for prefix in &keys {
            for encoding in &encodings {
                let mut key = Vec::with_capacity(prefix.len() + encoding.len());
                key.extend_from_slice(prefix);
                key.extend_from_slice(encoding);
                next.push(key);
            }
        }
        keys = next;
    }
    keys.sort();
    keys.dedup();
    Ok(IndexKeys {
        keys,
        multikey_fields,
    })
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

pub fn after_value(vb: &[u8]) -> Vec<u8> {
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
    bounds_direction(op, value, 1)
}

/// Index key range for a comparison operator in a storage direction.
pub fn bounds_direction(op: &str, value: &Value, direction: i8) -> Option<RangeBytes> {
    let vb = encode_value_direction(value, direction)?;
    Some(match (op, direction < 0) {
        ("$eq", _) => (
            Bound::Included(vb.clone()),
            Bound::Excluded(after_value(&vb)),
        ),
        ("$gte", false) => (Bound::Included(vb), Bound::Unbounded),
        ("$gt", false) => (Bound::Included(after_value(&vb)), Bound::Unbounded),
        ("$lte", false) => (Bound::Unbounded, Bound::Excluded(after_value(&vb))),
        ("$lt", false) => (Bound::Unbounded, Bound::Excluded(vb)),
        ("$gte", true) => (Bound::Unbounded, Bound::Excluded(after_value(&vb))),
        ("$gt", true) => (Bound::Unbounded, Bound::Excluded(vb)),
        ("$lte", true) => (Bound::Included(vb), Bound::Unbounded),
        ("$lt", true) => (Bound::Included(after_value(&vb)), Bound::Unbounded),
        _ => return None,
    })
}

/// Index key tag for a BSON type name or numeric code.
///
/// Several BSON types share one numeric index bracket. The residual matcher
/// still verifies the exact type, so using the wider bracket is safe.
fn type_index_tag(spec: &Value) -> Option<u8> {
    if let Some(items) = spec.as_array() {
        if items.is_empty() {
            return None;
        }
        let first = type_index_tag(&items[0])?;
        return items
            .iter()
            .all(|item| type_index_tag(item) == Some(first))
            .then_some(first);
    }
    if let Some(name) = spec.as_str() {
        return match name.to_ascii_lowercase().as_str() {
            "double" | "int" | "long" | "decimal" | "number" => Some(0x02),
            "string" => Some(0x03),
            "bool" => Some(0x04),
            "date" => Some(0x05),
            "bindata" => Some(0x06),
            "objectid" => Some(0x07),
            "timestamp" => Some(0x08),
            "maxkey" => Some(0x09),
            "minkey" => Some(0x00),
            "null" => Some(0x01),
            _ => None,
        };
    }
    match numeric::integer_value(spec)? {
        1 | 16 | 18 | 19 => Some(0x02),
        2 => Some(0x03),
        8 => Some(0x04),
        9 => Some(0x05),
        5 => Some(0x06),
        7 => Some(0x07),
        10 => Some(0x01),
        17 => Some(0x08),
        127 => Some(0x09),
        -1 => Some(0x00),
        _ => None,
    }
}

/// Range covering all encoded values of a BSON type. Missing fields are not
/// indexed, so this is safe for `$type` (which never matches a missing field).
fn type_bounds_direction(spec: &Value, direction: i8) -> Option<RangeBytes> {
    let tag = type_index_tag(spec)?;
    let tag = if direction < 0 { !tag } else { tag };
    Some((
        Bound::Included(vec![tag]),
        Bound::Excluded(vec![tag.checked_add(1)?]),
    ))
}

/// Range for a compound condition, e.g. `{"$gte": 18, "$lt": 30}`.
///
/// Supports one operator (`$eq`, `$gt`, `$gte`, `$lt`, `$lte`) or several
/// range operators on the same field. The range is limited to the same
/// type as the condition value; the matcher does not compare different types, so
/// otherwise `$gt: 2` would also include strings/bools from the index.
pub fn bounds_from_cond(cond: &Value) -> Option<RangeBytes> {
    bounds_from_cond_direction(cond, 1)
}

/// Range for a compound condition in a storage direction.
pub fn bounds_from_cond_direction(cond: &Value, direction: i8) -> Option<RangeBytes> {
    if let Some(spec) = cond
        .as_object()
        .and_then(|obj| (obj.len() == 1).then(|| obj.get("$type")).flatten())
    {
        return type_bounds_direction(spec, direction);
    }
    if date::is_date(cond) || numeric::is_numeric(cond) || bson_types::is_bson_value(cond) {
        return bounds_direction("$eq", cond, direction);
    }
    if let Some(prefix) = regex::condition_prefix(cond) {
        return Some(string_prefix_bounds_direction(&prefix, direction));
    }
    let Some(obj) = cond.as_object() else {
        return bounds_direction("$eq", cond, direction);
    };
    if obj.is_empty() {
        return None;
    }
    if obj.len() == 1 {
        let (op, value) = obj.iter().next()?;
        if op == "$eq" {
            return bounds_direction(op, value, direction);
        }
        if op == "$all" {
            let values = value.as_array()?;
            return if values.len() == 1 {
                bounds_direction("$eq", &values[0], direction)
            } else {
                None
            };
        }
        return range_bounds_direction(std::iter::once((op.as_str(), value)), direction);
    }
    range_bounds_direction(
        obj.iter().map(|(op, value)| (op.as_str(), value)),
        direction,
    )
}

fn range_bounds_direction<'a>(
    ops: impl Iterator<Item = (&'a str, &'a Value)>,
    direction: i8,
) -> Option<RangeBytes> {
    let mut tag: Option<u8> = None;
    let mut lower = Bound::Unbounded;
    let mut upper = Bound::Unbounded;
    for (op, value) in ops {
        let vb = encode_value_direction(value, direction)?;
        let t = *vb.first()?;
        if let Some(prev) = tag {
            if prev != t {
                return None; // mixed types -> full scan
            }
        } else {
            tag = Some(t);
        }
        match (op, direction < 0) {
            ("$gt", false) => lower = max_lower(lower, Bound::Included(after_value(&vb))),
            ("$gte", false) => lower = max_lower(lower, Bound::Included(vb)),
            ("$lt", false) => upper = min_upper(upper, Bound::Excluded(vb)),
            ("$lte", false) => upper = min_upper(upper, Bound::Excluded(after_value(&vb))),
            ("$gt", true) => upper = min_upper(upper, Bound::Excluded(vb)),
            ("$gte", true) => upper = min_upper(upper, Bound::Excluded(after_value(&vb))),
            ("$lt", true) => lower = max_lower(lower, Bound::Included(after_value(&vb))),
            ("$lte", true) => lower = max_lower(lower, Bound::Included(vb)),
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
    fn timestamps_and_key_extremes_encode_in_bson_order() {
        let min = bson_types::min_key_value();
        let max = bson_types::max_key_value();
        let earlier = bson_types::timestamp_value(bson_types::Timestamp {
            time: 1_000,
            increment: 1,
        });
        let later = bson_types::timestamp_value(bson_types::Timestamp {
            time: 1_000,
            increment: 2,
        });
        assert_eq!(encode_value(&min).unwrap(), vec![0x00]);
        assert_eq!(encode_value(&max).unwrap(), vec![0x09]);
        assert!(encode_value(&earlier) < encode_value(&later));
        assert!(encode_value(&min) < encode_value(&json!(null)));
        assert!(encode_value(&max) > encode_value(&json!("z")));
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
    fn type_conditions_use_type_brackets() {
        let (lo, hi) = bounds_from_cond(&json!({"$type": "number"})).unwrap();
        assert_eq!(bound_key(&lo), &[0x02]);
        assert_eq!(bound_key(&hi), &[0x03]);

        let (lo, hi) = bounds_from_cond(&json!({"$type": "objectId"})).unwrap();
        assert_eq!(bound_key(&lo), &[0x07]);
        assert_eq!(bound_key(&hi), &[0x08]);

        assert!(bounds_from_cond(&json!({"$type": ["int", "long", "decimal"]})).is_some());
        assert!(bounds_from_cond(&json!({"$type": ["int", "string"]})).is_none());
        assert!(bounds_from_cond(&json!({"$type": "array"})).is_none());
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
    fn descending_encoding_reverses_order_and_keeps_ranges_exact() {
        let low = encode_value_direction(&json!(4), -1).unwrap();
        let mid = encode_value_direction(&json!(6), -1).unwrap();
        let high = encode_value_direction(&json!(8), -1).unwrap();
        assert!(low > mid && mid > high);

        let (lo, hi) = bounds_direction("$eq", &json!(6), -1).unwrap();
        let mut key = encode_value_direction(&json!(6), -1).unwrap();
        key.extend_from_slice(b"doc-id");
        assert!(matches!(&lo, Bound::Included(v) if *v <= key));
        assert!(matches!(&hi, Bound::Excluded(v) if key < *v));

        let (lo, hi) = bounds_from_cond_direction(&json!({"$gte": 5, "$lt": 8}), -1).unwrap();
        let lower = bound_key(&lo);
        let upper = bound_key(&hi);
        for value in [5, 6, 7] {
            let key = encode_value_direction(&json!(value), -1).unwrap();
            assert!(lower <= key.as_slice(), "value {value} below lower bound");
            assert!(key.as_slice() < upper, "value {value} above upper bound");
        }
        let below = encode_value_direction(&json!(4), -1).unwrap();
        let at_upper = encode_value_direction(&json!(8), -1).unwrap();
        assert!(below.as_slice() > upper);
        assert!(at_upper.as_slice() < lower);
    }

    #[test]
    fn plain_indexes_encode_missing_as_null_and_sparse_omit() {
        let plain = IndexDef::single("a", false);
        let keys = encode_index_keys(&plain, &json!({"_id": "missing"})).unwrap();
        assert_eq!(keys.keys, vec![encode_value(&Value::Null).unwrap()]);

        let mut sparse = IndexDef::single("a", false);
        sparse.sparse = true;
        let keys = encode_index_keys(&sparse, &json!({"_id": "missing"})).unwrap();
        assert!(keys.keys.is_empty());

        let compound = IndexDef::compound(&["a".into(), "b".into()], false, &[]);
        let keys = encode_index_keys(&compound, &json!({"_id": "missing"})).unwrap();
        let mut expected = encode_value(&Value::Null).unwrap();
        expected.extend_from_slice(&encode_value(&Value::Null).unwrap());
        assert_eq!(keys.keys, vec![expected]);
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
