//! MongoDB-style filter matcher (subset).
//!
//! Supported operators: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`,
//! `$nin`, `$all`, `$size`, `$elemMatch`, `$exists`, `$regex`/`$options`,
//! `$type`, `$mod`, `$bitsAllClear`/`$bitsAllSet`/`$bitsAnyClear`/
//! `$bitsAnySet`, `$expr`, `$not`, `$and`, `$or`, `$nor`.
//!
//! Numeric comparison is exact (JSON integers are not rounded through `f64`)
//! and Decimal128 values participate in the same numeric type bracket.

use serde_json::Value;
use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::bson_types;
use crate::date;
use crate::geo;
use crate::numeric;
use crate::query::path;
use crate::regex;

/// Does the document match the filter? A non-object filter matches everything.
pub fn matches(doc: &Value, filter: &Value) -> bool {
    matches_with_vars(doc, filter, &BTreeMap::new())
}

/// Matches a filter with aggregation variables available to `$expr`.
pub(crate) fn matches_with_vars(
    doc: &Value,
    filter: &Value,
    vars: &BTreeMap<String, Value>,
) -> bool {
    let Some(obj) = filter.as_object() else {
        return true;
    };
    obj.iter().all(|(key, cond)| match key.as_str() {
        "$and" => cond
            .as_array()
            .is_some_and(|arr| arr.iter().all(|f| matches_with_vars(doc, f, vars))),
        "$or" => cond
            .as_array()
            .is_some_and(|arr| arr.iter().any(|f| matches_with_vars(doc, f, vars))),
        "$nor" => cond
            .as_array()
            .is_some_and(|arr| !arr.iter().any(|f| matches_with_vars(doc, f, vars))),
        // `$expr` evaluates an aggregation expression against the document.
        "$expr" => crate::aggregate::eval_expr_vars(doc, cond, vars)
            .ok()
            .is_some_and(|value| crate::aggregate::truthy(&value)),
        "$jsonSchema" => crate::query::schema::matches(doc, cond),
        // `$not` at filter level: negates the whole subfilter.
        "$not" => !matches_with_vars(doc, cond, vars),
        _ => {
            if key.contains('.') {
                let values = path::get_values(doc, key);
                if values.len() > 1 {
                    let array = Value::Array(values.into_iter().cloned().collect());
                    match_field_vars(Some(&array), cond, vars)
                } else {
                    match_field_vars(values.first().copied(), cond, vars)
                }
            } else {
                match_field_vars(doc.get(key), cond, vars)
            }
        }
    })
}

pub(crate) fn match_field(actual: Option<&Value>, cond: &Value) -> bool {
    match_field_vars(actual, cond, &BTreeMap::new())
}

fn match_field_vars(actual: Option<&Value>, cond: &Value, vars: &BTreeMap<String, Value>) -> bool {
    // Extended JSON dates are objects, not operators.
    if date::is_date(cond) || numeric::is_numeric(cond) || bson_types::is_bson_value(cond) {
        return eq_opt(actual, cond);
    }
    if let Some(ops) = cond.as_object() {
        if !ops.is_empty() && ops.keys().all(|k| k.starts_with('$')) {
            if let Some(pattern) = ops.get("$regex") {
                if !regex::is_match(actual, pattern, ops.get("$options")) {
                    return false;
                }
                return ops
                    .iter()
                    .filter(|(op, _)| op.as_str() != "$regex" && op.as_str() != "$options")
                    .all(|(op, arg)| match_simple_op(actual, op, arg, vars));
            }
            return ops
                .iter()
                .all(|(op, arg)| match_simple_op(actual, op, arg, vars));
        }
    }
    // Plain equality: { "name": "Ala" }.
    eq_opt(actual, cond)
}

fn match_simple_op(
    actual: Option<&Value>,
    op: &str,
    arg: &Value,
    vars: &BTreeMap<String, Value>,
) -> bool {
    match op {
        "$eq" => eq_opt(actual, arg),
        "$ne" => !eq_opt(actual, arg),
        "$gt" => cmp_any(actual, arg, |ord| ord == Ordering::Greater),
        "$gte" => cmp_any(actual, arg, |ord| {
            matches!(ord, Ordering::Greater | Ordering::Equal)
        }),
        "$lt" => cmp_any(actual, arg, |ord| ord == Ordering::Less),
        "$lte" => cmp_any(actual, arg, |ord| {
            matches!(ord, Ordering::Less | Ordering::Equal)
        }),
        "$in" => arg
            .as_array()
            .is_some_and(|arr| arr.iter().any(|x| operand_opt(actual, x))),
        "$nin" => arg
            .as_array()
            .is_some_and(|arr| !arr.iter().any(|x| operand_opt(actual, x))),
        "$exists" => actual.is_some() == arg.as_bool().unwrap_or(false),
        "$mod" => mod_opt(actual, arg),
        "$bitsAllClear" => bits_opt(actual, arg, BitTest::AllClear),
        "$bitsAllSet" => bits_opt(actual, arg, BitTest::AllSet),
        "$bitsAnyClear" => bits_opt(actual, arg, BitTest::AnyClear),
        "$bitsAnySet" => bits_opt(actual, arg, BitTest::AnySet),
        "$all" => all_opt(actual, arg),
        "$size" => size_opt(actual, arg),
        "$elemMatch" => elem_match_opt(actual, arg, vars),
        "$type" => actual.is_some_and(|v| bson_types::type_matches(v, arg)),
        "$near" | "$nearSphere" => geo::match_near(actual, arg),
        "$geoWithin" => geo::match_within(actual, arg),
        "$geoIntersects" => geo::match_intersects(actual, arg),
        "$not" => !match_field_vars(actual, arg, vars),
        _ => false,
    }
}

fn all_opt(actual: Option<&Value>, arg: &Value) -> bool {
    let Some(expected) = arg.as_array() else {
        return false;
    };
    // MongoDB treats `$all: []` as matching nothing.
    !expected.is_empty() && expected.iter().all(|value| operand_opt(actual, value))
}

/// Matches an `$in`/`$all` operand, including regex-shaped operands.
fn operand_opt(actual: Option<&Value>, expected: &Value) -> bool {
    if let Some(ops) = expected.as_object() {
        if let Some(pattern) = ops.get("$regex") {
            return regex::is_match(actual, pattern, ops.get("$options"));
        }
    }
    eq_opt(actual, expected)
}

fn mod_opt(actual: Option<&Value>, arg: &Value) -> bool {
    let Some(values) = arg.as_array() else {
        return false;
    };
    if values.len() != 2 {
        return false;
    }
    let Some(divisor) = numeric::as_bigdecimal(&values[0]) else {
        return false;
    };
    if divisor == 0 {
        return false;
    }
    let Some(remainder) = numeric::as_bigdecimal(&values[1]) else {
        return false;
    };
    let matches_value = |value: &Value| {
        numeric::as_bigdecimal(value).is_some_and(|number| number % &divisor == remainder)
    };
    match actual {
        Some(Value::Array(items)) => items.iter().any(matches_value),
        Some(value) => matches_value(value),
        None => false,
    }
}

#[derive(Clone, Copy)]
enum BitTest {
    AllClear,
    AllSet,
    AnyClear,
    AnySet,
}

fn bits_opt(actual: Option<&Value>, arg: &Value, test: BitTest) -> bool {
    let Some(mask) = bit_mask(arg) else {
        return false;
    };
    let matches_value = |value: &Value| {
        let Some(number) = bit_value(value) else {
            return false;
        };
        let set = number & mask;
        match test {
            BitTest::AllClear => set == 0,
            BitTest::AllSet => set == mask,
            BitTest::AnyClear => set != mask,
            BitTest::AnySet => set != 0,
        }
    };
    match actual {
        Some(Value::Array(items)) => items.iter().any(matches_value),
        Some(value) => matches_value(value),
        None => false,
    }
}

fn bit_mask(arg: &Value) -> Option<u64> {
    if let Some(positions) = arg.as_array() {
        let mut mask = 0u64;
        for position in positions {
            mask |= 1u64 << bit_position(position)?;
        }
        return Some(mask);
    }
    bit_value(arg)
}

fn bit_position(value: &Value) -> Option<u64> {
    let number = numeric::as_bigdecimal(value)?;
    if number != number.with_scale(0) {
        return None;
    }
    let position = bigdecimal::num_traits::ToPrimitive::to_u64(&number)?;
    (position < 64).then_some(position)
}

fn bit_value(value: &Value) -> Option<u64> {
    if let Some(number) = value.as_u64() {
        return Some(number);
    }
    if let Some(number) = value.as_i64() {
        return Some(number as u64);
    }
    let number = numeric::as_bigdecimal(value)?;
    if number != number.with_scale(0) {
        return None;
    }
    if let Some(number) = bigdecimal::num_traits::ToPrimitive::to_i64(&number) {
        return Some(number as u64);
    }
    bigdecimal::num_traits::ToPrimitive::to_u64(&number)
}

fn size_opt(actual: Option<&Value>, arg: &Value) -> bool {
    let Some(size) = arg.as_u64().and_then(|n| usize::try_from(n).ok()) else {
        return false;
    };
    matches!(actual, Some(Value::Array(items)) if items.len() == size)
}

fn elem_match_opt(actual: Option<&Value>, arg: &Value, vars: &BTreeMap<String, Value>) -> bool {
    let Some(Value::Array(items)) = actual else {
        return false;
    };
    if arg.as_object().is_some_and(|o| o.is_empty()) {
        return !items.is_empty();
    }
    items
        .iter()
        .any(|item| elem_item_matches_vars(item, arg, vars))
}

pub(crate) fn elem_item_matches(item: &Value, arg: &Value) -> bool {
    elem_item_matches_vars(item, arg, &BTreeMap::new())
}

fn elem_item_matches_vars(item: &Value, arg: &Value, vars: &BTreeMap<String, Value>) -> bool {
    let Some(query) = arg.as_object() else {
        return false;
    };
    if query.is_empty() {
        return true;
    }
    if elem_match_uses_filter(query) {
        matches_with_vars(item, arg, vars)
    } else {
        match_field_vars(Some(item), arg, vars)
    }
}

fn elem_match_uses_filter(query: &serde_json::Map<String, Value>) -> bool {
    if query.is_empty() {
        return false;
    }
    if query.keys().any(|key| !key.starts_with('$')) {
        return true;
    }
    if query.keys().any(|key| {
        matches!(
            key.as_str(),
            "$and" | "$or" | "$nor" | "$expr" | "$jsonSchema"
        )
    }) {
        return true;
    }
    if let Some(Value::Object(not)) = query.get("$not") {
        return not
            .keys()
            .any(|key| !key.starts_with('$') || matches!(key.as_str(), "$and" | "$or" | "$nor"));
    }
    false
}

/// Equality with MongoDB semantics:
/// - missing field == `null`,
/// - `{tags: "x"}` matches when `tags` is an array containing `x`.
///
/// Negation follows from this: `$ne: 1` matches a missing field, while
/// `$ne: null` does not match a missing/null field. `$nin` behaves the same
/// way because it negates `$in`.
pub(crate) fn eq_opt(actual: Option<&Value>, expected: &Value) -> bool {
    match actual {
        None => expected.is_null(),
        Some(Value::Array(items)) if !expected.is_array() => {
            items.iter().any(|item| num_eq(item, expected))
        }
        Some(a) => num_eq(a, expected),
    }
}

/// Value equality with unified numeric comparison.
pub(crate) fn num_eq(a: &Value, b: &Value) -> bool {
    if let (Some(x), Some(y)) = (date::parse_value(a), date::parse_value(b)) {
        return x == y;
    }
    if date::parse_value(a).is_some() || date::parse_value(b).is_some() {
        return false;
    }
    if let Some(ord) = numeric::cmp_values(a, b) {
        return ord == Ordering::Equal;
    }
    if numeric::is_numeric(a) || numeric::is_numeric(b) {
        return false;
    }
    if let Some(eq) = bson_types::eq_values(a, b) {
        return eq;
    }
    a == b
}

fn cmp_any(actual: Option<&Value>, expected: &Value, accept: impl Fn(Ordering) -> bool) -> bool {
    match actual {
        Some(Value::Array(items)) => items
            .iter()
            .any(|item| cmp(Some(item), expected).is_some_and(&accept)),
        _ => cmp(actual, expected).is_some_and(accept),
    }
}

fn cmp(actual: Option<&Value>, expected: &Value) -> Option<Ordering> {
    let a = actual?;
    if let Some(ord) = numeric::cmp_values(a, expected) {
        return Some(ord);
    }
    if let Some(ord) = bson_types::cmp_values(a, expected) {
        return Some(ord);
    }
    match (a, expected) {
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        _ => date::cmp(a, expected),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn plain_equality() {
        let doc = json!({"name": "Ala", "age": 30});
        assert!(matches(&doc, &json!({"name": "Ala"})));
        assert!(!matches(&doc, &json!({"name": "Bob"})));
    }

    #[test]
    fn numbers_are_unified() {
        let doc = json!({"age": 30});
        assert!(matches(&doc, &json!({"age": 30.0})));
        assert!(matches(&doc, &json!({"age": {"$eq": 30.0}})));
        assert!(matches(&doc, &json!({"age": {"$gte": 30.0}})));
    }

    #[test]
    fn range_and_logic() {
        let doc = json!({"age": 30, "role": "admin"});
        assert!(matches(
            &doc,
            &json!({"$and": [{"age": {"$gt": 18}}, {"role": "admin"}]})
        ));
        assert!(matches(
            &doc,
            &json!({"$or": [{"age": {"$lt": 18}}, {"role": "admin"}]})
        ));
        assert!(!matches(&doc, &json!({"$nor": [{"role": "admin"}]})));
    }

    #[test]
    fn exists_and_null() {
        let doc = json!({"a": null});
        assert!(matches(&doc, &json!({"a": {"$exists": true}})));
        assert!(matches(&doc, &json!({"a": null})));
        assert!(matches(&doc, &json!({"missing": null})));
        assert!(!matches(&doc, &json!({"missing": {"$exists": true}})));
        assert!(matches(&doc, &json!({"missing": {"$ne": 1}})));
        assert!(!matches(&doc, &json!({"missing": {"$ne": null}})));
        assert!(!matches(&doc, &json!({"a": {"$ne": null}})));
        assert!(matches(&doc, &json!({"missing": {"$nin": [1, 2]}})));
        assert!(!matches(&doc, &json!({"missing": {"$nin": [null, 2]}})));
    }

    #[test]
    fn nested_paths() {
        let doc = json!({"a": {"b": {"c": 7}}, "x": 1});
        assert!(matches(&doc, &json!({"a.b.c": 7})));
        assert!(matches(&doc, &json!({"a.b": {"c": 7}})));
        assert!(!matches(&doc, &json!({"a.b.c": 8})));
        assert!(matches(&doc, &json!({"a.b.c": {"$gte": 7}})));
        // A missing field still means `null`; `$exists` distinguishes these cases.
        assert!(matches(&doc, &json!({"a.b.missing": null})));
        assert!(!matches(&doc, &json!({"a.b.missing": {"$exists": true}})));
    }

    #[test]
    fn type_operator_and_extended_values() {
        let doc = json!({
            "s": "x",
            "n": {"$numberLong": "9007199254740993"},
            "b": {"$uuid": "00112233-4455-6677-8899-aabbccddeeff"},
            "o": {"$oid": "000000000000000000000001"}
        });
        assert!(matches(&doc, &json!({"s": {"$type": "string"}})));
        assert!(matches(&doc, &json!({"n": {"$type": "long"}})));
        assert!(matches(&doc, &json!({"n": {"$type": "number"}})));
        assert!(matches(&doc, &json!({"b": {"$type": "binData"}})));
        assert!(matches(
            &doc,
            &json!({"o": {"$type": ["objectId", "string"]}})
        ));
        assert!(matches(
            &doc,
            &json!({"b": {"$uuid": "00112233-4455-6677-8899-aabbccddeeff"}})
        ));
    }

    #[test]
    fn array_contains() {
        let doc = json!({"tags": ["a", "b"]});
        assert!(matches(&doc, &json!({"tags": "a"})));
        assert!(!matches(&doc, &json!({"tags": "c"})));
        assert!(matches(&doc, &json!({"tags": {"$in": ["c", "b"]}})));
    }
    #[test]
    fn expr_mod_bitwise_and_regex_operands() {
        let doc = json!({
            "a": 5,
            "b": 3,
            "name": "Ala",
            "tags": ["alpha", "beta"],
            "bits": 0b1010,
            "nums": [3, 7, 11]
        });
        assert!(matches(&doc, &json!({"$expr": {"$gt": ["$a", "$b"]}})));
        assert!(matches(
            &doc,
            &json!({"$expr": {"$eq": [{"$add": ["$b", 2]}, "$a"]}})
        ));
        assert!(!matches(&doc, &json!({"$expr": {"$lt": ["$a", "$b"]}})));
        assert!(matches(&doc, &json!({"$expr": {"$in": [7, "$nums"]}})));
        assert!(matches(
            &doc,
            &json!({"$expr": {"$gt": [{"$arrayElemAt": ["$nums", -1]}, 9]}})
        ));
        assert!(matches(
            &doc,
            &json!({"$expr": {"$regexMatch": {"input": "$name", "regex": "^Al"}}})
        ));

        assert!(matches(&doc, &json!({"a": {"$mod": [2, 1]}})));
        assert!(!matches(&doc, &json!({"a": {"$mod": [2, 0]}})));
        assert!(matches(&doc, &json!({"nums": {"$mod": [4, 3]}})));

        assert!(matches(&doc, &json!({"bits": {"$bitsAllSet": [1, 3]}})));
        assert!(matches(&doc, &json!({"bits": {"$bitsAnySet": [1]}})));
        assert!(matches(&doc, &json!({"bits": {"$bitsAllClear": [0, 2]}})));
        assert!(matches(&doc, &json!({"bits": {"$bitsAnyClear": 1}})));

        assert!(matches(
            &doc,
            &json!({"name": {"$in": [{"$regex": "^Al"}]}})
        ));
        assert!(matches(
            &doc,
            &json!({"tags": {"$all": [{"$regex": "^al"}]}})
        ));
        assert!(!matches(
            &doc,
            &json!({"name": {"$nin": [{"$regex": "^Al"}]}})
        ));
    }

    #[test]
    fn json_schema_keywords_and_combinators() {
        let doc = json!({
            "name": "Ala",
            "age": 30,
            "tags": ["a", "b"],
            "meta": {"active": true, "score": 4.5}
        });
        let schema = json!({
            "bsonType": "object",
            "required": ["name", "age", "tags"],
            "properties": {
                "name": {"bsonType": "string", "minLength": 2, "pattern": "^A"},
                "age": {"bsonType": "number", "minimum": 18, "maximum": 40},
                "tags": {
                    "bsonType": "array",
                    "minItems": 2,
                    "uniqueItems": true,
                    "items": {"bsonType": "string"}
                },
                "meta": {
                    "bsonType": "object",
                    "properties": {
                        "active": {"bsonType": "bool"},
                        "score": {"bsonType": "number", "multipleOf": 0.5}
                    }
                }
            },
            "allOf": [{"required": ["meta"]}],
            "anyOf": [{"properties": {"age": {"minimum": 21}}}, {"required": ["missing"]}],
            "not": {"required": ["forbidden"]}
        });
        assert!(matches(&doc, &json!({"$jsonSchema": schema})));
        assert!(!matches(
            &doc,
            &json!({"$jsonSchema": {"required": ["forbidden"]}})
        ));
        assert!(!matches(
            &doc,
            &json!({"$jsonSchema": {"properties": {"age": {"maximum": 20}}}})
        ));
        assert!(!matches(
            &doc,
            &json!({"$jsonSchema": {"properties": {"tags": {"uniqueItems": true, "items": {"bsonType": "number"}}}}})
        ));
    }

    #[test]
    fn regex_operator_and_options() {
        let doc = json!({"name": "Ala", "tags": ["kot", "pies"], "n": 42});
        assert!(matches(&doc, &json!({"name": {"$regex": "^Al"}})));
        assert!(matches(
            &doc,
            &json!({"name": {"$regex": "^ala$", "$options": "i"}})
        ));
        assert!(!matches(&doc, &json!({"name": {"$regex": "^ala$"}})));
        assert!(matches(&doc, &json!({"tags": {"$regex": "^pi"}})));
        assert!(matches(&doc, &json!({"name": {"$not": {"$regex": "^Bo"}}})));
        assert!(!matches(&doc, &json!({"n": {"$regex": "42"}})));
        assert!(matches(
            &doc,
            &json!({"name": {"$regex": {"$regularExpression": {
                "pattern": "^ala$", "options": "i"
            }}}})
        ));
    }

    #[test]
    fn dotted_paths_traverse_arrays() {
        let doc = json!({
            "items": [{"price": 5}, {"price": 15}, {"other": 1}],
            "tags": [{"name": "a"}, {"name": "b"}]
        });
        assert!(matches(&doc, &json!({"items.price": 5})));
        assert!(matches(&doc, &json!({"items.price": {"$gt": 10}})));
        assert!(!matches(&doc, &json!({"items.price": {"$lt": 5}})));
        assert!(!matches(&doc, &json!({"items.price": {"$ne": 5}})));
        assert!(matches(&doc, &json!({"tags.name": {"$in": ["b", "c"]}})));
        assert!(!matches(&doc, &json!({"items.missing": {"$exists": true}})));
        assert!(matches(&doc, &json!({"items.missing": null})));
    }

    #[test]
    fn array_operators() {
        let doc = json!({
            "tags": ["a", "b"],
            "nums": [1, 2, 3],
            "items": [{"x": 1, "y": 2}, {"x": 2, "y": 3}],
            "scalar": "a"
        });
        assert!(matches(&doc, &json!({"tags": {"$all": ["a", "b"]}})));
        assert!(!matches(&doc, &json!({"tags": {"$all": ["a", "c"]}})));
        assert!(!matches(&doc, &json!({"tags": {"$all": []}})));
        assert!(matches(&doc, &json!({"scalar": {"$all": ["a"]}})));
        assert!(matches(&doc, &json!({"tags": {"$size": 2}})));
        assert!(!matches(&doc, &json!({"tags": {"$size": 1}})));
        assert!(!matches(&doc, &json!({"scalar": {"$size": 1}})));
        assert!(matches(
            &doc,
            &json!({"nums": {"$elemMatch": {"$gt": 2, "$lt": 4}}})
        ));
        assert!(matches(
            &doc,
            &json!({"items": {"$elemMatch": {"x": 1, "y": {"$gte": 2}}}})
        ));
        assert!(!matches(
            &doc,
            &json!({"items": {"$elemMatch": {"x": 1, "y": 3}}})
        ));
        assert!(matches(
            &doc,
            &json!({"items": {"$elemMatch": {"$and": [{"x": 2}, {"y": 3}]}}})
        ));
        assert!(!matches(&doc, &json!({"missing": {"$elemMatch": {}}})));
    }

    #[test]
    fn dates_compare_by_instant_across_offsets() {
        let doc = json!({"at": {"$date": "2026-01-01T00:00:00Z"}});
        assert!(matches(
            &doc,
            &json!({"at": {"$gte": {"$date": "2026-01-01T01:00:00+01:00"}}})
        ));
        assert!(matches(
            &doc,
            &json!({"at": {"$eq": {"$date": "2025-12-31T19:00:00-05:00"}}})
        ));
        assert!(!matches(
            &doc,
            &json!({"at": {"$gt": {"$date": "2026-01-01T00:00:00Z"}}})
        ));
    }

    #[test]
    fn date_and_string_are_different_types() {
        let doc = json!({"at": {"$date": "2026-01-01T00:00:00Z"}});
        assert!(!matches(
            &doc,
            &json!({"at": {"$gte": "2020-01-01T00:00:00Z"}})
        ));
    }
}
