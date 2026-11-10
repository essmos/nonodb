//! MongoDB-style filter matcher (subset).
//!
//! Supported operators: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`,
//! `$nin`, `$exists`, `$not`, `$and`, `$or`, `$nor`.
//!
//! Comparison rule: numbers are always converted to `f64`, so `30` and `30.0`
//! compare as equal - consistently for `$eq` and `$gt` (see TODO 3.0).

use serde_json::Value;
use std::cmp::Ordering;

use crate::date;
use crate::query::path;

/// Does the document match the filter? A non-object filter matches everything.
pub fn matches(doc: &Value, filter: &Value) -> bool {
    let Some(obj) = filter.as_object() else {
        return true;
    };
    obj.iter().all(|(key, cond)| match key.as_str() {
        "$and" => cond
            .as_array()
            .is_some_and(|arr| arr.iter().all(|f| matches(doc, f))),
        "$or" => cond
            .as_array()
            .is_some_and(|arr| arr.iter().any(|f| matches(doc, f))),
        "$nor" => cond
            .as_array()
            .is_some_and(|arr| !arr.iter().any(|f| matches(doc, f))),
        // `$not` at filter level: negates the whole subfilter.
        "$not" => !matches(doc, cond),
        _ => {
            let actual = if key.contains('.') {
                path::get(doc, key)
            } else {
                doc.get(key)
            };
            match_field(actual, cond)
        }
    })
}

fn match_field(actual: Option<&Value>, cond: &Value) -> bool {
    // Extended JSON dates are objects, not operators.
    if date::is_date(cond) {
        return eq_opt(actual, cond);
    }
    if let Some(ops) = cond.as_object() {
        if !ops.is_empty() && ops.keys().all(|k| k.starts_with('$')) {
            return ops.iter().all(|(op, arg)| match op.as_str() {
                "$eq" => eq_opt(actual, arg),
                "$ne" => !eq_opt(actual, arg),
                "$gt" => cmp(actual, arg) == Some(Ordering::Greater),
                "$gte" => matches!(cmp(actual, arg), Some(Ordering::Greater | Ordering::Equal)),
                "$lt" => cmp(actual, arg) == Some(Ordering::Less),
                "$lte" => matches!(cmp(actual, arg), Some(Ordering::Less | Ordering::Equal)),
                "$in" => arg
                    .as_array()
                    .is_some_and(|arr| arr.iter().any(|x| eq_opt(actual, x))),
                "$nin" => arg
                    .as_array()
                    .is_some_and(|arr| !arr.iter().any(|x| eq_opt(actual, x))),
                "$exists" => actual.is_some() == arg.as_bool().unwrap_or(false),
                "$not" => !match_field(actual, arg),
                _ => false,
            });
        }
    }
    // Plain equality: { "name": "Ala" }.
    eq_opt(actual, cond)
}

/// Equality with MongoDB semantics:
/// - missing field == `null`,
/// - `{tags: "x"}` matches when `tags` is an array containing `x`.
pub(crate) fn eq_opt(actual: Option<&Value>, expected: &Value) -> bool {
    match actual {
        None => expected.is_null(),
        Some(Value::Array(items)) if !expected.is_array() => {
            items.iter().any(|item| num_eq(item, expected))
        }
        Some(a) => num_eq(a, expected),
    }
}

/// Value equality with unified number comparison.
pub(crate) fn num_eq(a: &Value, b: &Value) -> bool {
    if let (Some(x), Some(y)) = (date::parse_value(a), date::parse_value(b)) {
        return x == y;
    }
    if date::parse_value(a).is_some() || date::parse_value(b).is_some() {
        return false;
    }
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(x), Some(y)) => x == y,
            _ => x == y,
        },
        _ => a == b,
    }
}

fn cmp(actual: Option<&Value>, expected: &Value) -> Option<Ordering> {
    let a = actual?;
    match (a, expected) {
        (Value::Number(x), Value::Number(y)) => x.as_f64()?.partial_cmp(&y.as_f64()?),
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
    fn array_contains() {
        let doc = json!({"tags": ["a", "b"]});
        assert!(matches(&doc, &json!({"tags": "a"})));
        assert!(!matches(&doc, &json!({"tags": "c"})));
        assert!(matches(&doc, &json!({"tags": {"$in": ["c", "b"]}})));
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
