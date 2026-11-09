//! Matcher filtrów w stylu MongoDB (podzbiór).
//!
//! Obsługiwane operatory: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`,
//! `$nin`, `$exists`, `$not`, `$and`, `$or`, `$nor`.
//!
//! Reguła porównań: liczby są zawsze sprowadzane do `f64`, więc `30` i `30.0`
//! są sobie równe — spójnie dla `$eq` i `$gt` (patrz TODO 3.0).

use serde_json::Value;
use std::cmp::Ordering;

use crate::query::path;

/// Czy dokument pasuje do filtra. Filtr nie będący obiektem pasuje do wszystkiego.
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
        // `$not` na poziomie filtra: neguje cały podfiltr.
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
    // Zwykła równość: { "name": "Ala" }.
    eq_opt(actual, cond)
}

/// Równość z semantyką MongoDB:
/// - brak pola == `null`,
/// - `{tags: "x"}` pasuje, gdy `tags` jest tablicą zawierającą `x`.
pub(crate) fn eq_opt(actual: Option<&Value>, expected: &Value) -> bool {
    match actual {
        None => expected.is_null(),
        Some(Value::Array(items)) if !expected.is_array() => {
            items.iter().any(|item| num_eq(item, expected))
        }
        Some(a) => num_eq(a, expected),
    }
}

/// Równość wartości z ujednoliconym porównaniem liczb.
pub(crate) fn num_eq(a: &Value, b: &Value) -> bool {
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
        _ => None,
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
        // Brak pola nadal znaczy `null`; `$exists` odróżnia te przypadki.
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
}
