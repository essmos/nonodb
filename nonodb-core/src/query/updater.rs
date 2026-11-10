//! MongoDB-style document updater (subset).
//!
//! Supported operators: `$set`, `$unset`, `$inc`, `$push`, `$pull`,
//! `$addToSet` and `$setOnInsert` (only on upsert).
//! An update without operators (a plain object) replaces the document while keeping `_id`.

use crate::error::{Error, Result};
use serde_json::{Map, Value};

use super::matcher::num_eq;
use super::path;

/// Builds a base document from equality conditions in the filter (`{a: 1}`,
/// `{a: {$eq: 1}}`, including inside `$and`). Other conditions are ignored.
pub fn seed_from_filter(filter: &Value) -> Value {
    let mut doc = Value::Object(Map::new());
    collect_seed(filter, &mut doc);
    doc
}

fn collect_seed(filter: &Value, doc: &mut Value) {
    let Some(obj) = filter.as_object() else {
        return;
    };
    for (k, v) in obj {
        if k == "$and" {
            if let Some(items) = v.as_array() {
                for item in items {
                    collect_seed(item, doc);
                }
            }
            continue;
        }
        if k.starts_with('$') {
            continue;
        }
        let eq = match v {
            Value::Object(inner) if inner.len() == 1 => inner.get("$eq"),
            Value::Object(_) => None,
            _ => Some(v),
        };
        if let Some(value) = eq {
            let _ = path::set(doc, k, value.clone());
        }
    }
}

fn each_values(v: &Value) -> Option<&Vec<Value>> {
    let obj = v.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    obj.get("$each")?.as_array()
}

/// Applies `update` to `doc` in place.
pub fn apply(doc: &mut Value, update: &Value) -> Result<()> {
    apply_with(doc, update, false)
}

/// Like `apply`, but with insert context (for `$setOnInsert`). For a plain
/// document replacement, it adds equality fields from the upsert filter.
pub fn apply_upsert(doc: &mut Value, update: &Value) -> Result<()> {
    let Some(obj) = update.as_object() else {
        return Err(Error::Query("update must be an object".into()));
    };
    let has_op = obj.keys().any(|k| k.starts_with('$'));
    if has_op {
        return apply_with(doc, update, true);
    }

    // Replacement: keep `_id` and equality fields from the filter that are absent
    // from the new document (Mongo upsert semantics).
    let mut out = update.clone();
    if let Some(seed) = doc.as_object() {
        for (k, v) in seed {
            if k == "_id" || path::get(&out, k).is_some() {
                continue;
            }
            path::set(&mut out, k, v.clone())?;
        }
    }
    let id = doc.get("_id").cloned().or_else(|| out.get("_id").cloned());
    *doc = out;
    if let Some(id) = id {
        doc["_id"] = id;
    }
    Ok(())
}

fn apply_with(doc: &mut Value, update: &Value, inserting: bool) -> Result<()> {
    let Some(obj) = update.as_object() else {
        return Err(Error::Query("update must be an object".into()));
    };
    if obj.is_empty() {
        return Ok(());
    }

    let has_op = obj.keys().any(|k| k.starts_with('$'));
    let has_plain = obj.keys().any(|k| !k.starts_with('$'));
    if has_op && has_plain {
        return Err(Error::Query(
            "cannot mix update operators with plain fields".into(),
        ));
    }

    if has_plain {
        let id = doc.get("_id").cloned();
        *doc = update.clone();
        if let Some(id) = id {
            doc["_id"] = id;
        }
        return Ok(());
    }

    for (op, arg) in obj {
        let args = arg
            .as_object()
            .ok_or_else(|| Error::Query(format!("{op} expects an object")))?;
        match op.as_str() {
            "$setOnInsert" => {
                if inserting {
                    for (p, v) in args {
                        path::set(doc, p, v.clone())?;
                    }
                }
            }
            "$set" => {
                for (p, v) in args {
                    path::set(doc, p, v.clone())?;
                }
            }
            "$unset" => {
                for (p, _) in args {
                    path::unset(doc, p);
                }
            }
            "$inc" => {
                for (p, v) in args {
                    let delta = v
                        .as_f64()
                        .ok_or_else(|| Error::Query("$inc expects numbers".into()))?;
                    let current = path::get(doc, p).and_then(Value::as_f64).unwrap_or(0.0);
                    let next = current + delta;
                    let out = if next.fract() == 0.0 && next.abs() < 9.0e15 {
                        Value::from(next as i64)
                    } else {
                        Value::from(next)
                    };
                    path::set(doc, p, out)?;
                }
            }
            "$push" => {
                for (p, v) in args {
                    let values: Vec<&Value> = match each_values(v) {
                        Some(items) => items.iter().collect(),
                        None => vec![v],
                    };
                    if !matches!(path::get(doc, p), Some(Value::Array(_))) {
                        path::set(doc, p, Value::Array(Vec::new()))?;
                    }
                    let arr = path::get_mut(doc, p)
                        .and_then(Value::as_array_mut)
                        .expect("just ensured array");
                    arr.extend(values.into_iter().cloned());
                }
            }
            "$addToSet" => {
                for (p, v) in args {
                    let values: Vec<&Value> = match each_values(v) {
                        Some(items) => items.iter().collect(),
                        None => vec![v],
                    };
                    if !matches!(path::get(doc, p), Some(Value::Array(_))) {
                        path::set(doc, p, Value::Array(Vec::new()))?;
                    }
                    let arr = path::get_mut(doc, p)
                        .and_then(Value::as_array_mut)
                        .expect("just ensured array");
                    for value in values {
                        if !arr.iter().any(|item| num_eq(item, value)) {
                            arr.push(value.clone());
                        }
                    }
                }
            }
            "$pull" => {
                for (p, v) in args {
                    if let Some(Value::Array(arr)) = path::get_mut(doc, p) {
                        arr.retain(|item| !num_eq(item, v));
                    }
                }
            }
            other => {
                return Err(Error::Query(format!("unsupported update operator {other}")));
            }
        }
    }
    Ok(())
}

/// Replaces the whole document while keeping `_id`.
#[allow(dead_code)]
pub fn replace(doc: &mut Value, replacement: &Value) -> Result<()> {
    let _ = Map::<String, Value>::new();
    apply(doc, replacement)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn set_and_inc() {
        let mut doc = json!({"_id": "1", "n": 1, "a": {"b": 2}});
        apply(
            &mut doc,
            &json!({"$set": {"a.b": 5, "x": true}, "$inc": {"n": 2}}),
        )
        .unwrap();
        assert_eq!(doc, json!({"_id": "1", "n": 3, "a": {"b": 5}, "x": true}));
    }

    #[test]
    fn unset_nested() {
        let mut doc = json!({"a": {"b": 1, "c": 2}});
        apply(&mut doc, &json!({"$unset": {"a.b": ""}})).unwrap();
        assert_eq!(doc, json!({"a": {"c": 2}}));
    }

    #[test]
    fn push_pull() {
        let mut doc = json!({"tags": ["a"]});
        apply(&mut doc, &json!({"$push": {"tags": "b"}})).unwrap();
        apply(&mut doc, &json!({"$pull": {"tags": "a"}})).unwrap();
        assert_eq!(doc, json!({"tags": ["b"]}));
    }

    #[test]
    fn replace_keeps_id() {
        let mut doc = json!({"_id": "1", "a": 1});
        apply(&mut doc, &json!({"b": 2})).unwrap();
        assert_eq!(doc, json!({"_id": "1", "b": 2}));
    }

    #[test]
    fn rejects_mixed() {
        let mut doc = json!({"_id": "1"});
        assert!(apply(&mut doc, &json!({"$set": {"a": 1}, "b": 2})).is_err());
    }

    #[test]
    fn add_to_set_and_each() {
        let mut doc = json!({"tags": ["a"]});
        apply(
            &mut doc,
            &json!({"$addToSet": {"tags": {"$each": ["a", "b", "b", "c"]}}}),
        )
        .unwrap();
        assert_eq!(doc, json!({"tags": ["a", "b", "c"]}));
    }

    #[test]
    fn push_each() {
        let mut doc = json!({"tags": []});
        apply(&mut doc, &json!({"$push": {"tags": {"$each": ["a", "b"]}}})).unwrap();
        assert_eq!(doc, json!({"tags": ["a", "b"]}));
    }

    #[test]
    fn upsert_seed_and_set_on_insert() {
        let mut doc = seed_from_filter(&json!({"tenant": "a", "key": "x", "age": {"$gt": 3}}));
        apply_upsert(
            &mut doc,
            &json!({"$set": {"value": 1}, "$setOnInsert": {"created": true}}),
        )
        .unwrap();
        assert_eq!(
            doc,
            json!({"tenant": "a", "key": "x", "value": 1, "created": true})
        );
        // Outside an insert, `$setOnInsert` is ignored.
        let mut existing = json!({"tenant": "a"});
        apply(&mut existing, &json!({"$setOnInsert": {"created": true}})).unwrap();
        assert_eq!(existing, json!({"tenant": "a"}));
    }

    #[test]
    fn upsert_replacement_keeps_filter_equalities() {
        let mut doc = seed_from_filter(&json!({"tenant": "a", "key": "x"}));
        apply_upsert(&mut doc, &json!({"value": 2})).unwrap();
        assert_eq!(doc, json!({"tenant": "a", "key": "x", "value": 2}));
    }
}
