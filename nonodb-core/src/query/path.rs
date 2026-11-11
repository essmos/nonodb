//! Document path operations, e.g. `a.b.c` and `items.0.name`.

use crate::error::{Error, Result};
use serde_json::Value;

fn array_index(part: &str) -> Option<usize> {
    if part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    part.parse().ok()
}

fn child<'a>(value: &'a Value, part: &str) -> Option<&'a Value> {
    match value {
        Value::Object(obj) => obj.get(part),
        Value::Array(items) => array_index(part).and_then(|i| items.get(i)),
        _ => None,
    }
}

fn child_mut<'a>(value: &'a mut Value, part: &str) -> Option<&'a mut Value> {
    match value {
        Value::Object(obj) => obj.get_mut(part),
        Value::Array(items) => array_index(part).and_then(|i| items.get_mut(i)),
        _ => None,
    }
}

/// Reads a value at a path (dots denote nesting, decimal segments index arrays).
pub fn get<'a>(val: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = val;
    for part in path.split('.') {
        if part.is_empty() {
            return None;
        }
        cur = child(cur, part)?;
    }
    Some(cur)
}

/// Reads all values reached by a path, traversing arrays for non-numeric
/// segments. For example, `items.price` on an array of objects returns the
/// price from every object that has it.
pub fn get_values<'a>(val: &'a Value, path: &str) -> Vec<&'a Value> {
    fn walk<'a>(val: &'a Value, parts: &[&str], out: &mut Vec<&'a Value>) {
        let Some((part, rest)) = parts.split_first() else {
            out.push(val);
            return;
        };
        match val {
            Value::Object(obj) => {
                if let Some(child) = obj.get(*part) {
                    walk(child, rest, out);
                }
            }
            Value::Array(items) => {
                if let Some(index) = array_index(part) {
                    if let Some(child) = items.get(index) {
                        walk(child, rest, out);
                    }
                } else {
                    for item in items {
                        walk(item, parts, out);
                    }
                }
            }
            _ => {}
        }
    }
    let parts: Vec<&str> = path.split('.').collect();
    if parts.iter().any(|part| part.is_empty()) {
        return Vec::new();
    }
    let mut out = Vec::new();
    walk(val, &parts, &mut out);
    out
}

/// Mutable read of a value at a path (without creating missing fields).
pub fn get_mut<'a>(val: &'a mut Value, path: &str) -> Option<&'a mut Value> {
    match path.split_once('.') {
        None => child_mut(val, path),
        Some((head, rest)) => {
            let child = child_mut(val, head)?;
            get_mut(child, rest)
        }
    }
}

/// Sets a value; creates missing intermediate objects.
pub fn set(val: &mut Value, path: &str, new: Value) -> Result<()> {
    let (head, rest) = match path.split_once('.') {
        Some((h, r)) => (h, Some(r)),
        None => (path, None),
    };
    if head.is_empty() {
        return Err(Error::Query("empty path".into()));
    }

    if let Value::Array(items) = val {
        let Some(index) = array_index(head) else {
            return Err(Error::Query(format!(
                "array path segment `{head}` is not an index"
            )));
        };
        if index >= items.len() {
            items.resize(index + 1, Value::Null);
        }
        return match rest {
            None => {
                items[index] = new;
                Ok(())
            }
            Some(rest) => set(&mut items[index], rest, new),
        };
    }

    if !val.is_object() {
        *val = Value::Object(Default::default());
    }
    let obj = val.as_object_mut().expect("checked is_object");
    match rest {
        None => {
            obj.insert(head.to_string(), new);
        }
        Some(rest) => {
            let child = obj.entry(head.to_string()).or_insert(Value::Null);
            set(child, rest, new)?;
        }
    }
    Ok(())
}

/// Removes a field at a path. Returns `true` if something was removed.
///
/// An array element is set to `null` rather than removed, preserving indexes.
pub fn unset(val: &mut Value, path: &str) -> bool {
    match path.split_once('.') {
        None => match val {
            Value::Object(obj) => obj.remove(path).is_some(),
            Value::Array(items) => {
                let Some(index) = array_index(path) else {
                    return false;
                };
                match items.get_mut(index) {
                    Some(slot) => {
                        *slot = Value::Null;
                        true
                    }
                    None => false,
                }
            }
            _ => false,
        },
        Some((head, rest)) => match child_mut(val, head) {
            Some(child) => unset(child, rest),
            None => false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn numeric_array_segments() {
        let mut doc = json!({"items": [{"name": "a"}, {"name": "b"}]});
        assert_eq!(get(&doc, "items.1.name"), Some(&json!("b")));
        set(&mut doc, "items.1.name", json!("c")).unwrap();
        assert_eq!(doc["items"][1]["name"], "c");
        assert!(unset(&mut doc, "items.0.name"));
        assert!(doc["items"][0]["name"].is_null());
    }

    #[test]
    fn get_values_traverses_arrays() {
        let doc = json!({
            "items": [{"price": 5}, {"price": 10}, {"other": 1}],
            "nested": [[{"v": 2}]],
            "tags": ["a", "b"]
        });
        assert_eq!(get_values(&doc, "items.price"), vec![&json!(5), &json!(10)]);
        assert_eq!(get_values(&doc, "nested.v"), vec![&json!(2)]);
        assert_eq!(get_values(&doc, "tags"), vec![&json!(["a", "b"])]);
        assert!(get_values(&doc, "items.missing").is_empty());
    }

    #[test]
    fn set_extends_arrays_with_null() {
        let mut doc = json!({"a": []});
        set(&mut doc, "a.2", json!(1)).unwrap();
        assert_eq!(doc, json!({"a": [null, null, 1]}));
    }
}
