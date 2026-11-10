//! Document path operations, e.g. `a.b.c`.

use crate::error::{Error, Result};
use serde_json::Value;

/// Reads a value at a path (dots denote nesting).
pub fn get<'a>(val: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = val;
    for part in path.split('.') {
        if part.is_empty() {
            return None;
        }
        cur = cur.get(part)?;
    }
    Some(cur)
}

/// Mutable read of a value at a path (without creating missing fields).
pub fn get_mut<'a>(val: &'a mut Value, path: &str) -> Option<&'a mut Value> {
    match path.split_once('.') {
        None => val.as_object_mut()?.get_mut(path),
        Some((head, rest)) => {
            let child = val.as_object_mut()?.get_mut(head)?;
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
pub fn unset(val: &mut Value, path: &str) -> bool {
    match path.split_once('.') {
        None => val
            .as_object_mut()
            .map(|o| o.remove(path).is_some())
            .unwrap_or(false),
        Some((head, rest)) => match val.get_mut(head) {
            Some(child) => unset(child, rest),
            None => false,
        },
    }
}
