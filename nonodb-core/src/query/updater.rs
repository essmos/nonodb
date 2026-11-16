//! MongoDB-style document updater (subset).
//!
//! Supported operators: `$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`,
//! `$rename`, `$currentDate`, `$bit`, `$pop`, `$push`, `$pull`, `$pullAll`,
//! `$addToSet` and `$setOnInsert` (only on upsert). Updates may also be an
//! array of pipeline stages using `$set`/`$addFields`, `$unset`, `$project`,
//! `$replaceRoot`, and `$replaceWith`.
//! An update without operators (a plain object) replaces the document while keeping `_id`.

use std::cmp::Ordering;

use bigdecimal::BigDecimal;
use serde_json::{Map, Value};

use crate::bson_types;
use crate::date;
use crate::error::{Error, Result};
use crate::numeric;

use super::matcher::{elem_item_matches, match_field, matches, num_eq};
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
            Value::Object(_)
                if date::is_date(v) || numeric::is_numeric(v) || bson_types::is_bson_value(v) =>
            {
                Some(v)
            }
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

struct Context<'a> {
    filter: Option<&'a Value>,
    array_filters: &'a [Value],
}

/// Applies `update` to `doc` in place.
pub fn apply(doc: &mut Value, update: &Value) -> Result<()> {
    apply_with(doc, update, false, None, &[])
}

/// Applies `update` with the original filter and array filters available for
/// positional operators (`$`, `$[]`, `$[identifier]`).
pub fn apply_with_options(
    doc: &mut Value,
    update: &Value,
    filter: &Value,
    array_filters: &[Value],
) -> Result<()> {
    apply_with(doc, update, false, Some(filter), array_filters)
}

/// Like `apply`, but with insert context (for `$setOnInsert`). For a plain
/// document replacement, it adds equality fields from the upsert filter.
pub fn apply_upsert(doc: &mut Value, update: &Value) -> Result<()> {
    apply_upsert_with(doc, update, None, &[])
}

pub fn apply_upsert_with_options(
    doc: &mut Value,
    update: &Value,
    filter: &Value,
    array_filters: &[Value],
) -> Result<()> {
    apply_upsert_with(doc, update, Some(filter), array_filters)
}

fn apply_upsert_with(
    doc: &mut Value,
    update: &Value,
    filter: Option<&Value>,
    array_filters: &[Value],
) -> Result<()> {
    if update.is_array() {
        return apply_with(doc, update, true, filter, array_filters);
    }
    let Some(obj) = update.as_object() else {
        return Err(Error::Query("update must be an object or pipeline".into()));
    };
    let has_op = obj.keys().any(|k| k.starts_with('$'));
    if has_op {
        return apply_with(doc, update, true, filter, array_filters);
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

fn apply_with(
    doc: &mut Value,
    update: &Value,
    inserting: bool,
    filter: Option<&Value>,
    array_filters: &[Value],
) -> Result<()> {
    if let Some(pipeline) = update.as_array() {
        return apply_pipeline(doc, pipeline);
    }
    let Some(obj) = update.as_object() else {
        return Err(Error::Query("update must be an object or pipeline".into()));
    };
    if obj.is_empty() {
        return Ok(());
    }
    validate_array_filters(array_filters)?;
    let ctx = Context {
        filter,
        array_filters,
    };

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
                        for target in resolve_paths(doc, p, &ctx)? {
                            path::set(doc, &target, v.clone())?;
                        }
                    }
                }
            }
            "$set" => {
                for (p, v) in args {
                    for target in resolve_paths(doc, p, &ctx)? {
                        path::set(doc, &target, v.clone())?;
                    }
                }
            }
            "$unset" => {
                for (p, _) in args {
                    for target in resolve_paths(doc, p, &ctx)? {
                        path::unset(doc, &target);
                    }
                }
            }
            "$inc" => {
                for (p, v) in args {
                    for target in resolve_paths(doc, p, &ctx)? {
                        inc_path(doc, &target, v)?;
                    }
                }
            }
            "$mul" => {
                for (p, v) in args {
                    for target in resolve_paths(doc, p, &ctx)? {
                        mul_path(doc, &target, v)?;
                    }
                }
            }
            "$min" | "$max" => {
                for (p, v) in args {
                    for target in resolve_paths(doc, p, &ctx)? {
                        let replace = match path::get(doc, &target) {
                            None => true,
                            Some(current) => {
                                let ord = crate::aggregate::compare_value(v, current);
                                if op == "$min" {
                                    ord == Ordering::Less
                                } else {
                                    ord == Ordering::Greater
                                }
                            }
                        };
                        if replace {
                            path::set(doc, &target, v.clone())?;
                        }
                    }
                }
            }
            "$rename" => {
                for (p, v) in args {
                    let Some(new_path) = v.as_str() else {
                        return Err(Error::Query("$rename target must be a string".into()));
                    };
                    if p == "_id" || new_path == "_id" || p == new_path {
                        return Err(Error::Query(
                            "$rename cannot target _id or the same path".into(),
                        ));
                    }
                    for target in resolve_paths(doc, p, &ctx)? {
                        let Some(value) = path::get(doc, &target).cloned() else {
                            continue;
                        };
                        path::unset(doc, &target);
                        path::set(doc, new_path, value)?;
                    }
                }
            }
            "$currentDate" => {
                let now = date::canonical(now_millis());
                let now_timestamp = bson_types::timestamp_value(bson_types::Timestamp {
                    time: u32::try_from(now_millis() / 1_000).unwrap_or(u32::MAX),
                    increment: 1,
                });
                for (p, v) in args {
                    let value =
                        match v {
                            Value::Bool(true) => now.clone(),
                            Value::Bool(false) => continue,
                            Value::Object(spec) if spec.len() == 1 => match spec.get("$type") {
                                Some(Value::String(kind)) if kind == "date" => now.clone(),
                                Some(Value::String(kind)) if kind == "timestamp" => {
                                    now_timestamp.clone()
                                }
                                _ => return Err(Error::Query(
                                    "$currentDate expects true or {$type: \"date\"|\"timestamp\"}"
                                        .into(),
                                )),
                            },
                            _ => {
                                return Err(Error::Query(
                                    "$currentDate expects true or {$type: \"date\"|\"timestamp\"}"
                                        .into(),
                                ))
                            }
                        };
                    for target in resolve_paths(doc, p, &ctx)? {
                        path::set(doc, &target, value.clone())?;
                    }
                }
            }
            "$bit" => {
                for (p, v) in args {
                    for target in resolve_paths(doc, p, &ctx)? {
                        bit_path(doc, &target, v)?;
                    }
                }
            }
            "$pop" => {
                for (p, v) in args {
                    let direction = v
                        .as_i64()
                        .ok_or_else(|| Error::Query("$pop expects 1 or -1".into()))?;
                    if direction != 1 && direction != -1 {
                        return Err(Error::Query("$pop expects 1 or -1".into()));
                    }
                    for target in resolve_paths(doc, p, &ctx)? {
                        match path::get_mut(doc, &target) {
                            Some(Value::Array(items)) => {
                                if !items.is_empty() {
                                    if direction == 1 {
                                        items.pop();
                                    } else {
                                        items.remove(0);
                                    }
                                }
                            }
                            Some(_) => {
                                return Err(Error::Query("$pop target must be an array".into()))
                            }
                            None => {}
                        }
                    }
                }
            }
            "$push" => {
                for (p, v) in args {
                    let spec = parse_push(v)?;
                    for target in resolve_paths(doc, p, &ctx)? {
                        if !matches!(path::get(doc, &target), Some(Value::Array(_))) {
                            path::set(doc, &target, Value::Array(Vec::new()))?;
                        }
                        let arr = path::get_mut(doc, &target)
                            .and_then(Value::as_array_mut)
                            .expect("just ensured array");
                        apply_push(arr, &spec)?;
                    }
                }
            }
            "$addToSet" => {
                for (p, v) in args {
                    let values: Vec<&Value> = match each_values(v) {
                        Some(items) => items.iter().collect(),
                        None => vec![v],
                    };
                    for target in resolve_paths(doc, p, &ctx)? {
                        if !matches!(path::get(doc, &target), Some(Value::Array(_))) {
                            path::set(doc, &target, Value::Array(Vec::new()))?;
                        }
                        let arr = path::get_mut(doc, &target)
                            .and_then(Value::as_array_mut)
                            .expect("just ensured array");
                        for value in values.iter() {
                            if !arr.iter().any(|item| num_eq(item, value)) {
                                arr.push((*value).clone());
                            }
                        }
                    }
                }
            }
            "$pull" => {
                for (p, v) in args {
                    for target in resolve_paths(doc, p, &ctx)? {
                        if let Some(Value::Array(arr)) = path::get_mut(doc, &target) {
                            arr.retain(|item| !pull_matches(item, v));
                        }
                    }
                }
            }
            "$pullAll" => {
                for (p, v) in args {
                    let values = v
                        .as_array()
                        .ok_or_else(|| Error::Query("$pullAll expects an array".into()))?;
                    for target in resolve_paths(doc, p, &ctx)? {
                        if let Some(Value::Array(arr)) = path::get_mut(doc, &target) {
                            arr.retain(|item| !values.iter().any(|v| num_eq(item, v)));
                        }
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

fn apply_pipeline(doc: &mut Value, pipeline: &[Value]) -> Result<()> {
    for stage in pipeline {
        let Some(obj) = stage.as_object() else {
            return Err(Error::Query(
                "update pipeline stages must be objects".into(),
            ));
        };
        if obj.len() != 1 {
            return Err(Error::Query(
                "update pipeline stages must contain exactly one operator".into(),
            ));
        }
        let (name, _) = obj.iter().next().expect("one entry");
        if !matches!(
            name.as_str(),
            "$set" | "$addFields" | "$unset" | "$project" | "$replaceRoot" | "$replaceWith"
        ) {
            return Err(Error::Query(format!(
                "unsupported update pipeline stage `{name}`"
            )));
        }
    }
    let old_id = doc.get("_id").cloned();
    let mut docs = crate::aggregate::run(vec![doc.clone()], pipeline, 1)?;
    if docs.len() != 1 {
        return Err(Error::Query(
            "update pipeline must produce exactly one document".into(),
        ));
    }
    let next = docs.pop().expect("one document");
    if let Some(id) = old_id {
        if next.get("_id") != Some(&id) {
            return Err(Error::Query(
                "update pipeline cannot modify or remove `_id`".into(),
            ));
        }
    }
    *doc = next;
    Ok(())
}

fn inc_path(doc: &mut Value, target: &str, v: &Value) -> Result<()> {
    let current_value = path::get(doc, target).cloned();
    if numeric::is_decimal(v) || current_value.as_ref().is_some_and(numeric::is_decimal) {
        let has_non_finite = current_value
            .as_ref()
            .and_then(numeric::parse_value)
            .is_some_and(|value| !value.is_finite())
            || numeric::parse_value(v).is_some_and(|value| !value.is_finite());
        if has_non_finite {
            let current = current_value
                .as_ref()
                .and_then(numeric::as_f64)
                .unwrap_or(0.0);
            let delta =
                numeric::as_f64(v).ok_or_else(|| Error::Query("$inc expects numbers".into()))?;
            path::set(doc, target, numeric::decimal_from_f64(current + delta)?)?;
            return Ok(());
        }
        let mut current = match current_value.as_ref() {
            Some(value) => numeric::as_bigdecimal(value)
                .ok_or_else(|| Error::Query("$inc current value is not a number".into()))?,
            None => BigDecimal::from(0),
        };
        let delta =
            numeric::as_bigdecimal(v).ok_or_else(|| Error::Query("$inc expects numbers".into()))?;
        current += delta;
        path::set(doc, target, numeric::decimal_value(&current)?)?;
        return Ok(());
    }
    if let Some(delta) = numeric::integer_value(v) {
        let current = match current_value.as_ref() {
            Some(value) => numeric::integer_value(value).unwrap_or(0),
            None => 0,
        };
        if current_value
            .as_ref()
            .is_none_or(|value| numeric::integer_value(value).is_some())
        {
            let next = current
                .checked_add(delta)
                .ok_or_else(|| Error::InvalidNumeric("$inc integer overflow".into()))?;
            let current_kind = current_value
                .as_ref()
                .and_then(numeric::extended_integer_kind);
            let delta_kind = numeric::extended_integer_kind(v);
            let kind = match (current_kind, delta_kind) {
                (Some(numeric::NumericKind::Long), _) | (_, Some(numeric::NumericKind::Long)) => {
                    numeric::NumericKind::Long
                }
                (Some(numeric::NumericKind::Int), _) | (_, Some(numeric::NumericKind::Int)) => {
                    numeric::NumericKind::Int
                }
                _ => {
                    path::set(doc, target, numeric::integer_json(next)?)?;
                    return Ok(());
                }
            };
            path::set(doc, target, numeric::integer_result(next, kind)?)?;
            return Ok(());
        }
    }
    let delta = numeric::as_f64(v).ok_or_else(|| Error::Query("$inc expects numbers".into()))?;
    let current = current_value
        .as_ref()
        .and_then(numeric::as_f64)
        .unwrap_or(0.0);
    let next = current + delta;
    let out = if next.fract() == 0.0 && next.abs() < 9.0e15 {
        Value::from(next as i64)
    } else {
        Value::from(next)
    };
    path::set(doc, target, out)
}

fn mul_path(doc: &mut Value, target: &str, v: &Value) -> Result<()> {
    let current_value = path::get(doc, target).cloned();
    if numeric::is_decimal(v) || current_value.as_ref().is_some_and(numeric::is_decimal) {
        let has_non_finite = current_value
            .as_ref()
            .and_then(numeric::parse_value)
            .is_some_and(|value| !value.is_finite())
            || numeric::parse_value(v).is_some_and(|value| !value.is_finite());
        if has_non_finite {
            let current = current_value
                .as_ref()
                .and_then(numeric::as_f64)
                .unwrap_or(0.0);
            let factor =
                numeric::as_f64(v).ok_or_else(|| Error::Query("$mul expects numbers".into()))?;
            path::set(doc, target, numeric::decimal_from_f64(current * factor)?)?;
            return Ok(());
        }
        let mut current = match current_value.as_ref() {
            Some(value) => numeric::as_bigdecimal(value)
                .ok_or_else(|| Error::Query("$mul current value is not a number".into()))?,
            None => BigDecimal::from(0),
        };
        let factor =
            numeric::as_bigdecimal(v).ok_or_else(|| Error::Query("$mul expects numbers".into()))?;
        current *= factor;
        path::set(doc, target, numeric::decimal_value(&current)?)?;
        return Ok(());
    }
    if let Some(factor) = numeric::integer_value(v) {
        let current = current_value.as_ref().and_then(numeric::integer_value);
        if current_value
            .as_ref()
            .is_none_or(|value| numeric::integer_value(value).is_some())
        {
            let current = current.unwrap_or(0);
            let next = current
                .checked_mul(factor)
                .ok_or_else(|| Error::InvalidNumeric("$mul integer overflow".into()))?;
            let kind = match (
                current_value
                    .as_ref()
                    .and_then(numeric::extended_integer_kind),
                numeric::extended_integer_kind(v),
            ) {
                (Some(numeric::NumericKind::Long), _) | (_, Some(numeric::NumericKind::Long)) => {
                    Some(numeric::NumericKind::Long)
                }
                (Some(numeric::NumericKind::Int), _) | (_, Some(numeric::NumericKind::Int)) => {
                    Some(numeric::NumericKind::Int)
                }
                _ => None,
            };
            let result = match kind {
                Some(kind) => numeric::integer_result(next, kind)?,
                None => numeric::integer_json(next)?,
            };
            path::set(doc, target, result)?;
            return Ok(());
        }
    }
    let factor = numeric::as_f64(v).ok_or_else(|| Error::Query("$mul expects numbers".into()))?;
    let current = match current_value.as_ref() {
        Some(value) => numeric::as_f64(value)
            .ok_or_else(|| Error::Query("$mul current value is not a number".into()))?,
        None => 0.0,
    };
    path::set(doc, target, Value::from(current * factor))
}

fn bit_path(doc: &mut Value, target: &str, v: &Value) -> Result<()> {
    let Some(spec) = v.as_object() else {
        return Err(Error::Query("$bit expects {and|or|xor: integer}".into()));
    };
    if spec.len() != 1 {
        return Err(Error::Query(
            "$bit expects exactly one of and, or, xor".into(),
        ));
    }
    let (op, operand) = spec.iter().next().expect("one entry");
    if !matches!(op.as_str(), "and" | "or" | "xor") {
        return Err(Error::Query(format!("unsupported $bit operation {op}")));
    }
    let operand = numeric::integer_value(operand)
        .ok_or_else(|| Error::Query("$bit operand must be an integer".into()))?;
    let current_value = path::get(doc, target).cloned();
    let current = match current_value.as_ref() {
        Some(value) => numeric::integer_value(value)
            .ok_or_else(|| Error::Query("$bit current value must be an integer".into()))?,
        None => 0,
    };
    let next = match op.as_str() {
        "and" => current & operand,
        "or" => current | operand,
        "xor" => current ^ operand,
        _ => unreachable!(),
    };
    let kind = match (
        current_value
            .as_ref()
            .and_then(numeric::extended_integer_kind),
        spec.values()
            .next()
            .and_then(numeric::extended_integer_kind),
    ) {
        (Some(numeric::NumericKind::Long), _) | (_, Some(numeric::NumericKind::Long)) => {
            Some(numeric::NumericKind::Long)
        }
        (Some(numeric::NumericKind::Int), _) | (_, Some(numeric::NumericKind::Int)) => {
            Some(numeric::NumericKind::Int)
        }
        _ => None,
    };
    let result = match kind {
        Some(kind) => numeric::integer_result(next, kind)?,
        None => numeric::integer_json(next)?,
    };
    path::set(doc, target, result)
}

fn now_millis() -> i64 {
    (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
}

struct PushSpec {
    values: Vec<Value>,
    position: Option<i64>,
    slice: Option<i64>,
    sort: Option<Value>,
}

fn parse_push(v: &Value) -> Result<PushSpec> {
    let Some(obj) = v.as_object() else {
        return Ok(PushSpec {
            values: vec![v.clone()],
            position: None,
            slice: None,
            sort: None,
        });
    };
    if !obj.contains_key("$each") {
        if obj.keys().any(|k| k.starts_with('$')) {
            return Err(Error::Query("$push modifiers require $each".into()));
        }
        return Ok(PushSpec {
            values: vec![v.clone()],
            position: None,
            slice: None,
            sort: None,
        });
    }
    for key in obj.keys() {
        if !matches!(key.as_str(), "$each" | "$position" | "$slice" | "$sort") {
            return Err(Error::Query(format!("unsupported $push modifier {key}")));
        }
    }
    let values = obj
        .get("$each")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::Query("$push $each expects an array".into()))?
        .clone();
    let position = parse_optional_i64(obj.get("$position"), "$position")?;
    let slice = parse_optional_i64(obj.get("$slice"), "$slice")?;
    let sort = obj.get("$sort").cloned();
    if let Some(sort) = &sort {
        validate_sort_spec(sort)?;
    }
    Ok(PushSpec {
        values,
        position,
        slice,
        sort,
    })
}

fn parse_optional_i64(value: Option<&Value>, name: &str) -> Result<Option<i64>> {
    match value {
        None => Ok(None),
        Some(v) => v
            .as_i64()
            .map(Some)
            .ok_or_else(|| Error::Query(format!("{name} expects an integer"))),
    }
}

fn apply_push(arr: &mut Vec<Value>, spec: &PushSpec) -> Result<()> {
    let start = match spec.position {
        None => arr.len(),
        Some(pos) if pos >= 0 => (pos as usize).min(arr.len()),
        Some(pos) => {
            let from_end = arr.len() as i64 + pos;
            from_end.max(0) as usize
        }
    };
    for (offset, value) in spec.values.iter().enumerate() {
        let at = (start + offset).min(arr.len());
        arr.insert(at, value.clone());
    }
    if let Some(sort) = &spec.sort {
        sort_array(arr, sort)?;
    }
    if let Some(slice) = spec.slice {
        if slice == 0 {
            arr.clear();
        } else if slice > 0 {
            arr.truncate(slice as usize);
        } else {
            let keep = (-slice) as usize;
            if arr.len() > keep {
                arr.drain(..arr.len() - keep);
            }
        }
    }
    Ok(())
}

fn validate_sort_spec(spec: &Value) -> Result<()> {
    if let Some(dir) = spec.as_i64() {
        if matches!(dir, 1 | -1) {
            return Ok(());
        }
    }
    if let Some(obj) = spec.as_object() {
        if !obj.is_empty() && obj.values().all(|v| matches!(v.as_i64(), Some(1 | -1))) {
            return Ok(());
        }
    }
    Err(Error::Query(
        "$sort expects 1, -1, or an object of fields with 1/-1".into(),
    ))
}

fn sort_array(arr: &mut [Value], spec: &Value) -> Result<()> {
    if let Some(dir) = spec.as_i64() {
        arr.sort_by(|a, b| {
            let ord = compare_sort_values(Some(a), Some(b));
            if dir >= 0 {
                ord
            } else {
                ord.reverse()
            }
        });
        return Ok(());
    }
    let obj = spec
        .as_object()
        .ok_or_else(|| Error::Query("invalid $sort specification".into()))?;
    let fields: Vec<(String, i64)> = obj
        .iter()
        .map(|(k, v)| {
            let dir = v.as_i64().unwrap_or(1);
            (k.clone(), dir)
        })
        .collect();
    arr.sort_by(|a, b| {
        for (field, dir) in &fields {
            let ord = compare_sort_values(path::get(a, field), path::get(b, field));
            if ord != std::cmp::Ordering::Equal {
                return if *dir >= 0 { ord } else { ord.reverse() };
            }
        }
        std::cmp::Ordering::Equal
    });
    Ok(())
}

fn compare_sort_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(a), Some(b)) => {
            if let Some(ord) = numeric::cmp_values(a, b) {
                return ord;
            }
            if let Some(ord) = bson_types::cmp_values(a, b) {
                return ord;
            }
            match (a, b) {
                (Value::Null, Value::Null) => Ordering::Equal,
                (Value::Null, _) => Ordering::Greater,
                (_, Value::Null) => Ordering::Less,
                (Value::String(x), Value::String(y)) => x.cmp(y),
                (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
                _ => date::cmp(a, b).unwrap_or(Ordering::Equal),
            }
        }
    }
}

fn pull_matches(item: &Value, cond: &Value) -> bool {
    let Some(obj) = cond.as_object() else {
        return num_eq(item, cond);
    };
    if obj.is_empty() {
        return matches(item, cond);
    }
    if obj.keys().all(|k| k.starts_with('$')) {
        match_field(Some(item), cond)
    } else {
        matches(item, cond)
    }
}

fn validate_array_filters(filters: &[Value]) -> Result<()> {
    for filter in filters {
        let Some(obj) = filter.as_object() else {
            return Err(Error::Query("arrayFilters entries must be objects".into()));
        };
        if obj.is_empty() {
            return Err(Error::Query("arrayFilters entries cannot be empty".into()));
        }
        let mut id: Option<&str> = None;
        for key in obj.keys() {
            let prefix = key.split('.').next().unwrap_or(key);
            if !valid_identifier(prefix) {
                return Err(Error::Query(format!(
                    "invalid array filter identifier `{prefix}`"
                )));
            }
            match id {
                None => id = Some(prefix),
                Some(prev) if prev != prefix => {
                    return Err(Error::Query(
                        "one array filter cannot mix identifiers".into(),
                    ));
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn valid_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    first.is_ascii_lowercase() && chars.all(|c| c.is_ascii_alphanumeric())
}

fn resolve_paths(doc: &Value, raw: &str, ctx: &Context<'_>) -> Result<Vec<String>> {
    let parts: Vec<&str> = raw.split('.').collect();
    if parts.iter().any(|p| p.is_empty()) {
        return Err(Error::Query("empty path".into()));
    }
    let mut out = Vec::new();
    let mut built = Vec::new();
    expand_path(doc, &parts, 0, &mut built, ctx, &mut out)?;
    Ok(out)
}

fn expand_path(
    doc: &Value,
    parts: &[&str],
    index: usize,
    built: &mut Vec<String>,
    ctx: &Context<'_>,
    out: &mut Vec<String>,
) -> Result<()> {
    if index == parts.len() {
        out.push(built.join("."));
        return Ok(());
    }
    let part = parts[index];
    if part == "$" || part == "$[]" || (part.starts_with("$[") && part.ends_with(']')) {
        let base = built.join(".");
        let arr = path::get(doc, &base)
            .and_then(Value::as_array)
            .ok_or_else(|| Error::Query(format!("array path `{base}` is not an array")))?;
        if part == "$" {
            let Some(i) = positional_index(arr, &base, ctx)? else {
                return Ok(());
            };
            built.push(i.to_string());
            expand_path(doc, parts, index + 1, built, ctx, out)?;
            built.pop();
            return Ok(());
        }
        let filter_id = if part == "$[]" {
            None
        } else {
            let id = &part[2..part.len() - 1];
            if !valid_identifier(id) {
                return Err(Error::Query(format!(
                    "invalid array filter placeholder `{part}`"
                )));
            }
            Some(id)
        };
        for (i, item) in arr.iter().enumerate() {
            if let Some(id) = filter_id {
                if !array_filter_matches(item, id, ctx)? {
                    continue;
                }
            }
            built.push(i.to_string());
            expand_path(doc, parts, index + 1, built, ctx, out)?;
            built.pop();
        }
        return Ok(());
    }
    if part.starts_with('$') {
        return Err(Error::Query(format!("unsupported path segment `{part}`")));
    }
    built.push(part.to_string());
    expand_path(doc, parts, index + 1, built, ctx, out)?;
    built.pop();
    Ok(())
}

fn array_filter_matches(item: &Value, id: &str, ctx: &Context<'_>) -> Result<bool> {
    let mut found = false;
    for filter in ctx.array_filters {
        let obj = filter
            .as_object()
            .ok_or_else(|| Error::Query("arrayFilters entries must be objects".into()))?;
        let mut relevant = false;
        for key in obj.keys() {
            let prefix = key.split('.').next().unwrap_or(key);
            if prefix == id {
                relevant = true;
            }
        }
        if !relevant {
            continue;
        }
        found = true;
        for (key, cond) in obj {
            if key == id {
                if !array_filter_condition_matches(item, cond) {
                    return Ok(false);
                }
            } else {
                let rest = key
                    .strip_prefix(id)
                    .and_then(|s| s.strip_prefix('.'))
                    .ok_or_else(|| Error::Query("invalid array filter key".into()))?;
                if !matches(item, &serde_json::json!({ rest: cond })) {
                    return Ok(false);
                }
            }
        }
    }
    if !found {
        return Err(Error::Query(format!(
            "no arrayFilters entry for identifier `{id}`"
        )));
    }
    Ok(true)
}

fn array_filter_condition_matches(item: &Value, cond: &Value) -> bool {
    if cond
        .as_object()
        .is_some_and(|o| !o.is_empty() && o.keys().all(|k| k.starts_with('$')))
    {
        return match_field(Some(item), cond);
    }
    if item.is_object() && cond.is_object() {
        return matches(item, cond);
    }
    match_field(Some(item), cond)
}

fn positional_index(arr: &[Value], base: &str, ctx: &Context<'_>) -> Result<Option<usize>> {
    let Some(filter) = ctx.filter else {
        return Err(Error::Query(
            "positional `$` requires a filter condition on the array".into(),
        ));
    };
    let mut found = false;
    let mut doc_conds: Vec<(String, Value)> = Vec::new();
    let mut elem_conds: Vec<Value> = Vec::new();
    let mut value_conds: Vec<Value> = Vec::new();
    collect_positional(
        filter,
        base,
        &mut found,
        &mut doc_conds,
        &mut elem_conds,
        &mut value_conds,
    )?;
    if !found {
        return Err(Error::Query(format!(
            "positional `$` has no filter condition for `{base}`"
        )));
    }
    for (i, item) in arr.iter().enumerate() {
        let docs_ok = doc_conds
            .iter()
            .all(|(field, cond)| matches(item, &serde_json::json!({ field: cond })));
        let elems_ok = elem_conds.iter().all(|arg| elem_item_matches(item, arg));
        let values_ok = value_conds.iter().all(|cond| match_field(Some(item), cond));
        if docs_ok && elems_ok && values_ok {
            return Ok(Some(i));
        }
    }
    Ok(None)
}

fn collect_positional(
    filter: &Value,
    base: &str,
    found: &mut bool,
    doc_conds: &mut Vec<(String, Value)>,
    elem_conds: &mut Vec<Value>,
    value_conds: &mut Vec<Value>,
) -> Result<()> {
    let Some(obj) = filter.as_object() else {
        return Ok(());
    };
    for (key, cond) in obj {
        if key == "$and" {
            if let Some(items) = cond.as_array() {
                for item in items {
                    collect_positional(item, base, found, doc_conds, elem_conds, value_conds)?;
                }
            }
            continue;
        }
        if key == base {
            *found = true;
            if let Some(inner) = cond.as_object().and_then(|o| o.get("$elemMatch")) {
                elem_conds.push(inner.clone());
            } else {
                value_conds.push(cond.clone());
            }
        } else if let Some(rest) = key.strip_prefix(base).and_then(|s| s.strip_prefix('.')) {
            *found = true;
            doc_conds.push((rest.to_string(), cond.clone()));
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
    fn non_finite_decimal_arithmetic_stays_decimal() {
        let mut doc = json!({
            "n": {"$numberDecimal": "Infinity"},
            "m": {"$numberDecimal": "2.5"}
        });
        apply(
            &mut doc,
            &json!({
                "$inc": {"n": {"$numberDecimal": "1"}},
                "$mul": {"m": {"$numberDecimal": "Infinity"}}
            }),
        )
        .unwrap();
        assert_eq!(doc["n"], json!({"$numberDecimal": "Infinity"}));
        assert_eq!(doc["m"], json!({"$numberDecimal": "Infinity"}));

        apply(
            &mut doc,
            &json!({"$inc": {"n": {"$numberDecimal": "-Infinity"}}}),
        )
        .unwrap();
        assert_eq!(doc["n"], json!({"$numberDecimal": "NaN"}));
        assert_eq!(doc["m"], json!({"$numberDecimal": "Infinity"}));
    }

    #[test]
    fn unset_nested() {
        let mut doc = json!({"a": {"b": 1, "c": 2}});
        apply(&mut doc, &json!({"$unset": {"a.b": ""}})).unwrap();
        assert_eq!(doc, json!({"a": {"c": 2}}));
    }

    #[test]
    fn mul_min_max_rename_date_bit_and_pop() {
        let mut doc = json!({
            "n": 4,
            "low": 10,
            "high": 1,
            "old": "v",
            "bits": 0b1010,
            "arr": [1, 2, 3]
        });
        apply(
            &mut doc,
            &json!({
                "$mul": {"n": 3, "missing": 5},
                "$min": {"low": 5, "newlow": 7},
                "$max": {"high": 8, "newhigh": 9},
                "$rename": {"old": "new"},
                "$bit": {"bits": {"xor": 0b0101}},
                "$pop": {"arr": -1}
            }),
        )
        .unwrap();
        assert_eq!(doc["n"], 12);
        assert_eq!(doc["missing"], 0);
        assert_eq!(doc["low"], 5);
        assert_eq!(doc["newlow"], 7);
        assert_eq!(doc["high"], 8);
        assert_eq!(doc["newhigh"], 9);
        assert!(doc.get("old").is_none());
        assert_eq!(doc["new"], "v");
        assert_eq!(doc["bits"], 15);
        assert_eq!(doc["arr"], json!([2, 3]));

        apply(&mut doc, &json!({"$pop": {"arr": 1}})).unwrap();
        assert_eq!(doc["arr"], json!([2]));
        apply(&mut doc, &json!({"$currentDate": {"at": true}})).unwrap();
        assert!(date::is_date(&doc["at"]));

        assert!(apply(&mut doc, &json!({"$rename": {"old": "_id"}})).is_err());
        apply(
            &mut doc,
            &json!({"$currentDate": {"ts": {"$type": "timestamp"}}}),
        )
        .unwrap();
        assert!(bson_types::is_timestamp(&doc["ts"]));
        assert!(apply(&mut doc, &json!({"$pop": {"n": 1}})).is_err());
        assert!(apply(&mut doc, &json!({"$bit": {"n": {"and": "x"}}})).is_err());
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
    fn pipeline_updates_and_immutable_id() {
        let mut doc = json!({"_id": "1", "a": 2, "b": 3, "old": true});
        apply(
            &mut doc,
            &json!([
                {"$set": {"total": {"$add": ["$a", "$b"]}}},
                {"$unset": "old"},
                {"$project": {"_id": 1, "a": 1, "b": 1, "total": 1}}
            ]),
        )
        .unwrap();
        assert_eq!(doc, json!({"_id": "1", "a": 2, "b": 3, "total": 5}));

        apply(
            &mut doc,
            &json!([{"$replaceWith": {
                "$mergeObjects": ["$$ROOT", {"kind": "pipeline"}]
            }}]),
        )
        .unwrap();
        assert_eq!(doc["kind"], "pipeline");
        assert_eq!(doc["_id"], "1");

        let before = doc.clone();
        assert!(apply(&mut doc, &json!([{"$set": {"_id": "2"}}])).is_err());
        assert_eq!(doc, before);
        assert!(apply(&mut doc, &json!([{"$match": {}}])).is_err());
        assert!(apply(&mut doc, &json!([{"$limit": 1}])).is_err());
        assert!(apply(&mut doc, &json!([{"$set": {"a": 1}, "b": 2}])).is_err());
        assert!(apply(&mut doc, &json!([])).is_ok());

        let mut inserted = json!({"_id": "3"});
        apply_upsert(&mut inserted, &json!([{"$set": {"created": true}}])).unwrap();
        assert_eq!(inserted, json!({"_id": "3", "created": true}));
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
    fn array_paths_all_and_filters() {
        let mut doc = json!({
            "items": [
                {"sku": "x", "qty": 2},
                {"sku": "y", "qty": 9}
            ]
        });
        apply_with_options(
            &mut doc,
            &json!({"$inc": {"items.$[i].qty": 1}}),
            &json!({}),
            &[json!({"i.sku": "x"})],
        )
        .unwrap();
        assert_eq!(doc["items"][0]["qty"], 3);
        assert_eq!(doc["items"][1]["qty"], 9);

        apply_with_options(
            &mut doc,
            &json!({"$set": {"items.$[].checked": true}}),
            &json!({}),
            &[],
        )
        .unwrap();
        assert_eq!(doc["items"][0]["checked"], true);
        assert_eq!(doc["items"][1]["checked"], true);

        apply(&mut doc, &json!({"$set": {"items.0.qty": 100}})).unwrap();
        assert_eq!(doc["items"][0]["qty"], 100);
        apply(&mut doc, &json!({"$unset": {"items.1.qty": ""}})).unwrap();
        assert!(doc["items"][1]["qty"].is_null());
    }

    #[test]
    fn positional_dollar_uses_filter_match() {
        let mut doc = json!({"items": [{"sku": "x", "qty": 2}, {"sku": "y", "qty": 9}]});
        apply_with_options(
            &mut doc,
            &json!({"$set": {"items.$.qty": 5}}),
            &json!({"items.sku": "y"}),
            &[],
        )
        .unwrap();
        assert_eq!(doc["items"][1]["qty"], 5);
        assert_eq!(doc["items"][0]["qty"], 2);

        apply_with_options(
            &mut doc,
            &json!({"$inc": {"items.$.qty": 1}}),
            &json!({"items": {"$elemMatch": {"sku": "x", "qty": {"$lt": 5}}}}),
            &[],
        )
        .unwrap();
        assert_eq!(doc["items"][0]["qty"], 3);
    }

    #[test]
    fn push_modifiers_sort_position_slice() {
        let mut doc = json!({"tags": ["b"]});
        apply(
            &mut doc,
            &json!({"$push": {"tags": {
                "$each": ["a", "c"],
                "$sort": 1,
                "$slice": 2
            }}}),
        )
        .unwrap();
        assert_eq!(doc["tags"], json!(["a", "b"]));

        apply(
            &mut doc,
            &json!({"$push": {"tags": {
                "$each": ["z", "y"],
                "$position": 1,
                "$slice": -2
            }}}),
        )
        .unwrap();
        assert_eq!(doc["tags"], json!(["y", "b"]));

        let mut docs = json!({"scores": [{"n": 1}, {"n": 3}, {"n": 2}]});
        apply(
            &mut docs,
            &json!({"$push": {"scores": {
                "$each": [{"n": 4}],
                "$sort": {"n": -1},
                "$slice": 2
            }}}),
        )
        .unwrap();
        assert_eq!(docs["scores"], json!([{"n": 4}, {"n": 3}]));
    }

    #[test]
    fn pull_predicates_and_pull_all() {
        let mut doc = json!({
            "nums": [1, 5, 10],
            "items": [{"x": 1}, {"x": 2}, {"x": 3}]
        });
        apply(&mut doc, &json!({"$pull": {"nums": {"$gte": 5}}})).unwrap();
        assert_eq!(doc["nums"], json!([1]));
        apply(&mut doc, &json!({"$pull": {"items": {"x": {"$lte": 2}}}})).unwrap();
        assert_eq!(doc["items"], json!([{"x": 3}]));
        apply(&mut doc, &json!({"$pullAll": {"nums": [1]}})).unwrap();
        assert_eq!(doc["nums"], json!([]));
    }

    #[test]
    fn invalid_array_update_is_rejected() {
        let mut doc = json!({"a": [1, 2]});
        assert!(
            apply_with_options(&mut doc, &json!({"$set": {"a.$[x]": 3}}), &json!({}), &[]).is_err()
        );
        assert!(apply_with_options(
            &mut doc,
            &json!({"$set": {"a.$[x]": 3}}),
            &json!({}),
            &[json!({"y": {"$gt": 0}})]
        )
        .is_err());
        assert!(apply(
            &mut doc,
            &json!({"$push": {"a": {
                "$position": 1
            }}})
        )
        .is_err());
        assert!(apply(&mut doc, &json!({"$set": {"a.5": 3}})).is_ok());
        assert_eq!(doc["a"], json!([1, 2, null, null, null, 3]));
    }

    #[test]
    fn upsert_replacement_keeps_filter_equalities() {
        let mut doc = seed_from_filter(&json!({"tenant": "a", "key": "x"}));
        apply_upsert(&mut doc, &json!({"value": 2})).unwrap();
        assert_eq!(doc, json!({"tenant": "a", "key": "x", "value": 2}));
    }
}
