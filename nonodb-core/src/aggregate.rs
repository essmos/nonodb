//! Read-only aggregation pipeline.
//!
//! The first version is intentionally bounded: it operates on one collection,
//! materializes intermediate results, and supports the common reporting stages.
//! It never writes documents or indexes.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

use serde_json::{Map, Value};

use crate::collection::compare_docs;
use crate::date;
use crate::error::{Error, Result};
use crate::query::{matches, path};

/// Maximum number of documents allowed in an intermediate aggregation result.
pub const MAX_AGGREGATE_DOCS: usize = 100_000;

/// Parts of a pipeline that can be pushed into the normal `find_in` path.
#[derive(Debug, Default)]
pub(crate) struct Pushdown {
    pub(crate) filter: Value,
    pub(crate) sort: Vec<(String, i32)>,
    pub(crate) skip: usize,
    pub(crate) limit: Option<usize>,
    pub(crate) projection: Option<Value>,
    pub(crate) count: Option<String>,
    pub(crate) next: usize,
}

/// Extracts a safe prefix of the pipeline for the query planner.
///
/// This only moves stages that have exactly the same semantics before and
/// after the move. The remaining stages are still executed by [`run`].
pub(crate) fn pushdown(pipeline: &[Value]) -> Result<Pushdown> {
    let mut plan = Pushdown::default();
    let mut i = 0;

    if let Some(("$match", arg)) = stage_at(pipeline, i) {
        plan.filter = arg.clone();
        i += 1;
    }

    // `$count` is especially cheap when it is the only remaining stage.
    if let Some(("$count", arg)) = stage_at(pipeline, i) {
        if i + 1 == pipeline.len() {
            plan.count = Some(count_field_name(arg)?);
            plan.next = i + 1;
            return Ok(plan);
        }
    }

    if let Some(("$sort", arg)) = stage_at(pipeline, i) {
        // `find_in` uses a bounded TopK. That is a win only when a later
        // `$limit` keeps the heap small. With no limit, the plain in-memory
        // `sort_by` used by `run` is faster than a full-size heap.
        let mut next = i + 1;
        if matches!(stage_at(pipeline, next), Some(("$skip", _))) {
            next += 1;
        }
        if matches!(stage_at(pipeline, next), Some(("$limit", _))) {
            plan.sort = parse_sort(arg)?;
            i += 1;
        }
    }
    if let Some(("$skip", arg)) = stage_at(pipeline, i) {
        plan.skip = as_usize(arg, "$skip")?;
        i += 1;
    }
    if let Some(("$limit", arg)) = stage_at(pipeline, i) {
        plan.limit = Some(as_usize(arg, "$limit")?);
        i += 1;
    }
    if let Some(("$project", arg)) = stage_at(pipeline, i) {
        if let Some(projection) = simple_projection(arg) {
            plan.projection = Some(projection);
            i += 1;
        }
    }

    plan.next = i;
    Ok(plan)
}

fn stage_at(pipeline: &[Value], index: usize) -> Option<(&str, &Value)> {
    let obj = pipeline.get(index)?.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    let (name, arg) = obj.iter().next()?;
    name.starts_with('$').then_some((name.as_str(), arg))
}

fn simple_projection(spec: &Value) -> Option<Value> {
    let obj = spec.as_object()?;
    for (field, value) in obj {
        // `find` projections are top-level only.
        if field.contains('.') || field.starts_with('$') {
            return None;
        }
        let simple = match value {
            Value::Bool(_) => true,
            Value::Number(n) => n.as_f64().is_some_and(|v| v == 0.0 || v == 1.0),
            _ => false,
        };
        if !simple {
            return None;
        }
    }
    // `find` has a long-standing special case: `{_id: 1}` alone is treated as
    // an exclusion projection. Aggregation follows MongoDB and returns only
    // `_id`. Do not push that form (or mixed `_id` inclusion + exclusions).
    let non_id_inclusion = obj
        .iter()
        .any(|(field, value)| field != "_id" && is_inclusion(value));
    let id_inclusion = obj.get("_id").is_some_and(is_inclusion);
    if id_inclusion && !non_id_inclusion {
        return None;
    }
    Some(spec.clone())
}

fn count_field_name(arg: &Value) -> Result<String> {
    match arg {
        Value::String(s) if !s.is_empty() => Ok(s.clone()),
        Value::Null => Ok("count".to_string()),
        _ => Err(Error::Query(
            "$count requires a non-empty string field name".into(),
        )),
    }
}

/// Runs a pipeline over already selected documents.
pub fn run(mut docs: Vec<Value>, pipeline: &[Value], max_docs: usize) -> Result<Vec<Value>> {
    for stage in pipeline {
        let (name, arg) = parse_stage(stage)?;
        match name {
            "$match" => docs.retain(|doc| matches(doc, arg)),
            "$project" => {
                docs = docs
                    .into_iter()
                    .map(|doc| project(&doc, arg))
                    .collect::<Result<Vec<_>>>()?;
            }
            "$addFields" | "$set" => {
                docs = docs
                    .into_iter()
                    .map(|doc| add_fields(&doc, arg))
                    .collect::<Result<Vec<_>>>()?;
            }
            "$unset" => {
                docs = docs
                    .into_iter()
                    .map(|doc| unset_fields(doc, arg))
                    .collect::<Result<Vec<_>>>()?;
            }
            "$unwind" => docs = unwind(docs, arg)?,
            "$sort" => {
                let sort = parse_sort(arg)?;
                docs.sort_by(|a, b| compare_docs(a, b, &sort));
            }
            "$skip" => {
                let n = as_usize(arg, "$skip")?.min(docs.len());
                docs.drain(..n);
            }
            "$limit" => {
                let n = as_usize(arg, "$limit")?;
                docs.truncate(n);
            }
            "$group" => docs = group(docs, arg)?,
            "$count" => {
                if docs.is_empty() {
                    docs.clear();
                } else {
                    let field = count_field_name(arg)?;
                    let count = docs.len();
                    docs = vec![Value::Object(Map::from_iter([(
                        field,
                        Value::from(count as u64),
                    )]))];
                }
            }
            other => {
                return Err(Error::Query(format!(
                    "unsupported aggregation stage `{other}`"
                )));
            }
        }
        if docs.len() > max_docs {
            return Err(Error::Query(format!(
                "aggregation intermediate result exceeds {max_docs} documents"
            )));
        }
    }
    Ok(docs)
}

fn parse_stage(stage: &Value) -> Result<(&str, &Value)> {
    let obj = stage
        .as_object()
        .ok_or_else(|| Error::Query("aggregation stage must be an object".into()))?;
    if obj.len() != 1 {
        return Err(Error::Query(
            "aggregation stage must contain exactly one operator".into(),
        ));
    }
    let (name, arg) = obj.iter().next().expect("one entry");
    if !name.starts_with('$') {
        return Err(Error::Query(format!("invalid aggregation stage `{name}`")));
    }
    Ok((name.as_str(), arg))
}

fn as_usize(value: &Value, stage: &str) -> Result<usize> {
    value
        .as_u64()
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| Error::Query(format!("{stage} requires a non-negative integer")))
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|v| v != 0.0),
        Value::Null => false,
        _ => true,
    }
}

fn is_exclusion(value: &Value) -> bool {
    matches!(value, Value::Bool(false)) || value.as_f64().is_some_and(|n| n == 0.0)
}

fn is_inclusion(value: &Value) -> bool {
    matches!(value, Value::Bool(true)) || value.as_f64().is_some_and(|n| n == 1.0)
}

fn project(doc: &Value, spec: &Value) -> Result<Value> {
    let spec = spec
        .as_object()
        .ok_or_else(|| Error::Query("$project requires an object".into()))?;
    let include = spec.values().any(|v| !is_exclusion(v));

    if include {
        let mut out = Value::Object(Map::new());
        let include_id = spec.get("_id").map(|v| !is_exclusion(v)).unwrap_or(true);
        if include_id {
            if let Some(id) = doc.get("_id") {
                out.as_object_mut()
                    .expect("object")
                    .insert("_id".to_string(), id.clone());
            }
        }
        for (field, value) in spec {
            if field == "_id" {
                if is_exclusion(value) || is_inclusion(value) {
                    continue;
                }
                let value = eval_expr(doc, value)?;
                path::set(&mut out, field, value)?;
                continue;
            }
            if is_exclusion(value) {
                continue;
            }
            let value = if is_inclusion(value) {
                path::get(doc, field).cloned().unwrap_or(Value::Null)
            } else {
                eval_expr(doc, value)?
            };
            path::set(&mut out, field, value)?;
        }
        Ok(out)
    } else {
        let mut out = doc.clone();
        for (field, value) in spec {
            if is_exclusion(value) {
                path::unset(&mut out, field);
            }
        }
        Ok(out)
    }
}

fn add_fields(doc: &Value, spec: &Value) -> Result<Value> {
    let spec = spec
        .as_object()
        .ok_or_else(|| Error::Query("$addFields requires an object".into()))?;
    let mut out = doc.clone();
    for (field, expr) in spec {
        let value = eval_expr(doc, expr)?;
        path::set(&mut out, field, value)?;
    }
    Ok(out)
}

fn unset_fields(mut doc: Value, spec: &Value) -> Result<Value> {
    match spec {
        Value::String(field) => {
            path::unset(&mut doc, field);
        }
        Value::Array(fields) => {
            for field in fields {
                let field = field.as_str().ok_or_else(|| {
                    Error::Query("$unset array entries must be field names".into())
                })?;
                path::unset(&mut doc, field);
            }
        }
        _ => {
            return Err(Error::Query(
                "$unset requires a field name or array of field names".into(),
            ));
        }
    }
    Ok(doc)
}

pub(crate) fn parse_sort(spec: &Value) -> Result<Vec<(String, i32)>> {
    match spec {
        Value::Object(fields) => {
            let mut out = Vec::with_capacity(fields.len());
            for (field, dir) in fields {
                let dir = dir
                    .as_i64()
                    .filter(|d| *d == 1 || *d == -1)
                    .ok_or_else(|| Error::Query("$sort direction must be 1 or -1".into()))?;
                out.push((field.clone(), dir as i32));
            }
            Ok(out)
        }
        Value::Array(fields) => {
            let mut out = Vec::with_capacity(fields.len());
            for pair in fields {
                let pair = pair
                    .as_array()
                    .filter(|p| p.len() == 2)
                    .ok_or_else(|| Error::Query("$sort array entries must be [field, 1]".into()))?;
                let field = pair[0]
                    .as_str()
                    .ok_or_else(|| Error::Query("$sort field must be a string".into()))?;
                let dir = pair[1]
                    .as_i64()
                    .filter(|d| *d == 1 || *d == -1)
                    .ok_or_else(|| Error::Query("$sort direction must be 1 or -1".into()))?;
                out.push((field.to_string(), dir as i32));
            }
            Ok(out)
        }
        _ => Err(Error::Query(
            "$sort requires an object or array of [field, direction] pairs".into(),
        )),
    }
}

fn unwind(docs: Vec<Value>, spec: &Value) -> Result<Vec<Value>> {
    let (field, preserve, index_field) = match spec {
        Value::String(s) => (s.as_str(), false, None),
        Value::Object(obj) => {
            let field = obj
                .get("path")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Query("$unwind requires `path`".into()))?;
            let preserve = obj
                .get("preserveNullAndEmptyArrays")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let index_field = obj
                .get("includeArrayIndex")
                .and_then(Value::as_str)
                .map(str::to_string);
            (field, preserve, index_field)
        }
        _ => {
            return Err(Error::Query(
                "$unwind requires a field path or an object".into(),
            ));
        }
    };
    let field = field.strip_prefix('$').unwrap_or(field);
    let field_path = field.to_string();

    let mut out = Vec::new();
    for doc in docs {
        match path::get(&doc, &field_path).cloned() {
            Some(Value::Array(items)) if !items.is_empty() => {
                for (i, item) in items.into_iter().enumerate() {
                    let mut doc = doc.clone();
                    path::set(&mut doc, &field_path, item)?;
                    if let Some(index_field) = &index_field {
                        path::set(&mut doc, index_field, Value::from(i as u64))?;
                    }
                    out.push(doc);
                }
            }
            Some(Value::Array(_)) => {
                if preserve {
                    let mut doc = doc;
                    path::unset(&mut doc, &field_path);
                    out.push(doc);
                }
            }
            Some(Value::Null) | None => {
                if preserve {
                    out.push(doc);
                }
            }
            Some(_) => {
                let mut doc = doc;
                if let Some(index_field) = &index_field {
                    path::set(&mut doc, index_field, Value::from(0u64))?;
                }
                out.push(doc);
            }
        }
    }
    Ok(out)
}

fn group(docs: Vec<Value>, spec: &Value) -> Result<Vec<Value>> {
    let spec = spec
        .as_object()
        .ok_or_else(|| Error::Query("$group requires an object".into()))?;
    let id_expr = spec.get("_id").cloned().unwrap_or(Value::Null);

    let mut accumulators = Vec::new();
    for (field, value) in spec {
        if field == "_id" {
            continue;
        }
        let accumulator = value
            .as_object()
            .filter(|obj| obj.len() == 1)
            .and_then(|obj| obj.iter().next())
            .ok_or_else(|| {
                Error::Query(format!(
                    "group field `{field}` must contain one accumulator operator"
                ))
            })?;
        let op = accumulator.0.as_str();
        let arg = accumulator.1.clone();
        if !matches!(
            op,
            "$sum"
                | "$avg"
                | "$min"
                | "$max"
                | "$first"
                | "$last"
                | "$push"
                | "$addToSet"
                | "$count"
        ) {
            return Err(Error::Query(format!(
                "unsupported group accumulator `{op}`"
            )));
        }
        accumulators.push((field.clone(), op.to_string(), arg));
    }

    #[derive(Default)]
    struct Group {
        id: Value,
        values: Vec<Accumulator>,
    }

    let mut groups: BTreeMap<String, Group> = BTreeMap::new();
    for doc in docs {
        let id = eval_expr(&doc, &id_expr)?;
        let key = serde_json::to_string(&id)?;
        let group = groups.entry(key).or_insert_with(|| Group {
            id: id.clone(),
            values: accumulators
                .iter()
                .map(|(_, op, _)| Accumulator::new(op))
                .collect(),
        });
        for ((_, op, arg), state) in accumulators.iter().zip(group.values.iter_mut()) {
            let value = if op == "$count" {
                Value::Null
            } else {
                eval_expr(&doc, arg)?
            };
            state.update(op, &value)?;
        }
    }

    groups
        .into_values()
        .map(|group| {
            let mut out = Map::new();
            out.insert("_id".to_string(), group.id);
            for ((field, _, _), state) in accumulators.iter().zip(group.values) {
                out.insert(field.clone(), state.finish());
            }
            Ok(Value::Object(out))
        })
        .collect()
}

enum Accumulator {
    Sum(f64),
    Avg {
        sum: f64,
        count: u64,
    },
    Min(Option<Value>),
    Max(Option<Value>),
    First {
        seen: bool,
        value: Value,
    },
    Last(Value),
    Push(Vec<Value>),
    AddToSet {
        seen: HashSet<String>,
        values: Vec<Value>,
    },
    Count(u64),
}

impl Accumulator {
    fn new(op: &str) -> Self {
        match op {
            "$sum" => Self::Sum(0.0),
            "$avg" => Self::Avg { sum: 0.0, count: 0 },
            "$min" => Self::Min(None),
            "$max" => Self::Max(None),
            "$first" => Self::First {
                seen: false,
                value: Value::Null,
            },
            "$last" => Self::Last(Value::Null),
            "$push" => Self::Push(Vec::new()),
            "$addToSet" => Self::AddToSet {
                seen: HashSet::new(),
                values: Vec::new(),
            },
            "$count" => Self::Count(0),
            _ => unreachable!("validated accumulator"),
        }
    }

    fn update(&mut self, op: &str, value: &Value) -> Result<()> {
        match (self, op) {
            (Self::Sum(sum), "$sum") => {
                if let Some(n) = value.as_f64() {
                    *sum += n;
                }
            }
            (Self::Avg { sum, count }, "$avg") => {
                if let Some(n) = value.as_f64() {
                    *sum += n;
                    *count += 1;
                }
            }
            (Self::Min(current), "$min") => {
                if current
                    .as_ref()
                    .map(|old| compare_value(value, old) == Ordering::Less)
                    .unwrap_or(true)
                {
                    *current = Some(value.clone());
                }
            }
            (Self::Max(current), "$max") => {
                if current
                    .as_ref()
                    .map(|old| compare_value(value, old) == Ordering::Greater)
                    .unwrap_or(true)
                {
                    *current = Some(value.clone());
                }
            }
            (
                Self::First {
                    seen,
                    value: current,
                },
                "$first",
            ) => {
                if !*seen {
                    *current = value.clone();
                    *seen = true;
                }
            }
            (Self::Last(current), "$last") => *current = value.clone(),
            (Self::Push(values), "$push") => values.push(value.clone()),
            (Self::AddToSet { seen, values }, "$addToSet") => {
                let key = serde_json::to_string(value)?;
                if seen.insert(key) {
                    values.push(value.clone());
                }
            }
            (Self::Count(count), "$count") => *count += 1,
            _ => {
                return Err(Error::Query("internal group accumulator mismatch".into()));
            }
        }
        Ok(())
    }

    fn finish(self) -> Value {
        match self {
            Self::Sum(sum) => number_value(sum),
            Self::Avg { sum, count } => {
                if count == 0 {
                    Value::Null
                } else {
                    number_value(sum / count as f64)
                }
            }
            Self::Min(value) | Self::Max(value) => value.unwrap_or(Value::Null),
            Self::Last(value) => value,
            Self::First { seen, value } => {
                if seen {
                    value
                } else {
                    Value::Null
                }
            }
            Self::Push(values) | Self::AddToSet { values, .. } => Value::Array(values),
            Self::Count(count) => Value::from(count),
        }
    }
}

fn number_value(n: f64) -> Value {
    if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
        Value::from(n as i64)
    } else {
        Value::from(n)
    }
}

fn compare_value(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x
            .as_f64()
            .partial_cmp(&y.as_f64())
            .unwrap_or(Ordering::Equal),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        _ => date::cmp(a, b).unwrap_or(Ordering::Equal),
    }
}

fn eval_expr(doc: &Value, expr: &Value) -> Result<Value> {
    match expr {
        Value::String(s) if s == "$$ROOT" || s == "$$CURRENT" => Ok(doc.clone()),
        Value::String(s) if s.starts_with('$') => {
            Ok(path::get(doc, &s[1..]).cloned().unwrap_or(Value::Null))
        }
        Value::Array(items) => items
            .iter()
            .map(|item| eval_expr(doc, item))
            .collect::<Result<Vec<_>>>()
            .map(Value::Array),
        Value::Object(obj)
            if obj.len() == 1 && obj.keys().next().is_some_and(|k| k.starts_with('$')) =>
        {
            eval_operator(doc, obj)
        }
        Value::Object(obj) => {
            let mut out = Map::new();
            for (key, value) in obj {
                out.insert(key.clone(), eval_expr(doc, value)?);
            }
            Ok(Value::Object(out))
        }
        _ => Ok(expr.clone()),
    }
}

fn eval_operator(doc: &Value, obj: &Map<String, Value>) -> Result<Value> {
    let (op, arg) = obj.iter().next().expect("one entry");
    let args = || -> Result<Vec<Value>> {
        let values = arg
            .as_array()
            .ok_or_else(|| Error::Query(format!("{op} requires an array of arguments")))?;
        values.iter().map(|v| eval_expr(doc, v)).collect()
    };

    match op.as_str() {
        "$literal" => Ok(arg.clone()),
        "$ifNull" => {
            let values = args()?;
            if values.len() != 2 {
                return Err(Error::Query("$ifNull requires two arguments".into()));
            }
            Ok(if values[0].is_null() {
                values[1].clone()
            } else {
                values[0].clone()
            })
        }
        "$add" | "$subtract" | "$multiply" | "$divide" => {
            let values = args()?;
            if op == "$add" {
                let sum = values.iter().filter_map(Value::as_f64).sum::<f64>();
                return Ok(number_value(sum));
            }
            if values.len() != 2 {
                return Err(Error::Query(format!("{op} requires two arguments")));
            }
            let a = values[0]
                .as_f64()
                .ok_or_else(|| Error::Query(format!("{op} requires numbers")))?;
            let b = values[1]
                .as_f64()
                .ok_or_else(|| Error::Query(format!("{op} requires numbers")))?;
            let result = match op.as_str() {
                "$subtract" => a - b,
                "$multiply" => a * b,
                "$divide" => {
                    if b == 0.0 {
                        return Err(Error::Query("$divide by zero".into()));
                    }
                    a / b
                }
                _ => unreachable!(),
            };
            Ok(number_value(result))
        }
        "$concat" => {
            let values = args()?;
            let mut out = String::new();
            for value in values {
                let Some(s) = value.as_str() else {
                    return Ok(Value::Null);
                };
                out.push_str(s);
            }
            Ok(Value::String(out))
        }
        "$cond" => {
            let (condition, yes, no) = match arg {
                Value::Array(items) if items.len() == 3 => {
                    (eval_expr(doc, &items[0])?, &items[1], &items[2])
                }
                Value::Object(obj) => {
                    let condition = obj
                        .get("if")
                        .ok_or_else(|| Error::Query("$cond requires `if`".into()))?;
                    let yes = obj
                        .get("then")
                        .ok_or_else(|| Error::Query("$cond requires `then`".into()))?;
                    let no = obj
                        .get("else")
                        .ok_or_else(|| Error::Query("$cond requires `else`".into()))?;
                    (eval_expr(doc, condition)?, yes, no)
                }
                _ => return Err(Error::Query("$cond requires three arguments".into())),
            };
            if truthy(&condition) {
                eval_expr(doc, yes)
            } else {
                eval_expr(doc, no)
            }
        }
        "$eq" | "$ne" | "$gt" | "$gte" | "$lt" | "$lte" => {
            let values = args()?;
            if values.len() != 2 {
                return Err(Error::Query(format!("{op} requires two arguments")));
            }
            let ord = compare_value(&values[0], &values[1]);
            let result = match op.as_str() {
                "$eq" => ord == Ordering::Equal,
                "$ne" => ord != Ordering::Equal,
                "$gt" => ord == Ordering::Greater,
                "$gte" => ord != Ordering::Less,
                "$lt" => ord == Ordering::Less,
                "$lte" => ord != Ordering::Greater,
                _ => unreachable!(),
            };
            Ok(Value::Bool(result))
        }
        _ => Err(Error::Query(format!(
            "unsupported aggregation expression `{op}`"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn group_and_project() {
        let docs = vec![
            json!({"_id": "1", "tenant": "a", "amount": 10}),
            json!({"_id": "2", "tenant": "a", "amount": 20}),
            json!({"_id": "3", "tenant": "b", "amount": 5}),
        ];
        let pipeline = vec![
            json!({"$match": {"amount": {"$gte": 10}}}),
            json!({"$group": {
                "_id": "$tenant",
                "total": {"$sum": "$amount"},
                "avg": {"$avg": "$amount"},
                "n": {"$sum": 1},
                "ids": {"$push": "$_id"}
            }}),
            json!({"$sort": {"total": -1}}),
            json!({"$project": {"_id": 0, "tenant": "$_id", "total": 1, "n": 1}}),
        ];
        let out = run(docs, &pipeline, 100).unwrap();
        assert_eq!(out, vec![json!({"tenant": "a", "total": 30, "n": 2})]);
    }

    #[test]
    fn add_fields_unwind_and_count() {
        let docs = vec![
            json!({"_id": "1", "tags": ["a", "b"], "n": 2}),
            json!({"_id": "2", "tags": [], "n": 3}),
        ];
        let pipeline = vec![
            json!({"$addFields": {"double": {"$multiply": ["$n", 2]}}}),
            json!({"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": true}}),
            json!({"$count": "rows"}),
        ];
        let out = run(docs, &pipeline, 100).unwrap();
        assert_eq!(out, vec![json!({"rows": 3})]);
    }

    #[test]
    fn rejects_unknown_stage() {
        let err = run(vec![], &[json!({"$lookup": {}})], 100).unwrap_err();
        assert!(err.to_string().contains("unsupported aggregation stage"));
    }
}
