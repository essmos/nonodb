//! Read-only aggregation pipeline.
//!
//! The first version is intentionally bounded: it operates on one collection,
//! materializes intermediate results, and supports the common reporting stages.
//! It never writes documents or indexes.

use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::sync::Arc;

use bigdecimal::num_traits::Zero;
use bigdecimal::{BigDecimal, FromPrimitive};
use rand::seq::SliceRandom;
use serde_json::{Map, Value};

use crate::bson_types;
use crate::collection::compare_docs;
use crate::date;
use crate::error::{Error, Result};
use crate::numeric;
use crate::query::{matches, path};

/// Default maximum number of documents allowed in an intermediate aggregation
/// result. Override with `NONODB_AGGREGATE_MAX_DOCS`.
pub const MAX_AGGREGATE_DOCS: usize = 100_000;

/// Effective aggregation document budget. A missing, invalid, or zero value
/// falls back to [`MAX_AGGREGATE_DOCS`].
pub fn max_docs() -> usize {
    std::env::var("NONODB_AGGREGATE_MAX_DOCS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(MAX_AGGREGATE_DOCS)
}

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

    let mut filters = Vec::new();
    while let Some(("$match", arg)) = stage_at(pipeline, i) {
        filters.push(arg.clone());
        i += 1;
    }
    if !filters.is_empty() {
        plan.filter = if filters.len() == 1 {
            filters.pop().expect("one filter")
        } else {
            Value::Object(Map::from_iter([(
                "$and".to_string(),
                Value::Array(filters),
            )]))
        };
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

/// Detects a terminal `$count` preceded only by stages that preserve document
/// cardinality and cannot fail after validation. The caller may skip those
/// stages and count the selected documents directly.
pub(crate) fn terminal_count_skippable(pipeline: &[Value]) -> Result<Option<String>> {
    let Some((last_name, last_arg)) = pipeline
        .last()
        .and_then(|stage| stage_at(std::slice::from_ref(stage), 0))
    else {
        return Ok(None);
    };
    if last_name != "$count" {
        return Ok(None);
    }
    for stage in &pipeline[..pipeline.len() - 1] {
        let (name, arg) = parse_stage(stage)?;
        match name {
            "$project" => {
                if simple_projection(arg).is_none() {
                    return Ok(None);
                }
            }
            "$unset" => validate_unset(arg)?,
            "$sort" => {
                parse_sort(arg)?;
            }
            _ => return Ok(None),
        }
    }
    Ok(Some(count_field_name(last_arg)?))
}

fn validate_unset(spec: &Value) -> Result<()> {
    match spec {
        Value::String(_) => Ok(()),
        Value::Array(fields) => fields.iter().try_for_each(|field| {
            field
                .as_str()
                .map(|_| ())
                .ok_or_else(|| Error::Query("$unset array entries must be field names".into()))
        }),
        _ => Err(Error::Query(
            "$unset requires a field name or array of field names".into(),
        )),
    }
}

struct SortEntry {
    doc: Value,
    sort: Arc<Vec<(String, i32)>>,
    seq: usize,
}

impl PartialEq for SortEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for SortEntry {}

impl PartialOrd for SortEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_docs(&self.doc, &other.doc, &self.sort).then_with(|| self.seq.cmp(&other.seq))
    }
}

/// Bounded sort with an optional skip/limit. The heap keeps only the best
/// `skip + limit` documents, so a `$sort` followed by a small `$limit` does
/// not sort or retain the whole intermediate result.
fn sort_top_k(docs: Vec<Value>, sort: Vec<(String, i32)>, skip: usize, limit: usize) -> Vec<Value> {
    let keep = skip.saturating_add(limit);
    if keep == 0 {
        return Vec::new();
    }
    if keep >= docs.len() {
        let mut docs = docs;
        docs.sort_by(|a, b| compare_docs(a, b, &sort));
        return docs.into_iter().skip(skip).take(limit).collect();
    }
    let sort = Arc::new(sort);
    let mut heap = BinaryHeap::with_capacity(keep + 1);
    for (seq, doc) in docs.into_iter().enumerate() {
        heap.push(SortEntry {
            doc,
            sort: Arc::clone(&sort),
            seq,
        });
        if heap.len() > keep {
            heap.pop();
        }
    }
    heap.into_sorted_vec()
        .into_iter()
        .skip(skip)
        .take(limit)
        .map(|entry| entry.doc)
        .collect()
}

fn is_doc_local_stage(name: &str) -> bool {
    matches!(
        name,
        "$match" | "$project" | "$addFields" | "$set" | "$unset"
    )
}

/// Host callback used by stages that need access to another collection.
pub(crate) trait AggregateHost {
    fn lookup(&self, docs: Vec<Value>, spec: &Value) -> Result<Vec<Value>>;
}

/// Runs a pipeline over already selected documents.
pub fn run(docs: Vec<Value>, pipeline: &[Value], max_docs: usize) -> Result<Vec<Value>> {
    run_with_host(docs, pipeline, max_docs, None)
}

pub(crate) fn run_with_host(
    mut docs: Vec<Value>,
    pipeline: &[Value],
    max_docs: usize,
    host: Option<&dyn AggregateHost>,
) -> Result<Vec<Value>> {
    // A terminal `$count` does not need the transformed documents. Apply the
    // cardinality-preserving stages one document at a time and keep only the
    // counter; sorting is irrelevant to the result and is validated separately.
    if let Some(field) = terminal_count_skippable(pipeline)? {
        let mut count = 0usize;
        for mut doc in docs {
            for stage in &pipeline[..pipeline.len() - 1] {
                let (name, arg) = parse_stage(stage)?;
                doc = match name {
                    "$project" => project(doc, arg)?,
                    "$unset" => unset_fields(doc, arg)?,
                    "$sort" => doc,
                    _ => unreachable!("validated cardinality-preserving stage"),
                };
            }
            count += 1;
        }
        if count == 0 {
            return Ok(Vec::new());
        }
        return Ok(vec![Value::Object(Map::from_iter([(
            field,
            Value::from(count as u64),
        )]))]);
    }

    let mut i = 0;
    while i < pipeline.len() {
        let (name, arg) = parse_stage(&pipeline[i])?;

        // Fuse consecutive document-local stages into one pass. This avoids
        // materializing a separate vector for every `$match`/`$project`/
        // `$addFields`/`$unset` stage.
        if is_doc_local_stage(name) {
            let start = i;
            let mut end = i + 1;
            while end < pipeline.len() {
                let (next, _) = parse_stage(&pipeline[end])?;
                if !is_doc_local_stage(next) {
                    break;
                }
                end += 1;
            }
            let mut out = Vec::with_capacity(docs.len());
            for mut doc in docs {
                let mut keep = true;
                for stage in &pipeline[start..end] {
                    let (name, arg) = parse_stage(stage)?;
                    doc = match name {
                        "$match" => {
                            keep = matches(&doc, arg);
                            doc
                        }
                        "$project" => project(doc, arg)?,
                        "$addFields" | "$set" => add_fields(doc, arg)?,
                        "$unset" => unset_fields(doc, arg)?,
                        _ => unreachable!("document-local stage"),
                    };
                    if !keep {
                        break;
                    }
                }
                if keep {
                    out.push(doc);
                }
            }
            docs = out;
            i = end;
        } else {
            match name {
                "$unwind" => {
                    docs = unwind(docs, arg)?;
                    i += 1;
                }
                "$sort" => {
                    let sort = parse_sort(arg)?;
                    let mut next = i + 1;
                    let skip = if let Some(("$skip", value)) = stage_at(pipeline, next) {
                        next += 1;
                        as_usize(value, "$skip")?
                    } else {
                        0
                    };
                    if let Some(("$limit", value)) = stage_at(pipeline, next) {
                        let limit = as_usize(value, "$limit")?;
                        docs = sort_top_k(docs, sort, skip, limit);
                        i = next + 1;
                    } else if skip > 0 {
                        let mut sorted = docs;
                        sorted.sort_by(|a, b| compare_docs(a, b, &sort));
                        docs = sorted.into_iter().skip(skip).collect();
                        i = next;
                    } else {
                        docs.sort_by(|a, b| compare_docs(a, b, &sort));
                        i += 1;
                    }
                }
                "$skip" => {
                    let n = as_usize(arg, "$skip")?.min(docs.len());
                    docs.drain(..n);
                    i += 1;
                }
                "$limit" => {
                    let n = as_usize(arg, "$limit")?;
                    docs.truncate(n);
                    i += 1;
                }
                "$replaceRoot" => {
                    let expression = arg
                        .as_object()
                        .and_then(|obj| obj.get("newRoot"))
                        .ok_or_else(|| Error::Query("$replaceRoot requires `newRoot`".into()))?;
                    docs = docs
                        .into_iter()
                        .map(|doc| match eval_expr(&doc, expression)? {
                            value @ Value::Object(_) => Ok(value),
                            _ => Err(Error::Query(
                                "$replaceRoot expression must evaluate to an object".into(),
                            )),
                        })
                        .collect::<Result<Vec<_>>>()?;
                    i += 1;
                }
                "$replaceWith" => {
                    docs = docs
                        .into_iter()
                        .map(|doc| match eval_expr(&doc, arg)? {
                            value @ Value::Object(_) => Ok(value),
                            _ => Err(Error::Query(
                                "$replaceWith expression must evaluate to an object".into(),
                            )),
                        })
                        .collect::<Result<Vec<_>>>()?;
                    i += 1;
                }
                "$sample" => {
                    let size = arg
                        .as_object()
                        .and_then(|obj| obj.get("size"))
                        .ok_or_else(|| Error::Query("$sample requires `size`".into()))
                        .and_then(|size| as_usize(size, "$sample"))?;
                    docs.shuffle(&mut rand::thread_rng());
                    docs.truncate(size);
                    i += 1;
                }
                "$sortByCount" => {
                    let group_arg = Value::Object(Map::from_iter([
                        ("_id".to_string(), arg.clone()),
                        (
                            "count".to_string(),
                            Value::Object(Map::from_iter([("$sum".to_string(), Value::from(1))])),
                        ),
                    ]));
                    docs = group(docs, &group_arg)?;
                    let sort = parse_sort(&Value::Object(Map::from_iter([
                        ("count".to_string(), Value::from(-1)),
                        ("_id".to_string(), Value::from(1)),
                    ])))?;
                    docs.sort_by(|a, b| compare_docs(a, b, &sort));
                    i += 1;
                }
                "$lookup" => {
                    let host = host.ok_or_else(|| {
                        Error::Query("$lookup requires a collection-aware host".into())
                    })?;
                    docs = host.lookup(docs, arg)?;
                    i += 1;
                }
                "$facet" => {
                    let Some(facets) = arg.as_object() else {
                        return Err(Error::Query("$facet requires an object".into()));
                    };
                    let mut out = Map::new();
                    for (name, pipeline) in facets {
                        let Some(pipeline) = pipeline.as_array() else {
                            return Err(Error::Query("$facet pipelines must be arrays".into()));
                        };
                        out.insert(
                            name.clone(),
                            Value::Array(run_with_host(docs.clone(), pipeline, max_docs, host)?),
                        );
                    }
                    docs = vec![Value::Object(out)];
                    i += 1;
                }
                "$bucket" => {
                    docs = bucket(docs, arg)?;
                    i += 1;
                }
                "$bucketAuto" => {
                    docs = bucket_auto(docs, arg)?;
                    i += 1;
                }
                "$group" => {
                    docs = group(docs, arg)?;
                    i += 1;
                }
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
                    i += 1;
                }
                other => {
                    return Err(Error::Query(format!(
                        "unsupported aggregation stage `{other}`"
                    )));
                }
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

pub(crate) fn truthy(value: &Value) -> bool {
    match value {
        Value::Bool(b) => *b,
        Value::Number(_) => numeric::parse_value(value).is_some_and(|n| {
            !matches!(n.value(), numeric::Numeric::Finite { len: 1, digits, .. } if digits[0] == 0)
        }),
        Value::Null => false,
        _ => {
            if let Some(n) = numeric::parse_value(value) {
                !matches!(n.value(), numeric::Numeric::Finite { len: 1, digits, .. } if digits[0] == 0)
            } else {
                true
            }
        }
    }
}

fn is_exclusion(value: &Value) -> bool {
    matches!(value, Value::Bool(false)) || value.as_f64().is_some_and(|n| n == 0.0)
}

fn is_inclusion(value: &Value) -> bool {
    matches!(value, Value::Bool(true)) || value.as_f64().is_some_and(|n| n == 1.0)
}

fn project(doc: Value, spec: &Value) -> Result<Value> {
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
                let value = eval_expr(&doc, value)?;
                path::set(&mut out, field, value)?;
                continue;
            }
            if is_exclusion(value) {
                continue;
            }
            let value = if is_inclusion(value) {
                path::get(&doc, field).cloned().unwrap_or(Value::Null)
            } else {
                eval_expr(&doc, value)?
            };
            path::set(&mut out, field, value)?;
        }
        Ok(out)
    } else {
        let mut out = doc;
        for (field, value) in spec {
            if is_exclusion(value) {
                path::unset(&mut out, field);
            }
        }
        Ok(out)
    }
}

fn add_fields(mut doc: Value, spec: &Value) -> Result<Value> {
    let spec = spec
        .as_object()
        .ok_or_else(|| Error::Query("$addFields requires an object".into()))?;
    // Evaluate every expression against the original document first; later
    // expressions must not observe fields added by earlier entries.
    let mut updates = Vec::with_capacity(spec.len());
    for (field, expr) in spec {
        updates.push((field, eval_expr(&doc, expr)?));
    }
    for (field, value) in updates {
        path::set(&mut doc, field, value)?;
    }
    Ok(doc)
}

fn unset_fields(mut doc: Value, spec: &Value) -> Result<Value> {
    validate_unset(spec)?;
    match spec {
        Value::String(field) => {
            path::unset(&mut doc, field);
        }
        Value::Array(fields) => {
            for field in fields {
                path::unset(&mut doc, field.as_str().expect("validated field"));
            }
        }
        _ => unreachable!("validated $unset"),
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

pub(crate) struct GroupBuilder {
    id_expr: Value,
    accumulators: Vec<(String, String, Value)>,
    groups: BTreeMap<String, GroupState>,
}

#[derive(Default)]
struct GroupState {
    id: Value,
    values: Vec<Accumulator>,
}

impl GroupBuilder {
    pub(crate) fn new(spec: &Value) -> Result<Self> {
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
        Ok(Self {
            id_expr,
            accumulators,
            groups: BTreeMap::new(),
        })
    }

    /// True when the result does not depend on input document order.
    /// `$first`, `$last`, and `$push` are order-sensitive; `$addToSet` keeps
    /// set semantics but its array order follows discovery, so it stays on the
    /// sequential path as well.
    pub(crate) fn order_independent(&self) -> bool {
        self.accumulators
            .iter()
            .all(|(_, op, _)| matches!(op.as_str(), "$sum" | "$avg" | "$min" | "$max" | "$count"))
    }

    pub(crate) fn update(&mut self, doc: Value) -> Result<()> {
        let id = eval_expr(&doc, &self.id_expr)?;
        let key = serde_json::to_string(&id)?;
        let group = self.groups.entry(key).or_insert_with(|| GroupState {
            id: id.clone(),
            values: self
                .accumulators
                .iter()
                .map(|(_, op, _)| Accumulator::new(op))
                .collect(),
        });
        for ((_, op, arg), state) in self.accumulators.iter().zip(group.values.iter_mut()) {
            let value = if op == "$count" {
                Value::Null
            } else {
                eval_expr(&doc, arg)?
            };
            state.update(op, &value)?;
        }
        Ok(())
    }

    /// Merges another partial builder into this one. Only used for
    /// order-independent accumulators, so group iteration order is irrelevant.
    pub(crate) fn merge(&mut self, other: GroupBuilder) -> Result<()> {
        for (key, other_group) in other.groups {
            match self.groups.entry(key) {
                Entry::Vacant(slot) => {
                    slot.insert(other_group);
                }
                Entry::Occupied(mut slot) => {
                    let current = slot.get_mut();
                    if current.values.len() != other_group.values.len() {
                        return Err(Error::Query("internal group accumulator mismatch".into()));
                    }
                    for (left, right) in current.values.iter_mut().zip(other_group.values) {
                        left.merge(right)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Vec<Value>> {
        self.groups
            .into_values()
            .map(|group| {
                let mut out = Map::new();
                out.insert("_id".to_string(), group.id);
                for ((field, _, _), state) in self.accumulators.iter().zip(group.values) {
                    out.insert(field.clone(), state.finish()?);
                }
                Ok(Value::Object(out))
            })
            .collect()
    }
}

const BUCKET_KEY_FIELD: &str = "__nonodb_bucket_key__";

fn bucket_output_spec(output: Option<&Value>) -> Result<Value> {
    let mut spec = Map::new();
    spec.insert(
        "_id".to_string(),
        Value::String(format!("${BUCKET_KEY_FIELD}")),
    );
    match output {
        Some(output) => {
            let Some(output) = output.as_object() else {
                return Err(Error::Query("bucket output must be an object".into()));
            };
            for (field, value) in output {
                if field == "_id" {
                    return Err(Error::Query("bucket output cannot contain `_id`".into()));
                }
                spec.insert(field.clone(), value.clone());
            }
        }
        None => {
            spec.insert(
                "count".to_string(),
                Value::Object(Map::from_iter([("$sum".to_string(), Value::from(1))])),
            );
        }
    }
    Ok(Value::Object(spec))
}

fn with_bucket_key(mut doc: Value, key: Value) -> Result<Value> {
    path::set(&mut doc, BUCKET_KEY_FIELD, key)?;
    Ok(doc)
}

fn bucket(docs: Vec<Value>, spec: &Value) -> Result<Vec<Value>> {
    let spec = spec
        .as_object()
        .ok_or_else(|| Error::Query("$bucket requires an object".into()))?;
    let group_by = spec
        .get("groupBy")
        .ok_or_else(|| Error::Query("$bucket requires `groupBy`".into()))?;
    let Some(boundaries) = spec.get("boundaries").and_then(Value::as_array) else {
        return Err(Error::Query("$bucket requires a boundaries array".into()));
    };
    if boundaries.len() < 2 {
        return Err(Error::Query(
            "$bucket boundaries must contain at least two values".into(),
        ));
    }
    for pair in boundaries.windows(2) {
        if compare_value(&pair[0], &pair[1]) != Ordering::Less {
            return Err(Error::Query(
                "$bucket boundaries must be strictly ascending".into(),
            ));
        }
    }
    let default = spec.get("default").cloned();
    if let Some(default) = &default {
        if compare_value(default, &boundaries[0]) == Ordering::Greater
            && compare_value(default, boundaries.last().expect("boundaries")) == Ordering::Less
        {
            return Err(Error::Query(
                "$bucket default must fall outside the boundaries".into(),
            ));
        }
    }
    let group_spec = bucket_output_spec(spec.get("output"))?;
    let mut builder = GroupBuilder::new(&group_spec)?;
    for doc in docs {
        let value = eval_expr(&doc, group_by)?;
        let mut bucket = None;
        for pair in boundaries.windows(2) {
            if compare_value(&value, &pair[0]) != Ordering::Less
                && compare_value(&value, &pair[1]) == Ordering::Less
            {
                bucket = Some(pair[0].clone());
                break;
            }
        }
        let key = match bucket {
            Some(key) => key,
            None => match &default {
                Some(default) => default.clone(),
                None => {
                    return Err(Error::Query(
                        "$bucket value is outside the boundaries and no default is set".into(),
                    ))
                }
            },
        };
        builder.update(with_bucket_key(doc, key)?)?;
    }
    let mut out = builder.finish()?;
    out.sort_by(|a, b| {
        let a = a.get("_id").unwrap_or(&Value::Null);
        let b = b.get("_id").unwrap_or(&Value::Null);
        let rank = |value: &Value| {
            boundaries
                .iter()
                .position(|boundary| value == boundary)
                .unwrap_or(boundaries.len())
        };
        rank(a).cmp(&rank(b)).then_with(|| compare_value(a, b))
    });
    Ok(out)
}

fn bucket_auto(docs: Vec<Value>, spec: &Value) -> Result<Vec<Value>> {
    let spec = spec
        .as_object()
        .ok_or_else(|| Error::Query("$bucketAuto requires an object".into()))?;
    let group_by = spec
        .get("groupBy")
        .ok_or_else(|| Error::Query("$bucketAuto requires `groupBy`".into()))?;
    let buckets = spec
        .get("buckets")
        .ok_or_else(|| Error::Query("$bucketAuto requires `buckets`".into()))
        .and_then(|value| as_usize(value, "$bucketAuto buckets"))?;
    if buckets == 0 {
        return Err(Error::Query("$bucketAuto buckets must be positive".into()));
    }
    let group_spec = bucket_output_spec(spec.get("output"))?;
    let mut entries = docs
        .into_iter()
        .map(|doc| Ok((eval_expr(&doc, group_by)?, doc)))
        .collect::<Result<Vec<_>>>()?;
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    entries.sort_by(|a, b| compare_value(&a.0, &b.0));
    let groups = buckets.min(entries.len());
    let mut builder = GroupBuilder::new(&group_spec)?;
    for index in 0..groups {
        let start = index * entries.len() / groups;
        let end = (index + 1) * entries.len() / groups;
        let min = entries[start].0.clone();
        let max = if end < entries.len() {
            entries[end].0.clone()
        } else {
            entries[end - 1].0.clone()
        };
        let id = Value::Object(Map::from_iter([
            ("min".to_string(), min),
            ("max".to_string(), max),
        ]));
        for (_, doc) in &entries[start..end] {
            builder.update(with_bucket_key(doc.clone(), id.clone())?)?;
        }
    }
    let mut out = builder.finish()?;
    out.sort_by(|a, b| {
        let value = |doc: &Value| {
            doc.get("_id")
                .and_then(|id| id.get("min"))
                .cloned()
                .unwrap_or(Value::Null)
        };
        compare_value(&value(a), &value(b))
    });
    Ok(out)
}

fn group(docs: Vec<Value>, spec: &Value) -> Result<Vec<Value>> {
    let mut builder = GroupBuilder::new(spec)?;
    for doc in docs {
        builder.update(doc)?;
    }
    builder.finish()
}

enum NumericSum {
    Float(f64),
    Decimal(BigDecimal),
}

impl NumericSum {
    fn add(&mut self, value: &Value) {
        if numeric::is_decimal(value) {
            let Some(d) = numeric::as_bigdecimal(value) else {
                return;
            };
            match self {
                Self::Float(f) => {
                    let mut sum = BigDecimal::from_f64(*f).unwrap_or_default();
                    sum += d;
                    *self = Self::Decimal(sum);
                }
                Self::Decimal(sum) => *sum += d,
            }
            return;
        }
        let Some(f) = numeric::as_f64(value) else {
            return;
        };
        match self {
            Self::Float(sum) => *sum += f,
            Self::Decimal(sum) => {
                if let Some(d) = BigDecimal::from_f64(f) {
                    *sum += d;
                }
            }
        }
    }

    fn merge(&mut self, other: NumericSum) {
        match (&mut *self, other) {
            (Self::Float(left), Self::Float(right)) => *left += right,
            (Self::Float(left), Self::Decimal(right)) => {
                let mut sum = BigDecimal::from_f64(*left).unwrap_or_default();
                sum += right;
                *self = Self::Decimal(sum);
            }
            (Self::Decimal(left), Self::Float(right)) => {
                if let Some(value) = BigDecimal::from_f64(right) {
                    *left += value;
                }
            }
            (Self::Decimal(left), Self::Decimal(right)) => *left += right,
        }
    }

    fn finish(self) -> Result<Value> {
        match self {
            Self::Float(n) => Ok(number_value(n)),
            Self::Decimal(n) => numeric::decimal_value(&n),
        }
    }
}

enum Accumulator {
    Sum(NumericSum),
    Avg {
        sum: NumericSum,
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
            "$sum" => Self::Sum(NumericSum::Float(0.0)),
            "$avg" => Self::Avg {
                sum: NumericSum::Float(0.0),
                count: 0,
            },
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
            (Self::Sum(sum), "$sum") => sum.add(value),
            (Self::Avg { sum, count }, "$avg") => {
                if numeric::parse_value(value).is_some_and(|n| n.is_finite()) {
                    sum.add(value);
                    *count += 1;
                }
            }
            (Self::Min(current), "$min") => {
                if !value.is_null()
                    && current
                        .as_ref()
                        .map(|old| compare_value(value, old) == Ordering::Less)
                        .unwrap_or(true)
                {
                    *current = Some(value.clone());
                }
            }
            (Self::Max(current), "$max") => {
                if !value.is_null()
                    && current
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

    fn merge(&mut self, other: Accumulator) -> Result<()> {
        match (self, other) {
            (Self::Sum(left), Self::Sum(right)) => left.merge(right),
            (
                Self::Avg {
                    sum: left_sum,
                    count: left_count,
                },
                Self::Avg {
                    sum: right_sum,
                    count: right_count,
                },
            ) => {
                left_sum.merge(right_sum);
                *left_count = left_count.saturating_add(right_count);
            }
            (Self::Min(left), Self::Min(right)) => {
                if let Some(value) = right.filter(|value| !value.is_null()) {
                    if left
                        .as_ref()
                        .map(|current| compare_value(&value, current) == Ordering::Less)
                        .unwrap_or(true)
                    {
                        *left = Some(value);
                    }
                }
            }
            (Self::Max(left), Self::Max(right)) => {
                if let Some(value) = right.filter(|value| !value.is_null()) {
                    if left
                        .as_ref()
                        .map(|current| compare_value(&value, current) == Ordering::Greater)
                        .unwrap_or(true)
                    {
                        *left = Some(value);
                    }
                }
            }
            (Self::Count(left), Self::Count(right)) => *left = left.saturating_add(right),
            _ => {
                return Err(Error::Query(
                    "internal group accumulator merge mismatch".into(),
                ));
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<Value> {
        match self {
            Self::Sum(sum) => sum.finish(),
            Self::Avg { sum, count } => {
                if count == 0 {
                    Ok(Value::Null)
                } else {
                    match sum {
                        NumericSum::Float(sum) => Ok(number_value(sum / count as f64)),
                        NumericSum::Decimal(sum) => {
                            numeric::decimal_value(&(sum / BigDecimal::from(count)))
                        }
                    }
                }
            }
            Self::Min(value) | Self::Max(value) => Ok(value.unwrap_or(Value::Null)),
            Self::Last(value) => Ok(value),
            Self::First { seen, value } => Ok(if seen { value } else { Value::Null }),
            Self::Push(values) | Self::AddToSet { values, .. } => Ok(Value::Array(values)),
            Self::Count(count) => Ok(Value::from(count)),
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

pub(crate) fn compare_value(a: &Value, b: &Value) -> Ordering {
    if let Some(ord) = numeric::cmp_values(a, b) {
        return ord;
    }
    if let Some(ord) = bson_types::cmp_values(a, b) {
        return ord;
    }
    match (a, b) {
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        _ => date::cmp(a, b).unwrap_or(Ordering::Equal),
    }
}

fn decimal_arithmetic(op: &str, values: &[Value]) -> Result<Value> {
    let mut nums = Vec::with_capacity(values.len());
    for value in values {
        let n = numeric::as_bigdecimal(value)
            .ok_or_else(|| Error::Query(format!("{op} requires finite numbers")))?;
        nums.push(n);
    }
    let result = match op {
        "$add" => nums.into_iter().fold(BigDecimal::from(0), |a, b| a + b),
        "$subtract" => {
            if nums.len() != 2 {
                return Err(Error::Query("$subtract requires two arguments".into()));
            }
            nums[0].clone() - nums[1].clone()
        }
        "$multiply" => nums.into_iter().fold(BigDecimal::from(1), |a, b| a * b),
        "$divide" => {
            if nums.len() != 2 {
                return Err(Error::Query("$divide requires two arguments".into()));
            }
            if nums[1].is_zero() {
                return Err(Error::Query("$divide by zero".into()));
            }
            nums[0].clone() / nums[1].clone()
        }
        _ => unreachable!(),
    };
    numeric::decimal_value(&result)
}

pub(crate) fn eval_expr(doc: &Value, expr: &Value) -> Result<Value> {
    eval_expr_vars(doc, expr, &BTreeMap::new())
}

fn eval_expr_vars(doc: &Value, expr: &Value, vars: &BTreeMap<String, Value>) -> Result<Value> {
    match expr {
        Value::String(s) if s == "$$ROOT" || s == "$$CURRENT" => Ok(doc.clone()),
        Value::String(s) if s.starts_with("$$") => {
            let reference = &s[2..];
            let mut parts = reference.split('.');
            let Some(name) = parts.next() else {
                return Ok(Value::Null);
            };
            let Some(value) = vars.get(name) else {
                return Ok(Value::Null);
            };
            let tail = parts.collect::<Vec<_>>().join(".");
            if tail.is_empty() {
                Ok(value.clone())
            } else {
                Ok(path::get(value, &tail).cloned().unwrap_or(Value::Null))
            }
        }
        Value::String(s) if s.starts_with('$') => {
            Ok(path::get(doc, &s[1..]).cloned().unwrap_or(Value::Null))
        }
        Value::Array(items) => items
            .iter()
            .map(|item| eval_expr_vars(doc, item, vars))
            .collect::<Result<Vec<_>>>()
            .map(Value::Array),
        Value::Object(obj)
            if obj.len() == 1
                && obj.keys().next().is_some_and(|k| k.starts_with('$'))
                && !date::is_date(expr)
                && !numeric::is_numeric(expr)
                && !bson_types::is_bson_value(expr) =>
        {
            eval_operator(doc, obj, vars)
        }
        Value::Object(obj) => {
            let mut out = Map::new();
            for (key, value) in obj {
                out.insert(key.clone(), eval_expr_vars(doc, value, vars)?);
            }
            Ok(Value::Object(out))
        }
        _ => Ok(expr.clone()),
    }
}

fn eval_operator(
    doc: &Value,
    obj: &Map<String, Value>,
    vars: &BTreeMap<String, Value>,
) -> Result<Value> {
    let (op, arg) = obj.iter().next().expect("one entry");
    let args = || -> Result<Vec<Value>> {
        let values = arg
            .as_array()
            .ok_or_else(|| Error::Query(format!("{op} requires an array of arguments")))?;
        values
            .iter()
            .map(|v| eval_expr_vars(doc, v, vars))
            .collect()
    };
    let one_arg = || -> Result<Value> {
        if let Value::Array(items) = arg {
            if items.len() != 1 {
                return Err(Error::Query(format!("{op} requires one argument")));
            }
            eval_expr_vars(doc, &items[0], vars)
        } else {
            eval_expr_vars(doc, arg, vars)
        }
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
            if values.iter().any(numeric::is_decimal) {
                return decimal_arithmetic(op, &values);
            }
            if op == "$add" {
                let sum = values.iter().filter_map(numeric::as_f64).sum::<f64>();
                return Ok(number_value(sum));
            }
            if values.len() != 2 {
                return Err(Error::Query(format!("{op} requires two arguments")));
            }
            let a = numeric::as_f64(&values[0])
                .ok_or_else(|| Error::Query(format!("{op} requires numbers")))?;
            let b = numeric::as_f64(&values[1])
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
                    (eval_expr_vars(doc, &items[0], vars)?, &items[1], &items[2])
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
                    (eval_expr_vars(doc, condition, vars)?, yes, no)
                }
                _ => return Err(Error::Query("$cond requires three arguments".into())),
            };
            if truthy(&condition) {
                eval_expr_vars(doc, yes, vars)
            } else {
                eval_expr_vars(doc, no, vars)
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
        "$and" => {
            let values = args()?;
            Ok(Value::Bool(values.iter().all(truthy)))
        }
        "$or" => {
            let values = args()?;
            Ok(Value::Bool(values.iter().any(truthy)))
        }
        "$not" => {
            let values = args()?;
            if values.len() != 1 {
                return Err(Error::Query("$not requires one argument".into()));
            }
            Ok(Value::Bool(!truthy(&values[0])))
        }
        "$toLower" | "$toUpper" => {
            let values = args()?;
            if values.len() != 1 {
                return Err(Error::Query(format!("{op} requires one argument")));
            }
            Ok(match values[0].as_str() {
                Some(s) if op == "$toLower" => Value::String(s.to_lowercase()),
                Some(s) => Value::String(s.to_uppercase()),
                None => Value::Null,
            })
        }
        "$length" | "$size" => {
            let values = args()?;
            if values.len() != 1 {
                return Err(Error::Query(format!("{op} requires one argument")));
            }
            Ok(match &values[0] {
                Value::String(s) => Value::from(s.chars().count()),
                Value::Array(items) => Value::from(items.len()),
                _ => Value::Null,
            })
        }
        "$substr" => {
            let values = args()?;
            if values.len() != 3 {
                return Err(Error::Query("$substr requires three arguments".into()));
            }
            let Some(s) = values[0].as_str() else {
                return Ok(Value::Null);
            };
            let Some(start) = values[1].as_i64() else {
                return Ok(Value::Null);
            };
            let Some(len) = values[2].as_i64() else {
                return Ok(Value::Null);
            };
            let start = start.max(0) as usize;
            let len = len.max(0) as usize;
            Ok(Value::String(s.chars().skip(start).take(len).collect()))
        }
        "$trim" | "$ltrim" | "$rtrim" => {
            let values = args()?;
            if values.len() != 1 {
                return Err(Error::Query(format!("{op} requires one argument")));
            }
            Ok(match values[0].as_str() {
                Some(s) => Value::String(match op.as_str() {
                    "$trim" => s.trim().to_string(),
                    "$ltrim" => s.trim_start().to_string(),
                    _ => s.trim_end().to_string(),
                }),
                None => Value::Null,
            })
        }
        "$setDifference" => {
            let values = args()?;
            if values.len() != 2 {
                return Err(Error::Query("$setDifference requires two arrays".into()));
            }
            let (Value::Array(left), Value::Array(right)) = (&values[0], &values[1]) else {
                return Err(Error::Query("$setDifference requires arrays".into()));
            };
            let mut removed = HashSet::new();
            for value in right {
                removed.insert(serde_json::to_string(value)?);
            }
            let mut seen = HashSet::new();
            let mut out = Vec::new();
            for value in left {
                let key = serde_json::to_string(value)?;
                if !removed.contains(&key) && seen.insert(key) {
                    out.push(value.clone());
                }
            }
            Ok(Value::Array(out))
        }
        "$abs" | "$ceil" | "$floor" | "$round" => {
            let values = args()?;
            if values.is_empty() || values.len() > 2 {
                return Err(Error::Query(format!("{op} has invalid arity")));
            }
            let Some(n) = numeric::as_f64(&values[0]) else {
                return Ok(Value::Null);
            };
            let result = match op.as_str() {
                "$abs" => n.abs(),
                "$ceil" => n.ceil(),
                "$floor" => n.floor(),
                "$round" => {
                    let precision = values
                        .get(1)
                        .and_then(Value::as_i64)
                        .unwrap_or(0)
                        .clamp(-15, 15);
                    let factor = 10_f64.powi(precision as i32);
                    (n * factor).round() / factor
                }
                _ => unreachable!(),
            };
            Ok(number_value(result))
        }
        "$in" => {
            let values = args()?;
            if values.len() != 2 {
                return Err(Error::Query("$in requires two arguments".into()));
            }
            let Some(items) = values[1].as_array() else {
                return Err(Error::Query(
                    "$in requires an array as its second argument".into(),
                ));
            };
            Ok(Value::Bool(items.iter().any(|item| {
                compare_value(&values[0], item) == Ordering::Equal
            })))
        }
        "$isArray" => Ok(Value::Bool(one_arg()?.is_array())),
        "$arrayElemAt" => {
            let values = args()?;
            if values.len() != 2 {
                return Err(Error::Query("$arrayElemAt requires two arguments".into()));
            }
            let Some(items) = values[0].as_array() else {
                return Ok(Value::Null);
            };
            let Some(index) = expr_i64(&values[1]) else {
                return Err(Error::Query(
                    "$arrayElemAt requires an integer index".into(),
                ));
            };
            let index = if index < 0 {
                items.len() as i64 + index
            } else {
                index
            };
            Ok(usize::try_from(index)
                .ok()
                .and_then(|index| items.get(index))
                .cloned()
                .unwrap_or(Value::Null))
        }
        "$concatArrays" => {
            let values = args()?;
            let mut out = Vec::new();
            for value in values {
                if value.is_null() {
                    return Ok(Value::Null);
                }
                let Some(items) = value.as_array() else {
                    return Err(Error::Query("$concatArrays requires arrays".into()));
                };
                out.extend(items.iter().cloned());
            }
            Ok(Value::Array(out))
        }
        "$slice" => {
            let values = args()?;
            if values.len() != 2 && values.len() != 3 {
                return Err(Error::Query(
                    "$slice requires two or three arguments".into(),
                ));
            }
            let Some(items) = values[0].as_array() else {
                return Ok(Value::Null);
            };
            let (skip, limit) = if values.len() == 2 {
                let Some(count) = expr_i64(&values[1]) else {
                    return Err(Error::Query("$slice requires an integer count".into()));
                };
                if count >= 0 {
                    (0usize, usize::try_from(count).unwrap_or(usize::MAX))
                } else {
                    let take = usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX);
                    (items.len().saturating_sub(take.min(items.len())), take)
                }
            } else {
                let Some(skip) = expr_i64(&values[1]) else {
                    return Err(Error::Query("$slice requires an integer position".into()));
                };
                let Some(count) = expr_i64(&values[2]) else {
                    return Err(Error::Query("$slice requires an integer count".into()));
                };
                if count <= 0 {
                    return Ok(Value::Array(Vec::new()));
                }
                let start = if skip >= 0 {
                    usize::try_from(skip).unwrap_or(usize::MAX).min(items.len())
                } else {
                    items.len().saturating_sub(
                        usize::try_from(skip.unsigned_abs())
                            .unwrap_or(usize::MAX)
                            .min(items.len()),
                    )
                };
                (
                    start,
                    usize::try_from(count)
                        .unwrap_or(usize::MAX)
                        .min(items.len().saturating_sub(start)),
                )
            };
            let end = skip.saturating_add(limit).min(items.len());
            Ok(Value::Array(items[skip..end].to_vec()))
        }
        "$switch" => {
            let Some(obj) = arg.as_object() else {
                return Err(Error::Query("$switch requires an object".into()));
            };
            let Some(branches) = obj.get("branches").and_then(Value::as_array) else {
                return Err(Error::Query("$switch requires a branches array".into()));
            };
            for branch in branches {
                let Some(branch) = branch.as_object() else {
                    return Err(Error::Query("$switch branches must be objects".into()));
                };
                let Some(case) = branch.get("case") else {
                    return Err(Error::Query("$switch branch requires `case`".into()));
                };
                let case = eval_expr_vars(doc, case, vars)?;
                if truthy(&case) {
                    let then = branch
                        .get("then")
                        .ok_or_else(|| Error::Query("$switch branch requires `then`".into()))?;
                    return eval_expr_vars(doc, then, vars);
                }
            }
            match obj.get("default") {
                Some(default) => eval_expr_vars(doc, default, vars),
                None => Err(Error::Query("$switch found no matching branch".into())),
            }
        }
        "$let" => {
            let Some(obj) = arg.as_object() else {
                return Err(Error::Query("$let requires an object".into()));
            };
            let Some(bindings) = obj.get("vars").and_then(Value::as_object) else {
                return Err(Error::Query("$let requires a vars object".into()));
            };
            let Some(in_expr) = obj.get("in") else {
                return Err(Error::Query("$let requires `in`".into()));
            };
            let mut scope = vars.clone();
            for (name, value) in bindings {
                scope.insert(name.clone(), eval_expr_vars(doc, value, vars)?);
            }
            eval_expr_vars(doc, in_expr, &scope)
        }
        "$map" => {
            let Some(obj) = arg.as_object() else {
                return Err(Error::Query("$map requires an object".into()));
            };
            let Some(input) = obj.get("input") else {
                return Err(Error::Query("$map requires `input`".into()));
            };
            let Some(in_expr) = obj.get("in") else {
                return Err(Error::Query("$map requires `in`".into()));
            };
            let input = eval_expr_vars(doc, input, vars)?;
            let Some(items) = input.as_array() else {
                return Ok(Value::Null);
            };
            let name = obj.get("as").and_then(Value::as_str).unwrap_or("this");
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let mut scope = vars.clone();
                scope.insert(name.to_string(), item.clone());
                out.push(eval_expr_vars(doc, in_expr, &scope)?);
            }
            Ok(Value::Array(out))
        }
        "$filter" => {
            let Some(obj) = arg.as_object() else {
                return Err(Error::Query("$filter requires an object".into()));
            };
            let Some(input) = obj.get("input") else {
                return Err(Error::Query("$filter requires `input`".into()));
            };
            let Some(cond) = obj.get("cond") else {
                return Err(Error::Query("$filter requires `cond`".into()));
            };
            let input = eval_expr_vars(doc, input, vars)?;
            let Some(items) = input.as_array() else {
                return Ok(Value::Null);
            };
            let name = obj.get("as").and_then(Value::as_str).unwrap_or("this");
            let limit = match obj.get("limit") {
                Some(limit) => expr_i64(&eval_expr_vars(doc, limit, vars)?)
                    .filter(|limit| *limit >= 0)
                    .map(|limit| limit as usize),
                None => None,
            };
            if limit == Some(0) {
                return Ok(Value::Array(Vec::new()));
            }
            let mut out = Vec::new();
            for item in items {
                let mut scope = vars.clone();
                scope.insert(name.to_string(), item.clone());
                if truthy(&eval_expr_vars(doc, cond, &scope)?) {
                    out.push(item.clone());
                    if limit.is_some_and(|limit| out.len() >= limit) {
                        break;
                    }
                }
            }
            Ok(Value::Array(out))
        }
        "$reduce" => {
            let Some(obj) = arg.as_object() else {
                return Err(Error::Query("$reduce requires an object".into()));
            };
            let Some(input) = obj.get("input") else {
                return Err(Error::Query("$reduce requires `input`".into()));
            };
            let Some(initial) = obj.get("initialValue") else {
                return Err(Error::Query("$reduce requires `initialValue`".into()));
            };
            let Some(in_expr) = obj.get("in") else {
                return Err(Error::Query("$reduce requires `in`".into()));
            };
            let input = eval_expr_vars(doc, input, vars)?;
            let Some(items) = input.as_array() else {
                return Ok(Value::Null);
            };
            let mut value = eval_expr_vars(doc, initial, vars)?;
            for item in items {
                let mut scope = vars.clone();
                scope.insert("value".to_string(), value);
                scope.insert("this".to_string(), item.clone());
                value = eval_expr_vars(doc, in_expr, &scope)?;
            }
            Ok(value)
        }
        "$mod" | "$pow" => {
            let values = args()?;
            if values.len() != 2 {
                return Err(Error::Query(format!("{op} requires two arguments")));
            }
            if op == "$mod" {
                if values.iter().any(numeric::is_decimal) {
                    let a = numeric::as_bigdecimal(&values[0])
                        .ok_or_else(|| Error::Query("$mod requires numbers".into()))?;
                    let b = numeric::as_bigdecimal(&values[1])
                        .ok_or_else(|| Error::Query("$mod requires numbers".into()))?;
                    if b.is_zero() {
                        return Err(Error::Query("$mod divisor cannot be zero".into()));
                    }
                    return numeric::decimal_value(&(a % b));
                }
                let a = numeric::as_f64(&values[0])
                    .ok_or_else(|| Error::Query("$mod requires numbers".into()))?;
                let b = numeric::as_f64(&values[1])
                    .ok_or_else(|| Error::Query("$mod requires numbers".into()))?;
                if b == 0.0 {
                    return Err(Error::Query("$mod divisor cannot be zero".into()));
                }
                return Ok(number_value(a % b));
            }
            let a = numeric::as_f64(&values[0])
                .ok_or_else(|| Error::Query("$pow requires numbers".into()))?;
            let b = numeric::as_f64(&values[1])
                .ok_or_else(|| Error::Query("$pow requires numbers".into()))?;
            Ok(number_value(a.powf(b)))
        }
        "$min" | "$max" => {
            let mut values = if arg.is_array() {
                args()?
            } else {
                vec![eval_expr_vars(doc, arg, vars)?]
            };
            if values.len() == 1 {
                if let Some(items) = values[0].as_array() {
                    values = items.clone();
                }
            }
            let mut best: Option<Value> = None;
            for value in values {
                if value.is_null() {
                    continue;
                }
                let replace = best.as_ref().is_none_or(|current| {
                    let ord = compare_value(&value, current);
                    if op == "$min" {
                        ord == Ordering::Less
                    } else {
                        ord == Ordering::Greater
                    }
                });
                if replace {
                    best = Some(value);
                }
            }
            Ok(best.unwrap_or(Value::Null))
        }
        "$regexMatch" => {
            let Some(obj) = arg.as_object() else {
                return Err(Error::Query("$regexMatch requires an object".into()));
            };
            let input = obj
                .get("input")
                .ok_or_else(|| Error::Query("$regexMatch requires `input`".into()))?;
            let regex = obj
                .get("regex")
                .ok_or_else(|| Error::Query("$regexMatch requires `regex`".into()))?;
            let input = eval_expr_vars(doc, input, vars)?;
            let regex = eval_expr_vars(doc, regex, vars)?;
            let options = match obj.get("options") {
                Some(options) => Some(eval_expr_vars(doc, options, vars)?),
                None => None,
            };
            Ok(Value::Bool(crate::regex::is_match(
                Some(&input),
                &regex,
                options.as_ref(),
            )))
        }
        "$strLenCP" => Ok(match one_arg()?.as_str() {
            Some(s) => Value::from(s.chars().count()),
            None => Value::Null,
        }),
        "$substrCP" => {
            let values = args()?;
            if values.len() != 3 {
                return Err(Error::Query("$substrCP requires three arguments".into()));
            }
            let Some(s) = values[0].as_str() else {
                return Ok(Value::Null);
            };
            let Some(start) = expr_i64(&values[1]) else {
                return Ok(Value::Null);
            };
            let Some(count) = expr_i64(&values[2]) else {
                return Ok(Value::Null);
            };
            Ok(Value::String(
                s.chars()
                    .skip(start.max(0) as usize)
                    .take(count.max(0) as usize)
                    .collect(),
            ))
        }
        "$split" => {
            let values = args()?;
            if values.len() != 2 {
                return Err(Error::Query("$split requires two arguments".into()));
            }
            let (Some(s), Some(delimiter)) = (values[0].as_str(), values[1].as_str()) else {
                return Ok(Value::Null);
            };
            if delimiter.is_empty() {
                return Err(Error::Query("$split delimiter cannot be empty".into()));
            }
            Ok(Value::Array(s.split(delimiter).map(Value::from).collect()))
        }
        "$indexOfCP" => {
            let values = args()?;
            if values.len() < 2 || values.len() > 4 {
                return Err(Error::Query(
                    "$indexOfCP requires two to four arguments".into(),
                ));
            }
            let (Some(s), Some(search)) = (values[0].as_str(), values[1].as_str()) else {
                return Ok(Value::Null);
            };
            let start = values.get(2).and_then(expr_i64).unwrap_or(0).max(0) as usize;
            let end = values
                .get(3)
                .and_then(expr_i64)
                .map(|end| end.max(0) as usize);
            let chars: Vec<char> = s.chars().collect();
            let start = start.min(chars.len());
            let end = end.unwrap_or(chars.len()).min(chars.len()).max(start);
            let haystack: String = chars[start..end].iter().collect();
            Ok(match haystack.find(search) {
                Some(byte_index) => Value::from(start + haystack[..byte_index].chars().count()),
                None => Value::from(-1),
            })
        }
        "$objectToArray" => {
            let value = one_arg()?;
            let Some(obj) = value.as_object() else {
                return Ok(Value::Null);
            };
            Ok(Value::Array(
                obj.iter()
                    .map(|(key, value)| {
                        Value::Object(Map::from_iter([
                            ("k".to_string(), Value::String(key.clone())),
                            ("v".to_string(), value.clone()),
                        ]))
                    })
                    .collect(),
            ))
        }
        "$arrayToObject" => {
            let value = one_arg()?;
            let Some(items) = value.as_array() else {
                return Ok(Value::Null);
            };
            let mut out = Map::new();
            for item in items {
                let (key, value) = match item {
                    Value::Object(obj) => {
                        let key = obj.get("k").and_then(Value::as_str);
                        let value = obj.get("v");
                        match (key, value) {
                            (Some(key), Some(value)) => (key.to_string(), value.clone()),
                            _ => {
                                return Err(Error::Query(
                                    "$arrayToObject entries require k and v".into(),
                                ))
                            }
                        }
                    }
                    Value::Array(pair) if pair.len() == 2 => {
                        let Some(key) = pair[0].as_str() else {
                            return Err(Error::Query("$arrayToObject keys must be strings".into()));
                        };
                        (key.to_string(), pair[1].clone())
                    }
                    _ => {
                        return Err(Error::Query(
                            "$arrayToObject requires {k,v} objects or [k,v] pairs".into(),
                        ))
                    }
                };
                out.insert(key, value);
            }
            Ok(Value::Object(out))
        }
        "$mergeObjects" => {
            let values = args()?;
            let mut out = Map::new();
            for value in values {
                if value.is_null() {
                    continue;
                }
                let Some(obj) = value.as_object() else {
                    return Err(Error::Query("$mergeObjects requires objects".into()));
                };
                for (key, value) in obj {
                    out.insert(key.clone(), value.clone());
                }
            }
            Ok(Value::Object(out))
        }
        "$type" => Ok(Value::String(
            bson_types::type_name(&one_arg()?)
                .unwrap_or("null")
                .to_string(),
        )),
        _ => Err(Error::Query(format!(
            "unsupported aggregation expression `{op}`"
        ))),
    }
}

fn expr_i64(value: &Value) -> Option<i64> {
    if let Some(number) = value.as_i64() {
        return Some(number);
    }
    let number = numeric::as_bigdecimal(value)?;
    if number != number.with_scale(0) {
        return None;
    }
    bigdecimal::num_traits::ToPrimitive::to_i64(&number)
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
    fn top_k_sort_with_skip_and_limit() {
        let docs: Vec<Value> = (0..20).map(|n| json!({"n": n})).collect();
        let pipeline = vec![
            json!({"$sort": {"n": -1}}),
            json!({"$skip": 2}),
            json!({"$limit": 3}),
        ];
        let out = run(docs, &pipeline, 100).unwrap();
        assert_eq!(
            out,
            vec![json!({"n": 17}), json!({"n": 16}), json!({"n": 15})]
        );
    }

    #[test]
    fn fused_document_local_stages_preserve_order() {
        let docs: Vec<Value> = (1..=3)
            .map(|n| json!({"_id": n.to_string(), "n": n}))
            .collect();
        let pipeline = vec![
            json!({"$set": {"x": {"$multiply": ["$n", 2]}}}),
            json!({"$match": {"x": {"$gte": 4}}}),
            json!({"$project": {"_id": 0, "x": 1, "n": 1}}),
            json!({"$unset": "n"}),
        ];
        let out = run(docs, &pipeline, 100).unwrap();
        assert_eq!(out, vec![json!({"x": 4}), json!({"x": 6})]);
    }

    #[test]
    fn expression_compatibility_operators() {
        let doc = json!({
            "name": "Ala",
            "nums": [3, 7, 11],
            "items": [{"qty": 2}, {"qty": 5}],
            "obj": {"a": 1},
            "other": 4
        });
        let eval = |expr: Value| eval_expr(&doc, &expr).unwrap();

        assert_eq!(eval(json!({"$in": [7, "$nums"]})), json!(true));
        assert_eq!(eval(json!({"$isArray": "$nums"})), json!(true));
        assert_eq!(eval(json!({"$arrayElemAt": ["$nums", -1]})), json!(11));
        assert_eq!(eval(json!({"$slice": ["$nums", 1, 2]})), json!([7, 11]));
        assert_eq!(
            eval(json!({"$concatArrays": [[1, 2], [3]]})),
            json!([1, 2, 3])
        );
        assert_eq!(
            eval(json!({"$map": {
                "input": "$nums",
                "as": "n",
                "in": {"$multiply": ["$$n", 2]}
            }})),
            json!([6, 14, 22])
        );
        assert_eq!(
            eval(json!({"$filter": {
                "input": "$nums",
                "as": "n",
                "cond": {"$gt": ["$$n", 4]}
            }})),
            json!([7, 11])
        );
        assert_eq!(
            eval(json!({"$reduce": {
                "input": "$nums",
                "initialValue": 0,
                "in": {"$add": ["$$value", "$$this"]}
            }})),
            json!(21)
        );
        assert_eq!(
            eval(json!({"$let": {
                "vars": {"x": 5},
                "in": {"$add": ["$$x", 1]}
            }})),
            json!(6)
        );
        assert_eq!(
            eval(json!({"$switch": {
                "branches": [
                    {"case": {"$gte": ["$other", 5]}, "then": "high"},
                    {"case": true, "then": "low"}
                ]
            }})),
            json!("low")
        );
        assert_eq!(eval(json!({"$mod": [7, 3]})), json!(1));
        assert_eq!(eval(json!({"$pow": [2, 5]})), json!(32));
        assert_eq!(eval(json!({"$min": "$nums"})), json!(3));
        assert_eq!(eval(json!({"$max": "$nums"})), json!(11));
        assert_eq!(
            eval(json!({"$regexMatch": {"input": "$name", "regex": "^Al"}})),
            json!(true)
        );
        assert_eq!(eval(json!({"$strLenCP": "$name"})), json!(3));
        assert_eq!(eval(json!({"$substrCP": ["$name", 1, 1]})), json!("l"));
        assert_eq!(eval(json!({"$split": ["a,b", ","]})), json!(["a", "b"]));
        assert_eq!(eval(json!({"$indexOfCP": ["$name", "l"]})), json!(1));
        assert_eq!(
            eval(json!({"$objectToArray": "$obj"})),
            json!([{"k": "a", "v": 1}])
        );
        assert_eq!(
            eval(json!({"$arrayToObject": [[["b", 2]]]})),
            json!({"b": 2})
        );
        assert_eq!(
            eval(json!({"$mergeObjects": [{"a": 1}, {"b": 2}]})),
            json!({"a": 1, "b": 2})
        );
        assert_eq!(eval(json!({"$type": "$name"})), json!("string"));
    }

    #[test]
    fn replace_sample_sort_by_count_and_facet() {
        let docs = vec![
            json!({"_id": "1", "cat": "a", "n": 1, "nested": {"x": 1}}),
            json!({"_id": "2", "cat": "a", "n": 2, "nested": {"x": 2}}),
            json!({"_id": "3", "cat": "b", "n": 3, "nested": {"x": 3}}),
        ];

        assert_eq!(
            run(docs.clone(), &[json!({"$replaceWith": "$nested"})], 100).unwrap(),
            vec![json!({"x": 1}), json!({"x": 2}), json!({"x": 3})]
        );
        assert_eq!(
            run(
                docs.clone(),
                &[json!({"$replaceRoot": {"newRoot": "$nested"}})],
                100
            )
            .unwrap(),
            vec![json!({"x": 1}), json!({"x": 2}), json!({"x": 3})]
        );
        assert_eq!(
            run(docs.clone(), &[json!({"$sortByCount": "$cat"})], 100).unwrap(),
            vec![
                json!({"_id": "a", "count": 2}),
                json!({"_id": "b", "count": 1})
            ]
        );

        let faceted = run(
            docs.clone(),
            &[json!({"$facet": {
                "by_cat": [{"$sortByCount": "$cat"}],
                "total": [{"$count": "n"}],
                "top": [{"$sort": {"n": -1}}, {"$limit": 1}, {"$project": {"_id": 1}}]
            }})],
            100,
        )
        .unwrap();
        assert_eq!(faceted.len(), 1);
        assert_eq!(faceted[0]["total"], json!([{"n": 3}]));
        assert_eq!(faceted[0]["top"], json!([{"_id": "3"}]));
        assert_eq!(
            faceted[0]["by_cat"],
            json!([
                {"_id": "a", "count": 2},
                {"_id": "b", "count": 1}
            ])
        );

        for size in [0usize, 1, 3, 10] {
            let out = run(docs.clone(), &[json!({"$sample": {"size": size}})], 100).unwrap();
            assert_eq!(out.len(), size.min(docs.len()));
        }
    }

    #[test]
    fn bucket_and_bucket_auto() {
        let docs = vec![
            json!({"_id": "1", "v": 1}),
            json!({"_id": "2", "v": 5}),
            json!({"_id": "3", "v": 10}),
            json!({"_id": "4", "v": 15}),
            json!({"_id": "5", "v": 35}),
        ];
        assert_eq!(
            run(
                docs.clone(),
                &[json!({"$bucket": {
                    "groupBy": "$v",
                    "boundaries": [0, 10, 20, 30],
                    "default": "other",
                    "output": {
                        "count": {"$sum": 1},
                        "total": {"$sum": "$v"}
                    }
                }})],
                100
            )
            .unwrap(),
            vec![
                json!({"_id": 0, "count": 2, "total": 6}),
                json!({"_id": 10, "count": 2, "total": 25}),
                json!({"_id": "other", "count": 1, "total": 35})
            ]
        );
        assert!(run(
            docs.clone(),
            &[json!({"$bucket": {
                "groupBy": "$v",
                "boundaries": [0, 10, 20, 30]
            }})],
            100
        )
        .is_err());
        assert!(run(
            docs.clone(),
            &[json!({"$bucket": {
                "groupBy": "$v",
                "boundaries": [10, 0]
            }})],
            100
        )
        .is_err());

        let auto_docs: Vec<Value> = (1..=6)
            .map(|v| json!({"_id": v.to_string(), "v": v}))
            .collect();
        assert_eq!(
            run(
                auto_docs,
                &[json!({"$bucketAuto": {
                    "groupBy": "$v",
                    "buckets": 2,
                    "output": {"count": {"$sum": 1}, "total": {"$sum": "$v"}}
                }})],
                100
            )
            .unwrap(),
            vec![
                json!({"_id": {"min": 1, "max": 4}, "count": 3, "total": 6}),
                json!({"_id": {"min": 4, "max": 6}, "count": 3, "total": 15})
            ]
        );
        assert!(run(
            docs.clone(),
            &[json!({"$bucketAuto": {"groupBy": "$v", "buckets": 0}})],
            100
        )
        .is_err());
        assert!(run(
            docs,
            &[json!({"$bucket": {
                "groupBy": "$v",
                "boundaries": [0, 10],
                "output": {"_id": {"$sum": 1}}
            }})],
            100
        )
        .is_err());
    }

    #[test]
    fn rejects_unknown_stage() {
        let err = run(vec![], &[json!({"$geoNear": {}})], 100).unwrap_err();
        assert!(err.to_string().contains("unsupported aggregation stage"));
    }
}
