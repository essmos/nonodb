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
use time::{Date, Duration, Month, OffsetDateTime, Time, UtcOffset, Weekday};

use crate::bson_types;
use crate::collection::compare_docs;
use crate::date;
use crate::error::{Error, Result};
use crate::numeric;
use crate::query::matcher::matches_with_vars;
use crate::query::path;

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

/// Terminal aggregation stages that write their result into a collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateWriteKind {
    Out,
    Merge,
}

/// `$merge` behavior when the target already has a matching document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeWhenMatched {
    Replace,
    KeepExisting,
    Merge,
    Fail,
    /// A `whenMatched` pipeline, run with the existing document as the root
    /// and `$$new` bound to the incoming result document.
    Pipeline(Vec<Value>),
}

/// `$merge` behavior when no target document matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeWhenNotMatched {
    Insert,
    Discard,
    Fail,
}

/// Parsed terminal `$out` or `$merge` stage. The writer is responsible for
/// executing the read pipeline and applying the result transactionally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateWriteSpec {
    pub kind: AggregateWriteKind,
    pub target: String,
    pub database: Option<String>,
    pub on: Vec<String>,
    pub when_matched: MergeWhenMatched,
    pub when_not_matched: MergeWhenNotMatched,
    /// `$merge` `let` variables, evaluated against each incoming document.
    pub let_vars: Vec<(String, Value)>,
}

fn write_stage_key(value: &Value) -> Option<&str> {
    let obj = value.as_object()?;
    obj.keys()
        .find(|key| matches!(key.as_str(), "$out" | "$merge"))
        .map(String::as_str)
}

fn parse_write_target(value: &Value, stage: &str) -> Result<(String, Option<String>)> {
    match value {
        Value::String(target) if !target.is_empty() => Ok((target.clone(), None)),
        Value::Object(obj) => {
            let target = obj
                .get("coll")
                .and_then(Value::as_str)
                .filter(|target| !target.is_empty())
                .ok_or_else(|| Error::Query(format!("{stage} requires `coll`")))?;
            let database = match obj.get("db") {
                Some(value) => Some(
                    value
                        .as_str()
                        .filter(|database| !database.is_empty())
                        .ok_or_else(|| Error::Query(format!("{stage} `db` must be a string")))?
                        .to_string(),
                ),
                None => None,
            };
            Ok((target.to_string(), database))
        }
        _ => Err(Error::Query(format!(
            "{stage} requires a collection name or an object with `coll`"
        ))),
    }
}

fn parse_merge_on(value: Option<&Value>) -> Result<Vec<String>> {
    let Some(value) = value else {
        return Ok(vec!["_id".to_string()]);
    };
    let values = match value {
        Value::String(field) => vec![field.clone()],
        Value::Array(values) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .filter(|field| !field.is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| Error::Query("$merge `on` fields must be strings".into()))
            })
            .collect::<Result<Vec<_>>>()?,
        _ => {
            return Err(Error::Query(
                "$merge `on` must be a string or an array".into(),
            ))
        }
    };
    if values.is_empty() {
        return Err(Error::Query("$merge `on` must not be empty".into()));
    }
    Ok(values)
}

fn validate_merge_pipeline(pipeline: &[Value]) -> Result<()> {
    if pipeline.is_empty() {
        return Err(Error::Query(
            "$merge whenMatched pipeline must not be empty".into(),
        ));
    }
    for stage in pipeline {
        let Some(obj) = stage.as_object().filter(|obj| obj.len() == 1) else {
            return Err(Error::Query(
                "$merge whenMatched stages must contain exactly one operator".into(),
            ));
        };
        let Some(name) = obj.keys().next().map(String::as_str) else {
            return Err(Error::Query("$merge whenMatched stage is empty".into()));
        };
        if !matches!(
            name,
            "$set" | "$addFields" | "$project" | "$unset" | "$replaceRoot" | "$replaceWith"
        ) {
            return Err(Error::Query(format!(
                "unsupported $merge whenMatched stage `{name}`"
            )));
        }
    }
    Ok(())
}

/// Parses a terminal `$out` or `$merge` stage. Returns `None` for read-only
/// pipelines. A write stage anywhere except the last position is rejected.
pub fn write_stage_spec(pipeline: &[Value]) -> Result<Option<AggregateWriteSpec>> {
    let mut found = None;
    for (index, stage) in pipeline.iter().enumerate() {
        if let Some(key) = write_stage_key(stage) {
            if found.is_some() {
                return Err(Error::Query(
                    "aggregation may contain only one write stage".into(),
                ));
            }
            let obj = stage.as_object().expect("write stage object");
            if obj.len() != 1 {
                return Err(Error::Query(
                    "write stage must contain exactly one operator".into(),
                ));
            }
            found = Some((index, key, obj.get(key).expect("write stage argument")));
        }
    }
    let Some((index, key, arg)) = found else {
        return Ok(None);
    };
    if index + 1 != pipeline.len() {
        return Err(Error::Query(format!(
            "{key} must be the last stage in the pipeline"
        )));
    }
    if key == "$out" {
        let (target, database) = parse_write_target(arg, "$out")?;
        return Ok(Some(AggregateWriteSpec {
            kind: AggregateWriteKind::Out,
            target,
            database,
            on: vec!["_id".to_string()],
            when_matched: MergeWhenMatched::Merge,
            when_not_matched: MergeWhenNotMatched::Insert,
            let_vars: Vec::new(),
        }));
    }
    let obj = arg
        .as_object()
        .ok_or_else(|| Error::Query("$merge requires an object".into()))?;
    let into = obj
        .get("into")
        .ok_or_else(|| Error::Query("$merge requires `into`".into()))?;
    let (target, database) = parse_write_target(into, "$merge")?;
    let on = parse_merge_on(obj.get("on"))?;
    let when_matched = match obj.get("whenMatched") {
        None => MergeWhenMatched::Merge,
        Some(Value::String(value)) => match value.as_str() {
            "replace" => MergeWhenMatched::Replace,
            "keepExisting" => MergeWhenMatched::KeepExisting,
            "merge" => MergeWhenMatched::Merge,
            "fail" => MergeWhenMatched::Fail,
            _ => {
                return Err(Error::Query(format!(
                    "unsupported $merge whenMatched mode `{value}`"
                )));
            }
        },
        Some(Value::Array(stages)) => {
            validate_merge_pipeline(stages)?;
            MergeWhenMatched::Pipeline(stages.clone())
        }
        Some(_) => {
            return Err(Error::Query(
                "$merge `whenMatched` must be a string or pipeline".into(),
            ));
        }
    };
    let when_not_matched = match obj.get("whenNotMatched") {
        None => MergeWhenNotMatched::Insert,
        Some(Value::String(value)) => match value.as_str() {
            "insert" => MergeWhenNotMatched::Insert,
            "discard" => MergeWhenNotMatched::Discard,
            "fail" => MergeWhenNotMatched::Fail,
            _ => {
                return Err(Error::Query(format!(
                    "unsupported $merge whenNotMatched mode `{value}`"
                )));
            }
        },
        Some(_) => {
            return Err(Error::Query(
                "$merge `whenNotMatched` must be a string".into(),
            ));
        }
    };
    let let_vars = match obj.get("let") {
        None => Vec::new(),
        Some(Value::Object(vars)) => vars
            .iter()
            .map(|(name, value)| {
                if name.is_empty() {
                    return Err(Error::Query("$merge `let` names must not be empty".into()));
                }
                Ok((name.clone(), value.clone()))
            })
            .collect::<Result<Vec<_>>>()?,
        Some(_) => return Err(Error::Query("$merge `let` must be an object".into())),
    };
    Ok(Some(AggregateWriteSpec {
        kind: AggregateWriteKind::Merge,
        target,
        database,
        on,
        when_matched,
        when_not_matched,
        let_vars,
    }))
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
    fn lookup(
        &self,
        docs: Vec<Value>,
        spec: &Value,
        vars: &BTreeMap<String, Value>,
    ) -> Result<Vec<Value>>;

    fn union_with(&self, spec: &Value, vars: &BTreeMap<String, Value>) -> Result<Vec<Value>>;

    fn graph_lookup(
        &self,
        docs: Vec<Value>,
        spec: &Value,
        vars: &BTreeMap<String, Value>,
    ) -> Result<Vec<Value>>;
}

/// Runs a pipeline over already selected documents.
pub fn run(docs: Vec<Value>, pipeline: &[Value], max_docs: usize) -> Result<Vec<Value>> {
    run_with_host_vars(docs, pipeline, max_docs, None, &BTreeMap::new())
}

/// Runs a pipeline with `$$variables` bound for expressions.
pub fn run_with_vars(
    docs: Vec<Value>,
    pipeline: &[Value],
    max_docs: usize,
    vars: &BTreeMap<String, Value>,
) -> Result<Vec<Value>> {
    run_with_host_vars(docs, pipeline, max_docs, None, vars)
}

/// Evaluates an aggregation expression with `$$variables` bound.
pub fn eval_expression(
    doc: &Value,
    expression: &Value,
    vars: &BTreeMap<String, Value>,
) -> Result<Value> {
    eval_expr_vars(doc, expression, vars)
}

pub(crate) fn run_with_host(
    docs: Vec<Value>,
    pipeline: &[Value],
    max_docs: usize,
    host: Option<&dyn AggregateHost>,
) -> Result<Vec<Value>> {
    run_with_host_vars(docs, pipeline, max_docs, host, &BTreeMap::new())
}

pub(crate) fn run_with_host_vars(
    mut docs: Vec<Value>,
    pipeline: &[Value],
    max_docs: usize,
    host: Option<&dyn AggregateHost>,
    vars: &BTreeMap<String, Value>,
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
                    "$project" => project(doc, arg, vars)?,
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
                            keep = matches_with_vars(&doc, arg, vars);
                            doc
                        }
                        "$project" => project(doc, arg, vars)?,
                        "$addFields" | "$set" => add_fields(doc, arg, vars)?,
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
                        .map(|doc| match eval_expr_vars(&doc, expression, vars)? {
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
                        .map(|doc| match eval_expr_vars(&doc, arg, vars)? {
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
                    docs = group(docs, &group_arg, vars)?;
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
                    docs = host.lookup(docs, arg, vars)?;
                    i += 1;
                }
                "$unionWith" => {
                    let host = host.ok_or_else(|| {
                        Error::Query("$unionWith requires a collection-aware host".into())
                    })?;
                    let mut union = host.union_with(arg, vars)?;
                    docs.append(&mut union);
                    if docs.len() > max_docs {
                        return Err(Error::Query(format!(
                            "$unionWith result exceeds {max_docs} documents"
                        )));
                    }
                    i += 1;
                }
                "$graphLookup" => {
                    let host = host.ok_or_else(|| {
                        Error::Query("$graphLookup requires a collection-aware host".into())
                    })?;
                    docs = host.graph_lookup(docs, arg, vars)?;
                    i += 1;
                }
                "$setWindowFields" => {
                    docs = set_window_fields(docs, arg, vars)?;
                    i += 1;
                }
                "$densify" => {
                    docs = densify(docs, arg, vars)?;
                    i += 1;
                }
                "$fill" => {
                    docs = fill(docs, arg, vars)?;
                    i += 1;
                }
                "$out" | "$merge" => {
                    return Err(Error::Query(format!(
                        "{name} must be executed by the writer"
                    )));
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
                    docs = bucket(docs, arg, vars)?;
                    i += 1;
                }
                "$bucketAuto" => {
                    docs = bucket_auto(docs, arg, vars)?;
                    i += 1;
                }
                "$group" => {
                    docs = group(docs, arg, vars)?;
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
    if let Some(value) = numeric::integer_value(value) {
        return usize::try_from(value)
            .map_err(|_| Error::Query(format!("{stage} requires a non-negative integer")));
    }
    value
        .as_f64()
        .filter(|value| value.is_finite() && value.fract() == 0.0 && *value >= 0.0)
        .and_then(|value| usize::try_from(value as u64).ok())
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
    matches!(value, Value::Bool(false)) || crate::numeric::as_f64(value).is_some_and(|n| n == 0.0)
}

fn is_inclusion(value: &Value) -> bool {
    matches!(value, Value::Bool(true)) || crate::numeric::as_f64(value).is_some_and(|n| n == 1.0)
}

fn project(doc: Value, spec: &Value, vars: &BTreeMap<String, Value>) -> Result<Value> {
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
                let value = eval_expr_vars(&doc, value, vars)?;
                path::set(&mut out, field, value)?;
                continue;
            }
            if is_exclusion(value) {
                continue;
            }
            let value = if is_inclusion(value) {
                path::get(&doc, field).cloned().unwrap_or(Value::Null)
            } else {
                eval_expr_vars(&doc, value, vars)?
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

fn add_fields(mut doc: Value, spec: &Value, vars: &BTreeMap<String, Value>) -> Result<Value> {
    let spec = spec
        .as_object()
        .ok_or_else(|| Error::Query("$addFields requires an object".into()))?;
    // Evaluate every expression against the original document first; later
    // expressions must not observe fields added by earlier entries.
    let mut updates = Vec::with_capacity(spec.len());
    for (field, expr) in spec {
        updates.push((field, eval_expr_vars(&doc, expr, vars)?));
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
                let dir = numeric::as_f64(dir)
                    .filter(|d| *d == 1.0 || *d == -1.0)
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
    vars: BTreeMap<String, Value>,
}

#[derive(Default)]
struct GroupState {
    id: Value,
    values: Vec<Accumulator>,
}

impl GroupBuilder {
    pub(crate) fn new(spec: &Value) -> Result<Self> {
        Self::new_with_vars(spec, &BTreeMap::new())
    }

    pub(crate) fn new_with_vars(spec: &Value, vars: &BTreeMap<String, Value>) -> Result<Self> {
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
            vars: vars.clone(),
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
        let id = eval_expr_vars(&doc, &self.id_expr, &self.vars)?;
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
                eval_expr_vars(&doc, arg, &self.vars)?
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

fn bucket(docs: Vec<Value>, spec: &Value, vars: &BTreeMap<String, Value>) -> Result<Vec<Value>> {
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
    let mut builder = GroupBuilder::new_with_vars(&group_spec, vars)?;
    for doc in docs {
        let value = eval_expr_vars(&doc, group_by, vars)?;
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

fn bucket_auto(
    docs: Vec<Value>,
    spec: &Value,
    vars: &BTreeMap<String, Value>,
) -> Result<Vec<Value>> {
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
        .map(|doc| Ok((eval_expr_vars(&doc, group_by, vars)?, doc)))
        .collect::<Result<Vec<_>>>()?;
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    entries.sort_by(|a, b| compare_value(&a.0, &b.0));
    let groups = buckets.min(entries.len());
    let mut builder = GroupBuilder::new_with_vars(&group_spec, vars)?;
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

fn group(docs: Vec<Value>, spec: &Value, vars: &BTreeMap<String, Value>) -> Result<Vec<Value>> {
    let mut builder = GroupBuilder::new_with_vars(spec, vars)?;
    for doc in docs {
        builder.update(doc)?;
    }
    builder.finish()
}

#[derive(Clone, Debug)]
enum WindowBound {
    Unbounded,
    Current,
    Offset(i64),
}

#[derive(Clone, Debug)]
enum WindowRangeBound {
    Unbounded,
    Current,
    Offset(Value),
}

#[derive(Clone, Debug)]
enum WindowFrame {
    Documents {
        start: WindowBound,
        end: WindowBound,
    },
    Range {
        start: WindowRangeBound,
        end: WindowRangeBound,
        unit: Option<String>,
    },
}

#[derive(Clone, Debug)]
enum WindowOp {
    Accumulator(String, Value),
    Rank,
    DenseRank,
    DocumentNumber,
    Shift {
        output: Value,
        by: i64,
        default: Option<Value>,
    },
    ExpMovingAvg {
        input: Value,
        alpha: f64,
    },
    Derivative {
        input: Value,
        unit: Option<String>,
    },
    Integral {
        input: Value,
        unit: Option<String>,
    },
}

fn valid_date_unit(unit: &str) -> bool {
    matches!(
        unit,
        "year"
            | "quarter"
            | "month"
            | "week"
            | "day"
            | "hour"
            | "minute"
            | "second"
            | "millisecond"
    )
}

struct WindowOutput {
    field: String,
    op: WindowOp,
    window: Option<WindowFrame>,
}

fn parse_window_bound(value: &Value) -> Result<WindowBound> {
    match value {
        Value::String(value) if value == "unbounded" => Ok(WindowBound::Unbounded),
        Value::String(value) if value == "current" => Ok(WindowBound::Current),
        Value::Number(number) => number
            .as_i64()
            .map(WindowBound::Offset)
            .ok_or_else(|| Error::Query("window document bounds must be integers".into())),
        _ => Err(Error::Query(
            "window bounds must be `unbounded`, `current`, or an integer".into(),
        )),
    }
}

fn parse_window_range_bound(value: &Value) -> Result<WindowRangeBound> {
    match value {
        Value::String(value) if value == "unbounded" => Ok(WindowRangeBound::Unbounded),
        Value::String(value) if value == "current" => Ok(WindowRangeBound::Current),
        Value::Number(_) => Ok(WindowRangeBound::Offset(value.clone())),
        _ => Err(Error::Query(
            "range window bounds must be `unbounded`, `current`, or a number".into(),
        )),
    }
}

fn parse_window_frame(value: &Value) -> Result<WindowFrame> {
    let obj = value
        .as_object()
        .ok_or_else(|| Error::Query("window must be an object".into()))?;
    let has_documents = obj.contains_key("documents");
    let has_range = obj.contains_key("range");
    if has_documents && has_range {
        return Err(Error::Query(
            "window must contain either `documents` or `range`, not both".into(),
        ));
    }
    if has_range {
        let bounds = obj
            .get("range")
            .and_then(Value::as_array)
            .filter(|bounds| bounds.len() == 2)
            .ok_or_else(|| Error::Query("window requires [lower, upper] range bounds".into()))?;
        let unit = match obj.get("unit") {
            None => None,
            Some(Value::String(unit)) if valid_date_unit(unit) => Some(unit.clone()),
            Some(_) => {
                return Err(Error::Query(
                    "range window `unit` must be a supported date unit".into(),
                ));
            }
        };
        return Ok(WindowFrame::Range {
            start: parse_window_range_bound(&bounds[0])?,
            end: parse_window_range_bound(&bounds[1])?,
            unit,
        });
    }
    if obj.contains_key("unit") {
        return Err(Error::Query(
            "window `unit` is only valid with a `range` window".into(),
        ));
    }
    let bounds = obj
        .get("documents")
        .and_then(Value::as_array)
        .filter(|bounds| bounds.len() == 2)
        .ok_or_else(|| Error::Query("window requires [lower, upper] document bounds".into()))?;
    Ok(WindowFrame::Documents {
        start: parse_window_bound(&bounds[0])?,
        end: parse_window_bound(&bounds[1])?,
    })
}

fn parse_window_op(name: &str, arg: &Value) -> Result<WindowOp> {
    match name {
        "$sum" | "$avg" | "$min" | "$max" | "$first" | "$last" | "$push" | "$addToSet"
        | "$count" => Ok(WindowOp::Accumulator(name.to_string(), arg.clone())),
        "$rank" | "$denseRank" | "$documentNumber" => {
            if !arg.is_object() {
                return Err(Error::Query(format!("{name} requires an object argument")));
            }
            Ok(match name {
                "$rank" => WindowOp::Rank,
                "$denseRank" => WindowOp::DenseRank,
                _ => WindowOp::DocumentNumber,
            })
        }
        "$shift" => {
            let spec = arg
                .as_object()
                .ok_or_else(|| Error::Query("$shift requires an object".into()))?;
            let output = spec
                .get("output")
                .cloned()
                .ok_or_else(|| Error::Query("$shift requires `output`".into()))?;
            let by = spec
                .get("by")
                .and_then(Value::as_i64)
                .ok_or_else(|| Error::Query("$shift requires an integer `by`".into()))?;
            Ok(WindowOp::Shift {
                output,
                by,
                default: spec.get("default").cloned(),
            })
        }
        "$expMovingAvg" => {
            let spec = arg
                .as_object()
                .ok_or_else(|| Error::Query("$expMovingAvg requires an object".into()))?;
            let input = spec
                .get("input")
                .cloned()
                .ok_or_else(|| Error::Query("$expMovingAvg requires `input`".into()))?;
            let (n, alpha) = (spec.get("N"), spec.get("alpha"));
            let alpha = match (n, alpha) {
                (Some(_), Some(_)) => {
                    return Err(Error::Query(
                        "$expMovingAvg requires exactly one of `N` or `alpha`".into(),
                    ));
                }
                (Some(n), None) => {
                    let n = n
                        .as_i64()
                        .filter(|n| *n > 0)
                        .ok_or_else(|| Error::Query("$expMovingAvg `N` must be positive".into()))?;
                    2.0 / (n as f64 + 1.0)
                }
                (None, Some(alpha)) => {
                    let alpha = alpha.as_f64().ok_or_else(|| {
                        Error::Query("$expMovingAvg `alpha` must be a number".into())
                    })?;
                    if !(alpha > 0.0 && alpha < 1.0) {
                        return Err(Error::Query(
                            "$expMovingAvg `alpha` must be between 0 and 1".into(),
                        ));
                    }
                    alpha
                }
                (None, None) => {
                    return Err(Error::Query("$expMovingAvg requires `N` or `alpha`".into()));
                }
            };
            Ok(WindowOp::ExpMovingAvg { input, alpha })
        }
        "$derivative" | "$integral" => {
            let spec = arg
                .as_object()
                .ok_or_else(|| Error::Query(format!("{name} requires an object argument")))?;
            let input = spec
                .get("input")
                .cloned()
                .ok_or_else(|| Error::Query(format!("{name} requires `input`")))?;
            let unit = match spec.get("unit") {
                None => None,
                Some(Value::String(unit)) if valid_date_unit(unit) => Some(unit.clone()),
                Some(_) => {
                    return Err(Error::Query(format!(
                        "{name} `unit` must be a supported date unit"
                    )));
                }
            };
            Ok(if name == "$derivative" {
                WindowOp::Derivative { input, unit }
            } else {
                WindowOp::Integral { input, unit }
            })
        }
        _ => Err(Error::Query(format!(
            "unsupported $setWindowFields operator `{name}`"
        ))),
    }
}

fn parse_window_outputs(value: &Value) -> Result<Vec<WindowOutput>> {
    let outputs = value
        .as_object()
        .ok_or_else(|| Error::Query("$setWindowFields requires an `output` object".into()))?;
    if outputs.is_empty() {
        return Err(Error::Query(
            "$setWindowFields `output` must not be empty".into(),
        ));
    }
    let mut parsed = Vec::with_capacity(outputs.len());
    for (field, spec) in outputs {
        let spec = spec
            .as_object()
            .ok_or_else(|| Error::Query("$setWindowFields output fields must be objects".into()))?;
        let mut op = None;
        let mut window = None;
        for (name, value) in spec {
            if name == "window" {
                if window.is_some() {
                    return Err(Error::Query(
                        "$setWindowFields output has duplicate `window`".into(),
                    ));
                }
                window = Some(parse_window_frame(value)?);
                continue;
            }
            if op.is_some() {
                return Err(Error::Query(
                    "$setWindowFields output must contain one operator".into(),
                ));
            }
            op = Some(parse_window_op(name, value)?);
        }
        let op = op.ok_or_else(|| {
            Error::Query("$setWindowFields output field requires an operator".into())
        })?;
        if window.is_some()
            && matches!(
                op,
                WindowOp::Rank
                    | WindowOp::DenseRank
                    | WindowOp::DocumentNumber
                    | WindowOp::Shift { .. }
                    | WindowOp::ExpMovingAvg { .. }
            )
        {
            return Err(Error::Query(
                "$setWindowFields `window` is not valid for this operator".into(),
            ));
        }
        parsed.push(WindowOutput {
            field: field.clone(),
            op,
            window,
        });
    }
    Ok(parsed)
}

fn resolve_window_bound(bound: &WindowBound, current: usize, len: usize, start: bool) -> i64 {
    match bound {
        WindowBound::Unbounded => {
            if start {
                0
            } else {
                len.saturating_sub(1) as i64
            }
        }
        WindowBound::Current => current as i64,
        WindowBound::Offset(offset) => current as i64 + offset,
    }
}

fn window_slice(
    frame: &WindowFrame,
    current: usize,
    len: usize,
) -> Option<std::ops::RangeInclusive<usize>> {
    let WindowFrame::Documents { start, end } = frame else {
        return None;
    };
    if len == 0 {
        return None;
    }
    let mut start = resolve_window_bound(start, current, len, true);
    let mut end = resolve_window_bound(end, current, len, false);
    start = start.max(0);
    end = end.min(len.saturating_sub(1) as i64);
    if start > end || start >= len as i64 || end < 0 {
        return None;
    }
    Some(start as usize..=end as usize)
}

#[derive(Clone)]
enum RangeValue {
    Number(BigDecimal),
    Date(i64),
}

fn range_sort_value(doc: &Value, field: &str) -> Result<Option<RangeValue>> {
    let Some(value) = path::get(doc, field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    if date::is_date(value) {
        return Ok(Some(RangeValue::Date(date_millis(value)?)));
    }
    if let Some(number) = numeric::as_bigdecimal(value) {
        return Ok(Some(RangeValue::Number(number)));
    }
    Err(Error::Query(
        "range windows require a numeric or date sortBy field".into(),
    ))
}

fn range_bound_value(
    bound: &WindowRangeBound,
    current: &RangeValue,
    unit: Option<&str>,
) -> Result<Option<RangeValue>> {
    match bound {
        WindowRangeBound::Unbounded => Ok(None),
        WindowRangeBound::Current => Ok(Some(current.clone())),
        WindowRangeBound::Offset(offset) => match current {
            RangeValue::Number(current) => {
                if unit.is_some() {
                    return Err(Error::Query(
                        "range window `unit` is only valid for date sortBy fields".into(),
                    ));
                }
                let offset = numeric::as_bigdecimal(offset)
                    .ok_or_else(|| Error::Query("range offsets must be numbers".into()))?;
                Ok(Some(RangeValue::Number(current + offset)))
            }
            RangeValue::Date(current) => {
                let unit =
                    unit.ok_or_else(|| Error::Query("date range windows require a `unit`".into()))?;
                let offset = offset.as_i64().ok_or_else(|| {
                    Error::Query("date range offsets must be integer numbers".into())
                })?;
                Ok(Some(RangeValue::Date(densify_add_date(
                    *current, offset, unit,
                )?)))
            }
        },
    }
}

fn range_value_cmp(value: &RangeValue, bound: &RangeValue) -> Result<Ordering> {
    match (value, bound) {
        (RangeValue::Number(left), RangeValue::Number(right)) => Ok(left.cmp(right)),
        (RangeValue::Date(left), RangeValue::Date(right)) => Ok(left.cmp(right)),
        _ => Err(Error::Query(
            "range window sortBy values must have one consistent type".into(),
        )),
    }
}

fn range_window_positions(
    start: &WindowRangeBound,
    end: &WindowRangeBound,
    unit: Option<&str>,
    docs: &[Value],
    indices: &[usize],
    position: usize,
    sort: &[(String, i32)],
) -> Result<Vec<usize>> {
    let Some((field, _)) = sort.first().filter(|_| sort.len() == 1) else {
        return Err(Error::Query(
            "range windows require exactly one sortBy field".into(),
        ));
    };
    let Some(current) = range_sort_value(&docs[indices[position]], field)? else {
        return Err(Error::Query(
            "range window sortBy value is missing or null".into(),
        ));
    };
    let lower = range_bound_value(start, &current, unit)?;
    let upper = range_bound_value(end, &current, unit)?;
    let mut out = Vec::new();
    for (candidate_position, index) in indices.iter().enumerate() {
        let Some(value) = range_sort_value(&docs[*index], field)? else {
            continue;
        };
        if let Some(lower) = &lower {
            if range_value_cmp(&value, lower)? == Ordering::Less {
                continue;
            }
        }
        if let Some(upper) = &upper {
            if range_value_cmp(&value, upper)? == Ordering::Greater {
                continue;
            }
        }
        out.push(candidate_position);
    }
    Ok(out)
}

fn window_frame_positions(
    frame: &WindowFrame,
    docs: &[Value],
    indices: &[usize],
    position: usize,
    sort: &[(String, i32)],
) -> Result<Vec<usize>> {
    match frame {
        WindowFrame::Documents { .. } => Ok(window_slice(frame, position, indices.len())
            .map(|range| range.collect())
            .unwrap_or_default()),
        WindowFrame::Range { start, end, unit } => {
            range_window_positions(start, end, unit.as_deref(), docs, indices, position, sort)
        }
    }
}

fn window_accumulator_values(
    docs: &[Value],
    indices: &[usize],
    op: &str,
    expression: &Value,
    window: Option<&WindowFrame>,
    sort: &[(String, i32)],
    vars: &BTreeMap<String, Value>,
) -> Result<Vec<Value>> {
    let mut out = Vec::with_capacity(indices.len());
    if let Some(window) = window {
        for position in 0..indices.len() {
            let mut accumulator = Accumulator::new(op);
            for frame_position in window_frame_positions(window, docs, indices, position, sort)? {
                let frame_index = indices[frame_position];
                let value = if op == "$count" {
                    Value::Null
                } else {
                    eval_expr_vars(&docs[frame_index], expression, vars)?
                };
                accumulator.update(op, &value)?;
            }
            out.push(accumulator.finish()?);
        }
        return Ok(out);
    }

    // MongoDB's default window is unbounded to unbounded: the accumulator sees
    // the whole partition and every row receives the same result.
    let mut accumulator = Accumulator::new(op);
    for index in indices {
        let value = if op == "$count" {
            Value::Null
        } else {
            eval_expr_vars(&docs[*index], expression, vars)?
        };
        accumulator.update(op, &value)?;
    }
    let value = accumulator.finish()?;
    out.resize(indices.len(), value);
    Ok(out)
}

fn window_rank_values(
    docs: &[Value],
    indices: &[usize],
    sort: &[(String, i32)],
    dense: bool,
) -> Vec<Value> {
    let mut out = Vec::with_capacity(indices.len());
    let mut rank = 1usize;
    let mut dense_rank = 1usize;
    for position in 0..indices.len() {
        if position > 0 {
            let previous = &docs[indices[position - 1]];
            let current = &docs[indices[position]];
            if compare_docs(previous, current, sort) != Ordering::Equal {
                rank = position + 1;
                dense_rank += 1;
            }
        }
        out.push(Value::from(if dense { dense_rank } else { rank }));
    }
    out
}

fn window_shift_values(
    docs: &[Value],
    indices: &[usize],
    output: &Value,
    by: i64,
    default: Option<&Value>,
    vars: &BTreeMap<String, Value>,
) -> Result<Vec<Value>> {
    let mut out = Vec::with_capacity(indices.len());
    for (position, index) in indices.iter().enumerate() {
        let target = position as i64 + by;
        if target >= 0 && (target as usize) < indices.len() {
            out.push(eval_expr_vars(
                &docs[indices[target as usize]],
                output,
                vars,
            )?);
        } else {
            out.push(match default {
                Some(default) => eval_expr_vars(&docs[*index], default, vars)?,
                None => Value::Null,
            });
        }
    }
    Ok(out)
}

fn window_exp_moving_avg_values(
    docs: &[Value],
    indices: &[usize],
    input: &Value,
    alpha: f64,
    vars: &BTreeMap<String, Value>,
) -> Result<Vec<Value>> {
    let mut out = Vec::with_capacity(indices.len());
    let mut previous: Option<f64> = None;
    for index in indices {
        let value = eval_expr_vars(&docs[*index], input, vars)?;
        let Some(value) = numeric::as_f64(&value) else {
            out.push(Value::Null);
            continue;
        };
        let current = match previous {
            Some(previous) => alpha * value + (1.0 - alpha) * previous,
            None => value,
        };
        previous = Some(current);
        out.push(number_value(current));
    }
    Ok(out)
}

fn date_unit_millis(unit: &str) -> f64 {
    let day = 86_400_000.0;
    match unit {
        "year" => day * 365.0,
        "quarter" => day * 365.0 / 4.0,
        "month" => day * 365.0 / 12.0,
        "week" => day * 7.0,
        "day" => day,
        "hour" => 3_600_000.0,
        "minute" => 60_000.0,
        "second" => 1_000.0,
        _ => 1.0,
    }
}

fn window_x_value(
    doc: &Value,
    field: &str,
    unit: Option<&str>,
    operation: &str,
) -> Result<Option<f64>> {
    let Some(value) = path::get(doc, field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    if date::is_date(value) {
        let millis = date_millis(value)? as f64;
        return Ok(Some(match unit {
            Some(unit) => millis / date_unit_millis(unit),
            None => millis,
        }));
    }
    if unit.is_some() {
        return Err(Error::Query(format!(
            "{operation} `unit` is only valid for date sortBy fields"
        )));
    }
    Ok(numeric::as_f64(value))
}

fn window_frame_positions_or_all(
    docs: &[Value],
    indices: &[usize],
    position: usize,
    sort: &[(String, i32)],
    window: Option<&WindowFrame>,
) -> Result<Vec<usize>> {
    match window {
        Some(window) => window_frame_positions(window, docs, indices, position, sort),
        None => Ok((0..indices.len()).collect()),
    }
}

fn window_derivative_values(
    docs: &[Value],
    indices: &[usize],
    input: &Value,
    unit: Option<&str>,
    window: Option<&WindowFrame>,
    sort: &[(String, i32)],
    vars: &BTreeMap<String, Value>,
) -> Result<Vec<Value>> {
    let Some((field, _)) = sort.first().filter(|_| sort.len() == 1) else {
        return Err(Error::Query(
            "$derivative requires exactly one sortBy field".into(),
        ));
    };
    let mut out = Vec::with_capacity(indices.len());
    for position in 0..indices.len() {
        let frame = window_frame_positions_or_all(docs, indices, position, sort, window)?;
        let (Some(first), Some(last)) = (frame.first(), frame.last()) else {
            out.push(Value::Null);
            continue;
        };
        let first_doc = &docs[indices[*first]];
        let last_doc = &docs[indices[*last]];
        let (Some(x0), Some(x1)) = (
            window_x_value(first_doc, field, unit, "$derivative")?,
            window_x_value(last_doc, field, unit, "$derivative")?,
        ) else {
            out.push(Value::Null);
            continue;
        };
        let (Some(y0), Some(y1)) = (
            numeric::as_f64(&eval_expr_vars(first_doc, input, vars)?),
            numeric::as_f64(&eval_expr_vars(last_doc, input, vars)?),
        ) else {
            out.push(Value::Null);
            continue;
        };
        if x1 == x0 {
            out.push(Value::Null);
        } else {
            out.push(number_value((y1 - y0) / (x1 - x0)));
        }
    }
    Ok(out)
}

fn window_integral_values(
    docs: &[Value],
    indices: &[usize],
    input: &Value,
    unit: Option<&str>,
    window: Option<&WindowFrame>,
    sort: &[(String, i32)],
    vars: &BTreeMap<String, Value>,
) -> Result<Vec<Value>> {
    let Some((field, _)) = sort.first().filter(|_| sort.len() == 1) else {
        return Err(Error::Query(
            "$integral requires exactly one sortBy field".into(),
        ));
    };
    let mut out = Vec::with_capacity(indices.len());
    for position in 0..indices.len() {
        let frame = window_frame_positions_or_all(docs, indices, position, sort, window)?;
        if frame.len() < 2 {
            out.push(Value::from(0));
            continue;
        }
        let mut total = 0.0;
        let mut valid = true;
        for pair in frame.windows(2) {
            let left = &docs[indices[pair[0]]];
            let right = &docs[indices[pair[1]]];
            let (Some(x0), Some(x1)) = (
                window_x_value(left, field, unit, "$integral")?,
                window_x_value(right, field, unit, "$integral")?,
            ) else {
                valid = false;
                break;
            };
            let (Some(y0), Some(y1)) = (
                numeric::as_f64(&eval_expr_vars(left, input, vars)?),
                numeric::as_f64(&eval_expr_vars(right, input, vars)?),
            ) else {
                valid = false;
                break;
            };
            total += (x1 - x0) * (y0 + y1) / 2.0;
        }
        out.push(if valid {
            number_value(total)
        } else {
            Value::Null
        });
    }
    Ok(out)
}

fn window_output_values(
    docs: &[Value],
    indices: &[usize],
    output: &WindowOutput,
    sort: &[(String, i32)],
    vars: &BTreeMap<String, Value>,
) -> Result<Vec<Value>> {
    match &output.op {
        WindowOp::Accumulator(op, expression) => window_accumulator_values(
            docs,
            indices,
            op,
            expression,
            output.window.as_ref(),
            sort,
            vars,
        ),
        WindowOp::Rank => Ok(window_rank_values(docs, indices, sort, false)),
        WindowOp::DenseRank => Ok(window_rank_values(docs, indices, sort, true)),
        WindowOp::DocumentNumber => Ok((1..=indices.len()).map(Value::from).collect()),
        WindowOp::Shift {
            output,
            by,
            default,
        } => window_shift_values(docs, indices, output, *by, default.as_ref(), vars),
        WindowOp::ExpMovingAvg { input, alpha } => {
            window_exp_moving_avg_values(docs, indices, input, *alpha, vars)
        }
        WindowOp::Derivative { input, unit } => window_derivative_values(
            docs,
            indices,
            input,
            unit.as_deref(),
            output.window.as_ref(),
            sort,
            vars,
        ),
        WindowOp::Integral { input, unit } => window_integral_values(
            docs,
            indices,
            input,
            unit.as_deref(),
            output.window.as_ref(),
            sort,
            vars,
        ),
    }
}

fn set_window_fields(
    mut docs: Vec<Value>,
    spec: &Value,
    vars: &BTreeMap<String, Value>,
) -> Result<Vec<Value>> {
    let spec = spec
        .as_object()
        .ok_or_else(|| Error::Query("$setWindowFields requires an object".into()))?;
    let outputs = parse_window_outputs(
        spec.get("output")
            .ok_or_else(|| Error::Query("$setWindowFields requires `output`".into()))?,
    )?;
    let sort = match spec.get("sortBy") {
        Some(sort) => parse_sort(sort)?,
        None => Vec::new(),
    };
    let needs_sort = outputs.iter().any(|output| {
        matches!(
            output.op,
            WindowOp::Rank
                | WindowOp::DenseRank
                | WindowOp::DocumentNumber
                | WindowOp::Shift { .. }
                | WindowOp::ExpMovingAvg { .. }
                | WindowOp::Derivative { .. }
                | WindowOp::Integral { .. }
        ) || matches!(
            &output.op,
            WindowOp::Accumulator(op, _) if op == "$first" || op == "$last"
        ) || matches!(&output.window, Some(WindowFrame::Range { .. }))
    });
    if needs_sort && sort.is_empty() {
        return Err(Error::Query(
            "$setWindowFields ranking, $shift, $first, $last, range, $expMovingAvg, $derivative, and $integral require `sortBy`"
                .into(),
        ));
    }
    let needs_single_sort = outputs.iter().any(|output| {
        matches!(
            output.op,
            WindowOp::Derivative { .. } | WindowOp::Integral { .. }
        ) || matches!(&output.window, Some(WindowFrame::Range { .. }))
    });
    if needs_single_sort && sort.len() != 1 {
        return Err(Error::Query(
            "$setWindowFields range, $derivative, and $integral require exactly one sortBy field"
                .into(),
        ));
    }

    let mut partitions: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (index, doc) in docs.iter().enumerate() {
        let key = match spec.get("partitionBy") {
            Some(expression) => eval_expr_vars(doc, expression, vars)?,
            None => Value::Null,
        };
        partitions
            .entry(serde_json::to_string(&key)?)
            .or_default()
            .push(index);
    }

    for indices in partitions.values_mut() {
        if !sort.is_empty() {
            indices.sort_by(|left, right| compare_docs(&docs[*left], &docs[*right], &sort));
        }
        let mut results = Vec::with_capacity(outputs.len());
        for output in &outputs {
            results.push(window_output_values(&docs, indices, output, &sort, vars)?);
        }
        for (output, values) in outputs.iter().zip(results) {
            for (index, value) in indices.iter().zip(values) {
                path::set(&mut docs[*index], &output.field, value)?;
            }
        }
    }
    Ok(docs)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DensifyKind {
    Number,
    Date,
}

#[derive(Clone)]
enum DensifyBounds {
    Partition,
    Full,
    Explicit(Value, Value),
}

fn densify_add_date(ms: i64, step: i64, unit: &str) -> Result<i64> {
    let value = date_from_millis(ms)?;
    let result = match unit {
        "year" => add_date_months(
            value,
            step.checked_mul(12)
                .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?,
        )?,
        "quarter" => add_date_months(
            value,
            step.checked_mul(3)
                .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?,
        )?,
        "month" => add_date_months(value, step)?,
        "week" => value
            .checked_add(Duration::weeks(step))
            .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?,
        "day" => value
            .checked_add(Duration::days(step))
            .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?,
        "hour" => value
            .checked_add(Duration::hours(step))
            .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?,
        "minute" => value
            .checked_add(Duration::minutes(step))
            .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?,
        "second" => value
            .checked_add(Duration::seconds(step))
            .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?,
        "millisecond" => value
            .checked_add(Duration::milliseconds(step))
            .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?,
        _ => return Err(Error::Query(format!("unsupported date unit `{unit}`"))),
    };
    date_to_millis(result)
}

fn densify_value_kind(value: &Value, date_mode: bool) -> Result<()> {
    if date_mode {
        if date::is_date(value) {
            Ok(())
        } else {
            Err(Error::Query(
                "$densify with `unit` requires date field values".into(),
            ))
        }
    } else if numeric::as_bigdecimal(value).is_some() {
        Ok(())
    } else {
        Err(Error::Query(
            "$densify without `unit` requires numeric field values".into(),
        ))
    }
}

fn densify_key(value: &Value, date_mode: bool) -> Result<String> {
    if date_mode {
        serde_json::to_string(&date::canonical(date_millis(value)?)).map_err(Into::into)
    } else {
        let value = numeric::as_bigdecimal(value)
            .ok_or_else(|| Error::Query("$densify requires numeric values".into()))?;
        serde_json::to_string(&numeric::decimal_value(&value)?).map_err(Into::into)
    }
}

fn densify_number_value(value: &BigDecimal) -> Result<Value> {
    let normalized = value.normalized();
    let text = normalized.to_string();
    if !text.contains(['.', 'e', 'E']) {
        if let Ok(value) = text.parse::<i128>() {
            return numeric::integer_json(value);
        }
    }
    numeric::decimal_value(value)
}

fn densify_partition_key(doc: &Value, fields: &[String]) -> Result<String> {
    let mut values = Vec::with_capacity(fields.len());
    for field in fields {
        values.push(path::get(doc, field).cloned().unwrap_or(Value::Null));
    }
    Ok(serde_json::to_string(&Value::Array(values))?)
}

fn densify_partition_doc(doc: &Value, fields: &[String]) -> Result<Value> {
    let mut out = Value::Object(Map::new());
    for field in fields {
        let value = path::get(doc, field).cloned().unwrap_or(Value::Null);
        path::set(&mut out, field, value)?;
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn densify_group(
    docs: &[Value],
    indices: &[usize],
    field: &str,
    partition_fields: &[String],
    kind: DensifyKind,
    bounds: &DensifyBounds,
    global: Option<(Value, Value)>,
    step_number: Option<&BigDecimal>,
    step_date: Option<i64>,
    unit: Option<&str>,
) -> Result<Vec<Value>> {
    let mut existing = HashSet::new();
    let mut partition_min = None;
    let mut partition_max = None;
    for index in indices {
        let Some(value) = path::get(&docs[*index], field) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        densify_value_kind(value, kind == DensifyKind::Date)?;
        existing.insert(densify_key(value, kind == DensifyKind::Date)?);
        if partition_min
            .as_ref()
            .map(|current| compare_value(value, current) == Ordering::Less)
            .unwrap_or(true)
        {
            partition_min = Some(value.clone());
        }
        if partition_max
            .as_ref()
            .map(|current| compare_value(value, current) == Ordering::Greater)
            .unwrap_or(true)
        {
            partition_max = Some(value.clone());
        }
    }
    let (lower, upper) = match bounds {
        DensifyBounds::Partition => match (partition_min, partition_max) {
            (Some(lower), Some(upper)) => (lower, upper),
            _ => return Ok(Vec::new()),
        },
        DensifyBounds::Full => match global {
            Some((lower, upper)) => (lower, upper),
            None => return Ok(Vec::new()),
        },
        DensifyBounds::Explicit(lower, upper) => {
            densify_value_kind(lower, kind == DensifyKind::Date)?;
            densify_value_kind(upper, kind == DensifyKind::Date)?;
            (lower.clone(), upper.clone())
        }
    };
    let base = densify_partition_doc(
        indices
            .first()
            .map(|index| &docs[*index])
            .ok_or_else(|| Error::Query("internal densify partition error".into()))?,
        partition_fields,
    )?;
    let mut generated = Vec::new();
    match kind {
        DensifyKind::Number => {
            let Some(step) = step_number else {
                return Err(Error::Query("$densify requires a numeric `step`".into()));
            };
            let mut current = numeric::as_bigdecimal(&lower)
                .ok_or_else(|| Error::Query("$densify bounds must be numeric".into()))?;
            let upper = numeric::as_bigdecimal(&upper)
                .ok_or_else(|| Error::Query("$densify bounds must be numeric".into()))?;
            while current <= upper {
                let value = densify_number_value(&current)?;
                if !existing.contains(&densify_key(&value, false)?) {
                    let mut doc = base.clone();
                    path::set(&mut doc, field, value)?;
                    generated.push(doc);
                }
                current += step;
            }
        }
        DensifyKind::Date => {
            let Some(step) = step_date else {
                return Err(Error::Query("$densify requires an integer `step`".into()));
            };
            let unit = unit.ok_or_else(|| Error::Query("$densify requires `unit`".into()))?;
            let mut current = date_millis(&lower)?;
            let upper = date_millis(&upper)?;
            while current <= upper {
                let value = date::canonical(current);
                if !existing.contains(&densify_key(&value, true)?) {
                    let mut doc = base.clone();
                    path::set(&mut doc, field, value)?;
                    generated.push(doc);
                }
                let next = densify_add_date(current, step, unit)?;
                if next <= current {
                    return Err(Error::InvalidDate(
                        "$densify date step does not advance".into(),
                    ));
                }
                current = next;
            }
        }
    }
    Ok(generated)
}

fn densify(
    mut docs: Vec<Value>,
    spec: &Value,
    _vars: &BTreeMap<String, Value>,
) -> Result<Vec<Value>> {
    let spec = spec
        .as_object()
        .ok_or_else(|| Error::Query("$densify requires an object".into()))?;
    let field = spec
        .get("field")
        .and_then(Value::as_str)
        .filter(|field| !field.is_empty() && !field.starts_with('$'))
        .ok_or_else(|| Error::Query("$densify requires a non-empty `field`".into()))?;
    let partition_fields = match spec.get("partitionByFields") {
        Some(value) => value
            .as_array()
            .ok_or_else(|| Error::Query("$densify `partitionByFields` must be an array".into()))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .filter(|field| !field.is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| Error::Query("partition fields must be strings".into()))
            })
            .collect::<Result<Vec<_>>>()?,
        None => Vec::new(),
    };
    if partition_fields.iter().any(|partition| partition == field) {
        return Err(Error::Query(
            "$densify field cannot also be a partition field".into(),
        ));
    }
    let range = spec
        .get("range")
        .and_then(Value::as_object)
        .ok_or_else(|| Error::Query("$densify requires `range`".into()))?;
    let unit = range.get("unit").and_then(Value::as_str);
    let date_mode = unit.is_some();
    let (step_number, step_date) = if date_mode {
        let step = range
            .get("step")
            .and_then(Value::as_i64)
            .filter(|step| *step > 0)
            .ok_or_else(|| {
                Error::Query("$densify date `step` must be a positive integer".into())
            })?;
        (None, Some(step))
    } else {
        let step = range
            .get("step")
            .and_then(numeric::as_bigdecimal)
            .filter(|step| *step > 0)
            .ok_or_else(|| Error::Query("$densify `step` must be positive".into()))?;
        (Some(step), None)
    };
    let bounds = match range.get("bounds") {
        None => DensifyBounds::Partition,
        Some(Value::String(value)) if value == "partition" => DensifyBounds::Partition,
        Some(Value::String(value)) if value == "full" => DensifyBounds::Full,
        Some(Value::Array(values)) if values.len() == 2 => {
            DensifyBounds::Explicit(values[0].clone(), values[1].clone())
        }
        Some(_) => {
            return Err(Error::Query(
                "$densify `bounds` must be `partition`, `full`, or [lower, upper]".into(),
            ));
        }
    };
    let kind = if date_mode {
        DensifyKind::Date
    } else {
        DensifyKind::Number
    };

    let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (index, doc) in docs.iter().enumerate() {
        groups
            .entry(densify_partition_key(doc, &partition_fields)?)
            .or_default()
            .push(index);
    }
    let mut global_min = None;
    let mut global_max = None;
    for doc in &docs {
        let Some(value) = path::get(doc, field) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        densify_value_kind(value, date_mode)?;
        if global_min
            .as_ref()
            .map(|current| compare_value(value, current) == Ordering::Less)
            .unwrap_or(true)
        {
            global_min = Some(value.clone());
        }
        if global_max
            .as_ref()
            .map(|current| compare_value(value, current) == Ordering::Greater)
            .unwrap_or(true)
        {
            global_max = Some(value.clone());
        }
    }
    let global = global_min.zip(global_max);
    let mut generated = Vec::new();
    for indices in groups.values() {
        generated.extend(densify_group(
            &docs,
            indices,
            field,
            &partition_fields,
            kind,
            &bounds,
            global.clone(),
            step_number.as_ref(),
            step_date,
            unit,
        )?);
        if docs.len().saturating_add(generated.len()) > max_docs() {
            return Err(Error::Query(format!(
                "$densify result exceeds {} documents",
                max_docs()
            )));
        }
    }
    docs.extend(generated);
    Ok(docs)
}

#[derive(Clone)]
enum FillMethod {
    Value(Value),
    Locf,
    Linear,
}

fn fill_partition_key(doc: &Value, fields: &[String]) -> Result<String> {
    densify_partition_key(doc, fields)
}

fn fill_sort_x(doc: &Value, field: &str) -> Result<Option<f64>> {
    let Some(value) = path::get(doc, field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    if let Some(value) = numeric::as_f64(value) {
        return Ok(Some(value));
    }
    if date::is_date(value) {
        return Ok(Some(date_millis(value)? as f64));
    }
    Err(Error::Query(
        "$fill linear requires a numeric or date sort field".into(),
    ))
}

fn fill_values(
    docs: &[Value],
    indices: &[usize],
    field: &str,
    method: &FillMethod,
    sort: &[(String, i32)],
    vars: &BTreeMap<String, Value>,
) -> Result<Vec<Option<Value>>> {
    let mut out = vec![None; indices.len()];
    match method {
        FillMethod::Value(expression) => {
            for (position, index) in indices.iter().enumerate() {
                let current = path::get(&docs[*index], field);
                if current.is_none_or(Value::is_null) {
                    out[position] = Some(eval_expr_vars(&docs[*index], expression, vars)?);
                }
            }
        }
        FillMethod::Locf => {
            let mut last = None;
            for (position, index) in indices.iter().enumerate() {
                let current = path::get(&docs[*index], field);
                match current {
                    Some(value) if !value.is_null() => last = Some(value.clone()),
                    _ => {
                        if let Some(value) = &last {
                            out[position] = Some(value.clone());
                        }
                    }
                }
            }
        }
        FillMethod::Linear => {
            let sort_field = sort
                .first()
                .map(|(field, _)| field.as_str())
                .ok_or_else(|| Error::Query("$fill linear requires `sortBy`".into()))?;
            let mut known = Vec::new();
            for (position, index) in indices.iter().enumerate() {
                let Some(value) = path::get(&docs[*index], field) else {
                    continue;
                };
                if value.is_null() {
                    continue;
                }
                let Some(value) = numeric::as_f64(value) else {
                    return Err(Error::Query(
                        "$fill linear requires numeric field values".into(),
                    ));
                };
                let Some(x) = fill_sort_x(&docs[*index], sort_field)? else {
                    return Err(Error::Query(
                        "$fill linear requires non-null sort values".into(),
                    ));
                };
                known.push((position, x, value));
            }
            for (position, index) in indices.iter().enumerate() {
                let current = path::get(&docs[*index], field);
                if current.is_some_and(|value| !value.is_null()) {
                    continue;
                }
                let Some(previous) = known.iter().rev().find(|(known, _, _)| *known < position)
                else {
                    continue;
                };
                let Some(next) = known.iter().find(|(known, _, _)| *known > position) else {
                    continue;
                };
                let Some(x) = fill_sort_x(&docs[*index], sort_field)? else {
                    continue;
                };
                if next.1 == previous.1 {
                    out[position] = Some(number_value(previous.2));
                } else {
                    let ratio = (x - previous.1) / (next.1 - previous.1);
                    out[position] = Some(number_value(previous.2 + ratio * (next.2 - previous.2)));
                }
            }
        }
    }
    Ok(out)
}

fn fill(mut docs: Vec<Value>, spec: &Value, vars: &BTreeMap<String, Value>) -> Result<Vec<Value>> {
    let spec = spec
        .as_object()
        .ok_or_else(|| Error::Query("$fill requires an object".into()))?;
    let partition_fields = match spec.get("partitionByFields") {
        Some(value) => value
            .as_array()
            .ok_or_else(|| Error::Query("$fill `partitionByFields` must be an array".into()))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .filter(|field| !field.is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| Error::Query("partition fields must be strings".into()))
            })
            .collect::<Result<Vec<_>>>()?,
        None => Vec::new(),
    };
    if spec.contains_key("partitionBy") && spec.contains_key("partitionByFields") {
        return Err(Error::Query(
            "$fill cannot combine `partitionBy` and `partitionByFields`".into(),
        ));
    }
    let sort = match spec.get("sortBy") {
        Some(sort) => parse_sort(sort)?,
        None => Vec::new(),
    };
    let outputs = spec
        .get("output")
        .and_then(Value::as_object)
        .ok_or_else(|| Error::Query("$fill requires an `output` object".into()))?;
    if outputs.is_empty() {
        return Err(Error::Query("$fill `output` must not be empty".into()));
    }
    let mut methods = Vec::with_capacity(outputs.len());
    for (field, value) in outputs {
        let value = value
            .as_object()
            .ok_or_else(|| Error::Query("$fill output fields must be objects".into()))?;
        let method = if let Some(expression) = value.get("value") {
            if value.contains_key("method") {
                return Err(Error::Query(
                    "$fill output field cannot combine `value` and `method`".into(),
                ));
            }
            FillMethod::Value(expression.clone())
        } else {
            match value.get("method").and_then(Value::as_str) {
                Some("locf") => FillMethod::Locf,
                Some("linear") => FillMethod::Linear,
                Some(method) => {
                    return Err(Error::Query(format!("unsupported $fill method `{method}`")));
                }
                None => {
                    return Err(Error::Query(
                        "$fill output field requires `value` or `method`".into(),
                    ));
                }
            }
        };
        if matches!(method, FillMethod::Linear) && sort.is_empty() {
            return Err(Error::Query("$fill linear requires `sortBy`".into()));
        }
        methods.push((field.clone(), method));
    }

    let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (index, doc) in docs.iter().enumerate() {
        let key = if let Some(expression) = spec.get("partitionBy") {
            serde_json::to_string(&eval_expr_vars(doc, expression, vars)?)?
        } else {
            fill_partition_key(doc, &partition_fields)?
        };
        groups.entry(key).or_default().push(index);
    }

    for indices in groups.values_mut() {
        if !sort.is_empty() {
            indices.sort_by(|left, right| compare_docs(&docs[*left], &docs[*right], &sort));
        }
        let mut results = Vec::with_capacity(methods.len());
        for (field, method) in &methods {
            results.push(fill_values(&docs, indices, field, method, &sort, vars)?);
        }
        for ((field, _), values) in methods.iter().zip(results) {
            for (index, value) in indices.iter().zip(values) {
                if let Some(value) = value {
                    path::set(&mut docs[*index], field, value)?;
                }
            }
        }
    }
    Ok(docs)
}

#[derive(Clone)]
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

#[derive(Clone)]
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

pub(crate) fn eval_expr_vars(
    doc: &Value,
    expr: &Value,
    vars: &BTreeMap<String, Value>,
) -> Result<Value> {
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
        "$dateAdd" | "$dateSubtract" => date_add_op(doc, arg, vars, op == "$dateSubtract"),
        "$dateDiff" => date_diff_op(doc, arg, vars),
        "$dateTrunc" => date_trunc_op(doc, arg, vars),
        "$dateToString" => date_to_string_op(doc, arg, vars),
        "$dateFromString" => date_from_string_op(doc, arg, vars),
        "$dateFromParts" => date_from_parts_op(doc, arg, vars),
        "$dateToParts" => date_to_parts_op(doc, arg, vars),
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

/// Reports whether `eval_operator` implements `op`.
///
/// Filter validation uses this to reject unknown operators inside `$expr` before a
/// scan instead of letting them evaluate to `false`. The dispatch is probed with an
/// empty argument list, so unknown operators are reported before their arguments
/// are read; known operators may fail on arity, which still proves they exist.
pub(crate) fn expression_operator_supported(op: &str) -> bool {
    let mut obj = Map::new();
    obj.insert(op.to_string(), Value::Array(Vec::new()));
    !matches!(
        eval_operator(&Value::Object(Map::new()), &obj, &BTreeMap::new()),
        Err(Error::Query(message)) if message.starts_with("unsupported aggregation expression")
    )
}

fn date_millis(value: &Value) -> Result<i64> {
    date::parse_value(value)
        .ok_or_else(|| Error::InvalidDate("expected an Extended JSON date".into()))
}

fn date_from_millis(ms: i64) -> Result<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
        .map_err(|_| Error::InvalidDate("date is out of range".into()))
}

fn date_to_millis(value: OffsetDateTime) -> Result<i64> {
    let ms = value.unix_timestamp_nanos().div_euclid(1_000_000);
    i64::try_from(ms).map_err(|_| Error::InvalidDate("date is out of range".into()))
}

fn date_timezone(
    obj: &Map<String, Value>,
    doc: &Value,
    vars: &BTreeMap<String, Value>,
) -> Result<UtcOffset> {
    let Some(value) = obj.get("timezone") else {
        return Ok(UtcOffset::UTC);
    };
    let value = eval_expr_vars(doc, value, vars)?;
    parse_timezone_value(&value)
}

fn parse_timezone_value(value: &Value) -> Result<UtcOffset> {
    let Some(raw) = value.as_str() else {
        return Err(Error::Query("timezone must be a string".into()));
    };
    let raw = raw.trim();
    let upper = raw.to_ascii_uppercase();
    if matches!(upper.as_str(), "Z" | "UTC" | "GMT") {
        return Ok(UtcOffset::UTC);
    }
    let offset = upper
        .strip_prefix("UTC")
        .or_else(|| upper.strip_prefix("GMT"))
        .unwrap_or(&upper);
    let bytes = offset.as_bytes();
    let (sign, rest) = match bytes.first() {
        Some(b'+') => (1, &offset[1..]),
        Some(b'-') => (-1, &offset[1..]),
        _ => {
            return Err(Error::Query(format!(
                "unsupported timezone `{raw}`; use UTC or a fixed +HH:MM offset"
            )))
        }
    };
    let (hours, minutes) = match rest.len() {
        5 if rest.as_bytes()[2] == b':' => (&rest[..2], &rest[3..]),
        4 => (&rest[..2], &rest[2..]),
        2 => (rest, "0"),
        _ => return Err(Error::Query(format!("invalid timezone offset `{raw}`"))),
    };
    let hours = hours
        .parse::<i8>()
        .map_err(|_| Error::Query(format!("invalid timezone offset `{raw}`")))?;
    let minutes = minutes
        .parse::<i8>()
        .map_err(|_| Error::Query(format!("invalid timezone offset `{raw}`")))?;
    UtcOffset::from_hms(sign * hours, sign * minutes, 0)
        .map_err(|_| Error::Query(format!("invalid timezone offset `{raw}`")))
}

fn add_date_months(value: OffsetDateTime, months: i64) -> Result<OffsetDateTime> {
    let total = i64::from(value.year())
        .checked_mul(12)
        .and_then(|v| v.checked_add(i64::from(u8::from(value.month())) - 1))
        .and_then(|v| v.checked_add(months))
        .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?;
    let year = i32::try_from(total.div_euclid(12))
        .map_err(|_| Error::InvalidDate("date is out of range".into()))?;
    let month = Month::try_from((total.rem_euclid(12) + 1) as u8)
        .map_err(|_| Error::InvalidDate("date is out of range".into()))?;
    let day = value.day().min(month.length(year));
    let date = Date::from_calendar_date(year, month, day)
        .map_err(|_| Error::InvalidDate("date is out of range".into()))?;
    Ok(value.replace_date(date))
}

fn date_add_op(
    doc: &Value,
    arg: &Value,
    vars: &BTreeMap<String, Value>,
    subtract: bool,
) -> Result<Value> {
    let Some(obj) = arg.as_object() else {
        return Err(Error::Query("$dateAdd requires an object".into()));
    };
    let start = obj
        .get("startDate")
        .ok_or_else(|| Error::Query("$dateAdd requires `startDate`".into()))?;
    let start = eval_expr_vars(doc, start, vars)?;
    let start = date_millis(&start)?;
    let unit = obj
        .get("unit")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Query("$dateAdd requires `unit`".into()))?;
    let amount = obj
        .get("amount")
        .ok_or_else(|| Error::Query("$dateAdd requires `amount`".into()))?;
    let amount = eval_expr_vars(doc, amount, vars)?;
    let amount =
        expr_i64(&amount).ok_or_else(|| Error::Query("`amount` must be an integer".into()))?;
    let amount = if subtract {
        amount
            .checked_neg()
            .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?
    } else {
        amount
    };
    let timezone = date_timezone(obj, doc, vars)?;
    let value = date_from_millis(start)?.to_offset(timezone);
    let result = match unit {
        "year" => add_date_months(
            value,
            amount
                .checked_mul(12)
                .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?,
        )?,
        "quarter" => add_date_months(
            value,
            amount
                .checked_mul(3)
                .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?,
        )?,
        "month" => add_date_months(value, amount)?,
        "week" => value
            .checked_add(Duration::weeks(amount))
            .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?,
        "day" => value
            .checked_add(Duration::days(amount))
            .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?,
        "hour" => value
            .checked_add(Duration::hours(amount))
            .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?,
        "minute" => value
            .checked_add(Duration::minutes(amount))
            .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?,
        "second" => value
            .checked_add(Duration::seconds(amount))
            .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?,
        "millisecond" => value
            .checked_add(Duration::milliseconds(amount))
            .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?,
        _ => return Err(Error::Query(format!("unsupported date unit `{unit}`"))),
    };
    Ok(date::canonical(date_to_millis(result)?))
}

fn week_reference_date(start: Weekday) -> Result<Date> {
    let base = Date::from_calendar_date(2000, Month::January, 1)
        .map_err(|_| Error::InvalidDate("invalid reference date".into()))?;
    let offset_days = (i64::from(start.number_days_from_sunday())
        - i64::from(base.weekday().number_days_from_sunday()))
    .rem_euclid(7);
    base.checked_add(Duration::days(offset_days))
        .ok_or_else(|| Error::InvalidDate("invalid reference date".into()))
}

fn date_diff_op(doc: &Value, arg: &Value, vars: &BTreeMap<String, Value>) -> Result<Value> {
    let Some(obj) = arg.as_object() else {
        return Err(Error::Query("$dateDiff requires an object".into()));
    };
    let start = obj
        .get("startDate")
        .ok_or_else(|| Error::Query("$dateDiff requires `startDate`".into()))?;
    let end = obj
        .get("endDate")
        .ok_or_else(|| Error::Query("$dateDiff requires `endDate`".into()))?;
    let start = date_millis(&eval_expr_vars(doc, start, vars)?)?;
    let end = date_millis(&eval_expr_vars(doc, end, vars)?)?;
    let unit = obj
        .get("unit")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Query("$dateDiff requires `unit`".into()))?;
    let timezone = date_timezone(obj, doc, vars)?;
    let start = date_from_millis(start)?.to_offset(timezone);
    let end = date_from_millis(end)?.to_offset(timezone);
    let start_date = start.date();
    let end_date = end.date();
    let diff = match unit {
        "year" => i64::from(end_date.year()) - i64::from(start_date.year()),
        "quarter" => {
            (i64::from(end_date.year()) * 4 + (i64::from(u8::from(end_date.month())) - 1) / 3)
                - (i64::from(start_date.year()) * 4
                    + (i64::from(u8::from(start_date.month())) - 1) / 3)
        }
        "month" => {
            (i64::from(end_date.year()) * 12 + i64::from(u8::from(end_date.month())) - 1)
                - (i64::from(start_date.year()) * 12 + i64::from(u8::from(start_date.month())) - 1)
        }
        "week" => {
            let reference = week_reference_date(parse_start_of_week(obj.get("startOfWeek"))?)?;
            let start_week = (i64::from(start_date.to_julian_day())
                - i64::from(reference.to_julian_day()))
            .div_euclid(7);
            let end_week = (i64::from(end_date.to_julian_day())
                - i64::from(reference.to_julian_day()))
            .div_euclid(7);
            end_week - start_week
        }
        "day" => i64::from(end_date.to_julian_day()) - i64::from(start_date.to_julian_day()),
        "hour" | "minute" | "second" | "millisecond" => {
            let offset_ms = i64::from(timezone.whole_seconds()) * 1000;
            let start_ms = date_to_millis(start)? + offset_ms;
            let end_ms = date_to_millis(end)? + offset_ms;
            let width = match unit {
                "hour" => 3_600_000,
                "minute" => 60_000,
                "second" => 1_000,
                _ => 1,
            };
            end_ms.div_euclid(width) - start_ms.div_euclid(width)
        }
        _ => return Err(Error::Query(format!("unsupported date unit `{unit}`"))),
    };
    Ok(Value::from(diff))
}

fn parse_start_of_week(value: Option<&Value>) -> Result<Weekday> {
    let Some(value) = value else {
        return Ok(Weekday::Sunday);
    };
    let Some(name) = value.as_str() else {
        return Err(Error::Query("`startOfWeek` must be a string".into()));
    };
    match name.to_ascii_lowercase().as_str() {
        "monday" => Ok(Weekday::Monday),
        "tuesday" => Ok(Weekday::Tuesday),
        "wednesday" => Ok(Weekday::Wednesday),
        "thursday" => Ok(Weekday::Thursday),
        "friday" => Ok(Weekday::Friday),
        "saturday" => Ok(Weekday::Saturday),
        "sunday" => Ok(Weekday::Sunday),
        _ => Err(Error::Query(format!("invalid startOfWeek `{name}`"))),
    }
}

fn week_reference_millis(start: Weekday, _timezone: UtcOffset) -> Result<i64> {
    let reference = week_reference_date(start)?;
    let local = OffsetDateTime::new_in_offset(reference, Time::MIDNIGHT, UtcOffset::UTC);
    date_to_millis(local)
}

fn truncate_date(
    value: OffsetDateTime,
    unit: &str,
    bin_size: i64,
    start_of_week: Weekday,
) -> Result<i64> {
    let timezone = value.offset();
    if matches!(unit, "year" | "quarter" | "month") {
        let months_per_bin = match unit {
            "year" => 12,
            "quarter" => 3,
            _ => 1,
        } * bin_size;
        let total = i64::from(value.year()) * 12 + i64::from(u8::from(value.month())) - 1;
        let reference = 2000 * 12;
        let truncated = reference + (total - reference).div_euclid(months_per_bin) * months_per_bin;
        let year = i32::try_from(truncated.div_euclid(12))
            .map_err(|_| Error::InvalidDate("date is out of range".into()))?;
        let month = Month::try_from((truncated.rem_euclid(12) + 1) as u8)
            .map_err(|_| Error::InvalidDate("date is out of range".into()))?;
        let date = Date::from_calendar_date(year, month, 1)
            .map_err(|_| Error::InvalidDate("date is out of range".into()))?;
        let local = OffsetDateTime::new_in_offset(date, Time::MIDNIGHT, timezone);
        return date_to_millis(local);
    }

    let offset_ms = i64::from(timezone.whole_seconds()) * 1000;
    let local_ms = date_to_millis(value)? + offset_ms;
    let unit_ms: i64 = match unit {
        "week" => 7 * 86_400_000,
        "day" => 86_400_000,
        "hour" => 3_600_000,
        "minute" => 60_000,
        "second" => 1_000,
        "millisecond" => 1,
        _ => return Err(Error::Query(format!("unsupported date unit `{unit}`"))),
    };
    let width = unit_ms
        .checked_mul(bin_size)
        .ok_or_else(|| Error::InvalidDate("date arithmetic overflow".into()))?;
    let reference = if unit == "week" {
        week_reference_millis(start_of_week, timezone)?
    } else {
        946_684_800_000
    };
    let truncated = reference + (local_ms - reference).div_euclid(width) * width;
    Ok(truncated - offset_ms)
}

fn date_trunc_op(doc: &Value, arg: &Value, vars: &BTreeMap<String, Value>) -> Result<Value> {
    let Some(obj) = arg.as_object() else {
        return Err(Error::Query("$dateTrunc requires an object".into()));
    };
    let date = obj
        .get("date")
        .ok_or_else(|| Error::Query("$dateTrunc requires `date`".into()))?;
    let date = eval_expr_vars(doc, date, vars)?;
    let date = date_millis(&date)?;
    let unit = obj
        .get("unit")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Query("$dateTrunc requires `unit`".into()))?;
    let bin_size = match obj.get("binSize") {
        None => 1,
        Some(value) => {
            let value = eval_expr_vars(doc, value, vars)?;
            expr_i64(&value)
                .filter(|n| *n > 0)
                .ok_or_else(|| Error::Query("`binSize` must be a positive integer".into()))?
        }
    };
    let timezone = date_timezone(obj, doc, vars)?;
    let start_of_week = parse_start_of_week(obj.get("startOfWeek"))?;
    let value = date_from_millis(date)?.to_offset(timezone);
    let truncated = truncate_date(value, unit, bin_size, start_of_week)?;
    Ok(date::canonical(truncated))
}

fn format_date(value: OffsetDateTime, format: &str) -> Result<String> {
    let date = value.date();
    let time = value.time();
    let iso = date.to_iso_week_date();
    let offset_seconds = i64::from(value.offset().whole_seconds());
    let offset_sign = if offset_seconds < 0 { '-' } else { '+' };
    let offset_abs = offset_seconds.unsigned_abs();
    let offset_hours = offset_abs / 3600;
    let offset_minutes = (offset_abs % 3600) / 60;
    let mut out = String::new();
    let mut chars = format.chars();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            out.push(ch);
            continue;
        }
        let Some(spec) = chars.next() else {
            return Err(Error::Query("incomplete date format".into()));
        };
        match spec {
            '%' => out.push('%'),
            'Y' => out.push_str(&format!("{:04}", date.year())),
            'G' => out.push_str(&format!("{:04}", iso.0)),
            'g' => out.push_str(&format!("{:02}", iso.0.rem_euclid(100))),
            'm' => out.push_str(&format!("{:02}", u8::from(date.month()))),
            'd' => out.push_str(&format!("{:02}", date.day())),
            'H' => out.push_str(&format!("{:02}", time.hour())),
            'M' => out.push_str(&format!("{:02}", time.minute())),
            'S' => out.push_str(&format!("{:02}", time.second())),
            'L' => out.push_str(&format!("{:03}", time.millisecond())),
            'j' => out.push_str(&format!("{:03}", date.ordinal())),
            'w' => out.push_str(&format!("{}", date.weekday().number_days_from_sunday() + 1)),
            'u' => out.push_str(&format!("{}", date.weekday().number_days_from_monday() + 1)),
            'U' => {
                let week = (i32::from(date.ordinal())
                    - i32::from(date.weekday().number_days_from_sunday())
                    + 6)
                    / 7;
                out.push_str(&format!("{week:02}"));
            }
            'V' => out.push_str(&format!("{:02}", iso.1)),
            'z' => out.push_str(&format!(
                "{offset_sign}{offset_hours:02}{offset_minutes:02}"
            )),
            'Z' => out.push_str(&format!("{}", offset_seconds / 60)),
            other => {
                return Err(Error::Query(format!(
                    "unsupported date format specifier `%{other}`"
                )))
            }
        }
    }
    Ok(out)
}

fn date_to_string_op(doc: &Value, arg: &Value, vars: &BTreeMap<String, Value>) -> Result<Value> {
    let Some(obj) = arg.as_object() else {
        return Err(Error::Query("$dateToString requires an object".into()));
    };
    let date = obj
        .get("date")
        .ok_or_else(|| Error::Query("$dateToString requires `date`".into()))?;
    let date = eval_expr_vars(doc, date, vars)?;
    if date.is_null() {
        return match obj.get("onNull") {
            Some(on_null) => eval_expr_vars(doc, on_null, vars),
            None => Ok(Value::Null),
        };
    }
    let date = date_millis(&date)?;
    let format = obj
        .get("format")
        .and_then(Value::as_str)
        .unwrap_or("%Y-%m-%dT%H:%M:%S.%LZ");
    let timezone = date_timezone(obj, doc, vars)?;
    let value = date_from_millis(date)?.to_offset(timezone);
    Ok(Value::String(format_date(value, format)?))
}

fn take_date_digits(input: &str, position: &mut usize, min: usize, max: usize) -> Option<i64> {
    let bytes = input.as_bytes();
    let start = *position;
    while *position < bytes.len() && bytes[*position].is_ascii_digit() && *position - start < max {
        *position += 1;
    }
    let digits = *position - start;
    if digits < min {
        return None;
    }
    input[start..*position].parse().ok()
}

fn take_date_offset(input: &str, position: &mut usize) -> Option<UtcOffset> {
    let bytes = input.as_bytes();
    let sign = match bytes.get(*position)? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    *position += 1;
    let hours = take_date_digits(input, position, 2, 2)?;
    if bytes.get(*position) == Some(&b':') {
        *position += 1;
    }
    let minutes = take_date_digits(input, position, 2, 2)?;
    UtcOffset::from_hms(
        i8::try_from(sign * hours).ok()?,
        i8::try_from(sign * minutes).ok()?,
        0,
    )
    .ok()
}

fn parse_date_with_format(input: &str, format: &str, default_timezone: UtcOffset) -> Option<i64> {
    let format_bytes = format.as_bytes();
    let input_bytes = input.as_bytes();
    let mut fi = 0usize;
    let mut pi = 0usize;
    let mut year = 1970i32;
    let mut month = 1u8;
    let mut day = 1u8;
    let mut ordinal: Option<u16> = None;
    let mut hour = 0u8;
    let mut minute = 0u8;
    let mut second = 0u8;
    let mut millisecond = 0u16;
    let mut timezone = default_timezone;
    while fi < format_bytes.len() {
        if format_bytes[fi] != b'%' {
            if input_bytes.get(pi) != Some(&format_bytes[fi]) {
                return None;
            }
            fi += 1;
            pi += 1;
            continue;
        }
        fi += 1;
        let spec = *format_bytes.get(fi)?;
        fi += 1;
        match spec {
            b'%' => {
                if input_bytes.get(pi) != Some(&b'%') {
                    return None;
                }
                pi += 1;
            }
            b'Y' => year = i32::try_from(take_date_digits(input, &mut pi, 4, 4)?).ok()?,
            b'm' => month = u8::try_from(take_date_digits(input, &mut pi, 2, 2)?).ok()?,
            b'd' => day = u8::try_from(take_date_digits(input, &mut pi, 2, 2)?).ok()?,
            b'j' => ordinal = Some(u16::try_from(take_date_digits(input, &mut pi, 3, 3)?).ok()?),
            b'H' => hour = u8::try_from(take_date_digits(input, &mut pi, 2, 2)?).ok()?,
            b'M' => minute = u8::try_from(take_date_digits(input, &mut pi, 2, 2)?).ok()?,
            b'S' => second = u8::try_from(take_date_digits(input, &mut pi, 2, 2)?).ok()?,
            b'L' => millisecond = u16::try_from(take_date_digits(input, &mut pi, 3, 3)?).ok()?,
            b'f' => {
                let start = pi;
                let value = take_date_digits(input, &mut pi, 1, 9)?;
                let digits = pi - start;
                let scaled = if digits >= 3 {
                    value / 10_i64.pow((digits - 3) as u32)
                } else {
                    value * 10_i64.pow((3 - digits) as u32)
                };
                millisecond = u16::try_from(scaled).ok()?;
            }
            b'z' => timezone = take_date_offset(input, &mut pi)?,
            _ => return None,
        }
    }
    if pi != input_bytes.len() {
        return None;
    }
    let date = if let Some(ordinal) = ordinal {
        Date::from_ordinal_date(year, ordinal).ok()?
    } else {
        let month = Month::try_from(month).ok()?;
        Date::from_calendar_date(year, month, day).ok()?
    };
    let time = Time::from_hms_milli(hour, minute, second, millisecond).ok()?;
    let value = OffsetDateTime::new_in_offset(date, time, timezone);
    date_to_millis(value).ok()
}

fn parse_date_string(input: &str, format: Option<&str>, timezone: UtcOffset) -> Option<i64> {
    if let Some(format) = format {
        return parse_date_with_format(input, format, timezone);
    }
    if let Some(ms) = date::parse_rfc3339_ms(input) {
        return Some(ms);
    }
    for format in ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"] {
        if let Some(ms) = parse_date_with_format(input, format, timezone) {
            return Some(ms);
        }
    }
    None
}

fn date_from_string_op(doc: &Value, arg: &Value, vars: &BTreeMap<String, Value>) -> Result<Value> {
    let Some(obj) = arg.as_object() else {
        return Err(Error::Query("$dateFromString requires an object".into()));
    };
    let raw = obj
        .get("dateString")
        .ok_or_else(|| Error::Query("$dateFromString requires `dateString`".into()))?;
    let raw = eval_expr_vars(doc, raw, vars)?;
    if raw.is_null() {
        return match obj.get("onNull") {
            Some(on_null) => eval_expr_vars(doc, on_null, vars),
            None => Ok(Value::Null),
        };
    }
    let timezone = date_timezone(obj, doc, vars)?;
    let format = obj.get("format").and_then(Value::as_str);
    let parsed = raw
        .as_str()
        .and_then(|raw| parse_date_string(raw, format, timezone))
        .map(date::canonical);
    match parsed {
        Some(value) => Ok(value),
        None => match obj.get("onError") {
            Some(on_error) => eval_expr_vars(doc, on_error, vars),
            None => Err(Error::Query(
                "$dateFromString could not parse the date string".into(),
            )),
        },
    }
}

fn date_part_i64(
    obj: &Map<String, Value>,
    field: &str,
    doc: &Value,
    vars: &BTreeMap<String, Value>,
    default: i64,
) -> Result<i64> {
    let Some(value) = obj.get(field) else {
        return Ok(default);
    };
    let value = eval_expr_vars(doc, value, vars)?;
    expr_i64(&value).ok_or_else(|| Error::Query(format!("`{field}` must be an integer")))
}

fn date_from_parts_op(doc: &Value, arg: &Value, vars: &BTreeMap<String, Value>) -> Result<Value> {
    let Some(obj) = arg.as_object() else {
        return Err(Error::Query("$dateFromParts requires an object".into()));
    };
    let timezone = date_timezone(obj, doc, vars)?;
    let date = if obj.contains_key("isoWeekYear") {
        let year = i32::try_from(date_part_i64(obj, "isoWeekYear", doc, vars, 1970)?)
            .map_err(|_| Error::InvalidDate("date is out of range".into()))?;
        let week = u8::try_from(date_part_i64(obj, "isoWeek", doc, vars, 1)?)
            .map_err(|_| Error::InvalidDate("invalid ISO week".into()))?;
        let day = date_part_i64(obj, "isoDayOfWeek", doc, vars, 1)?;
        let day = u8::try_from(day)
            .ok()
            .filter(|day| (1..=7).contains(day))
            .ok_or_else(|| Error::InvalidDate("invalid ISO day of week".into()))?;
        let weekday = match day {
            1 => Weekday::Monday,
            2 => Weekday::Tuesday,
            3 => Weekday::Wednesday,
            4 => Weekday::Thursday,
            5 => Weekday::Friday,
            6 => Weekday::Saturday,
            7 => Weekday::Sunday,
            _ => return Err(Error::InvalidDate("invalid ISO day of week".into())),
        };
        Date::from_iso_week_date(year, week, weekday)
            .map_err(|_| Error::InvalidDate("invalid ISO date".into()))?
    } else {
        let year = i32::try_from(date_part_i64(obj, "year", doc, vars, 1970)?)
            .map_err(|_| Error::InvalidDate("date is out of range".into()))?;
        let month = u8::try_from(date_part_i64(obj, "month", doc, vars, 1)?)
            .ok()
            .and_then(|month| Month::try_from(month).ok())
            .ok_or_else(|| Error::InvalidDate("invalid month".into()))?;
        let day = u8::try_from(date_part_i64(obj, "day", doc, vars, 1)?)
            .map_err(|_| Error::InvalidDate("invalid day".into()))?;
        Date::from_calendar_date(year, month, day)
            .map_err(|_| Error::InvalidDate("invalid calendar date".into()))?
    };
    let hour = u8::try_from(date_part_i64(obj, "hour", doc, vars, 0)?)
        .map_err(|_| Error::InvalidDate("invalid hour".into()))?;
    let minute = u8::try_from(date_part_i64(obj, "minute", doc, vars, 0)?)
        .map_err(|_| Error::InvalidDate("invalid minute".into()))?;
    let second = u8::try_from(date_part_i64(obj, "second", doc, vars, 0)?)
        .map_err(|_| Error::InvalidDate("invalid second".into()))?;
    let millisecond = u16::try_from(date_part_i64(obj, "millisecond", doc, vars, 0)?)
        .map_err(|_| Error::InvalidDate("invalid millisecond".into()))?;
    let time = Time::from_hms_milli(hour, minute, second, millisecond)
        .map_err(|_| Error::InvalidDate("invalid time".into()))?;
    let value = OffsetDateTime::new_in_offset(date, time, timezone);
    Ok(date::canonical(date_to_millis(value)?))
}

fn date_to_parts_op(doc: &Value, arg: &Value, vars: &BTreeMap<String, Value>) -> Result<Value> {
    let Some(obj) = arg.as_object() else {
        return Err(Error::Query("$dateToParts requires an object".into()));
    };
    let date = obj
        .get("date")
        .ok_or_else(|| Error::Query("$dateToParts requires `date`".into()))?;
    let date = eval_expr_vars(doc, date, vars)?;
    let date = date_millis(&date)?;
    let timezone = date_timezone(obj, doc, vars)?;
    let value = date_from_millis(date)?.to_offset(timezone);
    let date = value.date();
    let time = value.time();
    let mut out = Map::new();
    if obj
        .get("iso8601")
        .is_some_and(|v| v.is_boolean() && v.as_bool() == Some(true))
    {
        let (year, week, weekday) = date.to_iso_week_date();
        out.insert("isoWeekYear".to_string(), Value::from(year));
        out.insert("isoWeek".to_string(), Value::from(week));
        out.insert(
            "isoDayOfWeek".to_string(),
            Value::from(weekday.number_days_from_monday() + 1),
        );
    } else {
        out.insert("year".to_string(), Value::from(date.year()));
        out.insert("month".to_string(), Value::from(u8::from(date.month())));
        out.insert("day".to_string(), Value::from(date.day()));
    }
    out.insert("hour".to_string(), Value::from(time.hour()));
    out.insert("minute".to_string(), Value::from(time.minute()));
    out.insert("second".to_string(), Value::from(time.second()));
    out.insert("millisecond".to_string(), Value::from(time.millisecond()));
    Ok(Value::Object(out))
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
    fn project_accepts_extended_json_numeric_inclusions() {
        let docs = vec![json!({
            "_id": "a",
            "n": {"$numberInt": "7"},
            "drop": {"$numberLong": "9"},
        })];
        let pipeline = vec![json!({
            "$project": {
                "_id": {"$numberInt": "1"},
                "n": {"$numberInt": "1"},
                "drop": {"$numberInt": "0"},
            }
        })];
        let out = run(docs, &pipeline, 100).unwrap();
        assert_eq!(out, vec![json!({"_id": "a", "n": {"$numberInt": "7"}})]);
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
    fn date_expression_operators() {
        let doc = json!({"at": {"$date": "2024-01-31T23:45:12.345Z"}});
        let eval = |expr: Value| eval_expr(&doc, &expr).unwrap();
        let canonical = |raw: &str| {
            let value = json!({"$date": raw});
            date::canonical(date::parse_value(&value).unwrap())
        };

        assert_eq!(
            eval(json!({"$dateAdd": {
                "startDate": "$at",
                "unit": "month",
                "amount": 1
            }})),
            canonical("2024-02-29T23:45:12.345Z")
        );
        assert_eq!(
            eval(json!({"$dateAdd": {
                "startDate": "$at",
                "unit": "year",
                "amount": 1
            }})),
            canonical("2025-01-31T23:45:12.345Z")
        );
        assert_eq!(
            eval(json!({"$dateSubtract": {
                "startDate": "$at",
                "unit": "day",
                "amount": 1
            }})),
            canonical("2024-01-30T23:45:12.345Z")
        );
        assert_eq!(
            eval(json!({"$dateTrunc": {
                "date": "$at",
                "unit": "day"
            }})),
            canonical("2024-01-31T00:00:00Z")
        );
        assert_eq!(
            eval(json!({"$dateTrunc": {
                "date": {"$date": "2024-03-07T12:00:00Z"},
                "unit": "week",
                "startOfWeek": "monday"
            }})),
            canonical("2024-03-04T00:00:00Z")
        );
        assert_eq!(
            eval(json!({"$dateTrunc": {
                "date": {"$date": "2024-01-01T00:30:00+01:00"},
                "unit": "day",
                "timezone": "+01:00"
            }})),
            canonical("2023-12-31T23:00:00Z")
        );
        assert_eq!(
            eval(json!({"$dateToString": {
                "date": {"$date": "2024-03-05T06:07:08.009Z"},
                "format": "%Y-%m-%d %H:%M:%S.%L %z %Z"
            }})),
            json!("2024-03-05 06:07:08.009 +0000 0")
        );
        assert_eq!(
            eval(json!({"$dateToString": {
                "date": {"$date": "2024-03-05T06:07:08.009Z"},
                "format": "%H:%M %z %Z",
                "timezone": "+02:00"
            }})),
            json!("08:07 +0200 120")
        );
        assert_eq!(
            eval(json!({"$dateFromString": {
                "dateString": "2024-03-05T06:07:08.009Z"
            }})),
            canonical("2024-03-05T06:07:08.009Z")
        );
        assert_eq!(
            eval(json!({"$dateFromString": {
                "dateString": "05/03/2024 06:07:08.009",
                "format": "%d/%m/%Y %H:%M:%S.%L",
                "timezone": "+02:00"
            }})),
            canonical("2024-03-05T04:07:08.009Z")
        );
        assert_eq!(
            eval(json!({"$dateFromString": {
                "dateString": "2024-03-05T06:07:08+02:00",
                "format": "%Y-%m-%dT%H:%M:%S%z"
            }})),
            canonical("2024-03-05T04:07:08Z")
        );
        assert_eq!(
            eval(json!({"$dateFromString": {
                "dateString": "2024-03-05T06:07:08.009",
                "timezone": "+02:00"
            }})),
            canonical("2024-03-05T04:07:08.009Z")
        );
        assert_eq!(
            eval(json!({"$dateDiff": {
                "startDate": "$at",
                "endDate": {"$date": "2024-03-01T00:00:00Z"},
                "unit": "month"
            }})),
            json!(2)
        );
        assert_eq!(
            eval(json!({"$dateDiff": {
                "startDate": {"$date": "2024-01-31T23:30:00Z"},
                "endDate": {"$date": "2024-02-01T00:15:00Z"},
                "unit": "day"
            }})),
            json!(1)
        );
        assert_eq!(
            eval(json!({"$dateDiff": {
                "startDate": {"$date": "2024-01-31T10:30:00Z"},
                "endDate": {"$date": "2024-01-31T11:15:00Z"},
                "unit": "hour"
            }})),
            json!(1)
        );
        assert_eq!(
            eval(json!({"$dateDiff": {
                "startDate": {"$date": "2024-03-11T00:00:00Z"},
                "endDate": {"$date": "2024-03-04T00:00:00Z"},
                "unit": "week",
                "startOfWeek": "monday"
            }})),
            json!(-1)
        );
        assert_eq!(
            eval(json!({"$dateFromString": {
                "dateString": "not a date",
                "onError": "bad"
            }})),
            json!("bad")
        );
        assert_eq!(
            eval(json!({"$dateFromString": {
                "dateString": null,
                "onNull": "none"
            }})),
            json!("none")
        );
        assert_eq!(
            eval(json!({"$dateFromParts": {
                "year": 2024,
                "month": 2,
                "day": 29,
                "hour": 23,
                "minute": 45,
                "second": 12,
                "millisecond": 345,
                "timezone": "+02:00"
            }})),
            canonical("2024-02-29T21:45:12.345Z")
        );
        assert_eq!(
            eval(json!({"$dateToParts": {
                "date": {"$date": "2024-02-29T21:45:12.345Z"},
                "timezone": "+02:00"
            }})),
            json!({
                "year": 2024,
                "month": 2,
                "day": 29,
                "hour": 23,
                "minute": 45,
                "second": 12,
                "millisecond": 345
            })
        );
        assert_eq!(
            eval(json!({"$dateToParts": {
                "date": {"$date": "2024-01-01T00:00:00Z"},
                "iso8601": true
            }})),
            json!({
                "isoWeekYear": 2024,
                "isoWeek": 1,
                "isoDayOfWeek": 1,
                "hour": 0,
                "minute": 0,
                "second": 0,
                "millisecond": 0
            })
        );
        assert!(eval_expr(
            &doc,
            &json!({"$dateAdd": {
                "startDate": "$at",
                "unit": "fortnight",
                "amount": 1
            }})
        )
        .is_err());
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

    #[test]
    fn parses_write_stage_contracts() {
        let spec = write_stage_spec(&[json!({"$out": "target"})])
            .unwrap()
            .unwrap();
        assert_eq!(spec.kind, AggregateWriteKind::Out);
        assert_eq!(spec.target, "target");
        assert_eq!(spec.database, None);

        let spec = write_stage_spec(&[json!({"$merge": {
            "into": {"db": "app", "coll": "target"},
            "on": ["tenant", "email"],
            "whenMatched": "keepExisting",
            "whenNotMatched": "discard"
        }})])
        .unwrap()
        .unwrap();
        assert_eq!(spec.kind, AggregateWriteKind::Merge);
        assert_eq!(spec.database.as_deref(), Some("app"));
        assert_eq!(spec.on, vec!["tenant", "email"]);
        assert_eq!(spec.when_matched, MergeWhenMatched::KeepExisting);
        assert_eq!(spec.when_not_matched, MergeWhenNotMatched::Discard);

        assert!(write_stage_spec(&[json!({"$out": "target"}), json!({"$match": {}})]).is_err());
        let spec = write_stage_spec(&[json!({"$merge": {
            "into": "target",
            "whenMatched": [{"$set": {"x": 1}}],
            "let": {"threshold": 10}
        }})])
        .unwrap()
        .unwrap();
        assert!(matches!(spec.when_matched, MergeWhenMatched::Pipeline(_)));
        assert_eq!(spec.let_vars.len(), 1);
        assert!(write_stage_spec(&[json!({"$merge": {
            "into": "target",
            "whenMatched": [{"$group": {"_id": "$x"}}]
        }})])
        .is_err());
    }
}
