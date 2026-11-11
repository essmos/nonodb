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

/// Runs a pipeline over already selected documents.
pub fn run(mut docs: Vec<Value>, pipeline: &[Value], max_docs: usize) -> Result<Vec<Value>> {
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

fn truthy(value: &Value) -> bool {
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
                if let Some(value) = right {
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
                if let Some(value) = right {
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

fn compare_value(a: &Value, b: &Value) -> Ordering {
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
            if obj.len() == 1
                && obj.keys().next().is_some_and(|k| k.starts_with('$'))
                && !date::is_date(expr)
                && !numeric::is_numeric(expr)
                && !bson_types::is_bson_value(expr) =>
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
        "$length" => {
            let values = args()?;
            if values.len() != 1 {
                return Err(Error::Query("$length requires one argument".into()));
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
    fn rejects_unknown_stage() {
        let err = run(vec![], &[json!({"$lookup": {}})], 100).unwrap_err();
        assert!(err.to_string().contains("unsupported aggregation stage"));
    }
}
