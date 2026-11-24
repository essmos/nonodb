//! Read-only Cypher compatibility layer.
//!
//! The parser/planner is provided by the MIT-licensed `cypher-rs-zk` front end;
//! this module supplies the storage adapter and execution engine over NonoDB's
//! document graph model.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use chrono::Datelike;

use cypher_rs_zk::ast::{
    BinOp, CaseAlternative, Clause, CollectionPredicateKind, Direction, Expr, FunctionArguments,
    Literal, MapProjectionItem, NodePattern, Pattern, Query, RelPattern, UnOp, YieldItem,
};
use cypher_rs_zk::{optimize, parse, plan as plan_query, Plan, ProjectExpr, SortKey};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::error::{Error, Result};
use crate::graph::{
    AdjEdge, GraphAlgorithmKind, GraphAlgorithmOptions, GraphDirection, GraphView,
    DEFAULT_ALGORITHM_EDGES, DEFAULT_ALGORITHM_MEMORY, DEFAULT_ALGORITHM_NODES,
    MAX_ALGORITHM_EDGES, MAX_ALGORITHM_MEMORY, MAX_ALGORITHM_NODES,
};

const DEFAULT_CYPHER_ROWS: usize = 1_000;
const DEFAULT_CYPHER_OPERATIONS: usize = 1_000_000;
const MAX_CYPHER_ROWS: usize = 100_000;
const MAX_CYPHER_HOPS: usize = 64;
const MAX_CYPHER_OPERATIONS: usize = 100_000_000;
const DEFAULT_CYPHER_TIMEOUT_MS: u64 = 30_000;
const MAX_CYPHER_TIMEOUT_MS: u64 = 300_000;
const DEFAULT_CYPHER_MEMORY: usize = 64 * 1024 * 1024;
const MAX_CYPHER_MEMORY: usize = 1024 * 1024 * 1024;
const HIDDEN_PATH_NODES: &str = "__path_nodes";
const HIDDEN_PATH_RELS: &str = "__path_rels";
const HIDDEN_ANON_ID: &str = "__anon_id";
const HIDDEN_ANON_NODE: &str = "__anon_node";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CypherOptions {
    pub query: String,
    #[serde(default)]
    pub params: Value,
    #[serde(default = "default_cypher_rows")]
    pub max_rows: usize,
    #[serde(default = "default_cypher_ops")]
    pub max_operations: usize,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_depth: usize,
    #[serde(default = "default_cypher_memory")]
    pub max_memory_bytes: usize,
    #[serde(default = "default_gds_nodes")]
    pub gds_max_nodes: usize,
    #[serde(default = "default_gds_edges")]
    pub gds_max_edges: usize,
    #[serde(default = "default_gds_memory")]
    pub gds_max_memory_bytes: usize,
    #[serde(default)]
    pub gds_timeout_ms: Option<u64>,
}

fn default_cypher_rows() -> usize {
    DEFAULT_CYPHER_ROWS
}
fn default_cypher_ops() -> usize {
    DEFAULT_CYPHER_OPERATIONS
}
fn default_cypher_memory() -> usize {
    DEFAULT_CYPHER_MEMORY
}
fn default_gds_nodes() -> usize {
    DEFAULT_ALGORITHM_NODES
}
fn default_gds_edges() -> usize {
    DEFAULT_ALGORITHM_EDGES
}
fn default_gds_memory() -> usize {
    DEFAULT_ALGORITHM_MEMORY
}

impl CypherOptions {
    pub fn validate(&self) -> Result<()> {
        if self.query.trim().is_empty() {
            return Err(Error::Query("cypher `query` must not be empty".into()));
        }
        if self.max_rows == 0 || self.max_rows > MAX_CYPHER_ROWS {
            return Err(Error::Query(format!(
                "cypher `max_rows` must be between 1 and {MAX_CYPHER_ROWS}"
            )));
        }
        if self.max_operations == 0 || self.max_operations > MAX_CYPHER_OPERATIONS {
            return Err(Error::Query(format!(
                "cypher `max_operations` must be between 1 and {MAX_CYPHER_OPERATIONS}"
            )));
        }
        let timeout = self.timeout_ms.unwrap_or(DEFAULT_CYPHER_TIMEOUT_MS);
        if timeout == 0 || timeout > MAX_CYPHER_TIMEOUT_MS {
            return Err(Error::Query(format!(
                "cypher `timeout_ms` must be between 1 and {MAX_CYPHER_TIMEOUT_MS}"
            )));
        }
        if !self.params.is_null() && !self.params.is_object() {
            return Err(Error::Query("cypher `params` must be a JSON object".into()));
        }
        if self.max_memory_bytes == 0 || self.max_memory_bytes > MAX_CYPHER_MEMORY {
            return Err(Error::Query(format!(
                "cypher `max_memory_bytes` must be between 1 and {MAX_CYPHER_MEMORY}"
            )));
        }
        if self.gds_max_nodes == 0 || self.gds_max_nodes > MAX_ALGORITHM_NODES {
            return Err(Error::Query(format!(
                "cypher `gds_max_nodes` must be between 1 and {MAX_ALGORITHM_NODES}"
            )));
        }
        if self.gds_max_edges == 0 || self.gds_max_edges > MAX_ALGORITHM_EDGES {
            return Err(Error::Query(format!(
                "cypher `gds_max_edges` must be between 1 and {MAX_ALGORITHM_EDGES}"
            )));
        }
        if self.gds_max_memory_bytes == 0 || self.gds_max_memory_bytes > MAX_ALGORITHM_MEMORY {
            return Err(Error::Query(format!(
                "cypher `gds_max_memory_bytes` must be between 1 and {MAX_ALGORITHM_MEMORY}"
            )));
        }
        if self.gds_timeout_ms == Some(0) {
            return Err(Error::Query(
                "cypher `gds_timeout_ms` must be positive".into(),
            ));
        }
        Ok(())
    }

    fn depth(&self) -> usize {
        if self.max_depth == 0 {
            MAX_CYPHER_HOPS
        } else {
            self.max_depth.min(MAX_CYPHER_HOPS)
        }
    }
}

#[derive(Debug, Clone)]
struct Row {
    values: HashMap<String, Value>,
    order: Vec<String>,
}

impl Row {
    fn new() -> Self {
        Self {
            values: HashMap::new(),
            order: Vec::new(),
        }
    }

    fn set(&mut self, name: impl Into<String>, value: Value) {
        let name = name.into();
        if !name.starts_with("__") && !self.order.contains(&name) {
            self.order.push(name.clone());
        }
        self.values.insert(name, value);
    }

    fn get(&self, name: &str) -> Option<&Value> {
        self.values.get(name)
    }

    fn merge(&mut self, other: &Row) {
        for (key, value) in &other.values {
            if key.starts_with("__") {
                continue;
            }
            self.set(key.clone(), value.clone());
        }
        if let Some(path) = other.get(HIDDEN_PATH_NODES) {
            self.set(HIDDEN_PATH_NODES, path.clone());
        }
        if let Some(path) = other.get(HIDDEN_PATH_RELS) {
            self.set(HIDDEN_PATH_RELS, path.clone());
        }
    }

    fn visible_values(&self) -> Map<String, Value> {
        let mut map = Map::new();
        for key in &self.order {
            if let Some(value) = self.values.get(key) {
                map.insert(key.clone(), value.clone());
            }
        }
        map
    }
}

struct Graph<'a> {
    view: &'a dyn GraphView,
}

impl Graph<'_> {
    fn node(&self, id: &str) -> Result<Option<Value>> {
        let Some(mut node) = self.view.node(id)? else {
            return Ok(None);
        };
        if !node.is_object() {
            node = json!({"_id": id, "value": node});
        }
        node["_id"] = Value::String(id.to_string());
        Ok(Some(node))
    }

    fn node_ids(&self) -> Result<Vec<String>> {
        let mut ids = self.view.node_ids()?;
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    fn relationship(&self, edge: &AdjEdge, src: &str, dst: &str) -> Result<Value> {
        let mut rel = self
            .view
            .edge(&edge.edge_id)?
            .unwrap_or_else(|| json!({"_id": edge.edge_id}));
        if !rel.is_object() {
            rel = json!({"_id": edge.edge_id, "value": rel});
        }
        rel["_id"] = Value::String(edge.edge_id.clone());
        rel["_type"] = Value::String(edge.label.clone());
        let start = rel
            .get("from")
            .cloned()
            .unwrap_or_else(|| Value::String(src.to_string()));
        let end = rel
            .get("to")
            .cloned()
            .unwrap_or_else(|| Value::String(dst.to_string()));
        rel["_start"] = start;
        rel["_end"] = end;
        if let Some(weight) = edge.weight {
            rel["weight"] = json!(weight);
        }
        Ok(rel)
    }

    fn has_label(node: &Value, label: &str) -> bool {
        for key in ["_labels", "labels"] {
            if let Some(values) = node.get(key).and_then(Value::as_array) {
                return values.iter().any(|value| value.as_str() == Some(label));
            }
            if node.get(key).and_then(Value::as_str) == Some(label) {
                return true;
            }
        }
        false
    }

    fn relationship_type(rel: &Value) -> Option<&str> {
        rel.get("_type")
            .and_then(Value::as_str)
            .or_else(|| rel.get("type").and_then(Value::as_str))
    }
}

struct Executor<'a> {
    graph: Graph<'a>,
    params: Value,
    options: &'a CypherOptions,
    deadline: Option<Instant>,
    operations: usize,
    memory_used: usize,
    truncated: bool,
    pending_input: Vec<Row>,
}

impl<'a> Executor<'a> {
    fn new(view: &'a dyn GraphView, options: &'a CypherOptions) -> Self {
        Self {
            graph: Graph { view },
            params: options.params.clone(),
            options,
            deadline: Some(
                Instant::now()
                    + Duration::from_millis(
                        options.timeout_ms.unwrap_or(DEFAULT_CYPHER_TIMEOUT_MS),
                    ),
            ),
            operations: 0,
            memory_used: 0,
            truncated: false,
            pending_input: Vec::new(),
        }
    }

    fn tick(&mut self) -> Result<()> {
        self.operations += 1;
        if self.operations > self.options.max_operations {
            return Err(Error::Query("cypher operation budget exceeded".into()));
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() > deadline)
        {
            return Err(Error::Query("cypher query timed out".into()));
        }
        Ok(())
    }

    fn charge_row(&mut self, row: &Row) -> Result<()> {
        let bytes = serde_json::to_vec(&row.values)
            .map_or(0, |raw| raw.len())
            .saturating_add(row.order.iter().map(|name| name.len() + 24).sum::<usize>());
        self.charge_memory(bytes)
    }

    fn charge_memory(&mut self, bytes: usize) -> Result<()> {
        self.memory_used = self.memory_used.saturating_add(bytes);
        if self.memory_used > self.options.max_memory_bytes {
            return Err(Error::Query(format!(
                "cypher memory budget exceeded ({} bytes)",
                self.options.max_memory_bytes
            )));
        }
        Ok(())
    }

    fn execute(&mut self, plan: &Plan, input: Vec<Row>) -> Result<Vec<Row>> {
        if !input.is_empty() {
            self.pending_input = input.clone();
        }
        match plan {
            Plan::Empty => {
                if input.is_empty() {
                    Ok(vec![Row::new()])
                } else {
                    Ok(input)
                }
            }
            Plan::Scan { var, label } => self.scan(var.as_deref(), label.as_deref(), input),
            Plan::Expand {
                input,
                src,
                rel_var,
                rel_types,
                range,
                direction,
                dst,
                dst_label,
            } => {
                let rows = self.execute_input(input)?;
                self.expand(
                    rows,
                    src.as_deref(),
                    rel_var.as_deref(),
                    rel_types,
                    range.as_ref(),
                    *direction,
                    dst.as_deref(),
                    dst_label.as_deref(),
                )
            }
            Plan::Filter { input, pred } => {
                if let Plan::Optional {
                    input: base,
                    optional,
                } = input.as_ref()
                {
                    return self.optional(base, optional, Some(pred));
                }
                if let Plan::Project {
                    input: inner,
                    include_existing,
                    exprs,
                } = input.as_ref()
                {
                    if !exprs.iter().any(|expr| contains_aggregate(&expr.expr)) {
                        return self.filter_project(inner, *include_existing, exprs, false, pred);
                    }
                }
                if let Plan::Distinct {
                    input: distinct_input,
                } = input.as_ref()
                {
                    if let Plan::Project {
                        input: inner,
                        include_existing,
                        exprs,
                    } = distinct_input.as_ref()
                    {
                        if !exprs.iter().any(|expr| contains_aggregate(&expr.expr)) {
                            return self.filter_project(
                                inner,
                                *include_existing,
                                exprs,
                                true,
                                pred,
                            );
                        }
                    }
                }
                let rows = self.execute_input(input)?;
                let mut out = Vec::new();
                for row in rows {
                    self.tick()?;
                    let keep = if let Some(value) = self.relationship_list_predicate(pred, &row)? {
                        value
                    } else {
                        truthy(&self.eval(pred, &row, None)?)
                    };
                    if keep {
                        out.push(row);
                    }
                }
                Ok(out)
            }
            Plan::Project {
                input,
                include_existing,
                exprs,
            } => self.project(input, *include_existing, exprs),
            Plan::Sort { input, keys } => self.sort(input, keys),
            Plan::Skip { input, count } => {
                let rows = self.execute_input(input)?;
                let skip = self.eval_usize(count, rows.first().unwrap_or(&Row::new()))?;
                Ok(rows.into_iter().skip(skip).collect())
            }
            Plan::Limit { input, count } => {
                if let Plan::Project {
                    input: inner,
                    include_existing,
                    exprs,
                } = input.as_ref()
                {
                    if exprs.iter().any(|expr| contains_aggregate(&expr.expr)) {
                        let rows = self.execute_input(inner)?;
                        let limit = self.eval_usize(count, rows.first().unwrap_or(&Row::new()))?;
                        let limited: Vec<Row> = rows.into_iter().take(limit).collect();
                        return self.project_rows(limited, *include_existing, exprs);
                    }
                }
                let rows = self.execute_input(input)?;
                let limit = self.eval_usize(count, rows.first().unwrap_or(&Row::new()))?;
                let out: Vec<Row> = rows.into_iter().take(limit).collect();
                if out.len() == limit && limit >= self.options.max_rows {
                    self.truncated = true;
                }
                Ok(out)
            }
            Plan::Distinct { input } => {
                let rows = self.execute_input(input)?;
                let mut seen = HashSet::new();
                let mut out = Vec::new();
                for row in rows {
                    self.tick()?;
                    let key = serde_json::to_string(&row.visible_values())?;
                    self.charge_memory(key.len().saturating_add(24))?;
                    if seen.insert(key) {
                        self.charge_row(&row)?;
                        out.push(row);
                    }
                }
                Ok(out)
            }
            Plan::Cartesian { left, right } => {
                let left_rows = self.execute_input(left)?;
                let mut out = Vec::new();
                for left_row in left_rows {
                    let mut seed = left_row.clone();
                    seed.set(HIDDEN_PATH_NODES, json!([]));
                    seed.set(HIDDEN_PATH_RELS, json!([]));
                    let right_rows = self.execute(right, vec![seed])?;
                    for right_row in right_rows {
                        self.tick()?;
                        if let Some(mut joined) = join_rows(&left_row, &right_row) {
                            joined.merge(&right_row);
                            self.charge_row(&joined)?;
                            out.push(joined);
                        }
                    }
                    if out.len() >= self.options.max_rows {
                        self.truncated = true;
                        break;
                    }
                }
                Ok(out)
            }
            Plan::Optional { input, optional } => self.optional(input, optional, None),
            Plan::ShortestPath { input, all } => self.shortest(input, *all),
            Plan::NamedPath { input, variable } => {
                let rows = self.execute_input(input)?;
                let mut out = Vec::new();
                for mut row in rows {
                    let nodes = row.get(HIDDEN_PATH_NODES).cloned().unwrap_or(json!([]));
                    let rels = row.get(HIDDEN_PATH_RELS).cloned().unwrap_or(json!([]));
                    let mut seen = HashSet::new();
                    let unique = rels.as_array().into_iter().flatten().all(|rel| {
                        rel.get("_id")
                            .and_then(Value::as_str)
                            .is_none_or(|id| seen.insert(id.to_string()))
                    });
                    if !unique {
                        continue;
                    }
                    row.set(
                        variable.clone(),
                        json!({"nodes": nodes, "relationships": rels}),
                    );
                    out.push(row);
                }
                Ok(out)
            }
            Plan::PlannerHint { input, .. } => self.execute_input(input),
            Plan::ProcedureCall {
                input,
                name,
                arguments,
                yields,
            } => self.procedure(input, name, arguments, yields),
            Plan::PropertyMapFilter {
                input,
                variable,
                map,
            } => {
                let want = self.eval(map, &Row::new(), None)?;
                let Some(want) = want.as_object() else {
                    return Ok(Vec::new());
                };
                let rows = self.execute_input(input)?;
                let mut out = Vec::new();
                for row in rows {
                    let Some(entity) = row.get(variable).and_then(Value::as_object) else {
                        continue;
                    };
                    if want
                        .iter()
                        .all(|(key, value)| entity.get(key) == Some(value))
                    {
                        out.push(row);
                    }
                }
                Ok(out)
            }
            Plan::Unwind {
                input,
                expression,
                alias,
            } => {
                let rows = self.execute_input(input)?;
                let mut out = Vec::new();
                for row in rows {
                    let value = self.eval(expression, &row, None)?;
                    let items = match value {
                        Value::Array(items) => items,
                        Value::Null => continue,
                        value => vec![value],
                    };
                    for item in items {
                        self.tick()?;
                        let mut next = row.clone();
                        next.set(alias.clone(), item);
                        self.charge_row(&next)?;
                        out.push(next);
                    }
                }
                Ok(out)
            }
            Plan::Union { left, right, all } => {
                let mut out = self.execute_input(left)?;
                let right_rows = self.execute_input(right)?;
                for row in &right_rows {
                    self.tick()?;
                    self.charge_row(row)?;
                }
                out.extend(right_rows);
                if !*all {
                    let mut seen = HashSet::new();
                    let mut deduped = Vec::new();
                    for row in out {
                        self.tick()?;
                        let key = serde_json::to_string(&row.visible_values())?;
                        self.charge_memory(key.len().saturating_add(24))?;
                        if seen.insert(key) {
                            deduped.push(row);
                        }
                    }
                    out = deduped;
                }
                Ok(out)
            }
            Plan::Explain { input } => Ok(vec![{
                let mut row = Row::new();
                row.set("plan", Value::String(format!("{input:?}")));
                row
            }]),
            Plan::Profile { input } => self.execute_input(input),
            Plan::PeriodicCommit { input, .. } => self.execute_input(input),
            other => Err(Error::Query(format!(
                "cypher plan is not supported by the read-only executor: {other:?}"
            ))),
        }
    }

    fn execute_input(&mut self, plan: &Plan) -> Result<Vec<Row>> {
        let input = std::mem::take(&mut self.pending_input);
        self.execute(plan, input)
    }

    fn scan(
        &mut self,
        var: Option<&str>,
        label: Option<&str>,
        input: Vec<Row>,
    ) -> Result<Vec<Row>> {
        let rows = if input.is_empty() {
            vec![Row::new()]
        } else {
            input
        };
        let ids = self.graph.node_ids()?;
        let mut out = Vec::new();
        for base in rows {
            if let Some(var) = var {
                if let Some(bound) = base.get(var).filter(|value| !value.is_null()) {
                    if bound.is_object() {
                        if let Some(label) = label {
                            if !Graph::has_label(bound, label) {
                                continue;
                            }
                        }
                        let mut row = base.clone();
                        let empty_path = row
                            .get(HIDDEN_PATH_NODES)
                            .and_then(Value::as_array)
                            .is_none_or(Vec::is_empty);
                        if empty_path {
                            row.set(HIDDEN_PATH_NODES, json!([bound]));
                            row.set(HIDDEN_PATH_RELS, json!([]));
                        }
                        self.tick()?;
                        self.charge_row(&row)?;
                        out.push(row);
                        if out.len() >= self.options.max_rows {
                            self.truncated = true;
                            return Ok(out);
                        }
                        continue;
                    }
                }
            }
            for id in &ids {
                self.tick()?;
                let Some(node) = self.graph.node(id)? else {
                    continue;
                };
                if let Some(label) = label {
                    if !Graph::has_label(&node, label) {
                        continue;
                    }
                }
                let mut row = base.clone();
                if let Some(var) = var {
                    row.set(var, node.clone());
                } else {
                    row.set(HIDDEN_ANON_NODE, node.clone());
                }
                row.set(HIDDEN_ANON_ID, Value::String(id.clone()));
                row.set(HIDDEN_PATH_NODES, json!([node]));
                row.set(HIDDEN_PATH_RELS, json!([]));
                self.charge_row(&row)?;
                out.push(row);
                if out.len() >= self.options.max_rows {
                    self.truncated = true;
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    fn expand(
        &mut self,
        rows: Vec<Row>,
        src: Option<&str>,
        rel_var: Option<&str>,
        rel_types: &[String],
        range: Option<&cypher_rs_zk::RelationshipRange>,
        direction: Direction,
        dst: Option<&str>,
        dst_label: Option<&str>,
    ) -> Result<Vec<Row>> {
        let graph_direction = match direction {
            Direction::Outgoing => GraphDirection::Out,
            Direction::Incoming => GraphDirection::In,
            Direction::Undirected => GraphDirection::Both,
        };
        let mut out = Vec::new();
        for row in rows {
            let Some(current) = source_id(&row, src) else {
                continue;
            };
            if let Some(range) = range {
                let min = range.start.unwrap_or(1) as usize;
                let max = range
                    .end
                    .unwrap_or(self.options.depth() as u64)
                    .min(self.options.depth() as u64) as usize;
                if max < min {
                    continue;
                }
                let existing_rels = row
                    .get(HIDDEN_PATH_RELS)
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let mut used: HashSet<String> = existing_rels
                    .iter()
                    .filter_map(|rel| rel.get("_id").and_then(Value::as_str).map(str::to_string))
                    .collect();
                let mut rels = existing_rels;
                let mut nodes: Vec<Value> = row
                    .get(HIDDEN_PATH_NODES)
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                if nodes.is_empty() {
                    if let Some(node) = self.graph.node(&current)? {
                        nodes.push(node);
                    }
                }
                self.expand_range(
                    &row,
                    &current,
                    0,
                    min,
                    max,
                    graph_direction,
                    rel_types,
                    rel_var,
                    dst,
                    dst_label,
                    &mut used,
                    &mut rels,
                    &mut nodes,
                    &mut out,
                )?;
            } else {
                let path_rels: HashSet<String> = row
                    .get(HIDDEN_PATH_RELS)
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|rel| rel.get("_id").and_then(Value::as_str).map(str::to_string))
                    .collect();
                let bound_rel = rel_var
                    .and_then(|var| row.get(var))
                    .filter(|value| value.is_object())
                    .and_then(|value| value.get("_id"))
                    .and_then(Value::as_str)
                    .map(str::to_string);
                for edge in self
                    .graph
                    .view
                    .neighbors(&current, graph_direction, rel_types)?
                {
                    self.tick()?;
                    if path_rels.contains(&edge.edge_id) {
                        continue;
                    }
                    if bound_rel.as_deref().is_some_and(|id| id != edge.edge_id) {
                        continue;
                    }
                    let Some(target) = self.graph.node(&edge.target)? else {
                        continue;
                    };
                    if let Some(label) = dst_label {
                        if !Graph::has_label(&target, label) {
                            continue;
                        }
                    }
                    let rel = self.graph.relationship(&edge, &current, &edge.target)?;
                    let mut next = row.clone();
                    append_path(&mut next, &target, &rel);
                    if let Some(rel_var) = rel_var {
                        next.set(rel_var, rel.clone());
                    }
                    if let Some(dst) = dst {
                        if let Some(bound) = row.get(dst).filter(|value| value.is_object()) {
                            if bound.get("_id") != target.get("_id") {
                                continue;
                            }
                        }
                        next.set(dst, target);
                    } else {
                        next.set(HIDDEN_ANON_NODE, target);
                    }
                    next.set(HIDDEN_ANON_ID, Value::String(edge.target.clone()));
                    self.charge_row(&next)?;
                    out.push(next);
                    if out.len() >= self.options.max_rows {
                        self.truncated = true;
                        return Ok(out);
                    }
                }
            }
            if out.len() >= self.options.max_rows {
                self.truncated = true;
                break;
            }
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    fn expand_range(
        &mut self,
        base: &Row,
        current: &str,
        hops: usize,
        min: usize,
        max: usize,
        direction: GraphDirection,
        rel_types: &[String],
        rel_var: Option<&str>,
        dst: Option<&str>,
        dst_label: Option<&str>,
        used: &mut HashSet<String>,
        rels: &mut Vec<Value>,
        nodes: &mut Vec<Value>,
        out: &mut Vec<Row>,
    ) -> Result<()> {
        if hops >= min {
            if let Some(target) = nodes.last().cloned() {
                let label_ok = dst_label
                    .map(|label| Graph::has_label(&target, label))
                    .unwrap_or(true);
                let dst_ok = dst
                    .and_then(|dst| base.get(dst))
                    .filter(|value| value.is_object())
                    .is_none_or(|bound| bound.get("_id") == target.get("_id"));
                if label_ok && dst_ok {
                    let mut next = base.clone();
                    next.set(HIDDEN_PATH_NODES, Value::Array(nodes.clone()));
                    next.set(HIDDEN_PATH_RELS, Value::Array(rels.clone()));
                    if let Some(rel_var) = rel_var {
                        next.set(rel_var, Value::Array(rels.clone()));
                    }
                    if let Some(dst) = dst {
                        next.set(dst, target.clone());
                    } else {
                        next.set(HIDDEN_ANON_NODE, target);
                    }
                    if let Some(id) = nodes
                        .last()
                        .and_then(|node| node.get("_id"))
                        .and_then(Value::as_str)
                    {
                        next.set(HIDDEN_ANON_ID, Value::String(id.to_string()));
                    }
                    self.tick()?;
                    self.charge_row(&next)?;
                    out.push(next);
                    if out.len() >= self.options.max_rows {
                        self.truncated = true;
                        return Ok(());
                    }
                }
            }
        }
        if hops >= max {
            return Ok(());
        }
        for edge in self.graph.view.neighbors(current, direction, rel_types)? {
            self.tick()?;
            if !used.insert(edge.edge_id.clone()) {
                continue;
            }
            let Some(target) = self.graph.node(&edge.target)? else {
                used.remove(&edge.edge_id);
                continue;
            };
            let rel = self.graph.relationship(&edge, current, &edge.target)?;
            rels.push(rel);
            nodes.push(target);
            self.expand_range(
                base,
                &edge.target,
                hops + 1,
                min,
                max,
                direction,
                rel_types,
                rel_var,
                dst,
                dst_label,
                used,
                rels,
                nodes,
                out,
            )?;
            nodes.pop();
            rels.pop();
            used.remove(&edge.edge_id);
            if out.len() >= self.options.max_rows {
                self.truncated = true;
                return Ok(());
            }
        }
        Ok(())
    }

    fn procedure(
        &mut self,
        input: &Plan,
        name: &str,
        _arguments: &[Expr],
        yields: &[YieldItem],
    ) -> Result<Vec<Row>> {
        let input_rows = self.execute_input(input)?;
        if input_rows.is_empty() {
            return Ok(Vec::new());
        }
        let short = match name.to_ascii_lowercase().as_str() {
            "gds.pagerank.stream" => "pagerank",
            "gds.degree.stream" => "degree",
            "gds.connectedcomponents.stream" => "wcc",
            "gds.nodesimilarity.stream" => "similarity",
            "gds.labelpropagation.stream" => "labelpropagation",
            "gds.kcore.stream" => "kcore",
            "gds.degreedistribution.stream" => "degreedistribution",
            _ => {
                return Err(Error::Query(format!(
                    "unsupported Cypher procedure `{name}`"
                )))
            }
        };
        let base = GraphAlgorithmOptions {
            kind: GraphAlgorithmKind::Pagerank,
            direction: GraphDirection::Out,
            labels: Vec::new(),
            edge_filter: None,
            node_filter: None,
            as_of: None,
            limit: self.options.max_rows,
            max_nodes: self.options.gds_max_nodes,
            max_edges: self.options.gds_max_edges,
            max_memory_bytes: self.options.gds_max_memory_bytes,
            damping: 0.85,
            iterations: 100,
            tolerance: 1e-6,
            projection: None,
            timeout_ms: self.options.gds_timeout_ms.or(self.options.timeout_ms),
        };
        let value = match short {
            "pagerank" => crate::graph::graph_algorithm(
                self.graph.view,
                &GraphAlgorithmOptions {
                    kind: GraphAlgorithmKind::Pagerank,
                    ..base
                },
            )?,
            "degree" => crate::graph::graph_algorithm(
                self.graph.view,
                &GraphAlgorithmOptions {
                    kind: GraphAlgorithmKind::DegreeCentrality,
                    direction: GraphDirection::Both,
                    ..base
                },
            )?,
            "wcc" => crate::graph::graph_algorithm(
                self.graph.view,
                &GraphAlgorithmOptions {
                    kind: GraphAlgorithmKind::ConnectedComponents,
                    direction: GraphDirection::Both,
                    ..base
                },
            )?,
            "similarity" => self.similarity_procedure()?,
            "labelpropagation" => crate::graph::graph_algorithm(
                self.graph.view,
                &GraphAlgorithmOptions {
                    kind: GraphAlgorithmKind::LabelPropagation,
                    ..base
                },
            )?,
            "kcore" => crate::graph::graph_algorithm(
                self.graph.view,
                &GraphAlgorithmOptions {
                    kind: GraphAlgorithmKind::KCore,
                    direction: GraphDirection::Both,
                    ..base
                },
            )?,
            "degreedistribution" => crate::graph::graph_algorithm(
                self.graph.view,
                &GraphAlgorithmOptions {
                    kind: GraphAlgorithmKind::DegreeDistribution,
                    direction: GraphDirection::Both,
                    ..base
                },
            )?,
            _ => {
                return Err(Error::Query(format!(
                    "unsupported Cypher procedure `{name}`"
                )))
            }
        };
        let mut rows = Vec::new();
        if value["truncated"].as_bool().unwrap_or(false) {
            self.truncated = true;
        }
        if short == "wcc" {
            for component in value["components"].as_array().into_iter().flatten() {
                let component_id = component["id"].clone();
                let size = component["size"].clone();
                for node in component["nodes"].as_array().into_iter().flatten() {
                    let mut row = Row::new();
                    set_yield(&mut row, yields, "nodeId", node.clone());
                    set_yield(&mut row, yields, "componentId", component_id.clone());
                    set_yield(&mut row, yields, "size", size.clone());
                    rows.push(row);
                }
            }
        } else if short == "similarity" {
            for item in value["items"].as_array().into_iter().flatten() {
                let mut row = Row::new();
                set_yield(&mut row, yields, "node1", item["node1"].clone());
                set_yield(&mut row, yields, "node2", item["node2"].clone());
                set_yield(&mut row, yields, "similarity", item["similarity"].clone());
                rows.push(row);
            }
        } else if short == "labelpropagation" {
            for item in value["items"].as_array().into_iter().flatten() {
                let mut row = Row::new();
                set_yield(&mut row, yields, "nodeId", item["id"].clone());
                set_yield(&mut row, yields, "label", item["label"].clone());
                rows.push(row);
            }
        } else if short == "kcore" {
            for item in value["items"].as_array().into_iter().flatten() {
                let mut row = Row::new();
                set_yield(&mut row, yields, "nodeId", item["id"].clone());
                set_yield(&mut row, yields, "core", item["core"].clone());
                rows.push(row);
            }
        } else if short == "degreedistribution" {
            for item in value["items"].as_array().into_iter().flatten() {
                let mut row = Row::new();
                set_yield(&mut row, yields, "degree", item["degree"].clone());
                set_yield(&mut row, yields, "count", item["count"].clone());
                rows.push(row);
            }
        } else {
            for item in value["items"].as_array().into_iter().flatten() {
                let mut row = Row::new();
                set_yield(&mut row, yields, "nodeId", item["id"].clone());
                if short == "degree" {
                    set_yield(&mut row, yields, "degree", item["total"].clone());
                    set_yield(&mut row, yields, "in", item["in"].clone());
                    set_yield(&mut row, yields, "out", item["out"].clone());
                } else {
                    set_yield(&mut row, yields, "score", item["score"].clone());
                }
                rows.push(row);
            }
        }
        if rows.len() > self.options.max_rows {
            rows.truncate(self.options.max_rows);
            self.truncated = true;
        }
        Ok(rows)
    }

    fn similarity_procedure(&mut self) -> Result<Value> {
        let ids = self.graph.node_ids()?;
        if ids.len() > self.options.gds_max_nodes {
            return Err(Error::Query(format!(
                "graph algorithm node budget exceeded (limit {})",
                self.options.gds_max_nodes
            )));
        }
        let mut neighbors: HashMap<String, HashSet<String>> = HashMap::new();
        for id in &ids {
            self.tick()?;
            let mut set = HashSet::new();
            for edge in self.graph.view.neighbors(id, GraphDirection::Both, &[])? {
                self.tick()?;
                set.insert(edge.target);
            }
            let bytes = id.len()
                + std::mem::size_of::<String>()
                + set
                    .iter()
                    .map(|target| target.len() + std::mem::size_of::<String>() + 32)
                    .sum::<usize>();
            self.charge_memory(bytes)?;
            neighbors.insert(id.clone(), set);
        }
        let mut ids: Vec<String> = neighbors.keys().cloned().collect();
        ids.sort();
        let mut items = Vec::new();
        for left in 0..ids.len() {
            for right in left + 1..ids.len() {
                self.tick()?;
                let a = &neighbors[&ids[left]];
                let b = &neighbors[&ids[right]];
                if a.is_empty() && b.is_empty() {
                    continue;
                }
                let intersection = a.intersection(b).count() as f64;
                let union = a.union(b).count() as f64;
                let similarity = if union == 0.0 {
                    0.0
                } else {
                    intersection / union
                };
                if similarity > 0.0 {
                    items.push(json!({
                        "node1": ids[left],
                        "node2": ids[right],
                        "similarity": similarity,
                    }));
                }
                if items.len() >= self.options.max_rows {
                    self.truncated = true;
                    break;
                }
            }
            if items.len() >= self.options.max_rows {
                break;
            }
        }
        items.sort_by(|a, b| {
            b["similarity"]
                .as_f64()
                .partial_cmp(&a["similarity"].as_f64())
                .unwrap_or(Ordering::Equal)
                .then_with(|| {
                    a["node1"]
                        .as_str()
                        .unwrap_or_default()
                        .cmp(b["node1"].as_str().unwrap_or_default())
                })
                .then_with(|| {
                    a["node2"]
                        .as_str()
                        .unwrap_or_default()
                        .cmp(b["node2"].as_str().unwrap_or_default())
                })
        });
        Ok(json!({"items": items}))
    }

    fn project(
        &mut self,
        input: &Plan,
        include_existing: bool,
        exprs: &[ProjectExpr],
    ) -> Result<Vec<Row>> {
        let rows = self.execute_input(input)?;
        self.project_rows(rows, include_existing, exprs)
    }

    fn project_rows(
        &mut self,
        rows: Vec<Row>,
        include_existing: bool,
        exprs: &[ProjectExpr],
    ) -> Result<Vec<Row>> {
        let aggregate = exprs.iter().any(|expr| contains_aggregate(&expr.expr));
        if !aggregate {
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                self.tick()?;
                let mut next = if include_existing {
                    row.clone()
                } else {
                    Row::new()
                };
                for expr in exprs {
                    let value = self.eval(&expr.expr, &row, None)?;
                    let name = expr.alias.clone().unwrap_or_else(|| expr_name(&expr.expr));
                    next.set(name, value);
                }
                out.push(next);
            }
            return Ok(out);
        }
        let mut groups: Vec<(String, Vec<Row>)> = Vec::new();
        if rows.is_empty() {
            if exprs.iter().any(|expr| !contains_aggregate(&expr.expr)) {
                return Ok(Vec::new());
            }
            groups.push((String::new(), Vec::new()));
        }
        for row in &rows {
            let key_values: Vec<Value> = exprs
                .iter()
                .filter(|expr| !contains_aggregate(&expr.expr))
                .map(|expr| self.eval(&expr.expr, row, None))
                .collect::<Result<Vec<_>>>()?;
            let key = serde_json::to_string(&key_values)?;
            if let Some((_, group)) = groups.iter_mut().find(|(existing, _)| *existing == key) {
                group.push(row.clone());
            } else {
                groups.push((key, vec![row.clone()]));
            }
        }
        let mut out = Vec::new();
        for (_, group) in groups {
            let base = group.first().cloned().unwrap_or_else(Row::new);
            let mut next = if include_existing {
                base.clone()
            } else {
                Row::new()
            };
            for expr in exprs {
                let value = self.eval(&expr.expr, &base, Some(&group))?;
                let name = expr.alias.clone().unwrap_or_else(|| expr_name(&expr.expr));
                next.set(name, value);
            }
            out.push(next);
            if out.len() >= self.options.max_rows {
                self.truncated = true;
                break;
            }
        }
        Ok(out)
    }

    fn optional(&mut self, input: &Plan, optional: &Plan, pred: Option<&Expr>) -> Result<Vec<Row>> {
        let rows = self.execute_input(input)?;
        let optional_vars = plan_vars(optional);
        let mut out = Vec::new();
        for row in rows {
            let mut seed = row.clone();
            seed.set(HIDDEN_PATH_NODES, json!([]));
            seed.set(HIDDEN_PATH_RELS, json!([]));
            self.pending_input = vec![seed];
            let mut matched = self.execute(optional, Vec::new())?;
            self.pending_input.clear();
            let null_check = pred.is_some_and(|pred| {
                matches!(
                    pred,
                    Expr::Unary {
                        op: UnOp::IsNull | UnOp::IsNotNull,
                        ..
                    }
                )
            });
            if matched.is_empty() {
                let mut next = row.clone();
                for var in &optional_vars {
                    if next.get(var).is_none() {
                        next.set(var.clone(), Value::Null);
                    }
                }
                if null_check {
                    let keep = pred
                        .map(|pred| self.eval(pred, &next, None))
                        .transpose()?
                        .is_some_and(|value| truthy(&value));
                    if !keep {
                        continue;
                    }
                }
                self.tick()?;
                self.charge_row(&next)?;
                out.push(next);
                continue;
            }
            if let Some(pred) = pred {
                let mut kept = Vec::new();
                for candidate in matched {
                    self.tick()?;
                    if truthy(&self.eval(pred, &candidate, None)?) {
                        kept.push(candidate);
                    }
                }
                matched = kept;
            }
            if matched.is_empty() && !null_check {
                let mut next = row.clone();
                for var in &optional_vars {
                    if next.get(var).is_none() {
                        next.set(var.clone(), Value::Null);
                    }
                }
                self.tick()?;
                self.charge_row(&next)?;
                out.push(next);
                continue;
            }
            out.append(&mut matched);
        }
        Ok(out)
    }

    fn relationship_list_predicate(&mut self, pred: &Expr, row: &Row) -> Result<Option<bool>> {
        let Expr::Binary { op, lhs, rhs } = pred else {
            return Ok(None);
        };
        if !matches!(op, BinOp::Eq | BinOp::Neq) {
            return Ok(None);
        }
        let Expr::Property { base, key } = lhs.as_ref() else {
            return Ok(None);
        };
        let Expr::Variable(variable) = base.as_ref() else {
            return Ok(None);
        };
        let Some(rels) = row.get(variable).and_then(Value::as_array) else {
            return Ok(None);
        };
        if rels.is_empty()
            || !rels
                .iter()
                .all(|rel| rel.get("_id").is_some() && rel.get("_type").is_some())
        {
            return Ok(None);
        }
        let want = self.eval(rhs, row, None)?;
        let mut ok = true;
        for rel in rels {
            let got = rel.get(key).cloned().unwrap_or(Value::Null);
            let equal = json_eq(&got, &want);
            ok &= if matches!(op, BinOp::Eq) {
                equal
            } else {
                !equal
            };
        }
        Ok(Some(ok))
    }

    fn filter_project(
        &mut self,
        inner: &Plan,
        include_existing: bool,
        exprs: &[ProjectExpr],
        distinct: bool,
        pred: &Expr,
    ) -> Result<Vec<Row>> {
        let rows = self.execute_input(inner)?;
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        for row in rows {
            self.tick()?;
            let projected = self.project_rows(vec![row.clone()], include_existing, exprs)?;
            let Some(projected) = projected.into_iter().next() else {
                continue;
            };
            let mut scope = row;
            scope.merge(&projected);
            if !truthy(&self.eval(pred, &scope, None)?) {
                continue;
            }
            if distinct {
                let key = serde_json::to_string(&projected.visible_values())?;
                if !seen.insert(key) {
                    continue;
                }
            }
            out.push(projected);
        }
        Ok(out)
    }

    fn projected_rows_with_scope(&mut self, plan: &Plan) -> Result<Vec<(Row, Row)>> {
        if let Plan::Project {
            input,
            include_existing,
            exprs,
        } = plan
        {
            if !exprs.iter().any(|expr| contains_aggregate(&expr.expr)) {
                let bases = self.projected_rows_with_scope(input)?;
                let mut out = Vec::with_capacity(bases.len());
                for (row, scope) in bases {
                    let projected = self.project_rows(vec![row], *include_existing, exprs)?;
                    let Some(projected) = projected.into_iter().next() else {
                        continue;
                    };
                    let mut merged = scope;
                    merged.merge(&projected);
                    out.push((projected, merged));
                }
                return Ok(out);
            }
        }
        let rows = self.execute_input(plan)?;
        Ok(rows.into_iter().map(|row| (row.clone(), row)).collect())
    }

    fn eval_sort_key_with_aliases(
        &mut self,
        expr: &Expr,
        row: &Row,
        aliases: &HashMap<String, String>,
    ) -> Result<Value> {
        let name = expr_name(expr);
        if let Some(alias) = aliases.get(&name) {
            if let Some(value) = row.get(alias) {
                return Ok(value.clone());
            }
        }
        if let Some(value) = row.get(&name) {
            return Ok(value.clone());
        }
        match expr {
            Expr::Binary { op, lhs, rhs } => {
                let left = self.eval_sort_key_with_aliases(lhs, row, aliases)?;
                let right = self.eval_sort_key_with_aliases(rhs, row, aliases)?;
                eval_binary(*op, left, right)
            }
            Expr::Unary { op, operand } => {
                let value = self.eval_sort_key_with_aliases(operand, row, aliases)?;
                eval_unary(*op, value)
            }
            _ => self.eval(expr, row, None),
        }
    }

    fn sort(&mut self, input: &Plan, keys: &[SortKey]) -> Result<Vec<Row>> {
        if let Plan::Project {
            input: inner,
            include_existing,
            exprs,
        } = input
        {
            if exprs.iter().any(|expr| contains_aggregate(&expr.expr)) {
                let aggregate_order = keys.iter().any(|key| {
                    contains_aggregate(&key.expr)
                        || exprs.iter().any(|expr| {
                            contains_aggregate(&expr.expr)
                                && (expr.alias.as_deref() == Some(expr_name(&key.expr).as_str())
                                    || expr_name(&expr.expr) == expr_name(&key.expr))
                        })
                });
                if !aggregate_order {
                    let rows = self.sort(inner, keys)?;
                    return self.project_rows(rows, *include_existing, exprs);
                }
                let rows = self.execute_input(inner)?;
                let projected = self.project_rows(rows, *include_existing, exprs)?;
                let aliases: HashMap<String, String> = exprs
                    .iter()
                    .filter(|expr| contains_aggregate(&expr.expr))
                    .filter_map(|expr| {
                        expr.alias
                            .as_ref()
                            .map(|alias| (expr_name(&expr.expr), alias.clone()))
                    })
                    .collect();
                let mut keyed = Vec::with_capacity(projected.len());
                for row in projected {
                    self.tick()?;
                    self.charge_row(&row)?;
                    let mut values = Vec::with_capacity(keys.len());
                    for key in keys {
                        values.push(self.eval_sort_key_with_aliases(&key.expr, &row, &aliases)?);
                    }
                    keyed.push((values, row));
                }
                keyed.sort_by(|(a, _), (b, _)| {
                    for (index, key) in keys.iter().enumerate() {
                        let order = cmp_json(&a[index], &b[index]);
                        if order != Ordering::Equal {
                            return if key.desc { order.reverse() } else { order };
                        }
                    }
                    Ordering::Equal
                });
                return Ok(keyed.into_iter().map(|(_, row)| row).collect());
            }
            let pairs = self.projected_rows_with_scope(input)?;
            let mut aliases = HashMap::new();
            collect_projection_aliases(input, &mut aliases);
            let mut keyed = Vec::new();
            for (projected, scope) in pairs {
                self.tick()?;
                self.charge_row(&projected)?;
                let mut values = Vec::with_capacity(keys.len());
                for key in keys {
                    values.push(self.eval_sort_key_with_aliases(&key.expr, &scope, &aliases)?);
                }
                keyed.push((values, projected));
            }
            keyed.sort_by(|(a, _), (b, _)| {
                for (index, key) in keys.iter().enumerate() {
                    let order = cmp_json(&a[index], &b[index]);
                    if order != Ordering::Equal {
                        return if key.desc { order.reverse() } else { order };
                    }
                }
                Ordering::Equal
            });
            return Ok(keyed.into_iter().map(|(_, row)| row).collect());
        }
        let mut rows = self.execute_input(input)?;
        let mut keyed = Vec::with_capacity(rows.len());
        for row in rows.drain(..) {
            self.tick()?;
            self.charge_row(&row)?;
            let mut values = Vec::with_capacity(keys.len());
            for key in keys {
                values.push(self.eval(&key.expr, &row, None)?);
            }
            keyed.push((values, row));
        }
        keyed.sort_by(|(a, _), (b, _)| {
            for (index, key) in keys.iter().enumerate() {
                let order = cmp_json(&a[index], &b[index]);
                if order != Ordering::Equal {
                    return if key.desc { order.reverse() } else { order };
                }
            }
            Ordering::Equal
        });
        Ok(keyed.into_iter().map(|(_, row)| row).collect())
    }

    fn shortest(&mut self, input: &Plan, all: bool) -> Result<Vec<Row>> {
        let rows = self.execute_input(input)?;
        let mut groups: HashMap<String, Vec<Row>> = HashMap::new();
        for row in rows {
            let nodes = row.get(HIDDEN_PATH_NODES).and_then(Value::as_array);
            let len = row
                .get(HIDDEN_PATH_RELS)
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            let key = nodes
                .and_then(|nodes| {
                    Some(format!(
                        "{}|{}",
                        nodes.first()?.get("_id")?.as_str()?,
                        nodes.last()?.get("_id")?.as_str()?
                    ))
                })
                .unwrap_or_default();
            let entry = groups.entry(key).or_default();
            let keep = entry
                .first()
                .and_then(|first| first.get(HIDDEN_PATH_RELS))
                .and_then(Value::as_array)
                .map_or(len, Vec::len);
            if len < keep {
                entry.clear();
            }
            if entry.is_empty() || (all && len == keep) || (!all && entry.is_empty()) {
                entry.push(row);
            }
        }
        Ok(groups.into_values().flatten().collect())
    }

    fn exists_subquery(&mut self, body: &str, row: &Row) -> Result<bool> {
        let trimmed = body.trim();
        let lower = trimmed.to_ascii_lowercase();
        let has_clause = ["match", "with", "unwind", "return", "optional", "call"]
            .iter()
            .any(|keyword| lower.starts_with(keyword));
        let mut subquery = if has_clause {
            trimmed.to_string()
        } else {
            format!("MATCH {trimmed} RETURN true")
        };
        if !subquery.to_ascii_lowercase().contains("return") {
            subquery.push_str(" RETURN true");
        }
        let expanded = expand_exists_subqueries(&prepend_optional_anchor(&subquery))?;
        let normalized = normalize_query_text(&expanded);
        let query: Query = parse(&normalized)
            .map_err(|error| Error::Query(format!("cypher subquery parse error: {error}")))?;
        if query.statement_count() != 1 {
            return Err(Error::Query(
                "cypher supports exactly one read-only statement".into(),
            ));
        }
        reject_unsupported_shapes(&query)?;
        if query.clauses.iter().any(is_write_clause) {
            return Err(Error::Query(
                "cypher write clauses are not supported in read-only mode".into(),
            ));
        }
        let extra_labels = collect_extra_labels(&query);
        let plan = plan_query(&query)
            .map_err(|error| Error::Query(format!("cypher subquery plan error: {error}")))?;
        let plan = inject_extra_labels(optimize(plan), &extra_labels);
        let mut bound: HashSet<String> = row.visible_values().keys().cloned().collect();
        validate_plan(&plan, &mut bound)?;
        let saved = std::mem::take(&mut self.pending_input);
        self.pending_input = vec![row.clone()];
        let result = self.execute(&plan, Vec::new());
        self.pending_input = saved;
        Ok(!result?.is_empty())
    }

    fn eval_usize(&mut self, expr: &Expr, row: &Row) -> Result<usize> {
        let value = self.eval(expr, row, None)?;
        match value {
            Value::Number(number) => number.as_u64().map(|value| value as usize).ok_or_else(|| {
                Error::Query("cypher LIMIT/SKIP must be a non-negative integer".into())
            }),
            _ => Err(Error::Query(
                "cypher LIMIT/SKIP must be a non-negative integer".into(),
            )),
        }
    }

    fn pattern_matches(&mut self, pattern: &Pattern, base: &Row) -> Result<Vec<Row>> {
        let mut seeds = Vec::new();
        let anchor = &pattern.anchor;
        if let Some(var) = &anchor.var {
            if let Some(bound) = base.get(var).filter(|value| value.is_object()) {
                if self.pattern_node_ok(bound, anchor, base)? {
                    let mut row = base.clone();
                    row.set(HIDDEN_PATH_NODES, json!([bound]));
                    row.set(HIDDEN_PATH_RELS, json!([]));
                    seeds.push(row);
                }
            } else {
                for id in self.graph.node_ids()? {
                    self.tick()?;
                    let Some(node) = self.graph.node(&id)? else {
                        continue;
                    };
                    if self.pattern_node_ok(&node, anchor, base)? {
                        let mut row = base.clone();
                        row.set(var.clone(), node.clone());
                        row.set(HIDDEN_ANON_ID, Value::String(id.clone()));
                        row.set(HIDDEN_PATH_NODES, json!([node]));
                        row.set(HIDDEN_PATH_RELS, json!([]));
                        seeds.push(row);
                    }
                }
            }
        } else {
            for id in self.graph.node_ids()? {
                self.tick()?;
                let Some(node) = self.graph.node(&id)? else {
                    continue;
                };
                if self.pattern_node_ok(&node, anchor, base)? {
                    let mut row = base.clone();
                    row.set(HIDDEN_ANON_ID, Value::String(id.clone()));
                    row.set(HIDDEN_PATH_NODES, json!([node]));
                    row.set(HIDDEN_PATH_RELS, json!([]));
                    seeds.push(row);
                }
            }
        }
        let mut out = Vec::new();
        for seed in seeds {
            let mut used = HashSet::new();
            self.walk_pattern(&pattern.chain, 0, seed, &mut used, &mut out)?;
            if out.len() >= self.options.max_rows {
                self.truncated = true;
                break;
            }
        }
        Ok(out)
    }

    fn walk_pattern(
        &mut self,
        chain: &[cypher_rs_zk::ast::RelChain],
        index: usize,
        row: Row,
        used: &mut HashSet<String>,
        out: &mut Vec<Row>,
    ) -> Result<()> {
        if index >= chain.len() {
            out.push(row);
            if out.len() >= self.options.max_rows {
                self.truncated = true;
            }
            return Ok(());
        }
        if out.len() >= self.options.max_rows {
            return Ok(());
        }
        let Some(current) = row
            .get(HIDDEN_PATH_NODES)
            .and_then(Value::as_array)
            .and_then(|nodes| nodes.last())
            .and_then(|node| node.get("_id"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| source_id(&row, None))
        else {
            return Ok(());
        };
        let chain_item = &chain[index];
        if let Some(range) = chain_item.rel.range {
            let min = range.start.unwrap_or(1) as usize;
            let max = range
                .end
                .unwrap_or(self.options.depth() as u64)
                .min(self.options.depth() as u64) as usize;
            if max < min {
                return Ok(());
            }
            self.walk_pattern_range(
                chain,
                index,
                row,
                &current,
                0,
                min,
                max,
                &mut HashSet::new(),
                &mut Vec::new(),
                used,
                out,
            )?;
            return Ok(());
        }
        let graph_direction = match chain_item.rel.direction {
            Direction::Outgoing => GraphDirection::Out,
            Direction::Incoming => GraphDirection::In,
            Direction::Undirected => GraphDirection::Both,
        };
        for edge in self
            .graph
            .view
            .neighbors(&current, graph_direction, &chain_item.rel.types)?
        {
            self.tick()?;
            if !used.insert(edge.edge_id.clone()) {
                continue;
            }
            let Some(target) = self.graph.node(&edge.target)? else {
                used.remove(&edge.edge_id);
                continue;
            };
            let rel = self.graph.relationship(&edge, &current, &edge.target)?;
            if !self.pattern_rel_ok(&rel, &chain_item.rel, &row)?
                || !self.pattern_node_ok(&target, &chain_item.node, &row)?
            {
                used.remove(&edge.edge_id);
                continue;
            }
            if let Some(var) = &chain_item.node.var {
                if let Some(bound) = row.get(var).filter(|value| value.is_object()) {
                    if bound.get("_id") != target.get("_id") {
                        used.remove(&edge.edge_id);
                        continue;
                    }
                }
            }
            let mut next = row.clone();
            append_path(&mut next, &target, &rel);
            if let Some(var) = &chain_item.rel.var {
                next.set(var.clone(), rel);
            }
            if let Some(var) = &chain_item.node.var {
                next.set(var.clone(), target);
            }
            self.walk_pattern(chain, index + 1, next, used, out)?;
            used.remove(&edge.edge_id);
            if out.len() >= self.options.max_rows {
                return Ok(());
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn walk_pattern_range(
        &mut self,
        chain: &[cypher_rs_zk::ast::RelChain],
        index: usize,
        row: Row,
        current: &str,
        hops: usize,
        min: usize,
        max: usize,
        path_used: &mut HashSet<String>,
        rels: &mut Vec<Value>,
        used: &mut HashSet<String>,
        out: &mut Vec<Row>,
    ) -> Result<()> {
        if hops >= min && !rels.is_empty() {
            let endpoint_ok = chain[index]
                .node
                .var
                .as_ref()
                .and_then(|var| row.get(var))
                .filter(|value| value.is_object())
                .is_none_or(|bound| {
                    row.get(HIDDEN_PATH_NODES)
                        .and_then(Value::as_array)
                        .and_then(|nodes| nodes.last())
                        .and_then(|node| node.get("_id"))
                        == bound.get("_id")
                });
            if endpoint_ok {
                let mut terminal = row.clone();
                terminal.set(HIDDEN_PATH_RELS, Value::Array(rels.clone()));
                if let Some(var) = &chain[index].rel.var {
                    terminal.set(var.clone(), Value::Array(rels.clone()));
                }
                if let Some(var) = &chain[index].node.var {
                    if let Some(target) = terminal
                        .get(HIDDEN_PATH_NODES)
                        .and_then(Value::as_array)
                        .and_then(|nodes| nodes.last())
                        .cloned()
                    {
                        terminal.set(var.clone(), target);
                    }
                }
                self.walk_pattern(chain, index + 1, terminal, used, out)?;
            }
        }
        if hops >= max || out.len() >= self.options.max_rows {
            return Ok(());
        }
        let graph_direction = match chain[index].rel.direction {
            Direction::Outgoing => GraphDirection::Out,
            Direction::Incoming => GraphDirection::In,
            Direction::Undirected => GraphDirection::Both,
        };
        for edge in self
            .graph
            .view
            .neighbors(current, graph_direction, &chain[index].rel.types)?
        {
            self.tick()?;
            if !path_used.insert(edge.edge_id.clone()) {
                continue;
            }
            let Some(target) = self.graph.node(&edge.target)? else {
                path_used.remove(&edge.edge_id);
                continue;
            };
            let rel = self.graph.relationship(&edge, current, &edge.target)?;
            if !self.pattern_rel_ok(&rel, &chain[index].rel, &row)?
                || !self.pattern_node_ok(&target, &chain[index].node, &row)?
            {
                path_used.remove(&edge.edge_id);
                continue;
            }
            let mut next = row.clone();
            append_path(&mut next, &target, &rel);
            rels.push(rel);
            let target_id = target
                .get("_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            self.walk_pattern_range(
                chain,
                index,
                next,
                &target_id,
                hops + 1,
                min,
                max,
                path_used,
                rels,
                used,
                out,
            )?;
            rels.pop();
            path_used.remove(&edge.edge_id);
        }
        Ok(())
    }

    fn pattern_node_ok(&mut self, node: &Value, pattern: &NodePattern, row: &Row) -> Result<bool> {
        for label in &pattern.labels {
            if !Graph::has_label(node, label) {
                return Ok(false);
            }
        }
        for (key, expected) in &pattern.properties {
            let expected = self.eval(expected, row, None)?;
            let actual = node.get(key).cloned().unwrap_or(Value::Null);
            if eq3(&actual, &expected) != Value::Bool(true) {
                return Ok(false);
            }
        }
        if let Some(map) = &pattern.property_map {
            let map = self.eval(map, row, None)?;
            if let Some(map) = map.as_object() {
                for (key, expected) in map {
                    if eq3(&node.get(key).cloned().unwrap_or(Value::Null), expected)
                        != Value::Bool(true)
                    {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    fn pattern_rel_ok(&mut self, rel: &Value, pattern: &RelPattern, row: &Row) -> Result<bool> {
        for (key, expected) in &pattern.properties {
            let expected = self.eval(expected, row, None)?;
            let actual = rel.get(key).cloned().unwrap_or(Value::Null);
            if eq3(&actual, &expected) != Value::Bool(true) {
                return Ok(false);
            }
        }
        if let Some(map) = &pattern.property_map {
            let map = self.eval(map, row, None)?;
            if let Some(map) = map.as_object() {
                for (key, expected) in map {
                    if eq3(&rel.get(key).cloned().unwrap_or(Value::Null), expected)
                        != Value::Bool(true)
                    {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    fn eval(&mut self, expr: &Expr, row: &Row, group: Option<&[Row]>) -> Result<Value> {
        self.tick()?;
        match expr {
            Expr::Literal(literal) => Ok(match literal {
                Literal::Int(value) => json!(value),
                Literal::Float(value) => json!(value),
                Literal::String(value) => json!(value),
                Literal::Bool(value) => json!(value),
                Literal::Null => Value::Null,
            }),
            Expr::List(items) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    out.push(self.eval(item, row, group)?);
                }
                Ok(Value::Array(out))
            }
            Expr::Map(items) => {
                let mut out = Map::new();
                for (key, value) in items {
                    out.insert(key.clone(), self.eval(value, row, group)?);
                }
                Ok(Value::Object(out))
            }
            Expr::Variable(name) => Ok(row.get(name).cloned().unwrap_or(Value::Null)),
            Expr::Param(name) => Ok(self.params.get(name).cloned().unwrap_or(Value::Null)),
            Expr::Property { base, key } => {
                let base = self.eval(base, row, group)?;
                Ok(base
                    .get(key)
                    .cloned()
                    .or_else(|| temporal_property(&base, key))
                    .or_else(|| base.get("properties").and_then(|p| p.get(key)).cloned())
                    .unwrap_or(Value::Null))
            }
            Expr::LabelPredicate { expression, labels } => {
                let value = self.eval(expression, row, group)?;
                if value.is_null() {
                    return Ok(Value::Null);
                }
                let ok = if let Some(rel_type) = Graph::relationship_type(&value) {
                    labels.iter().all(|label| label == rel_type)
                } else {
                    labels.iter().all(|label| Graph::has_label(&value, label))
                };
                Ok(json!(ok))
            }
            Expr::Binary { op, lhs, rhs } => {
                let lhs = self.eval(lhs, row, group)?;
                let rhs = self.eval(rhs, row, group)?;
                eval_binary(*op, lhs, rhs)
            }
            Expr::ComparisonChain {
                operators,
                arguments,
            } => {
                for (index, op) in operators.iter().enumerate() {
                    let lhs = self.eval(&arguments[index], row, group)?;
                    let rhs = self.eval(&arguments[index + 1], row, group)?;
                    if !truthy(&eval_binary(*op, lhs, rhs)?) {
                        return Ok(json!(false));
                    }
                }
                Ok(json!(true))
            }
            Expr::Unary { op, operand } => {
                let value = self.eval(operand, row, group)?;
                eval_unary(*op, value)
            }
            Expr::FunctionCall {
                name,
                distinct,
                arguments,
            } => {
                if is_aggregate(name) {
                    let Some(group) = group else {
                        return Err(Error::Query(format!(
                            "aggregate `{name}` used outside an aggregation context"
                        )));
                    };
                    return self.eval_aggregate(name, *distinct, arguments, group);
                }
                let args = match arguments {
                    FunctionArguments::Expressions(exprs) => exprs
                        .iter()
                        .map(|expr| self.eval(expr, row, group))
                        .collect::<Result<Vec<_>>>()?,
                    FunctionArguments::Wildcard => Vec::new(),
                };
                if name.eq_ignore_ascii_case("nonodb_exists") {
                    let Some(body) = args.first().and_then(Value::as_str) else {
                        return Ok(Value::Bool(false));
                    };
                    return Ok(Value::Bool(self.exists_subquery(body, row)?));
                }
                self.call_function(name, args)
            }
            Expr::ListComprehension {
                variable,
                source,
                predicate,
                projection,
            } => {
                let source = self.eval(source, row, group)?;
                let Value::Array(items) = source else {
                    return Ok(Value::Null);
                };
                let mut out = Vec::new();
                for item in items {
                    let mut local = row.clone();
                    local.set(variable.clone(), item);
                    if let Some(predicate) = predicate {
                        if !truthy(&self.eval(predicate, &local, group)?) {
                            continue;
                        }
                    }
                    out.push(match projection {
                        Some(projection) => self.eval(projection, &local, group)?,
                        None => local.get(variable).cloned().unwrap_or(Value::Null),
                    });
                }
                Ok(Value::Array(out))
            }
            Expr::CollectionPredicate {
                kind,
                variable,
                source,
                predicate,
            } => {
                let source = self.eval(source, row, group)?;
                let Value::Array(items) = source else {
                    return Ok(Value::Null);
                };
                let item_count = items.len();
                let mut matches = 0usize;
                for item in items {
                    let mut local = row.clone();
                    local.set(variable.clone(), item);
                    let matched = match predicate {
                        Some(predicate) => truthy(&self.eval(predicate, &local, group)?),
                        None => false,
                    };
                    if matched {
                        matches += 1;
                    }
                }
                Ok(json!(match kind {
                    CollectionPredicateKind::All => matches == item_count,
                    CollectionPredicateKind::Any => matches > 0,
                    CollectionPredicateKind::None => matches == 0,
                    CollectionPredicateKind::Single => matches == 1,
                }))
            }
            Expr::Case {
                operand,
                alternatives,
                else_expr,
            } => {
                let operand = match operand {
                    Some(operand) => Some(self.eval(operand, row, group)?),
                    None => None,
                };
                for CaseAlternative { when, then } in alternatives {
                    let matched = if let Some(operand) = &operand {
                        json_eq(operand, &self.eval(when, row, group)?)
                    } else {
                        truthy(&self.eval(when, row, group)?)
                    };
                    if matched {
                        return self.eval(then, row, group);
                    }
                }
                match else_expr {
                    Some(else_expr) => self.eval(else_expr, row, group),
                    None => Ok(Value::Null),
                }
            }
            Expr::Subscript { base, index } => {
                let base = self.eval(base, row, group)?;
                let index = self.eval(index, row, group)?;
                match (base, index) {
                    (Value::Array(items), Value::Number(index)) => Ok(index
                        .as_i64()
                        .and_then(|index| {
                            let index = if index < 0 {
                                items.len() as i64 + index
                            } else {
                                index
                            };
                            usize::try_from(index)
                                .ok()
                                .and_then(|index| items.get(index).cloned())
                        })
                        .unwrap_or(Value::Null)),
                    (Value::Object(map), Value::String(key)) => {
                        Ok(map.get(&key).cloned().unwrap_or(Value::Null))
                    }
                    _ => Ok(Value::Null),
                }
            }
            Expr::Slice { base, start, end } => {
                let base = self.eval(base, row, group)?;
                let Value::Array(items) = base else {
                    return Ok(Value::Null);
                };
                let start = match start {
                    Some(start) => self.eval(start, row, group)?,
                    None => json!(0),
                };
                let end = match end {
                    Some(end) => self.eval(end, row, group)?,
                    None => json!(items.len()),
                };
                let len = items.len() as i64;
                let normalize = |value: i64| -> usize {
                    if value < 0 {
                        (len + value).max(0) as usize
                    } else {
                        value as usize
                    }
                };
                let start = normalize(start.as_i64().unwrap_or(0)).min(items.len());
                let end = normalize(end.as_i64().unwrap_or(len)).min(items.len());
                Ok(Value::Array(
                    items
                        .into_iter()
                        .skip(start)
                        .take(end.saturating_sub(start))
                        .collect(),
                ))
            }
            Expr::MapProjection { base, items } => {
                let base = self.eval(base, row, group)?;
                let mut out = Map::new();
                for item in items {
                    match item {
                        MapProjectionItem::Literal { key, value } => {
                            out.insert(key.clone(), self.eval(value, row, group)?);
                        }
                        MapProjectionItem::Property(key) => {
                            out.insert(key.clone(), base.get(key).cloned().unwrap_or(Value::Null));
                        }
                        MapProjectionItem::Variable(name) => {
                            out.insert(name.clone(), row.get(name).cloned().unwrap_or(Value::Null));
                        }
                        MapProjectionItem::AllProperties => {
                            if let Some(map) = base.as_object() {
                                for (key, value) in map {
                                    out.insert(key.clone(), value.clone());
                                }
                            }
                        }
                    }
                }
                Ok(Value::Object(out))
            }
            Expr::Filter {
                variable,
                source,
                predicate,
            } => {
                let source = self.eval(source, row, group)?;
                let Value::Array(items) = source else {
                    return Ok(Value::Null);
                };
                let mut out = Vec::new();
                for item in items {
                    let mut local = row.clone();
                    local.set(variable.clone(), item.clone());
                    if predicate.as_ref().is_none_or(|predicate| {
                        truthy(&self.eval(predicate, &local, group).unwrap_or(Value::Null))
                    }) {
                        out.push(item);
                    }
                }
                Ok(Value::Array(out))
            }
            Expr::Extract {
                variable,
                source,
                projection,
            } => {
                let source = self.eval(source, row, group)?;
                let Value::Array(items) = source else {
                    return Ok(Value::Null);
                };
                let mut out = Vec::new();
                for item in items {
                    let mut local = row.clone();
                    local.set(variable.clone(), item);
                    out.push(match projection {
                        Some(projection) => self.eval(projection, &local, group)?,
                        None => local.get(variable).cloned().unwrap_or(Value::Null),
                    });
                }
                Ok(Value::Array(out))
            }
            Expr::Reduce {
                accumulator,
                initial,
                variable,
                source,
                expression,
            } => {
                let mut acc = self.eval(initial, row, group)?;
                let source = self.eval(source, row, group)?;
                let Value::Array(items) = source else {
                    return Ok(Value::Null);
                };
                for item in items {
                    let mut local = row.clone();
                    local.set(accumulator.clone(), acc);
                    local.set(variable.clone(), item);
                    acc = match expression {
                        Some(expression) => self.eval(expression, &local, group)?,
                        None => local.get(accumulator).cloned().unwrap_or(Value::Null),
                    };
                }
                Ok(acc)
            }
            Expr::PatternExpression { pattern } => {
                let matches = self.pattern_matches(pattern, row)?;
                Ok(Value::Bool(!matches.is_empty()))
            }
            Expr::PatternComprehension {
                path_variable,
                pattern,
                predicate,
                projection,
            } => {
                let matches = self.pattern_matches(pattern, row)?;
                let mut out = Vec::new();
                for mut local in matches {
                    if let Some(variable) = path_variable {
                        local.set(
                            variable.clone(),
                            json!({
                                "nodes": local.get(HIDDEN_PATH_NODES).cloned().unwrap_or(json!([])),
                                "relationships": local.get(HIDDEN_PATH_RELS).cloned().unwrap_or(json!([])),
                            }),
                        );
                    }
                    if let Some(predicate) = predicate {
                        if !truthy(&self.eval(predicate, &local, group)?) {
                            continue;
                        }
                    }
                    out.push(self.eval(projection, &local, group)?);
                }
                Ok(Value::Array(out))
            }
        }
    }

    fn eval_aggregate(
        &mut self,
        name: &str,
        distinct: bool,
        arguments: &FunctionArguments,
        group: &[Row],
    ) -> Result<Value> {
        let wildcard = matches!(arguments, FunctionArguments::Wildcard);
        let mut values: Vec<Value> = Vec::new();
        match arguments {
            FunctionArguments::Wildcard => {
                for _ in group {
                    values.push(Value::Null);
                }
            }
            FunctionArguments::Expressions(exprs) => {
                for row in group {
                    if exprs.len() == 1 {
                        values.push(self.eval(&exprs[0], row, None)?);
                    } else {
                        let mut tuple = Vec::new();
                        for expr in exprs {
                            tuple.push(self.eval(expr, row, None)?);
                        }
                        values.push(Value::Array(tuple));
                    }
                }
            }
        }
        if !wildcard {
            values.retain(|value| !value.is_null());
        }
        if distinct {
            let mut seen = HashSet::new();
            values.retain(|value| seen.insert(serde_json::to_string(value).unwrap_or_default()));
        }
        let name = name.to_ascii_lowercase();
        if name == "collect" {
            let bytes = serde_json::to_vec(&values).map_or(0, |bytes| bytes.len());
            self.charge_memory(bytes)?;
        }
        match name.as_str() {
            "count" => Ok(json!(values.len())),
            "collect" => Ok(Value::Array(values)),
            "sum" => {
                let sum = values.iter().filter_map(number).sum::<f64>();
                Ok(json!(if sum.abs() < 1e-12 { 0.0 } else { sum }))
            }
            "avg" => {
                let numbers: Vec<f64> = values.iter().filter_map(number).collect();
                if numbers.is_empty() {
                    Ok(Value::Null)
                } else {
                    Ok(json!(numbers.iter().sum::<f64>() / numbers.len() as f64))
                }
            }
            "min" => Ok(values.into_iter().min_by(cmp_json).unwrap_or(Value::Null)),
            "max" => Ok(values.into_iter().max_by(cmp_json).unwrap_or(Value::Null)),
            _ => Err(Error::Query(format!("unsupported aggregate `{name}`"))),
        }
    }

    fn call_function(&mut self, name: &str, args: Vec<Value>) -> Result<Value> {
        let arg = |index: usize| args.get(index).cloned().unwrap_or(Value::Null);
        let name = name.to_ascii_lowercase();
        match name.as_str() {
            "rand" => Ok(json!(0.5)),
            "coalesce" => Ok(args
                .into_iter()
                .find(|value| !value.is_null())
                .unwrap_or(Value::Null)),
            "size" => Ok(match arg(0) {
                Value::Array(items) => json!(items.len()),
                Value::String(value) => json!(value.chars().count()),
                Value::Object(map) => json!(map.len()),
                _ => Value::Null,
            }),
            "length" => Ok(match arg(0) {
                Value::Array(items) => json!(items.len()),
                Value::String(value) => json!(value.chars().count()),
                Value::Object(map) => map
                    .get("relationships")
                    .and_then(Value::as_array)
                    .map_or(Value::Null, |rels| json!(rels.len())),
                _ => Value::Null,
            }),
            "head" => Ok(arg(0)
                .as_array()
                .and_then(|items| items.first().cloned())
                .unwrap_or(Value::Null)),
            "last" => Ok(arg(0)
                .as_array()
                .and_then(|items| items.last().cloned())
                .unwrap_or(Value::Null)),
            "tail" => Ok(Value::Array(
                arg(0)
                    .as_array()
                    .map(|items| items.iter().skip(1).cloned().collect())
                    .unwrap_or_default(),
            )),
            "keys" => Ok(match arg(0) {
                Value::Object(map) => {
                    let relationship = map.contains_key("_type");
                    let entity =
                        relationship || map.contains_key("_id") || map.contains_key("_labels");
                    Value::Array(
                        map.iter()
                            .filter(|(key, value)| {
                                !key.starts_with('_')
                                    && *key != "from"
                                    && *key != "to"
                                    && (!relationship || *key != "label")
                                    && (!entity || !value.is_null())
                            })
                            .map(|(key, _)| key.clone())
                            .map(Value::String)
                            .collect(),
                    )
                }
                _ => Value::Null,
            }),
            "properties" => Ok(match arg(0) {
                Value::Object(map) => {
                    let relationship = map.contains_key("_type");
                    Value::Object(
                        map.into_iter()
                            .filter(|(key, _)| {
                                !key.starts_with('_')
                                    && key != "from"
                                    && key != "to"
                                    && (!relationship || key != "label")
                            })
                            .collect(),
                    )
                }
                _ => Value::Null,
            }),
            "nodes" => Ok(match arg(0) {
                Value::Null => Value::Null,
                value => value.get("nodes").cloned().unwrap_or_else(|| json!([])),
            }),
            "relationships" | "rels" => Ok(match arg(0) {
                Value::Null => Value::Null,
                value => value
                    .get("relationships")
                    .cloned()
                    .unwrap_or_else(|| json!([])),
            }),
            "labels" => Ok(match arg(0) {
                Value::Null => Value::Null,
                value => value
                    .get("_labels")
                    .cloned()
                    .or_else(|| value.get("labels").cloned())
                    .unwrap_or_else(|| json!([])),
            }),
            "type" => Ok(Graph::relationship_type(&arg(0))
                .map(|value| Value::String(value.to_string()))
                .unwrap_or(Value::Null)),
            "id" => Ok(arg(0).get("_id").cloned().unwrap_or(Value::Null)),
            "startnode" => {
                let id = arg(0)
                    .get("_start")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                match id {
                    Some(id) => Ok(self.graph.node(&id)?.unwrap_or(Value::Null)),
                    None => Ok(Value::Null),
                }
            }
            "endnode" => {
                let id = arg(0)
                    .get("_end")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                match id {
                    Some(id) => Ok(self.graph.node(&id)?.unwrap_or(Value::Null)),
                    None => Ok(Value::Null),
                }
            }
            "tostring" => Ok(match arg(0) {
                Value::String(value) => Value::String(value),
                Value::Null => Value::Null,
                value if value.get("__type").is_some() => Value::String(format_temporal(&value)),
                value => Value::String(value.to_string()),
            }),
            "tointeger" => Ok(match arg(0) {
                Value::String(value) => value
                    .trim()
                    .parse::<i64>()
                    .map(|value| json!(value))
                    .or_else(|_| {
                        value
                            .trim()
                            .parse::<f64>()
                            .map(|value| json!(value.trunc() as i64))
                    })
                    .unwrap_or(Value::Null),
                Value::Number(value) => value
                    .as_i64()
                    .or_else(|| value.as_f64().map(|value| value.trunc() as i64))
                    .map(|value| json!(value))
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            }),
            "tofloat" => Ok(match arg(0) {
                Value::String(value) => value
                    .trim()
                    .parse::<f64>()
                    .map(|value| json!(value))
                    .unwrap_or(Value::Null),
                value => number(&value)
                    .map(|value| json!(value))
                    .unwrap_or(Value::Null),
            }),
            "toboolean" => Ok(match arg(0) {
                Value::Bool(value) => json!(value),
                Value::String(value) if value.eq_ignore_ascii_case("true") => json!(true),
                Value::String(value) if value.eq_ignore_ascii_case("false") => json!(false),
                _ => Value::Null,
            }),
            "exists" => Ok(json!(!arg(0).is_null())),
            "range" => {
                let start = arg(0).as_i64().unwrap_or(0);
                let end = arg(1).as_i64().unwrap_or(0);
                let step = args.get(2).and_then(Value::as_i64).unwrap_or(1);
                if step == 0 {
                    return Err(Error::Query("cypher range step must not be zero".into()));
                }
                let length = if (step > 0 && start <= end) || (step < 0 && start >= end) {
                    ((i128::from(end) - i128::from(start)) / i128::from(step)).unsigned_abs() + 1
                } else {
                    0
                };
                let memory_limit = self.options.max_memory_bytes / 16;
                if length > memory_limit as u128 {
                    return Err(Error::Query(format!(
                        "cypher memory budget exceeded: range produces {length} elements"
                    )));
                }
                if length > self.options.max_operations as u128 {
                    return Err(Error::Query(format!(
                        "cypher operation budget exceeded: range produces {length} elements"
                    )));
                }
                self.charge_memory((length as usize).saturating_mul(16))?;
                let mut out = Vec::with_capacity(length as usize);
                let mut current = start;
                while (step > 0 && current <= end) || (step < 0 && current >= end) {
                    out.push(json!(current));
                    current = current
                        .checked_add(step)
                        .ok_or_else(|| Error::Query("cypher range integer overflow".into()))?;
                    if out.len() == length as usize {
                        break;
                    }
                }
                Ok(Value::Array(out))
            }
            "reverse" => Ok(match arg(0) {
                Value::Array(items) => Value::Array(items.into_iter().rev().collect()),
                Value::String(value) => Value::String(value.chars().rev().collect()),
                _ => Value::Null,
            }),
            "split" => Ok(Value::Array(
                arg(0)
                    .as_str()
                    .map(|value| {
                        value
                            .split(arg(1).as_str().unwrap_or(""))
                            .map(|item| json!(item))
                            .collect()
                    })
                    .unwrap_or_default(),
            )),
            "substring" => {
                let value = arg(0).as_str().unwrap_or("").to_string();
                let start = arg(1).as_u64().unwrap_or(0) as usize;
                let length = args
                    .get(2)
                    .and_then(Value::as_u64)
                    .map(|value| value as usize);
                let chars: Vec<char> = value.chars().collect();
                let end = length.map_or(chars.len(), |length| (start + length).min(chars.len()));
                Ok(json!(chars[start.min(chars.len())..end]
                    .iter()
                    .collect::<String>()))
            }
            "replace" => Ok(json!(arg(0)
                .as_str()
                .unwrap_or("")
                .replace(arg(1).as_str().unwrap_or(""), arg(2).as_str().unwrap_or("")))),
            "tolower" => Ok(json!(arg(0).as_str().unwrap_or("").to_lowercase())),
            "toupper" => Ok(json!(arg(0).as_str().unwrap_or("").to_uppercase())),
            "trim" => Ok(json!(arg(0).as_str().unwrap_or("").trim())),
            "ltrim" => Ok(json!(arg(0).as_str().unwrap_or("").trim_start())),
            "rtrim" => Ok(json!(arg(0).as_str().unwrap_or("").trim_end())),
            "left" => Ok(json!(arg(0)
                .as_str()
                .unwrap_or("")
                .chars()
                .take(arg(1).as_u64().unwrap_or(0) as usize)
                .collect::<String>())),
            "right" => Ok(json!(arg(0)
                .as_str()
                .unwrap_or("")
                .chars()
                .rev()
                .take(arg(1).as_u64().unwrap_or(0) as usize)
                .collect::<String>()
                .chars()
                .rev()
                .collect::<String>())),
            "abs" => Ok(number(&arg(0))
                .map(|value| json!(value.abs()))
                .unwrap_or(Value::Null)),
            "ceil" => Ok(number(&arg(0))
                .map(|value| json!(value.ceil()))
                .unwrap_or(Value::Null)),
            "floor" => Ok(number(&arg(0))
                .map(|value| json!(value.floor()))
                .unwrap_or(Value::Null)),
            "round" => Ok(number(&arg(0))
                .map(|value| json!(value.round()))
                .unwrap_or(Value::Null)),
            "sign" => Ok(number(&arg(0))
                .map(|value| json!(value.signum()))
                .unwrap_or(Value::Null)),
            "sqrt" => Ok(number(&arg(0))
                .map(|value| json!(value.sqrt()))
                .unwrap_or(Value::Null)),
            "exp" => Ok(number(&arg(0))
                .map(|value| json!(value.exp()))
                .unwrap_or(Value::Null)),
            "log" => Ok(number(&arg(0))
                .map(|value| json!(value.ln()))
                .unwrap_or(Value::Null)),
            "log10" => Ok(number(&arg(0))
                .map(|value| json!(value.log10()))
                .unwrap_or(Value::Null)),
            "sin" => Ok(number(&arg(0))
                .map(|value| json!(value.sin()))
                .unwrap_or(Value::Null)),
            "cos" => Ok(number(&arg(0))
                .map(|value| json!(value.cos()))
                .unwrap_or(Value::Null)),
            "tan" => Ok(number(&arg(0))
                .map(|value| json!(value.tan()))
                .unwrap_or(Value::Null)),
            "asin" => Ok(number(&arg(0))
                .map(|value| json!(value.asin()))
                .unwrap_or(Value::Null)),
            "acos" => Ok(number(&arg(0))
                .map(|value| json!(value.acos()))
                .unwrap_or(Value::Null)),
            "atan" => Ok(number(&arg(0))
                .map(|value| json!(value.atan()))
                .unwrap_or(Value::Null)),
            "atan2" => Ok(number(&arg(0))
                .zip(number(&arg(1)))
                .map(|(a, b)| json!(a.atan2(b)))
                .unwrap_or(Value::Null)),
            "degrees" => Ok(number(&arg(0))
                .map(|value| json!(value.to_degrees()))
                .unwrap_or(Value::Null)),
            "radians" => Ok(number(&arg(0))
                .map(|value| json!(value.to_radians()))
                .unwrap_or(Value::Null)),
            "pi" => Ok(json!(std::f64::consts::PI)),
            "e" => Ok(json!(std::f64::consts::E)),
            "date" | "localtime" | "time" | "localdatetime" | "datetime" => {
                temporal_value(&name, &args)
            }
            "duration.inseconds" => duration_seconds(&args),
            _ => Err(Error::Query(format!(
                "unsupported Cypher function `{name}`"
            ))),
        }
    }
}

fn join_rows(left: &Row, right: &Row) -> Option<Row> {
    for (key, left_value) in &left.values {
        if key.starts_with("__") {
            continue;
        }
        if let Some(right_value) = right.get(key) {
            if !json_eq(left_value, right_value) {
                return None;
            }
        }
    }
    let mut out = left.clone();
    out.merge(right);
    Some(out)
}

fn set_yield(row: &mut Row, yields: &[YieldItem], field: &str, value: Value) {
    if let Some(item) = yields
        .iter()
        .find(|item| item.field.eq_ignore_ascii_case(field))
    {
        row.set(item.binding().to_string(), value);
    } else if yields.is_empty() {
        row.set(field, value);
    }
}

fn source_id(row: &Row, src: Option<&str>) -> Option<String> {
    if let Some(src) = src {
        return row
            .get(src)
            .and_then(|value| value.get("_id"))
            .and_then(Value::as_str)
            .map(str::to_string);
    }
    row.get(HIDDEN_ANON_ID)
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn append_path(row: &mut Row, node: &Value, rel: &Value) {
    let mut nodes = row
        .get(HIDDEN_PATH_NODES)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut rels = row
        .get(HIDDEN_PATH_RELS)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    nodes.push(node.clone());
    rels.push(rel.clone());
    row.set(HIDDEN_PATH_NODES, Value::Array(nodes));
    row.set(HIDDEN_PATH_RELS, Value::Array(rels));
}

fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall {
            name, arguments, ..
        } => {
            is_aggregate(name)
                || match arguments {
                    FunctionArguments::Expressions(exprs) => exprs.iter().any(contains_aggregate),
                    FunctionArguments::Wildcard => false,
                }
        }
        Expr::List(items) => items.iter().any(contains_aggregate),
        Expr::Map(items) => items.iter().any(|(_, value)| contains_aggregate(value)),
        Expr::Property { base, .. } => contains_aggregate(base),
        Expr::LabelPredicate { expression, .. } => contains_aggregate(expression),
        Expr::Binary { lhs, rhs, .. } => contains_aggregate(lhs) || contains_aggregate(rhs),
        Expr::ComparisonChain { arguments, .. } => arguments.iter().any(contains_aggregate),
        Expr::Unary { operand, .. } => contains_aggregate(operand),
        Expr::ListComprehension {
            source,
            predicate,
            projection,
            ..
        } => {
            contains_aggregate(source)
                || predicate
                    .as_ref()
                    .is_some_and(|expr| contains_aggregate(expr))
                || projection
                    .as_ref()
                    .is_some_and(|expr| contains_aggregate(expr))
        }
        Expr::CollectionPredicate {
            source, predicate, ..
        } => {
            contains_aggregate(source)
                || predicate
                    .as_ref()
                    .is_some_and(|expr| contains_aggregate(expr))
        }
        Expr::Case {
            operand,
            alternatives,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| contains_aggregate(expr))
                || alternatives.iter().any(|CaseAlternative { when, then }| {
                    contains_aggregate(when) || contains_aggregate(then)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| contains_aggregate(expr))
        }
        Expr::Subscript { base, index } => contains_aggregate(base) || contains_aggregate(index),
        Expr::Slice { base, start, end } => {
            contains_aggregate(base)
                || start.as_ref().is_some_and(|expr| contains_aggregate(expr))
                || end.as_ref().is_some_and(|expr| contains_aggregate(expr))
        }
        Expr::MapProjection { base, items } => {
            contains_aggregate(base)
                || items.iter().any(|item| match item {
                    MapProjectionItem::Literal { value, .. } => contains_aggregate(value),
                    _ => false,
                })
        }
        Expr::Filter {
            source, predicate, ..
        }
        | Expr::Extract {
            source,
            projection: predicate,
            ..
        } => {
            contains_aggregate(source)
                || predicate
                    .as_ref()
                    .is_some_and(|expr| contains_aggregate(expr))
        }
        Expr::Reduce {
            initial,
            source,
            expression,
            ..
        } => {
            contains_aggregate(initial)
                || contains_aggregate(source)
                || expression
                    .as_ref()
                    .is_some_and(|expr| contains_aggregate(expr))
        }
        Expr::PatternComprehension { projection, .. } => contains_aggregate(projection),
        Expr::Literal(_) | Expr::Variable(_) | Expr::Param(_) | Expr::PatternExpression { .. } => {
            false
        }
    }
}

fn is_aggregate(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count" | "sum" | "avg" | "min" | "max" | "collect"
    )
}

fn collect_projection_aliases(plan: &Plan, out: &mut HashMap<String, String>) {
    if let Plan::Project { input, exprs, .. } = plan {
        for expr in exprs {
            if contains_aggregate(&expr.expr) {
                if let Some(alias) = &expr.alias {
                    out.insert(expr_name(&expr.expr), alias.clone());
                }
            }
        }
        collect_projection_aliases(input, out);
    }
}

fn expr_name(expr: &Expr) -> String {
    expr_name_prec(expr, 0)
}

fn expr_prec(expr: &Expr) -> u8 {
    match expr {
        Expr::Binary { op, .. } => match op {
            BinOp::Pow => 70,
            BinOp::Mul | BinOp::Div | BinOp::Mod => 60,
            BinOp::Add | BinOp::Sub => 50,
            BinOp::In
            | BinOp::StartsWith
            | BinOp::EndsWith
            | BinOp::Contains
            | BinOp::RegexMatch => 40,
            BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => 40,
            BinOp::And => 30,
            BinOp::Xor => 25,
            BinOp::Or => 20,
        },
        Expr::Unary { .. } => 75,
        Expr::Property { .. } | Expr::Subscript { .. } | Expr::Slice { .. } => 90,
        _ => 100,
    }
}

fn expr_name_prec(expr: &Expr, parent: u8) -> String {
    let text = match expr {
        Expr::Variable(name) => name.clone(),
        Expr::Param(name) => format!("${name}"),
        Expr::Property { base, key } => {
            let wrap = matches!(
                base.as_ref(),
                Expr::Subscript { .. } | Expr::Slice { .. } | Expr::Binary { .. }
            );
            let base = expr_name_prec(base, 90);
            if wrap {
                format!("({base}).{key}")
            } else {
                format!("{base}.{key}")
            }
        }
        Expr::Literal(Literal::Int(value)) => value.to_string(),
        Expr::Literal(Literal::Float(value)) => value.to_string(),
        Expr::Literal(Literal::String(value)) => format!("'{value}'"),
        Expr::Literal(Literal::Bool(value)) => value.to_string(),
        Expr::Literal(Literal::Null) => "null".into(),
        Expr::List(items) => format!(
            "[{}]",
            items.iter().map(expr_name).collect::<Vec<_>>().join(", ")
        ),
        Expr::Map(items) => format!(
            "{{{}}}",
            items
                .iter()
                .map(|(key, value)| format!("{key}: {}", expr_name(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::FunctionCall {
            name,
            distinct,
            arguments,
        } => {
            let args = match arguments {
                FunctionArguments::Wildcard => "*".to_string(),
                FunctionArguments::Expressions(items) => {
                    items.iter().map(expr_name).collect::<Vec<_>>().join(", ")
                }
            };
            if *distinct {
                format!("{name}(DISTINCT {args})")
            } else {
                format!("{name}({args})")
            }
        }
        Expr::Binary { op, lhs, rhs } => {
            let prec = expr_prec(expr);
            let left = expr_name_prec(lhs, prec);
            let right = expr_name_prec(rhs, prec.saturating_add(1));
            format!("{left} {} {right}", bin_op_name(*op))
        }
        Expr::Unary { op, operand } => match op {
            UnOp::Neg => format!("-{}", expr_name_prec(operand, 75)),
            UnOp::Pos => format!("+{}", expr_name_prec(operand, 75)),
            UnOp::Not => format!("NOT {}", expr_name_prec(operand, 75)),
            UnOp::IsNull => format!("{} IS NULL", expr_name_prec(operand, 40)),
            UnOp::IsNotNull => format!("{} IS NOT NULL", expr_name_prec(operand, 40)),
        },
        Expr::LabelPredicate { expression, labels } => {
            format!("({}:{})", expr_name_prec(expression, 0), labels.join(":"))
        }
        Expr::ComparisonChain { arguments, .. } => arguments
            .iter()
            .map(expr_name)
            .collect::<Vec<_>>()
            .join(" "),
        Expr::Subscript { base, index } => {
            format!("{}[{}]", expr_name_prec(base, 90), expr_name(index))
        }
        Expr::Slice { base, start, end } => format!(
            "{}[{}..{}]",
            expr_name_prec(base, 90),
            start
                .as_ref()
                .map(|expr| expr_name(expr))
                .unwrap_or_default(),
            end.as_ref().map(|expr| expr_name(expr)).unwrap_or_default()
        ),
        _ => format!("{expr:?}"),
    };
    if expr_prec(expr) < parent {
        format!("({text})")
    } else {
        text
    }
}

fn bin_op_name(op: BinOp) -> &'static str {
    match op {
        BinOp::Eq => "=",
        BinOp::Neq => "<>",
        BinOp::Lt => "<",
        BinOp::Lte => "<=",
        BinOp::Gt => ">",
        BinOp::Gte => ">=",
        BinOp::And => "AND",
        BinOp::Xor => "XOR",
        BinOp::Or => "OR",
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
        BinOp::Pow => "^",
        BinOp::RegexMatch => "=~",
        BinOp::In => "IN",
        BinOp::StartsWith => "STARTS WITH",
        BinOp::EndsWith => "ENDS WITH",
        BinOp::Contains => "CONTAINS",
    }
}

fn plan_vars(plan: &Plan) -> Vec<String> {
    let mut out = Vec::new();
    collect_plan_vars(plan, &mut out);
    out.sort();
    out.dedup();
    out
}

fn collect_plan_vars(plan: &Plan, out: &mut Vec<String>) {
    match plan {
        Plan::Scan { var: Some(var), .. } => {
            out.push(var.clone());
        }
        Plan::Scan { var: None, .. } => {}
        Plan::Expand {
            input,
            rel_var,
            dst,
            ..
        } => {
            collect_plan_vars(input, out);
            if let Some(var) = rel_var {
                out.push(var.clone());
            }
            if let Some(var) = dst {
                out.push(var.clone());
            }
        }
        Plan::Filter { input, .. }
        | Plan::PropertyMapFilter { input, .. }
        | Plan::Sort { input, .. }
        | Plan::Skip { input, .. }
        | Plan::Limit { input, .. }
        | Plan::ShortestPath { input, .. }
        | Plan::PlannerHint { input, .. }
        | Plan::Distinct { input } => collect_plan_vars(input, out),
        Plan::Project { input, exprs, .. } => {
            collect_plan_vars(input, out);
            for expr in exprs {
                if let Some(alias) = &expr.alias {
                    out.push(alias.clone());
                } else if let Expr::Variable(name) = &expr.expr {
                    out.push(name.clone());
                }
            }
        }
        Plan::NamedPath { input, variable } => {
            collect_plan_vars(input, out);
            out.push(variable.clone());
        }
        Plan::Unwind {
            input,
            expression: _,
            alias,
        } => {
            collect_plan_vars(input, out);
            out.push(alias.clone());
        }
        Plan::Optional { input, optional } => {
            collect_plan_vars(input, out);
            collect_plan_vars(optional, out);
        }
        Plan::ProcedureCall { input, yields, .. } => {
            collect_plan_vars(input, out);
            for item in yields {
                out.push(item.binding().to_string());
            }
        }
        Plan::Cartesian { left, right } | Plan::Union { left, right, .. } => {
            collect_plan_vars(left, out);
            collect_plan_vars(right, out);
        }
        _ => {}
    }
}

fn number(value: &Value) -> Option<f64> {
    value.as_f64()
}

fn truthy(value: &Value) -> bool {
    value.as_bool().unwrap_or(false)
}

fn eq3(a: &Value, b: &Value) -> Value {
    if a.is_null() || b.is_null() {
        return Value::Null;
    }
    if let (Some(left), Some(right)) = (
        a.get("__type").and_then(Value::as_str),
        b.get("__type").and_then(Value::as_str),
    ) {
        if left == right
            && matches!(
                left,
                "date" | "localtime" | "time" | "localdatetime" | "datetime"
            )
        {
            return json!(format_temporal(a) == format_temporal(b));
        }
    }
    match (a, b) {
        (Value::Array(a), Value::Array(b)) => {
            if a.len() != b.len() {
                return json!(false);
            }
            let mut saw_null = false;
            for (left, right) in a.iter().zip(b.iter()) {
                match eq3(left, right) {
                    Value::Bool(true) => {}
                    Value::Bool(false) => return json!(false),
                    _ => saw_null = true,
                }
            }
            if saw_null {
                Value::Null
            } else {
                json!(true)
            }
        }
        (Value::Object(a), Value::Object(b)) => {
            if a.len() != b.len() {
                return json!(false);
            }
            let mut saw_null = false;
            for (key, left) in a {
                match b.get(key) {
                    None => return json!(false),
                    Some(right) => match eq3(left, right) {
                        Value::Bool(true) => {}
                        Value::Bool(false) => return json!(false),
                        _ => saw_null = true,
                    },
                }
            }
            if saw_null {
                Value::Null
            } else {
                json!(true)
            }
        }
        _ => json!(json_eq(a, b)),
    }
}

fn json_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(a), Value::Number(b)) => {
            if let (Some(a), Some(b)) = (a.as_i64(), b.as_i64()) {
                a == b
            } else if let (Some(a), Some(b)) = (a.as_u64(), b.as_u64()) {
                a == b
            } else {
                a.as_f64() == b.as_f64()
            }
        }
        _ => a == b,
    }
}

fn cmp_json(a: &Value, b: &Value) -> Ordering {
    fn rank(value: &Value) -> u8 {
        match value {
            Value::Object(map) => {
                if map.contains_key("_labels") {
                    1
                } else if map.contains_key("_type") {
                    2
                } else if map.contains_key("nodes") && map.contains_key("relationships") {
                    4
                } else {
                    match map.get("__type").and_then(Value::as_str) {
                        Some("node") => 1,
                        Some("rel") => 2,
                        Some("path") => 4,
                        _ => 0,
                    }
                }
            }
            Value::Array(_) => 3,
            Value::String(_) => 5,
            Value::Bool(_) => 6,
            Value::Number(number) if number.as_f64().is_some_and(f64::is_nan) => 8,
            Value::Number(_) => 7,
            Value::Null => 9,
        }
    }
    if let (Some(a_type), Some(b_type)) = (
        a.get("__type").and_then(Value::as_str),
        b.get("__type").and_then(Value::as_str),
    ) {
        if a_type == b_type
            && matches!(
                a_type,
                "date" | "localtime" | "time" | "localdatetime" | "datetime"
            )
        {
            return temporal_key(a).cmp(&temporal_key(b));
        }
    }
    let (a_rank, b_rank) = (rank(a), rank(b));
    if a_rank != b_rank {
        return a_rank.cmp(&b_rank);
    }
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Number(a), Value::Number(b)) => a
            .as_f64()
            .partial_cmp(&b.as_f64())
            .unwrap_or(Ordering::Equal),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Array(a), Value::Array(b)) => {
            for (left, right) in a.iter().zip(b.iter()) {
                let order = cmp_json(left, right);
                if order != Ordering::Equal {
                    return order;
                }
            }
            a.len().cmp(&b.len())
        }
        _ => a.to_string().cmp(&b.to_string()),
    }
}

fn temporal_property(value: &Value, key: &str) -> Option<Value> {
    let kind = value.get("__type").and_then(Value::as_str)?;
    if matches!(
        key,
        "year" | "month" | "day" | "hour" | "minute" | "second" | "nanosecond"
    ) {
        return value.get(key).cloned();
    }
    let nanosecond = value.get("nanosecond").and_then(Value::as_i64).unwrap_or(0);
    let offset = offset_seconds(value);
    let offset_text = offset.map(|offset| {
        let sign = if offset < 0 { '-' } else { '+' };
        let offset = offset.abs();
        format!("{sign}{:02}:{:02}", offset / 3600, offset % 3600 / 60)
    });
    match key {
        "offset" | "timezone" => Some(offset_text.map_or(Value::Null, Value::String)),
        "offsetMinutes" => Some(offset.map_or(Value::Null, |value| json!(value / 60))),
        "offsetSeconds" => Some(offset.map_or(Value::Null, |value| json!(value))),
        "millisecond" => Some(json!(nanosecond / 1_000_000)),
        "microsecond" => Some(json!(nanosecond / 1_000)),
        "quarter" => {
            let month = value.get("month").and_then(Value::as_i64).unwrap_or(1);
            Some(json!((month - 1) / 3 + 1))
        }
        "week" | "weekYear" | "weekDay" | "ordinalDay" | "dayOfQuarter"
            if matches!(kind, "date" | "datetime" | "localdatetime") =>
        {
            let year = value.get("year").and_then(Value::as_i64).unwrap_or(1970) as i32;
            let month = value.get("month").and_then(Value::as_i64).unwrap_or(1) as u32;
            let day = value.get("day").and_then(Value::as_i64).unwrap_or(1) as u32;
            let date = chrono::NaiveDate::from_ymd_opt(year, month, day)?;
            Some(match key {
                "week" => json!(date.iso_week().week()),
                "weekYear" => json!(date.iso_week().year()),
                "weekDay" => json!(date.weekday().number_from_monday()),
                "ordinalDay" => json!(date.ordinal()),
                _ => {
                    let quarter_start_month = ((month - 1) / 3) * 3 + 1;
                    let start = chrono::NaiveDate::from_ymd_opt(year, quarter_start_month, 1)?;
                    json!((date - start).num_days() + 1)
                }
            })
        }
        _ => None,
    }
}

fn offset_seconds(value: &Value) -> Option<i64> {
    if let Some(offset) = value.get("offset_seconds").and_then(Value::as_i64) {
        return Some(offset);
    }
    let text = value
        .get("offset")
        .and_then(Value::as_str)
        .or_else(|| value.get("timezone").and_then(Value::as_str))?;
    if !text.starts_with('+') && !text.starts_with('-') {
        return None;
    }
    let (hours, minutes) = text[1..].split_once(':')?;
    let sign = if text.starts_with('-') { -1 } else { 1 };
    Some(sign * (hours.parse::<i64>().ok()? * 3600 + minutes.parse::<i64>().ok()? * 60))
}

fn temporal_key(value: &Value) -> (i64, i128) {
    let kind = value.get("__type").and_then(Value::as_str).unwrap_or("");
    let year = value.get("year").and_then(Value::as_i64).unwrap_or(0);
    let month = value.get("month").and_then(Value::as_i64).unwrap_or(1);
    let day = value.get("day").and_then(Value::as_i64).unwrap_or(1);
    let hour = value.get("hour").and_then(Value::as_i64).unwrap_or(0) as i128;
    let minute = value.get("minute").and_then(Value::as_i64).unwrap_or(0) as i128;
    let second = value.get("second").and_then(Value::as_i64).unwrap_or(0) as i128;
    let nano = value.get("nanosecond").and_then(Value::as_i64).unwrap_or(0) as i128;
    let offset = offset_seconds(value).unwrap_or(0) as i128;
    let time_nanos =
        ((hour * 3600 + minute * 60 + second) * 1_000_000_000 + nano) - offset * 1_000_000_000;
    match kind {
        "date" => (
            0,
            days_from_civil(year, month, day) as i128 * 86_400_000_000_000,
        ),
        "localtime" => (0, time_nanos),
        "time" => (0, time_nanos.rem_euclid(86_400_000_000_000)),
        _ => (
            0,
            days_from_civil(year, month, day) as i128 * 86_400_000_000_000 + time_nanos,
        ),
    }
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * shifted_month + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn eval_binary(op: BinOp, lhs: Value, rhs: Value) -> Result<Value> {
    if matches!(op, BinOp::And | BinOp::Or | BinOp::Xor) {
        let lhs = lhs.as_bool();
        let rhs = rhs.as_bool();
        return Ok(match op {
            BinOp::And => match (lhs, rhs) {
                (Some(false), _) | (_, Some(false)) => json!(false),
                (Some(true), Some(true)) => json!(true),
                _ => Value::Null,
            },
            BinOp::Or => match (lhs, rhs) {
                (Some(true), _) | (_, Some(true)) => json!(true),
                (Some(false), Some(false)) => json!(false),
                _ => Value::Null,
            },
            BinOp::Xor => match (lhs, rhs) {
                (Some(a), Some(b)) => json!(a ^ b),
                _ => Value::Null,
            },
            _ => unreachable!(),
        });
    }
    if matches!(op, BinOp::Eq | BinOp::Neq) {
        let equal = eq3(&lhs, &rhs);
        if matches!(op, BinOp::Neq) {
            return Ok(match equal {
                Value::Bool(value) => json!(!value),
                Value::Null => Value::Null,
                other => other,
            });
        }
        return Ok(equal);
    }
    if matches!(op, BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte) {
        if lhs.is_null() || rhs.is_null() {
            return Ok(Value::Null);
        }
        let comparable = |value: &Value| match value {
            Value::Number(_) => 0,
            Value::String(_) => 1,
            Value::Bool(_) => 2,
            Value::Array(_) => 3,
            Value::Object(_) => 4,
            Value::Null => 5,
        };
        if comparable(&lhs) != comparable(&rhs) {
            return Ok(Value::Null);
        }
        let order = cmp_json(&lhs, &rhs);
        return Ok(json!(match op {
            BinOp::Lt => order == Ordering::Less,
            BinOp::Lte => order != Ordering::Greater,
            BinOp::Gt => order == Ordering::Greater,
            BinOp::Gte => order != Ordering::Less,
            _ => unreachable!(),
        }));
    }
    if matches!(op, BinOp::In) {
        let Value::Array(items) = rhs else {
            return Ok(Value::Null);
        };
        let mut saw_null = false;
        for item in items {
            match eq3(&lhs, &item) {
                Value::Bool(true) => return Ok(json!(true)),
                Value::Null => saw_null = true,
                _ => {}
            }
        }
        return Ok(if saw_null { Value::Null } else { json!(false) });
    }
    if matches!(
        op,
        BinOp::StartsWith | BinOp::EndsWith | BinOp::Contains | BinOp::RegexMatch
    ) {
        return Ok(match (lhs.as_str(), rhs.as_str()) {
            (Some(lhs), Some(rhs)) => json!(match op {
                BinOp::StartsWith => lhs.starts_with(rhs),
                BinOp::EndsWith => lhs.ends_with(rhs),
                BinOp::Contains => lhs.contains(rhs),
                BinOp::RegexMatch => regex::Regex::new(rhs).is_ok_and(|re| re.is_match(lhs)),
                _ => false,
            }),
            _ => Value::Null,
        });
    }
    if lhs.is_null() || rhs.is_null() {
        return Ok(Value::Null);
    }
    if matches!(op, BinOp::Add) {
        if let (Some(lhs), Some(rhs)) = (lhs.as_str(), rhs.as_str()) {
            return Ok(json!(format!("{lhs}{rhs}")));
        }
        if let Some(lhs) = lhs.as_array() {
            if let Some(rhs) = rhs.as_array() {
                let mut out = lhs.clone();
                out.extend(rhs.clone());
                return Ok(Value::Array(out));
            }
            let mut out = lhs.clone();
            out.push(rhs);
            return Ok(Value::Array(out));
        }
        if let Some(rhs) = rhs.as_array() {
            let mut out = Vec::with_capacity(rhs.len() + 1);
            out.push(lhs);
            out.extend(rhs.clone());
            return Ok(Value::Array(out));
        }
    }
    let Some(lhs) = number(&lhs) else {
        return Ok(Value::Null);
    };
    let Some(rhs) = number(&rhs) else {
        return Ok(Value::Null);
    };
    Ok(json!(match op {
        BinOp::Add => lhs + rhs,
        BinOp::Sub => lhs - rhs,
        BinOp::Mul => lhs * rhs,
        BinOp::Div => lhs / rhs,
        BinOp::Mod => lhs % rhs,
        BinOp::Pow => lhs.powf(rhs),
        _ => return Ok(Value::Null),
    }))
}

fn eval_unary(op: UnOp, value: Value) -> Result<Value> {
    Ok(match op {
        UnOp::Not => match value.as_bool() {
            Some(value) => json!(!value),
            None => Value::Null,
        },
        UnOp::Pos => value,
        UnOp::Neg => match &value {
            Value::Number(number) if number.as_i64().is_some() => {
                json!(-number.as_i64().unwrap_or(0))
            }
            _ => number(&value)
                .map(|value| json!(-value))
                .unwrap_or(Value::Null),
        },
        UnOp::IsNull => json!(value.is_null()),
        UnOp::IsNotNull => json!(!value.is_null()),
    })
}

fn temporal_int(value: Option<&Value>, default: i64) -> i64 {
    value.and_then(Value::as_i64).unwrap_or(default)
}

fn temporal_value(kind: &str, args: &[Value]) -> Result<Value> {
    let input = args.first().cloned().unwrap_or(Value::Null);
    let mut out = Map::new();
    out.insert("__type".into(), Value::String(kind.to_string()));
    if let Some(map) = input.as_object() {
        for (key, value) in map {
            out.insert(key.clone(), value.clone());
        }
    } else if let Some(text) = input.as_str() {
        parse_temporal_text(kind, text, &mut out)?;
    } else {
        return Ok(Value::Null);
    }
    if kind == "date" {
        out.entry("month").or_insert(json!(1));
        out.entry("day").or_insert(json!(1));
    } else {
        for (key, value) in [("hour", 0), ("minute", 0), ("second", 0), ("nanosecond", 0)] {
            out.entry(key).or_insert(json!(value));
        }
    }
    if matches!(kind, "time" | "datetime") {
        if let Some(offset) = out.get("timezone").and_then(Value::as_str) {
            if offset.starts_with('+') || offset.starts_with('-') {
                out.insert("offset".into(), Value::String(offset.to_string()));
            }
        }
    }
    if kind == "datetime" {
        if let Some(zone) = out
            .get("timezone")
            .and_then(Value::as_str)
            .map(str::to_string)
        {
            if !zone.starts_with('+') && !zone.starts_with('-') {
                if let Ok(tz) = zone.parse::<chrono_tz::Tz>() {
                    if let Some(offset) = named_offset(&out, tz) {
                        out.insert("offset".into(), Value::String(offset));
                    }
                }
            }
        }
    }
    Ok(Value::Object(out))
}

fn parse_temporal_text(kind: &str, text: &str, out: &mut Map<String, Value>) -> Result<()> {
    let (date_part, time_part) = if let Some((date, time)) = text.split_once('T') {
        (Some(date), Some(time))
    } else if kind == "date" {
        (Some(text), None)
    } else {
        (None, Some(text))
    };
    if let Some(date) = date_part {
        let negative = date.starts_with('-');
        let mut parts = date.trim_start_matches('-').split('-');
        let year: i64 = parts
            .next()
            .unwrap_or("1")
            .parse()
            .map_err(|_| Error::Query("invalid date".into()))?;
        let year = if negative { -year } else { year };
        let month: i64 = parts
            .next()
            .unwrap_or("1")
            .parse()
            .map_err(|_| Error::Query("invalid date".into()))?;
        let day: i64 = parts
            .next()
            .unwrap_or("1")
            .parse()
            .map_err(|_| Error::Query("invalid date".into()))?;
        out.insert("year".into(), json!(year));
        out.insert("month".into(), json!(month));
        out.insert("day".into(), json!(day));
    }
    if let Some(time) = time_part {
        let (clock, offset) = split_offset(time);
        let mut parts = clock.split(':');
        let hour: i64 = parts.next().unwrap_or("0").parse().unwrap_or(0);
        let minute: i64 = parts.next().unwrap_or("0").parse().unwrap_or(0);
        let seconds = parts.next().unwrap_or("0");
        let (second, nanosecond) = if let Some((sec, nanos)) = seconds.split_once('.') {
            let sec = sec.parse().unwrap_or(0);
            let mut nanos = nanos.to_string();
            while nanos.len() < 9 {
                nanos.push('0');
            }
            (sec, nanos[..9].parse().unwrap_or(0))
        } else {
            (seconds.parse().unwrap_or(0), 0)
        };
        out.insert("hour".into(), json!(hour));
        out.insert("minute".into(), json!(minute));
        out.insert("second".into(), json!(second));
        out.insert("nanosecond".into(), json!(nanosecond));
        if let Some((offset, zone)) = offset {
            if !offset.is_empty() {
                out.insert("offset".into(), Value::String(offset.clone()));
                out.insert("timezone".into(), Value::String(offset));
            }
            if let Some(zone) = zone {
                out.insert("timezone".into(), Value::String(zone));
            }
        }
    }
    Ok(())
}

fn split_offset(text: &str) -> (&str, Option<(String, Option<String>)>) {
    if let Some((clock, zone)) = text.split_once('[') {
        let zone = zone.trim_end_matches(']');
        return (clock, Some((String::new(), Some(zone.to_string()))));
    }
    for index in (1..text.len()).rev() {
        let ch = text.as_bytes()[index] as char;
        if ch == '+' || ch == '-' {
            let clock = &text[..index];
            let mut offset = text[index..].to_string();
            if offset.len() == 5 && !offset.contains(':') {
                offset.insert(3, ':');
            }
            return (clock, Some((offset, None)));
        }
    }
    (text, None)
}

fn named_offset(values: &Map<String, Value>, tz: chrono_tz::Tz) -> Option<String> {
    use chrono::TimeZone;
    let year = temporal_int(values.get("year"), 1970) as i32;
    let month = temporal_int(values.get("month"), 1) as u32;
    let day = temporal_int(values.get("day"), 1) as u32;
    let hour = temporal_int(values.get("hour"), 0) as u32;
    let minute = temporal_int(values.get("minute"), 0) as u32;
    let second = temporal_int(values.get("second"), 0) as u32;
    let date = chrono::NaiveDate::from_ymd_opt(year, month, day)?;
    let time = chrono::NaiveTime::from_hms_opt(hour, minute, second)?;
    let local = date.and_time(time);
    tz.from_local_datetime(&local)
        .single()
        .map(|value| value.format("%:z").to_string())
}

fn format_temporal(value: &Value) -> String {
    let kind = value.get("__type").and_then(Value::as_str).unwrap_or("");
    let year = temporal_int(value.get("year"), 1970);
    let month = temporal_int(value.get("month"), 1);
    let day = temporal_int(value.get("day"), 1);
    let hour = temporal_int(value.get("hour"), 0);
    let minute = temporal_int(value.get("minute"), 0);
    let second = temporal_int(value.get("second"), 0);
    let nanosecond = temporal_int(value.get("nanosecond"), 0);
    let fraction = if nanosecond == 0 {
        String::new()
    } else {
        format!(".{nanosecond:09}")
    };
    let clock = format!("{hour:02}:{minute:02}:{second:02}{fraction}");
    let date = format!("{year:04}-{month:02}-{day:02}");
    let offset = value
        .get("offset")
        .and_then(Value::as_str)
        .or_else(|| value.get("timezone").and_then(Value::as_str))
        .unwrap_or("");
    let zone = value
        .get("timezone")
        .and_then(Value::as_str)
        .filter(|zone| !zone.starts_with('+') && !zone.starts_with('-'))
        .map(|zone| format!("[{zone}]"))
        .unwrap_or_default();
    match kind {
        "date" => date,
        "localtime" => clock,
        "time" => format!("{clock}{offset}"),
        "localdatetime" => format!("{date}T{clock}"),
        "datetime" => format!("{date}T{clock}{offset}{zone}"),
        _ => value.to_string(),
    }
}

fn duration_seconds(args: &[Value]) -> Result<Value> {
    let (Some(left), Some(right)) = (args.first(), args.get(1)) else {
        return Ok(Value::Null);
    };
    for value in [left, right] {
        let year = temporal_int(value.get("year"), 0);
        if year.unsigned_abs() > 10_000 {
            return Err(Error::Query(
                "temporal durations outside the supported year range are not supported".into(),
            ));
        }
    }
    let parse = |value: &Value| -> Option<chrono::NaiveDateTime> {
        let text = format_temporal(value);
        chrono::NaiveDateTime::parse_from_str(&text, "%Y-%m-%dT%H:%M:%S%.f").ok()
    };
    let (Some(left), Some(right)) = (parse(left), parse(right)) else {
        return Ok(Value::Null);
    };
    let diff = right.signed_duration_since(left);
    let negative = diff.num_nanoseconds().is_some_and(|value| value < 0);
    let abs = diff.abs();
    let seconds = abs.num_seconds();
    let nanos = abs.subsec_nanos();
    let sign = if negative { "-" } else { "" };
    let text = if nanos == 0 {
        format!("PT{sign}{seconds}S")
    } else {
        let mut fraction = format!("{nanos:09}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        format!("PT{sign}{seconds}.{fraction}S")
    };
    Ok(Value::String(text))
}

fn validate_bindings(plan: &Plan) -> Result<()> {
    let mut bound = std::collections::HashSet::new();
    validate_plan(plan, &mut bound)
}

fn validate_plan(plan: &Plan, bound: &mut std::collections::HashSet<String>) -> Result<()> {
    match plan {
        Plan::Empty => {}
        Plan::Scan { var: Some(var), .. } => {
            bound.insert(var.clone());
        }
        Plan::Scan { var: None, .. } => {}
        Plan::Expand {
            input,
            src,
            rel_var,
            dst,
            ..
        } => {
            validate_plan(input, bound)?;
            if let Some(src) = src {
                require_bound(bound, src)?;
            }
            if let Some(rel_var) = rel_var {
                bound.insert(rel_var.clone());
            }
            if let Some(dst) = dst {
                bound.insert(dst.clone());
            }
        }
        Plan::Filter { input, pred } => {
            validate_plan(input, bound)?;
            let mut available = bound.clone();
            let mut plan_vars = Vec::new();
            collect_plan_vars(input, &mut plan_vars);
            available.extend(plan_vars);
            validate_expr(pred, &available)?;
        }
        Plan::PropertyMapFilter {
            input,
            variable,
            map,
        } => {
            validate_plan(input, bound)?;
            require_bound(bound, variable)?;
            validate_expr(map, bound)?;
        }
        Plan::Project {
            input,
            include_existing,
            exprs,
        } => {
            validate_plan(input, bound)?;
            let input_bound = bound.clone();
            for expr in exprs {
                validate_expr(&expr.expr, bound)?;
            }
            let mut next = if *include_existing {
                bound.clone()
            } else {
                std::collections::HashSet::new()
            };
            for expr in exprs {
                if let Some(alias) = &expr.alias {
                    next.insert(alias.clone());
                } else if let Expr::Variable(name) = &expr.expr {
                    next.insert(name.clone());
                }
            }
            if exprs.iter().any(|expr| contains_aggregate(&expr.expr)) {
                // ORDER BY after an aggregate may refer to rows before aggregation.
                next.extend(input_bound);
            }
            *bound = next;
        }
        Plan::Sort { input, keys } => {
            let before = bound.clone();
            validate_plan(input, bound)?;
            let mut available = bound.clone();
            available.extend(before);
            let mut plan_vars = Vec::new();
            collect_plan_vars(input, &mut plan_vars);
            available.extend(plan_vars);
            // Sorting an aggregate/projection may refer to pre-projection keys.
            for key in keys {
                validate_expr(&key.expr, &available)?;
            }
        }
        Plan::Skip { input, count } | Plan::Limit { input, count } => {
            validate_plan(input, bound)?;
            validate_expr(count, bound)?;
        }
        Plan::Cartesian { left, right } => {
            validate_plan(left, bound)?;
            validate_plan(right, bound)?;
        }
        Plan::Optional { input, optional } => {
            validate_plan(input, bound)?;
            let mut optional_bound = bound.clone();
            validate_plan(optional, &mut optional_bound)?;
            bound.extend(optional_bound);
        }
        Plan::ShortestPath { input, .. } | Plan::NamedPath { input, .. } => {
            validate_plan(input, bound)?;
            if let Plan::NamedPath { variable, .. } = plan {
                bound.insert(variable.clone());
            }
        }
        Plan::PlannerHint { input, .. } => validate_plan(input, bound)?,
        Plan::ProcedureCall {
            input,
            arguments,
            yields,
            ..
        } => {
            validate_plan(input, bound)?;
            for argument in arguments {
                validate_expr(argument, bound)?;
            }
            for item in yields {
                bound.insert(item.binding().to_string());
            }
        }
        Plan::Unwind {
            input,
            expression,
            alias,
        } => {
            validate_plan(input, bound)?;
            validate_expr(expression, bound)?;
            bound.insert(alias.clone());
        }
        Plan::Explain { input } | Plan::Profile { input } | Plan::Distinct { input } => {
            validate_plan(input, bound)?;
        }
        Plan::Union { left, right, .. } => {
            validate_plan(left, bound)?;
            validate_plan(right, bound)?;
        }
        Plan::PeriodicCommit { input, .. } => validate_plan(input, bound)?,
        _ => {}
    }
    Ok(())
}

fn require_bound(bound: &std::collections::HashSet<String>, name: &str) -> Result<()> {
    if bound.contains(name) {
        Ok(())
    } else {
        Err(Error::Query(format!("unbound variable `{name}`")))
    }
}

fn validate_expr(expr: &Expr, bound: &std::collections::HashSet<String>) -> Result<()> {
    match expr {
        Expr::Variable(name) => require_bound(bound, name),
        Expr::Literal(_) | Expr::Param(_) => Ok(()),
        Expr::List(items) => {
            for item in items {
                validate_expr(item, bound)?;
            }
            Ok(())
        }
        Expr::Map(items) => {
            for (_, item) in items {
                validate_expr(item, bound)?;
            }
            Ok(())
        }
        Expr::FunctionCall { arguments, .. } => match arguments {
            cypher_rs_zk::FunctionArguments::Expressions(items) => {
                for item in items {
                    validate_expr(item, bound)?;
                }
                Ok(())
            }
            cypher_rs_zk::FunctionArguments::Wildcard => Ok(()),
        },
        Expr::ListComprehension {
            variable,
            source,
            predicate,
            projection,
        } => {
            validate_expr(source, bound)?;
            let mut inner = bound.clone();
            inner.insert(variable.clone());
            if let Some(predicate) = predicate {
                validate_expr(predicate, &inner)?;
            }
            if let Some(projection) = projection {
                validate_expr(projection, &inner)?;
            }
            Ok(())
        }
        Expr::CollectionPredicate {
            variable,
            source,
            predicate,
            ..
        } => {
            validate_expr(source, bound)?;
            let mut inner = bound.clone();
            inner.insert(variable.clone());
            if let Some(predicate) = predicate {
                validate_expr(predicate, &inner)?;
            }
            Ok(())
        }
        // Pattern expressions/comprehensions bind their own pattern variables.
        Expr::PatternComprehension { .. } | Expr::PatternExpression { .. } => Ok(()),
        Expr::Filter {
            variable,
            source,
            predicate,
        } => {
            validate_expr(source, bound)?;
            let mut inner = bound.clone();
            inner.insert(variable.clone());
            if let Some(predicate) = predicate {
                validate_expr(predicate, &inner)?;
            }
            Ok(())
        }
        Expr::Extract {
            variable,
            source,
            projection,
        } => {
            validate_expr(source, bound)?;
            let mut inner = bound.clone();
            inner.insert(variable.clone());
            if let Some(projection) = projection {
                validate_expr(projection, &inner)?;
            }
            Ok(())
        }
        Expr::Reduce {
            accumulator,
            initial,
            variable,
            source,
            expression,
        } => {
            validate_expr(initial, bound)?;
            validate_expr(source, bound)?;
            let mut inner = bound.clone();
            inner.insert(variable.clone());
            inner.insert(accumulator.clone());
            if let Some(expression) = expression {
                validate_expr(expression, &inner)?;
            }
            Ok(())
        }
        Expr::Case {
            operand,
            alternatives,
            else_expr,
        } => {
            if let Some(operand) = operand {
                validate_expr(operand, bound)?;
            }
            for alternative in alternatives {
                validate_expr(&alternative.when, bound)?;
                validate_expr(&alternative.then, bound)?;
            }
            if let Some(else_expr) = else_expr {
                validate_expr(else_expr, bound)?;
            }
            Ok(())
        }
        Expr::Property { base, .. }
        | Expr::LabelPredicate {
            expression: base, ..
        } => validate_expr(base, bound),
        Expr::Subscript { base, index } => {
            validate_expr(base, bound)?;
            validate_expr(index, bound)
        }
        Expr::Slice { base, start, end } => {
            validate_expr(base, bound)?;
            if let Some(start) = start {
                validate_expr(start, bound)?;
            }
            if let Some(end) = end {
                validate_expr(end, bound)?;
            }
            Ok(())
        }
        Expr::MapProjection { base, items } => {
            validate_expr(base, bound)?;
            for item in items {
                if let cypher_rs_zk::MapProjectionItem::Literal { value, .. } = item {
                    validate_expr(value, bound)?;
                }
            }
            Ok(())
        }
        Expr::ComparisonChain { arguments, .. } => {
            for argument in arguments {
                validate_expr(argument, bound)?;
            }
            Ok(())
        }
        Expr::Binary { lhs, rhs, .. } => {
            validate_expr(lhs, bound)?;
            validate_expr(rhs, bound)
        }
        Expr::Unary { operand, .. } => validate_expr(operand, bound),
    }
}

fn normalize_is_null_comparisons(query: &str) -> String {
    let mut out = query.to_string();
    while let Some(marker) = out.find(" IS NULL = ") {
        let left_end = marker;
        let bytes = out.as_bytes();
        let mut cursor = left_end;
        while cursor > 0 && bytes[cursor - 1].is_ascii_whitespace() {
            cursor -= 1;
        }
        let left_start = if cursor > 0 && bytes[cursor - 1] == b')' {
            let mut depth = 0usize;
            let mut index = cursor;
            while index > 0 {
                index -= 1;
                match bytes[index] {
                    b')' => depth += 1,
                    b'(' => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => {}
                }
            }
            index
        } else {
            let mut index = cursor;
            while index > 0 {
                let byte = bytes[index - 1];
                if byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'.' {
                    index -= 1;
                } else {
                    break;
                }
            }
            index
        };
        let right_start = marker + " IS NULL = ".len();
        let Some(right_marker_rel) = out[right_start..].find(" IS NULL") else {
            break;
        };
        let right_marker = right_start + right_marker_rel;
        let left = out[left_start..left_end].trim();
        let right = out[right_start..right_marker].trim();
        if left.is_empty() || right.is_empty() {
            break;
        }
        let replacement = format!("({left} IS NULL) = ({right} IS NULL)");
        let mut next = String::with_capacity(out.len() + replacement.len());
        next.push_str(&out[..left_start]);
        next.push_str(&replacement);
        next.push_str(&out[right_marker + " IS NULL".len()..]);
        out = next;
    }
    out
}

fn normalize_reserved_labels(query: &str) -> String {
    let mut out = String::with_capacity(query.len() + 16);
    let reserved = ["single", "any", "none", "all", "exists", "end", "contains"];
    let chars: Vec<char> = query.chars().collect();
    let mut index = 0;
    let mut quote: Option<char> = None;
    while index < chars.len() {
        let ch = chars[index];
        if let Some(active) = quote {
            out.push(ch);
            if ch == '\\' && index + 1 < chars.len() {
                index += 1;
                out.push(chars[index]);
            } else if ch == active {
                quote = None;
            }
            index += 1;
            continue;
        }
        if ch == '\'' || ch == '"' || ch == '`' {
            quote = Some(ch);
            out.push(ch);
            index += 1;
            continue;
        }
        if ch == ':'
            && index + 1 < chars.len()
            && (chars[index + 1].is_alphabetic() || chars[index + 1] == '_')
        {
            let start = index + 1;
            let mut end = start;
            while end < chars.len() && (chars[end].is_alphanumeric() || chars[end] == '_') {
                end += 1;
            }
            let word: String = chars[start..end].iter().collect();
            out.push(':');
            if reserved.contains(&word.to_ascii_lowercase().as_str()) {
                out.push('`');
                out.push_str(&word);
                out.push('`');
            } else {
                out.push_str(&word);
            }
            index = end;
            continue;
        }
        out.push(ch);
        index += 1;
    }
    out
}

fn check_nesting_depth(query: &str) -> Result<()> {
    let mut depth = 0usize;
    let mut max_depth = 0usize;
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for ch in query.chars() {
        if let Some(active) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == active {
                quote = None;
            }
            continue;
        }
        if ch == '\'' || ch == '"' {
            quote = Some(ch);
        } else if matches!(ch, '(' | '[' | '{') {
            depth += 1;
            max_depth = max_depth.max(depth);
            if max_depth > 16 {
                return Err(Error::Query(
                    "cypher literal/pattern nesting exceeds 16 levels".into(),
                ));
            }
        } else if matches!(ch, ')' | ']' | '}') {
            depth = depth.saturating_sub(1);
        }
    }
    Ok(())
}

fn normalize_literals(query: &str) -> String {
    let mut out = String::with_capacity(query.len());
    let mut chars = query.chars().peekable();
    let mut quote: Option<char> = None;
    while let Some(ch) = chars.next() {
        if let Some(active) = quote {
            if ch == '\\' {
                if let Some(next) = chars.next() {
                    match next {
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'u' | 'U' => {
                            let digits_count = if next == 'u' { 4 } else { 8 };
                            let mut digits = String::new();
                            for _ in 0..digits_count {
                                if let Some(digit) = chars.next() {
                                    digits.push(digit);
                                }
                            }
                            if let Ok(code) = u32::from_str_radix(&digits, 16) {
                                if let Some(ch) = char::from_u32(code) {
                                    match ch {
                                        '\'' if active == '\'' => out.push_str("\\'"),
                                        '"' if active == '"' => out.push_str("\\\""),
                                        '\\' => out.push_str("\\\\"),
                                        other => out.push(other),
                                    }
                                }
                            }
                        }
                        other => {
                            out.push('\\');
                            out.push(other);
                        }
                    }
                }
            } else {
                out.push(ch);
                if ch == active {
                    quote = None;
                }
            }
            continue;
        }
        if ch == '\'' || ch == '"' {
            quote = Some(ch);
            out.push(ch);
            continue;
        }
        if ch == '0' && matches!(chars.peek(), Some('o' | 'O' | 'x' | 'X')) {
            let radix = chars.peek().copied().unwrap_or('o');
            chars.next();
            let mut digits = String::new();
            while let Some(next) = chars.peek().copied() {
                let valid = if radix == 'o' || radix == 'O' {
                    next.is_ascii_digit() && next < '8'
                } else {
                    next.is_ascii_hexdigit()
                };
                if !valid {
                    break;
                }
                digits.push(next);
                chars.next();
            }
            if !digits.is_empty() {
                let parsed =
                    u64::from_str_radix(&digits, if radix == 'o' || radix == 'O' { 8 } else { 16 });
                if let Ok(value) = parsed {
                    if value <= i64::MAX as u64 {
                        out.push_str(&value.to_string());
                        continue;
                    }
                    if value == i64::MIN.unsigned_abs() && out.ends_with('-') {
                        out.pop();
                        out.push_str("(-9223372036854775807 - 1)");
                        continue;
                    }
                }
            }
            out.push('0');
            out.push(radix);
            out.push_str(&digits);
            continue;
        }
        out.push(ch);
    }
    out
}

pub fn execute(view: &dyn GraphView, options: &CypherOptions) -> Result<Value> {
    options.validate()?;
    check_nesting_depth(&options.query)?;
    let expanded = expand_exists_subqueries(&prepend_optional_anchor(&options.query))?;
    let normalized = normalize_query_text(&expanded);
    let query: Query =
        parse(&normalized).map_err(|error| Error::Query(format!("cypher parse error: {error}")))?;
    if query.statement_count() != 1 {
        return Err(Error::Query(
            "cypher supports exactly one read-only statement".into(),
        ));
    }
    reject_unsupported_shapes(&query)?;
    if query.clauses.iter().any(is_write_clause) {
        return Err(Error::Query(
            "cypher write clauses are not supported in read-only mode".into(),
        ));
    }
    let extra_labels = collect_extra_labels(&query);
    let plan =
        plan_query(&query).map_err(|error| Error::Query(format!("cypher plan error: {error}")))?;
    let plan = inject_extra_labels(optimize(plan), &extra_labels);
    if std::env::var_os("NONODB_CYPHER_DEBUG_PLAN").is_some() {
        eprintln!("{plan:#?}");
    }
    validate_bindings(&plan)?;
    let mut executor = Executor::new(view, options);
    let rows = executor.execute(&plan, Vec::new())?;
    let columns = rows
        .first()
        .map(|row| row.order.clone())
        .unwrap_or_default();
    let row_count = rows.len();
    let mut out = Vec::with_capacity(row_count.min(options.max_rows));
    for row in rows.into_iter().take(options.max_rows) {
        out.push(Value::Object(row.visible_values()));
    }
    if row_count > options.max_rows {
        executor.truncated = true;
    }
    let result = json!({
        "columns": columns,
        "rows": out,
        "row_count": out.len(),
        "truncated": executor.truncated,
    });
    if serde_json::to_vec(&result).is_ok_and(|bytes| bytes.len() > options.max_memory_bytes) {
        return Err(Error::Query(format!(
            "cypher memory budget exceeded ({} bytes)",
            options.max_memory_bytes
        )));
    }
    Ok(result)
}

fn reject_unsupported_shapes(query: &Query) -> Result<()> {
    let mut after_with = false;
    let mut after_with_skip = false;
    for clause in &query.clauses {
        match clause {
            Clause::With(_) => {
                after_with = true;
                after_with_skip = false;
            }
            Clause::Skip(_) if after_with => after_with_skip = true,
            Clause::Match(_) if after_with_skip => {
                return Err(Error::Query(
                    "cypher WITH ... SKIP followed by MATCH is not supported".into(),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn replace_outside_literals(input: &str, needle: &str, replacement: &str) -> String {
    let chars: Vec<char> = input.chars().collect();
    let needle: Vec<char> = needle.chars().collect();
    let mut out = String::with_capacity(input.len());
    let mut index = 0usize;
    let mut quote: Option<char> = None;
    while index < chars.len() {
        let ch = chars[index];
        if let Some(active) = quote {
            out.push(ch);
            if ch == '\\' && index + 1 < chars.len() {
                index += 1;
                out.push(chars[index]);
            } else if ch == active {
                quote = None;
            }
            index += 1;
            continue;
        }
        if ch == '\'' || ch == '"' || ch == '`' {
            quote = Some(ch);
            out.push(ch);
            index += 1;
            continue;
        }
        if ch == '/' && chars.get(index + 1) == Some(&'/') {
            while index < chars.len() && chars[index] != '\n' {
                out.push(chars[index]);
                index += 1;
            }
            continue;
        }
        if ch == '/' && chars.get(index + 1) == Some(&'*') {
            out.push(ch);
            out.push(chars[index + 1]);
            index += 2;
            while index < chars.len() {
                if chars[index] == '*' && chars.get(index + 1) == Some(&'/') {
                    out.push('*');
                    out.push('/');
                    index += 2;
                    break;
                }
                out.push(chars[index]);
                index += 1;
            }
            continue;
        }
        if chars.get(index..index + needle.len()) == Some(needle.as_slice()) {
            out.push_str(replacement);
            index += needle.len();
            continue;
        }
        out.push(ch);
        index += 1;
    }
    out
}

fn normalize_query_text(query: &str) -> String {
    let normalized = normalize_literals(query);
    let normalized = replace_outside_literals(&normalized, "<-->", "--");
    let normalized = replace_outside_literals(&normalized, ":T|:T", ":T");
    let normalized = replace_outside_literals(
        &normalized,
        "-9223372036854775808",
        "(-9223372036854775807 - 1)",
    );
    normalize_reserved_labels(&normalize_is_null_comparisons(&normalized))
}

fn prepend_optional_anchor(query: &str) -> String {
    let trimmed = query.trim_start();
    if trimmed
        .get(..14)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("OPTIONAL MATCH"))
    {
        format!("WITH 1 AS __nonodb_anchor {query}")
    } else {
        query.to_string()
    }
}

fn cypher_string_literal(value: &str) -> String {
    let mut out = String::from("'");
    for character in value.chars() {
        match character {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out.push('\'');
    out
}

fn expand_exists_subqueries(query: &str) -> Result<String> {
    let chars: Vec<char> = query.chars().collect();
    let mut out = String::new();
    let mut index = 0;
    while index < chars.len() {
        let character = chars[index];
        if character == '\'' || character == '"' {
            let quote = character;
            out.push(character);
            index += 1;
            while index < chars.len() {
                let current = chars[index];
                out.push(current);
                index += 1;
                if current == '\\' && index < chars.len() {
                    out.push(chars[index]);
                    index += 1;
                } else if current == quote {
                    break;
                }
            }
            continue;
        }
        if character == '/' && chars.get(index + 1) == Some(&'/') {
            while index < chars.len() && chars[index] != '\n' {
                out.push(chars[index]);
                index += 1;
            }
            continue;
        }
        let word = chars
            .get(index..index + 6)
            .map(|slice| slice.iter().collect::<String>())
            .unwrap_or_default();
        let before_ok =
            index == 0 || !(chars[index - 1].is_ascii_alphanumeric() || chars[index - 1] == '_');
        let after_ok = chars
            .get(index + 6)
            .is_none_or(|next| !(next.is_ascii_alphanumeric() || *next == '_'));
        if before_ok && after_ok && word.eq_ignore_ascii_case("exists") {
            let mut cursor = index + 6;
            while chars.get(cursor).is_some_and(|c| c.is_whitespace()) {
                cursor += 1;
            }
            if chars.get(cursor) == Some(&'{') {
                let body_start = cursor + 1;
                let mut depth = 1usize;
                let mut end = body_start;
                while end < chars.len() {
                    let current = chars[end];
                    if current == '\'' || current == '"' {
                        let quote = current;
                        end += 1;
                        while end < chars.len() {
                            if chars[end] == '\\' {
                                end += 2;
                                continue;
                            }
                            if chars[end] == quote {
                                end += 1;
                                break;
                            }
                            end += 1;
                        }
                        continue;
                    }
                    if current == '{' {
                        depth += 1;
                    } else if current == '}' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    end += 1;
                }
                if depth != 0 {
                    return Err(Error::Query(
                        "cypher exists subquery is missing a closing brace".into(),
                    ));
                }
                let body: String = chars[body_start..end].iter().collect();
                let body = expand_exists_subqueries(&body)?;
                out.push_str("nonodb_exists(");
                out.push_str(&cypher_string_literal(&body));
                out.push(')');
                index = end + 1;
                continue;
            }
        }
        out.push(character);
        index += 1;
    }
    Ok(out)
}

fn collect_extra_labels(query: &Query) -> HashMap<String, Vec<String>> {
    let mut extras: HashMap<String, Vec<String>> = HashMap::new();
    for clause in &query.clauses {
        if let Clause::Match(matched) = clause {
            for pattern in &matched.patterns {
                collect_node_extra_labels(&pattern.anchor, &mut extras);
                for chain in &pattern.chain {
                    collect_node_extra_labels(&chain.node, &mut extras);
                }
            }
        }
    }
    extras
}

fn collect_node_extra_labels(node: &NodePattern, extras: &mut HashMap<String, Vec<String>>) {
    let Some(variable) = &node.var else {
        return;
    };
    for label in node.labels.iter().skip(1) {
        let labels = extras.entry(variable.clone()).or_default();
        if !labels.contains(label) {
            labels.push(label.clone());
        }
    }
}

fn inject_extra_labels(plan: Plan, extras: &HashMap<String, Vec<String>>) -> Plan {
    if extras.is_empty() {
        return plan;
    }
    match plan {
        Plan::Scan {
            var: Some(variable),
            label,
        } => {
            let mut plan = Plan::Scan {
                var: Some(variable.clone()),
                label,
            };
            for extra in extras.get(&variable).into_iter().flatten() {
                plan = Plan::Filter {
                    input: Box::new(plan),
                    pred: Expr::LabelPredicate {
                        expression: Box::new(Expr::Variable(variable.clone())),
                        labels: vec![extra.clone()],
                    },
                };
            }
            plan
        }
        Plan::Expand {
            input,
            src,
            rel_var,
            rel_types,
            range,
            direction,
            dst,
            dst_label,
        } => Plan::Expand {
            input: Box::new(inject_extra_labels(*input, extras)),
            src,
            rel_var,
            rel_types,
            range,
            direction,
            dst,
            dst_label,
        },
        Plan::Filter { input, pred } => Plan::Filter {
            input: Box::new(inject_extra_labels(*input, extras)),
            pred,
        },
        Plan::PropertyMapFilter {
            input,
            variable,
            map,
        } => Plan::PropertyMapFilter {
            input: Box::new(inject_extra_labels(*input, extras)),
            variable,
            map,
        },
        Plan::Project {
            input,
            include_existing,
            exprs,
        } => Plan::Project {
            input: Box::new(inject_extra_labels(*input, extras)),
            include_existing,
            exprs,
        },
        Plan::Sort { input, keys } => Plan::Sort {
            input: Box::new(inject_extra_labels(*input, extras)),
            keys,
        },
        Plan::Skip { input, count } => Plan::Skip {
            input: Box::new(inject_extra_labels(*input, extras)),
            count,
        },
        Plan::Limit { input, count } => Plan::Limit {
            input: Box::new(inject_extra_labels(*input, extras)),
            count,
        },
        Plan::Cartesian { left, right } => Plan::Cartesian {
            left: Box::new(inject_extra_labels(*left, extras)),
            right: Box::new(inject_extra_labels(*right, extras)),
        },
        Plan::Optional { input, optional } => Plan::Optional {
            input: Box::new(inject_extra_labels(*input, extras)),
            optional: Box::new(inject_extra_labels(*optional, extras)),
        },
        Plan::ShortestPath { input, all } => Plan::ShortestPath {
            input: Box::new(inject_extra_labels(*input, extras)),
            all,
        },
        Plan::NamedPath { input, variable } => Plan::NamedPath {
            input: Box::new(inject_extra_labels(*input, extras)),
            variable,
        },
        Plan::PlannerHint { input, hint } => Plan::PlannerHint {
            input: Box::new(inject_extra_labels(*input, extras)),
            hint,
        },
        Plan::ProcedureCall {
            input,
            name,
            arguments,
            yields,
        } => Plan::ProcedureCall {
            input: Box::new(inject_extra_labels(*input, extras)),
            name,
            arguments,
            yields,
        },
        Plan::Unwind {
            input,
            expression,
            alias,
        } => Plan::Unwind {
            input: Box::new(inject_extra_labels(*input, extras)),
            expression,
            alias,
        },
        Plan::Distinct { input } => Plan::Distinct {
            input: Box::new(inject_extra_labels(*input, extras)),
        },
        Plan::Union { left, right, all } => Plan::Union {
            left: Box::new(inject_extra_labels(*left, extras)),
            right: Box::new(inject_extra_labels(*right, extras)),
            all,
        },
        Plan::Explain { input } => Plan::Explain {
            input: Box::new(inject_extra_labels(*input, extras)),
        },
        Plan::Profile { input } => Plan::Profile {
            input: Box::new(inject_extra_labels(*input, extras)),
        },
        other => other,
    }
}

fn is_write_clause(clause: &Clause) -> bool {
    matches!(
        clause,
        Clause::Create(_)
            | Clause::Merge(_)
            | Clause::Set(_)
            | Clause::Remove(_)
            | Clause::Delete(_)
            | Clause::Foreach(_)
            | Clause::LoadCsv(_)
            | Clause::SchemaCommand(_)
            | Clause::Start(_)
    )
}
