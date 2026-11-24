//! Read-only Cypher compatibility layer.
//!
//! The parser/planner is provided by the MIT-licensed `cypher-rs-zk` front end;
//! this module supplies the storage adapter and execution engine over NonoDB's
//! document graph model.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use cypher_rs_zk::ast::{
    BinOp, CaseAlternative, Clause, CollectionPredicateKind, Direction, Expr, FunctionArguments,
    Literal, MapProjectionItem, Query, UnOp,
};
use cypher_rs_zk::{optimize, parse, plan as plan_query, Plan, ProjectExpr, SortKey};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::error::{Error, Result};
use crate::graph::{AdjEdge, GraphDirection, GraphView};

const DEFAULT_CYPHER_ROWS: usize = 1_000;
const DEFAULT_CYPHER_OPERATIONS: usize = 1_000_000;
const MAX_CYPHER_ROWS: usize = 100_000;
const MAX_CYPHER_HOPS: usize = 64;
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
}

fn default_cypher_rows() -> usize {
    DEFAULT_CYPHER_ROWS
}
fn default_cypher_ops() -> usize {
    DEFAULT_CYPHER_OPERATIONS
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
        if self.max_operations == 0 {
            return Err(Error::Query(
                "cypher `max_operations` must be positive".into(),
            ));
        }
        if !self.params.is_null() && !self.params.is_object() {
            return Err(Error::Query("cypher `params` must be a JSON object".into()));
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
    truncated: bool,
}

impl<'a> Executor<'a> {
    fn new(view: &'a dyn GraphView, options: &'a CypherOptions) -> Self {
        Self {
            graph: Graph { view },
            params: options.params.clone(),
            options,
            deadline: options
                .timeout_ms
                .map(|ms| Instant::now() + Duration::from_millis(ms)),
            operations: 0,
            truncated: false,
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

    fn execute(&mut self, plan: &Plan, input: Vec<Row>) -> Result<Vec<Row>> {
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
                let rows = self.execute_input(input)?;
                let mut out = Vec::new();
                for row in rows {
                    self.tick()?;
                    if truthy(&self.eval(pred, &row, None)?) {
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
                    let key = serde_json::to_string(&row.visible_values())?;
                    if seen.insert(key) {
                        out.push(row);
                    }
                }
                Ok(out)
            }
            Plan::Cartesian { left, right } => {
                let left_rows = self.execute_input(left)?;
                let mut out = Vec::new();
                for left_row in left_rows {
                    let right_rows = self.execute(right, vec![left_row.clone()])?;
                    for right_row in right_rows {
                        self.tick()?;
                        if let Some(mut joined) = join_rows(&left_row, &right_row) {
                            joined.merge(&right_row);
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
            Plan::Optional { input, optional } => {
                let rows = self.execute_input(input)?;
                let optional_vars = plan_vars(optional);
                let mut out = Vec::new();
                for row in rows {
                    let mut matched = self.execute(optional, vec![row.clone()])?;
                    if matched.is_empty() {
                        let mut next = row.clone();
                        for var in &optional_vars {
                            next.set(var.clone(), Value::Null);
                        }
                        out.push(next);
                    } else {
                        out.append(&mut matched);
                    }
                }
                Ok(out)
            }
            Plan::ShortestPath { input, all } => self.shortest(input, *all),
            Plan::NamedPath { input, variable } => {
                let rows = self.execute_input(input)?;
                let mut out = Vec::new();
                for mut row in rows {
                    let nodes = row.get(HIDDEN_PATH_NODES).cloned().unwrap_or(json!([]));
                    let rels = row.get(HIDDEN_PATH_RELS).cloned().unwrap_or(json!([]));
                    row.set(
                        variable.clone(),
                        json!({"nodes": nodes, "relationships": rels}),
                    );
                    out.push(row);
                }
                Ok(out)
            }
            Plan::PlannerHint { input, .. } => self.execute_input(input),
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
                    match value {
                        Value::Array(items) => {
                            for item in items {
                                let mut next = row.clone();
                                next.set(alias.clone(), item);
                                out.push(next);
                            }
                        }
                        Value::Null => {}
                        value => {
                            let mut next = row.clone();
                            next.set(alias.clone(), value);
                            out.push(next);
                        }
                    }
                }
                Ok(out)
            }
            Plan::Union { left, right, all } => {
                let mut out = self.execute_input(left)?;
                out.extend(self.execute_input(right)?);
                if !*all {
                    let mut seen = HashSet::new();
                    out.retain(|row| {
                        let key = serde_json::to_string(&row.visible_values()).unwrap_or_default();
                        seen.insert(key)
                    });
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
        self.execute(plan, Vec::new())
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
                if row.get(HIDDEN_PATH_NODES).is_none() {
                    row.set(HIDDEN_PATH_NODES, json!([node]));
                }
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
                let mut used = HashSet::new();
                let mut rels = Vec::new();
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
                for edge in self
                    .graph
                    .view
                    .neighbors(&current, graph_direction, rel_types)?
                {
                    self.tick()?;
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
                        next.set(dst, target);
                    } else {
                        next.set(HIDDEN_ANON_NODE, target);
                    }
                    next.set(HIDDEN_ANON_ID, Value::String(edge.target.clone()));
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
                if label_ok {
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

    fn project(
        &mut self,
        input: &Plan,
        include_existing: bool,
        exprs: &[ProjectExpr],
    ) -> Result<Vec<Row>> {
        let rows = self.execute_input(input)?;
        let aggregate = exprs.iter().any(|expr| contains_aggregate(&expr.expr));
        if !aggregate {
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                self.tick()?;
                let mut next = Row::new();
                if include_existing {
                    next = row.clone();
                }
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
            let mut next = Row::new();
            if include_existing {
                next = base.clone();
            }
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

    fn sort(&mut self, input: &Plan, keys: &[SortKey]) -> Result<Vec<Row>> {
        let mut rows = self.execute_input(input)?;
        let mut keyed = Vec::with_capacity(rows.len());
        for row in rows.drain(..) {
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
                    .or_else(|| base.get("properties").and_then(|p| p.get(key)).cloned())
                    .unwrap_or(Value::Null))
            }
            Expr::LabelPredicate { expression, labels } => {
                let value = self.eval(expression, row, group)?;
                Ok(json!(labels
                    .iter()
                    .all(|label| Graph::has_label(&value, label))))
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
                let start = start.as_i64().unwrap_or(0).max(0) as usize;
                let end = end.as_i64().unwrap_or(items.len() as i64).max(0) as usize;
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
            Expr::PatternExpression { .. } | Expr::PatternComprehension { .. } => Ok(Value::Null),
        }
    }

    fn eval_aggregate(
        &mut self,
        name: &str,
        distinct: bool,
        arguments: &FunctionArguments,
        group: &[Row],
    ) -> Result<Value> {
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
        if distinct {
            let mut seen = HashSet::new();
            values.retain(|value| seen.insert(serde_json::to_string(value).unwrap_or_default()));
        }
        match name {
            "count" => Ok(json!(values.len())),
            "collect" => Ok(Value::Array(values)),
            "sum" => Ok(json!(values.iter().filter_map(number).sum::<f64>())),
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
        match name {
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
            "keys" => Ok(Value::Array(
                arg(0)
                    .as_object()
                    .map(|map| map.keys().cloned().map(Value::String).collect())
                    .unwrap_or_default(),
            )),
            "properties" => Ok(arg(0)),
            "nodes" => Ok(arg(0).get("nodes").cloned().unwrap_or_else(|| json!([]))),
            "relationships" | "rels" => Ok(arg(0)
                .get("relationships")
                .cloned()
                .unwrap_or_else(|| json!([]))),
            "labels" => Ok(arg(0)
                .get("_labels")
                .cloned()
                .or_else(|| arg(0).get("labels").cloned())
                .unwrap_or_else(|| json!([]))),
            "type" => Ok(Value::String(
                Graph::relationship_type(&arg(0))
                    .unwrap_or_default()
                    .to_string(),
            )),
            "id" => Ok(arg(0).get("_id").cloned().unwrap_or(Value::Null)),
            "startNode" => {
                let id = arg(0)
                    .get("_start")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                match id {
                    Some(id) => Ok(self.graph.node(&id)?.unwrap_or(Value::Null)),
                    None => Ok(Value::Null),
                }
            }
            "endNode" => {
                let id = arg(0)
                    .get("_end")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                match id {
                    Some(id) => Ok(self.graph.node(&id)?.unwrap_or(Value::Null)),
                    None => Ok(Value::Null),
                }
            }
            "toString" => Ok(Value::String(match arg(0) {
                Value::String(value) => value,
                Value::Null => return Ok(Value::Null),
                value => value.to_string(),
            })),
            "toInteger" => Ok(match arg(0) {
                Value::String(value) => value
                    .parse::<i64>()
                    .map(|value| json!(value))
                    .unwrap_or(Value::Null),
                Value::Number(value) => value
                    .as_i64()
                    .map(|value| json!(value))
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            }),
            "toFloat" => Ok(number(&arg(0))
                .map(|value| json!(value))
                .unwrap_or(Value::Null)),
            "toBoolean" => Ok(match arg(0) {
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
                let mut out = Vec::new();
                if step != 0 {
                    let mut current = start;
                    while (step > 0 && current <= end) || (step < 0 && current >= end) {
                        out.push(json!(current));
                        current += step;
                        if out.len() > 100_000 {
                            break;
                        }
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
            "toLower" => Ok(json!(arg(0).as_str().unwrap_or("").to_lowercase())),
            "toUpper" => Ok(json!(arg(0).as_str().unwrap_or("").to_uppercase())),
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

fn expr_name(expr: &Expr) -> String {
    match expr {
        Expr::Variable(name) => name.clone(),
        Expr::Property { base, key } => format!("{}.{}", expr_name(base), key),
        Expr::FunctionCall { name, .. } => name.clone(),
        _ => format!("{expr:?}"),
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
        | Plan::Project { input, .. }
        | Plan::Sort { input, .. }
        | Plan::Skip { input, .. }
        | Plan::Limit { input, .. }
        | Plan::ShortestPath { input, .. }
        | Plan::NamedPath { input, .. }
        | Plan::PlannerHint { input, .. }
        | Plan::Unwind { input, .. }
        | Plan::Distinct { input }
        | Plan::Optional { input, .. } => collect_plan_vars(input, out),
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

fn json_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(a), Value::Number(b)) => a.as_f64() == b.as_f64(),
        _ => a == b,
    }
}

fn cmp_json(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::Number(a), Value::Number(b)) => a
            .as_f64()
            .partial_cmp(&b.as_f64())
            .unwrap_or(Ordering::Equal),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        _ => a.to_string().cmp(&b.to_string()),
    }
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
    if matches!(
        op,
        BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte
    ) {
        if lhs.is_null() || rhs.is_null() {
            return Ok(Value::Null);
        }
        if matches!(op, BinOp::Eq) {
            return Ok(json!(json_eq(&lhs, &rhs)));
        }
        if matches!(op, BinOp::Neq) {
            return Ok(json!(!json_eq(&lhs, &rhs)));
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
        return Ok(match rhs {
            Value::Array(items) => json!(items.iter().any(|item| json_eq(item, &lhs))),
            _ => Value::Null,
        });
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
        if let (Some(lhs), Some(rhs)) = (lhs.as_array(), rhs.as_array()) {
            let mut out = lhs.clone();
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
        UnOp::Neg => number(&value)
            .map(|value| json!(-value))
            .unwrap_or(Value::Null),
        UnOp::IsNull => json!(value.is_null()),
        UnOp::IsNotNull => json!(!value.is_null()),
    })
}

pub fn execute(view: &dyn GraphView, options: &CypherOptions) -> Result<Value> {
    options.validate()?;
    let query: Query = parse(&options.query)
        .map_err(|error| Error::Query(format!("cypher parse error: {error}")))?;
    if query.statement_count() != 1 {
        return Err(Error::Query(
            "cypher supports exactly one read-only statement".into(),
        ));
    }
    if query.clauses.iter().any(is_write_clause) {
        return Err(Error::Query(
            "cypher write clauses are not supported in read-only mode".into(),
        ));
    }
    let plan =
        plan_query(&query).map_err(|error| Error::Query(format!("cypher plan error: {error}")))?;
    let plan = optimize(plan);
    let mut executor = Executor::new(view, options);
    let rows = executor.execute(&plan, Vec::new())?;
    let columns = rows
        .first()
        .map(|row| row.order.clone())
        .unwrap_or_default();
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(Value::Object(row.visible_values()));
        if out.len() >= options.max_rows {
            executor.truncated = true;
            break;
        }
    }
    Ok(json!({
        "columns": columns,
        "rows": out,
        "row_count": out.len(),
        "truncated": executor.truncated,
    }))
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
