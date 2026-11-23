//! Graph traversal over ordinary documents.
//!
//! Nodes are documents in a node collection. Edges are documents in an edge
//! collection with `from`, `to`, and optional `label`/`weight` fields. A graph
//! definition is stored in `meta` under `graph:<node_collection>` and the
//! reverse mapping under `graph_edge:<edge_collection>`.
//!
//! The adjacency index is derived data: every edge insert, update, delete, TTL
//! expiry, and replication apply updates it in the same LMDB transaction as the
//! document. It can always be rebuilt from the edge collection with
//! `reindex_graph`.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::date;
use crate::error::{Error, Result};
use crate::id;
use crate::query::{matches, path, validate::validate_filter};
use crate::vector::VectorMetric;

/// Prefix for the node-collection graph definition.
pub const NODE_DEF_PREFIX: &str = "graph:";
/// Prefix for the edge-collection reverse definition.
pub const EDGE_DEF_PREFIX: &str = "graph_edge:";

/// Maximum accepted traversal depth.
pub const MAX_GRAPH_DEPTH: usize = 64;
/// Default traversal depth.
pub const DEFAULT_GRAPH_DEPTH: usize = 3;
/// Default node budget.
pub const DEFAULT_GRAPH_NODES: usize = 1000;
/// Default edge budget.
pub const DEFAULT_GRAPH_EDGES: usize = 10_000;
/// Default serialized result budget (64 MiB), matching lookup.
pub const DEFAULT_GRAPH_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphDef {
    pub node_collection: String,
    pub edge_collection: String,
    #[serde(default = "default_from_field")]
    pub from_field: String,
    #[serde(default = "default_to_field")]
    pub to_field: String,
    #[serde(default = "default_label_field")]
    pub label_field: String,
    #[serde(default = "default_weight_field")]
    pub weight_field: String,
    /// Optional edge validity window fields (Unix milliseconds / BSON dates).
    #[serde(default)]
    pub valid_from_field: String,
    #[serde(default)]
    pub valid_to_field: String,
    #[serde(default = "default_true")]
    pub directed: bool,
    /// Active adjacency generation. Bumped by an online rebuild so readers
    /// keep using the old slots until the atomic swap.
    #[serde(default)]
    pub generation: u64,
}

fn default_from_field() -> String {
    "from".into()
}
fn default_to_field() -> String {
    "to".into()
}
fn default_label_field() -> String {
    "label".into()
}
fn default_weight_field() -> String {
    "weight".into()
}
fn default_true() -> bool {
    true
}

impl GraphDef {
    pub fn validate(&self) -> Result<()> {
        if self.node_collection.trim().is_empty() || self.edge_collection.trim().is_empty() {
            return Err(Error::Query(
                "graph requires `collection` and `edge_collection`".into(),
            ));
        }
        for (name, field) in [
            ("from_field", &self.from_field),
            ("to_field", &self.to_field),
            ("label_field", &self.label_field),
            ("weight_field", &self.weight_field),
            ("valid_from_field", &self.valid_from_field),
            ("valid_to_field", &self.valid_to_field),
        ] {
            if field.is_empty() && (name.starts_with("valid_")) {
                continue;
            }
            if field.is_empty() {
                return Err(Error::Query(format!("graph `{name}` must not be empty")));
            }
            if field.starts_with('$') || field.contains('\0') {
                return Err(Error::Query(format!("invalid graph `{name}`")));
            }
        }
        if self.from_field == self.to_field {
            return Err(Error::Query(
                "graph `from_field` and `to_field` must differ".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum GraphDirection {
    #[default]
    Out,
    In,
    Both,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TraverseAlgorithm {
    #[default]
    Bfs,
    Dfs,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct VectorSeed {
    pub field: String,
    pub vector: Vec<f32>,
    #[serde(default)]
    pub filter: Value,
    #[serde(default = "default_seed_k")]
    pub k: usize,
}

fn default_seed_k() -> usize {
    5
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TraverseSeed {
    pub vector: VectorSeed,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TraverseOptions {
    #[serde(default)]
    pub start: Vec<Value>,
    #[serde(default)]
    pub seed: Option<TraverseSeed>,
    #[serde(default)]
    pub direction: GraphDirection,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub edge_filter: Option<Value>,
    #[serde(default)]
    pub node_filter: Option<Value>,
    /// Only follow edges valid at this instant (Unix ms or BSON date).
    #[serde(default)]
    pub as_of: Option<Value>,
    #[serde(default = "default_depth")]
    pub max_depth: usize,
    #[serde(default = "default_nodes")]
    pub limit_nodes: usize,
    #[serde(default = "default_edges")]
    pub limit_edges: usize,
    #[serde(default = "default_bytes")]
    pub max_bytes: usize,
    #[serde(default = "default_true")]
    pub tree: bool,
    #[serde(default)]
    pub include_edges: bool,
    #[serde(default)]
    pub algorithm: TraverseAlgorithm,
    /// Optional flat projection applied to returned node documents. `_id` is
    /// always retained for tree identity.
    #[serde(default)]
    pub projection: Option<Value>,
}

fn default_depth() -> usize {
    DEFAULT_GRAPH_DEPTH
}
fn default_nodes() -> usize {
    DEFAULT_GRAPH_NODES
}
fn default_edges() -> usize {
    DEFAULT_GRAPH_EDGES
}
fn default_bytes() -> usize {
    DEFAULT_GRAPH_BYTES
}

impl TraverseOptions {
    pub fn validate(&self) -> Result<()> {
        if self.start.is_empty() && self.seed.is_none() {
            return Err(Error::Query(
                "traverse requires `start` or `seed.vector`".into(),
            ));
        }
        if self.start.len() > 10_000 {
            return Err(Error::Query("traverse `start` exceeds 10000 ids".into()));
        }
        if self.max_depth > MAX_GRAPH_DEPTH {
            return Err(Error::Query(format!(
                "traverse `max_depth` must be at most {MAX_GRAPH_DEPTH}"
            )));
        }
        if self.limit_nodes == 0 || self.limit_edges == 0 || self.max_bytes == 0 {
            return Err(Error::Query(
                "traverse limits must be greater than zero".into(),
            ));
        }
        if let Some(filter) = &self.edge_filter {
            validate_filter(filter)?;
        }
        if let Some(filter) = &self.node_filter {
            validate_filter(filter)?;
        }
        if let Some(seed) = &self.seed {
            if seed.vector.k == 0 || seed.vector.k > 1000 {
                return Err(Error::Query(
                    "traverse seed `k` must be between 1 and 1000".into(),
                ));
            }
            if seed.vector.vector.is_empty() {
                return Err(Error::Query(
                    "traverse seed vector must not be empty".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ShortestPathOptions {
    pub start: Value,
    pub target: Value,
    #[serde(default)]
    pub direction: GraphDirection,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub edge_filter: Option<Value>,
    #[serde(default)]
    pub node_filter: Option<Value>,
    #[serde(default)]
    pub as_of: Option<Value>,
    #[serde(default = "default_depth")]
    pub max_depth: usize,
    #[serde(default = "default_nodes")]
    pub limit_nodes: usize,
    #[serde(default)]
    pub weighted: bool,
    /// Optional flat projection applied to path nodes.
    #[serde(default)]
    pub projection: Option<Value>,
}

impl ShortestPathOptions {
    pub fn validate(&self) -> Result<()> {
        if self.max_depth > MAX_GRAPH_DEPTH {
            return Err(Error::Query(format!(
                "shortest_path `max_depth` must be at most {MAX_GRAPH_DEPTH}"
            )));
        }
        if self.limit_nodes == 0 {
            return Err(Error::Query(
                "shortest_path `limit_nodes` must be greater than zero".into(),
            ));
        }
        if let Some(filter) = &self.edge_filter {
            validate_filter(filter)?;
        }
        if let Some(filter) = &self.node_filter {
            validate_filter(filter)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NeighborsOptions {
    pub start: Value,
    #[serde(default)]
    pub direction: GraphDirection,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub edge_filter: Option<Value>,
    #[serde(default)]
    pub node_filter: Option<Value>,
    #[serde(default)]
    pub as_of: Option<Value>,
    #[serde(default)]
    pub projection: Option<Value>,
    #[serde(default)]
    pub include_edges: bool,
    #[serde(default = "default_neighbors_limit")]
    pub limit: usize,
}

fn default_neighbors_limit() -> usize {
    100
}

impl NeighborsOptions {
    pub fn validate(&self) -> Result<()> {
        if self.limit == 0 || self.limit > DEFAULT_GRAPH_EDGES {
            return Err(Error::Query(format!(
                "neighbors `limit` must be between 1 and {DEFAULT_GRAPH_EDGES}"
            )));
        }
        if let Some(filter) = &self.edge_filter {
            validate_filter(filter)?;
        }
        if let Some(filter) = &self.node_filter {
            validate_filter(filter)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HybridOptions {
    #[serde(flatten)]
    pub vector: VectorSeed,
    #[serde(default)]
    pub direction: GraphDirection,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub edge_filter: Option<Value>,
    #[serde(default)]
    pub node_filter: Option<Value>,
    #[serde(default)]
    pub as_of: Option<Value>,
    #[serde(default = "default_hybrid_depth")]
    pub max_depth: usize,
    #[serde(default = "default_hybrid_limit")]
    pub limit: usize,
    #[serde(default = "default_nodes")]
    pub max_nodes: usize,
    #[serde(default = "default_edges")]
    pub max_edges: usize,
    #[serde(default = "default_vector_weight")]
    pub vector_weight: f64,
    #[serde(default = "default_graph_weight")]
    pub graph_weight: f64,
    #[serde(default)]
    pub projection: Option<Value>,
}

fn default_hybrid_depth() -> usize {
    2
}
fn default_hybrid_limit() -> usize {
    20
}
fn default_vector_weight() -> f64 {
    0.7
}
fn default_graph_weight() -> f64 {
    0.3
}

impl HybridOptions {
    pub fn validate(&self) -> Result<()> {
        if self.vector.k == 0 || self.vector.k > 1000 {
            return Err(Error::Query("hybrid `k` must be between 1 and 1000".into()));
        }
        if self.vector.vector.is_empty() {
            return Err(Error::Query("hybrid vector must not be empty".into()));
        }
        if self.max_depth > MAX_GRAPH_DEPTH {
            return Err(Error::Query(format!(
                "hybrid `max_depth` must be at most {MAX_GRAPH_DEPTH}"
            )));
        }
        if self.limit == 0 || self.limit > DEFAULT_GRAPH_NODES {
            return Err(Error::Query(format!(
                "hybrid `limit` must be between 1 and {DEFAULT_GRAPH_NODES}"
            )));
        }
        if self.max_nodes == 0 || self.max_nodes > 10 * DEFAULT_GRAPH_NODES {
            return Err(Error::Query(
                "hybrid `max_nodes` must be between 1 and 10000".into(),
            ));
        }
        if self.max_edges == 0 || self.max_edges > 10 * DEFAULT_GRAPH_EDGES {
            return Err(Error::Query(
                "hybrid `max_edges` must be between 1 and 100000".into(),
            ));
        }
        if !self.vector_weight.is_finite()
            || !self.graph_weight.is_finite()
            || self.vector_weight < 0.0
            || self.graph_weight < 0.0
        {
            return Err(Error::Query(
                "hybrid weights must be finite non-negative numbers".into(),
            ));
        }
        if self.vector_weight + self.graph_weight <= 0.0 {
            return Err(Error::Query(
                "hybrid requires a positive vector_weight or graph_weight".into(),
            ));
        }
        if let Some(filter) = &self.edge_filter {
            validate_filter(filter)?;
        }
        if let Some(filter) = &self.node_filter {
            validate_filter(filter)?;
        }
        Ok(())
    }
}

/// One adjacency entry returned by a [`GraphView`].
#[derive(Debug, Clone)]
pub struct AdjEdge {
    pub edge_id: String,
    pub label: String,
    pub weight: Option<f64>,
    pub target: String,
    pub valid_from: Option<i64>,
    pub valid_to: Option<i64>,
}

pub trait GraphView {
    fn node(&self, id: &str) -> Result<Option<Value>>;
    fn edge(&self, id: &str) -> Result<Option<Value>>;
    fn neighbors(
        &self,
        id: &str,
        direction: GraphDirection,
        labels: &[String],
    ) -> Result<Vec<AdjEdge>>;
}

pub const REBUILD_PREFIX: &str = "graph_rebuild:";

pub fn rebuild_key(edge_collection: &str) -> String {
    format!("{REBUILD_PREFIX}{edge_collection}")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphRebuildState {
    pub node_collection: String,
    pub edge_collection: String,
    pub target_generation: u64,
    #[serde(default)]
    pub after: Option<String>,
    #[serde(default)]
    pub scanned: u64,
}

pub fn node_def_key(collection: &str) -> String {
    format!("{NODE_DEF_PREFIX}{collection}")
}

pub fn edge_def_key(collection: &str) -> String {
    format!("{EDGE_DEF_PREFIX}{collection}")
}

pub fn adjacency_resource(edge_collection: &str, direction: GraphDirection) -> String {
    adjacency_resource_gen(edge_collection, direction, 0)
}

pub fn adjacency_resource_gen(
    edge_collection: &str,
    direction: GraphDirection,
    generation: u64,
) -> String {
    let side = match direction {
        GraphDirection::In => "in",
        _ => "out",
    };
    if generation == 0 {
        format!("graph:{edge_collection}:{side}")
    } else {
        format!("graph:{edge_collection}:{side}:g{generation}")
    }
}

pub fn parse_def(raw: &[u8]) -> Result<GraphDef> {
    let def: GraphDef = serde_json::from_slice(raw)?;
    def.validate()?;
    Ok(def)
}

/// Canonical node key. Graph edges may reference a string or ObjectId `_id`.
pub fn node_key(value: &Value) -> Result<String> {
    id::key_from_value(value)
        .ok_or_else(|| Error::Query("graph id must be a string or ObjectId".into()))
}

fn push_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| Error::Query("graph key component is too long".into()))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value);
    Ok(())
}

/// Key prefix for every adjacency entry leaving (or entering) `node`.
pub fn node_prefix(node: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + node.len());
    out.extend_from_slice(&(node.len() as u32).to_be_bytes());
    out.extend_from_slice(node.as_bytes());
    out
}

/// Key prefix for adjacency entries for one label.
pub fn label_prefix(node: &str, label: &str) -> Vec<u8> {
    let mut out = node_prefix(node);
    out.extend_from_slice(&(label.len() as u32).to_be_bytes());
    out.extend_from_slice(label.as_bytes());
    out
}

pub fn adjacency_key(node: &str, label: &str, target: &str, edge_id: &str) -> Result<Vec<u8>> {
    let mut out = node_prefix(node);
    push_bytes(&mut out, label.as_bytes())?;
    push_bytes(&mut out, target.as_bytes())?;
    out.extend_from_slice(edge_id.as_bytes());
    Ok(out)
}

pub fn decode_adjacency_key(raw: &[u8]) -> Option<(&str, &str, &str, &str)> {
    fn take<'a>(raw: &'a [u8], at: &mut usize) -> Option<&'a str> {
        if raw.len().saturating_sub(*at) < 4 {
            return None;
        }
        let len = u32::from_be_bytes(raw[*at..*at + 4].try_into().ok()?) as usize;
        *at += 4;
        let end = at.checked_add(len)?;
        let value = std::str::from_utf8(raw.get(*at..end)?).ok()?;
        *at = end;
        Some(value)
    }
    let mut at = 0;
    let node = take(raw, &mut at)?;
    let label = take(raw, &mut at)?;
    let target = take(raw, &mut at)?;
    let edge_id = std::str::from_utf8(raw.get(at..)?).ok()?;
    Some((node, label, target, edge_id))
}

/// Fields extracted from one edge document.
pub struct EdgeParts {
    pub from: String,
    pub to: String,
    pub label: String,
    pub weight: Option<f64>,
    pub valid_from: Option<i64>,
    pub valid_to: Option<i64>,
}

fn edge_time(doc: &Value, field: &str, name: &str) -> Result<Option<i64>> {
    if field.is_empty() {
        return Ok(None);
    }
    match path::get(doc, field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => {
            if let Some(at) = value.as_i64() {
                return Ok(Some(at));
            }
            if let Some(at) = value.as_u64() {
                return Ok(i64::try_from(at).ok());
            }
            date::parse_value(value).map(Some).ok_or_else(|| {
                Error::Query(format!(
                    "edge `{name}` field `{field}` must be a date or Unix milliseconds"
                ))
            })
        }
    }
}

pub fn edge_parts(def: &GraphDef, doc: &Value) -> Result<EdgeParts> {
    let from = path::get(doc, &def.from_field)
        .ok_or_else(|| Error::Query(format!("edge is missing `{}`", def.from_field)))?;
    let to = path::get(doc, &def.to_field)
        .ok_or_else(|| Error::Query(format!("edge is missing `{}`", def.to_field)))?;
    let from = node_key(from)?;
    let to = node_key(to)?;
    let label = path::get(doc, &def.label_field)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let weight = path::get(doc, &def.weight_field).and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_i64().map(|n| n as f64))
            .or_else(|| value.as_u64().map(|n| n as f64))
    });
    Ok(EdgeParts {
        from,
        to,
        label,
        weight,
        valid_from: edge_time(doc, &def.valid_from_field, "valid_from")?,
        valid_to: edge_time(doc, &def.valid_to_field, "valid_to")?,
    })
}

pub fn edge_id(doc: &Value) -> Result<String> {
    doc.get("_id")
        .and_then(id::key_from_value)
        .ok_or(Error::InvalidId)
}

/// Encoded adjacency writes for one edge document, in both directions.
pub type AdjacencyEntry = (GraphDirection, Vec<u8>, Vec<u8>);

/// Compact adjacency value. The full edge document stays in the edge
/// collection and is loaded only when a caller asks for `include_edges` or an
/// edge-level filter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdjacencyValue {
    pub edge_id: String,
    pub label: String,
    pub weight: Option<f64>,
    pub valid_from: Option<i64>,
    pub valid_to: Option<i64>,
}

pub const ADJACENCY_VALUE_VERSION: u8 = 1;

pub fn encode_adjacency_value(value: &AdjacencyValue) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(32);
    out.push(ADJACENCY_VALUE_VERSION);
    push_bytes(&mut out, value.edge_id.as_bytes())?;
    push_bytes(&mut out, value.label.as_bytes())?;
    match value.weight {
        Some(weight) => {
            out.push(1);
            out.extend_from_slice(&weight.to_le_bytes());
        }
        None => out.push(0),
    }
    for at in [value.valid_from, value.valid_to] {
        match at {
            Some(at) => {
                out.push(1);
                out.extend_from_slice(&at.to_le_bytes());
            }
            None => out.push(0),
        }
    }
    Ok(out)
}

pub fn decode_adjacency_value(raw: &[u8]) -> Result<AdjacencyValue> {
    fn take<'a>(raw: &'a [u8], at: &mut usize) -> Result<&'a str> {
        if raw.len().saturating_sub(*at) < 4 {
            return Err(Error::Query("corrupt graph adjacency value".into()));
        }
        let len = u32::from_be_bytes(
            raw[*at..*at + 4]
                .try_into()
                .map_err(|_| Error::Query("corrupt graph adjacency value".into()))?,
        ) as usize;
        *at += 4;
        let end = at
            .checked_add(len)
            .ok_or_else(|| Error::Query("corrupt graph adjacency value".into()))?;
        let value = std::str::from_utf8(
            raw.get(*at..end)
                .ok_or_else(|| Error::Query("corrupt graph adjacency value".into()))?,
        )
        .map_err(|_| Error::Query("corrupt graph adjacency value".into()))?;
        *at = end;
        Ok(value)
    }
    if raw.first() != Some(&ADJACENCY_VALUE_VERSION) {
        return Err(Error::Query(
            "unsupported graph adjacency value version".into(),
        ));
    }
    let mut at = 1;
    let edge_id = take(raw, &mut at)?.to_string();
    let label = take(raw, &mut at)?.to_string();
    fn flag(raw: &[u8], at: &mut usize) -> Result<bool> {
        let byte = *raw
            .get(*at)
            .ok_or_else(|| Error::Query("corrupt graph adjacency value".into()))?;
        *at += 1;
        Ok(byte != 0)
    }
    fn i64_at(raw: &[u8], at: &mut usize) -> Result<i64> {
        let end = at
            .checked_add(8)
            .ok_or_else(|| Error::Query("corrupt graph adjacency value".into()))?;
        let bytes: [u8; 8] = raw
            .get(*at..end)
            .ok_or_else(|| Error::Query("corrupt graph adjacency value".into()))?
            .try_into()
            .map_err(|_| Error::Query("corrupt graph adjacency value".into()))?;
        *at = end;
        Ok(i64::from_le_bytes(bytes))
    }
    let weight = if flag(raw, &mut at)? {
        let bytes: [u8; 8] = raw
            .get(at..at + 8)
            .ok_or_else(|| Error::Query("corrupt graph adjacency value".into()))?
            .try_into()
            .map_err(|_| Error::Query("corrupt graph adjacency value".into()))?;
        at += 8;
        Some(f64::from_le_bytes(bytes))
    } else {
        None
    };
    let valid_from = if flag(raw, &mut at)? {
        Some(i64_at(raw, &mut at)?)
    } else {
        None
    };
    let valid_to = if flag(raw, &mut at)? {
        Some(i64_at(raw, &mut at)?)
    } else {
        None
    };
    Ok(AdjacencyValue {
        edge_id,
        label,
        weight,
        valid_from,
        valid_to,
    })
}

pub fn adjacency_entries(def: &GraphDef, doc: &Value) -> Result<Vec<AdjacencyEntry>> {
    let parts = edge_parts(def, doc)?;
    let id = edge_id(doc)?;
    let encoded = encode_adjacency_value(&AdjacencyValue {
        edge_id: id.clone(),
        label: parts.label.clone(),
        weight: parts.weight,
        valid_from: parts.valid_from,
        valid_to: parts.valid_to,
    })?;
    Ok(vec![
        (
            GraphDirection::Out,
            adjacency_key(&parts.from, &parts.label, &parts.to, &id)?,
            encoded.clone(),
        ),
        (
            GraphDirection::In,
            adjacency_key(&parts.to, &parts.label, &parts.from, &id)?,
            encoded,
        ),
    ])
}

struct Record {
    id: String,
    depth: usize,
    parent: Option<usize>,
    node: Value,
    via: Option<Value>,
    cycle: bool,
}

pub(crate) fn as_of_ms(value: Option<&Value>) -> Result<Option<i64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if let Some(at) = value.as_i64() {
        return Ok(Some(at));
    }
    if let Some(at) = value.as_u64() {
        return Ok(i64::try_from(at).ok());
    }
    date::parse_value(value)
        .map(Some)
        .ok_or_else(|| Error::Query("graph `as_of` must be a date or Unix milliseconds".into()))
}

pub(crate) fn active_filter(filter: &Option<Value>) -> bool {
    !filter
        .as_ref()
        .is_none_or(|value| value.is_null() || value.as_object().is_some_and(|obj| obj.is_empty()))
}

pub(crate) fn edge_in_window(edge: &AdjEdge, as_of: Option<i64>) -> bool {
    let Some(at) = as_of else {
        return true;
    };
    edge.valid_from.is_none_or(|from| from <= at) && edge.valid_to.is_none_or(|to| at < to)
}

pub(crate) fn edge_matches(
    view: &dyn GraphView,
    edge: &AdjEdge,
    filter: &Option<Value>,
) -> Result<bool> {
    if !active_filter(filter) {
        return Ok(true);
    }
    let doc = view
        .edge(&edge.edge_id)?
        .ok_or_else(|| Error::Query(format!("graph edge `{}` is missing", edge.edge_id)))?;
    Ok(matches(&doc, filter.as_ref().expect("filter checked")))
}

pub(crate) fn node_matches(node: &Value, filter: &Option<Value>) -> bool {
    !active_filter(filter) || matches(node, filter.as_ref().expect("filter checked"))
}

fn via_value(view: &dyn GraphView, edge: &AdjEdge, include_edges: bool) -> Result<Value> {
    if include_edges {
        if let Some(doc) = view.edge(&edge.edge_id)? {
            return Ok(doc);
        }
    }
    let mut out = json!({
        "_id": edge.edge_id,
        "label": edge.label,
    });
    if let Some(weight) = edge.weight {
        out["weight"] = json!(weight);
    }
    if let Some(at) = edge.valid_from {
        out["valid_from"] = date::canonical(at);
    }
    if let Some(at) = edge.valid_to {
        out["valid_to"] = date::canonical(at);
    }
    Ok(out)
}

fn project_node(node: Value, projection: Option<&Value>) -> Result<Value> {
    let mut out = crate::collection::project_document(&node, projection);
    if out.get("_id").is_none() {
        if let Some(id) = node.get("_id") {
            out["_id"] = id.clone();
        }
    }
    Ok(out)
}

fn record_value(record: &Record, children: Vec<Value>) -> Value {
    let mut out = json!({
        "node": record.node,
        "depth": record.depth,
        "children": children,
    });
    if let Some(via) = &record.via {
        out["via"] = via.clone();
    }
    if record.cycle {
        out["cycle"] = json!(true);
    }
    out
}

pub fn traverse(view: &dyn GraphView, options: &TraverseOptions) -> Result<Value> {
    options.validate()?;
    let as_of = as_of_ms(options.as_of.as_ref())?;
    let mut records: Vec<Record> = Vec::new();
    let mut visited: HashMap<String, usize> = HashMap::new();
    let mut missing: Vec<Value> = Vec::new();
    let mut truncated = false;
    let mut edges = 0usize;
    let mut bytes = 0usize;

    for start in &options.start {
        let id = match node_key(start) {
            Ok(id) => id,
            Err(_) => {
                missing.push(start.clone());
                continue;
            }
        };
        if visited.contains_key(&id) {
            continue;
        }
        if records.len() >= options.limit_nodes {
            truncated = true;
            break;
        }
        match view.node(&id)? {
            Some(node) => {
                if !node_matches(&node, &options.node_filter) {
                    continue;
                }
                let node = project_node(node, options.projection.as_ref())?;
                bytes = bytes.saturating_add(serde_json::to_vec(&node)?.len());
                if bytes > options.max_bytes {
                    truncated = true;
                    break;
                }
                let index = records.len();
                visited.insert(id.clone(), index);
                records.push(Record {
                    id,
                    depth: 0,
                    parent: None,
                    node,
                    via: None,
                    cycle: false,
                });
            }
            None => missing.push(start.clone()),
        }
    }

    let mut frontier: VecDeque<usize> = records.iter().enumerate().map(|(i, _)| i).collect();
    let mut stack: Vec<usize> = records.iter().enumerate().rev().map(|(i, _)| i).collect();
    let mut stop = false;

    while !stop {
        let current = match options.algorithm {
            TraverseAlgorithm::Bfs => frontier.pop_front(),
            TraverseAlgorithm::Dfs => stack.pop(),
        };
        let Some(current) = current else {
            break;
        };
        if records[current].depth >= options.max_depth {
            continue;
        }
        let neighbors = view.neighbors(&records[current].id, options.direction, &options.labels)?;
        for edge in neighbors {
            if edges >= options.limit_edges {
                truncated = true;
                stop = true;
                break;
            }
            edges += 1;
            if !edge_in_window(&edge, as_of) || !edge_matches(view, &edge, &options.edge_filter)? {
                continue;
            }
            let via = via_value(view, &edge, options.include_edges)?;
            bytes = bytes.saturating_add(serde_json::to_vec(&via)?.len());
            if bytes > options.max_bytes {
                truncated = true;
                stop = true;
                break;
            }
            if let Some(&existing) = visited.get(&edge.target) {
                if records.len() >= options.limit_nodes {
                    truncated = true;
                    stop = true;
                    break;
                }
                bytes = bytes.saturating_add(serde_json::to_vec(&records[existing].node)?.len());
                if bytes > options.max_bytes {
                    truncated = true;
                    stop = true;
                    break;
                }
                records.push(Record {
                    id: edge.target.clone(),
                    depth: records[current].depth + 1,
                    parent: Some(current),
                    node: records[existing].node.clone(),
                    via: Some(via),
                    cycle: true,
                });
                continue;
            }
            let Some(node) = view.node(&edge.target)? else {
                missing.push(Value::String(edge.target));
                continue;
            };
            if !node_matches(&node, &options.node_filter) {
                continue;
            }
            let node = project_node(node, options.projection.as_ref())?;
            if records.len() >= options.limit_nodes {
                truncated = true;
                stop = true;
                break;
            }
            bytes = bytes.saturating_add(serde_json::to_vec(&node)?.len());
            if bytes > options.max_bytes {
                truncated = true;
                stop = true;
                break;
            }
            let index = records.len();
            visited.insert(edge.target.clone(), index);
            records.push(Record {
                id: edge.target,
                depth: records[current].depth + 1,
                parent: Some(current),
                node,
                via: Some(via),
                cycle: false,
            });
            match options.algorithm {
                TraverseAlgorithm::Bfs => frontier.push_back(index),
                TraverseAlgorithm::Dfs => stack.push(index),
            }
        }
    }

    let node_count = records.len();
    let result = if options.tree {
        let mut children: Vec<Vec<usize>> = vec![Vec::new(); records.len()];
        let mut roots = Vec::new();
        for (index, record) in records.iter().enumerate() {
            match record.parent {
                Some(parent) => children[parent].push(index),
                None => roots.push(index),
            }
        }
        fn build(index: usize, records: &[Record], children: &[Vec<usize>]) -> Value {
            let nested = children[index]
                .iter()
                .map(|child| build(*child, records, children))
                .collect();
            record_value(&records[index], nested)
        }
        let roots: Vec<Value> = roots
            .into_iter()
            .map(|index| build(index, &records, &children))
            .collect();
        json!({
            "roots": roots,
            "nodes": node_count,
            "edges": edges,
            "truncated": truncated,
            "missing": missing,
        })
    } else {
        let items: Vec<Value> = records
            .iter()
            .map(|record| {
                let mut item = json!({
                    "node": record.node,
                    "depth": record.depth,
                    "cycle": record.cycle,
                });
                if let Some(via) = &record.via {
                    item["via"] = via.clone();
                }
                if let Some(parent) = record.parent {
                    item["parent"] = json!(records[parent].id);
                }
                item
            })
            .collect();
        json!({
            "items": items,
            "nodes": node_count,
            "edges": edges,
            "truncated": truncated,
            "missing": missing,
        })
    };
    Ok(result)
}

struct QueueEntry {
    distance: f64,
    hops: usize,
    id: String,
}

impl PartialEq for QueueEntry {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance && self.hops == other.hops && self.id == other.id
    }
}
impl Eq for QueueEntry {}
impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.hops.cmp(&self.hops))
            .then_with(|| other.id.cmp(&self.id))
    }
}

fn path_result(
    view: &dyn GraphView,
    goal: &str,
    prev: &HashMap<String, (String, AdjEdge)>,
    weight: f64,
    projection: Option<&Value>,
) -> Result<Value> {
    let mut chain: Vec<(String, Option<AdjEdge>)> = Vec::new();
    let mut current = goal.to_string();
    while let Some((parent, edge)) = prev.get(&current) {
        chain.push((current.clone(), Some(edge.clone())));
        current = parent.clone();
    }
    chain.push((current, None));
    chain.reverse();
    let mut path = Vec::with_capacity(chain.len());
    for (id, edge) in chain {
        let node = view
            .node(&id)?
            .unwrap_or_else(|| json!({"_id": id, "missing": true}));
        let node = project_node(node, projection)?;
        let mut item = json!({"node": node});
        if let Some(edge) = edge {
            item["via"] = via_value(view, &edge, true)?;
        }
        path.push(item);
    }
    Ok(json!({
        "found": true,
        "path": path,
        "weight": weight,
        "hops": path.len().saturating_sub(1),
    }))
}

pub fn shortest_path(view: &dyn GraphView, options: &ShortestPathOptions) -> Result<Value> {
    options.validate()?;
    let as_of = as_of_ms(options.as_of.as_ref())?;
    let start = node_key(&options.start)?;
    let target = node_key(&options.target)?;
    if active_filter(&options.node_filter) {
        match view.node(&start)? {
            Some(node) if node_matches(&node, &options.node_filter) => {}
            _ => return Ok(json!({"found": false, "path": [], "weight": 0.0, "hops": 0})),
        }
    }
    if start == target {
        let node = view.node(&start)?.unwrap_or_else(|| json!({"_id": start}));
        if !node_matches(&node, &options.node_filter) {
            return Ok(json!({"found": false, "path": [], "weight": 0.0, "hops": 0}));
        }
        let node = project_node(node, options.projection.as_ref())?;
        return Ok(json!({
            "found": true,
            "path": [{"node": node}],
            "weight": 0.0,
            "hops": 0,
        }));
    }

    if !options.weighted {
        let mut prev: HashMap<String, (String, AdjEdge)> = HashMap::new();
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<(String, usize)> = VecDeque::new();
        visited.insert(start.clone());
        queue.push_back((start, 0));
        while let Some((current, depth)) = queue.pop_front() {
            if depth >= options.max_depth {
                continue;
            }
            for edge in view.neighbors(&current, options.direction, &options.labels)? {
                if !edge_in_window(&edge, as_of)
                    || !edge_matches(view, &edge, &options.edge_filter)?
                {
                    continue;
                }
                if visited.contains(&edge.target) {
                    continue;
                }
                match view.node(&edge.target)? {
                    Some(node) if node_matches(&node, &options.node_filter) => {}
                    _ => continue,
                }
                if visited.len() >= options.limit_nodes {
                    break;
                }
                visited.insert(edge.target.clone());
                prev.insert(edge.target.clone(), (current.clone(), edge.clone()));
                if edge.target == target {
                    return path_result(
                        view,
                        &target,
                        &prev,
                        (depth + 1) as f64,
                        options.projection.as_ref(),
                    );
                }
                queue.push_back((edge.target, depth + 1));
            }
        }
    } else {
        let mut prev: HashMap<String, (String, AdjEdge)> = HashMap::new();
        let mut best: HashMap<String, f64> = HashMap::new();
        let mut heap = BinaryHeap::new();
        best.insert(start.clone(), 0.0);
        heap.push(QueueEntry {
            distance: 0.0,
            hops: 0,
            id: start,
        });
        let mut popped = 0usize;
        while let Some(entry) = heap.pop() {
            if entry.distance > *best.get(&entry.id).unwrap_or(&f64::INFINITY) {
                continue;
            }
            popped += 1;
            if popped > options.limit_nodes {
                break;
            }
            if entry.id == target {
                return path_result(
                    view,
                    &target,
                    &prev,
                    entry.distance,
                    options.projection.as_ref(),
                );
            }
            if entry.hops >= options.max_depth {
                continue;
            }
            for edge in view.neighbors(&entry.id, options.direction, &options.labels)? {
                if !edge_in_window(&edge, as_of)
                    || !edge_matches(view, &edge, &options.edge_filter)?
                {
                    continue;
                }
                match view.node(&edge.target)? {
                    Some(node) if node_matches(&node, &options.node_filter) => {}
                    _ => continue,
                }
                let weight = edge.weight.ok_or_else(|| {
                    Error::Query(format!("edge `{}` has no numeric weight", edge.edge_id))
                })?;
                if !weight.is_finite() || weight < 0.0 {
                    return Err(Error::Query(format!(
                        "edge `{}` has invalid weight",
                        edge.edge_id
                    )));
                }
                let distance = entry.distance + weight;
                let improve = best
                    .get(&edge.target)
                    .is_none_or(|current| distance < *current);
                if improve {
                    best.insert(edge.target.clone(), distance);
                    prev.insert(edge.target.clone(), (entry.id.clone(), edge.clone()));
                    heap.push(QueueEntry {
                        distance,
                        hops: entry.hops + 1,
                        id: edge.target,
                    });
                }
            }
        }
    }
    Ok(json!({"found": false}))
}

/// Returns the immediate neighbors of a node with the same filtering rules as
/// `traverse`, without materializing a traversal tree.
pub fn neighbors(view: &dyn GraphView, options: &NeighborsOptions) -> Result<Value> {
    options.validate()?;
    let as_of = as_of_ms(options.as_of.as_ref())?;
    let id = node_key(&options.start)?;
    let Some(start_node) = view.node(&id)? else {
        return Ok(json!({
            "node": id,
            "missing": [options.start],
            "edges": [],
            "nodes": 0,
            "truncated": false,
        }));
    };
    if !node_matches(&start_node, &options.node_filter) {
        return Ok(json!({
            "node": id,
            "edges": [],
            "nodes": 0,
            "truncated": false,
        }));
    }
    let mut items = Vec::new();
    let mut missing = Vec::new();
    let mut seen = HashSet::new();
    let mut scanned = 0usize;
    let mut truncated = false;
    for edge in view.neighbors(&id, options.direction, &options.labels)? {
        scanned += 1;
        if scanned > DEFAULT_GRAPH_EDGES {
            truncated = true;
            break;
        }
        if !edge_in_window(&edge, as_of) || !edge_matches(view, &edge, &options.edge_filter)? {
            continue;
        }
        if items.len() >= options.limit {
            truncated = true;
            break;
        }
        let Some(node) = view.node(&edge.target)? else {
            missing.push(Value::String(edge.target));
            continue;
        };
        if !node_matches(&node, &options.node_filter) {
            continue;
        }
        let node = project_node(node, options.projection.as_ref())?;
        let via = via_value(view, &edge, options.include_edges)?;
        if seen.insert(edge.edge_id.clone()) {
            items.push(json!({"node": node, "via": via}));
        }
    }
    Ok(json!({
        "node": id,
        "edges": items,
        "nodes": items.len(),
        "truncated": truncated,
        "missing": missing,
    }))
}

fn vector_score(query: &[f32], node: &Value, field: &str, metric: VectorMetric) -> Option<f64> {
    let raw = path::get(node, field)?;
    let values = raw.as_array()?;
    if values.len() != query.len() {
        return None;
    }
    let mut dot = 0.0f64;
    let mut query_norm = 0.0f64;
    let mut node_norm = 0.0f64;
    let mut squared_distance = 0.0f64;
    for (q, value) in query.iter().zip(values) {
        let value = value.as_f64()?;
        if !value.is_finite() {
            return None;
        }
        let q = f64::from(*q);
        dot += q * value;
        query_norm += q * q;
        node_norm += value * value;
        squared_distance += (q - value) * (q - value);
    }
    Some(match metric {
        VectorMetric::Cosine => {
            if query_norm <= f64::EPSILON || node_norm <= f64::EPSILON {
                0.5
            } else {
                ((dot / (query_norm.sqrt() * node_norm.sqrt())).clamp(-1.0, 1.0) + 1.0) / 2.0
            }
        }
        VectorMetric::Dot => {
            // Numerically stable logistic transform of an unbounded dot product.
            if dot >= 0.0 {
                1.0 / (1.0 + (-dot).exp())
            } else {
                let exp = dot.exp();
                exp / (1.0 + exp)
            }
        }
        VectorMetric::L2 => 1.0 / (1.0 + squared_distance),
    })
}

/// Hybrid vector seed + graph expansion rerank.
///
/// `seed_ids` are the vector-search hits in rank order; the graph expands from
/// them up to `max_depth`. Every candidate receives a vector relevance in
/// `[0, 1]` using the index metric and a graph proximity score
/// `1 / (1 + depth)`. The final score is a normalized weighted sum, with
/// deterministic `_id` tie-breaking.
pub fn hybrid(
    view: &dyn GraphView,
    options: &HybridOptions,
    seed_ids: &[String],
    metric: VectorMetric,
) -> Result<Value> {
    options.validate()?;
    let as_of = as_of_ms(options.as_of.as_ref())?;
    let total_weight = options.vector_weight + options.graph_weight;
    let vector_weight = options.vector_weight / total_weight;
    let graph_weight = options.graph_weight / total_weight;
    let mut depth: HashMap<String, (usize, String)> = HashMap::new();
    let mut queue: VecDeque<(String, usize, String)> = VecDeque::new();
    for id in seed_ids {
        if depth.contains_key(id) {
            continue;
        }
        let Some(node) = view.node(id)? else {
            continue;
        };
        if !node_matches(&node, &options.node_filter) {
            continue;
        }
        depth.insert(id.clone(), (0, id.clone()));
        queue.push_back((id.clone(), 0, id.clone()));
    }
    let mut edges = 0usize;
    let mut truncated = false;
    while let Some((current, current_depth, seed)) = queue.pop_front() {
        if current_depth >= options.max_depth {
            continue;
        }
        for edge in view.neighbors(&current, options.direction, &options.labels)? {
            if edges >= options.max_edges {
                truncated = true;
                break;
            }
            edges += 1;
            if !edge_in_window(&edge, as_of) || !edge_matches(view, &edge, &options.edge_filter)? {
                continue;
            }
            if depth.contains_key(&edge.target) {
                continue;
            }
            if depth.len() >= options.max_nodes {
                truncated = true;
                break;
            }
            let Some(node) = view.node(&edge.target)? else {
                continue;
            };
            if !node_matches(&node, &options.node_filter) {
                continue;
            }
            depth.insert(edge.target.clone(), (current_depth + 1, seed.clone()));
            queue.push_back((edge.target, current_depth + 1, seed.clone()));
        }
    }
    let mut scored = Vec::with_capacity(depth.len());
    for (id, (at_depth, seed)) in depth {
        let Some(node) = view.node(&id)? else {
            continue;
        };
        let vector_score =
            vector_score(&options.vector.vector, &node, &options.vector.field, metric)
                .unwrap_or(0.0);
        let graph_score = 1.0 / (1.0 + at_depth as f64);
        let score = vector_weight * vector_score + graph_weight * graph_score;
        scored.push((
            id,
            seed,
            at_depth,
            vector_score,
            graph_score,
            score,
            project_node(node, options.projection.as_ref())?,
        ));
    }
    scored.sort_by(|a, b| {
        b.5.partial_cmp(&a.5)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    let total = scored.len();
    if total > options.limit {
        truncated = true;
        scored.truncate(options.limit);
    }
    let items: Vec<Value> = scored
        .into_iter()
        .map(
            |(id, seed, at_depth, vector_score, graph_score, score, node)| {
                json!({
                    "id": id,
                    "seed": seed,
                    "depth": at_depth,
                    "score": score,
                    "vector_score": vector_score,
                    "graph_score": graph_score,
                    "node": node,
                })
            },
        )
        .collect();
    Ok(json!({
        "items": items,
        "nodes": total,
        "edges": edges,
        "truncated": truncated,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct MockView {
        edges: HashMap<String, Vec<AdjEdge>>,
    }

    impl MockView {
        fn edge(id: &str, _from: &str, to: &str, label: &str, weight: Option<f64>) -> AdjEdge {
            AdjEdge {
                edge_id: id.into(),
                label: label.into(),
                weight,
                target: to.into(),
                valid_from: None,
                valid_to: None,
            }
        }
    }

    impl GraphView for MockView {
        fn node(&self, id: &str) -> Result<Option<Value>> {
            if id == "missing" {
                return Ok(None);
            }
            Ok(Some(json!({"_id": id})))
        }

        fn edge(&self, id: &str) -> Result<Option<Value>> {
            Ok(Some(json!({"_id": id})))
        }

        fn neighbors(
            &self,
            id: &str,
            _direction: GraphDirection,
            labels: &[String],
        ) -> Result<Vec<AdjEdge>> {
            Ok(self
                .edges
                .get(id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|edge| labels.is_empty() || labels.contains(&edge.label))
                .collect())
        }
    }

    fn view() -> MockView {
        let mut edges = HashMap::new();
        edges.insert(
            "a".into(),
            vec![
                MockView::edge("e1", "a", "b", "KNOWS", Some(1.0)),
                MockView::edge("e2", "a", "c", "KNOWS", Some(10.0)),
            ],
        );
        edges.insert(
            "b".into(),
            vec![MockView::edge("e3", "b", "c", "KNOWS", Some(1.0))],
        );
        edges.insert(
            "c".into(),
            vec![MockView::edge("e4", "c", "a", "BACK", None)],
        );
        MockView { edges }
    }

    #[test]
    fn adjacency_key_roundtrip() {
        let key = adjacency_key("a", "KNOWS", "b", "e1").unwrap();
        assert_eq!(decode_adjacency_key(&key), Some(("a", "KNOWS", "b", "e1")));
        assert!(key.starts_with(&label_prefix("a", "KNOWS")));
    }

    #[test]
    fn graph_def_validation_rejects_bad_fields() {
        let mut def = GraphDef {
            node_collection: "nodes".into(),
            edge_collection: "edges".into(),
            from_field: "from".into(),
            to_field: "to".into(),
            label_field: "label".into(),
            weight_field: "weight".into(),
            valid_from_field: String::new(),
            valid_to_field: String::new(),
            directed: true,
            generation: 0,
        };
        assert!(def.validate().is_ok());
        def.to_field = "from".into();
        assert!(def.validate().is_err());
    }

    #[test]
    fn traverse_builds_tree_and_marks_cycles() {
        let options = TraverseOptions {
            start: vec![json!("a")],
            seed: None,
            direction: GraphDirection::Out,
            labels: Vec::new(),
            edge_filter: None,
            node_filter: None,
            as_of: None,
            max_depth: 4,
            limit_nodes: 100,
            limit_edges: 100,
            max_bytes: 1_000_000,
            tree: true,
            include_edges: false,
            algorithm: TraverseAlgorithm::Bfs,
            projection: None,
        };
        let out = traverse(&view(), &options).unwrap();
        assert_eq!(out["nodes"], 5);
        assert_eq!(out["edges"], 4);
        assert_eq!(out["truncated"], false);
        let roots = out["roots"].as_array().unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0]["node"]["_id"], "a");
        assert!(out.to_string().contains("\"cycle\":true"));
        let limited = TraverseOptions {
            limit_nodes: 3,
            ..TraverseOptions {
                start: vec![json!("a")],
                seed: None,
                direction: GraphDirection::Out,
                labels: Vec::new(),
                edge_filter: None,
                node_filter: None,
                as_of: None,
                max_depth: 4,
                limit_nodes: 100,
                limit_edges: 100,
                max_bytes: 1_000_000,
                tree: true,
                include_edges: false,
                algorithm: TraverseAlgorithm::Bfs,
                projection: None,
            }
        };
        let out = traverse(&view(), &limited).unwrap();
        assert_eq!(out["nodes"], 3);
        assert_eq!(out["truncated"], true);

        let tiny = TraverseOptions {
            max_bytes: 1,
            ..limited
        };
        let out = traverse(&view(), &tiny).unwrap();
        assert_eq!(out["nodes"], 0);
        assert_eq!(out["truncated"], true);
    }

    #[test]
    fn traverse_respects_depth_and_edge_budget() {
        let mut options = TraverseOptions {
            start: vec![json!("a")],
            seed: None,
            direction: GraphDirection::Out,
            labels: Vec::new(),
            edge_filter: None,
            node_filter: None,
            as_of: None,
            max_depth: 1,
            limit_nodes: 100,
            limit_edges: 100,
            max_bytes: 1_000_000,
            tree: false,
            include_edges: true,
            algorithm: TraverseAlgorithm::Dfs,
            projection: None,
        };
        let out = traverse(&view(), &options).unwrap();
        assert_eq!(out["items"].as_array().unwrap().len(), 3);
        assert_eq!(out["edges"], 2);

        options.max_depth = 2;
        options.limit_edges = 1;
        let out = traverse(&view(), &options).unwrap();
        assert_eq!(out["truncated"], true);
        assert_eq!(out["edges"], 1);
    }

    #[test]
    fn traverse_filters_labels_and_handles_missing_start() {
        let options = TraverseOptions {
            start: vec![json!("a"), json!("missing")],
            seed: None,
            direction: GraphDirection::Out,
            labels: vec!["BACK".into()],
            edge_filter: None,
            node_filter: None,
            as_of: None,
            max_depth: 2,
            limit_nodes: 100,
            limit_edges: 100,
            max_bytes: 1_000_000,
            tree: true,
            include_edges: false,
            algorithm: TraverseAlgorithm::Bfs,
            projection: None,
        };
        let out = traverse(&view(), &options).unwrap();
        assert_eq!(out["edges"], 0);
        assert_eq!(out["missing"], json!(["missing"]));
    }

    #[test]
    fn adjacency_value_is_compact_and_roundtrips() {
        let value = AdjacencyValue {
            edge_id: "e1".into(),
            label: "KNOWS".into(),
            weight: Some(0.5),
            valid_from: Some(1000),
            valid_to: Some(2000),
        };
        let encoded = encode_adjacency_value(&value).unwrap();
        assert_eq!(decode_adjacency_value(&encoded).unwrap(), value);
        let edge = json!({"_id":"e1","from":"a","to":"b","label":"KNOWS","weight":0.5,
            "valid_from":{"$date":{"$numberLong":"1000"}},
            "valid_to":{"$date":{"$numberLong":"2000"}},"padding":"x".repeat(200)});
        assert!(encoded.len() < serde_json::to_vec(&edge).unwrap().len());
    }

    #[test]
    fn projection_keeps_id_and_removes_other_fields() {
        let node = json!({"_id":"a","name":"A","secret":"x"});
        let out = project_node(node, Some(&json!({"name":1}))).unwrap();
        assert_eq!(out["_id"], "a");
        assert_eq!(out["name"], "A");
        assert!(out.get("secret").is_none());
    }

    #[test]
    fn hybrid_vector_score_honors_index_metric() {
        let node = json!({"_id":"a","embedding":[1.0,0.0]});
        let query = [1.0f32, 0.0];
        assert_eq!(
            vector_score(&query, &node, "embedding", VectorMetric::Cosine).unwrap(),
            1.0
        );
        assert!(
            (vector_score(&query, &node, "embedding", VectorMetric::Dot).unwrap()
                - 1.0 / (1.0 + (-1.0f64).exp()))
            .abs()
                < 1e-12
        );
        assert_eq!(
            vector_score(&query, &node, "embedding", VectorMetric::L2).unwrap(),
            1.0
        );
        let orthogonal = json!({"_id":"b","embedding":[0.0,1.0]});
        assert_eq!(
            vector_score(&query, &orthogonal, "embedding", VectorMetric::L2).unwrap(),
            1.0 / 3.0
        );
        assert!(vector_score(&query, &orthogonal, "embedding", VectorMetric::Cosine).is_some());
        assert!(vector_score(
            &query,
            &json!({"_id":"c","embedding":[1.0]}),
            "embedding",
            VectorMetric::L2
        )
        .is_none());
    }

    #[test]
    fn shortest_path_bfs_and_dijkstra() {
        let options = ShortestPathOptions {
            start: json!("a"),
            target: json!("c"),
            direction: GraphDirection::Out,
            labels: Vec::new(),
            edge_filter: None,
            node_filter: None,
            as_of: None,
            max_depth: 4,
            limit_nodes: 100,
            weighted: false,
            projection: None,
        };
        let out = shortest_path(&view(), &options).unwrap();
        assert_eq!(out["found"], true);
        assert_eq!(out["hops"], 1);
        assert_eq!(out["weight"], 1.0);

        let weighted = ShortestPathOptions {
            weighted: true,
            ..options
        };
        let out = shortest_path(&view(), &weighted).unwrap();
        assert_eq!(out["weight"], 2.0);
        assert_eq!(out["path"].as_array().unwrap().len(), 3);
    }
}
