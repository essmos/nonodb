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
use std::time::Instant;

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
/// Maximum accepted node budget for graph operations.
pub const MAX_GRAPH_NODES: usize = 100_000;
/// Maximum accepted edge/adjacency budget for graph operations.
pub const MAX_GRAPH_EDGES: usize = 100_000;
/// Maximum accepted serialized result budget (256 MiB).
pub const MAX_GRAPH_BYTES: usize = 256 * 1024 * 1024;
/// Maximum number of labels/types accepted by one graph request.
pub const MAX_GRAPH_LABELS: usize = 64;
/// Maximum byte length of one label/type.
pub const MAX_GRAPH_LABEL_LEN: usize = 256;
/// Maximum accepted adjacency generation; rebuilds stop with an explicit error
/// once this is exceeded.
pub const MAX_GRAPH_GENERATION: u64 = 1_024;
/// Default traversal depth.
pub const DEFAULT_GRAPH_DEPTH: usize = 3;
/// Default node budget.
pub const DEFAULT_GRAPH_NODES: usize = 1000;
/// Default edge budget.
pub const DEFAULT_GRAPH_EDGES: usize = 10_000;
/// Default serialized result budget (64 MiB), matching lookup.
pub const DEFAULT_GRAPH_BYTES: usize = 64 * 1024 * 1024;
/// Hard limit for one graph algorithm run.
pub const MAX_ALGORITHM_NODES: usize = 1_000_000;
/// Hard limit for adjacency entries inspected by one graph algorithm run.
pub const MAX_ALGORITHM_EDGES: usize = 10_000_000;
/// Hard limit for graph algorithm working memory (1 GiB).
pub const MAX_ALGORITHM_MEMORY: usize = 1024 * 1024 * 1024;
/// Default graph algorithm node budget.
pub const DEFAULT_ALGORITHM_NODES: usize = 100_000;
/// Default graph algorithm adjacency-entry budget.
pub const DEFAULT_ALGORITHM_EDGES: usize = 1_000_000;
/// Default graph algorithm working-memory budget (256 MiB).
pub const DEFAULT_ALGORITHM_MEMORY: usize = 256 * 1024 * 1024;

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
        if self.generation > MAX_GRAPH_GENERATION {
            return Err(Error::Query(format!(
                "graph generation must be at most {MAX_GRAPH_GENERATION}"
            )));
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

fn validate_graph_labels(labels: &[String]) -> Result<()> {
    if labels.len() > MAX_GRAPH_LABELS {
        return Err(Error::Query(format!(
            "graph labels must contain at most {MAX_GRAPH_LABELS} entries"
        )));
    }
    for label in labels {
        if label.is_empty() || label.len() > MAX_GRAPH_LABEL_LEN || label.contains('\0') {
            return Err(Error::Query(format!(
                "graph label must be 1..={MAX_GRAPH_LABEL_LEN} bytes"
            )));
        }
    }
    Ok(())
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
        if self.limit_nodes == 0
            || self.limit_nodes > MAX_GRAPH_NODES
            || self.limit_edges == 0
            || self.limit_edges > MAX_GRAPH_EDGES
            || self.max_bytes == 0
            || self.max_bytes > MAX_GRAPH_BYTES
        {
            return Err(Error::Query(format!(
                "traverse limits must be between 1 and ({MAX_GRAPH_NODES} nodes, {MAX_GRAPH_EDGES} edges, {MAX_GRAPH_BYTES} bytes)"
            )));
        }
        validate_graph_labels(&self.labels)?;
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
        if self.limit_nodes == 0 || self.limit_nodes > MAX_GRAPH_NODES {
            return Err(Error::Query(format!(
                "shortest_path `limit_nodes` must be between 1 and {MAX_GRAPH_NODES}"
            )));
        }
        validate_graph_labels(&self.labels)?;
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
        validate_graph_labels(&self.labels)?;
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
        validate_graph_labels(&self.labels)?;
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AllShortestPathsOptions {
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
    #[serde(default = "default_shortest_depth")]
    pub max_depth: usize,
    #[serde(default = "default_all_paths")]
    pub limit_paths: usize,
    #[serde(default = "default_algo_nodes")]
    pub limit_nodes: usize,
    #[serde(default)]
    pub weighted: bool,
    #[serde(default)]
    pub include_edges: bool,
    #[serde(default)]
    pub projection: Option<Value>,
}

fn default_shortest_depth() -> usize {
    8
}
fn default_all_paths() -> usize {
    100
}
fn default_algo_nodes() -> usize {
    10_000
}

impl AllShortestPathsOptions {
    pub fn validate(&self) -> Result<()> {
        validate_graph_labels(&self.labels)?;
        if self.max_depth > MAX_GRAPH_DEPTH {
            return Err(Error::Query(format!(
                "all_shortest_paths `max_depth` must be at most {MAX_GRAPH_DEPTH}"
            )));
        }
        if self.limit_paths == 0 || self.limit_paths > 1000 {
            return Err(Error::Query(
                "all_shortest_paths `limit_paths` must be between 1 and 1000".into(),
            ));
        }
        if self.limit_nodes == 0 || self.limit_nodes > 100_000 {
            return Err(Error::Query(
                "all_shortest_paths `limit_nodes` must be between 1 and 100000".into(),
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
pub struct MatchStep {
    #[serde(default)]
    pub direction: GraphDirection,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default = "default_one")]
    pub min_hops: usize,
    #[serde(default = "default_one")]
    pub max_hops: usize,
    #[serde(default)]
    pub edge_filter: Option<Value>,
    #[serde(default)]
    pub node_filter: Option<Value>,
}

fn default_one() -> usize {
    1
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GraphMatchOptions {
    pub start: Value,
    pub pattern: Vec<MatchStep>,
    #[serde(default)]
    pub node_filter: Option<Value>,
    #[serde(default)]
    pub as_of: Option<Value>,
    #[serde(default)]
    pub projection: Option<Value>,
    #[serde(default)]
    pub include_edges: bool,
    #[serde(default = "default_match_limit")]
    pub limit: usize,
    #[serde(default = "default_algo_nodes")]
    pub limit_nodes: usize,
    #[serde(default = "default_edges")]
    pub limit_edges: usize,
    #[serde(default = "default_shortest_depth")]
    pub max_depth: usize,
}

fn default_match_limit() -> usize {
    100
}

impl GraphMatchOptions {
    pub fn validate(&self) -> Result<()> {
        if self.pattern.len() > 16 {
            return Err(Error::Query(
                "graph_match supports at most 16 pattern steps".into(),
            ));
        }
        if self.max_depth > MAX_GRAPH_DEPTH {
            return Err(Error::Query(format!(
                "graph_match `max_depth` must be at most {MAX_GRAPH_DEPTH}"
            )));
        }
        if self.limit == 0 || self.limit > 1000 {
            return Err(Error::Query(
                "graph_match `limit` must be between 1 and 1000".into(),
            ));
        }
        if self.limit_nodes == 0 || self.limit_nodes > 100_000 {
            return Err(Error::Query(
                "graph_match `limit_nodes` must be between 1 and 100000".into(),
            ));
        }
        if self.limit_edges == 0 || self.limit_edges > 100_000 {
            return Err(Error::Query(
                "graph_match `limit_edges` must be between 1 and 100000".into(),
            ));
        }
        for step in &self.pattern {
            validate_graph_labels(&step.labels)?;
            if step.min_hops > step.max_hops {
                return Err(Error::Query(
                    "graph_match step `min_hops` must not exceed `max_hops`".into(),
                ));
            }
            if step.max_hops > MAX_GRAPH_DEPTH {
                return Err(Error::Query(format!(
                    "graph_match step `max_hops` must be at most {MAX_GRAPH_DEPTH}"
                )));
            }
            if let Some(filter) = &step.edge_filter {
                validate_filter(filter)?;
            }
            if let Some(filter) = &step.node_filter {
                validate_filter(filter)?;
            }
        }
        if let Some(filter) = &self.node_filter {
            validate_filter(filter)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GraphAlgorithmKind {
    Pagerank,
    DegreeCentrality,
    ConnectedComponents,
    LabelPropagation,
    KCore,
    DegreeDistribution,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GraphAlgorithmOptions {
    pub kind: GraphAlgorithmKind,
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
    #[serde(default = "default_match_limit")]
    pub limit: usize,
    #[serde(default = "default_algorithm_nodes")]
    pub max_nodes: usize,
    #[serde(default = "default_algorithm_edges")]
    pub max_edges: usize,
    #[serde(default = "default_algorithm_memory")]
    pub max_memory_bytes: usize,
    #[serde(default = "default_damping")]
    pub damping: f64,
    #[serde(default = "default_iterations")]
    pub iterations: usize,
    #[serde(default = "default_tolerance")]
    pub tolerance: f64,
    #[serde(default)]
    pub projection: Option<Value>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

fn default_algorithm_nodes() -> usize {
    DEFAULT_ALGORITHM_NODES
}
fn default_algorithm_edges() -> usize {
    DEFAULT_ALGORITHM_EDGES
}
fn default_algorithm_memory() -> usize {
    DEFAULT_ALGORITHM_MEMORY
}
fn default_damping() -> f64 {
    0.85
}
fn default_iterations() -> usize {
    20
}
fn default_tolerance() -> f64 {
    1e-6
}

impl GraphAlgorithmOptions {
    pub fn validate(&self) -> Result<()> {
        validate_graph_labels(&self.labels)?;
        if self.limit == 0 || self.limit > MAX_ALGORITHM_NODES {
            return Err(Error::Query(format!(
                "graph algorithm `limit` must be between 1 and {MAX_ALGORITHM_NODES}"
            )));
        }
        if self.max_nodes == 0 || self.max_nodes > MAX_ALGORITHM_NODES {
            return Err(Error::Query(format!(
                "graph algorithm `max_nodes` must be between 1 and {MAX_ALGORITHM_NODES}"
            )));
        }
        if self.max_edges == 0 || self.max_edges > MAX_ALGORITHM_EDGES {
            return Err(Error::Query(format!(
                "graph algorithm `max_edges` must be between 1 and {MAX_ALGORITHM_EDGES}"
            )));
        }
        if self.max_memory_bytes == 0 || self.max_memory_bytes > MAX_ALGORITHM_MEMORY {
            return Err(Error::Query(format!(
                "graph algorithm `max_memory_bytes` must be between 1 and {MAX_ALGORITHM_MEMORY}"
            )));
        }
        if self.timeout_ms == Some(0) {
            return Err(Error::Query(
                "graph algorithm `timeout_ms` must be positive".into(),
            ));
        }
        if !self.damping.is_finite() || !(0.0..1.0).contains(&self.damping) {
            return Err(Error::Query(
                "graph algorithm `damping` must be in [0, 1)".into(),
            ));
        }
        if self.iterations == 0 || self.iterations > 100 {
            return Err(Error::Query(
                "graph algorithm `iterations` must be between 1 and 100".into(),
            ));
        }
        if !self.tolerance.is_finite() || self.tolerance <= 0.0 {
            return Err(Error::Query(
                "graph algorithm `tolerance` must be positive".into(),
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

    /// All node ids in the node collection. Used by graph algorithms.
    fn node_ids(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
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

fn all_paths_result(
    view: &dyn GraphView,
    start: &str,
    steps: &[AdjEdge],
    weight: f64,
    projection: Option<&Value>,
    include_edges: bool,
) -> Result<Value> {
    let mut ids = Vec::with_capacity(steps.len() + 1);
    ids.push(start.to_string());
    ids.extend(steps.iter().map(|edge| edge.target.clone()));
    let mut path = Vec::with_capacity(ids.len());
    for (index, id) in ids.iter().enumerate() {
        let node = view
            .node(id)?
            .unwrap_or_else(|| json!({"_id": id, "missing": true}));
        let node = project_node(node, projection)?;
        let mut item = json!({"node": node});
        if let Some(edge) = steps.get(index) {
            item["via"] = via_value(view, edge, include_edges)?;
        }
        path.push(item);
    }
    Ok(json!({
        "path": path,
        "weight": weight,
        "hops": steps.len(),
    }))
}

fn collect_shortest_steps(
    target: &str,
    start: &str,
    preds: &HashMap<String, Vec<(String, AdjEdge)>>,
    limit: usize,
) -> (Vec<Vec<AdjEdge>>, bool) {
    #[allow(clippy::too_many_arguments)]
    fn walk(
        current: &str,
        start: &str,
        preds: &HashMap<String, Vec<(String, AdjEdge)>>,
        steps: &mut Vec<AdjEdge>,
        visited: &mut HashSet<String>,
        out: &mut Vec<Vec<AdjEdge>>,
        limit: usize,
        truncated: &mut bool,
    ) {
        if out.len() >= limit {
            *truncated = true;
            return;
        }
        if current == start {
            let mut ordered = steps.clone();
            ordered.reverse();
            out.push(ordered);
            return;
        }
        if !visited.insert(current.to_string()) {
            return;
        }
        if let Some(parents) = preds.get(current) {
            for (parent, edge) in parents {
                if out.len() >= limit {
                    *truncated = true;
                    break;
                }
                steps.push(edge.clone());
                walk(parent, start, preds, steps, visited, out, limit, truncated);
                steps.pop();
            }
        }
        visited.remove(current);
    }

    let mut out = Vec::new();
    let mut steps = Vec::new();
    let mut visited = HashSet::new();
    let mut truncated = false;
    walk(
        target,
        start,
        preds,
        &mut steps,
        &mut visited,
        &mut out,
        limit,
        &mut truncated,
    );
    (out, truncated)
}

/// Returns every shortest path between two nodes. Unweighted mode uses BFS and
/// reports hop distance; weighted mode uses Dijkstra and requires numeric edge
/// weights. The result is capped by `limit_paths` and `limit_nodes`.
pub fn all_shortest_paths(
    view: &dyn GraphView,
    options: &AllShortestPathsOptions,
) -> Result<Value> {
    options.validate()?;
    let as_of = as_of_ms(options.as_of.as_ref())?;
    let start = node_key(&options.start)?;
    let target = node_key(&options.target)?;
    if active_filter(&options.node_filter) {
        match view.node(&start)? {
            Some(node) if node_matches(&node, &options.node_filter) => {}
            _ => {
                return Ok(json!({
                    "found": false,
                    "paths": [],
                    "path_count": 0,
                    "truncated": false,
                }))
            }
        }
    }
    if start == target {
        let one = all_paths_result(
            view,
            &start,
            &[],
            0.0,
            options.projection.as_ref(),
            options.include_edges,
        )?;
        return Ok(json!({
            "found": true,
            "paths": [one],
            "path_count": 1,
            "truncated": false,
        }));
    }

    let mut preds: HashMap<String, Vec<(String, AdjEdge)>> = HashMap::new();
    let mut truncated = false;
    let path_weight = if !options.weighted {
        let mut dist: HashMap<String, usize> = HashMap::new();
        let mut queue: VecDeque<(String, usize)> = VecDeque::new();
        dist.insert(start.clone(), 0);
        queue.push_back((start.clone(), 0));
        let mut target_depth: Option<usize> = None;
        while let Some((current, depth)) = queue.pop_front() {
            if depth >= options.max_depth {
                continue;
            }
            if target_depth.is_some_and(|target_depth| depth >= target_depth) {
                continue;
            }
            for edge in view.neighbors(&current, options.direction, &options.labels)? {
                if !edge_in_window(&edge, as_of)
                    || !edge_matches(view, &edge, &options.edge_filter)?
                {
                    continue;
                }
                let next_depth = depth + 1;
                if next_depth > options.max_depth
                    || target_depth.is_some_and(|target_depth| next_depth > target_depth)
                {
                    continue;
                }
                match view.node(&edge.target)? {
                    Some(node) if node_matches(&node, &options.node_filter) => {}
                    _ => continue,
                }
                let known = dist.get(&edge.target).copied();
                match known {
                    None => {
                        if dist.len() >= options.limit_nodes {
                            truncated = true;
                            continue;
                        }
                        dist.insert(edge.target.clone(), next_depth);
                        preds
                            .entry(edge.target.clone())
                            .or_default()
                            .push((current.clone(), edge.clone()));
                        queue.push_back((edge.target.clone(), next_depth));
                    }
                    Some(known) if known == next_depth => {
                        preds
                            .entry(edge.target.clone())
                            .or_default()
                            .push((current.clone(), edge.clone()));
                    }
                    Some(_) => continue,
                }
                if edge.target == target {
                    target_depth = Some(next_depth);
                }
            }
        }
        if !dist.contains_key(&target) {
            return Ok(json!({
                "found": false,
                "paths": [],
                "path_count": 0,
                "truncated": truncated,
            }));
        }
        dist[&target] as f64
    } else {
        let mut best: HashMap<String, f64> = HashMap::new();
        let mut heap = BinaryHeap::new();
        let mut settled: HashSet<String> = HashSet::new();
        best.insert(start.clone(), 0.0);
        heap.push(QueueEntry {
            distance: 0.0,
            hops: 0,
            id: start.clone(),
        });
        let mut popped = 0usize;
        while let Some(entry) = heap.pop() {
            if settled.contains(&entry.id) {
                continue;
            }
            if entry
                .distance
                .partial_cmp(best.get(&entry.id).unwrap_or(&f64::INFINITY))
                != Some(Ordering::Equal)
            {
                continue;
            }
            if let Some(target_best) = best.get(&target).copied() {
                if entry.distance > target_best {
                    break;
                }
            }
            settled.insert(entry.id.clone());
            popped += 1;
            if popped > options.limit_nodes {
                truncated = true;
                break;
            }
            if entry.id == target {
                continue;
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
                if let Some(target_best) = best.get(&target).copied() {
                    if distance > target_best {
                        continue;
                    }
                }
                match best.get(&edge.target).copied() {
                    None => {
                        best.insert(edge.target.clone(), distance);
                        preds
                            .entry(edge.target.clone())
                            .or_default()
                            .push((entry.id.clone(), edge.clone()));
                        heap.push(QueueEntry {
                            distance,
                            hops: entry.hops + 1,
                            id: edge.target.clone(),
                        });
                    }
                    Some(known) if distance < known => {
                        best.insert(edge.target.clone(), distance);
                        preds
                            .entry(edge.target.clone())
                            .or_default()
                            .retain(|_| false);
                        preds
                            .entry(edge.target.clone())
                            .or_default()
                            .push((entry.id.clone(), edge.clone()));
                        heap.push(QueueEntry {
                            distance,
                            hops: entry.hops + 1,
                            id: edge.target.clone(),
                        });
                    }
                    Some(known) if distance == known => {
                        preds
                            .entry(edge.target.clone())
                            .or_default()
                            .push((entry.id.clone(), edge.clone()));
                    }
                    Some(_) => {}
                }
            }
        }
        let Some(weight) = best.get(&target).copied() else {
            return Ok(json!({
                "found": false,
                "paths": [],
                "path_count": 0,
                "truncated": truncated,
            }));
        };
        weight
    };

    let (all_steps, paths_truncated) =
        collect_shortest_steps(&target, &start, &preds, options.limit_paths);
    let mut paths = Vec::with_capacity(all_steps.len());
    for steps in all_steps {
        paths.push(all_paths_result(
            view,
            &start,
            &steps,
            path_weight,
            options.projection.as_ref(),
            options.include_edges,
        )?);
    }
    Ok(json!({
        "found": true,
        "paths": paths,
        "path_count": paths.len(),
        "weight": path_weight,
        "truncated": truncated || paths_truncated,
    }))
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
struct MatchState<'a> {
    view: &'a dyn GraphView,
    options: &'a GraphMatchOptions,
    as_of: Option<i64>,
    chain: Vec<(String, Option<AdjEdge>)>,
    used_edges: HashSet<String>,
    paths: Vec<Value>,
    truncated: bool,
    edges: usize,
    expansions: usize,
}

impl MatchState<'_> {
    fn walk_step(&mut self, step_index: usize) -> Result<()> {
        if self.expansions >= self.options.limit_nodes {
            self.truncated = true;
            return Ok(());
        }
        self.expansions += 1;
        if step_index == self.options.pattern.len() {
            return self.emit();
        }
        self.expand(step_index, 0)
    }

    fn expand(&mut self, step_index: usize, hops: usize) -> Result<()> {
        if self.paths.len() >= self.options.limit {
            self.truncated = true;
            return Ok(());
        }
        let step = self.options.pattern[step_index].clone();
        if hops >= step.min_hops {
            self.walk_step(step_index + 1)?;
            if self.paths.len() >= self.options.limit {
                self.truncated = true;
                return Ok(());
            }
        }
        if hops >= step.max_hops || self.chain.len().saturating_sub(1) >= self.options.max_depth {
            return Ok(());
        }
        let current = self
            .chain
            .last()
            .map(|(id, _)| id.clone())
            .unwrap_or_default();
        for edge in self
            .view
            .neighbors(&current, step.direction, &step.labels)?
        {
            if self.edges >= self.options.limit_edges {
                self.truncated = true;
                return Ok(());
            }
            self.edges += 1;
            if !edge_in_window(&edge, self.as_of)
                || !edge_matches(self.view, &edge, &step.edge_filter)?
            {
                continue;
            }
            if self.used_edges.contains(&edge.edge_id) {
                continue;
            }
            let Some(node) = self.view.node(&edge.target)? else {
                continue;
            };
            if !node_matches(&node, &step.node_filter) {
                continue;
            }
            self.used_edges.insert(edge.edge_id.clone());
            self.chain.push((edge.target.clone(), Some(edge.clone())));
            self.expand(step_index, hops + 1)?;
            self.chain.pop();
            self.used_edges.remove(&edge.edge_id);
            if self.paths.len() >= self.options.limit {
                self.truncated = true;
                return Ok(());
            }
        }
        Ok(())
    }

    fn emit(&mut self) -> Result<()> {
        if self.paths.len() >= self.options.limit {
            self.truncated = true;
            return Ok(());
        }
        let mut nodes = Vec::with_capacity(self.chain.len());
        let mut edges = Vec::with_capacity(self.chain.len().saturating_sub(1));
        for (id, edge) in &self.chain {
            let node = self
                .view
                .node(id)?
                .unwrap_or_else(|| json!({"_id": id, "missing": true}));
            nodes.push(project_node(node, self.options.projection.as_ref())?);
            if let Some(edge) = edge {
                edges.push(via_value(self.view, edge, self.options.include_edges)?);
            }
        }
        self.paths.push(json!({
            "nodes": nodes,
            "edges": edges,
            "hops": self.chain.len().saturating_sub(1),
        }));
        Ok(())
    }
}

/// Matches a linear graph pattern starting at one node. Each pattern step is a
/// variable-length relationship segment with its own direction, labels, and
/// optional node/edge filters. Relationship uniqueness is enforced per path.
pub fn graph_match(view: &dyn GraphView, options: &GraphMatchOptions) -> Result<Value> {
    options.validate()?;
    let as_of = as_of_ms(options.as_of.as_ref())?;
    let start = node_key(&options.start)?;
    if active_filter(&options.node_filter) {
        match view.node(&start)? {
            Some(node) if node_matches(&node, &options.node_filter) => {}
            _ => {
                return Ok(json!({
                    "paths": [],
                    "matches": 0,
                    "truncated": false,
                }))
            }
        }
    }
    let mut state = MatchState {
        view,
        options,
        as_of,
        chain: vec![(start, None)],
        used_edges: HashSet::new(),
        paths: Vec::new(),
        truncated: false,
        edges: 0,
        expansions: 0,
    };
    state.walk_step(0)?;
    Ok(json!({
        "paths": state.paths,
        "matches": state.paths.len(),
        "truncated": state.truncated,
    }))
}

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

/// Receives bounded progress updates and cooperative cancellation checks for a
/// graph algorithm run. Implementations must be cheap and non-blocking.
pub trait GraphAlgorithmObserver {
    fn progress(&self, phase: &str, done: usize, total: Option<usize>);
    fn is_cancelled(&self) -> bool {
        false
    }
}

fn check_algorithm_observer(observer: Option<&dyn GraphAlgorithmObserver>) -> Result<()> {
    if observer.is_some_and(GraphAlgorithmObserver::is_cancelled) {
        return Err(Error::Query("graph algorithm cancelled".into()));
    }
    Ok(())
}

fn note_algorithm_progress(
    observer: Option<&dyn GraphAlgorithmObserver>,
    phase: &str,
    done: usize,
    total: Option<usize>,
) {
    if let Some(observer) = observer {
        observer.progress(phase, done, total);
    }
}

fn check_algorithm_memory(options: &GraphAlgorithmOptions, used: usize) -> Result<()> {
    if used > options.max_memory_bytes {
        return Err(Error::Query(format!(
            "graph algorithm memory budget exceeded ({} bytes, limit {})",
            used, options.max_memory_bytes
        )));
    }
    Ok(())
}

fn estimate_ids_memory(ids: &[String]) -> usize {
    ids.iter()
        .map(|id| id.len() + std::mem::size_of::<String>() + 32)
        .sum()
}

fn estimate_adjacency_memory(ids: &[String], adjacency: &HashMap<String, Vec<AdjEdge>>) -> usize {
    estimate_ids_memory(ids)
        + adjacency
            .values()
            .map(|_| 64 + std::mem::size_of::<Vec<AdjEdge>>())
            .sum::<usize>()
        + adjacency
            .values()
            .flatten()
            .map(|edge| {
                std::mem::size_of::<AdjEdge>()
                    + edge.edge_id.len()
                    + edge.label.len()
                    + edge.target.len()
                    + 32
            })
            .sum::<usize>()
}

fn algorithm_nodes(
    view: &dyn GraphView,
    options: &GraphAlgorithmOptions,
    started: Instant,
    observer: Option<&dyn GraphAlgorithmObserver>,
) -> Result<Vec<String>> {
    let mut ids = view.node_ids()?;
    ids.sort();
    ids.dedup();
    let mut id_bytes = ids.len().saturating_mul(std::mem::size_of::<String>() + 24);
    id_bytes = id_bytes.saturating_add(ids.iter().map(String::len).sum::<usize>());
    check_algorithm_memory(options, id_bytes)?;
    let total = ids.len();
    let mut kept = Vec::new();
    let mut scanned = 0usize;
    for id in ids {
        check_algorithm_timeout(options, started)?;
        check_algorithm_observer(observer)?;
        scanned += 1;
        if scanned.is_multiple_of(1024) {
            note_algorithm_progress(observer, "scan_nodes", scanned, Some(total));
        }
        let Some(node) = view.node(&id)? else {
            continue;
        };
        if !node_matches(&node, &options.node_filter) {
            continue;
        }
        if kept.len() >= options.max_nodes {
            return Err(Error::Query(format!(
                "graph algorithm node budget exceeded (limit {})",
                options.max_nodes
            )));
        }
        id_bytes = id_bytes.saturating_add(id.len() + std::mem::size_of::<String>());
        check_algorithm_memory(options, id_bytes)?;
        kept.push(id);
    }
    note_algorithm_progress(observer, "scan_nodes", scanned, Some(total));
    Ok(kept)
}

fn algorithm_adjacency(
    view: &dyn GraphView,
    ids: &[String],
    options: &GraphAlgorithmOptions,
    as_of: Option<i64>,
    direction: GraphDirection,
    started: Instant,
    observer: Option<&dyn GraphAlgorithmObserver>,
) -> Result<HashMap<String, Vec<AdjEdge>>> {
    let allowed: HashSet<&str> = ids.iter().map(String::as_str).collect();
    let mut adjacency: HashMap<String, Vec<AdjEdge>> = HashMap::new();
    let mut used = ids
        .iter()
        .map(|id| id.len() + std::mem::size_of::<String>() + 64)
        .sum::<usize>();
    check_algorithm_memory(options, used)?;
    let mut scanned = 0usize;
    for id in ids {
        for edge in view.neighbors(id, direction, &options.labels)? {
            check_algorithm_timeout(options, started)?;
            check_algorithm_observer(observer)?;
            scanned = scanned.saturating_add(1);
            if scanned.is_multiple_of(1024) {
                note_algorithm_progress(observer, "scan_edges", scanned, None);
            }
            if scanned > options.max_edges {
                return Err(Error::Query(format!(
                    "graph algorithm edge budget exceeded (limit {})",
                    options.max_edges
                )));
            }
            if !edge_in_window(&edge, as_of) || !edge_matches(view, &edge, &options.edge_filter)? {
                continue;
            }
            if !allowed.contains(edge.target.as_str()) {
                continue;
            }
            used = used.saturating_add(
                std::mem::size_of::<AdjEdge>()
                    + edge.edge_id.len()
                    + edge.label.len()
                    + edge.target.len()
                    + 32,
            );
            check_algorithm_memory(options, used)?;
            adjacency.entry(id.clone()).or_default().push(edge);
        }
    }
    Ok(adjacency)
}

fn pagerank(
    view: &dyn GraphView,
    options: &GraphAlgorithmOptions,
    observer: Option<&dyn GraphAlgorithmObserver>,
) -> Result<Value> {
    let started = Instant::now();
    let as_of = as_of_ms(options.as_of.as_ref())?;
    let ids = algorithm_nodes(view, options, started, observer)?;
    if ids.is_empty() {
        return Ok(json!({
            "kind": "pagerank",
            "items": [],
            "nodes": 0,
            "iterations": 0,
            "converged": true,
            "truncated": false,
        }));
    }
    let adjacency = algorithm_adjacency(
        view,
        &ids,
        options,
        as_of,
        options.direction,
        started,
        observer,
    )?;
    let n = ids.len() as f64;
    let mut incoming: HashMap<String, Vec<String>> = HashMap::new();
    for (source, edges) in &adjacency {
        for edge in edges {
            incoming
                .entry(edge.target.clone())
                .or_default()
                .push(source.clone());
        }
    }
    let mut scores: HashMap<String, f64> = ids.iter().map(|id| (id.clone(), 1.0 / n)).collect();
    let mut working = estimate_adjacency_memory(&ids, &adjacency)
        .saturating_add(
            incoming
                .values()
                .map(|parents| parents.iter().map(|p| p.len() + 48).sum::<usize>())
                .sum::<usize>(),
        )
        .saturating_add(ids.len().saturating_mul(std::mem::size_of::<String>() + 32));
    check_algorithm_memory(options, working)?;
    let mut converged = false;
    let mut iterations = 0usize;
    for iteration in 0..options.iterations {
        check_algorithm_timeout(options, started)?;
        check_algorithm_observer(observer)?;
        note_algorithm_progress(observer, "pagerank", iteration, Some(options.iterations));
        let dangling: f64 = ids
            .iter()
            .filter(|id| adjacency.get(*id).is_none_or(|edges| edges.is_empty()))
            .map(|id| scores.get(id).copied().unwrap_or(0.0))
            .sum();
        let base = (1.0 - options.damping) / n + options.damping * dangling / n;
        let mut next: HashMap<String, f64> = HashMap::with_capacity(ids.len());
        for id in &ids {
            let mut score = base;
            if let Some(parents) = incoming.get(id) {
                for parent in parents {
                    let degree = adjacency.get(parent).map_or(0, Vec::len) as f64;
                    if degree > 0.0 {
                        score +=
                            options.damping * scores.get(parent).copied().unwrap_or(0.0) / degree;
                    }
                }
            }
            next.insert(id.clone(), score);
        }
        let delta: f64 = ids
            .iter()
            .map(|id| {
                (next.get(id).copied().unwrap_or(0.0) - scores.get(id).copied().unwrap_or(0.0))
                    .abs()
            })
            .sum();
        scores = next;
        working =
            working.saturating_add(ids.len().saturating_mul(std::mem::size_of::<String>() + 48));
        check_algorithm_memory(options, working)?;
        iterations += 1;
        if delta < options.tolerance {
            converged = true;
            break;
        }
    }
    let mut ranked: Vec<(String, f64)> = scores.into_iter().collect();
    ranked.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    let limited = ranked.len() > options.limit;
    ranked.truncate(options.limit);
    let items: Vec<Value> = ranked
        .into_iter()
        .map(|(id, score)| json!({"id": id, "score": score}))
        .collect();
    Ok(json!({
        "kind": "pagerank",
        "items": items,
        "nodes": ids.len(),
        "iterations": iterations,
        "converged": converged,
        "truncated": limited,
    }))
}

fn degree_centrality(
    view: &dyn GraphView,
    options: &GraphAlgorithmOptions,
    observer: Option<&dyn GraphAlgorithmObserver>,
) -> Result<Value> {
    let started = Instant::now();
    let as_of = as_of_ms(options.as_of.as_ref())?;
    let ids = algorithm_nodes(view, options, started, observer)?;
    let empty = HashMap::new();
    let outgoing = if options.direction == GraphDirection::In {
        empty
    } else {
        algorithm_adjacency(
            view,
            &ids,
            options,
            as_of,
            GraphDirection::Out,
            started,
            observer,
        )?
    };
    let incoming = if options.direction == GraphDirection::Out {
        HashMap::new()
    } else {
        algorithm_adjacency(
            view,
            &ids,
            options,
            as_of,
            GraphDirection::In,
            started,
            observer,
        )?
    };
    check_algorithm_timeout(options, started)?;
    let mut ranked: Vec<(String, usize, usize)> = ids
        .iter()
        .map(|id| {
            let out = outgoing.get(id).map_or(0, Vec::len);
            let incoming = incoming.get(id).map_or(0, Vec::len);
            (id.clone(), out, incoming)
        })
        .collect();
    ranked.sort_by(|a, b| {
        (b.1 + b.2)
            .cmp(&(a.1 + a.2))
            .then_with(|| b.1.cmp(&a.1))
            .then_with(|| a.0.cmp(&b.0))
    });
    let limited = ranked.len() > options.limit;
    ranked.truncate(options.limit);
    let items: Vec<Value> = ranked
        .into_iter()
        .map(|(id, out, incoming)| {
            json!({
                "id": id,
                "out": out,
                "in": incoming,
                "total": out + incoming,
            })
        })
        .collect();
    Ok(json!({
        "kind": "degree_centrality",
        "items": items,
        "nodes": ids.len(),
        "truncated": limited,
    }))
}

fn label_propagation(
    view: &dyn GraphView,
    options: &GraphAlgorithmOptions,
    observer: Option<&dyn GraphAlgorithmObserver>,
) -> Result<Value> {
    let started = Instant::now();
    let as_of = as_of_ms(options.as_of.as_ref())?;
    let ids = algorithm_nodes(view, options, started, observer)?;
    let adjacency = algorithm_adjacency(
        view,
        &ids,
        options,
        as_of,
        options.direction,
        started,
        observer,
    )?;
    let mut labels: HashMap<String, String> =
        ids.iter().map(|id| (id.clone(), id.clone())).collect();
    let mut working = estimate_adjacency_memory(&ids, &adjacency).saturating_add(
        ids.len()
            .saturating_mul(2 * (std::mem::size_of::<String>() + 32)),
    );
    check_algorithm_memory(options, working)?;
    let mut converged = false;
    let mut iterations = 0usize;
    for iteration in 0..options.iterations {
        check_algorithm_timeout(options, started)?;
        check_algorithm_observer(observer)?;
        note_algorithm_progress(
            observer,
            "label_propagation",
            iteration,
            Some(options.iterations),
        );
        let mut next = labels.clone();
        let mut changed = false;
        for id in &ids {
            let mut counts: HashMap<&str, usize> = HashMap::new();
            if let Some(edges) = adjacency.get(id) {
                for edge in edges {
                    if let Some(label) = labels.get(&edge.target) {
                        *counts.entry(label.as_str()).or_default() += 1;
                    }
                }
            }
            let best = counts
                .into_iter()
                .max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(a.0)))
                .map(|(label, _)| label.to_string());
            if let Some(best) = best {
                if labels.get(id) != Some(&best) {
                    next.insert(id.clone(), best);
                    changed = true;
                }
            }
        }
        labels = next;
        working =
            working.saturating_add(ids.len().saturating_mul(std::mem::size_of::<String>() + 32));
        check_algorithm_memory(options, working)?;
        iterations += 1;
        if !changed {
            converged = true;
            break;
        }
    }
    let mut items: Vec<Value> = ids
        .iter()
        .map(|id| json!({"id": id, "label": labels.get(id).cloned().unwrap_or_else(|| id.clone())}))
        .collect();
    items.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    let limited = items.len() > options.limit;
    items.truncate(options.limit);
    Ok(json!({
        "kind": "label_propagation",
        "items": items,
        "nodes": ids.len(),
        "iterations": iterations,
        "converged": converged,
        "truncated": limited,
    }))
}

fn k_core(
    view: &dyn GraphView,
    options: &GraphAlgorithmOptions,
    observer: Option<&dyn GraphAlgorithmObserver>,
) -> Result<Value> {
    let started = Instant::now();
    let as_of = as_of_ms(options.as_of.as_ref())?;
    let ids = algorithm_nodes(view, options, started, observer)?;
    let adjacency = algorithm_adjacency(
        view,
        &ids,
        options,
        as_of,
        GraphDirection::Both,
        started,
        observer,
    )?;
    let allowed: HashSet<&str> = ids.iter().map(String::as_str).collect();
    let mut neighbors: HashMap<String, HashSet<String>> = HashMap::new();
    for id in &ids {
        let mut set = HashSet::new();
        if let Some(edges) = adjacency.get(id) {
            for edge in edges {
                if allowed.contains(edge.target.as_str()) {
                    set.insert(edge.target.clone());
                }
            }
        }
        neighbors.insert(id.clone(), set);
    }
    let mut degree: HashMap<String, usize> = ids
        .iter()
        .map(|id| (id.clone(), neighbors.get(id).map_or(0, HashSet::len)))
        .collect();
    let mut heap: std::collections::BinaryHeap<std::cmp::Reverse<(usize, String)>> = ids
        .iter()
        .map(|id| std::cmp::Reverse((degree[id], id.clone())))
        .collect();
    let mut core: HashMap<String, usize> = HashMap::new();
    let mut working = estimate_adjacency_memory(&ids, &adjacency)
        .saturating_add(
            neighbors
                .values()
                .map(|set| 64 + set.iter().map(|n| n.len() + 48).sum::<usize>())
                .sum::<usize>(),
        )
        .saturating_add(
            ids.len()
                .saturating_mul(3 * (std::mem::size_of::<String>() + 32)),
        );
    check_algorithm_memory(options, working)?;
    while let Some(std::cmp::Reverse((d, id))) = heap.pop() {
        check_algorithm_timeout(options, started)?;
        check_algorithm_observer(observer)?;
        if core.contains_key(&id) || degree.get(&id).copied() != Some(d) {
            continue;
        }
        core.insert(id.clone(), d);
        working = working.saturating_add(id.len() + std::mem::size_of::<String>() + 32);
        check_algorithm_memory(options, working)?;
        note_algorithm_progress(observer, "k_core", core.len(), Some(ids.len()));
        if let Some(set) = neighbors.get(&id) {
            for neighbor in set {
                if core.contains_key(neighbor) {
                    continue;
                }
                if let Some(value) = degree.get_mut(neighbor) {
                    if *value > d {
                        *value -= 1;
                        heap.push(std::cmp::Reverse((*value, neighbor.clone())));
                    }
                }
            }
        }
    }
    let mut items: Vec<Value> = ids
        .iter()
        .map(|id| json!({"id": id, "core": core.get(id).copied().unwrap_or(0)}))
        .collect();
    items.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    let limited = items.len() > options.limit;
    items.truncate(options.limit);
    Ok(json!({
        "kind": "k_core",
        "items": items,
        "nodes": ids.len(),
        "truncated": limited,
    }))
}

fn degree_distribution(
    view: &dyn GraphView,
    options: &GraphAlgorithmOptions,
    observer: Option<&dyn GraphAlgorithmObserver>,
) -> Result<Value> {
    let started = Instant::now();
    let as_of = as_of_ms(options.as_of.as_ref())?;
    let ids = algorithm_nodes(view, options, started, observer)?;
    let outgoing = if options.direction == GraphDirection::In {
        HashMap::new()
    } else {
        algorithm_adjacency(
            view,
            &ids,
            options,
            as_of,
            GraphDirection::Out,
            started,
            observer,
        )?
    };
    let incoming = if options.direction == GraphDirection::Out {
        HashMap::new()
    } else {
        algorithm_adjacency(
            view,
            &ids,
            options,
            as_of,
            GraphDirection::In,
            started,
            observer,
        )?
    };
    let mut histogram: std::collections::BTreeMap<usize, usize> = std::collections::BTreeMap::new();
    for id in &ids {
        let degree = outgoing.get(id).map_or(0, Vec::len) + incoming.get(id).map_or(0, Vec::len);
        *histogram.entry(degree).or_default() += 1;
    }
    let mut items: Vec<Value> = histogram
        .into_iter()
        .map(|(degree, count)| json!({"degree": degree, "count": count}))
        .collect();
    let limited = items.len() > options.limit;
    items.truncate(options.limit);
    Ok(json!({
        "kind": "degree_distribution",
        "items": items,
        "nodes": ids.len(),
        "truncated": limited,
    }))
}

fn connected_components(
    view: &dyn GraphView,
    options: &GraphAlgorithmOptions,
    observer: Option<&dyn GraphAlgorithmObserver>,
) -> Result<Value> {
    let started = Instant::now();
    let as_of = as_of_ms(options.as_of.as_ref())?;
    let ids = algorithm_nodes(view, options, started, observer)?;
    let allowed: HashSet<&str> = ids.iter().map(String::as_str).collect();
    let adjacency = algorithm_adjacency(
        view,
        &ids,
        options,
        as_of,
        GraphDirection::Both,
        started,
        observer,
    )?;
    check_algorithm_timeout(options, started)?;
    let mut seen: HashSet<String> = HashSet::new();
    let mut components: Vec<(String, Vec<String>)> = Vec::new();
    let mut working = estimate_adjacency_memory(&ids, &adjacency);
    check_algorithm_memory(options, working)?;
    let mut truncated = false;
    let mut walked = 0usize;
    for id in &ids {
        check_algorithm_timeout(options, started)?;
        check_algorithm_observer(observer)?;
        if seen.contains(id) {
            continue;
        }
        if components.len() >= options.limit {
            truncated = true;
            break;
        }
        let mut queue = VecDeque::new();
        let mut members = Vec::new();
        seen.insert(id.clone());
        queue.push_back(id.clone());
        while let Some(current) = queue.pop_front() {
            walked += 1;
            if walked.is_multiple_of(1024) {
                note_algorithm_progress(observer, "components", walked, Some(ids.len()));
                check_algorithm_observer(observer)?;
            }
            members.push(current.clone());
            working = working.saturating_add(current.len() + std::mem::size_of::<String>() + 32);
            check_algorithm_memory(options, working)?;
            if let Some(edges) = adjacency.get(&current) {
                for edge in edges {
                    if allowed.contains(edge.target.as_str()) && seen.insert(edge.target.clone()) {
                        queue.push_back(edge.target.clone());
                    }
                }
            }
        }
        if let Some(first) = members.first() {
            components.push((first.clone(), members));
        }
    }
    components.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then_with(|| a.0.cmp(&b.0)));
    let count = components.len();
    let items: Vec<Value> = components
        .into_iter()
        .enumerate()
        .map(|(index, (_first, members))| {
            json!({
                "id": index,
                "size": members.len(),
                "nodes": members,
            })
        })
        .collect();
    Ok(json!({
        "kind": "connected_components",
        "components": items,
        "count": count,
        "nodes": ids.len(),
        "truncated": truncated,
    }))
}

/// Runs a read-only graph algorithm over all nodes in the graph's node
/// collection. The result is bounded by `limit` and `max_nodes`.
fn check_algorithm_timeout(options: &GraphAlgorithmOptions, started: Instant) -> Result<()> {
    if let Some(timeout_ms) = options.timeout_ms {
        if started.elapsed().as_millis() as u64 >= timeout_ms {
            return Err(Error::Query("graph algorithm timed out".into()));
        }
    }
    Ok(())
}

pub fn graph_algorithm(view: &dyn GraphView, options: &GraphAlgorithmOptions) -> Result<Value> {
    graph_algorithm_with_observer(view, options, None)
}

/// Runs a graph algorithm with cooperative cancellation and progress callbacks.
pub fn graph_algorithm_with_observer(
    view: &dyn GraphView,
    options: &GraphAlgorithmOptions,
    observer: Option<&dyn GraphAlgorithmObserver>,
) -> Result<Value> {
    options.validate()?;
    match options.kind {
        GraphAlgorithmKind::Pagerank => pagerank(view, options, observer),
        GraphAlgorithmKind::DegreeCentrality => degree_centrality(view, options, observer),
        GraphAlgorithmKind::ConnectedComponents => connected_components(view, options, observer),
        GraphAlgorithmKind::LabelPropagation => label_propagation(view, options, observer),
        GraphAlgorithmKind::KCore => k_core(view, options, observer),
        GraphAlgorithmKind::DegreeDistribution => degree_distribution(view, options, observer),
    }
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

        fn node_ids(&self) -> Result<Vec<String>> {
            let mut ids: HashSet<String> = self.edges.keys().cloned().collect();
            for edges in self.edges.values() {
                for edge in edges {
                    ids.insert(edge.target.clone());
                }
            }
            Ok(ids.into_iter().collect())
        }

        fn neighbors(
            &self,
            id: &str,
            direction: GraphDirection,
            labels: &[String],
        ) -> Result<Vec<AdjEdge>> {
            let mut out = Vec::new();
            if direction != GraphDirection::In {
                out.extend(self.edges.get(id).cloned().unwrap_or_default());
            }
            if direction != GraphDirection::Out {
                for (source, edges) in &self.edges {
                    for edge in edges {
                        if edge.target == id {
                            let mut incoming = edge.clone();
                            incoming.target = source.clone();
                            out.push(incoming);
                        }
                    }
                }
            }
            let mut seen = HashSet::new();
            out.retain(|edge| {
                (labels.is_empty() || labels.contains(&edge.label))
                    && seen.insert(edge.edge_id.clone())
            });
            Ok(out)
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
    fn rejects_oversized_graph_budgets_generation_and_labels() {
        let mut options: TraverseOptions = serde_json::from_value(json!({
            "start": ["a"],
            "limit_nodes": MAX_GRAPH_NODES + 1
        }))
        .unwrap();
        assert!(options.validate().is_err());
        options.limit_nodes = 1;
        options.limit_edges = MAX_GRAPH_EDGES + 1;
        assert!(options.validate().is_err());
        options.limit_edges = 1;
        options.max_bytes = MAX_GRAPH_BYTES + 1;
        assert!(options.validate().is_err());

        let labels = vec!["L".to_string(); MAX_GRAPH_LABELS + 1];
        let options: TraverseOptions = serde_json::from_value(json!({
            "start": ["a"],
            "labels": labels
        }))
        .unwrap();
        assert!(options.validate().is_err());

        let shortest: ShortestPathOptions = serde_json::from_value(json!({
            "start": "a",
            "target": "b",
            "limit_nodes": MAX_GRAPH_NODES + 1
        }))
        .unwrap();
        assert!(shortest.validate().is_err());

        let def: GraphDef = serde_json::from_value(json!({
            "node_collection": "nodes",
            "edge_collection": "edges",
            "generation": MAX_GRAPH_GENERATION + 1
        }))
        .unwrap();
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

    #[test]
    fn all_shortest_paths_bfs_and_dijkstra() {
        let options: AllShortestPathsOptions = serde_json::from_value(json!({
            "start": "a",
            "target": "c",
            "direction": "out",
            "max_depth": 4,
            "limit_paths": 10,
            "limit_nodes": 100
        }))
        .unwrap();
        let out = all_shortest_paths(&view(), &options).unwrap();
        assert_eq!(out["found"], true);
        assert_eq!(out["path_count"], 1);
        assert_eq!(out["paths"][0]["hops"], 1);

        let weighted: AllShortestPathsOptions = serde_json::from_value(json!({
            "start": "a",
            "target": "c",
            "direction": "out",
            "max_depth": 4,
            "limit_paths": 10,
            "limit_nodes": 100,
            "weighted": true
        }))
        .unwrap();
        let out = all_shortest_paths(&view(), &weighted).unwrap();
        assert_eq!(out["found"], true);
        assert_eq!(out["weight"], 2.0);
        assert_eq!(out["paths"][0]["hops"], 2);
    }

    struct LargeMockView {
        ids: Vec<String>,
    }

    impl GraphView for LargeMockView {
        fn node(&self, id: &str) -> Result<Option<Value>> {
            Ok(Some(json!({"_id": id})))
        }

        fn edge(&self, id: &str) -> Result<Option<Value>> {
            Ok(Some(json!({"_id": id})))
        }

        fn node_ids(&self) -> Result<Vec<String>> {
            Ok(self.ids.clone())
        }

        fn neighbors(
            &self,
            _id: &str,
            _direction: GraphDirection,
            _labels: &[String],
        ) -> Result<Vec<AdjEdge>> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn graph_algorithm_scales_to_100k_nodes_and_rejects_budget_overruns() {
        let large = LargeMockView {
            ids: (0..100_000).map(|i| format!("n{i:06}")).collect(),
        };
        let options: GraphAlgorithmOptions = serde_json::from_value(json!({
            "kind": "pagerank",
            "limit": 10,
            "max_nodes": 100_000,
            "max_edges": 1_000_000,
            "max_memory_bytes": 256 * 1024 * 1024,
            "iterations": 2,
            "tolerance": 1e-9
        }))
        .unwrap();
        let out = graph_algorithm(&large, &options).unwrap();
        assert_eq!(out["nodes"], 100_000);

        let too_few: GraphAlgorithmOptions = serde_json::from_value(json!({
            "kind": "degree_centrality",
            "max_nodes": 99_999
        }))
        .unwrap();
        let err = graph_algorithm(&large, &too_few).unwrap_err().to_string();
        assert!(err.contains("node budget exceeded"), "{err}");

        let low_memory: GraphAlgorithmOptions = serde_json::from_value(json!({
            "kind": "degree_centrality",
            "max_nodes": 100_000,
            "max_memory_bytes": 1
        }))
        .unwrap();
        let err = graph_algorithm(&large, &low_memory)
            .unwrap_err()
            .to_string();
        assert!(err.contains("memory budget exceeded"), "{err}");

        let low_edges: GraphAlgorithmOptions = serde_json::from_value(json!({
            "kind": "pagerank",
            "max_nodes": 100,
            "max_edges": 1
        }))
        .unwrap();
        let err = graph_algorithm(&view(), &low_edges)
            .unwrap_err()
            .to_string();
        assert!(err.contains("edge budget exceeded"), "{err}");

        let timed: GraphAlgorithmOptions = serde_json::from_value(json!({
            "kind": "pagerank",
            "max_nodes": 100_000,
            "iterations": 100,
            "timeout_ms": 1
        }))
        .unwrap();
        let err = graph_algorithm(&large, &timed).unwrap_err().to_string();
        assert!(err.contains("timed out"), "{err}");
    }

    #[test]
    fn cheap_analytics_algorithms_are_deterministic_and_handle_empty_graphs() {
        let kcore: GraphAlgorithmOptions = serde_json::from_value(json!({
            "kind": "k_core",
            "direction": "both",
            "limit": 10,
            "max_nodes": 100
        }))
        .unwrap();
        let out = graph_algorithm(&view(), &kcore).unwrap();
        assert_eq!(
            out["items"],
            json!([
                {"id": "a", "core": 2},
                {"id": "b", "core": 2},
                {"id": "c", "core": 2}
            ])
        );

        let path_view = MockView {
            edges: HashMap::from([
                (
                    "a".into(),
                    vec![MockView::edge("p1", "a", "b", "ROAD", None)],
                ),
                (
                    "b".into(),
                    vec![MockView::edge("p2", "b", "c", "ROAD", None)],
                ),
            ]),
        };
        let out = graph_algorithm(&path_view, &kcore).unwrap();
        assert_eq!(
            out["items"],
            json!([
                {"id": "a", "core": 1},
                {"id": "b", "core": 1},
                {"id": "c", "core": 1}
            ])
        );

        let distribution: GraphAlgorithmOptions = serde_json::from_value(json!({
            "kind": "degree_distribution",
            "direction": "both",
            "max_nodes": 100
        }))
        .unwrap();
        let out = graph_algorithm(&view(), &distribution).unwrap();
        assert_eq!(
            out["items"],
            json!([
                {"degree": 2, "count": 1},
                {"degree": 3, "count": 2}
            ])
        );
        let limited: GraphAlgorithmOptions = serde_json::from_value(json!({
            "kind": "degree_distribution",
            "direction": "both",
            "max_nodes": 100,
            "limit": 1
        }))
        .unwrap();
        let out = graph_algorithm(&view(), &limited).unwrap();
        assert_eq!(out["items"], json!([{"degree": 2, "count": 1}]));
        assert_eq!(out["truncated"], true);

        let labels: GraphAlgorithmOptions = serde_json::from_value(json!({
            "kind": "label_propagation",
            "direction": "both",
            "max_nodes": 100,
            "iterations": 5
        }))
        .unwrap();
        let first = graph_algorithm(&view(), &labels).unwrap();
        let second = graph_algorithm(&view(), &labels).unwrap();
        assert_eq!(first, second);
        assert_eq!(first["items"].as_array().unwrap().len(), 3);

        let empty = MockView {
            edges: HashMap::new(),
        };
        let out = graph_algorithm(&empty, &kcore).unwrap();
        assert_eq!(out["nodes"], 0);
        assert_eq!(out["items"], json!([]));
    }

    #[test]
    fn graph_match_variable_length_and_algorithms() {
        let match_options: GraphMatchOptions = serde_json::from_value(json!({
            "start": "a",
            "pattern": [{
                "direction": "out",
                "labels": ["KNOWS"],
                "min_hops": 1,
                "max_hops": 2
            }],
            "limit": 10,
            "max_depth": 4
        }))
        .unwrap();
        let out = graph_match(&view(), &match_options).unwrap();
        assert_eq!(out["matches"], 3);

        let pagerank_options: GraphAlgorithmOptions = serde_json::from_value(json!({
            "kind": "pagerank",
            "direction": "out",
            "limit": 10,
            "max_nodes": 100
        }))
        .unwrap();
        let out = graph_algorithm(&view(), &pagerank_options).unwrap();
        assert_eq!(out["kind"], "pagerank");
        assert_eq!(out["items"].as_array().unwrap().len(), 3);

        let degree_options: GraphAlgorithmOptions = serde_json::from_value(json!({
            "kind": "degree_centrality",
            "direction": "both",
            "limit": 10,
            "max_nodes": 100
        }))
        .unwrap();
        let out = graph_algorithm(&view(), &degree_options).unwrap();
        assert_eq!(out["items"][0]["total"], 3);
        assert!(out["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == "c" && item["total"] == 3));

        let components_options: GraphAlgorithmOptions = serde_json::from_value(json!({
            "kind": "connected_components",
            "direction": "both",
            "limit": 10,
            "max_nodes": 100
        }))
        .unwrap();
        let out = graph_algorithm(&view(), &components_options).unwrap();
        assert_eq!(out["count"], 1);
        assert_eq!(out["components"][0]["size"], 3);
    }
}
