//! HTTP/JSON protocol: requests, responses, and API errors.

use serde::{ser::SerializeMap, Deserialize, Serialize, Serializer};
use serde_json::{value::RawValue, Value};

use nonodb_core::graph::{
    AllShortestPathsOptions, GraphAlgorithmOptions, GraphDef, GraphMatchOptions, HybridOptions,
    NeighborsOptions, ShortestPathOptions, TraverseOptions,
};
use nonodb_core::{CollectionProfile, IndexSpec, SearchAllRequest, VaultKey, VectorMetric};

use crate::webhooks::WebhookRule;

/// Per-request vault credentials. Never format the key.
#[derive(Clone)]
pub struct VaultAuth {
    pub key: VaultKey,
    pub kid: String,
    pub tenant: String,
}

impl std::fmt::Debug for VaultAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VaultAuth")
            .field("key", &"<redacted>")
            .field("kid", &self.kid)
            .field("tenant", &self.tenant)
            .finish()
    }
}

/// One index entry for `create_indexes`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IndexRequest {
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    /// Explicit index name; empty means the default name.
    #[serde(default)]
    pub name: String,
    /// Per-field directions (`1` or `-1`); empty means all ascending.
    #[serde(default)]
    pub directions: Vec<i8>,
    #[serde(default)]
    pub unique: bool,
    #[serde(default)]
    pub sparse: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "partialFilterExpression"
    )]
    pub partial_filter_expression: Option<Value>,
    #[serde(default)]
    pub include: Vec<String>,
    /// Create a GeoJSON point index.
    #[serde(default)]
    pub geo: bool,
}

/// Lookup key selector: a single field or a compound field order.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum LookupBy {
    Field(String),
    Fields(Vec<String>),
}

impl LookupBy {
    pub fn into_fields(self) -> Vec<String> {
        match self {
            Self::Field(field) => vec![field],
            Self::Fields(fields) => fields,
        }
    }
}

/// Request to `/v1/exec`.
fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WatchThreshold {
    pub count: usize,
    #[serde(default)]
    pub window_ms: Option<u64>,
    #[serde(default)]
    pub group_by: Vec<String>,
}

/// Write durability requested for one operation.
///
/// Atomicity is always preserved. These modes only control when LMDB forces
/// data/metadata to stable storage.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum Durability {
    /// Force data and metadata fsync before replying.
    #[default]
    Full,
    /// Commit normally, without an explicit metadata fsync.
    Relaxed,
    /// Skip the extra sync entirely. Requires NONODB_ALLOW_UNSAFE_WRITES=1.
    Unsafe,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum Request {
    Insert {
        coll: String,
        doc: Value,
        #[serde(default)]
        durability: Option<Durability>,
    },
    InsertMany {
        coll: String,
        docs: Vec<Value>,
        #[serde(default)]
        durability: Option<Durability>,
    },
    Find {
        coll: String,
        #[serde(default)]
        filter: Value,
        #[serde(default)]
        projection: Option<Value>,
        #[serde(default)]
        sort: Vec<(String, i32)>,
        #[serde(default)]
        skip: usize,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        comment: Option<String>,
        #[serde(default)]
        max_time_ms: Option<u64>,
        #[serde(default)]
        hint: Option<Value>,
    },
    FindOne {
        coll: String,
        #[serde(default)]
        filter: Value,
        #[serde(default)]
        projection: Option<Value>,
        #[serde(default)]
        comment: Option<String>,
        #[serde(default)]
        max_time_ms: Option<u64>,
        #[serde(default)]
        hint: Option<Value>,
    },
    /// Ordered point lookups by `_id` or a unique single-field index.
    Lookup {
        coll: String,
        #[serde(default)]
        by: Option<LookupBy>,
        #[serde(default)]
        values: Vec<Value>,
        #[serde(default)]
        fields: Option<Vec<String>>,
        #[serde(default)]
        projection: Option<Value>,
    },
    /// Plan and counters for an ordered point lookup.
    ExplainLookup {
        coll: String,
        #[serde(default)]
        by: Option<LookupBy>,
        #[serde(default)]
        values: Vec<Value>,
        #[serde(default)]
        fields: Option<Vec<String>>,
        #[serde(default)]
        projection: Option<Value>,
    },
    /// Long-polls an append-only event collection after a sortable `_id`.
    Watch {
        coll: String,
        #[serde(default)]
        after: Option<String>,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        timeout_ms: Option<u64>,
        #[serde(default)]
        types: Option<Vec<String>>,
        /// Additional MongoDB-style filter applied to event documents.
        #[serde(default)]
        filter: Option<Value>,
        /// Optional projection. `_id` is always retained for resume tokens.
        #[serde(default)]
        projection: Option<Value>,
        /// Keepalive interval for `/v1/watch` streaming. Ignored by long-poll.
        #[serde(default)]
        heartbeat_ms: Option<u64>,
        /// Late-commit rescan window for the continuous stream, in milliseconds.
        #[serde(default)]
        overlap_ms: Option<u64>,
        /// Optional server-side event count threshold. Continuous stream only.
        #[serde(default)]
        threshold: Option<WatchThreshold>,
        /// `live` evaluates a condition on `coll` and delivers only to this
        /// connection, without writing to `_events`.
        #[serde(default)]
        mode: Option<String>,
        #[serde(default)]
        ops: Option<Vec<String>>,
        #[serde(default)]
        count: Option<nonodb_core::subscriptions::CountCondition>,
        /// Edge-triggered live subscription (`false -> true`).
        #[serde(default)]
        once: Option<bool>,
        #[serde(default)]
        event_type: Option<String>,
        #[serde(default)]
        name: Option<String>,
    },
    /// Read-only query plan and actual count diagnostics.
    Explain {
        coll: String,
        #[serde(default)]
        filter: Value,
        #[serde(default)]
        sort: Vec<(String, i32)>,
        #[serde(default)]
        skip: usize,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        comment: Option<String>,
        #[serde(default)]
        max_time_ms: Option<u64>,
        #[serde(default)]
        hint: Option<Value>,
    },
    Update {
        coll: String,
        #[serde(default)]
        filter: Value,
        update: Value,
        #[serde(default)]
        multi: bool,
        #[serde(default)]
        upsert: bool,
        #[serde(default)]
        array_filters: Vec<Value>,
        #[serde(default)]
        durability: Option<Durability>,
    },
    /// Atomically update or remove one document and return its old/new image.
    FindAndModify {
        coll: String,
        #[serde(default)]
        filter: Value,
        #[serde(default)]
        update: Option<Value>,
        #[serde(default)]
        sort: Vec<(String, i32)>,
        #[serde(default)]
        new: bool,
        #[serde(default)]
        upsert: bool,
        #[serde(default)]
        remove: bool,
        #[serde(default)]
        array_filters: Vec<Value>,
        #[serde(default)]
        projection: Option<Value>,
        #[serde(default)]
        durability: Option<Durability>,
    },
    Delete {
        coll: String,
        #[serde(default)]
        filter: Value,
        #[serde(default)]
        multi: bool,
        #[serde(default)]
        durability: Option<Durability>,
    },
    Count {
        coll: String,
        #[serde(default)]
        filter: Value,
        #[serde(default)]
        comment: Option<String>,
        #[serde(default)]
        max_time_ms: Option<u64>,
    },
    /// Aggregation pipeline. A terminal `$out` or `$merge` makes this a write.
    Aggregate {
        coll: String,
        #[serde(default)]
        pipeline: Vec<Value>,
        #[serde(default)]
        comment: Option<String>,
        #[serde(default)]
        max_time_ms: Option<u64>,
        #[serde(default)]
        durability: Option<Durability>,
    },
    /// Read-only SQL SELECT over one collection.
    Sql {
        query: String,
        #[serde(default)]
        params: Vec<Value>,
        #[serde(default)]
        durability: Option<Durability>,
    },
    /// Explain a read-only SQL SELECT.
    ExplainSql {
        query: String,
        #[serde(default)]
        params: Vec<Value>,
    },
    /// Opens a bounded cursor over `find`. Supports an empty sort or `_id`.
    CursorFind {
        coll: String,
        #[serde(default)]
        filter: Value,
        #[serde(default)]
        projection: Option<Value>,
        #[serde(default)]
        sort: Vec<(String, i32)>,
        #[serde(default)]
        skip: usize,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        batch_size: Option<usize>,
        #[serde(default)]
        comment: Option<String>,
        #[serde(default)]
        max_time_ms: Option<u64>,
        #[serde(default)]
        hint: Option<Value>,
    },
    /// Opens a bounded cursor over document-local aggregation stages.
    CursorAggregate {
        coll: String,
        #[serde(default)]
        pipeline: Vec<Value>,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        batch_size: Option<usize>,
        #[serde(default)]
        comment: Option<String>,
        #[serde(default)]
        max_time_ms: Option<u64>,
    },
    GetMore {
        cursor_id: u64,
        #[serde(default)]
        batch_size: Option<usize>,
    },
    CloseCursor {
        cursor_id: u64,
    },
    CreateIndex {
        coll: String,
        #[serde(default)]
        field: Option<String>,
        #[serde(default)]
        fields: Option<Vec<String>>,
        #[serde(default)]
        unique: bool,
        #[serde(default)]
        sparse: bool,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            alias = "partialFilterExpression"
        )]
        partial_filter_expression: Option<Value>,
        #[serde(default)]
        include: Vec<String>,
        #[serde(default)]
        geo: bool,
    },
    /// Creates several secondary indexes in one collection scan.
    CreateIndexes {
        coll: String,
        indexes: Vec<IndexRequest>,
    },
    DropIndex {
        coll: String,
        field: String,
    },
    ListIndexes {
        coll: String,
    },
    CreateTextIndex {
        coll: String,
        field: String,
    },
    DropTextIndex {
        coll: String,
        field: String,
    },
    ListTextIndexes {
        coll: String,
    },
    /// Active and recently finished index backfills for one collection.
    IndexStatus {
        coll: String,
    },
    CreateFuzzyIndex {
        coll: String,
        field: String,
    },
    DropFuzzyIndex {
        coll: String,
        field: String,
    },
    ListFuzzyIndexes {
        coll: String,
    },
    CreateVectorIndex {
        coll: String,
        field: String,
        dims: usize,
        #[serde(default)]
        metric: Option<VectorMetric>,
    },
    Reindex {
        coll: String,
    },
    DropVectorIndex {
        coll: String,
        field: String,
    },
    SetGraph {
        coll: String,
        def: GraphDef,
    },
    DropGraph {
        coll: String,
    },
    ReindexGraph {
        coll: String,
        #[serde(default)]
        online: bool,
    },
    GraphRebuildStatus {
        coll: String,
    },
    CancelGraphRebuild {
        coll: String,
    },
    ListVectorIndexes {
        coll: String,
    },
    VectorSearch {
        coll: String,
        field: String,
        vector: Vec<f32>,
        #[serde(default)]
        filter: Value,
        #[serde(default)]
        skip: usize,
        #[serde(default)]
        limit: Option<usize>,
    },
    /// Reads the graph definition for a node collection.
    GetGraph {
        coll: String,
    },
    /// Breadth-first or depth-first traversal over the graph rooted at `coll`.
    Traverse {
        coll: String,
        #[serde(flatten)]
        options: TraverseOptions,
    },
    /// Shortest path over the graph rooted at `coll`.
    ShortestPath {
        coll: String,
        #[serde(flatten)]
        options: ShortestPathOptions,
    },
    /// Every shortest path between two nodes.
    AllShortestPaths {
        coll: String,
        #[serde(flatten)]
        options: AllShortestPathsOptions,
    },
    /// Linear variable-length graph pattern matching.
    GraphMatch {
        coll: String,
        #[serde(flatten)]
        options: GraphMatchOptions,
    },
    /// Read-only graph algorithm (PageRank, degree, components).
    GraphAlgorithm {
        coll: String,
        #[serde(flatten)]
        options: GraphAlgorithmOptions,
    },
    /// One-hop neighbors of a node with graph filters.
    Neighbors {
        coll: String,
        #[serde(flatten)]
        options: NeighborsOptions,
    },
    /// Vector seed + graph expansion rerank.
    HybridSearch {
        coll: String,
        #[serde(flatten)]
        options: HybridOptions,
    },
    Search {
        coll: String,
        field: String,
        query: String,
        #[serde(default)]
        filter: Value,
        #[serde(default)]
        skip: usize,
        #[serde(default)]
        limit: Option<usize>,
    },
    /// Unified lexical/fuzzy/vector/hybrid search.
    SearchAll {
        #[serde(flatten)]
        request: SearchAllRequest,
    },
    Fuzzy {
        coll: String,
        field: String,
        query: String,
        #[serde(default)]
        max_distance: Option<usize>,
        #[serde(default)]
        filter: Value,
        #[serde(default)]
        limit: Option<usize>,
    },
    History {
        coll: String,
        id: String,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        include_docs: bool,
    },
    GetVersion {
        coll: String,
        id: String,
        #[serde(alias = "rev")]
        version: u64,
    },
    /// Multiple **read** operations in one LMDB transaction.
    Batch {
        ops: Vec<Request>,
    },
    /// Atomic write batch: all operations in one LMDB transaction.
    /// If any operation fails, the whole transaction is rolled back.
    WriteBatch {
        ops: Vec<WriteOp>,
        #[serde(default)]
        durability: Option<Durability>,
    },
    ListCollections,
    /// Per-collection record counts and storage estimates for this database.
    DatabaseStats,
    /// Current storage usage and quota limits for this database.
    GetQuota,
    /// Replace the storage quota limits for this database. Zero or null clears
    /// a limit.
    SetQuota {
        limits: crate::quota::QuotaLimits,
    },
    /// Creates a collection explicitly. `profile` is `standard` (default),
    /// `vault`, or `timeseries`; `timeseries` configures the time-series bucket
    /// layout when the profile is `timeseries`.
    CreateCollection {
        coll: String,
        #[serde(default)]
        profile: Option<String>,
        #[serde(default)]
        ttl_seconds: Option<u64>,
        /// Purge stored history on TTL expiry instead of adding a tombstone.
        #[serde(default)]
        ttl_purge_history: Option<bool>,
        /// Time-series settings (`timeField`, `metaField`, granularity, ...).
        #[serde(default)]
        timeseries: Option<nonodb_core::timeseries::Settings>,
    },
    DropCollection {
        coll: String,
    },
    /// Removes time-series rows older than `before` (date or epoch ms).
    #[serde(alias = "retain_timeseries")]
    RetainTimeSeries {
        coll: String,
        before: Value,
    },
    /// Reads the time-series settings of a collection.
    #[serde(alias = "get_collection_timeseries")]
    GetCollectionTimeSeries {
        coll: String,
    },
    /// Replaces the time-series settings of a collection.
    #[serde(alias = "set_collection_timeseries")]
    SetCollectionTimeSeries {
        coll: String,
        #[serde(alias = "timeseries")]
        settings: nonodb_core::timeseries::Settings,
    },
    /// Sets or clears the default document TTL for a collection.
    SetCollectionTtl {
        coll: String,
        #[serde(default)]
        ttl_seconds: Option<u64>,
        #[serde(default)]
        purge_history: Option<bool>,
    },
    /// Sets a date-field TTL for a collection.
    SetCollectionTtlAfter {
        coll: String,
        field: String,
        ttl_seconds: u64,
        #[serde(default)]
        purge_history: Option<bool>,
    },
    /// Backfills date-field TTL metadata for one page of documents.
    BackfillCollectionTtl {
        coll: String,
        field: String,
        ttl_seconds: u64,
        #[serde(default)]
        after: Option<Value>,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        dry_run: Option<bool>,
    },
    /// Returns the collection default TTL.
    GetCollectionTtl {
        coll: String,
    },
    /// Set per-collection revision history retention. `0` disables it.
    SetCollectionHistory {
        coll: String,
        max_versions: usize,
    },
    /// Read per-collection revision history retention.
    GetCollectionHistory {
        coll: String,
    },
    /// Configure automatic collection change events.
    SetCollectionEvents {
        coll: String,
        enabled: bool,
        #[serde(default)]
        fields: Vec<String>,
        #[serde(default)]
        ops: Vec<String>,
        #[serde(default)]
        filter: Value,
        #[serde(default = "default_true")]
        include_before: bool,
        #[serde(default = "default_true")]
        include_after: bool,
        #[serde(default)]
        projection: Option<Value>,
    },
    /// Read automatic collection change-event settings.
    GetCollectionEvents {
        coll: String,
    },
    /// Runtime request, read/write, cache, error, and latency counters for one
    /// collection. Requires read access to the collection.
    GetCollectionMetrics {
        coll: String,
    },
    CreateSubscription {
        coll: String,
        name: String,
        #[serde(default)]
        ops: Vec<String>,
        #[serde(default)]
        filter: Value,
        #[serde(default)]
        count: Option<nonodb_core::subscriptions::CountCondition>,
        #[serde(default)]
        once: bool,
        #[serde(default)]
        event_type: Option<String>,
        /// Client lease in seconds; default 600. The client must renew it.
        #[serde(default)]
        lease_seconds: Option<u64>,
    },
    RenewSubscription {
        coll: String,
        name: String,
        #[serde(default)]
        lease_seconds: Option<u64>,
    },
    DeleteSubscription {
        coll: String,
        name: String,
    },
    ListSubscriptions {
        coll: String,
    },
    /// Creates or replaces an asynchronous webhook rule.
    CreateWebhook {
        #[serde(flatten)]
        rule: WebhookRule,
    },
    DeleteWebhook {
        name: String,
    },
    ListWebhooks,
    ListWebhookDeliveries {
        #[serde(default)]
        status: Option<String>,
        #[serde(default)]
        limit: Option<usize>,
    },
    RetryWebhookDelivery {
        id: String,
    },
    /// Lists databases in this daemon.
    ListDatabases,
    /// Creates a database. Names are case-sensitive and validated.
    CreateDatabase {
        name: String,
    },
    /// Drops a non-default database and its files.
    DropDatabase {
        name: String,
    },
    /// Forces an fsync of this database, making all prior relaxed commits durable.
    Sync,
    /// Returns the current LMDB map size and high-water mark.
    MapInfo,
    /// Grows the LMDB map without restarting the daemon.
    ResizeMap {
        /// New map size in bytes; must be a multiple of 4096 and >= current.
        size: u64,
    },
}

/// Write operation allowed in an atomic `write_batch`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum WriteOp {
    Insert {
        coll: String,
        doc: Value,
    },
    InsertMany {
        coll: String,
        docs: Vec<Value>,
    },
    Update {
        coll: String,
        #[serde(default)]
        filter: Value,
        update: Value,
        #[serde(default)]
        multi: bool,
        #[serde(default)]
        upsert: bool,
        #[serde(default)]
        array_filters: Vec<Value>,
    },
    Delete {
        coll: String,
        #[serde(default)]
        filter: Value,
        #[serde(default)]
        multi: bool,
    },
}

/// One due TTL entry passed to the writer's expiry job.
#[derive(Debug, Clone)]
pub struct ExpireEntry {
    pub coll: String,
    pub id: String,
    pub expected_ttl_at: u64,
}

/// Write operations passed to the single writer task.
#[derive(Debug)]
pub enum WriteReq {
    Insert {
        coll: String,
        doc: Value,
        vault: Option<VaultAuth>,
    },
    InsertMany {
        coll: String,
        docs: Vec<Value>,
        vault: Option<VaultAuth>,
    },
    Update {
        coll: String,
        filter: Value,
        update: Value,
        multi: bool,
        upsert: bool,
        array_filters: Vec<Value>,
        /// `_id` to use when an upsert would otherwise generate a core UUID.
        default_id: Option<Value>,
        vault: Option<VaultAuth>,
    },
    FindAndModify {
        coll: String,
        filter: Value,
        update: Option<Value>,
        sort: Vec<(String, i32)>,
        new: bool,
        upsert: bool,
        remove: bool,
        array_filters: Vec<Value>,
        projection: Option<Value>,
        /// `_id` to use when an upsert would otherwise generate a core UUID.
        default_id: Option<Value>,
        vault: Option<VaultAuth>,
    },
    Delete {
        coll: String,
        filter: Value,
        multi: bool,
        vault: Option<VaultAuth>,
    },
    CreateCollection {
        coll: String,
        profile: CollectionProfile,
        ttl_seconds: Option<u64>,
        ttl_purge_history: Option<bool>,
        timeseries: Option<nonodb_core::timeseries::Settings>,
    },
    DropCollection {
        coll: String,
    },
    SetCollectionTtl {
        coll: String,
        ttl_seconds: Option<u64>,
        purge_history: Option<bool>,
    },
    SetCollectionTimeSeries {
        coll: String,
        settings: nonodb_core::timeseries::Settings,
    },
    SetCollectionTtlAfter {
        coll: String,
        field: String,
        ttl_seconds: u64,
        purge_history: Option<bool>,
    },
    RetainTimeSeries {
        coll: String,
        before_ms: i64,
    },
    BackfillCollectionTtl {
        coll: String,
        field: String,
        ttl_seconds: u64,
        after: Option<Value>,
        limit: usize,
        dry_run: bool,
    },
    SetCollectionHistory {
        coll: String,
        max_versions: usize,
    },
    SetCollectionEvents {
        coll: String,
        enabled: bool,
        fields: Vec<String>,
        ops: Vec<String>,
        filter: Value,
        include_before: bool,
        include_after: bool,
        projection: Option<Value>,
    },
    CreateSubscription {
        coll: String,
        subscription: nonodb_core::subscriptions::Subscription,
    },
    RenewSubscription {
        coll: String,
        name: String,
        lease_seconds: Option<u64>,
    },
    DeleteSubscription {
        coll: String,
        name: String,
    },
    /// Internal maintenance: delete expired subscriptions.
    PruneSubscriptions,
    CreateWebhook {
        rule: WebhookRule,
    },
    DeleteWebhook {
        name: String,
    },
    RetryWebhookDelivery {
        id: String,
    },
    /// Internal maintenance: delete expired documents if their TTL still matches.
    Expire {
        entries: Vec<ExpireEntry>,
    },
    /// Internal maintenance: prune old document revisions.
    PruneHistory {
        limit: usize,
    },
    CreateIndex {
        coll: String,
        fields: Vec<String>,
        unique: bool,
        sparse: bool,
        partial_filter_expression: Option<Value>,
        include: Vec<String>,
        geo: bool,
    },
    CreateIndexes {
        coll: String,
        indexes: Vec<IndexSpec>,
    },
    CreateSearchIndex {
        coll: String,
        index: nonodb_core::SearchIndexDef,
    },
    DropSearchIndex {
        coll: String,
        kind: nonodb_core::SearchKind,
        name: String,
    },
    DropIndex {
        coll: String,
        field: String,
    },
    CreateTextIndex {
        coll: String,
        field: String,
    },
    DropTextIndex {
        coll: String,
        field: String,
    },
    CreateFuzzyIndex {
        coll: String,
        field: String,
    },
    DropFuzzyIndex {
        coll: String,
        field: String,
    },
    CreateVectorIndex {
        coll: String,
        field: String,
        dims: usize,
        metric: VectorMetric,
    },
    Reindex {
        coll: String,
    },
    DropVectorIndex {
        coll: String,
        field: String,
    },
    SetGraph {
        coll: String,
        def: GraphDef,
    },
    DropGraph {
        coll: String,
    },
    ReindexGraph {
        coll: String,
    },
    StartGraphRebuild {
        coll: String,
    },
    CancelGraphRebuild {
        coll: String,
    },
    GraphRebuildStep {
        edge: String,
        limit: usize,
    },
    /// SQL `INSERT` or `UPDATE` executed in one writer transaction.
    Sql {
        query: String,
        params: Vec<Value>,
    },
    /// Aggregation with a terminal `$out` or `$merge` write stage.
    Aggregate {
        coll: String,
        pipeline: Vec<Value>,
        max_time_ms: Option<u64>,
        database: String,
    },
    /// Atomic write batch (all or nothing).
    Batch {
        ops: Vec<WriteReq>,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
}

impl ApiError {
    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            code: "internal".to_string(),
            message: message.into(),
        }
    }

    pub fn timeout() -> Self {
        Self {
            code: "timeout".to_string(),
            message: "request timed out".to_string(),
        }
    }

    pub fn unauthorized() -> Self {
        Self {
            code: "unauthorized".to_string(),
            message: "missing or invalid credentials".to_string(),
        }
    }

    pub fn forbidden() -> Self {
        Self {
            code: "forbidden".to_string(),
            message: "principal is not allowed to perform this operation".to_string(),
        }
    }

    pub fn rate_limited(retry_after_secs: u64) -> Self {
        Self {
            code: "rate_limited".to_string(),
            message: format!(
                "too many failed authentication attempts; retry after {retry_after_secs} s"
            ),
        }
    }
}

impl Request {
    pub fn op_name(&self) -> &'static str {
        match self {
            Self::Insert { .. } => "insert",
            Self::InsertMany { .. } => "insert_many",
            Self::Find { .. } => "find",
            Self::FindOne { .. } => "find_one",
            Self::Lookup { .. } => "lookup",
            Self::ExplainLookup { .. } => "explain_lookup",
            Self::Watch { .. } => "watch",
            Self::Explain { .. } => "explain",
            Self::Update { .. } => "update",
            Self::FindAndModify { .. } => "find_and_modify",
            Self::Delete { .. } => "delete",
            Self::Count { .. } => "count",
            Self::Aggregate { .. } => "aggregate",
            Self::Sql { .. } => "sql",
            Self::ExplainSql { .. } => "explain_sql",
            Self::CursorFind { .. } => "cursor_find",
            Self::CursorAggregate { .. } => "cursor_aggregate",
            Self::GetMore { .. } => "get_more",
            Self::CloseCursor { .. } => "close_cursor",
            Self::CreateIndex { .. } => "create_index",
            Self::CreateIndexes { .. } => "create_indexes",
            Self::DropIndex { .. } => "drop_index",
            Self::ListIndexes { .. } => "list_indexes",
            Self::CreateTextIndex { .. } => "create_text_index",
            Self::DropTextIndex { .. } => "drop_text_index",
            Self::ListTextIndexes { .. } => "list_text_indexes",
            Self::IndexStatus { .. } => "index_status",
            Self::CreateFuzzyIndex { .. } => "create_fuzzy_index",
            Self::DropFuzzyIndex { .. } => "drop_fuzzy_index",
            Self::ListFuzzyIndexes { .. } => "list_fuzzy_indexes",
            Self::Search { .. } => "search",
            Self::SearchAll { .. } => "search_all",
            Self::Fuzzy { .. } => "fuzzy",
            Self::CreateVectorIndex { .. } => "create_vector_index",
            Self::Reindex { .. } => "reindex",
            Self::GetGraph { .. } => "get_graph",
            Self::Traverse { .. } => "traverse",
            Self::ShortestPath { .. } => "shortest_path",
            Self::AllShortestPaths { .. } => "all_shortest_paths",
            Self::GraphMatch { .. } => "graph_match",
            Self::GraphAlgorithm { .. } => "graph_algorithm",
            Self::Neighbors { .. } => "neighbors",
            Self::HybridSearch { .. } => "hybrid_search",
            Self::SetGraph { .. } => "set_graph",
            Self::DropGraph { .. } => "drop_graph",
            Self::ReindexGraph { .. } => "reindex_graph",
            Self::GraphRebuildStatus { .. } => "graph_rebuild_status",
            Self::CancelGraphRebuild { .. } => "cancel_graph_rebuild",
            Self::DropVectorIndex { .. } => "drop_vector_index",
            Self::ListVectorIndexes { .. } => "list_vector_indexes",
            Self::VectorSearch { .. } => "vector_search",
            Self::History { .. } => "history",
            Self::GetVersion { .. } => "get_version",
            Self::Batch { .. } => "batch",
            Self::WriteBatch { .. } => "write_batch",
            Self::ListCollections => "list_collections",
            Self::DatabaseStats => "database_stats",
            Self::GetQuota => "get_quota",
            Self::SetQuota { .. } => "set_quota",
            Self::CreateCollection { .. } => "create_collection",
            Self::DropCollection { .. } => "drop_collection",
            Self::GetCollectionTimeSeries { .. } => "get_collection_timeseries",
            Self::SetCollectionTimeSeries { .. } => "set_collection_timeseries",
            Self::SetCollectionTtl { .. } => "set_collection_ttl",
            Self::SetCollectionTtlAfter { .. } => "set_collection_ttl_after",
            Self::RetainTimeSeries { .. } => "retain_timeseries",
            Self::BackfillCollectionTtl { .. } => "backfill_collection_ttl",
            Self::GetCollectionTtl { .. } => "get_collection_ttl",
            Self::SetCollectionHistory { .. } => "set_collection_history",
            Self::GetCollectionHistory { .. } => "get_collection_history",
            Self::SetCollectionEvents { .. } => "set_collection_events",
            Self::GetCollectionEvents { .. } => "get_collection_events",
            Self::GetCollectionMetrics { .. } => "get_collection_metrics",
            Self::CreateSubscription { .. } => "create_subscription",
            Self::RenewSubscription { .. } => "renew_subscription",
            Self::DeleteSubscription { .. } => "delete_subscription",
            Self::ListSubscriptions { .. } => "list_subscriptions",
            Self::CreateWebhook { .. } => "create_webhook",
            Self::DeleteWebhook { .. } => "delete_webhook",
            Self::ListWebhooks => "list_webhooks",
            Self::ListWebhookDeliveries { .. } => "list_webhook_deliveries",
            Self::RetryWebhookDelivery { .. } => "retry_webhook_delivery",
            Self::ListDatabases => "list_databases",
            Self::CreateDatabase { .. } => "create_database",
            Self::DropDatabase { .. } => "drop_database",
            Self::Sync => "sync",
            Self::MapInfo => "map_info",
            Self::ResizeMap { .. } => "resize_map",
        }
    }

    /// Effective durability for this request. Non-write requests are Full.
    pub fn write_durability(&self) -> Durability {
        match self {
            Self::Insert { durability, .. }
            | Self::InsertMany { durability, .. }
            | Self::Update { durability, .. }
            | Self::FindAndModify { durability, .. }
            | Self::Delete { durability, .. }
            | Self::Aggregate { durability, .. }
            | Self::Sql { durability, .. }
            | Self::WriteBatch { durability, .. } => durability.unwrap_or_default(),
            _ => Durability::Full,
        }
    }

    pub fn is_read_only(&self) -> bool {
        match self {
            Self::Find { .. }
            | Self::FindOne { .. }
            | Self::Lookup { .. }
            | Self::ExplainLookup { .. }
            | Self::Watch { .. }
            | Self::Explain { .. }
            | Self::Count { .. }
            | Self::ExplainSql { .. }
            | Self::CursorFind { .. }
            | Self::CursorAggregate { .. }
            | Self::GetMore { .. }
            | Self::CloseCursor { .. }
            | Self::ListIndexes { .. }
            | Self::ListTextIndexes { .. }
            | Self::IndexStatus { .. }
            | Self::ListFuzzyIndexes { .. }
            | Self::ListVectorIndexes { .. }
            | Self::VectorSearch { .. }
            | Self::GetGraph { .. }
            | Self::Traverse { .. }
            | Self::ShortestPath { .. }
            | Self::AllShortestPaths { .. }
            | Self::GraphMatch { .. }
            | Self::GraphAlgorithm { .. }
            | Self::Neighbors { .. }
            | Self::HybridSearch { .. }
            | Self::GraphRebuildStatus { .. }
            | Self::Search { .. }
            | Self::SearchAll { .. }
            | Self::Fuzzy { .. }
            | Self::History { .. }
            | Self::GetVersion { .. }
            | Self::ListCollections
            | Self::DatabaseStats
            | Self::GetQuota
            | Self::GetCollectionTtl { .. }
            | Self::GetCollectionHistory { .. }
            | Self::GetCollectionEvents { .. }
            | Self::GetCollectionMetrics { .. }
            | Self::ListSubscriptions { .. }
            | Self::ListWebhooks
            | Self::ListWebhookDeliveries { .. }
            | Self::ListDatabases => true,
            Self::Aggregate { pipeline, .. } => nonodb_core::aggregate::write_stage_spec(pipeline)
                .ok()
                .flatten()
                .is_none(),
            Self::Sql { query, .. } => nonodb_core::sql::is_read_only(query).unwrap_or(false),
            Self::Batch { ops } => ops.iter().all(Self::is_read_only),
            _ => false,
        }
    }

    pub fn collections(&self) -> Vec<String> {
        match self {
            Self::Insert { coll, .. }
            | Self::InsertMany { coll, .. }
            | Self::Find { coll, .. }
            | Self::FindOne { coll, .. }
            | Self::Lookup { coll, .. }
            | Self::ExplainLookup { coll, .. }
            | Self::Watch { coll, .. }
            | Self::Explain { coll, .. }
            | Self::Update { coll, .. }
            | Self::FindAndModify { coll, .. }
            | Self::Delete { coll, .. }
            | Self::Count { coll, .. }
            | Self::CursorFind { coll, .. }
            | Self::CursorAggregate { coll, .. }
            | Self::CreateIndex { coll, .. }
            | Self::CreateIndexes { coll, .. }
            | Self::DropIndex { coll, .. }
            | Self::ListIndexes { coll }
            | Self::CreateTextIndex { coll, .. }
            | Self::DropTextIndex { coll, .. }
            | Self::ListTextIndexes { coll }
            | Self::IndexStatus { coll }
            | Self::CreateFuzzyIndex { coll, .. }
            | Self::DropFuzzyIndex { coll, .. }
            | Self::ListFuzzyIndexes { coll }
            | Self::CreateVectorIndex { coll, .. }
            | Self::Reindex { coll }
            | Self::DropVectorIndex { coll, .. }
            | Self::ListVectorIndexes { coll }
            | Self::VectorSearch { coll, .. }
            | Self::GetGraph { coll }
            | Self::Traverse { coll, .. }
            | Self::ShortestPath { coll, .. }
            | Self::AllShortestPaths { coll, .. }
            | Self::GraphMatch { coll, .. }
            | Self::GraphAlgorithm { coll, .. }
            | Self::Neighbors { coll, .. }
            | Self::HybridSearch { coll, .. }
            | Self::DropGraph { coll }
            | Self::ReindexGraph { coll, .. }
            | Self::GraphRebuildStatus { coll }
            | Self::CancelGraphRebuild { coll }
            | Self::Search { coll, .. }
            | Self::Fuzzy { coll, .. }
            | Self::History { coll, .. }
            | Self::GetVersion { coll, .. }
            | Self::CreateCollection { coll, .. }
            | Self::DropCollection { coll }
            | Self::GetCollectionTimeSeries { coll }
            | Self::SetCollectionTimeSeries { coll, .. }
            | Self::SetCollectionTtl { coll, .. }
            | Self::SetCollectionTtlAfter { coll, .. }
            | Self::RetainTimeSeries { coll, .. }
            | Self::BackfillCollectionTtl { coll, .. }
            | Self::GetCollectionTtl { coll }
            | Self::SetCollectionHistory { coll, .. }
            | Self::GetCollectionHistory { coll }
            | Self::SetCollectionEvents { coll, .. }
            | Self::GetCollectionEvents { coll }
            | Self::GetCollectionMetrics { coll }
            | Self::CreateSubscription { coll, .. }
            | Self::RenewSubscription { coll, .. }
            | Self::DeleteSubscription { coll, .. }
            | Self::ListSubscriptions { coll } => vec![coll.clone()],
            Self::SetGraph { coll, def } => {
                if def.edge_collection == *coll {
                    vec![coll.clone()]
                } else {
                    vec![coll.clone(), def.edge_collection.clone()]
                }
            }
            Self::SearchAll { request } => vec![request.coll.clone()],
            Self::CreateWebhook { rule } => vec![rule.coll.clone()],
            Self::DeleteWebhook { .. }
            | Self::ListWebhooks
            | Self::ListWebhookDeliveries { .. }
            | Self::RetryWebhookDelivery { .. } => Vec::new(),
            Self::Aggregate { coll, pipeline, .. } => {
                let mut out = vec![coll.clone()];
                if let Ok(Some(spec)) = nonodb_core::aggregate::write_stage_spec(pipeline) {
                    if spec.target != *coll {
                        out.push(spec.target);
                    }
                }
                out
            }
            Self::Sql { query, .. } | Self::ExplainSql { query, .. } => {
                nonodb_core::sql::collection_name(query)
                    .map(|coll| vec![coll])
                    .unwrap_or_default()
            }
            Self::Batch { ops } => {
                let mut out = Vec::new();
                for op in ops {
                    out.extend(op.collections());
                }
                out
            }
            Self::WriteBatch { ops, .. } => ops.iter().flat_map(|op| op.collections()).collect(),
            Self::GetMore { .. } | Self::CloseCursor { .. } => Vec::new(),
            Self::ListCollections
            | Self::DatabaseStats
            | Self::GetQuota
            | Self::SetQuota { .. }
            | Self::ListDatabases
            | Self::CreateDatabase { .. }
            | Self::DropDatabase { .. }
            | Self::Sync
            | Self::MapInfo
            | Self::ResizeMap { .. } => Vec::new(),
        }
    }
}

impl WriteOp {
    pub fn op_name(&self) -> &'static str {
        match self {
            Self::Insert { .. } => "insert",
            Self::InsertMany { .. } => "insert_many",
            Self::Update { .. } => "update",
            Self::Delete { .. } => "delete",
        }
    }

    pub fn collections(&self) -> Vec<String> {
        match self {
            Self::Insert { coll, .. }
            | Self::InsertMany { coll, .. }
            | Self::Update { coll, .. }
            | Self::Delete { coll, .. } => vec![coll.clone()],
        }
    }
}

impl WriteReq {
    pub(crate) fn is_index_build(&self) -> bool {
        match self {
            Self::CreateIndex { .. }
            | Self::CreateIndexes { .. }
            | Self::CreateSearchIndex { .. }
            | Self::CreateTextIndex { .. }
            | Self::CreateFuzzyIndex { .. }
            | Self::CreateVectorIndex { .. }
            | Self::Reindex { .. }
            | Self::ReindexGraph { .. }
            | Self::StartGraphRebuild { .. }
            | Self::GraphRebuildStep { .. } => true,
            Self::Batch { ops } => ops.iter().any(Self::is_index_build),
            _ => false,
        }
    }

    pub(crate) fn is_atomic(&self) -> bool {
        matches!(
            self,
            WriteReq::Batch { .. } | WriteReq::Sql { .. } | WriteReq::Aggregate { .. }
        )
    }
}

impl From<WriteOp> for WriteReq {
    fn from(op: WriteOp) -> Self {
        match op {
            WriteOp::Insert { coll, doc } => WriteReq::Insert {
                coll,
                doc,
                vault: None,
            },
            WriteOp::InsertMany { coll, docs } => WriteReq::InsertMany {
                coll,
                docs,
                vault: None,
            },
            WriteOp::Update {
                coll,
                filter,
                update,
                multi,
                upsert,
                array_filters,
            } => WriteReq::Update {
                coll,
                filter,
                update,
                multi,
                upsert,
                array_filters,
                default_id: None,
                vault: None,
            },
            WriteOp::Delete {
                coll,
                filter,
                multi,
            } => WriteReq::Delete {
                coll,
                filter,
                multi,
                vault: None,
            },
        }
    }
}

impl From<nonodb_core::Error> for ApiError {
    fn from(e: nonodb_core::Error) -> Self {
        use nonodb_core::Error as E;
        let code = match &e {
            E::DuplicateKey(_) => "duplicate_key",
            E::InvalidId => "invalid_id",
            E::Query(_) => "invalid_query",
            E::Timeout(_) => "timeout",
            E::Sql(_) => "invalid_sql",
            E::InvalidCollectionName(_) => "invalid_collection",
            E::DocumentTooLarge(_, _) => "document_too_large",
            E::ResultTooLarge(_, _) => "result_too_large",
            E::InvalidTtl(_) => "invalid_ttl",
            E::InvalidDate(_) => "invalid_date",
            E::InvalidNumeric(_) => "invalid_numeric",
            E::InvalidBinary(_) => "invalid_binary",
            E::InvalidObjectId(_) => "invalid_object_id",
            E::InvalidTimestamp(_) => "invalid_timestamp",
            E::InvalidRegex(_) => "invalid_regex",
            E::Json(_) | E::JsonFast(_) => "invalid_json",
            E::Lmdb(_) => "storage",
            E::Io(_) => "io",
            E::Vault { code, .. } => *code,
        };
        Self {
            code: code.to_string(),
            message: e.to_string(),
        }
    }
}

#[derive(Debug)]
pub struct ApiResponse {
    pub ok: bool,
    pub result: Option<Value>,
    /// Already-serialized `result` used by the raw `find` fast path.
    pub raw_result: Option<Box<RawValue>>,
    pub error: Option<ApiError>,
}

impl Serialize for ApiResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("ok", &self.ok)?;
        if let Some(raw) = &self.raw_result {
            map.serialize_entry("result", raw.as_ref())?;
        } else if let Some(result) = &self.result {
            map.serialize_entry("result", result)?;
        }
        if let Some(error) = &self.error {
            map.serialize_entry("error", error)?;
        }
        map.end()
    }
}

impl ApiResponse {
    pub fn ok(result: Value) -> Self {
        Self {
            ok: true,
            result: Some(result),
            raw_result: None,
            error: None,
        }
    }

    pub fn ok_raw(raw: Box<RawValue>) -> Self {
        Self {
            ok: true,
            result: None,
            raw_result: Some(raw),
            error: None,
        }
    }

    pub fn err(error: ApiError) -> Self {
        Self {
            ok: false,
            result: None,
            raw_result: None,
            error: Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_series_aliases_deserialize() {
        let get: Request = serde_json::from_value(serde_json::json!({
            "op": "get_collection_timeseries", "coll": "readings"
        }))
        .unwrap();
        assert!(matches!(get, Request::GetCollectionTimeSeries { coll } if coll == "readings"));

        let retain: Request = serde_json::from_value(serde_json::json!({
            "op": "retain_timeseries", "coll": "readings", "before": 0
        }))
        .unwrap();
        assert!(matches!(retain, Request::RetainTimeSeries { .. }));

        let set: Request = serde_json::from_value(serde_json::json!({
            "op": "set_collection_timeseries",
            "coll": "readings",
            "timeseries": {"timeField": "ts"}
        }))
        .unwrap();
        assert!(
            matches!(set, Request::SetCollectionTimeSeries { coll, settings } if coll == "readings" && settings.time_field == "ts")
        );

        let canonical: Request = serde_json::from_value(serde_json::json!({
            "op": "set_collection_time_series",
            "coll": "readings",
            "settings": {"timeField": "ts"}
        }))
        .unwrap();
        assert!(matches!(canonical, Request::SetCollectionTimeSeries { .. }));
    }

    #[test]
    fn raw_result_is_serialized_under_result() {
        let raw = RawValue::from_string(r#"[{"_id":"a","n":1}]"#.to_string()).unwrap();
        let response = ApiResponse::ok_raw(raw);
        assert_eq!(
            serde_json::to_string(&response).unwrap(),
            r#"{"ok":true,"result":[{"_id":"a","n":1}]}"#
        );
    }
}
