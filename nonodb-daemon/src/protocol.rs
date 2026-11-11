//! HTTP/JSON protocol: requests, responses, and API errors.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use nonodb_core::{CollectionProfile, VaultKey};

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

/// Request to `/v1/exec`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    Insert {
        coll: String,
        doc: Value,
    },
    InsertMany {
        coll: String,
        docs: Vec<Value>,
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
    },
    FindOne {
        coll: String,
        #[serde(default)]
        filter: Value,
        #[serde(default)]
        projection: Option<Value>,
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
    },
    Delete {
        coll: String,
        #[serde(default)]
        filter: Value,
        #[serde(default)]
        multi: bool,
    },
    Count {
        coll: String,
        #[serde(default)]
        filter: Value,
    },
    /// Read-only aggregation pipeline.
    Aggregate {
        coll: String,
        #[serde(default)]
        pipeline: Vec<Value>,
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
        include: Vec<String>,
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
    },
    ListCollections,
    /// Creates a collection explicitly. `profile` is `standard` (default) or `vault`.
    CreateCollection {
        coll: String,
        #[serde(default)]
        profile: Option<String>,
        #[serde(default)]
        ttl_seconds: Option<u64>,
        /// Purge stored history on TTL expiry instead of adding a tombstone.
        #[serde(default)]
        ttl_purge_history: Option<bool>,
    },
    /// Sets or clears the default document TTL for a collection.
    SetCollectionTtl {
        coll: String,
        #[serde(default)]
        ttl_seconds: Option<u64>,
        #[serde(default)]
        purge_history: Option<bool>,
    },
    /// Returns the collection default TTL.
    GetCollectionTtl {
        coll: String,
    },
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
    },
    SetCollectionTtl {
        coll: String,
        ttl_seconds: Option<u64>,
        purge_history: Option<bool>,
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
        include: Vec<String>,
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
}

impl WriteReq {
    pub(crate) fn is_atomic(&self) -> bool {
        matches!(self, WriteReq::Batch { .. })
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
            E::InvalidCollectionName(_) => "invalid_collection",
            E::DocumentTooLarge(_, _) => "document_too_large",
            E::InvalidTtl(_) => "invalid_ttl",
            E::InvalidDate(_) => "invalid_date",
            E::InvalidNumeric(_) => "invalid_numeric",
            E::InvalidBinary(_) => "invalid_binary",
            E::InvalidObjectId(_) => "invalid_object_id",
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

#[derive(Debug, Serialize)]
pub struct ApiResponse {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiError>,
}

impl ApiResponse {
    pub fn ok(result: Value) -> Self {
        Self {
            ok: true,
            result: Some(result),
            error: None,
        }
    }

    pub fn err(error: ApiError) -> Self {
        Self {
            ok: false,
            result: None,
            error: Some(error),
        }
    }
}
