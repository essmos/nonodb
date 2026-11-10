//! HTTP/JSON protocol: requests, responses, and API errors.

use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    Update {
        coll: String,
        #[serde(default)]
        filter: Value,
        update: Value,
        #[serde(default)]
        multi: bool,
        #[serde(default)]
        upsert: bool,
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
    },
    Delete {
        coll: String,
        #[serde(default)]
        filter: Value,
        #[serde(default)]
        multi: bool,
    },
}

/// Write operations passed to the single writer task.
#[derive(Debug)]
pub enum WriteReq {
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
        filter: Value,
        update: Value,
        multi: bool,
        upsert: bool,
    },
    Delete {
        coll: String,
        filter: Value,
        multi: bool,
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
            WriteOp::Insert { coll, doc } => WriteReq::Insert { coll, doc },
            WriteOp::InsertMany { coll, docs } => WriteReq::InsertMany { coll, docs },
            WriteOp::Update {
                coll,
                filter,
                update,
                multi,
                upsert,
            } => WriteReq::Update {
                coll,
                filter,
                update,
                multi,
                upsert,
            },
            WriteOp::Delete {
                coll,
                filter,
                multi,
            } => WriteReq::Delete {
                coll,
                filter,
                multi,
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
            E::Json(_) | E::JsonFast(_) => "invalid_json",
            E::Lmdb(_) => "storage",
            E::Io(_) => "io",
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
