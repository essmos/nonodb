use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("lmdb: {0}")]
    Lmdb(#[from] heed::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("json: {0}")]
    JsonFast(String),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid query: {0}")]
    Query(String),

    #[error("timeout: {0}")]
    Timeout(String),

    #[error("invalid SQL: {0}")]
    Sql(String),

    #[error("invalid collection name: {0:?}")]
    InvalidCollectionName(String),

    #[error("duplicate key: {0}")]
    DuplicateKey(String),

    #[error("document `_id` must be a string")]
    InvalidId,

    #[error("document too large: {0} bytes (limit {1})")]
    DocumentTooLarge(usize, usize),

    #[error("result too large: {0} bytes (limit {1})")]
    ResultTooLarge(usize, usize),

    #[error("invalid TTL: {0}")]
    InvalidTtl(String),

    #[error("invalid date: {0}")]
    InvalidDate(String),

    #[error("invalid numeric: {0}")]
    InvalidNumeric(String),

    #[error("invalid binary: {0}")]
    InvalidBinary(String),

    #[error("invalid object id: {0}")]
    InvalidObjectId(String),

    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(String),

    #[error("invalid regex: {0}")]
    InvalidRegex(String),

    /// Vault/encryption error with a stable API code.
    #[error("{message}")]
    Vault { code: &'static str, message: String },

    /// A consumer group fell behind the retained part of its stream.
    #[error("stream truncated: {0}")]
    StreamTruncated(String),

    /// A commit presented a lease that no longer owns the batch.
    #[error("stale lease: {0}")]
    StaleLease(String),

    /// A commit moved a group offset backwards or past the stream head.
    #[error("commit out of order: {0}")]
    CommitOutOfOrder(String),
}
