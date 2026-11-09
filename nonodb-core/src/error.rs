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

    #[error("invalid collection name: {0:?}")]
    InvalidCollectionName(String),

    #[error("duplicate key: {0}")]
    DuplicateKey(String),

    #[error("document `_id` must be a string")]
    InvalidId,

    #[error("document too large: {0} bytes (limit {1})")]
    DocumentTooLarge(usize, usize),
}
