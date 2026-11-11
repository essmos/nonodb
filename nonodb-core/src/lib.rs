//! nonodb-core - the document layer on top of LMDB.
//!
//! The storage layer knows nothing about the protocol (HTTP/JSON over UDS) or
//! the daemon - it receives documents as `serde_json::Value` and operates on LMDB sub-databases.
//!
//! Sub-database layout:
//!   `meta`               - collection registry and index/slot definitions;
//!   `history`            - collection + `_id` + revision -> revision JSON;
//!   `ttl`                - expiry metadata;
//!   `coll:<name>`        - `_id` (string) -> document (JSON);
//!   `dbslot:<n>`         - reusable secondary/text/fuzzy index slot.

pub mod aggregate;
pub mod bson_types;
pub mod collection;
pub mod date;
pub mod error;
pub mod geo;
pub mod history;
pub mod index;
pub mod numeric;
pub mod progress;
pub mod query;
mod regex;
pub mod search;
pub mod store;
pub mod ttl;
pub mod vault;
pub mod vector;
pub mod verify;

pub use aggregate::MAX_AGGREGATE_DOCS;
pub use collection::{
    project_document, Collection, FindAndModifyOptions, FindAndModifyOutcome, FindQuery, IndexSpec,
    UpdateOutcome, MAX_LOOKUP_RESPONSE_BYTES, MAX_LOOKUP_VALUES,
};
pub use error::{Error, Result};
pub use history::{HistoryConfig, HistoryEntry, DEFAULT_HISTORY_MAX_VERSIONS};
pub use index::{IndexDef, RangeBytes};
pub use search::DEFAULT_FUZZY_DISTANCE;
pub use store::{
    parse_size, ReadTxn, Store, WriteTxn, MAP_SIZE, MAX_DBS, MAX_DOCUMENT_BYTES, META_DB, PAGE_SIZE,
};
pub use ttl::DueEntry;
pub use vault::{CollectionProfile, VaultKey};
pub use vector::{VectorDef, VectorMetric, MAX_VECTOR_DIMS};
pub use verify::{
    verify_backup, verify_data_dir, BackupManifest, BackupVerification, CollectionVerification,
    IndexVerification, VerificationReport, MANIFEST_FILE, MANIFEST_VERSION,
};

// Re-export `heed` so the daemon layer can operate on `RwTxn`
// without a separate dependency (version consistency).
pub use heed;
