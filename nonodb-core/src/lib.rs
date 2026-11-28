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
pub mod backup_compress;
pub mod backup_crypto;
pub mod backup_info;
pub mod bson_types;
pub mod collection;
pub mod cypher;
pub mod date;
pub mod error;
pub mod events;
pub mod geo;
pub mod graph;
pub mod history;
pub mod id;
pub mod index;
pub mod instance;
pub mod numeric;
pub mod observer;
pub mod pitr;
pub mod progress;
pub mod query;
mod regex;
pub mod replication;
pub mod search;
pub mod search_all;
pub mod search_compat;
pub mod search_segment;
pub mod sql;
pub mod store;
pub mod streams;
pub mod subscriptions;
pub mod timeseries;
pub mod ttl;
pub mod vault;
pub mod vector;
pub mod vector_ann;
pub mod verify;

pub use aggregate::MAX_AGGREGATE_DOCS;
pub use backup_info::INFO_FILE as BACKUP_INFO_FILE;
pub use collection::{
    project_document, Collection, FindAndModifyOptions, FindAndModifyOutcome, FindQuery, IndexSpec,
    LookupStats, ReindexReport, UpdateOutcome, MAX_LOOKUP_RESPONSE_BYTES, MAX_LOOKUP_VALUES,
};
pub use error::{Error, Result};
pub use history::{HistoryEntry, DEFAULT_HISTORY_MAX_VERSIONS};
pub use index::{IndexDef, RangeBytes};
pub use replication::{ReplicationChange, ReplicationTxn, REPLICATION_DB};
pub use search::{
    SearchField, SearchIndexDef, SearchKind, TextQuery, TextQueryMode, DEFAULT_FUZZY_DISTANCE,
};
pub use search_all::{SearchAllRequest, SearchAllResponse, SearchMode};
pub use store::{
    parse_size, sync_interval_ms, sync_policy, ReadTxn, Store, SyncPolicy, WriteTxn, CONTROL_DB,
    MAP_SIZE, MAX_DBS, MAX_DOCUMENT_BYTES, META_DB, PAGE_SIZE,
};
pub use timeseries::{
    Compression as TimeSeriesCompression, Granularity as TimeSeriesGranularity,
    Settings as TimeSeriesSettings, Stats as TimeSeriesStats,
};
pub use ttl::DueEntry;
pub use vault::{CollectionProfile, VaultKey};
pub use vector::{VectorDef, VectorMetric, MAX_VECTOR_DIMS};
pub use verify::{
    verify_backup, verify_data_dir, verify_data_dir_allow_missing_search, BackupCompression,
    BackupManifest, BackupVerification, CollectionVerification, IndexVerification,
    VerificationReport, MANIFEST_FILE, MANIFEST_VERSION,
};

// Re-export `heed` so the daemon layer can operate on `RwTxn`
// without a separate dependency (version consistency).
pub use heed;
