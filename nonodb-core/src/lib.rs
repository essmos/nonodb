//! nonodb-core - the document layer on top of LMDB.
//!
//! The storage layer knows nothing about the protocol (HTTP/JSON over UDS) or
//! the daemon - it receives documents as `serde_json::Value` and operates on LMDB sub-databases.
//!
//! Sub-database layout:
//!   `meta`               - collection registry (key `collection:<name>`);
//!   `history`            - collection + `_id` + revision -> revision JSON;
//!   `coll:<name>`        - `_id` (string) -> document (JSON);
//!   `idx:<coll>:<field>` - secondary index;
//!   `ft:<coll>:<field>`  - BM25 postings (full-text);
//!   `fz:<coll>:<field>`  - trigrams (fuzzy search).

pub mod aggregate;
pub mod collection;
pub mod error;
pub mod history;
pub mod index;
pub mod query;
pub mod search;
pub mod store;
pub mod vault;

pub use aggregate::MAX_AGGREGATE_DOCS;
pub use collection::{Collection, FindQuery, UpdateOutcome};
pub use error::{Error, Result};
pub use history::{HistoryConfig, HistoryEntry, DEFAULT_HISTORY_MAX_VERSIONS};
pub use index::{IndexDef, RangeBytes};
pub use search::DEFAULT_FUZZY_DISTANCE;
pub use store::{Store, MAP_SIZE, MAX_DBS, MAX_DOCUMENT_BYTES, META_DB, PAGE_SIZE};
pub use vault::{CollectionProfile, VaultKey};

// Re-export `heed` so the daemon layer can operate on `RwTxn`
// without a separate dependency (version consistency).
pub use heed;
