//! nonodb-core — dokumentowa warstwa nad LMDB.
//!
//! Warstwa storage nie zna protokołu (HTTP/JSON po UDS) ani daemona — dostaje
//! dokumenty jako `serde_json::Value` i operuje na sub-bazach LMDB.
//!
//! Układ sub-baz:
//!   `meta`               — rejestr kolekcji (klucz `collection:<nazwa>`);
//!   `coll:<nazwa>`       — `_id` (string) -> dokument (JSON);
//!   `idx:<coll>:<pole>`  — indeks wtórny (etap 4.0);
//!   `ft:<coll>:<pole>`   — postings BM25 (full-text);
//!   `fz:<coll>:<pole>`   — trigramy (fuzzy search).

pub mod collection;
pub mod error;
pub mod index;
pub mod query;
pub mod search;
pub mod store;

pub use collection::{Collection, FindQuery, UpdateOutcome};
pub use error::{Error, Result};
pub use index::{IndexDef, RangeBytes};
pub use search::DEFAULT_FUZZY_DISTANCE;
pub use store::{Store, MAP_SIZE, MAX_DBS, MAX_DOCUMENT_BYTES, META_DB, PAGE_SIZE};

// Re-eksport `heed`, aby warstwa daemona mogła operować na `RwTxn`
// bez osobnej zależności (spójność wersji).
pub use heed;
