//! Document collection operations.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use heed::types::Bytes;
use heed::{Database, Env, RoTxn, RwTxn};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use crate::aggregate;
use crate::bson_types;
use crate::cypher::CypherOptions;
use crate::date;
use crate::error::{Error, Result};
use crate::events::{self, EventConfig, EVENTS_COLLECTION};
use crate::geo;
use crate::graph::{
    self, AdjEdge, AllShortestPathsOptions, GraphAlgorithmObserver, GraphAlgorithmOptions,
    GraphDef, GraphDirection, GraphMatchOptions, GraphRebuildState, GraphView, HybridOptions,
    NeighborsOptions, ShortestPathOptions, TraverseOptions,
};
use crate::history::{self, HistoryEntry};
use crate::id;
use crate::index::{self, IndexDef};
use crate::numeric;
use crate::query::{
    apply_upsert_with_options, apply_with_options, matches, path, seed_from_filter,
};
use crate::search::{self, SearchIndexDef, SearchKind, TextQueryMode};
use crate::search_segment::{
    DocVerifier, FuzzySegmentBuilder, FuzzySegmentReader, SegmentBuilder, SegmentReader, SegmentSet,
};
use crate::store::MAX_DOCUMENT_BYTES;
use crate::subscriptions::{self, Subscription};
use crate::ttl::{self, TtlUpdate};
use crate::vault::{self, CollectionProfile, VaultKey};
use crate::vector::{self, Candidate, VectorAnnDef, VectorDef, VectorMetric};
use crate::vector_ann;
use crate::verify::{CollectionVerification, IndexVerification};

/// Meta prefix for reusable physical sub-database slots.
const DB_SLOT_META_PREFIX: &str = "\u{1}dbslot:";
/// Reserve some LMDB sub-database slots for collections and fixed databases.
const MAX_DB_SLOTS: u32 = crate::store::MAX_DBS - 64;

/// Maximum number of indexed `_id`s used to prefilter a text search.
const SEARCH_FILTER_CANDIDATE_LIMIT: usize = 10_000_000;

/// Durable manifest for one segment-backed text index.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SegmentManifest {
    /// Paths relative to the LMDB data directory.
    segments: Vec<String>,
    /// id -> number of segments that existed when the id was deleted.
    tombstones: HashMap<String, usize>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct FuzzySegmentManifest {
    segments: Vec<String>,
    values: Vec<String>,
    tombstones: HashMap<String, usize>,
}

const SEGMENT_DELTA_FLUSH_DOCS: u64 = 4096;
const SEGMENT_COMPACT_MIN_SEGMENTS: usize = 4;
const SEGMENT_COMPACT_MAX_DOCS: u32 = 200_000;
const VECTOR_ANN_DELTA_MAX: usize = 4096;

fn fuzzy_segment_index_dir(coll: &str, index: &str) -> String {
    let digest = Sha256::digest(format!("fuzzy\0{coll}\0{index}").as_bytes());
    format!("search/{digest:x}")
}

fn segment_index_dir(coll: &str, index: &str) -> String {
    let digest = Sha256::digest(format!("{coll}\0{index}").as_bytes());
    format!("search/{digest:x}")
}

fn vector_ann_index_dir(coll: &str, field: &str) -> String {
    let digest = Sha256::digest(format!("vector\0{coll}\0{field}").as_bytes());
    format!("vector-ann/{digest:x}")
}

/// Maximum number of keys accepted by one `lookup` request.
pub const MAX_LOOKUP_VALUES: usize = 10_000;

/// Maximum serialized size of the `lookup` result payload.
pub const MAX_LOOKUP_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

/// Execution counters for one ordered point lookup.
#[derive(Debug, Clone, Default)]
pub struct LookupStats {
    /// Selected primary or secondary index name.
    pub selected_index: String,
    /// Fields in the selected index order.
    pub index_fields: Vec<String>,
    /// Covering fields copied into the index value.
    pub index_include: Vec<String>,
    /// Whether the selected index is the primary `_id` index.
    pub primary: bool,
    /// Whether the projection was served without reading documents.
    pub covering: bool,
    /// Number of keys in the request, including duplicates.
    pub requested_keys: usize,
    /// Number of distinct keys after deduplication.
    pub unique_keys: usize,
    /// Non-null results returned, including duplicate inputs.
    pub found: usize,
    /// Null results returned.
    pub missing: usize,
    /// Index entries examined while resolving keys.
    pub index_entries_read: u64,
    /// Full documents fetched from the main collection.
    pub documents_read: u64,
    /// Stored document or index-value bytes read.
    pub bytes_read: u64,
    /// Stored bytes represented by the returned results.
    pub result_bytes: u64,
}

/// Minimum document count for a sorted full scan to use multiple threads.
#[cfg(not(test))]
const PARALLEL_SCAN_MIN_ENTRIES: usize = 50_000;
#[cfg(test)]
const PARALLEL_SCAN_MIN_ENTRIES: usize = 200;

/// Maximum `skip + limit` for the parallel path. With a deep `skip`
/// each thread would have to keep its own large TopK; it is better to stay on
/// one thread and keep memory use bounded.
const PARALLEL_SCAN_MAX_WANT: usize = 50_000;

/// Minimum `skip + limit` for a parallel scan without sorting to be worthwhile.
/// For `find_one` and very small limits, early exit is usually faster.
const PARALLEL_UNSORTED_MIN_WANT: usize = 16;

/// Size of the raw-byte chunk passed to worker threads.
const PARALLEL_SCAN_CHUNK: usize = 8192;

/// Minimum collection size before single-term text and exact vector searches
/// use worker threads.
#[cfg(not(test))]
const PARALLEL_SEARCH_MIN_ENTRIES: usize = 50_000;
#[cfg(test)]
const PARALLEL_SEARCH_MIN_ENTRIES: usize = 200;

/// Maximum result window for a parallel search. Larger pages are better served
/// by the serial streaming path.
const PARALLEL_SEARCH_MAX_WANT: usize = 10_000;

/// Posting/vector entries per search worker chunk.
const PARALLEL_SEARCH_CHUNK: usize = 8192;

/// Compact worker chunk: one byte buffer plus `(offset, key length, value length)`
/// entries. Keeping raw keys/values in one allocation avoids per-posting and
/// per-vector `String`/`Vec` allocations in the coordinator.
struct SearchChunk {
    buffer: Vec<u8>,
    entries: Vec<(u32, u32, u32)>,
}

impl SearchChunk {
    fn with_capacity(entries: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(entries * 32),
            entries: Vec::with_capacity(entries),
        }
    }

    fn push(&mut self, key: &[u8], value: &[u8]) {
        let offset = self.buffer.len() as u32;
        self.buffer.extend_from_slice(key);
        self.buffer.extend_from_slice(value);
        self.entries
            .push((offset, key.len() as u32, value.len() as u32));
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Soft byte cap for one index-backfill chunk. Large documents stop a chunk
/// early so the bounded worker queue cannot hold gigabytes of raw documents.
const BACKFILL_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// Default cap for worker threads on large scans/backfills.
const DEFAULT_WORKERS: usize = 16;
/// Hard cap for configurable worker counts, to avoid runaway memory use.
const MAX_WORKERS: usize = 256;

fn configured_workers(var: &str) -> usize {
    let detected = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let default = detected.clamp(1, DEFAULT_WORKERS);
    std::env::var(var)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.min(MAX_WORKERS))
        .unwrap_or(default)
}

pub(crate) fn scan_workers() -> usize {
    configured_workers("NONODB_SCAN_WORKERS")
}

fn backfill_workers() -> usize {
    configured_workers("NONODB_BACKFILL_WORKERS")
}

pub(crate) fn aggregate_workers() -> usize {
    configured_workers("NONODB_AGGREGATE_WORKERS")
}

fn search_workers() -> usize {
    configured_workers("NONODB_SEARCH_WORKERS")
}

/// Scan chunk: one byte buffer plus document offsets (fewer allocations).
struct ScanChunk {
    idx: usize,
    data: Vec<u8>,
    docs: Vec<(usize, u32, u32)>,
}

/// Cheap full-scan prefilter: for simple string/bool equalities, checks
/// whether the key and value appear in the raw JSON at all. False positives are
/// acceptable (the matcher runs afterwards), but false negatives are not
/// allowed. Numbers and `null` are skipped because their semantics/formatting
/// differences require the full parser.
#[derive(Debug, Clone, Default)]
pub(crate) struct RawPrefilter {
    needles: Vec<Vec<u8>>,
}

impl RawPrefilter {
    fn matches(&self, bytes: &[u8]) -> bool {
        self.needles
            .iter()
            .all(|needle| memchr::memmem::find(bytes, needle).is_some())
    }
}

/// Builds a prefilter for safe equality conditions. Conditions we cannot
/// check at the byte level are ignored (the filter is an AND, so this is safe).
pub(crate) fn raw_prefilter(filter: &Value) -> Option<RawPrefilter> {
    let obj = filter.as_object()?;
    let mut needles = Vec::new();
    for (key, cond) in obj {
        if key.starts_with('$') {
            // `$or` / `$not` / `$nor` cannot be safely ignored.
            return None;
        }
        if key.contains('.') || key == "_id" {
            continue;
        }
        let Some(value) = raw_equality_value(cond) else {
            continue;
        };
        let key_bytes = serde_json::to_vec(&Value::String(key.clone())).ok()?;
        let value_bytes = serde_json::to_vec(value).ok()?;
        if !needles.contains(&key_bytes) {
            needles.push(key_bytes);
        }
        if !needles.contains(&value_bytes) {
            needles.push(value_bytes);
        }
    }
    if needles.is_empty() {
        None
    } else {
        Some(RawPrefilter { needles })
    }
}

/// Returns the equality value if it can be safely found in the raw
/// JSON. Only strings and booleans are supported; the rest goes to the parser.
fn raw_equality_value(cond: &Value) -> Option<&Value> {
    match cond {
        Value::String(_) | Value::Bool(_) => Some(cond),
        Value::Object(ops) if ops.len() == 1 => match ops.get("$eq") {
            Some(Value::String(_)) | Some(Value::Bool(_)) => ops.get("$eq"),
            _ => None,
        },
        _ => None,
    }
}

/// Parses a document only when the prefilter did not reject it.
pub(crate) fn parse_prefiltered(bytes: &[u8], raw: Option<&RawPrefilter>) -> Result<Option<Value>> {
    if raw.is_some_and(|r| !r.matches(bytes)) {
        return Ok(None);
    }
    parse_json(bytes).map(Some)
}

/// Fast JSON parsing (SIMD) into `serde_json::Value`.
fn parse_json(bytes: &[u8]) -> Result<Value> {
    sonic_rs::from_slice::<Value>(bytes).map_err(|e| Error::JsonFast(e.to_string()))
}

/// `update` operation result (with optional upsert).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateOutcome {
    pub matched: usize,
    pub modified: usize,
    pub upserted_id: Option<String>,
    /// `(old_id, new_id)` rows whose time or series changed (time-series only).
    pub moved: Vec<(String, String)>,
}

/// Options for an atomic find-and-modify operation.
#[derive(Debug, Clone)]
pub struct FindAndModifyOptions<'a> {
    pub filter: &'a Value,
    /// Update document, or `None` when `remove` is set.
    pub update: Option<&'a Value>,
    pub sort: &'a [(String, i32)],
    /// Return the document after the operation instead of before it.
    pub new: bool,
    pub upsert: bool,
    pub remove: bool,
    pub array_filters: &'a [Value],
    pub projection: Option<&'a Value>,
}

/// Result of an atomic find-and-modify operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindAndModifyOutcome {
    /// Pre-image when `new = false`, post-image when `new = true`.
    pub value: Option<Value>,
    pub matched: usize,
    pub modified: usize,
    pub upserted_id: Option<String>,
    /// New synthetic `_id` when a time-series update moved the row.
    pub new_id: Option<String>,
}
type IndexIter<'a> = Box<dyn Iterator<Item = heed::Result<(&'a [u8], &'a [u8])>> + 'a>;
type IdBounds = (Bound<Vec<u8>>, Bound<Vec<u8>>);

/// Backfill chunk: a buffer of `_id` values plus documents.
struct BackfillChunk {
    data: Vec<u8>,
    docs: Vec<(u32, u32, u32, u32)>, // id_start, id_len, doc_start, doc_len
}

fn push_backfill_doc(chunk: &mut BackfillChunk, id_bytes: &[u8], doc_bytes: &[u8]) {
    let id_start = chunk.data.len() as u32;
    chunk.data.extend_from_slice(id_bytes);
    let id_len = id_bytes.len() as u32;
    let doc_start = chunk.data.len() as u32;
    chunk.data.extend_from_slice(doc_bytes);
    let doc_len = doc_bytes.len() as u32;
    chunk.docs.push((id_start, id_len, doc_start, doc_len));
}

/// Selected index and key range for a filter.
struct IndexPlan {
    db: Database<Bytes, Bytes>,
    bounds: index::RangeBytes,
    def: IndexDef,
}

/// Selected geo index and the cell ranges to scan.
struct GeoIndexPlan {
    db: Database<Bytes, Bytes>,
    ranges: Vec<index::RangeBytes>,
    def: IndexDef,
}
type PostingLocator = (u64, u32, i64, u16);

const POSTING_TAG: [u8; 2] = [0, 0];

fn posting_key(vb: &[u8], locator: PostingLocator) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + vb.len() + 22);
    key.extend_from_slice(&POSTING_TAG);
    key.extend_from_slice(vb);
    key.extend_from_slice(&locator.0.to_be_bytes());
    key.extend_from_slice(&locator.1.to_be_bytes());
    key.extend_from_slice(&locator.2.to_be_bytes());
    key.extend_from_slice(&locator.3.to_be_bytes());
    key
}

fn posting_locator(key: &[u8]) -> Option<PostingLocator> {
    if key.len() < 24 || key[..2] != POSTING_TAG {
        return None;
    }
    let tail = &key[key.len() - 22..];
    Some((
        u64::from_be_bytes(tail[0..8].try_into().ok()?),
        u32::from_be_bytes(tail[8..12].try_into().ok()?),
        i64::from_be_bytes(tail[12..20].try_into().ok()?),
        u16::from_be_bytes(tail[20..22].try_into().ok()?),
    ))
}

fn posting_bounds(bounds: &index::RangeBytes) -> index::RangeBytes {
    let map = |bound: &Bound<Vec<u8>>| match bound {
        Bound::Included(value) => {
            let mut out = POSTING_TAG.to_vec();
            out.extend_from_slice(value);
            Bound::Included(out)
        }
        Bound::Excluded(value) => {
            let mut out = POSTING_TAG.to_vec();
            out.extend_from_slice(value);
            Bound::Excluded(out)
        }
        Bound::Unbounded => Bound::Unbounded,
    };
    (map(&bounds.0), map(&bounds.1))
}

fn posting_update(
    db: &Database<Bytes, Bytes>,
    txn: &mut RwTxn<'_>,
    vb: &[u8],
    locator: PostingLocator,
    seq: u32,
    add: bool,
) -> Result<()> {
    let key = posting_key(vb, locator);
    let mut bits = db.get(&*txn, &key)?.unwrap_or_default().to_vec();
    let byte = (seq / 8) as usize;
    let mask = 1u8 << (seq % 8);
    if add {
        if bits.len() <= byte {
            bits.resize(byte + 1, 0);
        }
        bits[byte] |= mask;
    } else if let Some(value) = bits.get_mut(byte) {
        *value &= !mask;
        while bits.last() == Some(&0) {
            bits.pop();
        }
    }
    if bits.is_empty() {
        db.delete(&mut *txn, &key)?;
    } else {
        db.put(&mut *txn, &key, &bits)?;
    }
    Ok(())
}

fn posting_conflict(
    db: &Database<Bytes, Bytes>,
    txn: &RoTxn<'_>,
    vb: &[u8],
    allowed: &HashSet<(PostingLocator, u32)>,
) -> Result<bool> {
    let mut prefix = POSTING_TAG.to_vec();
    prefix.extend_from_slice(vb);
    for item in db.prefix_iter(txn, prefix.as_slice())? {
        let (key, bits) = item?;
        let Some(locator) = posting_locator(key) else {
            continue;
        };
        for (byte, &bits_byte) in bits.iter().enumerate() {
            let mut value = bits_byte;
            while value != 0 {
                let bit = value.trailing_zeros();
                let seq = (byte * 8) as u32 + bit;
                if !allowed.contains(&(locator, seq)) {
                    return Ok(true);
                }
                value &= value - 1;
            }
        }
    }
    Ok(false)
}

fn posting_ids(
    db: &Database<Bytes, Bytes>,
    txn: &RoTxn<'_>,
    bounds: &index::RangeBytes,
) -> Result<HashSet<String>> {
    let posting_bounds = posting_bounds(bounds);
    let range = (as_bound(&posting_bounds.0), as_bound(&posting_bounds.1));
    let mut ids = HashSet::new();
    for item in db.range(txn, &range)? {
        let (key, bits) = item?;
        let Some(locator) = posting_locator(key) else {
            continue;
        };
        for (byte, &bits_byte) in bits.iter().enumerate() {
            let mut value = bits_byte;
            while value != 0 {
                let bit = value.trailing_zeros();
                let seq = (byte * 8) as u32 + bit;
                ids.insert(crate::timeseries::row_id(
                    locator.0, locator.1, locator.2, locator.3, seq,
                ));
                value &= value - 1;
            }
        }
    }
    Ok(ids)
}

/// `find` query parameters.
#[derive(Debug, Clone, Default)]
pub struct FindQuery {
    /// MongoDB-style filter; `Null`/`{}` = match everything.
    pub filter: Value,
    /// Field projection (currently flat, top-level).
    pub projection: Option<Value>,
    /// Sort keys: `(path, 1 | -1)`.
    pub sort: Vec<(String, i32)>,
    pub skip: usize,
    pub limit: Option<usize>,
    /// Optional application comment, echoed by `explain`.
    pub comment: Option<String>,
    /// Maximum server-side execution time in milliseconds.
    pub max_time_ms: Option<u64>,
    /// Optional index hint (`"name"`, `{"field": 1}`, or `{"$natural": 1}`).
    pub hint: Option<Value>,
}

fn operation_deadline(max_time_ms: Option<u64>) -> Option<Instant> {
    max_time_ms
        .filter(|ms| *ms > 0)
        .and_then(|ms| Instant::now().checked_add(Duration::from_millis(ms)))
}

fn find_deadline(query: &FindQuery) -> Option<Instant> {
    operation_deadline(query.max_time_ms)
}

fn deadline_remaining_ms(deadline: Option<Instant>) -> Option<u64> {
    deadline.map(|deadline| {
        u64::try_from(
            deadline
                .saturating_duration_since(Instant::now())
                .as_millis(),
        )
        .unwrap_or(u64::MAX)
        .max(1)
    })
}

fn check_find_deadline(deadline: Option<Instant>) -> Result<()> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        Err(Error::Timeout("operation exceeded maxTimeMS".into()))
    } else {
        Ok(())
    }
}

/// Parsed MongoDB-style index hint.
#[derive(Debug, Clone, PartialEq, Eq)]
enum IndexHint {
    Natural,
    Name(String),
    Fields(Vec<String>),
}

fn parse_index_hint(hint: &Value) -> Result<IndexHint> {
    match hint {
        Value::String(name) if !name.is_empty() => Ok(IndexHint::Name(name.clone())),
        Value::Object(obj) => {
            if let Some(direction) = obj.get("$natural") {
                if obj.len() != 1 || !matches!(direction.as_i64(), Some(1 | -1)) {
                    return Err(Error::Query(
                        "$natural hint must be {\"$natural\": 1} or {\"$natural\": -1}".into(),
                    ));
                }
                return Ok(IndexHint::Natural);
            }
            if obj.is_empty() {
                return Err(Error::Query("index hint cannot be empty".into()));
            }
            for key in obj.keys() {
                if key.starts_with('$') {
                    return Err(Error::Query(format!(
                        "unsupported index hint operator `{key}`"
                    )));
                }
            }
            Ok(IndexHint::Fields(obj.keys().cloned().collect()))
        }
        _ => Err(Error::Query(
            "index hint must be a string, an object, or {$natural: 1}".into(),
        )),
    }
}

fn normalize_natural_sort(sort: &[(String, i32)]) -> Result<Option<Vec<(String, i32)>>> {
    if !sort.iter().any(|(field, _)| field == "$natural") {
        return Ok(None);
    }
    if sort.len() != 1 {
        return Err(Error::Query("$natural must be the only sort key".into()));
    }
    let direction = sort[0].1;
    if direction != 1 && direction != -1 {
        return Err(Error::Query(
            "$natural sort direction must be 1 or -1".into(),
        ));
    }
    Ok(Some(vec![("_id".to_string(), direction)]))
}

fn normalized_find_query(query: &FindQuery) -> Result<Cow<'_, FindQuery>> {
    let Some(sort) = normalize_natural_sort(&query.sort)? else {
        return Ok(Cow::Borrowed(query));
    };
    let mut normalized = query.clone();
    normalized.sort = sort;
    Ok(Cow::Owned(normalized))
}

/// Summary of a full collection reindex.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReindexReport {
    pub secondary: Vec<String>,
    pub text: Vec<String>,
    pub fuzzy: Vec<String>,
    pub vector: Vec<String>,
}

/// One secondary index to create in a single collection scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexSpec {
    pub fields: Vec<String>,
    /// Explicit MongoDB index name; empty means the legacy logical name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// Per-field directions (`1` or `-1`); empty means all ascending.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
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
    /// Single-field GeoJSON point index.
    #[serde(default)]
    pub geo: bool,
}

#[cfg(test)]
static GRAPH_LOOKUP_FAST_PATH_HITS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

pub struct Collection<'e> {
    store: Option<&'e crate::store::Store>,
    db: Database<Bytes, Bytes>,
    meta: Database<Bytes, Bytes>,
    history: Database<Bytes, Bytes>,
    ttl_db: Database<Bytes, Bytes>,
    history_max_versions: usize,
    name: String,
    profile: CollectionProfile,
    ttl_default_seconds: Option<u64>,
    ttl_field: Option<String>,
    ttl_purge_history: bool,
    strict_reads: bool,
    env: &'e Env,
    txn_gate: &'e crate::store::TxnGate,
}

#[derive(Debug, Clone, Serialize)]
pub struct CollectionStorageStats {
    pub records: u64,
    pub data_bytes: u64,
    pub index_bytes: u64,
    pub indexes: u64,
    pub id_bytes: u64,
    pub logical_bytes: u64,
}

fn stat_bytes(stat: &heed::DatabaseStat) -> u64 {
    let pages = stat.branch_pages + stat.leaf_pages + stat.overflow_pages;
    pages as u64 * stat.page_size as u64
}

/// Strict read-time TTL filtering is on by default; `NONODB_TTL_STRICT_READS=0`
/// restores the legacy eventual-visibility behavior.
fn strict_reads_enabled() -> bool {
    std::env::var("NONODB_TTL_STRICT_READS")
        .map(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            )
        })
        .unwrap_or(true)
}

impl<'e> Collection<'e> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        store: Option<&'e crate::store::Store>,
        env: &'e Env,
        db: Database<Bytes, Bytes>,
        meta: Database<Bytes, Bytes>,
        history: Database<Bytes, Bytes>,
        ttl_db: Database<Bytes, Bytes>,
        history_max_versions: usize,
        name: String,
        profile: CollectionProfile,
        ttl_default_seconds: Option<u64>,
        ttl_field: Option<String>,
        ttl_purge_history: bool,
        txn_gate: &'e crate::store::TxnGate,
    ) -> Self {
        Self {
            store,
            db,
            meta,
            history,
            ttl_db,
            history_max_versions,
            name,
            profile,
            ttl_default_seconds,
            ttl_field,
            ttl_purge_history,
            strict_reads: strict_reads_enabled(),
            env,
            txn_gate,
        }
    }

    /// Identifier of the owning LMDB environment.
    pub fn store_id(&self) -> usize {
        self.env as *const _ as usize
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Collection profile (`standard`, `vault`, `timeseries`, or `columnar`).
    pub fn profile(&self) -> CollectionProfile {
        self.profile
    }

    /// Time-series settings, or `None` for non-time-series collections.
    pub fn timeseries_settings_in(
        &self,
        txn: &RoTxn<'_>,
    ) -> Result<Option<crate::timeseries::Settings>> {
        if !self.profile.is_timeseries() {
            return Ok(None);
        }
        let key = format!("{}{}", crate::timeseries::SETTINGS_META_PREFIX, self.name);
        match self.meta.get(txn, key.as_bytes())? {
            Some(raw) => Ok(Some(crate::timeseries::parse_settings(raw)?)),
            None => Ok(Some(crate::timeseries::Settings::default())),
        }
    }

    /// Stores time-series settings for this collection.
    pub fn set_timeseries_settings_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        settings: &crate::timeseries::Settings,
    ) -> Result<()> {
        settings.validate()?;
        let key = format!("{}{}", crate::timeseries::SETTINGS_META_PREFIX, self.name);
        let bytes = serde_json::to_vec(settings)?;
        self.meta.put(&mut *wtxn, key.as_bytes(), &bytes)?;
        Ok(())
    }

    /// Stores time-series settings for this collection using its own write
    /// transaction.
    pub fn set_timeseries_settings(&self, settings: &crate::timeseries::Settings) -> Result<()> {
        let mut wtxn = self.write_txn()?;
        self.set_timeseries_settings_in(&mut wtxn, settings)?;
        wtxn.commit()?;
        Ok(())
    }

    /// Columnar settings, or `None` for non-columnar collections.
    pub fn columnar_settings_in(
        &self,
        txn: &RoTxn<'_>,
    ) -> Result<Option<crate::columnar::Settings>> {
        if !self.profile.is_columnar() {
            return Ok(None);
        }
        let key = format!("{}{}", crate::columnar::SETTINGS_META_PREFIX, self.name);
        match self.meta.get(txn, key.as_bytes())? {
            Some(raw) => Ok(Some(crate::columnar::parse_settings(raw)?)),
            None => Ok(Some(crate::columnar::Settings::default())),
        }
    }

    /// Stores columnar settings for this collection.
    pub fn set_columnar_settings_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        settings: &crate::columnar::Settings,
    ) -> Result<()> {
        if !self.profile.is_columnar() {
            return Err(Error::Query(format!(
                "collection `{}` is not a columnar collection",
                self.name
            )));
        }
        settings.validate()?;
        let key = format!("{}{}", crate::columnar::SETTINGS_META_PREFIX, self.name);
        let bytes = serde_json::to_vec(settings)?;
        self.meta.put(&mut *wtxn, key.as_bytes(), &bytes)?;
        Ok(())
    }

    /// Compression and size statistics for a columnar collection.
    pub fn columnar_stats(&self) -> Result<Option<crate::columnar::Stats>> {
        if !self.profile.is_columnar() {
            return Ok(None);
        }
        let rtxn = self.read_txn()?;
        let stats = crate::columnar::stats(&rtxn, self.env, &self.name)?;
        rtxn.commit()?;
        Ok(Some(stats))
    }

    /// Compression and size statistics for a time-series collection.
    /// Time-based retention that also removes index entries for reaped rows.
    pub fn retain_timeseries_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        before_ms: i64,
    ) -> Result<crate::timeseries::RetainReport> {
        let settings = self.timeseries_settings_in(&*wtxn)?.ok_or_else(|| {
            Error::Query(format!(
                "collection `{}` is not a time-series collection",
                self.name
            ))
        })?;
        let has_indexes = !self.index_defs(&*wtxn)?.is_empty()
            || !self.search_index_defs_in(&*wtxn)?.is_empty()
            || !self.vector_defs(&*wtxn)?.is_empty();
        if !has_indexes {
            return crate::timeseries::retain(wtxn, self.env, &self.name, &settings, before_ms);
        }
        let report = crate::timeseries::retain_with_reaped(
            wtxn, self.env, &self.name, &settings, before_ms,
        )?;
        for (id, doc) in &report.reaped {
            self.apply_all_index_entries(wtxn, Some(doc), None, id)?;
        }
        Ok(report)
    }

    pub fn timeseries_stats(&self) -> Result<Option<crate::timeseries::Stats>> {
        if !self.profile.is_timeseries() {
            return Ok(None);
        }
        let rtxn = self.read_txn()?;
        let settings = self.timeseries_settings_in(&rtxn)?.unwrap_or_default();
        let stats = crate::timeseries::stats(&rtxn, self.env, &self.name, &settings)?;
        rtxn.commit()?;
        Ok(Some(stats))
    }

    fn timeseries_index_ids(
        &self,
        rtxn: &RoTxn<'_>,
        filter: &Value,
        hint: Option<&IndexHint>,
    ) -> Result<Option<HashSet<String>>> {
        let Some(plan) = self.query_index_plan(rtxn, filter, hint)? else {
            return Ok(None);
        };
        if plan.def.postings && !plan.def.geo {
            return Ok(Some(posting_ids(&plan.db, rtxn, &plan.bounds)?));
        }
        let range = (as_bound(&plan.bounds.0), as_bound(&plan.bounds.1));
        let mut ids = HashSet::new();
        for item in plan.db.range(rtxn, &range)? {
            let (key, value) = item?;
            if let Some((id, _)) = index::decode_index_entry(key, value) {
                ids.insert(id);
            }
        }
        Ok(Some(ids))
    }

    fn find_timeseries_in(&self, rtxn: &RoTxn<'_>, query: &FindQuery) -> Result<Vec<Value>> {
        let settings = self.timeseries_settings_in(rtxn)?.unwrap_or_default();
        if let Some((field, near)) = geo::extract_near(&query.filter) {
            let docs = match self.geo_index_candidates(rtxn, &query.filter)? {
                Some(ids) => {
                    let ids: HashSet<String> = ids.into_iter().collect();
                    crate::timeseries::find_with_ids(
                        rtxn,
                        self.env,
                        &self.name,
                        &settings,
                        query,
                        Some(&ids),
                    )?
                }
                None => crate::timeseries::find(rtxn, self.env, &self.name, &settings, query)?,
            };
            let limit = query.limit.unwrap_or(usize::MAX);
            let mut hits: Vec<(f64, Value)> = docs
                .into_iter()
                .filter_map(|doc| {
                    geo::field_distance(&doc, &field, near.center).map(|distance| (distance, doc))
                })
                .collect();
            hits.sort_by(|a, b| a.0.total_cmp(&b.0));
            return Ok(hits
                .into_iter()
                .skip(query.skip)
                .take(limit)
                .map(|(_, doc)| match &query.projection {
                    Some(projection) => project(&doc, projection, Some(&query.filter)),
                    None => doc,
                })
                .collect());
        }
        let hint = query.hint.as_ref().map(parse_index_hint).transpose()?;
        if let Some(ids) = self.timeseries_index_ids(rtxn, &query.filter, hint.as_ref())? {
            return crate::timeseries::find_with_ids(
                rtxn,
                self.env,
                &self.name,
                &settings,
                query,
                Some(&ids),
            );
        }
        crate::timeseries::find(rtxn, self.env, &self.name, &settings, query)
    }

    /// Storage summary for this collection. Data and index bytes come from
    /// LMDB B-tree page allocation; shared history/TTL bytes are added by the
    /// store-level aggregation.
    pub fn storage_stats(&self, txn: &RoTxn<'_>) -> Result<CollectionStorageStats> {
        if self.profile.is_timeseries() {
            let settings = self.timeseries_settings_in(txn)?.unwrap_or_default();
            let stats = crate::timeseries::stats(txn, self.env, &self.name, &settings)?;
            let db_name = format!("{}{}", crate::timeseries::BUCKET_DB_PREFIX, self.name);
            let data_bytes = self
                .env
                .open_database::<Bytes, Bytes>(txn, Some(&db_name))?
                .map(|db| db.stat(txn).map(|stat| stat_bytes(&stat)))
                .transpose()?
                .unwrap_or(0);
            let mut index_bytes = 0u64;
            let mut indexes = 0u64;
            for def in self.index_defs(txn)? {
                let resource = index::db_name(&self.name, &def.name());
                if let Some(bytes) = self.resource_db_bytes(txn, &resource, &resource)? {
                    index_bytes = index_bytes.saturating_add(bytes);
                    indexes += 1;
                }
            }
            return Ok(CollectionStorageStats {
                records: stats.rows,
                data_bytes,
                index_bytes,
                indexes,
                id_bytes: 0,
                logical_bytes: data_bytes,
            });
        }
        if self.profile.is_columnar() {
            let stats = crate::columnar::stats(txn, self.env, &self.name)?;
            let db_name = format!("{}{}", crate::columnar::DB_PREFIX, self.name);
            let data_bytes = self
                .env
                .open_database::<Bytes, Bytes>(txn, Some(&db_name))?
                .map(|db| db.stat(txn).map(|stat| stat_bytes(&stat)))
                .transpose()?
                .unwrap_or(0);
            let mut index_bytes = 0u64;
            let mut indexes = 0u64;
            for def in self.index_defs(txn)? {
                let resource = index::db_name(&self.name, &def.name());
                if let Some(bytes) = self.resource_db_bytes(txn, &resource, &resource)? {
                    index_bytes = index_bytes.saturating_add(bytes);
                    indexes += 1;
                }
            }
            for def in self.vector_defs(txn)? {
                let resource = vector::db_name(&self.name, &def.field);
                if let Some(bytes) = self.resource_db_bytes(txn, &resource, &resource)? {
                    index_bytes = index_bytes.saturating_add(bytes);
                    indexes += 1;
                }
            }
            return Ok(CollectionStorageStats {
                records: stats.rows,
                data_bytes,
                index_bytes,
                indexes,
                id_bytes: stats.id_bytes,
                logical_bytes: stats.data_bytes.saturating_add(stats.id_bytes),
            });
        }
        let data = self.db.stat(txn)?;
        let mut index_bytes = 0u64;
        let mut indexes = 0u64;

        for def in self.index_defs(txn)? {
            let resource = index::db_name(&self.name, &def.name());
            if let Some(bytes) = self.resource_db_bytes(txn, &resource, &resource)? {
                index_bytes = index_bytes.saturating_add(bytes);
                indexes += 1;
            }
        }
        for def in self.vector_defs(txn)? {
            let resource = vector::db_name(&self.name, &def.field);
            if let Some(bytes) = self.resource_db_bytes(txn, &resource, &resource)? {
                index_bytes = index_bytes.saturating_add(bytes);
                indexes += 1;
            }
        }

        Ok(CollectionStorageStats {
            records: data.entries as u64,
            data_bytes: stat_bytes(&data),
            index_bytes,
            indexes,
            id_bytes: 0,
            logical_bytes: stat_bytes(&data),
        })
    }

    fn resource_db_bytes(
        &self,
        txn: &RoTxn<'_>,
        resource: &str,
        legacy_name: &str,
    ) -> Result<Option<u64>> {
        let Some(db) = self.resource_db(txn, resource, legacy_name)? else {
            return Ok(None);
        };
        Ok(Some(stat_bytes(&db.stat(txn)?)))
    }

    fn ensure_vault(&self) -> Result<()> {
        if self.profile.is_vault() {
            Ok(())
        } else {
            Err(Error::Vault {
                code: "not_a_vault_collection",
                message: format!("collection `{}` is not a vault collection", self.name),
            })
        }
    }

    fn ensure_standard(&self) -> Result<()> {
        if self.profile.is_vault() {
            Err(vault::standard_write_forbidden())
        } else {
            Ok(())
        }
    }

    pub(crate) fn set_ttl(&self, wtxn: &mut RwTxn<'_>, id: &str, at: Option<u64>) -> Result<()> {
        ttl::set_entry(&self.ttl_db, wtxn, &self.name, id, at)
    }

    fn ttl_at(&self, txn: &RoTxn<'_>, id: &str) -> Result<Option<u64>> {
        ttl::get_entry(&self.ttl_db, txn, &self.name, id)
    }

    /// Whether strict read-time TTL filtering is enabled for this collection.
    pub fn strict_ttl_reads(&self) -> bool {
        self.strict_reads
    }

    /// Whether any TTL metadata exists. Used to skip the per-document expiry
    /// check for collections/databases that never use TTL.
    fn ttl_filter_active(&self, txn: &RoTxn<'_>) -> Result<bool> {
        Ok(self.strict_reads && self.ttl_db.stat(txn)?.entries > 0)
    }

    pub fn ttl_expiry_in(&self, txn: &RoTxn<'_>, id: &str) -> Result<Option<u64>> {
        self.ttl_at(txn, id)
    }

    /// Returns true when `id` has an expiry at or before `now`.
    fn ttl_expired_in(&self, txn: &RoTxn<'_>, id: &str, now: u64) -> Result<bool> {
        if !self.strict_reads {
            return Ok(false);
        }
        Ok(self.ttl_at(txn, id)?.is_some_and(|at| at <= now))
    }

    fn ttl_expired_at(&self, txn: &RoTxn<'_>, id: &str, now: u64) -> Result<bool> {
        self.ttl_expired_in(txn, id, now)
    }

    fn ttl_doc_visible(&self, txn: &RoTxn<'_>, doc: &Value, now: u64) -> Result<bool> {
        match doc.get("_id").and_then(Value::as_str) {
            Some(id) => Ok(!self.ttl_expired_at(txn, id, now)?),
            None => Ok(true),
        }
    }

    /// Filters expired documents from a materialized result using a fresh read
    /// transaction. Used by cursor pages that outlive the transaction which
    /// produced them.
    pub fn retain_visible(&self, docs: Vec<Value>) -> Result<Vec<Value>> {
        let rtxn = self.read_txn()?;
        let out = self.retain_visible_docs(&rtxn, docs)?;
        rtxn.commit()?;
        Ok(out)
    }

    /// Filters expired documents out of a materialized result without writing.
    fn retain_visible_docs(&self, txn: &RoTxn<'_>, docs: Vec<Value>) -> Result<Vec<Value>> {
        if !self.ttl_filter_active(txn)? {
            return Ok(docs);
        }
        let now = ttl::now_ms();
        let mut out = Vec::with_capacity(docs.len());
        for doc in docs {
            let visible = match doc.get("_id").and_then(Value::as_str) {
                Some(id) => !self.ttl_expired_at(txn, id, now)?,
                None => true,
            };
            if visible {
                out.push(doc);
            }
        }
        Ok(out)
    }

    /// Collection default TTL in seconds, if configured.
    pub fn ttl_default_seconds(&self) -> Option<u64> {
        self.ttl_default_seconds
    }

    /// Date field used for TTL, if the collection uses date-based TTL.
    pub fn ttl_field(&self) -> Option<&str> {
        self.ttl_field.as_deref()
    }

    /// Whether TTL expiry purges stored history instead of appending a tombstone.
    pub fn ttl_purge_history(&self) -> bool {
        self.ttl_purge_history
    }

    fn ttl_field_pair(&self) -> Option<(&str, u64)> {
        self.ttl_field.as_deref().zip(self.ttl_default_seconds)
    }

    /// Backfills `_ttl` metadata from a date field in one bounded page.
    pub fn backfill_ttl_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        field: &str,
        seconds: u64,
        after: Option<&Value>,
        limit: usize,
        dry_run: bool,
    ) -> Result<ttl::BackfillPage> {
        let limit = limit.clamp(1, 10_000);
        let filter = match after {
            Some(after) => serde_json::json!({"_id": {"$gt": after}}),
            None => serde_json::json!({}),
        };
        // Scan stored documents directly: a document hidden by an older TTL
        // must still be backfilled when the collection switches to date-field
        // semantics.
        let mut docs: Vec<Value> = Vec::with_capacity(limit);
        if self.profile.is_columnar() {
            let mut all = crate::columnar::all_docs(&*wtxn, self.env, &self.name)?;
            all.sort_by(|a, b| {
                a.get("_id")
                    .map(id::key_from_value)
                    .unwrap_or_default()
                    .cmp(&b.get("_id").map(id::key_from_value).unwrap_or_default())
            });
            for doc in all {
                if !matches(&doc, &filter) {
                    continue;
                }
                docs.push(doc);
                if docs.len() >= limit {
                    break;
                }
            }
        } else {
            for item in self.db.iter(wtxn)? {
                let (_key, bytes) = item?;
                let doc = parse_json(bytes)?;
                if !matches(&doc, &filter) {
                    continue;
                }
                docs.push(doc);
                if docs.len() >= limit {
                    break;
                }
            }
        }
        let mut updated = 0u64;
        let mut cleared = 0u64;
        let mut next_after = None;
        let mut entries = Vec::with_capacity(docs.len());
        for doc in &docs {
            let Some(id) = doc.get("_id").and_then(id::key_from_value) else {
                continue;
            };
            let at = ttl::expiry_from_field(doc, field, seconds)?;
            if at.is_some() {
                updated += 1;
            } else {
                cleared += 1;
            }
            if !dry_run {
                self.set_ttl(wtxn, &id, at)?;
                entries.push(ttl::BackfillEntry {
                    id: id.clone(),
                    ttl_at: at,
                });
            }
            next_after = doc.get("_id").cloned();
        }
        Ok(ttl::BackfillPage {
            scanned: docs.len() as u64,
            updated,
            cleared,
            next_after,
            done: docs.len() < limit,
            entries,
        })
    }

    // ----------------------------- indexes -----------------------------

    fn db_slot_map_key(resource: &str) -> String {
        format!("{DB_SLOT_META_PREFIX}map:{resource}")
    }

    fn db_slot_owner_key(slot: u32) -> String {
        format!("{DB_SLOT_META_PREFIX}owner:{slot}")
    }

    fn db_slot_name(slot: u32) -> String {
        format!("{DB_SLOT_META_PREFIX}db:{slot}")
    }

    fn db_slot_for(&self, txn: &RoTxn<'_>, resource: &str) -> Result<Option<u32>> {
        let key = Self::db_slot_map_key(resource);
        let Some(bytes) = self.meta.get(txn, key.as_bytes())? else {
            return Ok(None);
        };
        let bytes: [u8; 4] = bytes
            .try_into()
            .map_err(|_| Error::Query("invalid sub-database slot mapping".into()))?;
        Ok(Some(u32::from_le_bytes(bytes)))
    }

    /// Opens a resource DB by its reusable slot, falling back to the legacy
    /// logical name for databases created before slot reuse was introduced.
    fn resource_db(
        &self,
        txn: &RoTxn<'_>,
        resource: &str,
        legacy_name: &str,
    ) -> Result<Option<Database<Bytes, Bytes>>> {
        match self.db_slot_for(txn, resource)? {
            Some(slot) => Ok(self
                .env
                .open_database(txn, Some(&Self::db_slot_name(slot)))?),
            None => Ok(self.env.open_database(txn, Some(legacy_name))?),
        }
    }

    fn resource_db_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        resource: &str,
        legacy_name: &str,
    ) -> Result<Database<Bytes, Bytes>> {
        if let Some(slot) = self.db_slot_for(&*wtxn, resource)? {
            return Ok(self
                .env
                .create_database(&mut *wtxn, Some(&Self::db_slot_name(slot)))?);
        }
        // Keep using an existing legacy DB until it is dropped. Its LMDB slot
        // cannot be deregistered by heed, but all newly created resources use
        // the reusable pool below.
        if self
            .env
            .open_database::<Bytes, Bytes>(&*wtxn, Some(legacy_name))?
            .is_some()
        {
            return Ok(self.env.create_database(&mut *wtxn, Some(legacy_name))?);
        }
        let slot = self.allocate_db_slot(wtxn, resource)?;
        Ok(self
            .env
            .create_database(&mut *wtxn, Some(&Self::db_slot_name(slot)))?)
    }

    fn allocate_db_slot(&self, wtxn: &mut RwTxn<'_>, resource: &str) -> Result<u32> {
        for slot in 1..=MAX_DB_SLOTS {
            let owner_key = Self::db_slot_owner_key(slot);
            if self.meta.get(&*wtxn, owner_key.as_bytes())?.is_some() {
                continue;
            }
            let map_key = Self::db_slot_map_key(resource);
            self.meta
                .put(&mut *wtxn, owner_key.as_bytes(), resource.as_bytes())?;
            self.meta
                .put(&mut *wtxn, map_key.as_bytes(), &slot.to_le_bytes())?;
            return Ok(slot);
        }
        Err(Error::Query(
            "LMDB sub-database slot limit reached; drop unused indexes".into(),
        ))
    }

    /// Clears a resource DB and returns its slot to the reusable pool.
    fn drop_resource_db(
        &self,
        wtxn: &mut RwTxn<'_>,
        resource: &str,
        legacy_name: &str,
    ) -> Result<()> {
        if let Some(slot) = self.db_slot_for(&*wtxn, resource)? {
            if let Some(db) = self
                .env
                .open_database::<Bytes, Bytes>(&*wtxn, Some(&Self::db_slot_name(slot)))?
            {
                db.clear(&mut *wtxn)?;
            }
            self.meta
                .delete(&mut *wtxn, Self::db_slot_map_key(resource).as_bytes())?;
            self.meta
                .delete(&mut *wtxn, Self::db_slot_owner_key(slot).as_bytes())?;
            return Ok(());
        }
        if let Some(db) = self
            .env
            .open_database::<Bytes, Bytes>(&*wtxn, Some(legacy_name))?
        {
            db.clear(&mut *wtxn)?;
        }
        Ok(())
    }

    /// Index definitions for this collection (read in the given transaction).
    fn index_defs(&self, txn: &RoTxn<'_>) -> Result<Vec<IndexDef>> {
        let prefix = format!("index:{}:", self.name);
        let mut defs = Vec::new();
        for item in self.meta.prefix_iter(txn, prefix.as_bytes())? {
            let (_key, bytes) = item?;
            defs.push(serde_json::from_slice(bytes)?);
        }
        Ok(defs)
    }

    fn index_db(&self, txn: &RoTxn<'_>, def: &IndexDef) -> Result<Option<Database<Bytes, Bytes>>> {
        let name = index::db_name(&self.name, &def.name());
        self.resource_db(txn, &name, &name)
    }

    fn index_db_in(&self, wtxn: &mut RwTxn<'_>, def: &IndexDef) -> Result<Database<Bytes, Bytes>> {
        let name = index::db_name(&self.name, &def.name());
        self.resource_db_in(wtxn, &name, &name)
    }

    /// Fetches a document by `_id`, branching to the bucket store for
    /// time-series collections.
    fn document_in(&self, txn: &RoTxn<'_>, id: &str) -> Result<Option<Value>> {
        if self.profile.is_timeseries() {
            let settings = self.timeseries_settings_in(txn)?.unwrap_or_default();
            return crate::timeseries::find_by_id(txn, self.env, &self.name, &settings, id);
        }
        if self.profile.is_columnar() {
            return crate::columnar::find_by_id(txn, self.env, &self.name, id);
        }
        self.db.get(txn, id.as_bytes())?.map(parse_json).transpose()
    }

    /// Streams documents for index backfill. Columnar buckets are decoded one
    /// at a time instead of materializing the whole collection.
    fn for_each_backfill_doc<F>(&self, txn: &RoTxn<'_>, mut emit: F) -> Result<()>
    where
        F: FnMut(&str, &Value) -> Result<()>,
    {
        if self.profile.is_timeseries() {
            let settings = self.timeseries_settings_in(txn)?.unwrap_or_default();
            for (id, doc) in crate::timeseries::all_docs(txn, self.env, &self.name, &settings)? {
                emit(&id, &doc)?;
            }
            return Ok(());
        }
        if self.profile.is_columnar() {
            let mut error = None;
            let result = crate::columnar::for_each(txn, self.env, &self.name, |doc| {
                let id = doc
                    .get("_id")
                    .and_then(id::key_from_value)
                    .ok_or(Error::InvalidId)?;
                if let Err(err) = emit(&id, &doc) {
                    error = Some(err);
                    return Err(Error::Query("backfill stopped".into()));
                }
                Ok(())
            });
            if let Some(err) = error {
                return Err(err);
            }
            return result;
        }
        for item in self.db.iter(txn)? {
            let (id, bytes) = item?;
            let id = std::str::from_utf8(id)
                .map_err(|e| Error::Query(format!("invalid document id: {e}")))?;
            emit(id, &parse_json(bytes)?)?;
        }
        Ok(())
    }

    /// Every document, used to backfill text/fuzzy/vector indexes.
    fn documents_for_backfill(&self, txn: &RoTxn<'_>) -> Result<Vec<(String, Value)>> {
        if self.profile.is_timeseries() {
            let settings = self.timeseries_settings_in(txn)?.unwrap_or_default();
            return crate::timeseries::all_docs(txn, self.env, &self.name, &settings);
        }
        if self.profile.is_columnar() {
            let mut out = Vec::new();
            for doc in crate::columnar::all_docs(txn, self.env, &self.name)? {
                let id = doc
                    .get("_id")
                    .and_then(id::key_from_value)
                    .ok_or(Error::InvalidId)?;
                out.push((id, doc));
            }
            return Ok(out);
        }
        let mut out = Vec::new();
        for item in self.db.iter(txn)? {
            let (id, bytes) = item?;
            let id = std::str::from_utf8(id)
                .map_err(|e| Error::Query(format!("invalid document id: {e}")))?
                .to_string();
            out.push((id, parse_json(bytes)?));
        }
        Ok(out)
    }

    /// Creates multiple secondary indexes in one collection scan.
    ///
    /// Parsing and key encoding run in parallel; all index writes happen in the
    /// caller's single LMDB write transaction. This is the preferred path for
    /// migration/backfill jobs that add several indexes at once.
    pub fn create_indexes(&self, specs: &[IndexSpec]) -> Result<()> {
        let mut wtxn = self.write_txn()?;
        self.create_indexes_txn(&mut wtxn, specs)?;
        wtxn.commit()?;
        Ok(())
    }

    pub fn create_indexes_txn(&self, wtxn: &mut RwTxn<'_>, specs: &[IndexSpec]) -> Result<()> {
        self.ensure_standard()?;
        if specs.is_empty() {
            return Ok(());
        }
        let mut defs: Vec<(String, IndexDef)> = Vec::with_capacity(specs.len());
        let mut names = HashSet::new();
        let existing_defs = self.index_defs(&*wtxn)?;
        for spec in specs {
            if spec.fields.is_empty() {
                return Err(Error::Query("index requires at least one field".into()));
            }
            if spec.fields.iter().any(|f| f.is_empty() || f.contains('+')) {
                return Err(Error::Query(
                    "index fields must be non-empty and must not contain `+`".into(),
                ));
            }
            if spec.geo && (spec.fields.len() != 1 || spec.unique || !spec.include.is_empty()) {
                return Err(Error::Query(
                    "geo indexes require exactly one field and cannot be unique or covering".into(),
                ));
            }
            let mut def = if spec.geo {
                IndexDef::geo(&spec.fields[0])
            } else {
                IndexDef::compound(&spec.fields, spec.unique, &spec.include)
            };
            if !spec.name.is_empty() {
                if spec.name.contains('\0') {
                    return Err(Error::Query(
                        "index name must not contain a NUL byte".into(),
                    ));
                }
                def.name = spec.name.clone();
            }
            def.sparse = spec.sparse;
            def.partial_filter_expression = spec.partial_filter_expression.clone();
            validate_partial_filter(def.partial_filter_expression.as_ref())?;
            if !spec.directions.is_empty() {
                if spec.directions.len() != spec.fields.len()
                    || spec
                        .directions
                        .iter()
                        .any(|direction| !matches!(direction, 1 | -1))
                {
                    return Err(Error::Query(
                        "index directions must match fields and be 1 or -1".into(),
                    ));
                }
                def.directions = spec.directions.clone();
                def.directions_encoded = def.directions.iter().any(|direction| *direction < 0);
            }
            if self.profile.is_timeseries() && !def.geo {
                def.postings = true;
            }
            let name = def.name();
            if name == "_id_" {
                return Err(Error::Query("index name `_id_` is reserved".into()));
            }
            if !names.insert(name.clone()) {
                return Err(Error::Query(format!("duplicate index in batch: {name}")));
            }
            if let Some(existing) = existing_defs
                .iter()
                .find(|existing| existing.name() == name)
            {
                if existing.effective_fields() != def.effective_fields()
                    || existing.unique != def.unique
                    || existing.sparse != def.sparse
                    || existing.partial_filter_expression != def.partial_filter_expression
                    || existing.include != def.include
                    || existing.geo != def.geo
                    || existing.directions() != def.directions()
                {
                    return Err(Error::Query(format!("index name conflict: {name}")));
                }
            }
            let key = index::def_key(&self.name, &name);
            self.meta.put(
                &mut *wtxn,
                key.as_bytes(),
                serde_json::to_vec(&def)?.as_slice(),
            )?;
            defs.push((key, def));
        }

        if self.profile.is_timeseries() {
            self.backfill_timeseries_indexes_txn(wtxn, &mut defs)?;
            return Ok(());
        }
        if self.profile.is_columnar() {
            self.backfill_columnar_indexes_txn(wtxn, &mut defs)?;
            return Ok(());
        }
        let total_docs = self.db.stat(wtxn)?.entries as u64;
        let index_dbs: Vec<_> = defs
            .iter()
            .map(|(_key, def)| self.index_db_in(wtxn, def))
            .collect::<Result<_>>()?;
        for db in &index_dbs {
            db.clear(&mut *wtxn)?;
        }
        let mut docs_per_index = vec![0u64; defs.len()];
        let mut multikey_fields: Vec<Vec<String>> = vec![Vec::new(); defs.len()];
        let target = format!("{} indexes", defs.len());
        let defs_ref = &defs;
        self.parallel_backfill_apply(
            wtxn,
            "index",
            &target,
            |id, doc| {
                let mut out = Vec::new();
                for (slot, (_key, def)) in defs_ref.iter().enumerate() {
                    let keys = index::encode_index_keys(def, &doc)?;
                    if keys.keys.is_empty() {
                        continue;
                    }
                    let value = if self.compact_index_ids(def) {
                        Vec::new()
                    } else {
                        index::encode_index_value(def, &doc, &id)
                    };
                    out.push((slot, keys, value, id.clone()));
                }
                Ok(out)
            },
            |wtxn, entries| {
                for (slot, keys, value, id) in entries {
                    docs_per_index[slot] = docs_per_index[slot].saturating_add(1);
                    for field in &keys.multikey_fields {
                        if !multikey_fields[slot].contains(field) {
                            multikey_fields[slot].push(field.clone());
                        }
                    }
                    let unique = defs_ref[slot].1.unique;
                    for vb in keys.keys {
                        if unique {
                            let range = index::exact_prefix(&vb);
                            let bounds = (as_bound(&range.0), as_bound(&range.1));
                            for item in index_dbs[slot].range(&*wtxn, &bounds)? {
                                let (existing_key, existing_value) = item?;
                                if let Some((existing_id, _)) =
                                    index::decode_index_entry(existing_key, existing_value)
                                {
                                    if existing_id != id {
                                        return Err(Error::DuplicateKey(id));
                                    }
                                }
                            }
                        }
                        let mut key = vb;
                        index::append_index_id(
                            &mut key,
                            &id,
                            self.compact_index_ids(&defs_ref[slot].1),
                        );
                        index_dbs[slot].put(&mut *wtxn, key.as_slice(), value.as_slice())?;
                    }
                }
                Ok(())
            },
        )?;

        for (slot, (_key, def)) in defs.iter_mut().enumerate() {
            for field in &multikey_fields[slot] {
                if !def.multikey_fields.contains(field) {
                    def.multikey_fields.push(field.clone());
                }
            }
            def.multikey_all_docs = docs_per_index[slot] == total_docs;
            let key = index::def_key(&self.name, &def.name());
            self.meta.put(
                &mut *wtxn,
                key.as_bytes(),
                serde_json::to_vec(def)?.as_slice(),
            )?;
        }
        Ok(())
    }

    /// Backfills secondary indexes from decoded columnar documents.
    fn backfill_columnar_indexes_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        defs: &mut [(String, IndexDef)],
    ) -> Result<()> {
        let total = crate::columnar::stats(&*wtxn, self.env, &self.name)?.rows;
        let target = format!("{} indexes", defs.len());
        let progress = crate::progress::begin(&self.name, &target, "index", total);
        let index_dbs: Vec<_> = defs
            .iter()
            .map(|(_key, def)| self.index_db_in(wtxn, def))
            .collect::<Result<_>>()?;
        for db in &index_dbs {
            db.clear(&mut *wtxn)?;
        }
        let mut docs_per_index = vec![0u64; defs.len()];
        let mut multikey_fields: Vec<Vec<String>> = vec![Vec::new(); defs.len()];
        crate::columnar::for_each_bucket(wtxn, self.env, &self.name, |wtxn, batch| {
            let processed = batch.len() as u64;
            for (id, doc) in batch {
                for (slot, ((_key, def), db)) in defs.iter().zip(index_dbs.iter()).enumerate() {
                    let keys = index::encode_index_keys(def, &doc)?;
                    if keys.keys.is_empty() {
                        continue;
                    }
                    docs_per_index[slot] = docs_per_index[slot].saturating_add(1);
                    for field in &keys.multikey_fields {
                        if !multikey_fields[slot].contains(field) {
                            multikey_fields[slot].push(field.clone());
                        }
                    }
                    let value = self.index_value_for(def, &doc, &id);
                    for vb in keys.keys {
                        if def.unique {
                            let range = index::exact_prefix(&vb);
                            let bounds = (as_bound(&range.0), as_bound(&range.1));
                            for item in db.range(&*wtxn, &bounds)? {
                                let (existing_key, existing_value) = item?;
                                if let Some((existing_id, _)) =
                                    index::decode_index_entry(existing_key, existing_value)
                                {
                                    if existing_id != id {
                                        return Err(Error::DuplicateKey(id.clone()));
                                    }
                                }
                            }
                        }
                        let mut key = vb;
                        index::append_index_id(&mut key, &id, self.compact_index_ids(def));
                        db.put(&mut *wtxn, key.as_slice(), value.as_slice())?;
                    }
                }
            }
            progress.advance(processed);
            Ok(())
        })?;
        for (slot, (key, def)) in defs.iter_mut().enumerate() {
            for field in &multikey_fields[slot] {
                if !def.multikey_fields.contains(field) {
                    def.multikey_fields.push(field.clone());
                }
            }
            def.multikey_all_docs = docs_per_index[slot] == total;
            self.meta.put(
                &mut *wtxn,
                key.as_bytes(),
                serde_json::to_vec(def)?.as_slice(),
            )?;
        }
        Ok(())
    }

    /// Backfills secondary indexes from time-series rows. Rows live in bucket
    /// databases instead of `coll:<name>`, so they cannot use the document
    /// backfill path.
    fn backfill_timeseries_indexes_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        defs: &mut [(String, IndexDef)],
    ) -> Result<()> {
        let settings = self.timeseries_settings_in(&*wtxn)?.unwrap_or_default();
        let index_dbs: Vec<_> = defs
            .iter()
            .map(|(_key, def)| self.index_db_in(wtxn, def))
            .collect::<Result<_>>()?;
        for db in &index_dbs {
            db.clear(&mut *wtxn)?;
        }
        let mut total = 0u64;
        let mut docs_per_index = vec![0u64; defs.len()];
        let mut multikey_fields: Vec<Vec<String>> = vec![Vec::new(); defs.len()];
        crate::timeseries::scan_index_docs(
            wtxn,
            self.env,
            &self.name,
            &settings,
            4096,
            |wtxn, docs| {
                total = total.saturating_add(docs.len() as u64);
                for (id, doc) in &docs {
                    for (slot, ((_key, def), db)) in defs.iter().zip(index_dbs.iter()).enumerate() {
                        let keys = index::encode_index_keys(def, doc)?;
                        if keys.keys.is_empty() {
                            continue;
                        }
                        docs_per_index[slot] = docs_per_index[slot].saturating_add(1);
                        for field in &keys.multikey_fields {
                            if !multikey_fields[slot].contains(field) {
                                multikey_fields[slot].push(field.clone());
                            }
                        }
                        if def.geo {
                            let value = self.index_value_for(def, doc, id);
                            for vb in keys.keys {
                                if def.unique {
                                    for item in db.prefix_iter(&*wtxn, vb.as_slice())? {
                                        let (existing_key, existing) = item?;
                                        if index::decode_index_entry(existing_key, existing)
                                            .is_some_and(|(existing_id, _)| existing_id != *id)
                                        {
                                            return Err(Error::DuplicateKey(id.clone()));
                                        }
                                    }
                                }
                                let mut key = vb;
                                index::append_index_id(&mut key, id, self.compact_index_ids(def));
                                db.put(&mut *wtxn, key.as_slice(), value.as_slice())?;
                            }
                            continue;
                        }
                        let Some((hash, salt, window, serial, seq)) =
                            crate::timeseries::locator_for_id(&*wtxn, self.env, &self.name, id)?
                        else {
                            continue;
                        };
                        let locator = (hash, salt, window, serial);
                        let allowed: HashSet<(PostingLocator, u32)> =
                            std::iter::once((locator, seq)).collect();
                        for vb in keys.keys {
                            if def.unique && posting_conflict(db, &*wtxn, &vb, &allowed)? {
                                return Err(Error::DuplicateKey(id.clone()));
                            }
                            posting_update(db, wtxn, &vb, locator, seq, true)?;
                        }
                    }
                }
                Ok(())
            },
        )?;
        for (slot, (key, def)) in defs.iter_mut().enumerate() {
            for field in &multikey_fields[slot] {
                if !def.multikey_fields.contains(field) {
                    def.multikey_fields.push(field.clone());
                }
            }
            def.multikey_all_docs = docs_per_index[slot] == total;
            self.meta.put(
                &mut *wtxn,
                key.as_bytes(),
                serde_json::to_vec(def)?.as_slice(),
            )?;
        }
        Ok(())
    }

    /// Rebuilds every secondary, text, fuzzy, and vector index on this
    /// collection from the current documents.
    ///
    /// The whole operation runs in the caller's write transaction, so a failed
    /// rebuild rolls back to the previous index definitions and data.
    pub fn reindex(&self) -> Result<ReindexReport> {
        let mut wtxn = self.write_txn()?;
        let report = self.reindex_txn(&mut wtxn)?;
        wtxn.commit()?;
        Ok(report)
    }

    pub fn reindex_txn(&self, wtxn: &mut RwTxn<'_>) -> Result<ReindexReport> {
        self.ensure_standard()?;
        let secondary = self.index_defs(&*wtxn)?;
        let search_defs = self.search_index_defs_in(&*wtxn)?;
        let text: Vec<String> = search_defs
            .iter()
            .filter(|def| def.kind == SearchKind::Text)
            .map(|def| def.name.clone())
            .collect();
        let fuzzy: Vec<String> = search_defs
            .iter()
            .filter(|def| def.kind == SearchKind::Fuzzy)
            .map(|def| def.name.clone())
            .collect();
        let vector = self.vector_defs(&*wtxn)?;
        let report = ReindexReport {
            secondary: secondary.iter().map(IndexDef::name).collect(),
            text: text.clone(),
            fuzzy: fuzzy.clone(),
            vector: vector.iter().map(|def| def.field.clone()).collect(),
        };

        for def in &secondary {
            self.drop_index_txn(wtxn, &def.name())?;
        }
        for def in &search_defs {
            self.drop_search_index_txn(wtxn, def.kind, &def.name)?;
        }
        for def in &vector {
            self.drop_vector_index_txn(wtxn, &def.field)?;
        }

        let secondary_specs: Vec<IndexSpec> = secondary
            .iter()
            .map(|def| IndexSpec {
                fields: def.effective_fields(),
                name: def.name(),
                directions: def.directions(),
                unique: def.unique,
                sparse: def.sparse,
                partial_filter_expression: def.partial_filter_expression.clone(),
                include: def.include.clone(),
                geo: def.geo,
            })
            .collect();
        self.create_indexes_txn(wtxn, &secondary_specs)?;
        for def in &search_defs {
            self.create_search_index_txn(wtxn, def)?;
        }
        for def in &vector {
            self.create_vector_index_txn(wtxn, &def.field, def.dims, def.metric)?;
        }
        Ok(report)
    }

    /// Creates a secondary index (single field) and backfills it with existing
    /// documents.
    pub fn create_index(&self, field: &str, unique: bool) -> Result<()> {
        self.create_index_fields(&[field.to_string()], unique, &[])
    }

    pub fn create_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str, unique: bool) -> Result<()> {
        self.create_index_fields_txn(wtxn, &[field.to_string()], unique, &[], false, false, None)
    }

    /// Creates a single-field GeoJSON point index.
    pub fn create_geo_index(&self, field: &str) -> Result<()> {
        let mut wtxn = self.write_txn()?;
        self.create_geo_index_txn(&mut wtxn, field)?;
        wtxn.commit()?;
        Ok(())
    }

    pub fn create_geo_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<()> {
        self.create_index_fields_txn(wtxn, &[field.to_string()], false, &[], true, false, None)
    }

    /// Creates a compound index. Field order matters: a field prefix can
    /// serve a filter, and the full index can also serve sorting.
    pub fn create_index_fields(
        &self,
        fields: &[String],
        unique: bool,
        include: &[String],
    ) -> Result<()> {
        let mut wtxn = self.write_txn()?;
        self.create_index_fields_txn(&mut wtxn, fields, unique, include, false, false, None)?;
        wtxn.commit()?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_index_fields_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        fields: &[String],
        unique: bool,
        include: &[String],
        geo: bool,
        sparse: bool,
        partial_filter_expression: Option<Value>,
    ) -> Result<()> {
        self.ensure_standard()?;
        if fields.is_empty() {
            return Err(Error::Query("index requires at least one field".into()));
        }
        if fields.iter().any(|f| f.is_empty() || f.contains('+')) {
            return Err(Error::Query(
                "index fields must be non-empty and must not contain `+`".into(),
            ));
        }
        if geo && (fields.len() != 1 || unique || !include.is_empty()) {
            return Err(Error::Query(
                "geo indexes require exactly one field and cannot be unique or covering".into(),
            ));
        }
        let mut def = if geo {
            IndexDef::geo(&fields[0])
        } else {
            IndexDef::compound(fields, unique, include)
        };
        def.sparse = sparse;
        def.partial_filter_expression = partial_filter_expression;
        validate_partial_filter(def.partial_filter_expression.as_ref())?;
        if self.profile.is_timeseries() && !def.geo {
            def.postings = true;
        }
        let key = index::def_key(&self.name, &def.name());
        self.meta.put(
            &mut *wtxn,
            key.as_bytes(),
            serde_json::to_vec(&def)?.as_slice(),
        )?;
        let idx_db = self.index_db_in(wtxn, &def)?;
        idx_db.clear(&mut *wtxn)?;
        if self.profile.is_timeseries() {
            self.backfill_timeseries_indexes_txn(wtxn, &mut [(key, def)])?;
            return Ok(());
        }
        if self.profile.is_columnar() {
            self.backfill_columnar_indexes_txn(wtxn, &mut [(key, def)])?;
            return Ok(());
        }

        // Backfill incrementally: workers parse/encode, the main thread writes
        // each completed chunk into the index immediately.
        let total_docs = self.db.stat(wtxn)?.entries as u64;
        let def_ref = &def;
        let mut docs = 0u64;
        let mut multikey_fields: Vec<String> = Vec::new();
        self.parallel_backfill_apply(
            wtxn,
            "index",
            &def.name(),
            |id, doc| {
                let keys = index::encode_index_keys(def_ref, &doc)?;
                if keys.keys.is_empty() {
                    return Ok(Vec::new());
                }
                let value = if self.compact_index_ids(def_ref) {
                    Vec::new()
                } else {
                    index::encode_index_value(def_ref, &doc, &id)
                };
                Ok(vec![(keys, value, id)])
            },
            |wtxn, entries| {
                for (keys, value, id) in entries {
                    docs = docs.saturating_add(1);
                    for field in &keys.multikey_fields {
                        if !multikey_fields.contains(field) {
                            multikey_fields.push(field.clone());
                        }
                    }
                    for vb in keys.keys {
                        if def_ref.unique {
                            let range = index::exact_prefix(&vb);
                            let bounds = (as_bound(&range.0), as_bound(&range.1));
                            for item in idx_db.range(&*wtxn, &bounds)? {
                                let (existing_key, existing_value) = item?;
                                if let Some((existing_id, _)) =
                                    index::decode_index_entry(existing_key, existing_value)
                                {
                                    if existing_id != id {
                                        return Err(Error::DuplicateKey(id));
                                    }
                                }
                            }
                        }
                        let mut key = vb;
                        index::append_index_id(&mut key, &id, self.compact_index_ids(def_ref));
                        idx_db.put(&mut *wtxn, key.as_slice(), value.as_slice())?;
                    }
                }
                Ok(())
            },
        )?;

        def.multikey_fields = multikey_fields;
        def.multikey_all_docs = docs == total_docs;
        self.meta.put(
            &mut *wtxn,
            key.as_bytes(),
            serde_json::to_vec(&def)?.as_slice(),
        )?;
        Ok(())
    }

    /// Drops an index (clears its data and definition).
    pub fn drop_index(&self, field: &str) -> Result<bool> {
        let mut wtxn = self.write_txn()?;
        let existed = self.drop_index_txn(&mut wtxn, field)?;
        wtxn.commit()?;
        Ok(existed)
    }

    pub fn drop_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<bool> {
        let key = index::def_key(&self.name, field);
        let existed = self.meta.delete(&mut *wtxn, key.as_bytes())?;
        if existed {
            let name = index::db_name(&self.name, field);
            self.drop_resource_db(wtxn, &name, &name)?;
        }
        Ok(existed)
    }

    /// Index list `(name, unique)`; a compound index is named `a+b`.
    pub fn list_indexes(&self) -> Result<Vec<(String, bool)>> {
        let rtxn = self.read_txn()?;
        let out = self.list_indexes_in(&rtxn)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn list_indexes_in(&self, rtxn: &RoTxn<'_>) -> Result<Vec<(String, bool)>> {
        Ok(self
            .index_defs(rtxn)?
            .into_iter()
            .map(|d| (d.name(), d.unique))
            .collect())
    }

    /// Full index definitions in the given transaction.
    pub fn index_defs_in(&self, rtxn: &RoTxn<'_>) -> Result<Vec<IndexDef>> {
        self.index_defs(rtxn)
    }

    /// Full index definitions (for the API/CLI).
    pub fn list_index_defs(&self) -> Result<Vec<IndexDef>> {
        let rtxn = self.read_txn()?;
        let out = self.index_defs(&rtxn)?;
        rtxn.commit()?;
        Ok(out)
    }

    /// Checks the new value for uniqueness across all unique indexes.
    fn validate_unique(&self, txn: &RoTxn<'_>, doc: &Value, id: &str) -> Result<()> {
        for def in self.index_defs(txn)? {
            if !def.unique {
                continue;
            }
            let keys = index::encode_index_keys(&def, doc)?;
            if keys.keys.is_empty() {
                continue;
            }
            let Some(db) = self.index_db(txn, &def)? else {
                continue;
            };
            if def.postings && !def.geo {
                let allowed: HashSet<(PostingLocator, u32)> =
                    crate::timeseries::locator_for_id(txn, self.env, &self.name, id)?
                        .map(|(hash, salt, window, serial, seq)| {
                            ((hash, salt, window, serial), seq)
                        })
                        .into_iter()
                        .collect();
                for vb in keys.keys {
                    if posting_conflict(&db, txn, &vb, &allowed)? {
                        return Err(Error::DuplicateKey(id.to_string()));
                    }
                }
                continue;
            }
            for vb in keys.keys {
                for item in db.prefix_iter(txn, vb.as_slice())? {
                    let (key, existing) = item?;
                    if index::decode_index_entry(key, existing)
                        .is_some_and(|(existing_id, _)| existing_id != id)
                    {
                        return Err(Error::DuplicateKey(id.to_string()));
                    }
                }
            }
        }
        Ok(())
    }

    /// Validates unique indexes for a whole insert batch before any row is
    /// written, including duplicates inside the batch.
    fn validate_unique_batch(&self, txn: &RoTxn<'_>, docs: &[Value]) -> Result<()> {
        for def in self.index_defs(txn)? {
            if !def.unique {
                continue;
            }
            let Some(db) = self.index_db(txn, &def)? else {
                continue;
            };
            let mut seen: HashSet<Vec<u8>> = HashSet::new();
            let no_locators: HashSet<(PostingLocator, u32)> = HashSet::new();
            for doc in docs {
                let id = doc
                    .get("_id")
                    .and_then(id::key_from_value)
                    .unwrap_or_default();
                let keys = index::encode_index_keys(&def, doc)?;
                for vb in keys.keys {
                    if !seen.insert(vb.clone()) {
                        return Err(Error::DuplicateKey(id));
                    }
                    if def.postings && !def.geo {
                        if posting_conflict(&db, txn, &vb, &no_locators)? {
                            return Err(Error::DuplicateKey(id));
                        }
                        continue;
                    }
                    for item in db.prefix_iter(txn, vb.as_slice())? {
                        let (key, existing) = item?;
                        if index::decode_index_entry(key, existing)
                            .is_some_and(|(existing_id, _)| existing_id != id)
                        {
                            return Err(Error::DuplicateKey(id));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Validates a multi-row update as one set: old entries of every updated id
    /// are ignored (they are about to be replaced), while collisions within the
    /// new batch are rejected before any row is written.
    fn validate_unique_updates(&self, txn: &RoTxn<'_>, updates: &[(String, Value)]) -> Result<()> {
        let updated: HashSet<String> = updates.iter().map(|(id, _)| id.clone()).collect();
        for def in self.index_defs(txn)? {
            if !def.unique {
                continue;
            }
            let Some(db) = self.index_db(txn, &def)? else {
                continue;
            };
            let mut seen: HashSet<Vec<u8>> = HashSet::new();
            let mut allowed: HashSet<(PostingLocator, u32)> = HashSet::new();
            if def.postings && !def.geo {
                for id in &updated {
                    if let Some((hash, salt, window, serial, seq)) =
                        crate::timeseries::locator_for_id(txn, self.env, &self.name, id)?
                    {
                        allowed.insert(((hash, salt, window, serial), seq));
                    }
                }
            }
            for (id, doc) in updates {
                let keys = index::encode_index_keys(&def, doc)?;
                for vb in keys.keys {
                    if !seen.insert(vb.clone()) {
                        return Err(Error::DuplicateKey(id.clone()));
                    }
                    if def.postings && !def.geo {
                        if posting_conflict(&db, txn, &vb, &allowed)? {
                            return Err(Error::DuplicateKey(id.clone()));
                        }
                        continue;
                    }
                    for item in db.prefix_iter(txn, vb.as_slice())? {
                        let (key, existing) = item?;
                        if let Some((existing_id, _)) = index::decode_index_entry(key, existing) {
                            if existing_id != *id && !updated.contains(&existing_id) {
                                return Err(Error::DuplicateKey(id.clone()));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn normalize_for_index(doc: &mut Value) -> Result<()> {
        date::normalize_document(doc)?;
        numeric::normalize_document(doc)?;
        bson_types::normalize_document(doc)?;
        Ok(())
    }

    /// Updates secondary, text, fuzzy, and vector entries for one row.
    fn apply_all_index_entries(
        &self,
        wtxn: &mut RwTxn<'_>,
        old: Option<&Value>,
        new: Option<&Value>,
        id: &str,
    ) -> Result<()> {
        self.apply_index_entries(wtxn, old, new, id)?;
        self.apply_text_entries(wtxn, old, new, id)?;
        self.apply_fuzzy_entries(wtxn, old, new, id)?;
        self.apply_vector_entries(wtxn, old, new, id)?;
        Ok(())
    }

    fn apply_timeseries_posting_entries(
        &self,
        wtxn: &mut RwTxn<'_>,
        def: &IndexDef,
        old: Option<&Value>,
        new: Option<&Value>,
        id: &str,
    ) -> Result<()> {
        let empty = index::IndexKeys {
            keys: Vec::new(),
            multikey_fields: Vec::new(),
        };
        let mut old_keys = old
            .map(|doc| index::encode_index_keys(def, doc))
            .transpose()?
            .unwrap_or(empty);
        let new_keys = new
            .map(|doc| index::encode_index_keys(def, doc))
            .transpose()?
            .unwrap_or(index::IndexKeys {
                keys: Vec::new(),
                multikey_fields: Vec::new(),
            });
        old_keys.keys.sort();
        old_keys.keys.dedup();
        if old_keys.keys == new_keys.keys {
            return Ok(());
        }
        let Some((hash, salt, window, serial, seq)) =
            crate::timeseries::locator_for_id(&*wtxn, self.env, &self.name, id)?
        else {
            return Ok(());
        };
        let locator = (hash, salt, window, serial);
        let idx_db = self.index_db_in(wtxn, def)?;
        let allowed: HashSet<(PostingLocator, u32)> = std::iter::once((locator, seq)).collect();
        for vb in &old_keys.keys {
            if !new_keys.keys.contains(vb) {
                posting_update(&idx_db, wtxn, vb, locator, seq, false)?;
            }
        }
        for vb in &new_keys.keys {
            if old_keys.keys.contains(vb) {
                continue;
            }
            if def.unique && posting_conflict(&idx_db, &*wtxn, vb, &allowed)? {
                return Err(Error::DuplicateKey(id.to_string()));
            }
            posting_update(&idx_db, wtxn, vb, locator, seq, true)?;
        }
        Ok(())
    }

    /// Updates index entries for an `old -> new` change (insert: `old=None`,
    /// delete: `new=None`).
    fn compact_index_ids(&self, def: &IndexDef) -> bool {
        self.profile.is_columnar() && def.include.is_empty()
    }

    fn index_value_for(&self, def: &IndexDef, doc: &Value, id: &str) -> Vec<u8> {
        if self.compact_index_ids(def) {
            Vec::new()
        } else {
            index::encode_index_value(def, doc, id)
        }
    }

    fn apply_index_entries(
        &self,
        wtxn: &mut RwTxn<'_>,
        old: Option<&Value>,
        new: Option<&Value>,
        id: &str,
    ) -> Result<()> {
        let defs = self.index_defs(&*wtxn)?;
        for mut def in defs {
            if self.profile.is_timeseries() && def.postings && !def.geo {
                self.apply_timeseries_posting_entries(wtxn, &def, old, new, id)?;
                continue;
            }
            let mut old_keys = old
                .map(|doc| index::encode_index_keys(&def, doc))
                .transpose()?
                .unwrap_or(index::IndexKeys {
                    keys: Vec::new(),
                    multikey_fields: Vec::new(),
                });
            let new_keys = new
                .map(|doc| index::encode_index_keys(&def, doc))
                .transpose()?
                .unwrap_or(index::IndexKeys {
                    keys: Vec::new(),
                    multikey_fields: Vec::new(),
                });
            // With `include`, copied fields must also be compared because they can
            // change without changing the index key.
            let old_value = old.map(|d| self.index_value_for(&def, d, id));
            let new_value = new.map(|d| self.index_value_for(&def, d, id));
            let mut def_changed = false;
            for field in &new_keys.multikey_fields {
                if !def.multikey_fields.contains(field) {
                    def.multikey_fields.push(field.clone());
                    def_changed = true;
                }
            }
            if new.is_some() && def.multikey_all_docs && new_keys.keys.is_empty() {
                def.multikey_all_docs = false;
                def_changed = true;
            }
            if def_changed {
                let def_key = index::def_key(&self.name, &def.name());
                self.meta.put(
                    &mut *wtxn,
                    def_key.as_bytes(),
                    serde_json::to_vec(&def)?.as_slice(),
                )?;
            }
            old_keys.keys.sort();
            old_keys.keys.dedup();
            if old_keys.keys == new_keys.keys && old_value == new_value {
                continue;
            }
            let idx_db = self.index_db_in(wtxn, &def)?;
            let compact = self.compact_index_ids(&def);
            for vb in old_keys.keys {
                let mut key = vb;
                index::append_index_id(&mut key, id, compact);
                idx_db.delete(&mut *wtxn, key.as_slice())?;
            }
            for vb in new_keys.keys {
                let mut key = vb;
                index::append_index_id(&mut key, id, compact);
                let value = new_value.as_deref().expect("nowy dokument");
                idx_db.put(&mut *wtxn, key.as_slice(), value)?;
            }
        }
        Ok(())
    }

    /// Selects the index sub-database and key range for a filter.
    ///
    /// A compound index `[a, b, c]` is used when the filter has conditions for
    /// all fields, with every field except the last being equality conditions
    /// (`a: 1` or `a: {$eq: 1}`). The last field may have a range condition.
    fn query_index_plan(
        &self,
        rtxn: &RoTxn<'_>,
        filter: &Value,
        hint: Option<&IndexHint>,
    ) -> Result<Option<IndexPlan>> {
        match hint {
            None => self.index_plan(rtxn, filter),
            Some(IndexHint::Natural) => Ok(None),
            Some(hint) => self.hinted_index_plan(rtxn, filter, hint),
        }
    }

    fn hinted_index_plan(
        &self,
        rtxn: &RoTxn<'_>,
        filter: &Value,
        hint: &IndexHint,
    ) -> Result<Option<IndexPlan>> {
        let defs = self.index_defs(rtxn)?;
        let def = defs.into_iter().find(|def| match hint {
            IndexHint::Natural => false,
            IndexHint::Name(name) => {
                def.name() == *name
                    || (def.effective_fields().len() == 1 && def.effective_fields()[0] == *name)
            }
            IndexHint::Fields(fields) => def.effective_fields() == *fields,
        });
        let Some(def) = def else {
            let label = match hint {
                IndexHint::Natural => "$natural".to_string(),
                IndexHint::Name(name) => name.clone(),
                IndexHint::Fields(fields) => format!("{{{}}}", fields.join(": 1, ")),
            };
            return Err(Error::Query(format!("hint index not found: {label}")));
        };
        if def.geo {
            return Err(Error::Query(
                "hint on a geo index is not supported; use $near/$geoWithin/$geoIntersects without a hint"
                    .into(),
            ));
        }
        if !def.current_format() {
            return Err(Error::Query(format!(
                "hint index `{}` uses an older format and must be reindexed",
                def.name()
            )));
        }
        if !partial_filter_allows(&def, filter) {
            return Err(Error::Query(format!(
                "hint index `{}` requires the query to include its partialFilterExpression",
                def.name()
            )));
        }
        let Some(db) = self.index_db(rtxn, &def)? else {
            return Err(Error::Query(format!(
                "hint index `{}` is missing its storage",
                def.name()
            )));
        };

        let bounds = filter
            .as_object()
            .filter(|obj| !obj.keys().any(|key| key.starts_with('$')))
            .and_then(|obj| bounds_for_index(&def, obj));
        if let Some(bounds) = bounds {
            return Ok(Some(IndexPlan { db, bounds, def }));
        }

        // An index that omits documents cannot be used for a residual scan:
        // those documents would never be visited. A full index scan is safe
        // only when every document has at least one entry.
        let document_count = self.db.stat(rtxn)?.entries;
        let covers_all = !def.sparse
            && def.partial_filter_expression.is_none()
            && if def.is_multikey() {
                def.multikey_all_docs
            } else {
                db.stat(rtxn)?.entries == document_count
            };
        if !covers_all {
            return Err(Error::Query(format!(
                "hint index `{}` cannot be used for this filter because it does not contain every document",
                def.name()
            )));
        }
        Ok(Some(IndexPlan {
            db,
            bounds: (Bound::Unbounded, Bound::Unbounded),
            def,
        }))
    }

    fn index_plan(&self, rtxn: &RoTxn<'_>, filter: &Value) -> Result<Option<IndexPlan>> {
        let Some(obj) = filter.as_object() else {
            return Ok(None);
        };
        if obj.is_empty() || obj.keys().any(|k| k.starts_with('$')) {
            return Ok(None);
        }

        // Try indexes with the longest field prefix first (most selective).
        let mut defs = self.index_defs(rtxn)?;
        defs.sort_by_key(|d| std::cmp::Reverse(d.effective_fields().len()));

        for def in defs {
            // Geo indexes have their own multi-range plan.
            if def.geo {
                continue;
            }
            // Older indexes skipped arrays and some Extended JSON values during
            // backfill. Only current-format indexes are safe for planning.
            if !def.current_format() || !partial_filter_allows(&def, filter) {
                continue;
            }
            let Some(bounds) = bounds_for_index(&def, obj) else {
                continue;
            };
            let Some(db) = self.index_db(rtxn, &def)? else {
                continue;
            };
            return Ok(Some(IndexPlan { db, bounds, def }));
        }
        Ok(None)
    }

    /// Selects a geo index and the grid-cell ranges for a geo filter.
    fn geo_index_plan(&self, rtxn: &RoTxn<'_>, filter: &Value) -> Result<Option<GeoIndexPlan>> {
        let Some((field, query)) = geo::index_query(filter) else {
            return Ok(None);
        };
        let Some(ranges) = geo::index_ranges(&query, geo::MAX_GEO_RANGES) else {
            return Ok(None);
        };
        let Some(def) = self.index_defs(rtxn)?.into_iter().find(|def| {
            def.geo
                && def.current_format()
                && def.effective_fields().as_slice() == [field.as_str()]
                && partial_filter_allows(def, filter)
        }) else {
            return Ok(None);
        };
        let Some(db) = self.index_db(rtxn, &def)? else {
            return Ok(None);
        };
        Ok(Some(GeoIndexPlan { db, ranges, def }))
    }

    /// Distinct `_id` candidates from all geo index cell ranges.
    fn geo_index_candidates(
        &self,
        rtxn: &RoTxn<'_>,
        filter: &Value,
    ) -> Result<Option<Vec<String>>> {
        let Some(plan) = self.geo_index_plan(rtxn, filter)? else {
            return Ok(None);
        };
        let mut ids = Vec::new();
        let mut seen = HashSet::new();
        for (lower, upper) in &plan.ranges {
            let range = (as_bound(lower), as_bound(upper));
            for item in plan.db.range(rtxn, &range)? {
                let (key, value) = item?;
                if let Some((id, _)) = index::decode_index_entry(key, value) {
                    if seen.insert(id.clone()) {
                        ids.push(id);
                    }
                }
            }
        }
        Ok(Some(ids))
    }

    /// Selects an index that can provide a sort-only query order.
    ///
    /// Missing fields are absent from secondary indexes, while a sort must place
    /// them last. The index is therefore usable only when it has one entry for
    /// every document; otherwise the query falls back to a collection scan.
    fn sort_index_plan(
        &self,
        rtxn: &RoTxn<'_>,
        sort: &[(String, i32)],
    ) -> Result<Option<IndexPlan>> {
        if sort.is_empty() {
            return Ok(None);
        }
        let document_count = self.db.stat(rtxn)?.entries;
        let mut defs = self.index_defs(rtxn)?;
        defs.sort_by_key(|d| std::cmp::Reverse(d.effective_fields().len()));
        for def in defs {
            if def.geo
                || !def.current_format()
                || def.is_multikey()
                || index_matches_sort(&def, sort).is_none()
            {
                continue;
            }
            let Some(db) = self.index_db(rtxn, &def)? else {
                continue;
            };
            if db.stat(rtxn)?.entries != document_count {
                continue;
            }
            return Ok(Some(IndexPlan {
                db,
                bounds: (Bound::Unbounded, Bound::Unbounded),
                def,
            }));
        }
        Ok(None)
    }

    /// Selects a multikey index for a sort-only query.
    ///
    /// The array field must be the last index field. Scanning the index in the
    /// requested direction and taking the first entry per document then yields
    /// the minimum (ascending) or maximum (descending) array element, matching
    /// MongoDB's multikey sort rule. `multikey_all_docs` guarantees that no
    /// document is missing from the index.
    fn multikey_sort_plan(
        &self,
        rtxn: &RoTxn<'_>,
        sort: &[(String, i32)],
    ) -> Result<Option<IndexPlan>> {
        if sort.is_empty() {
            return Ok(None);
        }
        let mut defs = self.index_defs(rtxn)?;
        defs.sort_by_key(|d| std::cmp::Reverse(d.effective_fields().len()));
        for def in defs {
            if def.geo
                || !def.current_format()
                || !def.is_multikey()
                || !def.multikey_all_docs
                || index_matches_sort(&def, sort).is_none()
            {
                continue;
            }
            let fields = def.effective_fields();
            let Some(last) = fields.last() else {
                continue;
            };
            if !def.multikey_fields.contains(last) {
                continue;
            }
            let Some(db) = self.index_db(rtxn, &def)? else {
                continue;
            };
            return Ok(Some(IndexPlan {
                db,
                bounds: (Bound::Unbounded, Bound::Unbounded),
                def,
            }));
        }
        Ok(None)
    }

    /// Sorted match-all find using primary-key or secondary-index order.
    ///
    /// Returns `None` when no safe ordered path is available; the caller then
    /// falls back to the normal collection scan.
    fn find_sorted_match_all(
        &self,
        rtxn: &RoTxn<'_>,
        query: &FindQuery,
    ) -> Result<Option<Vec<Value>>> {
        let limit = query.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Some(Vec::new()));
        }
        let deadline = find_deadline(query);
        check_find_deadline(deadline)?;
        if let Some(reverse) = id_sort_direction(&query.sort) {
            let iter: IndexIter<'_> = if reverse {
                Box::new(self.db.rev_iter(rtxn)?)
            } else {
                Box::new(self.db.iter(rtxn)?)
            };
            let mut out = Vec::new();
            for item in iter.skip(query.skip) {
                check_find_deadline(deadline)?;
                let (_key, bytes) = item?;
                out.push(parse_json(bytes)?);
                if out.len() >= limit {
                    break;
                }
            }
            return Ok(Some(
                out.into_iter()
                    .map(|doc| match &query.projection {
                        Some(projection) => project(&doc, projection, Some(&query.filter)),
                        None => doc,
                    })
                    .collect(),
            ));
        }

        let plan = match self.sort_index_plan(rtxn, &query.sort)? {
            Some(plan) => plan,
            None => match self.multikey_sort_plan(rtxn, &query.sort)? {
                Some(plan) => plan,
                None => return Ok(None),
            },
        };
        let reverse = index_matches_sort(&plan.def, &query.sort).expect("matched sort");
        let multikey = plan.def.is_multikey();
        let range = (Bound::Unbounded, Bound::Unbounded);
        let iter: IndexIter<'_> = if reverse {
            Box::new(plan.db.rev_range(rtxn, &range)?)
        } else {
            Box::new(plan.db.range(rtxn, &range)?)
        };
        let mut skipped = 0usize;
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        for item in iter {
            check_find_deadline(deadline)?;
            let (key, value) = item?;
            let Some((id, inc)) = index::decode_index_entry(key, value) else {
                continue;
            };
            if multikey && !seen.insert(id.clone()) {
                continue;
            }
            let doc = if !multikey {
                if let Some(doc) = covering_doc(&id, inc.as_ref(), &query.filter, &query.projection)
                {
                    doc
                } else {
                    let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                        continue;
                    };
                    parse_json(bytes)?
                }
            } else {
                let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                    continue;
                };
                parse_json(bytes)?
            };
            if skipped < query.skip {
                skipped += 1;
                continue;
            }
            out.push(doc);
            if out.len() >= limit {
                break;
            }
        }
        Ok(Some(
            out.into_iter()
                .map(|doc| match &query.projection {
                    Some(projection) => project(&doc, projection, Some(&query.filter)),
                    None => doc,
                })
                .collect(),
        ))
    }

    /// Finds the single simple `$in` branch in a filter or in a top-level
    /// `$and` (as produced by cursor pagination). Returns `None` when there are
    /// several `$in` branches, so a union cannot accidentally weaken the query.
    fn in_filter_branch(filter: &Value) -> Option<&Value> {
        fn simple(filter: &Value) -> bool {
            filter
                .as_object()
                .filter(|obj| obj.len() == 1)
                .and_then(|obj| obj.values().next())
                .and_then(Value::as_object)
                .is_some_and(|ops| ops.len() == 1 && ops.contains_key("$in"))
        }
        if simple(filter) {
            return Some(filter);
        }
        let branches = filter
            .as_object()
            .filter(|obj| obj.len() == 1)
            .and_then(|obj| obj.get("$and"))
            .and_then(Value::as_array)?;
        let mut found = None;
        for branch in branches {
            if simple(branch) {
                if found.is_some() {
                    return None;
                }
                found = Some(branch);
            }
        }
        found
    }

    /// Replaces a filter whose candidates come from one exact `$in` branch
    /// with the remaining residual conditions. A simple `$in` leaves `{}`.
    fn residual_without_in(filter: &Value) -> Option<Value> {
        let branch = Self::in_filter_branch(filter)?;
        if branch == filter {
            return Some(Value::Object(Map::new()));
        }
        let branches = filter
            .as_object()
            .filter(|obj| obj.len() == 1)
            .and_then(|obj| obj.get("$and"))
            .and_then(Value::as_array)?;
        let mut rest = Vec::with_capacity(branches.len().saturating_sub(1));
        let mut removed = false;
        for item in branches {
            if !removed && item == branch {
                removed = true;
            } else {
                rest.push(item.clone());
            }
        }
        if !removed {
            return None;
        }
        Some(match rest.len() {
            0 => Value::Object(Map::new()),
            1 => rest.pop().expect("one residual"),
            _ => Value::Object(Map::from_iter([("$and".to_string(), Value::Array(rest))])),
        })
    }

    /// Returns one equality index plan per value of a simple single-field
    /// `$in` filter. Returns `None` unless every value is indexable.
    fn in_index_plans(&self, rtxn: &RoTxn<'_>, filter: &Value) -> Result<Option<Vec<IndexPlan>>> {
        let Some(branch) = Self::in_filter_branch(filter) else {
            return Ok(None);
        };
        let Some(obj) = branch.as_object() else {
            return Ok(None);
        };
        let Some((field, cond)) = obj.iter().next() else {
            return Ok(None);
        };
        if field == "_id" {
            return Ok(None);
        }
        let Some(ops) = cond.as_object().filter(|ops| ops.len() == 1) else {
            return Ok(None);
        };
        let Some(values) = ops.get("$in").and_then(Value::as_array) else {
            return Ok(None);
        };
        if values.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let mut plans = Vec::with_capacity(values.len());
        for value in values {
            let branch = Value::Object(Map::from_iter([(
                field.clone(),
                Value::Object(Map::from_iter([("$eq".to_string(), value.clone())])),
            )]));
            let Some(plan) = self.index_plan(rtxn, &branch)? else {
                return Ok(None);
            };
            plans.push(plan);
        }
        Ok(Some(plans))
    }

    /// Returns one index plan per top-level `$or` branch or `$in` value, but
    /// only when every branch is indexable. A union is not semantics-preserving
    /// if any branch would require a collection scan.
    fn or_index_plans(&self, rtxn: &RoTxn<'_>, filter: &Value) -> Result<Option<Vec<IndexPlan>>> {
        if let Some(plans) = self.in_index_plans(rtxn, filter)? {
            return Ok(Some(plans));
        }
        let Some(obj) = filter.as_object() else {
            return Ok(None);
        };
        if obj.len() != 1 {
            return Ok(None);
        }
        let Some(branches) = obj.get("$or").and_then(Value::as_array) else {
            return Ok(None);
        };
        if branches.is_empty() {
            return Ok(None);
        }
        let mut plans = Vec::with_capacity(branches.len());
        for branch in branches {
            let Some(plan) = self.index_plan(rtxn, branch)? else {
                return Ok(None);
            };
            plans.push(plan);
        }
        Ok(Some(plans))
    }

    /// Candidate ids for a single-field `$all` filter. Every value must be
    /// present in the array, so the result is the intersection of the per-value
    /// index ranges. Returns `None` when no suitable multikey index exists.
    fn all_index_candidates(
        &self,
        rtxn: &RoTxn<'_>,
        filter: &Value,
    ) -> Result<Option<Vec<String>>> {
        let Some(obj) = filter.as_object() else {
            return Ok(None);
        };
        if obj.len() != 1 {
            return Ok(None);
        }
        let Some((field, cond)) = obj.iter().next() else {
            return Ok(None);
        };
        let Some(values) = cond
            .as_object()
            .filter(|ops| ops.len() == 1)
            .and_then(|ops| ops.get("$all"))
            .and_then(Value::as_array)
        else {
            return Ok(None);
        };
        if values.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let mut defs = self.index_defs(rtxn)?;
        defs.sort_by_key(|d| std::cmp::Reverse(d.effective_fields().len()));
        let Some(def) = defs.into_iter().find(|def| {
            !def.geo
                && def.current_format()
                && def.effective_fields().as_slice() == [field.as_str()]
                && partial_filter_allows(def, filter)
        }) else {
            return Ok(None);
        };
        let Some(db) = self.index_db(rtxn, &def)? else {
            return Ok(None);
        };
        let direction = def.storage_directions().first().copied().unwrap_or(1);
        let mut result: Option<HashSet<String>> = None;
        for value in values {
            let Some((lower, upper)) = index::bounds_direction("$eq", value, direction) else {
                return Ok(None);
            };
            let range = (as_bound(&lower), as_bound(&upper));
            let mut ids = HashSet::new();
            for item in db.range(rtxn, &range)? {
                let (key, value) = item?;
                if let Some((id, _)) = index::decode_index_entry(key, value) {
                    ids.insert(id);
                }
            }
            result = Some(match result {
                Some(current) => current.intersection(&ids).cloned().collect(),
                None => ids,
            });
            if result.as_ref().is_some_and(HashSet::is_empty) {
                break;
            }
        }
        Ok(Some(result.unwrap_or_default().into_iter().collect()))
    }

    fn index_candidates(
        &self,
        rtxn: &RoTxn<'_>,
        filter: &Value,
        max: Option<usize>,
    ) -> Result<Option<Vec<String>>> {
        let Some(plan) = self.index_plan(rtxn, filter)? else {
            return Ok(None);
        };
        Ok(Some(self.index_plan_candidates(rtxn, &plan, max)?))
    }

    /// Candidate IDs for a segment text/fuzzy filter. Only a single indexed
    /// field is accepted, so the index range is an exact candidate set rather
    /// than a superset that would still need a residual filter.
    fn segment_filter_candidates(
        &self,
        rtxn: &RoTxn<'_>,
        filter: &Value,
    ) -> Result<Option<HashSet<String>>> {
        if filter.as_object().is_none_or(|object| object.is_empty()) {
            return Ok(None);
        }
        if let Some(id) = direct_id(filter) {
            return Ok(Some(HashSet::from([id])));
        }
        let Some(object) = filter.as_object() else {
            return Ok(None);
        };
        if object.len() != 1 {
            return Ok(None);
        }
        let Some(plan) = self.index_plan(rtxn, filter)? else {
            return Ok(None);
        };
        let ids =
            self.index_plan_candidates(rtxn, &plan, Some(SEARCH_FILTER_CANDIDATE_LIMIT + 1))?;
        if ids.len() > SEARCH_FILTER_CANDIDATE_LIMIT {
            return Ok(None);
        }
        Ok(Some(ids.into_iter().collect()))
    }

    fn index_plan_candidates(
        &self,
        rtxn: &RoTxn<'_>,
        plan: &IndexPlan,
        max: Option<usize>,
    ) -> Result<Vec<String>> {
        let (lower, upper) = &plan.bounds;
        let range = (as_bound(lower), as_bound(upper));

        let mut ids = Vec::new();
        // Non-multikey indexes contain at most one entry per document. Avoid
        // a second full-size `HashSet<String>` when deduplication is unnecessary.
        let mut seen = plan.def.is_multikey().then(HashSet::new);
        for item in plan.db.range(rtxn, &range)? {
            let (key, value) = item?;
            if let Some((id, _)) = index::decode_index_entry(key, value) {
                if let Some(seen) = seen.as_mut() {
                    if !seen.insert(id.clone()) {
                        continue;
                    }
                }
                ids.push(id);
            }
            if max.is_some_and(|m| ids.len() >= m) {
                break;
            }
        }
        Ok(ids)
    }

    /// Counts index entries in a range without allocating an `_id` list.
    fn index_count_plan(&self, rtxn: &RoTxn<'_>, plan: &IndexPlan) -> Result<usize> {
        let (lower, upper) = &plan.bounds;
        let range = (as_bound(lower), as_bound(upper));
        let mut n = 0usize;
        for item in plan.db.range(rtxn, &range)? {
            item?;
            n += 1;
        }
        Ok(n)
    }

    /// Counts distinct documents in a multikey index range.
    fn index_count_unique_plan(&self, rtxn: &RoTxn<'_>, plan: &IndexPlan) -> Result<usize> {
        let (lower, upper) = &plan.bounds;
        let range = (as_bound(lower), as_bound(upper));
        let mut ids = HashSet::new();
        for item in plan.db.range(rtxn, &range)? {
            let (key, value) = item?;
            if let Some((id, _)) = index::decode_index_entry(key, value) {
                ids.insert(id);
            }
        }
        Ok(ids.len())
    }

    // --------------------------- full-text ----------------------------

    /// All search index definitions for this collection.
    pub fn search_index_defs_in(&self, txn: &RoTxn<'_>) -> Result<Vec<SearchIndexDef>> {
        let prefix = search::def_prefix(&self.name);
        let mut out = Vec::new();
        for item in self.meta.prefix_iter(txn, prefix.as_bytes())? {
            let (_key, value) = item?;
            let def: SearchIndexDef = serde_json::from_slice(value)?;
            def.validate().map_err(Error::Query)?;
            out.push(def);
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    fn search_index_def(
        &self,
        txn: &RoTxn<'_>,
        kind: SearchKind,
        name: &str,
    ) -> Result<Option<SearchIndexDef>> {
        let key = search::def_key(&self.name, kind, name);
        let Some(bytes) = self.meta.get(txn, key.as_bytes())? else {
            return Ok(None);
        };
        let def: SearchIndexDef = serde_json::from_slice(bytes)?;
        def.validate().map_err(Error::Query)?;
        Ok(Some(def))
    }

    fn text_defs(&self, txn: &RoTxn<'_>) -> Result<Vec<String>> {
        Ok(self
            .search_index_defs_in(txn)?
            .into_iter()
            .filter(|def| def.kind == SearchKind::Text)
            .map(|def| def.name)
            .collect())
    }

    fn fuzzy_defs(&self, txn: &RoTxn<'_>) -> Result<Vec<String>> {
        Ok(self
            .search_index_defs_in(txn)?
            .into_iter()
            .filter(|def| def.kind == SearchKind::Fuzzy)
            .map(|def| def.name)
            .collect())
    }

    fn vector_db(&self, txn: &RoTxn<'_>, field: &str) -> Result<Option<Database<Bytes, Bytes>>> {
        let name = vector::db_name(&self.name, field);
        self.resource_db(txn, &name, &name)
    }

    fn vector_db_in(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<Database<Bytes, Bytes>> {
        let name = vector::db_name(&self.name, field);
        self.resource_db_in(wtxn, &name, &name)
    }

    fn vector_defs(&self, txn: &RoTxn<'_>) -> Result<Vec<VectorDef>> {
        let prefix = format!("vecdef:{}:", self.name);
        let mut out = Vec::new();
        for item in self.meta.prefix_iter(txn, prefix.as_bytes())? {
            let (_key, value) = item?;
            out.push(serde_json::from_slice(value)?);
        }
        Ok(out)
    }

    /// Creates an exact vector index and backfills it with existing documents.
    pub fn create_vector_index(
        &self,
        field: &str,
        dims: usize,
        metric: VectorMetric,
    ) -> Result<()> {
        let mut wtxn = self.write_txn()?;
        self.create_vector_index_txn(&mut wtxn, field, dims, metric)?;
        wtxn.commit()?;
        Ok(())
    }

    pub fn create_vector_index_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        field: &str,
        dims: usize,
        metric: VectorMetric,
    ) -> Result<()> {
        self.ensure_standard()?;
        if field.is_empty() || field.contains('+') {
            return Err(Error::Query(
                "vector index field must be non-empty and must not contain `+`".into(),
            ));
        }
        if dims == 0 || dims > vector::MAX_VECTOR_DIMS {
            return Err(Error::Query(format!(
                "vector index dimensions must be between 1 and {}",
                vector::MAX_VECTOR_DIMS
            )));
        }
        let def_key = vector::def_key(&self.name, field);
        if let Some(bytes) = self.meta.get(&*wtxn, def_key.as_bytes())? {
            let existing: VectorDef = serde_json::from_slice(bytes)?;
            if existing.dims == dims && existing.metric == metric {
                return Ok(());
            }
            return Err(Error::Query(format!(
                "vector index on `{field}` already exists with different dimensions or metric; drop it first"
            )));
        }

        let db = self.vector_db_in(wtxn, field)?;
        db.clear(&mut *wtxn)?;
        if self.profile.is_columnar() {
            crate::columnar::for_each_bucket(wtxn, self.env, &self.name, |wtxn, batch| {
                for (id, doc) in batch {
                    let Some(vector) =
                        index::field_value(&doc, field).and_then(|v| vector::parse_vector(v, dims))
                    else {
                        continue;
                    };
                    db.put(
                        &mut *wtxn,
                        id.as_bytes(),
                        vector::encode_vector(&vector).as_slice(),
                    )?;
                }
                Ok(())
            })?;
            let def = VectorDef::new(field, dims, metric);
            self.meta.put(
                &mut *wtxn,
                def_key.as_bytes(),
                serde_json::to_vec(&def)?.as_slice(),
            )?;
            return Ok(());
        }
        if self.profile.is_timeseries() {
            for (id, doc) in self.documents_for_backfill(&*wtxn)? {
                let Some(vector) =
                    index::field_value(&doc, field).and_then(|v| vector::parse_vector(v, dims))
                else {
                    continue;
                };
                db.put(
                    &mut *wtxn,
                    id.as_bytes(),
                    vector::encode_vector(&vector).as_slice(),
                )?;
            }
            let def = VectorDef::new(field, dims, metric);
            self.meta.put(
                &mut *wtxn,
                def_key.as_bytes(),
                serde_json::to_vec(&def)?.as_slice(),
            )?;
            return Ok(());
        }
        self.parallel_backfill_apply(
            wtxn,
            "vector",
            field,
            |id, doc| {
                let Some(vector) =
                    index::field_value(&doc, field).and_then(|v| vector::parse_vector(v, dims))
                else {
                    return Ok(Vec::new());
                };
                Ok(vec![(id, vector::encode_vector(&vector))])
            },
            |wtxn, entries| {
                for (id, value) in entries {
                    db.put(&mut *wtxn, id.as_bytes(), value.as_slice())?;
                }
                Ok(())
            },
        )?;

        let def = VectorDef::new(field, dims, metric);
        self.meta.put(
            &mut *wtxn,
            def_key.as_bytes(),
            serde_json::to_vec(&def)?.as_slice(),
        )?;
        Ok(())
    }

    /// Whether the optional ANN sidecar exists and uses the current format.
    pub fn vector_ann_sidecar_exists(&self, field: &str) -> bool {
        self.vector_ann_path(field)
            .map(|path| vector_ann::is_compatible(&path))
            .unwrap_or(false)
    }

    /// Builds or rebuilds the optional immutable IVF sidecar for a vector
    /// index. The exact LMDB index remains the source of truth.
    pub fn build_vector_ann_index(
        &self,
        field: &str,
        nprobe: usize,
    ) -> Result<vector_ann::AnnStats> {
        let mut wtxn = self.write_txn()?;
        let def_key = vector::def_key(&self.name, field);
        let Some(bytes) = self.meta.get(&wtxn, def_key.as_bytes())? else {
            return Err(Error::Query(format!("no vector index on field `{field}`")));
        };
        let mut def: VectorDef = serde_json::from_slice(bytes)?;
        let db = self.vector_db_in(&mut wtxn, field)?;
        let mut vectors = Vec::with_capacity(db.stat(&wtxn)?.entries);
        for item in db.iter(&wtxn)? {
            let (key, value) = item?;
            let Ok(id) = std::str::from_utf8(key) else {
                continue;
            };
            let Some(values) = vector::decode_vector(value, def.dims) else {
                continue;
            };
            vectors.push((id.to_string(), values));
        }
        let path = self.vector_ann_path(field)?;
        let stats = vector_ann::build(&path, def.dims, def.metric, &vectors, nprobe)?;
        vector_ann::invalidate_cache(&path);
        def.ann = Some(VectorAnnDef {
            version: 1,
            clusters: stats.clusters as u32,
            nprobe: stats.nprobe as u32,
        });
        self.meta
            .put(&mut wtxn, def_key.as_bytes(), &serde_json::to_vec(&def)?)?;
        self.meta.delete(
            &mut wtxn,
            vector::ann_dirty_key(&self.name, field).as_bytes(),
        )?;
        self.clear_vector_ann_delta(&mut wtxn, field)?;
        wtxn.commit()?;
        Ok(stats)
    }

    /// Drops a vector index and its stored vectors.
    pub fn drop_vector_index(&self, field: &str) -> Result<bool> {
        let mut wtxn = self.write_txn()?;
        let existed = self.drop_vector_index_txn(&mut wtxn, field)?;
        wtxn.commit()?;
        Ok(existed)
    }

    pub fn drop_vector_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<bool> {
        let def_key = vector::def_key(&self.name, field);
        let existed = self.meta.delete(&mut *wtxn, def_key.as_bytes())?;
        if existed {
            let name = vector::db_name(&self.name, field);
            self.drop_resource_db(wtxn, &name, &name)?;
            self.meta.delete(
                &mut *wtxn,
                vector::ann_dirty_key(&self.name, field).as_bytes(),
            )?;
            self.clear_vector_ann_delta(wtxn, field)?;
            if let Some(store) = self.store {
                let path = self.vector_ann_path(field)?;
                vector_ann::invalidate_cache(&path);
                let dir = store
                    .env()
                    .path()
                    .join(vector_ann_index_dir(&self.name, field));
                match std::fs::remove_dir_all(&dir) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Ok(existed)
    }

    pub fn list_vector_indexes(&self) -> Result<Vec<VectorDef>> {
        let rtxn = self.read_txn()?;
        let out = self.list_vector_indexes_in(&rtxn)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn list_vector_indexes_in(&self, rtxn: &RoTxn<'_>) -> Result<Vec<VectorDef>> {
        self.vector_defs(rtxn)
    }

    /// Maintains vector indexes on an `old -> new` change.
    fn apply_vector_entries(
        &self,
        wtxn: &mut RwTxn<'_>,
        old: Option<&Value>,
        new: Option<&Value>,
        id: &str,
    ) -> Result<()> {
        for def in self.vector_defs(&*wtxn)? {
            let old_vector = old
                .and_then(|doc| index::field_value(doc, &def.field))
                .and_then(|value| vector::parse_vector(value, def.dims));
            let new_vector = new
                .and_then(|doc| index::field_value(doc, &def.field))
                .and_then(|value| vector::parse_vector(value, def.dims));
            if old_vector == new_vector {
                continue;
            }
            let db = self.vector_db_in(wtxn, &def.field)?;
            if old_vector.is_some() {
                db.delete(&mut *wtxn, id.as_bytes())?;
            }
            if let Some(vector) = &new_vector {
                db.put(
                    &mut *wtxn,
                    id.as_bytes(),
                    vector::encode_vector(vector).as_slice(),
                )?;
            }
            if def.ann.is_some() {
                let delta_key =
                    format!("{}{}", vector::ann_delta_prefix(&self.name, &def.field), id);
                let existed = self.meta.get(&*wtxn, delta_key.as_bytes())?.is_some();
                let count_key = vector::ann_delta_count_key(&self.name, &def.field);
                let count = self
                    .meta
                    .get(&*wtxn, count_key.as_bytes())?
                    .and_then(|value| std::str::from_utf8(value).ok())
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(0);
                if !existed {
                    self.meta.put(
                        &mut *wtxn,
                        count_key.as_bytes(),
                        (count + 1).to_string().as_bytes(),
                    )?;
                }
                match &new_vector {
                    Some(vector) => self.meta.put(
                        &mut *wtxn,
                        delta_key.as_bytes(),
                        vector::encode_vector(vector).as_slice(),
                    )?,
                    None => self.meta.put(&mut *wtxn, delta_key.as_bytes(), b"")?,
                }
            }
        }
        Ok(())
    }

    /// Exact top-K vector search. Results include `_distance`, lowest first.
    pub fn vector_search(
        &self,
        field: &str,
        query: &[f32],
        filter: &Value,
        skip: usize,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        let rtxn = self.read_txn()?;
        let out = self.vector_search_in(&rtxn, field, query, filter, skip, limit)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn vector_search_in(
        &self,
        rtxn: &RoTxn<'_>,
        field: &str,
        query: &[f32],
        filter: &Value,
        skip: usize,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        if !self.ttl_filter_active(rtxn)? {
            return self.vector_search_in_unfiltered(rtxn, field, query, filter, skip, limit);
        }
        let target = skip.saturating_add(limit.unwrap_or(10).min(1000));
        if target == 0 {
            return Ok(Vec::new());
        }
        let mut fetch = target.saturating_mul(2).clamp(target, 1000);
        loop {
            let docs =
                self.vector_search_in_unfiltered(rtxn, field, query, filter, 0, Some(fetch))?;
            let filtered = self.retain_visible_docs(rtxn, docs)?;
            if filtered.len() >= target || fetch >= 1000 {
                return Ok(filtered
                    .into_iter()
                    .skip(skip)
                    .take(limit.unwrap_or(10))
                    .collect());
            }
            fetch = (fetch * 2).min(1000);
        }
    }

    fn vector_search_in_unfiltered(
        &self,
        rtxn: &RoTxn<'_>,
        field: &str,
        query: &[f32],
        filter: &Value,
        skip: usize,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        crate::query::validate::validate_filter(filter)?;
        let def_key = vector::def_key(&self.name, field);
        let Some(raw_def) = self.meta.get(rtxn, def_key.as_bytes())? else {
            return Err(Error::Query(format!("no vector index on field `{field}`")));
        };
        let def: VectorDef = serde_json::from_slice(raw_def)?;
        if query.len() != def.dims {
            return Err(Error::Query(format!(
                "vector query has {} dimensions; index `{field}` expects {}",
                query.len(),
                def.dims
            )));
        }
        if query.iter().any(|value| !value.is_finite()) {
            return Err(Error::Query(
                "vector query contains a non-finite component".into(),
            ));
        }
        let Some(db) = self.vector_db(rtxn, field)? else {
            return Err(Error::Query(format!("no vector index on field `{field}`")));
        };

        let limit = limit.unwrap_or(10).min(1000);
        let want = limit.saturating_add(skip);
        if want == 0 {
            return Ok(Vec::new());
        }
        let query_norm = vector::norm(query);
        let filter_active =
            !(filter.is_null() || filter.as_object().is_some_and(|object| object.is_empty()));
        if !filter_active {
            if let Some(ann) = def.ann.filter(|ann| ann.version == 1) {
                let dirty = self
                    .meta
                    .get(rtxn, vector::ann_dirty_key(&self.name, field).as_bytes())?
                    .is_some();
                let delta = self.vector_ann_delta_in(rtxn, field)?;
                if !dirty && delta.len() <= VECTOR_ANN_DELTA_MAX {
                    if let Ok(path) = self.vector_ann_path(field) {
                        if let Ok(reader) = vector_ann::open_cached(&path) {
                            let nprobe = std::env::var("NONODB_VECTOR_ANN_PROBE")
                                .ok()
                                .and_then(|value| value.parse::<usize>().ok())
                                .unwrap_or(ann.nprobe as usize);
                            let delta_ids: HashSet<&str> =
                                delta.iter().map(|(id, _)| id.as_str()).collect();
                            let mut heap: BinaryHeap<Candidate> = BinaryHeap::new();
                            let search_want = want.saturating_add(delta_ids.len());
                            for candidate in
                                reader.search_with_nprobe(query, search_want, nprobe)?
                            {
                                if !delta_ids.contains(candidate.id.as_str()) {
                                    heap.push(candidate);
                                    if heap.len() > want {
                                        heap.pop();
                                    }
                                }
                            }
                            for (id, bytes) in &delta {
                                if bytes.is_empty() {
                                    continue;
                                }
                                let Some(distance) = vector::distance_from_bytes(
                                    def.metric, query, query_norm, bytes, def.dims,
                                ) else {
                                    continue;
                                };
                                heap.push(Candidate {
                                    distance,
                                    id: id.clone(),
                                });
                                if heap.len() > want {
                                    heap.pop();
                                }
                            }
                            return self.vector_results_from_heap(rtxn, heap, skip, limit);
                        }
                    }
                }
            }
        }
        if filter_active {
            const FILTER_INDEX_LIMIT: usize = 50_000;
            let ids = if let Some(id) = direct_id(filter) {
                Some(vec![id.to_string()])
            } else {
                self.index_candidates(rtxn, filter, Some(FILTER_INDEX_LIMIT + 1))?
                    .filter(|ids| ids.len() <= FILTER_INDEX_LIMIT)
            };
            if let Some(ids) = ids {
                let mut heap: BinaryHeap<Candidate> = BinaryHeap::new();
                for id in ids {
                    let Some(doc) = self.document_in(rtxn, &id)? else {
                        continue;
                    };
                    if !matches(&doc, filter) {
                        continue;
                    }
                    let Some(value) = db.get(rtxn, id.as_bytes())? else {
                        continue;
                    };
                    let Some(distance) =
                        vector::distance_from_bytes(def.metric, query, query_norm, value, def.dims)
                    else {
                        continue;
                    };
                    heap.push(Candidate { distance, id });
                    if heap.len() > want {
                        heap.pop();
                    }
                }
                return self.vector_results_from_heap(rtxn, heap, skip, limit);
            }
        }
        let mut heap: BinaryHeap<Candidate> = BinaryHeap::new();
        let workers = search_workers();
        if !filter_active
            && workers > 1
            && want <= PARALLEL_SEARCH_MAX_WANT
            && db.stat(rtxn)?.entries >= PARALLEL_SEARCH_MIN_ENTRIES
        {
            let heap =
                self.parallel_vector_scan(rtxn, db, &def, query, query_norm, want, workers)?;
            return self.vector_results_from_heap(rtxn, heap, skip, limit);
        }
        for item in db.iter(rtxn)? {
            let (key, value) = item?;
            let Ok(id) = std::str::from_utf8(key) else {
                continue;
            };
            let Some(distance) =
                vector::distance_from_bytes(def.metric, query, query_norm, value, def.dims)
            else {
                continue;
            };
            // Once we have enough results, candidates that cannot beat the
            // current worst entry are irrelevant. This matters most for
            // filtered searches: without the bound we would load and parse a
            // document for every indexed vector.
            if heap.len() >= want {
                let worst = heap.peek().expect("non-empty vector heap");
                if distance > worst.distance
                    || (distance == worst.distance && id >= worst.id.as_str())
                {
                    continue;
                }
            }
            if filter_active {
                let Some(id) = std::str::from_utf8(key).ok() else {
                    continue;
                };
                let Some(doc) = self.document_in(rtxn, id)? else {
                    continue;
                };
                if !matches(&doc, filter) {
                    continue;
                }
            }
            heap.push(Candidate {
                distance,
                id: id.to_string(),
            });
            if heap.len() > want {
                heap.pop();
            }
        }

        self.vector_results_from_heap(rtxn, heap, skip, limit)
    }

    #[allow(clippy::too_many_arguments)]
    fn parallel_vector_scan(
        &self,
        rtxn: &RoTxn<'_>,
        db: Database<Bytes, Bytes>,
        def: &VectorDef,
        query: &[f32],
        query_norm: f32,
        want: usize,
        workers: usize,
    ) -> Result<BinaryHeap<Candidate>> {
        let mut job_txs = Vec::with_capacity(workers);
        let mut job_rxs = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (tx, rx) = std::sync::mpsc::sync_channel::<(usize, SearchChunk)>(1);
            job_txs.push(tx);
            job_rxs.push(rx);
        }
        let (res_tx, res_rx) = std::sync::mpsc::channel::<Result<(usize, Vec<Candidate>)>>();
        let mut iter = db.iter(rtxn)?;

        std::thread::scope(|scope| -> Result<BinaryHeap<Candidate>> {
            for rx in job_rxs {
                let tx = res_tx.clone();
                scope.spawn(move || {
                    while let Ok((idx, chunk)) = rx.recv() {
                        let mut local: BinaryHeap<Candidate> =
                            BinaryHeap::with_capacity(want.saturating_add(1));
                        for &(offset, key_len, value_len) in &chunk.entries {
                            let offset = offset as usize;
                            let key_len = key_len as usize;
                            let value_len = value_len as usize;
                            let key = &chunk.buffer[offset..offset + key_len];
                            let value =
                                &chunk.buffer[offset + key_len..offset + key_len + value_len];
                            let Some(distance) = vector::distance_from_bytes(
                                def.metric, query, query_norm, value, def.dims,
                            ) else {
                                continue;
                            };
                            let Ok(id) = std::str::from_utf8(key) else {
                                continue;
                            };
                            if local.len() >= want {
                                let worst = local.peek().expect("non-empty vector heap");
                                if distance > worst.distance
                                    || (distance == worst.distance && id >= worst.id.as_str())
                                {
                                    continue;
                                }
                            }
                            local.push(Candidate {
                                distance,
                                id: id.to_string(),
                            });
                            if local.len() > want {
                                local.pop();
                            }
                        }
                        if tx.send(Ok((idx, local.into_vec()))).is_err() {
                            break;
                        }
                    }
                });
            }
            drop(res_tx);

            let mut top = BinaryHeap::with_capacity(want.saturating_add(1));
            let mut next_idx = 0usize;
            let mut error: Option<Error> = None;
            loop {
                let mut sent = 0usize;
                for tx in &job_txs {
                    let mut chunk = SearchChunk::with_capacity(PARALLEL_SEARCH_CHUNK);
                    while chunk.len() < PARALLEL_SEARCH_CHUNK {
                        match iter.next() {
                            Some(Ok((key, value))) => chunk.push(key, value),
                            Some(Err(err)) => {
                                error = Some(err.into());
                                break;
                            }
                            None => break,
                        }
                    }
                    if error.is_some() || chunk.is_empty() {
                        break;
                    }
                    let idx = next_idx;
                    next_idx += 1;
                    if tx.send((idx, chunk)).is_err() {
                        error = Some(Error::Query("parallel vector worker stopped".into()));
                        break;
                    }
                    sent += 1;
                }
                if error.is_some() || sent == 0 {
                    break;
                }
                for _ in 0..sent {
                    match res_rx.recv() {
                        Ok(Ok((_idx, local))) => {
                            for candidate in local {
                                top.push(candidate);
                                if top.len() > want {
                                    top.pop();
                                }
                            }
                        }
                        Ok(Err(err)) => {
                            error = Some(err);
                            break;
                        }
                        Err(_) => {
                            error = Some(Error::Query(
                                "parallel vector scan returned no result".into(),
                            ));
                            break;
                        }
                    }
                }
            }
            drop(job_txs);
            if let Some(error) = error {
                Err(error)
            } else {
                Ok(top)
            }
        })
    }

    fn vector_results_from_heap(
        &self,
        rtxn: &RoTxn<'_>,
        heap: BinaryHeap<Candidate>,
        skip: usize,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let mut ranked = heap.into_vec();
        ranked.sort_by(|a, b| {
            a.distance
                .total_cmp(&b.distance)
                .then_with(|| a.id.cmp(&b.id))
        });
        let mut out = Vec::new();
        for candidate in ranked.into_iter().skip(skip).take(limit) {
            let Some(mut doc) = self.document_in(rtxn, &candidate.id)? else {
                continue;
            };
            if let Some(object) = doc.as_object_mut() {
                object.insert(
                    "_distance".to_string(),
                    serde_json::json!(f64::from(candidate.distance)),
                );
            }
            out.push(doc);
        }
        Ok(out)
    }

    /// Creates a search index and backfills it with existing documents.
    pub fn create_search_index(&self, def: &SearchIndexDef) -> Result<()> {
        let mut wtxn = self.write_txn()?;
        self.create_search_index_txn(&mut wtxn, def)?;
        wtxn.commit()?;
        Ok(())
    }

    pub fn create_search_index_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        def: &SearchIndexDef,
    ) -> Result<()> {
        self.ensure_standard()?;
        def.validate().map_err(Error::Query)?;
        let def_key = search::def_key(&self.name, def.kind, &def.name);
        if let Some(bytes) = self.meta.get(&*wtxn, def_key.as_bytes())? {
            let existing: SearchIndexDef = serde_json::from_slice(bytes)?;
            if existing == *def {
                return Ok(());
            }
            return Err(Error::Query(format!(
                "search index `{}` already exists with different options",
                def.name
            )));
        }

        match def.kind {
            SearchKind::Text => self.backfill_segment_text_index(wtxn, def)?,
            SearchKind::Fuzzy => self.backfill_fuzzy_segment_index(wtxn, def)?,
        }
        self.meta.put(
            &mut *wtxn,
            def_key.as_bytes(),
            serde_json::to_vec(def)?.as_slice(),
        )?;
        Ok(())
    }

    /// Creates a text index backed by immutable search segments.
    pub fn create_text_segment_index(&self, field: &str) -> Result<()> {
        self.create_search_index(&SearchIndexDef::single(SearchKind::Text, field))
    }

    pub fn create_text_segment_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<()> {
        self.create_search_index_txn(wtxn, &SearchIndexDef::single(SearchKind::Text, field))
    }

    pub fn create_fuzzy_segment_index(&self, field: &str) -> Result<()> {
        self.create_search_index(&SearchIndexDef::single(SearchKind::Fuzzy, field))
    }

    pub fn create_fuzzy_segment_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<()> {
        self.create_search_index_txn(wtxn, &SearchIndexDef::single(SearchKind::Fuzzy, field))
    }

    fn cleanup_segment_orphans(&self, dir: &str, live: &[String]) -> Result<()> {
        let root = self.segment_root()?;
        let full = root.join(dir);
        let Ok(entries) = std::fs::read_dir(&full) else {
            return Ok(());
        };
        let live: HashSet<&str> = live
            .iter()
            .filter_map(|rel| std::path::Path::new(rel).file_name())
            .filter_map(|name| name.to_str())
            .collect();
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if (name.ends_with(".seg") || name.ends_with(".values")) && !live.contains(name) {
                let _ = std::fs::remove_file(entry.path());
            }
        }
        Ok(())
    }

    fn maybe_compact_text_segments(
        &self,
        wtxn: &mut RwTxn<'_>,
        def: &SearchIndexDef,
    ) -> Result<()> {
        let Some(mut manifest) = self.segment_manifest_in(&*wtxn, &def.name)? else {
            return Ok(());
        };
        if manifest.segments.len() < SEGMENT_COMPACT_MIN_SEGMENTS {
            return Ok(());
        }
        let root = self.segment_root()?;
        let dir = segment_index_dir(&self.name, &def.name);
        let mut suffix_docs = 0u32;
        for rel in &manifest.segments[1..] {
            suffix_docs += SegmentReader::open(root.join(rel))?.documents();
        }
        if suffix_docs > SEGMENT_COMPACT_MAX_DOCS {
            return Ok(());
        }
        self.cleanup_segment_orphans(&dir, &manifest.segments)?;

        let prefix = self.segment_delta_prefix(&def.name);
        let mut ids = Vec::new();
        for (idx, rel) in manifest.segments.iter().enumerate().skip(1) {
            let reader = SegmentReader::open(root.join(rel))?;
            for doc in 0..reader.documents() {
                let id = reader.doc_id(doc)?;
                if manifest
                    .tombstones
                    .get(&id)
                    .is_some_and(|deleted_at| idx < *deleted_at)
                {
                    continue;
                }
                let key = format!("{prefix}{id}");
                if self.meta.get(&*wtxn, key.as_bytes())?.is_some() {
                    continue;
                }
                ids.push(id);
            }
        }
        ids.sort_unstable();
        ids.dedup();

        manifest.segments.truncate(1);
        if !ids.is_empty() {
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let rel = format!("{dir}/compact-{stamp}.seg");
            let path = root.join(&rel);
            let mut builder = SegmentBuilder::new(&path)?;
            let field = &def.fields[0].path;
            for id in &ids {
                let Some(bytes) = self.db.get(&*wtxn, id.as_bytes())? else {
                    continue;
                };
                let doc: Value = serde_json::from_slice(bytes)?;
                if let Some(text) = index::field_value(&doc, field).and_then(search::field_text) {
                    builder.add_document(id, &text)?;
                }
            }
            builder.finish()?;
            manifest.segments.push(rel);
        }
        let new_idx = manifest.segments.len().saturating_sub(1);
        for deleted_at in manifest.tombstones.values_mut() {
            *deleted_at = (*deleted_at).min(new_idx);
        }
        manifest.tombstones.retain(|_, deleted_at| *deleted_at > 0);
        self.put_segment_manifest(wtxn, &def.name, &manifest)?;
        Ok(())
    }

    fn full_compact_text_segment_index(
        &self,
        wtxn: &mut RwTxn<'_>,
        def: &SearchIndexDef,
    ) -> Result<()> {
        let root = self.segment_root()?;
        let dir = segment_index_dir(&self.name, &def.name);
        let manifest = self
            .segment_manifest_in(&*wtxn, &def.name)?
            .unwrap_or_default();
        self.cleanup_segment_orphans(&dir, &manifest.segments)?;
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let rel = format!("{dir}/full-{stamp}.seg");
        let mut builder = SegmentBuilder::new(root.join(&rel))?;
        let field = &def.fields[0].path;
        self.for_each_backfill_doc(&*wtxn, |id, doc| {
            if let Some(text) = index::field_value(doc, field).and_then(search::field_text) {
                builder.add_document(id, &text)?;
            }
            Ok(())
        })?;
        builder.finish()?;
        let prefix = self.segment_delta_prefix(&def.name);
        let keys: Vec<Vec<u8>> = self
            .meta
            .prefix_iter(&*wtxn, prefix.as_bytes())?
            .map(|item| item.map(|(key, _)| key.to_vec()))
            .collect::<std::result::Result<_, _>>()?;
        for key in keys {
            self.meta.delete(&mut *wtxn, &key)?;
        }
        self.set_segment_delta_count(wtxn, &def.name, 0)?;
        self.put_segment_manifest(
            wtxn,
            &def.name,
            &SegmentManifest {
                segments: vec![rel],
                tombstones: HashMap::new(),
            },
        )?;
        self.rebuild_segment_stats(wtxn, def)?;
        Ok(())
    }

    fn maybe_full_compact_text_segments(
        &self,
        wtxn: &mut RwTxn<'_>,
        def: &SearchIndexDef,
    ) -> Result<()> {
        let Some(manifest) = self.segment_manifest_in(&*wtxn, &def.name)? else {
            return Ok(());
        };
        let Some(base) = manifest.segments.first() else {
            return Ok(());
        };
        let base_docs = SegmentReader::open(self.segment_root()?.join(base))?.documents();
        if base_docs == 0
            || (manifest.tombstones.len() as u64).saturating_mul(4) <= u64::from(base_docs)
        {
            return Ok(());
        }
        self.full_compact_text_segment_index(wtxn, def)
    }

    fn full_compact_fuzzy_segment_index(
        &self,
        wtxn: &mut RwTxn<'_>,
        def: &SearchIndexDef,
    ) -> Result<()> {
        let root = self.segment_root()?;
        let dir = fuzzy_segment_index_dir(&self.name, &def.name);
        let manifest = self
            .fuzzy_segment_manifest_in(&*wtxn, &def.name)?
            .unwrap_or_default();
        let mut live = manifest.segments.clone();
        live.extend(manifest.values.iter().cloned());
        self.cleanup_segment_orphans(&dir, &live)?;
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let rel = format!("{dir}/full-{stamp}.seg");
        let values_rel = format!("{dir}/full-{stamp}.values");
        let mut builder = FuzzySegmentBuilder::new(root.join(&rel))?;
        let field = &def.fields[0].path;
        self.for_each_backfill_doc(&*wtxn, |id, doc| {
            if let Some(text) = index::field_value(doc, field).and_then(search::field_text) {
                builder.add_document(id, &text)?;
            }
            Ok(())
        })?;
        builder.finish(root.join(&values_rel))?;
        let prefix = self.fuzzy_segment_delta_prefix(&def.name);
        let keys: Vec<Vec<u8>> = self
            .meta
            .prefix_iter(&*wtxn, prefix.as_bytes())?
            .map(|item| item.map(|(key, _)| key.to_vec()))
            .collect::<std::result::Result<_, _>>()?;
        for key in keys {
            self.meta.delete(&mut *wtxn, &key)?;
        }
        self.set_fuzzy_segment_delta_count(wtxn, &def.name, 0)?;
        self.put_fuzzy_segment_manifest(
            wtxn,
            &def.name,
            &FuzzySegmentManifest {
                segments: vec![rel],
                values: vec![values_rel],
                tombstones: HashMap::new(),
            },
        )?;
        Ok(())
    }

    fn maybe_full_compact_fuzzy_segments(
        &self,
        wtxn: &mut RwTxn<'_>,
        def: &SearchIndexDef,
    ) -> Result<()> {
        let Some(manifest) = self.fuzzy_segment_manifest_in(&*wtxn, &def.name)? else {
            return Ok(());
        };
        let Some(base) = manifest.segments.first() else {
            return Ok(());
        };
        let base_docs = SegmentReader::open(self.segment_root()?.join(base))?.documents();
        if base_docs == 0
            || (manifest.tombstones.len() as u64).saturating_mul(4) <= u64::from(base_docs)
        {
            return Ok(());
        }
        self.full_compact_fuzzy_segment_index(wtxn, def)
    }

    fn maybe_compact_fuzzy_segments(
        &self,
        wtxn: &mut RwTxn<'_>,
        def: &SearchIndexDef,
    ) -> Result<()> {
        let Some(mut manifest) = self.fuzzy_segment_manifest_in(&*wtxn, &def.name)? else {
            return Ok(());
        };
        if manifest.segments.len() < SEGMENT_COMPACT_MIN_SEGMENTS {
            return Ok(());
        }
        let root = self.segment_root()?;
        let dir = fuzzy_segment_index_dir(&self.name, &def.name);
        let mut suffix_docs = 0u32;
        for rel in &manifest.segments[1..] {
            suffix_docs += SegmentReader::open(root.join(rel))?.documents();
        }
        if suffix_docs > SEGMENT_COMPACT_MAX_DOCS {
            return Ok(());
        }
        let mut live = manifest.segments.clone();
        live.extend(manifest.values.iter().cloned());
        self.cleanup_segment_orphans(&dir, &live)?;

        let prefix = self.fuzzy_segment_delta_prefix(&def.name);
        let mut ids = Vec::new();
        for (idx, rel) in manifest.segments.iter().enumerate().skip(1) {
            let reader = SegmentReader::open(root.join(rel))?;
            for doc in 0..reader.documents() {
                let id = reader.doc_id(doc)?;
                if manifest
                    .tombstones
                    .get(&id)
                    .is_some_and(|deleted_at| idx < *deleted_at)
                {
                    continue;
                }
                let key = format!("{prefix}{id}");
                if self.meta.get(&*wtxn, key.as_bytes())?.is_some() {
                    continue;
                }
                ids.push(id);
            }
        }
        ids.sort_unstable();
        ids.dedup();

        manifest.segments.truncate(1);
        manifest.values.truncate(1);
        if !ids.is_empty() {
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let rel = format!("{dir}/compact-{stamp}.seg");
            let values_rel = format!("{dir}/compact-{stamp}.values");
            let path = root.join(&rel);
            let values_path = root.join(&values_rel);
            let mut builder = FuzzySegmentBuilder::new(&path)?;
            let field = &def.fields[0].path;
            for id in &ids {
                let Some(bytes) = self.db.get(&*wtxn, id.as_bytes())? else {
                    continue;
                };
                let doc: Value = serde_json::from_slice(bytes)?;
                if let Some(text) = index::field_value(&doc, field).and_then(search::field_text) {
                    builder.add_document(id, &text)?;
                }
            }
            builder.finish(&values_path)?;
            manifest.segments.push(rel);
            manifest.values.push(values_rel);
        }
        let new_idx = manifest.segments.len().saturating_sub(1);
        for deleted_at in manifest.tombstones.values_mut() {
            *deleted_at = (*deleted_at).min(new_idx);
        }
        manifest.tombstones.retain(|_, deleted_at| *deleted_at > 0);
        self.put_fuzzy_segment_manifest(wtxn, &def.name, &manifest)?;
        Ok(())
    }

    fn fuzzy_segment_manifest_key(&self, index: &str) -> String {
        format!("fsegmanifest:{}:{index}", self.name)
    }

    fn fuzzy_segment_delta_prefix(&self, index: &str) -> String {
        format!("fsegdelta:{}:{index}:", self.name)
    }

    fn fuzzy_segment_delta_count_key(&self, index: &str) -> String {
        format!("fsegdeltacount:{}:{index}", self.name)
    }

    fn fuzzy_segment_manifest_in(
        &self,
        txn: &RoTxn<'_>,
        index: &str,
    ) -> Result<Option<FuzzySegmentManifest>> {
        let key = self.fuzzy_segment_manifest_key(index);
        let Some(bytes) = self.meta.get(txn, key.as_bytes())? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(bytes)?))
    }

    fn put_fuzzy_segment_manifest(
        &self,
        wtxn: &mut RwTxn<'_>,
        index: &str,
        manifest: &FuzzySegmentManifest,
    ) -> Result<()> {
        let key = self.fuzzy_segment_manifest_key(index);
        self.meta.put(
            &mut *wtxn,
            key.as_bytes(),
            serde_json::to_vec(manifest)?.as_slice(),
        )?;
        Ok(())
    }

    fn fuzzy_segment_delta_count(&self, txn: &RoTxn<'_>, index: &str) -> u64 {
        self.meta
            .get(txn, self.fuzzy_segment_delta_count_key(index).as_bytes())
            .ok()
            .flatten()
            .and_then(search::read_u64)
            .unwrap_or(0)
    }

    fn set_fuzzy_segment_delta_count(
        &self,
        wtxn: &mut RwTxn<'_>,
        index: &str,
        count: u64,
    ) -> Result<()> {
        self.meta.put(
            &mut *wtxn,
            self.fuzzy_segment_delta_count_key(index).as_bytes(),
            &count.to_le_bytes(),
        )?;
        Ok(())
    }

    fn backfill_fuzzy_segment_index(
        &self,
        wtxn: &mut RwTxn<'_>,
        def: &SearchIndexDef,
    ) -> Result<()> {
        if def.fields.len() != 1 {
            return Err(Error::Query(
                "segment fuzzy indexes support exactly one field".into(),
            ));
        }
        let root = self.segment_root()?;
        let dir = fuzzy_segment_index_dir(&self.name, &def.name);
        let rel = format!("{dir}/seg-000000.seg");
        let values_rel = format!("{dir}/seg-000000.values");
        let path = root.join(&rel);
        let values_path = root.join(&values_rel);
        let mut builder = FuzzySegmentBuilder::new(&path)?;
        let field = &def.fields[0].path;
        self.for_each_backfill_doc(&*wtxn, |id, doc| {
            if let Some(text) = index::field_value(doc, field).and_then(search::field_text) {
                builder.add_document(id, &text)?;
            }
            Ok(())
        })?;
        builder.finish(&values_path)?;
        let manifest = FuzzySegmentManifest {
            segments: vec![rel],
            values: vec![values_rel],
            tombstones: HashMap::new(),
        };
        self.put_fuzzy_segment_manifest(wtxn, &def.name, &manifest)?;
        self.set_fuzzy_segment_delta_count(wtxn, &def.name, 0)?;
        Ok(())
    }

    fn flush_fuzzy_segment_delta(&self, wtxn: &mut RwTxn<'_>, def: &SearchIndexDef) -> Result<()> {
        let Some(mut manifest) = self.fuzzy_segment_manifest_in(&*wtxn, &def.name)? else {
            return Ok(());
        };
        let prefix = self.fuzzy_segment_delta_prefix(&def.name);
        let mut entries: Vec<(String, String)> = Vec::new();
        for item in self.meta.prefix_iter(&*wtxn, prefix.as_bytes())? {
            let (key, value) = item?;
            let id = std::str::from_utf8(&key[prefix.len()..])
                .map_err(|e| Error::Query(format!("invalid fuzzy delta id: {e}")))?;
            let text = std::str::from_utf8(value)
                .map_err(|e| Error::Query(format!("invalid fuzzy delta text: {e}")))?;
            entries.push((id.to_string(), text.to_string()));
        }
        if entries.is_empty() {
            self.set_fuzzy_segment_delta_count(wtxn, &def.name, 0)?;
            return Ok(());
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let root = self.segment_root()?;
        let dir = fuzzy_segment_index_dir(&self.name, &def.name);
        let gen = manifest.segments.len();
        let rel = format!("{dir}/seg-{gen:06}.seg");
        let values_rel = format!("{dir}/seg-{gen:06}.values");
        let path = root.join(&rel);
        let values_path = root.join(&values_rel);
        let mut builder = FuzzySegmentBuilder::new(&path)?;
        for (id, text) in &entries {
            builder.add_document(id, text)?;
        }
        builder.finish(&values_path)?;
        manifest.segments.push(rel);
        manifest.values.push(values_rel);
        self.put_fuzzy_segment_manifest(wtxn, &def.name, &manifest)?;
        for (id, _) in &entries {
            let key = format!("{prefix}{id}");
            self.meta.delete(&mut *wtxn, key.as_bytes())?;
        }
        self.set_fuzzy_segment_delta_count(wtxn, &def.name, 0)?;
        self.maybe_compact_fuzzy_segments(wtxn, def)?;
        self.maybe_full_compact_fuzzy_segments(wtxn, def)?;
        Ok(())
    }

    fn segment_root(&self) -> Result<std::path::PathBuf> {
        self.store
            .map(|store| store.env().path().to_path_buf())
            .ok_or_else(|| Error::Query("segment search requires a store path".into()))
    }

    fn vector_ann_path(&self, field: &str) -> Result<std::path::PathBuf> {
        Ok(self
            .segment_root()?
            .join(vector_ann_index_dir(&self.name, field))
            .join("ann-000000.ivf"))
    }

    fn vector_ann_delta_in(&self, rtxn: &RoTxn<'_>, field: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let prefix = vector::ann_delta_prefix(&self.name, field);
        let mut out = Vec::new();
        for item in self.meta.prefix_iter(rtxn, prefix.as_bytes())? {
            let (key, value) = item?;
            let id = std::str::from_utf8(&key[prefix.len()..])
                .map_err(|error| Error::Query(format!("invalid ANN delta id: {error}")))?;
            out.push((id.to_string(), value.to_vec()));
        }
        Ok(out)
    }

    fn clear_vector_ann_delta(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<()> {
        let prefix = vector::ann_delta_prefix(&self.name, field);
        let keys: Vec<Vec<u8>> = self
            .meta
            .prefix_iter(&*wtxn, prefix.as_bytes())?
            .map(|item| item.map(|(key, _)| key.to_vec()))
            .collect::<std::result::Result<_, _>>()?;
        for key in keys {
            self.meta.delete(&mut *wtxn, &key)?;
        }
        self.meta.delete(
            &mut *wtxn,
            vector::ann_delta_count_key(&self.name, field).as_bytes(),
        )?;
        Ok(())
    }

    fn segment_manifest_key(&self, index: &str) -> String {
        format!("segmanifest:{}:{index}", self.name)
    }

    fn segment_stats_key(&self, index: &str) -> String {
        format!("segstats:{}:{index}", self.name)
    }

    fn segment_stats_df_prefix(&self, index: &str) -> String {
        format!("segstatsdf:{}:{index}:", self.name)
    }

    fn segment_stats_df_key(&self, index: &str, term: &str) -> String {
        format!("segstatsdf:{}:{index}:{term}", self.name)
    }

    fn segment_stats_in(&self, txn: &RoTxn<'_>, index: &str) -> Result<Option<(u64, u64)>> {
        let Some(bytes) = self
            .meta
            .get(txn, self.segment_stats_key(index).as_bytes())?
        else {
            return Ok(None);
        };
        if bytes.len() != 16 {
            return Ok(None);
        }
        let documents = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        let total_doc_len = u64::from_le_bytes(bytes[8..].try_into().unwrap());
        Ok(Some((documents, total_doc_len)))
    }

    fn put_segment_stats(
        &self,
        wtxn: &mut RwTxn<'_>,
        index: &str,
        documents: u64,
        total_doc_len: u64,
    ) -> Result<()> {
        let mut value = [0u8; 16];
        value[..8].copy_from_slice(&documents.to_le_bytes());
        value[8..].copy_from_slice(&total_doc_len.to_le_bytes());
        self.meta
            .put(&mut *wtxn, self.segment_stats_key(index).as_bytes(), &value)?;
        Ok(())
    }

    fn segment_df_in(&self, txn: &RoTxn<'_>, index: &str, term: &str) -> Result<u64> {
        Ok(self
            .meta
            .get(txn, self.segment_stats_df_key(index, term).as_bytes())?
            .and_then(search::read_u64)
            .unwrap_or(0))
    }

    fn set_segment_df(&self, wtxn: &mut RwTxn<'_>, index: &str, term: &str, df: u64) -> Result<()> {
        let key = self.segment_stats_df_key(index, term);
        if df == 0 {
            self.meta.delete(&mut *wtxn, key.as_bytes())?;
        } else {
            self.meta
                .put(&mut *wtxn, key.as_bytes(), &df.to_le_bytes())?;
        }
        Ok(())
    }

    fn clear_segment_stats(&self, wtxn: &mut RwTxn<'_>, index: &str) -> Result<()> {
        self.meta
            .delete(&mut *wtxn, self.segment_stats_key(index).as_bytes())?;
        let prefix = self.segment_stats_df_prefix(index);
        let keys: Vec<Vec<u8>> = self
            .meta
            .prefix_iter(&*wtxn, prefix.as_bytes())?
            .map(|item| item.map(|(key, _)| key.to_vec()))
            .collect::<std::result::Result<_, _>>()?;
        for key in keys {
            self.meta.delete(&mut *wtxn, &key)?;
        }
        Ok(())
    }

    /// Recompute exact live BM25 statistics from the current documents.
    fn rebuild_segment_stats(&self, wtxn: &mut RwTxn<'_>, def: &SearchIndexDef) -> Result<()> {
        self.clear_segment_stats(wtxn, &def.name)?;
        let field = &def.fields[0].path;
        let mut documents = 0u64;
        let mut total_doc_len = 0u64;
        let mut df: HashMap<String, u64> = HashMap::new();
        for (_, doc) in self.documents_for_backfill(&*wtxn)? {
            let Some(text) = index::field_value(&doc, field).and_then(search::field_text) else {
                continue;
            };
            let tokens = search::tokenize(&text);
            documents += 1;
            total_doc_len += tokens.len() as u64;
            let mut terms: HashMap<&str, ()> = HashMap::new();
            for token in &tokens {
                terms.insert(token.as_str(), ());
            }
            for term in terms.keys() {
                *df.entry((*term).to_string()).or_insert(0) += 1;
            }
        }
        self.put_segment_stats(wtxn, &def.name, documents, total_doc_len)?;
        for (term, count) in df {
            self.set_segment_df(wtxn, &def.name, &term, count)?;
        }
        Ok(())
    }

    fn adjust_segment_stats(
        &self,
        wtxn: &mut RwTxn<'_>,
        def: &SearchIndexDef,
        old_text: Option<&str>,
        new_text: Option<&str>,
    ) -> Result<()> {
        let Some((mut documents, mut total_doc_len)) = self.segment_stats_in(&*wtxn, &def.name)?
        else {
            // Direct core API callers may open an index before the daemon's
            // startup statistics pass; keep the segment-sum fallback for that
            // case.
            return Ok(());
        };
        let mut changes: HashMap<String, i64> = HashMap::new();
        if let Some(text) = old_text {
            documents = documents.saturating_sub(1);
            let tokens = search::tokenize(text);
            total_doc_len = total_doc_len.saturating_sub(tokens.len() as u64);
            let mut terms: HashMap<&str, ()> = HashMap::new();
            for token in &tokens {
                terms.insert(token.as_str(), ());
            }
            for term in terms.keys() {
                *changes.entry((*term).to_string()).or_insert(0) -= 1;
            }
        }
        if let Some(text) = new_text {
            documents += 1;
            let tokens = search::tokenize(text);
            total_doc_len += tokens.len() as u64;
            let mut terms: HashMap<&str, ()> = HashMap::new();
            for token in &tokens {
                terms.insert(token.as_str(), ());
            }
            for term in terms.keys() {
                *changes.entry((*term).to_string()).or_insert(0) += 1;
            }
        }
        for (term, change) in changes {
            if change == 0 {
                continue;
            }
            let current = self.segment_df_in(&*wtxn, &def.name, &term)? as i64;
            let next = (current + change).max(0) as u64;
            self.set_segment_df(wtxn, &def.name, &term, next)?;
        }
        self.put_segment_stats(wtxn, &def.name, documents, total_doc_len)?;
        Ok(())
    }

    fn segment_delta_prefix(&self, index: &str) -> String {
        format!("segdelta:{}:{index}:", self.name)
    }

    fn segment_delta_count_key(&self, index: &str) -> String {
        format!("segdeltacount:{}:{index}", self.name)
    }

    fn segment_manifest_in(&self, txn: &RoTxn<'_>, index: &str) -> Result<Option<SegmentManifest>> {
        let key = self.segment_manifest_key(index);
        let Some(bytes) = self.meta.get(txn, key.as_bytes())? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(bytes)?))
    }

    fn put_segment_manifest(
        &self,
        wtxn: &mut RwTxn<'_>,
        index: &str,
        manifest: &SegmentManifest,
    ) -> Result<()> {
        let key = self.segment_manifest_key(index);
        self.meta.put(
            &mut *wtxn,
            key.as_bytes(),
            serde_json::to_vec(manifest)?.as_slice(),
        )?;
        Ok(())
    }

    fn segment_delta_count(&self, txn: &RoTxn<'_>, index: &str) -> u64 {
        self.meta
            .get(txn, self.segment_delta_count_key(index).as_bytes())
            .ok()
            .flatten()
            .and_then(search::read_u64)
            .unwrap_or(0)
    }

    fn set_segment_delta_count(&self, wtxn: &mut RwTxn<'_>, index: &str, count: u64) -> Result<()> {
        self.meta.put(
            &mut *wtxn,
            self.segment_delta_count_key(index).as_bytes(),
            &count.to_le_bytes(),
        )?;
        Ok(())
    }

    fn backfill_segment_text_index(
        &self,
        wtxn: &mut RwTxn<'_>,
        def: &SearchIndexDef,
    ) -> Result<()> {
        if def.fields.len() != 1 {
            return Err(Error::Query(
                "segment text indexes support exactly one field".into(),
            ));
        }
        let root = self.segment_root()?;
        let dir = segment_index_dir(&self.name, &def.name);
        let rel = format!("{dir}/seg-000000.seg");
        let path = root.join(&rel);
        let mut builder = SegmentBuilder::new(&path)?;
        let field = &def.fields[0].path;
        self.for_each_backfill_doc(&*wtxn, |id, doc| {
            if let Some(text) = index::field_value(doc, field).and_then(search::field_text) {
                builder.add_document(id, &text)?;
            }
            Ok(())
        })?;
        builder.finish()?;
        let manifest = SegmentManifest {
            segments: vec![rel],
            tombstones: HashMap::new(),
        };
        self.put_segment_manifest(wtxn, &def.name, &manifest)?;
        self.set_segment_delta_count(wtxn, &def.name, 0)?;
        self.rebuild_segment_stats(wtxn, def)?;
        Ok(())
    }

    fn flush_segment_delta(&self, wtxn: &mut RwTxn<'_>, def: &SearchIndexDef) -> Result<()> {
        let Some(mut manifest) = self.segment_manifest_in(&*wtxn, &def.name)? else {
            return Ok(());
        };
        let prefix = self.segment_delta_prefix(&def.name);
        let mut entries: Vec<(String, String)> = Vec::new();
        for item in self.meta.prefix_iter(&*wtxn, prefix.as_bytes())? {
            let (key, value) = item?;
            let id = std::str::from_utf8(&key[prefix.len()..])
                .map_err(|e| Error::Query(format!("invalid segment delta id: {e}")))?;
            let text = std::str::from_utf8(value)
                .map_err(|e| Error::Query(format!("invalid segment delta text: {e}")))?;
            entries.push((id.to_string(), text.to_string()));
        }
        if entries.is_empty() {
            self.set_segment_delta_count(wtxn, &def.name, 0)?;
            return Ok(());
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let root = self.segment_root()?;
        let dir = segment_index_dir(&self.name, &def.name);
        let rel = format!("{dir}/seg-{:06}.seg", manifest.segments.len());
        let path = root.join(&rel);
        let mut builder = SegmentBuilder::new(&path)?;
        for (id, text) in &entries {
            builder.add_document(id, text)?;
        }
        builder.finish()?;
        manifest.segments.push(rel);
        self.put_segment_manifest(wtxn, &def.name, &manifest)?;
        for (id, _) in &entries {
            let key = format!("{prefix}{id}");
            self.meta.delete(&mut *wtxn, key.as_bytes())?;
        }
        self.set_segment_delta_count(wtxn, &def.name, 0)?;
        self.maybe_compact_text_segments(wtxn, def)?;
        self.maybe_full_compact_text_segments(wtxn, def)?;
        Ok(())
    }

    /// Creates a full-text (BM25) index and backfills it with existing documents.
    pub fn create_text_index(&self, field: &str) -> Result<()> {
        self.create_search_index(&SearchIndexDef::single(SearchKind::Text, field))
    }

    pub fn create_text_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<()> {
        self.create_search_index_txn(wtxn, &SearchIndexDef::single(SearchKind::Text, field))
    }

    /// Drops a full-text index.
    pub fn drop_text_index(&self, field: &str) -> Result<bool> {
        self.drop_search_index(SearchKind::Text, field)
    }

    pub fn drop_text_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<bool> {
        self.drop_search_index_txn(wtxn, SearchKind::Text, field)
    }

    /// Lists immutable search files referenced by segment text/fuzzy indexes
    /// and optional vector ANN indexes. Paths are relative to the database
    /// directory and are suitable for snapshot sidecar transfer.
    pub fn search_sidecar_files_in(&self, rtxn: &RoTxn<'_>) -> Result<Vec<String>> {
        let mut files = Vec::new();
        for def in self.search_index_defs_in(rtxn)? {
            match def.kind {
                SearchKind::Text => {
                    if let Some(manifest) = self.segment_manifest_in(rtxn, &def.name)? {
                        files.extend(manifest.segments);
                    }
                }
                SearchKind::Fuzzy => {
                    if let Some(manifest) = self.fuzzy_segment_manifest_in(rtxn, &def.name)? {
                        files.extend(manifest.segments);
                        files.extend(manifest.values);
                    }
                }
            }
        }
        for def in self.list_vector_indexes_in(rtxn)? {
            if def.ann.is_some() {
                files.push(format!(
                    "{}/ann-000000.ivf",
                    vector_ann_index_dir(&self.name, &def.field)
                ));
            }
        }
        files.sort_unstable();
        files.dedup();
        Ok(files)
    }

    pub fn list_text_indexes(&self) -> Result<Vec<String>> {
        let rtxn = self.read_txn()?;
        let out = self.list_text_indexes_in(&rtxn)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn list_text_indexes_in(&self, rtxn: &RoTxn<'_>) -> Result<Vec<String>> {
        self.text_defs(rtxn)
    }

    /// Creates a trigram (fuzzy) index and backfills it with existing documents.
    pub fn create_fuzzy_index(&self, field: &str) -> Result<()> {
        self.create_search_index(&SearchIndexDef::single(SearchKind::Fuzzy, field))
    }

    pub fn create_fuzzy_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<()> {
        self.create_search_index_txn(wtxn, &SearchIndexDef::single(SearchKind::Fuzzy, field))
    }

    /// Drops a fuzzy search index.
    pub fn drop_fuzzy_index(&self, field: &str) -> Result<bool> {
        self.drop_search_index(SearchKind::Fuzzy, field)
    }

    pub fn drop_fuzzy_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<bool> {
        self.drop_search_index_txn(wtxn, SearchKind::Fuzzy, field)
    }

    pub fn list_fuzzy_indexes(&self) -> Result<Vec<String>> {
        let rtxn = self.read_txn()?;
        let out = self.list_fuzzy_indexes_in(&rtxn)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn list_fuzzy_indexes_in(&self, rtxn: &RoTxn<'_>) -> Result<Vec<String>> {
        self.fuzzy_defs(rtxn)
    }

    fn clear_meta_prefix(&self, wtxn: &mut RwTxn<'_>, prefix: &str) -> Result<()> {
        let keys: Vec<Vec<u8>> = self
            .meta
            .prefix_iter(&*wtxn, prefix.as_bytes())?
            .map(|item| item.map(|(key, _)| key.to_vec()))
            .collect::<std::result::Result<_, _>>()?;
        for key in keys {
            self.meta.delete(&mut *wtxn, &key)?;
        }
        Ok(())
    }

    fn search_sidecar_missing(&self, rtxn: &RoTxn<'_>, def: &SearchIndexDef) -> Result<bool> {
        let root = self.segment_root()?;
        match def.kind {
            SearchKind::Text => {
                let Some(manifest) = self.segment_manifest_in(rtxn, &def.name)? else {
                    return Ok(true);
                };
                Ok(manifest
                    .segments
                    .iter()
                    .any(|relative| !root.join(relative).is_file()))
            }
            SearchKind::Fuzzy => {
                let Some(manifest) = self.fuzzy_segment_manifest_in(rtxn, &def.name)? else {
                    return Ok(true);
                };
                Ok(manifest
                    .segments
                    .iter()
                    .chain(manifest.values.iter())
                    .any(|relative| !root.join(relative).is_file()))
            }
        }
    }

    /// Rebuilds segment files and vector ANN sidecars that are missing from
    /// the data directory after a restore. LMDB manifests and definitions are
    /// the source of truth; each rebuilt index gets its own transaction.
    pub fn rebuild_missing_search_sidecars(&self) -> Result<usize> {
        let (search_defs, vector_defs) = {
            let rtxn = self.read_txn()?;
            let mut search_defs = Vec::new();
            for def in self.search_index_defs_in(&rtxn)? {
                if self.search_sidecar_missing(&rtxn, &def)? {
                    search_defs.push(def);
                }
            }
            let vector_defs = self
                .vector_defs(&rtxn)?
                .into_iter()
                .filter(|def| def.ann.is_some())
                .collect::<Vec<_>>();
            rtxn.commit()?;
            (search_defs, vector_defs)
        };
        let mut rebuilt = 0usize;
        for def in search_defs {
            let mut wtxn = self.write_txn()?;
            let dir = match def.kind {
                SearchKind::Text => segment_index_dir(&self.name, &def.name),
                SearchKind::Fuzzy => fuzzy_segment_index_dir(&self.name, &def.name),
            };
            if let Some(store) = self.store {
                let path = store.env().path().join(dir);
                match std::fs::remove_dir_all(&path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            match def.kind {
                SearchKind::Text => {
                    self.backfill_segment_text_index(&mut wtxn, &def)?;
                    self.clear_meta_prefix(&mut wtxn, &self.segment_delta_prefix(&def.name))?;
                    self.set_segment_delta_count(&mut wtxn, &def.name, 0)?;
                }
                SearchKind::Fuzzy => {
                    self.backfill_fuzzy_segment_index(&mut wtxn, &def)?;
                    self.clear_meta_prefix(&mut wtxn, &self.fuzzy_segment_delta_prefix(&def.name))?;
                    self.set_fuzzy_segment_delta_count(&mut wtxn, &def.name, 0)?;
                }
            }
            wtxn.commit()?;
            rebuilt += 1;
        }
        for def in vector_defs {
            if !self.vector_ann_sidecar_exists(&def.field) {
                self.build_vector_ann_index(
                    &def.field,
                    def.ann.map(|ann| ann.nprobe as usize).unwrap_or(16),
                )?;
                rebuilt += 1;
            }
        }
        Ok(rebuilt)
    }

    /// Fills exact live BM25 statistics for segment text indexes that do not
    /// have them yet. The daemon calls this at startup. Returns the number of
    /// indexes initialized.
    pub fn ensure_segment_text_stats(&self) -> Result<usize> {
        let defs = {
            let rtxn = self.read_txn()?;
            let defs = self
                .search_index_defs_in(&rtxn)?
                .into_iter()
                .filter(|def| def.kind == SearchKind::Text && def.fields.len() == 1)
                .collect::<Vec<_>>();
            rtxn.commit()?;
            defs
        };
        let mut initialized = 0usize;
        for def in defs {
            let missing = {
                let rtxn = self.read_txn()?;
                let missing = self.segment_stats_in(&rtxn, &def.name)?.is_none();
                rtxn.commit()?;
                missing
            };
            if !missing {
                continue;
            }
            let mut wtxn = self.write_txn()?;
            self.rebuild_segment_stats(&mut wtxn, &def)?;
            wtxn.commit()?;
            initialized += 1;
        }
        Ok(initialized)
    }

    /// Recomputes exact live BM25 statistics for every segment-backed text index.
    pub fn rebuild_segment_text_stats(&self) -> Result<usize> {
        let defs = {
            let rtxn = self.read_txn()?;
            let defs = self
                .search_index_defs_in(&rtxn)?
                .into_iter()
                .filter(|def| def.kind == SearchKind::Text && def.fields.len() == 1)
                .collect::<Vec<_>>();
            rtxn.commit()?;
            defs
        };
        let mut rebuilt = 0usize;
        for def in defs {
            let mut wtxn = self.write_txn()?;
            self.rebuild_segment_stats(&mut wtxn, &def)?;
            wtxn.commit()?;
            rebuilt += 1;
        }
        Ok(rebuilt)
    }

    pub fn drop_search_index(&self, kind: SearchKind, name: &str) -> Result<bool> {
        let mut wtxn = self.write_txn()?;
        let existed = self.drop_search_index_txn(&mut wtxn, kind, name)?;
        wtxn.commit()?;
        Ok(existed)
    }

    pub fn drop_search_index_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        kind: SearchKind,
        name: &str,
    ) -> Result<bool> {
        let def_key = search::def_key(&self.name, kind, name);
        let Some(bytes) = self.meta.get(&*wtxn, def_key.as_bytes())? else {
            return Ok(false);
        };
        let def: SearchIndexDef = serde_json::from_slice(bytes)?;
        self.meta.delete(&mut *wtxn, def_key.as_bytes())?;
        let (manifest_key, prefix, count_key, dir) = match def.kind {
            SearchKind::Text => (
                self.segment_manifest_key(&def.name),
                self.segment_delta_prefix(&def.name),
                self.segment_delta_count_key(&def.name),
                segment_index_dir(&self.name, &def.name),
            ),
            SearchKind::Fuzzy => (
                self.fuzzy_segment_manifest_key(&def.name),
                self.fuzzy_segment_delta_prefix(&def.name),
                self.fuzzy_segment_delta_count_key(&def.name),
                fuzzy_segment_index_dir(&self.name, &def.name),
            ),
        };
        self.meta.delete(&mut *wtxn, manifest_key.as_bytes())?;
        let keys: Vec<Vec<u8>> = self
            .meta
            .prefix_iter(&*wtxn, prefix.as_bytes())?
            .map(|item| item.map(|(key, _)| key.to_vec()))
            .collect::<std::result::Result<_, _>>()?;
        for key in keys {
            self.meta.delete(&mut *wtxn, &key)?;
        }
        self.meta.delete(&mut *wtxn, count_key.as_bytes())?;
        self.clear_segment_stats(wtxn, &def.name)?;
        if let Some(store) = self.store {
            let dir = store.env().path().join(dir);
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(true)
    }

    fn apply_segment_text_entry(
        &self,
        wtxn: &mut RwTxn<'_>,
        def: &SearchIndexDef,
        old_text: Option<&str>,
        new_text: Option<&str>,
        id: &str,
    ) -> Result<()> {
        let prefix = self.segment_delta_prefix(&def.name);
        let key = format!("{prefix}{id}");
        let existed = self.meta.get(&*wtxn, key.as_bytes())?.is_some();
        let mut count = self.segment_delta_count(&*wtxn, &def.name);
        let mut manifest = self
            .segment_manifest_in(&*wtxn, &def.name)?
            .unwrap_or_default();
        let mut manifest_changed = false;
        if old_text.is_some() {
            manifest
                .tombstones
                .insert(id.to_string(), manifest.segments.len());
            manifest_changed = true;
        }
        if let Some(text) = new_text {
            self.meta.put(&mut *wtxn, key.as_bytes(), text.as_bytes())?;
            if !existed {
                count = count.saturating_add(1);
            }
        } else {
            self.meta.delete(&mut *wtxn, key.as_bytes())?;
            if existed {
                count = count.saturating_sub(1);
            }
        }
        if manifest_changed {
            self.put_segment_manifest(wtxn, &def.name, &manifest)?;
        }
        self.adjust_segment_stats(wtxn, def, old_text, new_text)?;
        self.set_segment_delta_count(wtxn, &def.name, count)?;
        if count >= SEGMENT_DELTA_FLUSH_DOCS {
            self.flush_segment_delta(wtxn, def)?;
        }
        Ok(())
    }

    /// Maintains full-text indexes on an `old -> new` change.
    fn apply_text_entries(
        &self,
        wtxn: &mut RwTxn<'_>,
        old: Option<&Value>,
        new: Option<&Value>,
        id: &str,
    ) -> Result<()> {
        for def in self.search_index_defs_in(&*wtxn)? {
            if def.kind != SearchKind::Text {
                continue;
            }
            for field in &def.fields {
                let old_text = old
                    .and_then(|d| index::field_value(d, &field.path))
                    .and_then(search::field_text);
                let new_text = new
                    .and_then(|d| index::field_value(d, &field.path))
                    .and_then(search::field_text);
                if old_text == new_text {
                    continue;
                }
                self.apply_segment_text_entry(
                    wtxn,
                    &def,
                    old_text.as_deref(),
                    new_text.as_deref(),
                    id,
                )?;
            }
        }
        Ok(())
    }

    /// Applies deletions for many documents while writing each text manifest
    /// once. Bulk deletes otherwise reserialize a growing tombstone map for
    /// every document, which is quadratic for large collections.
    fn apply_text_deletes(&self, wtxn: &mut RwTxn<'_>, docs: &[(String, Value)]) -> Result<()> {
        if docs.is_empty() {
            return Ok(());
        }
        for def in self.search_index_defs_in(&*wtxn)? {
            if def.kind != SearchKind::Text {
                continue;
            }
            let mut manifest = self
                .segment_manifest_in(&*wtxn, &def.name)?
                .unwrap_or_default();
            let prefix = self.segment_delta_prefix(&def.name);
            let mut count = self.segment_delta_count(&*wtxn, &def.name);
            let mut manifest_changed = false;
            for field in &def.fields {
                for (id, old) in docs {
                    let Some(old_text) =
                        index::field_value(old, &field.path).and_then(search::field_text)
                    else {
                        continue;
                    };
                    manifest
                        .tombstones
                        .insert(id.clone(), manifest.segments.len());
                    manifest_changed = true;
                    let key = format!("{prefix}{id}");
                    if self.meta.get(&*wtxn, key.as_bytes())?.is_some() {
                        self.meta.delete(&mut *wtxn, key.as_bytes())?;
                        count = count.saturating_sub(1);
                    }
                    self.adjust_segment_stats(wtxn, &def, Some(&old_text), None)?;
                }
            }
            if manifest_changed {
                self.put_segment_manifest(wtxn, &def.name, &manifest)?;
            }
            self.set_segment_delta_count(wtxn, &def.name, count)?;
        }
        Ok(())
    }

    /// Applies fuzzy-index deletions for many documents with one manifest
    /// write, mirroring [`Self::apply_text_deletes`].
    fn apply_fuzzy_deletes(&self, wtxn: &mut RwTxn<'_>, docs: &[(String, Value)]) -> Result<()> {
        if docs.is_empty() {
            return Ok(());
        }
        for def in self.search_index_defs_in(&*wtxn)? {
            if def.kind != SearchKind::Fuzzy {
                continue;
            }
            let mut manifest = self
                .fuzzy_segment_manifest_in(&*wtxn, &def.name)?
                .unwrap_or_default();
            let prefix = self.fuzzy_segment_delta_prefix(&def.name);
            let mut count = self.fuzzy_segment_delta_count(&*wtxn, &def.name);
            let mut manifest_changed = false;
            for field in &def.fields {
                for (id, old) in docs {
                    let Some(_old_text) = index::field_value(old, &field.path)
                        .and_then(search::field_text)
                        .map(|text| search::normalize(&text))
                    else {
                        continue;
                    };
                    manifest
                        .tombstones
                        .insert(id.clone(), manifest.segments.len());
                    manifest_changed = true;
                    let key = format!("{prefix}{id}");
                    if self.meta.get(&*wtxn, key.as_bytes())?.is_some() {
                        self.meta.delete(&mut *wtxn, key.as_bytes())?;
                        count = count.saturating_sub(1);
                    }
                }
            }
            if manifest_changed {
                self.put_fuzzy_segment_manifest(wtxn, &def.name, &manifest)?;
            }
            self.set_fuzzy_segment_delta_count(wtxn, &def.name, count)?;
        }
        Ok(())
    }

    fn apply_segment_fuzzy_entry(
        &self,
        wtxn: &mut RwTxn<'_>,
        def: &SearchIndexDef,
        old_text: Option<&str>,
        new_text: Option<&str>,
        id: &str,
    ) -> Result<()> {
        let prefix = self.fuzzy_segment_delta_prefix(&def.name);
        let key = format!("{prefix}{id}");
        let existed = self.meta.get(&*wtxn, key.as_bytes())?.is_some();
        let mut count = self.fuzzy_segment_delta_count(&*wtxn, &def.name);
        let mut manifest = self
            .fuzzy_segment_manifest_in(&*wtxn, &def.name)?
            .unwrap_or_default();
        let mut manifest_changed = false;
        if old_text.is_some() {
            manifest
                .tombstones
                .insert(id.to_string(), manifest.segments.len());
            manifest_changed = true;
        }
        if let Some(text) = new_text {
            self.meta.put(&mut *wtxn, key.as_bytes(), text.as_bytes())?;
            if !existed {
                count = count.saturating_add(1);
            }
        } else {
            self.meta.delete(&mut *wtxn, key.as_bytes())?;
            if existed {
                count = count.saturating_sub(1);
            }
        }
        if manifest_changed {
            self.put_fuzzy_segment_manifest(wtxn, &def.name, &manifest)?;
        }
        self.set_fuzzy_segment_delta_count(wtxn, &def.name, count)?;
        if count >= SEGMENT_DELTA_FLUSH_DOCS {
            self.flush_fuzzy_segment_delta(wtxn, def)?;
        }
        Ok(())
    }

    /// Maintains trigram indexes on an `old -> new` change.
    fn apply_fuzzy_entries(
        &self,
        wtxn: &mut RwTxn<'_>,
        old: Option<&Value>,
        new: Option<&Value>,
        id: &str,
    ) -> Result<()> {
        for def in self.search_index_defs_in(&*wtxn)? {
            if def.kind != SearchKind::Fuzzy {
                continue;
            }
            for field in &def.fields {
                let old_text = old
                    .and_then(|d| index::field_value(d, &field.path))
                    .and_then(search::field_text)
                    .map(|t| search::normalize(&t));
                let new_text = new
                    .and_then(|d| index::field_value(d, &field.path))
                    .and_then(search::field_text)
                    .map(|t| search::normalize(&t));
                if old_text == new_text {
                    continue;
                }
                self.apply_segment_fuzzy_entry(
                    wtxn,
                    &def,
                    old_text.as_deref(),
                    new_text.as_deref(),
                    id,
                )?;
            }
        }
        Ok(())
    }

    /// Full-text search (BM25). Results include an extra `_score` field.
    pub fn search(
        &self,
        field: &str,
        query: &str,
        filter: &Value,
        skip: usize,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        let rtxn = self.read_txn()?;
        let out = self.search_in(&rtxn, field, query, filter, skip, limit)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn search_in(
        &self,
        rtxn: &RoTxn<'_>,
        field: &str,
        query: &str,
        filter: &Value,
        skip: usize,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        self.search_text_index_in(
            rtxn,
            field,
            query,
            filter,
            skip,
            limit,
            TextQueryMode::Native,
        )
    }

    /// Full-text search through a named search index.
    pub fn search_text_index(
        &self,
        name: &str,
        query: &str,
        filter: &Value,
        skip: usize,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        let rtxn = self.read_txn()?;
        let out = self.search_text_index_in(
            &rtxn,
            name,
            query,
            filter,
            skip,
            limit,
            TextQueryMode::Mongo,
        )?;
        rtxn.commit()?;
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    fn search_segment_text_index_in(
        &self,
        rtxn: &RoTxn<'_>,
        def: &SearchIndexDef,
        query: &str,
        skip: usize,
        limit: Option<usize>,
        allowed: Option<&HashSet<String>>,
        verify: Option<&DocVerifier<'_>>,
        phrases: &[Vec<String>],
    ) -> Result<Vec<Value>> {
        let max = limit.unwrap_or(10).min(1000);
        if max == 0 {
            return Ok(Vec::new());
        }
        let Some(manifest) = self.segment_manifest_in(rtxn, &def.name)? else {
            return Ok(Vec::new());
        };
        let root = self.segment_root()?;
        let mut segments = Vec::with_capacity(manifest.segments.len());
        for rel in &manifest.segments {
            segments.push(SegmentReader::open(root.join(rel))?);
        }
        let mut set = SegmentSet::with_segments(segments);
        set.set_tombstones(manifest.tombstones);
        let prefix = self.segment_delta_prefix(&def.name);
        for item in self.meta.prefix_iter(rtxn, prefix.as_bytes())? {
            let (key, value) = item?;
            let id = std::str::from_utf8(&key[prefix.len()..])
                .map_err(|e| Error::Query(format!("invalid segment delta id: {e}")))?;
            let text = std::str::from_utf8(value)
                .map_err(|e| Error::Query(format!("invalid segment delta text: {e}")))?;
            set.add_document(id, text)?;
        }
        if let Some((documents, total_doc_len)) = self.segment_stats_in(rtxn, &def.name)? {
            let mut terms = search::tokenize(query);
            terms.sort();
            terms.dedup();
            let mut df = HashMap::with_capacity(terms.len());
            for term in terms {
                df.insert(term.clone(), self.segment_df_in(rtxn, &def.name, &term)?);
            }
            set.set_exact_stats(documents, total_doc_len, df);
        }
        let hits = if phrases.is_empty() {
            set.search_verified(query, skip, max, allowed, verify)?
        } else {
            set.search_verified_phrases(query, skip, max, allowed, verify, phrases)?
        };
        let mut out = Vec::with_capacity(hits.len());
        for hit in hits {
            let Some(mut doc) = self.document_in(rtxn, &hit.id)? else {
                continue;
            };
            if let Some(obj) = doc.as_object_mut() {
                obj.insert("_score".to_string(), serde_json::json!(hit.score));
            }
            out.push(doc);
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn search_text_index_in(
        &self,
        rtxn: &RoTxn<'_>,
        name: &str,
        query: &str,
        filter: &Value,
        skip: usize,
        limit: Option<usize>,
        mode: TextQueryMode,
    ) -> Result<Vec<Value>> {
        if !self.ttl_filter_active(rtxn)? {
            return self
                .search_text_index_in_unfiltered(rtxn, name, query, filter, skip, limit, mode);
        }
        let target = skip.saturating_add(limit.unwrap_or(10).min(1000));
        if target == 0 {
            return Ok(Vec::new());
        }
        let mut fetch = target.saturating_mul(2).clamp(target, 1000);
        loop {
            let docs = self.search_text_index_in_unfiltered(
                rtxn,
                name,
                query,
                filter,
                0,
                Some(fetch),
                mode,
            )?;
            let filtered = self.retain_visible_docs(rtxn, docs)?;
            let exhausted =
                fetch >= 1000 || (fetch > target && filtered.len() < target && fetch == 1000);
            if filtered.len() >= target || fetch >= 1000 {
                return Ok(filtered
                    .into_iter()
                    .skip(skip)
                    .take(limit.unwrap_or(10))
                    .collect());
            }
            if exhausted {
                return Ok(filtered
                    .into_iter()
                    .skip(skip)
                    .take(limit.unwrap_or(10))
                    .collect());
            }
            fetch = (fetch * 2).min(1000);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn search_text_index_in_unfiltered(
        &self,
        rtxn: &RoTxn<'_>,
        name: &str,
        query: &str,
        filter: &Value,
        skip: usize,
        limit: Option<usize>,
        _mode: TextQueryMode,
    ) -> Result<Vec<Value>> {
        crate::query::validate::validate_filter(filter)?;
        let Some(def) = self.search_index_def(rtxn, SearchKind::Text, name)? else {
            return Err(Error::Query(format!("no text index `{name}`")));
        };
        let parsed = search::parse_text_query(query);
        if parsed.positive_terms.is_empty()
            && parsed.phrases.is_empty()
            && parsed.negative_terms.is_empty()
            && parsed.negative_phrases.is_empty()
        {
            return Ok(Vec::new());
        }

        let empty_filter = filter.as_object().is_none_or(|object| object.is_empty());
        if def.fields.len() != 1 {
            return Err(Error::Query(
                "segment text indexes support exactly one field".into(),
            ));
        }
        let allowed = if empty_filter {
            None
        } else {
            self.segment_filter_candidates(rtxn, filter)?
        };
        let needs_filter = !empty_filter && allowed.is_none();
        let needs_negative =
            !parsed.negative_terms.is_empty() || !parsed.negative_phrases.is_empty();
        let mut candidate_terms = parsed.positive_terms.clone();
        for phrase in &parsed.phrases {
            candidate_terms.extend(phrase.iter().cloned());
        }
        candidate_terms.sort();
        candidate_terms.dedup();
        if candidate_terms.is_empty() {
            return Ok(Vec::new());
        }
        let candidate_query = candidate_terms.join(" ");
        let verify = |id: &str| -> std::io::Result<Option<f64>> {
            let Some(doc) = self.document_in(rtxn, id).map_err(std::io::Error::other)? else {
                return Ok(None);
            };
            if needs_filter && !matches(&doc, filter) {
                return Ok(None);
            }
            if needs_negative {
                let tokens: Vec<String> = def
                    .fields
                    .iter()
                    .filter_map(|field| {
                        index::field_value(&doc, &field.path).and_then(search::field_text)
                    })
                    .flat_map(|text| search::tokenize(&text))
                    .collect();
                if parsed
                    .negative_terms
                    .iter()
                    .any(|term| tokens.iter().any(|token| token == term))
                {
                    return Ok(None);
                }
                if parsed
                    .negative_phrases
                    .iter()
                    .any(|phrase| search::contains_phrase(&tokens, phrase))
                {
                    return Ok(None);
                }
            }
            Ok(Some(0.0))
        };
        let verify: Option<&DocVerifier<'_>> = (needs_filter || needs_negative).then_some(&verify);
        self.search_segment_text_index_in(
            rtxn,
            &def,
            &candidate_query,
            skip,
            limit,
            allowed.as_ref(),
            verify,
            &parsed.phrases,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn search_segment_fuzzy_in(
        &self,
        rtxn: &RoTxn<'_>,
        def: &SearchIndexDef,
        query: &str,
        max_distance: usize,
        limit: Option<usize>,
        allowed: Option<&HashSet<String>>,
        verify: Option<&DocVerifier<'_>>,
    ) -> Result<Vec<Value>> {
        let limit = limit.unwrap_or(10).min(1000);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let Some(manifest) = self.fuzzy_segment_manifest_in(rtxn, &def.name)? else {
            return Ok(Vec::new());
        };
        let root = self.segment_root()?;
        let prefix = self.fuzzy_segment_delta_prefix(&def.name);
        let delta_count = self.fuzzy_segment_delta_count(rtxn, &def.name) as usize;
        let want = limit
            .saturating_add(manifest.tombstones.len())
            .saturating_add(delta_count)
            .max(limit);
        let mut merged: HashMap<String, usize> = HashMap::new();
        for (idx, (segment, values)) in manifest.segments.iter().zip(&manifest.values).enumerate() {
            let reader = FuzzySegmentReader::open(root.join(segment), root.join(values))?;
            for hit in reader.search_verified(query, max_distance, want, allowed, verify)? {
                let hidden = manifest
                    .tombstones
                    .get(&hit.id)
                    .is_some_and(|deleted_at| idx < *deleted_at);
                if !hidden {
                    merged
                        .entry(hit.id)
                        .and_modify(|distance| *distance = (*distance).min(hit.distance))
                        .or_insert(hit.distance);
                }
            }
        }
        let normalized = search::normalize(query);
        for item in self.meta.prefix_iter(rtxn, prefix.as_bytes())? {
            let (key, value) = item?;
            let id = std::str::from_utf8(&key[prefix.len()..])
                .map_err(|e| Error::Query(format!("invalid fuzzy delta id: {e}")))?;
            if allowed.is_some_and(|set| !set.contains(id)) {
                continue;
            }
            if let Some(predicate) = verify {
                if predicate(id)?.is_none() {
                    continue;
                }
            }
            let text = std::str::from_utf8(value)
                .map_err(|e| Error::Query(format!("invalid fuzzy delta text: {e}")))?;
            if let Some(distance) = search::fuzzy_distance(text, &normalized, max_distance) {
                merged
                    .entry(id.to_string())
                    .and_modify(|current| *current = (*current).min(distance))
                    .or_insert(distance);
            }
        }
        let mut hits: Vec<(usize, String)> = merged
            .into_iter()
            .map(|(id, distance)| (distance, id))
            .collect();
        hits.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        hits.truncate(limit);
        let mut out = Vec::with_capacity(hits.len());
        for (distance, id) in hits {
            let Some(mut doc) = self.document_in(rtxn, &id)? else {
                continue;
            };
            if let Some(obj) = doc.as_object_mut() {
                obj.insert("_distance".to_string(), serde_json::json!(distance));
            }
            out.push(doc);
        }
        Ok(out)
    }

    /// Approximate search (trigrams + Levenshtein). Results include `_distance`.
    pub fn fuzzy_search(
        &self,
        field: &str,
        query: &str,
        max_distance: usize,
        filter: &Value,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        let rtxn = self.read_txn()?;
        let out = self.fuzzy_search_in(&rtxn, field, query, max_distance, filter, limit)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn fuzzy_search_in(
        &self,
        rtxn: &RoTxn<'_>,
        field: &str,
        query: &str,
        max_distance: usize,
        filter: &Value,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        if !self.ttl_filter_active(rtxn)? {
            return self.fuzzy_search_in_unfiltered(
                rtxn,
                field,
                query,
                max_distance,
                filter,
                limit,
            );
        }
        let max = limit.unwrap_or(10).min(1000);
        if max == 0 {
            return Ok(Vec::new());
        }
        let mut fetch = max.saturating_mul(2).clamp(max, 1000);
        loop {
            let docs = self.fuzzy_search_in_unfiltered(
                rtxn,
                field,
                query,
                max_distance,
                filter,
                Some(fetch),
            )?;
            let filtered = self.retain_visible_docs(rtxn, docs)?;
            if filtered.len() >= max || fetch >= 1000 {
                return Ok(filtered.into_iter().take(max).collect());
            }
            fetch = (fetch * 2).min(1000);
        }
    }

    fn fuzzy_search_in_unfiltered(
        &self,
        rtxn: &RoTxn<'_>,
        field: &str,
        query: &str,
        max_distance: usize,
        filter: &Value,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        crate::query::validate::validate_filter(filter)?;
        let Some(def) = self.search_index_def(rtxn, SearchKind::Fuzzy, field)? else {
            return Err(Error::Query(format!("no fuzzy index on field `{field}`")));
        };
        let empty_filter = filter.as_object().is_none_or(|object| object.is_empty());
        let allowed = if empty_filter {
            None
        } else {
            self.segment_filter_candidates(rtxn, filter)?
        };
        let needs_filter = !empty_filter && allowed.is_none();
        let verify = |id: &str| -> std::io::Result<Option<f64>> {
            let doc = self.document_in(rtxn, id).map_err(std::io::Error::other)?;
            let Some(doc) = doc else {
                return Ok(None);
            };
            Ok(matches(&doc, filter).then_some(0.0))
        };
        let verify: Option<&DocVerifier<'_>> = needs_filter.then_some(&verify);
        self.search_segment_fuzzy_in(
            rtxn,
            &def,
            query,
            max_distance,
            limit,
            allowed.as_ref(),
            verify,
        )
    }

    pub fn insert_one(&self, doc: Value) -> Result<String> {
        let mut wtxn = self.write_txn()?;
        let id = self.insert_one_txn(&mut wtxn, doc)?;
        wtxn.commit()?;
        Ok(id)
    }

    /// Inserts many documents in one writer transaction. Time-series
    /// collections resolve each open bucket once and flush pointers at the end.
    fn normalize_columnar_batch(
        docs: &[Value],
        default_seconds: Option<u64>,
        ttl_field: Option<(&str, u64)>,
    ) -> Result<Vec<(Value, Option<u64>)>> {
        let normalize = |doc: &Value| -> Result<(Value, Option<u64>)> {
            let mut doc = doc.clone();
            let ttl_at = ttl::take_insert_ttl_with_field(&mut doc, default_seconds, ttl_field)?;
            date::normalize_document(&mut doc)?;
            numeric::normalize_document(&mut doc)?;
            bson_types::normalize_document(&mut doc)?;
            Ok((doc, ttl_at))
        };
        let workers = scan_workers().min(docs.len()).max(1);
        if workers <= 1 || docs.len() < 64 {
            return docs.iter().map(normalize).collect();
        }
        let chunk_size = docs.len().div_ceil(workers);
        let results = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for chunk in docs.chunks(chunk_size) {
                handles.push(scope.spawn(move || -> Result<Vec<(Value, Option<u64>)>> {
                    chunk.iter().map(normalize).collect()
                }));
            }
            handles
                .into_iter()
                .map(|handle| {
                    handle.join().unwrap_or_else(|_| {
                        Err(Error::Query("columnar normalize worker panicked".into()))
                    })
                })
                .collect::<Vec<_>>()
        });
        let mut out = Vec::with_capacity(docs.len());
        for result in results {
            out.extend(result?);
        }
        Ok(out)
    }

    pub fn insert_many_txn(&self, wtxn: &mut RwTxn<'_>, docs: &[Value]) -> Result<Vec<String>> {
        if self.profile.is_columnar() {
            let settings = self.columnar_settings_in(&*wtxn)?.unwrap_or_default();
            let ttl_field = self.ttl_field.as_deref().zip(self.ttl_default_seconds);
            let normalized =
                Self::normalize_columnar_batch(docs, self.ttl_default_seconds, ttl_field)?;
            let mut prepared = Vec::with_capacity(normalized.len());
            let mut ttls = Vec::with_capacity(normalized.len());
            for (mut doc, ttl_at) in normalized {
                self.validate_graph_edge_txn(&*wtxn, &doc)?;
                if doc.get("_id").is_none() {
                    doc.as_object_mut()
                        .ok_or_else(|| Error::Query("document must be an object".into()))?
                        .insert(
                            "_id".into(),
                            Value::String(uuid::Uuid::new_v4().to_string()),
                        );
                }
                prepared.push(doc);
                ttls.push(ttl_at);
            }
            // Validate the whole batch before writing any row. The daemon can
            // commit the shared transaction after this job reports an error,
            // so insert_batch must not be the first place a duplicate shows up.
            let mut seen_ids = HashSet::with_capacity(prepared.len());
            for doc in &prepared {
                let id = doc
                    .get("_id")
                    .and_then(id::key_from_value)
                    .ok_or(Error::InvalidId)?;
                if !seen_ids.insert(id.clone()) {
                    return Err(Error::DuplicateKey(id));
                }
            }
            self.validate_unique_batch(&*wtxn, &prepared)?;
            let inserted =
                crate::columnar::insert_batch(wtxn, self.env, &self.name, &settings, prepared)?;
            let index_defs = self.index_defs(&*wtxn)?;
            let has_unique = index_defs.iter().any(|def| def.unique);
            let has_indexes = !index_defs.is_empty()
                || !self.text_defs(&*wtxn)?.is_empty()
                || !self.fuzzy_defs(&*wtxn)?.is_empty()
                || !self.vector_defs(&*wtxn)?.is_empty();
            let has_graph = self.edge_graph_def_in(&*wtxn)?.is_some();
            let mut ids = Vec::with_capacity(inserted.len());
            for ((id, stored), ttl_at) in inserted.into_iter().zip(ttls) {
                if has_unique {
                    self.validate_unique(&*wtxn, &stored, &id)?;
                }
                if has_indexes {
                    self.apply_all_index_entries(wtxn, None, Some(&stored), &id)?;
                }
                if has_graph {
                    self.apply_graph_entries(wtxn, None, Some(&stored))?;
                }
                if ttl_at.is_some() {
                    self.set_ttl(wtxn, &id, ttl_at)?;
                }
                self.record_history(wtxn, &id, "insert", Some(stored))?;
                ids.push(id);
            }
            return Ok(ids);
        }
        if self.profile.is_timeseries() {
            let settings = self.timeseries_settings_in(&*wtxn)?.unwrap_or_default();
            let mut normalized = Vec::with_capacity(docs.len());
            for doc in docs {
                let mut doc = doc.clone();
                Self::normalize_for_index(&mut doc)?;
                normalized.push(doc);
            }
            self.validate_unique_batch(&*wtxn, &normalized)?;
            let ids = crate::timeseries::insert_many(wtxn, self.env, &self.name, &settings, docs)?;
            for (doc, id) in docs.iter().zip(ids.iter()) {
                let mut doc = doc.clone();
                if !doc
                    .as_object()
                    .is_some_and(|object| object.contains_key("_id"))
                {
                    if let Some(object) = doc.as_object_mut() {
                        object.insert("_id".into(), Value::String(id.clone()));
                    }
                }
                self.validate_unique(&*wtxn, &doc, id)?;
                self.apply_all_index_entries(wtxn, None, Some(&doc), id)?;
            }
            return Ok(ids);
        }
        let mut ids = Vec::with_capacity(docs.len());
        for doc in docs {
            ids.push(self.insert_one_txn(wtxn, doc.clone())?);
        }
        Ok(ids)
    }

    /// Inserts a document in the given transaction. Assigns `_id` (UUID v4) if missing.
    pub fn insert_one_txn(&self, wtxn: &mut RwTxn<'_>, mut doc: Value) -> Result<String> {
        if self.profile.is_columnar() {
            let settings = self.columnar_settings_in(&*wtxn)?.unwrap_or_default();
            let ttl_at = ttl::take_insert_ttl_with_field(
                &mut doc,
                self.ttl_default_seconds,
                self.ttl_field_pair(),
            )?;
            date::normalize_document(&mut doc)?;
            numeric::normalize_document(&mut doc)?;
            bson_types::normalize_document(&mut doc)?;
            self.validate_graph_edge_txn(&*wtxn, &doc)?;
            let id = match doc.get("_id") {
                Some(value) => id::key_from_value(value).ok_or(Error::InvalidId)?,
                None => {
                    let id = uuid::Uuid::new_v4().to_string();
                    doc.as_object_mut()
                        .ok_or_else(|| Error::Query("document must be an object".into()))?
                        .insert("_id".into(), Value::String(id.clone()));
                    id
                }
            };
            // `columnar::insert` checks duplicate `_id`s before writing. Validate
            // uniques before the row is written: the daemon may
            // commit the shared transaction even when this job reports an
            // error, so a late failure must not leave a partial document.
            self.validate_unique(&*wtxn, &doc, &id)?;
            let id = crate::columnar::insert(wtxn, self.env, &self.name, &settings, doc)?;
            if let Some(stored) = crate::columnar::find_by_id(&*wtxn, self.env, &self.name, &id)? {
                self.apply_all_index_entries(wtxn, None, Some(&stored), &id)?;
                crate::columnar::failpoint("insert_after_index")?;
                self.apply_graph_entries(wtxn, None, Some(&stored))?;
                self.set_ttl(wtxn, &id, ttl_at)?;
                crate::columnar::failpoint("insert_after_ttl")?;
                self.record_history(wtxn, &id, "insert", Some(stored))?;
                crate::columnar::failpoint("insert_after_history")?;
            }
            return Ok(id);
        }
        if self.profile.is_timeseries() {
            let settings = self.timeseries_settings_in(&*wtxn)?.unwrap_or_default();
            let mut check = doc.clone();
            Self::normalize_for_index(&mut check)?;
            let check_id = check
                .get("_id")
                .and_then(id::key_from_value)
                .unwrap_or_default();
            self.validate_unique(&*wtxn, &check, &check_id)?;
            let id = crate::timeseries::insert(wtxn, self.env, &self.name, &settings, doc)?;
            if let Some(doc) =
                crate::timeseries::find_by_id(&*wtxn, self.env, &self.name, &settings, &id)?
            {
                self.validate_unique(&*wtxn, &doc, &id)?;
                self.apply_index_entries(wtxn, None, Some(&doc), &id)?;
                self.apply_text_entries(wtxn, None, Some(&doc), &id)?;
                self.apply_fuzzy_entries(wtxn, None, Some(&doc), &id)?;
                self.apply_vector_entries(wtxn, None, Some(&doc), &id)?;
            }
            return Ok(id);
        }
        self.ensure_standard()?;
        let ttl_at = ttl::take_insert_ttl_with_field(
            &mut doc,
            self.ttl_default_seconds,
            self.ttl_field_pair(),
        )?;
        date::normalize_document(&mut doc)?;
        numeric::normalize_document(&mut doc)?;
        bson_types::normalize_document(&mut doc)?;
        let obj = doc
            .as_object_mut()
            .ok_or_else(|| Error::Query("document must be an object".into()))?;

        let id = match obj.get("_id") {
            Some(value) => id::key_from_value(value).ok_or(Error::InvalidId)?,
            None => {
                let id = uuid::Uuid::new_v4().to_string();
                obj.insert("_id".to_string(), Value::String(id.clone()));
                id
            }
        };

        if self.db.get(&*wtxn, id.as_bytes())?.is_some() {
            return Err(Error::DuplicateKey(id));
        }
        self.validate_unique(&*wtxn, &doc, &id)?;

        self.validate_graph_edge_txn(&*wtxn, &doc)?;
        let bytes = serde_json::to_vec(&doc)?;
        if bytes.len() > MAX_DOCUMENT_BYTES {
            return Err(Error::DocumentTooLarge(bytes.len(), MAX_DOCUMENT_BYTES));
        }
        self.db.put(&mut *wtxn, id.as_bytes(), &bytes)?;
        self.apply_index_entries(wtxn, None, Some(&doc), &id)?;
        self.apply_graph_entries(wtxn, None, Some(&doc))?;
        self.apply_text_entries(wtxn, None, Some(&doc), &id)?;
        self.apply_fuzzy_entries(wtxn, None, Some(&doc), &id)?;
        self.apply_vector_entries(wtxn, None, Some(&doc), &id)?;
        self.set_ttl(wtxn, &id, ttl_at)?;
        self.record_history(wtxn, &id, "insert", Some(doc))?;
        Ok(id)
    }

    /// Reads one document by its internal LMDB key.
    ///
    /// This is used by the MongoDB wire layer to translate an internal
    /// upserted key back into the original BSON `_id` value.
    pub fn find_by_key(&self, key: &str) -> Result<Option<Value>> {
        let rtxn = self.read_txn()?;
        if self.profile.is_columnar() {
            let out = crate::columnar::find_by_id(&rtxn, self.env, &self.name, key)?;
            rtxn.commit()?;
            return Ok(out);
        }
        if self.profile.is_timeseries() {
            let settings = self.timeseries_settings_in(&rtxn)?.unwrap_or_default();
            let out = crate::timeseries::find_by_id(&rtxn, self.env, &self.name, &settings, key)?;
            rtxn.commit()?;
            return Ok(out);
        }
        let out = self
            .db
            .get(&rtxn, key.as_bytes())?
            .map(parse_json)
            .transpose()?;
        rtxn.commit()?;
        Ok(out)
    }

    /// Applies a replicated document change without generating history or
    /// automatic events. Used only by the replica apply path.
    pub fn apply_replication_change_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        op: &str,
        id: &str,
        after: Option<&Value>,
        ttl_at: Option<u64>,
    ) -> Result<()> {
        if self.profile.is_columnar() {
            let settings = self.columnar_settings_in(&*wtxn)?.unwrap_or_default();
            match op {
                "insert" | "update" => {
                    let after = after.ok_or_else(|| {
                        Error::Query("replication change is missing `after`".into())
                    })?;
                    if after.get("_id").and_then(id::key_from_value).as_deref() != Some(id) {
                        return Err(Error::Query(
                            "replication document `_id` does not match change id".into(),
                        ));
                    }
                    let before = crate::columnar::find_by_id(&*wtxn, self.env, &self.name, id)?;
                    match op {
                        "insert" => {
                            if before.is_some() {
                                return Err(Error::DuplicateKey(id.to_string()));
                            }
                            let inserted = crate::columnar::insert(
                                wtxn,
                                self.env,
                                &self.name,
                                &settings,
                                after.clone(),
                            )?;
                            if inserted != id {
                                return Err(Error::Query(
                                    "replicated insert returned a different id".into(),
                                ));
                            }
                        }
                        "update" => {
                            if before.is_none() {
                                return Err(Error::Query(format!(
                                    "replication update target `{id}` does not exist"
                                )));
                            }
                            let object = after.as_object().ok_or_else(|| {
                                Error::Query("replicated document must be an object".into())
                            })?;
                            crate::columnar::replace_by_id(wtxn, self.env, &self.name, id, object)?;
                        }
                        _ => unreachable!(),
                    }
                    let after = crate::columnar::find_by_id(&*wtxn, self.env, &self.name, id)?;
                    if let Some(doc) = &after {
                        self.validate_unique(&*wtxn, doc, id)?;
                    }
                    self.apply_all_index_entries(wtxn, before.as_ref(), after.as_ref(), id)?;
                    self.apply_graph_entries(wtxn, before.as_ref(), after.as_ref())?;
                    self.set_ttl(wtxn, id, ttl_at)?;
                    self.record_history(wtxn, id, op, after)?;
                    return Ok(());
                }
                "delete" => {
                    let before = crate::columnar::find_by_id(&*wtxn, self.env, &self.name, id)?;
                    if let Some(doc) = &before {
                        let id_value = doc
                            .get("_id")
                            .cloned()
                            .unwrap_or_else(|| serde_json::Value::String(id.to_string()));
                        crate::columnar::delete(
                            wtxn,
                            self.env,
                            &self.name,
                            &serde_json::json!({"_id": id_value}),
                            false,
                        )?;
                        self.apply_all_index_entries(wtxn, Some(doc), None, id)?;
                        self.apply_graph_entries(wtxn, Some(doc), None)?;
                    }
                    self.set_ttl(wtxn, id, None)?;
                    if before.is_some() {
                        self.record_history(wtxn, id, "delete", None)?;
                    }
                    return Ok(());
                }
                other => {
                    return Err(Error::Query(format!(
                        "unknown replication operation `{other}`"
                    )))
                }
            }
        }
        if self.profile.is_timeseries() {
            // Time-series rows do not live in `coll:<name>`; appending them to
            // the bucket store keeps replicas byte-compatible with the primary
            // (same settings, same insert order, same synthetic ids).
            let settings = self.timeseries_settings_in(&*wtxn)?.unwrap_or_default();
            match op {
                "insert" => {
                    let after = after.ok_or_else(|| {
                        Error::Query("replication change is missing `after`".into())
                    })?;
                    crate::timeseries::insert_replicated(
                        wtxn, self.env, &self.name, &settings, id, after,
                    )?;
                    if let Some(doc) =
                        crate::timeseries::find_by_id(&*wtxn, self.env, &self.name, &settings, id)?
                    {
                        self.validate_unique(&*wtxn, &doc, id)?;
                        self.apply_index_entries(wtxn, None, Some(&doc), id)?;
                        self.apply_text_entries(wtxn, None, Some(&doc), id)?;
                        self.apply_fuzzy_entries(wtxn, None, Some(&doc), id)?;
                        self.apply_vector_entries(wtxn, None, Some(&doc), id)?;
                    }
                    return Ok(());
                }
                "update" => {
                    let after = after.ok_or_else(|| {
                        Error::Query("replication change is missing `after`".into())
                    })?;
                    let before =
                        crate::timeseries::find_by_id(&*wtxn, self.env, &self.name, &settings, id)?;
                    crate::timeseries::replace_by_id(
                        wtxn, self.env, &self.name, &settings, id, after,
                    )?;
                    let after =
                        crate::timeseries::find_by_id(&*wtxn, self.env, &self.name, &settings, id)?;
                    if let Some(doc) = &after {
                        self.validate_unique(&*wtxn, doc, id)?;
                    }
                    self.apply_index_entries(wtxn, before.as_ref(), after.as_ref(), id)?;
                    self.apply_text_entries(wtxn, before.as_ref(), after.as_ref(), id)?;
                    self.apply_fuzzy_entries(wtxn, before.as_ref(), after.as_ref(), id)?;
                    self.apply_vector_entries(wtxn, before.as_ref(), after.as_ref(), id)?;
                    return Ok(());
                }
                "delete" => {
                    let before =
                        crate::timeseries::find_by_id(&*wtxn, self.env, &self.name, &settings, id)?;
                    crate::timeseries::delete_by_id(wtxn, self.env, &self.name, &settings, id)?;
                    if let Some(doc) = &before {
                        self.apply_index_entries(wtxn, Some(doc), None, id)?;
                        self.apply_text_entries(wtxn, Some(doc), None, id)?;
                        self.apply_fuzzy_entries(wtxn, Some(doc), None, id)?;
                        self.apply_vector_entries(wtxn, Some(doc), None, id)?;
                    }
                    return Ok(());
                }
                other => {
                    return Err(Error::Query(format!(
                        "unknown replication operation `{other}`"
                    )))
                }
            }
        }
        self.ensure_standard()?;
        let existing = self.db.get(&*wtxn, id.as_bytes())?.map(|raw| raw.to_vec());
        let old = existing.as_deref().map(parse_json).transpose()?;

        match op {
            "insert" | "update" => {
                let after = after
                    .ok_or_else(|| Error::Query("replication change is missing `after`".into()))?;
                if after.get("_id").and_then(id::key_from_value).as_deref() != Some(id) {
                    return Err(Error::Query(
                        "replication document `_id` does not match change id".into(),
                    ));
                }
                if op == "insert" && old.is_some() {
                    return Err(Error::DuplicateKey(id.to_string()));
                }
                if op == "update" && old.is_none() {
                    return Err(Error::Query(format!(
                        "replication update target `{id}` does not exist"
                    )));
                }
                if let Some(old) = &old {
                    self.apply_index_entries(wtxn, Some(old), None, id)?;
                    self.apply_graph_entries(wtxn, Some(old), None)?;
                    self.apply_text_entries(wtxn, Some(old), None, id)?;
                    self.apply_fuzzy_entries(wtxn, Some(old), None, id)?;
                    self.apply_vector_entries(wtxn, Some(old), None, id)?;
                    self.set_ttl(wtxn, id, ttl_at)?;
                }
                self.validate_unique(&*wtxn, after, id)?;
                let bytes = serde_json::to_vec(after)?;
                if bytes.len() > MAX_DOCUMENT_BYTES {
                    return Err(Error::DocumentTooLarge(bytes.len(), MAX_DOCUMENT_BYTES));
                }
                self.db.put(&mut *wtxn, id.as_bytes(), &bytes)?;
                self.apply_index_entries(wtxn, None, Some(after), id)?;
                self.apply_graph_entries(wtxn, None, Some(after))?;
                self.apply_text_entries(wtxn, None, Some(after), id)?;
                self.apply_fuzzy_entries(wtxn, None, Some(after), id)?;
                self.apply_vector_entries(wtxn, None, Some(after), id)?;
                self.set_ttl(wtxn, id, ttl_at)?;
                self.record_history(wtxn, id, op, Some(after.clone()))?;
            }
            "delete" => {
                if let Some(old) = &old {
                    self.db.delete(&mut *wtxn, id.as_bytes())?;
                    self.apply_index_entries(wtxn, Some(old), None, id)?;
                    self.apply_graph_entries(wtxn, Some(old), None)?;
                    self.apply_text_entries(wtxn, Some(old), None, id)?;
                    self.apply_fuzzy_entries(wtxn, Some(old), None, id)?;
                    self.apply_vector_entries(wtxn, Some(old), None, id)?;
                    self.set_ttl(wtxn, id, None)?;
                    self.record_history(wtxn, id, "delete", None)?;
                }
            }
            other => {
                return Err(Error::Query(format!(
                    "unknown replication operation `{other}`"
                )))
            }
        }
        Ok(())
    }

    /// Update with optional upsert; returns the full operation result.
    pub fn update_opts_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        filter: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
    ) -> Result<UpdateOutcome> {
        self.update_opts_txn_filtered(wtxn, filter, update, multi, upsert, &[])
    }

    pub fn update_opts_txn_filtered(
        &self,
        wtxn: &mut RwTxn<'_>,
        filter: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
        array_filters: &[Value],
    ) -> Result<UpdateOutcome> {
        self.update_opts_txn_filtered_with_id(
            wtxn,
            filter,
            update,
            multi,
            upsert,
            array_filters,
            None,
        )
    }

    /// Like [`Self::update_opts_txn_filtered`], but uses `default_id` when an
    /// upsert would otherwise let the core generate a UUID `_id`. The MongoDB
    /// wire frontend passes an ObjectId here so driver-visible upsert IDs match
    /// MongoDB.
    #[allow(clippy::too_many_arguments)]
    pub fn update_opts_txn_filtered_with_id(
        &self,
        wtxn: &mut RwTxn<'_>,
        filter: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
        array_filters: &[Value],
        default_id: Option<&Value>,
    ) -> Result<UpdateOutcome> {
        if self.profile.is_columnar() {
            return self.update_columnar_txn(
                wtxn,
                filter,
                update,
                multi,
                upsert,
                array_filters,
                default_id,
            );
        }
        if self.profile.is_timeseries() {
            let settings = self.timeseries_settings_in(&*wtxn)?.unwrap_or_default();
            let direct_id = (!multi)
                .then(|| crate::timeseries::exact_id_filter(filter))
                .flatten();
            let candidate_ids = if direct_id.is_some() {
                None
            } else {
                self.timeseries_index_ids(&*wtxn, filter, None)?
            };
            let before = if let Some(id) = direct_id {
                crate::timeseries::find_by_id(&*wtxn, self.env, &self.name, &settings, id)?
                    .into_iter()
                    .collect()
            } else {
                let query = FindQuery {
                    filter: filter.clone(),
                    projection: None,
                    sort: Vec::new(),
                    skip: 0,
                    limit: if multi { None } else { Some(1) },
                    comment: None,
                    max_time_ms: None,
                    hint: None,
                };
                if let Some(ids) = &candidate_ids {
                    crate::timeseries::find_with_ids(
                        &*wtxn,
                        self.env,
                        &self.name,
                        &settings,
                        &query,
                        Some(ids),
                    )?
                } else {
                    crate::timeseries::find(&*wtxn, self.env, &self.name, &settings, &query)?
                }
            };
            let mut checks = Vec::with_capacity(before.len());
            for old in &before {
                let mut check = old.clone();
                apply_with_options(&mut check, update, filter, array_filters)?;
                Self::normalize_for_index(&mut check)?;
                if let Some(id) = old.get("_id").and_then(id::key_from_value) {
                    checks.push((id, check));
                }
            }
            self.validate_unique_updates(&*wtxn, &checks)?;
            if before.is_empty() && upsert {
                let mut check = seed_from_filter(filter);
                apply_upsert_with_options(&mut check, update, filter, array_filters)?;
                Self::normalize_for_index(&mut check)?;
                let check_id = check
                    .get("_id")
                    .and_then(id::key_from_value)
                    .or_else(|| default_id.and_then(id::key_from_value))
                    .unwrap_or_default();
                self.validate_unique(&*wtxn, &check, &check_id)?;
            }
            let report = if let Some(ids) = &candidate_ids {
                crate::timeseries::update_with_ids(
                    wtxn,
                    self.env,
                    &self.name,
                    &settings,
                    filter,
                    update,
                    multi,
                    array_filters,
                    upsert,
                    ids,
                )?
            } else {
                crate::timeseries::update(
                    wtxn,
                    self.env,
                    &self.name,
                    &settings,
                    filter,
                    update,
                    multi,
                    array_filters,
                    upsert,
                )?
            };
            for old in &before {
                let Some(old_id) = old.get("_id").and_then(Value::as_str) else {
                    continue;
                };
                if let Some((_, new_id)) = report.moved.iter().find(|(old, _)| old == old_id) {
                    self.apply_all_index_entries(wtxn, Some(old), None, old_id)?;
                    if let Some(new_doc) = crate::timeseries::find_by_id(
                        &*wtxn, self.env, &self.name, &settings, new_id,
                    )? {
                        self.apply_all_index_entries(wtxn, None, Some(&new_doc), new_id)?;
                    }
                } else if let Some(new_doc) =
                    crate::timeseries::find_by_id(&*wtxn, self.env, &self.name, &settings, old_id)?
                {
                    self.apply_all_index_entries(wtxn, Some(old), Some(&new_doc), old_id)?;
                }
            }
            if let Some(id) = &report.upserted_id {
                if let Some(new_doc) =
                    crate::timeseries::find_by_id(&*wtxn, self.env, &self.name, &settings, id)?
                {
                    self.apply_all_index_entries(wtxn, None, Some(&new_doc), id)?;
                }
            }
            return Ok(UpdateOutcome {
                matched: report.matched,
                modified: report.modified,
                upserted_id: report.upserted_id,
                moved: report.moved,
            });
        }
        self.ensure_standard()?;
        let keys = self.matching_keys(&*wtxn, filter, multi)?;
        if keys.is_empty() {
            if !upsert {
                return Ok(UpdateOutcome {
                    matched: 0,
                    modified: 0,
                    upserted_id: None,
                    moved: Vec::new(),
                });
            }
            let mut doc = seed_from_filter(filter);
            let filter_id = doc.get("_id").cloned();
            apply_upsert_with_options(&mut doc, update, filter, array_filters)?;
            // `_id` from the filter is immutable; do not let an update lose it.
            if let Some(id) = filter_id {
                doc["_id"] = id;
            }
            if doc.get("_id").is_none() {
                if let Some(id) = default_id {
                    doc["_id"] = id.clone();
                }
            }
            let id = self.insert_one_txn(wtxn, doc)?;
            return Ok(UpdateOutcome {
                matched: 0,
                modified: 0,
                upserted_id: Some(id),
                moved: Vec::new(),
            });
        }

        let matched = keys.len();
        let mut modified = 0usize;
        for key in keys {
            let raw = self
                .db
                .get(&*wtxn, key.as_slice())?
                .ok_or_else(|| Error::Query("document disappeared".into()))?
                .to_vec();
            let old: Value = parse_json(&raw)?;
            self.write_update_one(wtxn, old, filter, update, array_filters)?;
            modified += 1;
        }
        Ok(UpdateOutcome {
            matched,
            modified,
            upserted_id: None,
            moved: Vec::new(),
        })
    }

    fn write_update_one(
        &self,
        wtxn: &mut RwTxn<'_>,
        old: Value,
        filter: &Value,
        update: &Value,
        array_filters: &[Value],
    ) -> Result<Value> {
        let id = old
            .get("_id")
            .and_then(id::key_from_value)
            .ok_or(Error::InvalidId)?;
        let old_ttl = self.ttl_at(&*wtxn, &id)?;
        let mut new = old.clone();
        apply_with_options(&mut new, update, filter, array_filters)?;
        let ttl_at = match ttl::take_update_ttl_with_field(&mut new, self.ttl_field_pair())? {
            TtlUpdate::Preserve => old_ttl,
            TtlUpdate::Clear => None,
            TtlUpdate::Set(at) => Some(at),
        };
        date::normalize_document(&mut new)?;
        numeric::normalize_document(&mut new)?;
        bson_types::normalize_document(&mut new)?;
        let new_id = new
            .get("_id")
            .and_then(id::key_from_value)
            .ok_or(Error::InvalidId)?;
        if new_id != id {
            return Err(Error::Query("update cannot change `_id`".into()));
        }

        self.validate_graph_edge_txn(&*wtxn, &new)?;
        self.validate_unique(&*wtxn, &new, &id)?;
        let bytes = serde_json::to_vec(&new)?;
        if bytes.len() > MAX_DOCUMENT_BYTES {
            return Err(Error::DocumentTooLarge(bytes.len(), MAX_DOCUMENT_BYTES));
        }
        self.db.put(&mut *wtxn, id.as_bytes(), &bytes)?;
        self.apply_index_entries(wtxn, Some(&old), Some(&new), &id)?;
        self.apply_graph_entries(wtxn, Some(&old), Some(&new))?;
        self.apply_text_entries(wtxn, Some(&old), Some(&new), &id)?;
        self.apply_fuzzy_entries(wtxn, Some(&old), Some(&new), &id)?;
        self.apply_vector_entries(wtxn, Some(&old), Some(&new), &id)?;
        self.set_ttl(wtxn, &id, ttl_at)?;
        self.record_history(wtxn, &id, "update", Some(new.clone()))?;
        Ok(new)
    }

    /// Legacy signature: returns only the number of modified documents.
    pub fn update_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        filter: &Value,
        update: &Value,
        multi: bool,
    ) -> Result<usize> {
        Ok(self
            .update_opts_txn(wtxn, filter, update, multi, false)?
            .modified)
    }

    pub fn update_opts(
        &self,
        filter: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
    ) -> Result<UpdateOutcome> {
        self.update_opts_filtered(filter, update, multi, upsert, &[])
    }

    pub fn update_opts_filtered(
        &self,
        filter: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
        array_filters: &[Value],
    ) -> Result<UpdateOutcome> {
        let mut wtxn = self.write_txn()?;
        let out =
            self.update_opts_txn_filtered(&mut wtxn, filter, update, multi, upsert, array_filters)?;
        wtxn.commit()?;
        Ok(out)
    }

    pub fn update(&self, filter: &Value, update: &Value, multi: bool) -> Result<usize> {
        Ok(self.update_opts(filter, update, multi, false)?.modified)
    }

    /// Deletes matching documents. Returns the number deleted.
    /// Atomically selects one document, applies an update or removes it, and
    /// returns the pre-image or post-image in the same write transaction.
    pub fn find_and_modify_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        opts: &FindAndModifyOptions<'_>,
    ) -> Result<FindAndModifyOutcome> {
        self.find_and_modify_txn_with_id(wtxn, opts, None)
    }

    /// Like [`Self::find_and_modify_txn`], but uses `default_id` when an upsert
    /// would otherwise let the core generate a UUID `_id`.
    pub fn find_and_modify_txn_with_id(
        &self,
        wtxn: &mut RwTxn<'_>,
        opts: &FindAndModifyOptions<'_>,
        default_id: Option<&Value>,
    ) -> Result<FindAndModifyOutcome> {
        if self.profile.is_columnar() {
            return self.find_and_modify_columnar(wtxn, opts, default_id);
        }
        if self.profile.is_timeseries() {
            return self.find_and_modify_timeseries(wtxn, opts);
        }
        self.ensure_standard()?;
        validate_find_and_modify(opts)?;
        let query = FindQuery {
            filter: opts.filter.clone(),
            projection: None,
            sort: opts.sort.to_vec(),
            skip: 0,
            limit: Some(1),
            comment: None,
            max_time_ms: None,
            hint: None,
        };
        let selected = self.find_in(&*wtxn, &query)?.into_iter().next();

        if opts.remove {
            let Some(old) = selected else {
                return Ok(empty_find_and_modify());
            };
            let id_value = old.get("_id").cloned().ok_or(Error::InvalidId)?;
            let removed = self.delete_txn(wtxn, &serde_json::json!({"_id": id_value}), false)?;
            return Ok(FindAndModifyOutcome {
                value: Some(apply_projection(old, opts.projection, Some(opts.filter))),
                matched: removed,
                modified: removed,
                upserted_id: None,
                new_id: None,
            });
        }

        let update = opts
            .update
            .ok_or_else(|| Error::Query("find_and_modify requires `update` or `remove`".into()))?;
        if let Some(old) = selected {
            let new =
                self.write_update_one(wtxn, old.clone(), opts.filter, update, opts.array_filters)?;
            return Ok(FindAndModifyOutcome {
                value: Some(apply_projection(
                    if opts.new { new } else { old },
                    opts.projection,
                    Some(opts.filter),
                )),
                matched: 1,
                modified: 1,
                upserted_id: None,
                new_id: None,
            });
        }

        if !opts.upsert {
            return Ok(empty_find_and_modify());
        }
        let mut doc = seed_from_filter(opts.filter);
        let filter_id = doc.get("_id").cloned();
        apply_upsert_with_options(&mut doc, update, opts.filter, opts.array_filters)?;
        if let Some(id) = filter_id {
            doc["_id"] = id;
        }
        if doc.get("_id").is_none() {
            if let Some(id) = default_id {
                doc["_id"] = id.clone();
            }
        }
        let id = self.insert_one_txn(wtxn, doc)?;
        let value = if opts.new {
            let raw = self
                .db
                .get(&*wtxn, id.as_bytes())?
                .ok_or_else(|| Error::Query("upserted document disappeared".into()))?;
            Some(apply_projection(
                parse_json(raw)?,
                opts.projection,
                Some(opts.filter),
            ))
        } else {
            None
        };
        Ok(FindAndModifyOutcome {
            value,
            matched: 0,
            modified: 0,
            upserted_id: Some(id),
            new_id: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn update_columnar_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        filter: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
        array_filters: &[Value],
        default_id: Option<&Value>,
    ) -> Result<UpdateOutcome> {
        let settings = self.columnar_settings_in(&*wtxn)?.unwrap_or_default();
        if self.index_defs(&*wtxn)?.iter().any(|def| def.unique) {
            let (preview, upsert) = crate::columnar::preview_update(
                &*wtxn,
                self.env,
                &self.name,
                filter,
                update,
                multi,
                array_filters,
                upsert,
                default_id,
            )?;
            let updates: Vec<(String, Value)> = preview
                .iter()
                .map(|(id, _old, new)| (id.clone(), Value::Object(new.clone())))
                .collect();
            self.validate_unique_updates(&*wtxn, &updates)?;
            if let Some(doc) = upsert {
                let doc = Value::Object(doc);
                let id = doc
                    .get("_id")
                    .and_then(id::key_from_value)
                    .unwrap_or_default();
                self.validate_unique(&*wtxn, &doc, &id)?;
            }
        }
        let (outcome, changed) = crate::columnar::update_with_changes(
            wtxn,
            self.env,
            &self.name,
            &settings,
            filter,
            update,
            multi,
            array_filters,
            upsert,
            default_id,
        )?;
        for (id, old, new) in changed {
            let old_ttl = self.ttl_at(&*wtxn, &id)?;
            let old = Value::Object(old);
            let mut new = Value::Object(new);
            let directive = ttl::take_update_ttl_with_field(&mut new, self.ttl_field_pair())?;
            let stripped = new
                .as_object_mut()
                .is_some_and(|object| object.remove("_ttl").is_some());
            if stripped {
                let object = new
                    .as_object()
                    .ok_or_else(|| Error::Query("updated document must be an object".into()))?;
                crate::columnar::replace_by_id(wtxn, self.env, &self.name, &id, object)?;
            }
            let ttl_at = match directive {
                ttl::TtlUpdate::Preserve => old_ttl,
                ttl::TtlUpdate::Clear => None,
                ttl::TtlUpdate::Set(at) => Some(at),
            };
            self.validate_unique(&*wtxn, &new, &id)?;
            self.apply_all_index_entries(wtxn, Some(&old), Some(&new), &id)?;
            crate::columnar::failpoint("update_after_index")?;
            self.apply_graph_entries(wtxn, Some(&old), Some(&new))?;
            self.set_ttl(wtxn, &id, ttl_at)?;
            crate::columnar::failpoint("update_after_ttl")?;
            self.record_history(wtxn, &id, "update", Some(new))?;
            crate::columnar::failpoint("update_after_history")?;
        }
        if let Some(id) = outcome.upserted_id.as_deref() {
            if let Some(mut new) = crate::columnar::find_by_id(&*wtxn, self.env, &self.name, id)? {
                let ttl_at = ttl::take_insert_ttl_with_field(
                    &mut new,
                    self.ttl_default_seconds,
                    self.ttl_field_pair(),
                )?;
                if let Some(object) = new.as_object() {
                    crate::columnar::replace_by_id(wtxn, self.env, &self.name, id, object)?;
                }
                self.set_ttl(wtxn, id, ttl_at)?;
                self.validate_unique(&*wtxn, &new, id)?;
                self.apply_all_index_entries(wtxn, None, Some(&new), id)?;
                self.apply_graph_entries(wtxn, None, Some(&new))?;
                self.record_history(wtxn, id, "insert", Some(new))?;
            }
        }
        Ok(outcome)
    }

    fn delete_columnar_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        filter: &Value,
        multi: bool,
    ) -> Result<usize> {
        let removed = crate::columnar::delete_with_docs(wtxn, self.env, &self.name, filter, multi)?;
        for (id, old) in &removed {
            let old = Value::Object(old.clone());
            self.apply_all_index_entries(wtxn, Some(&old), None, id)?;
            crate::columnar::failpoint("delete_after_index")?;
            self.apply_graph_entries(wtxn, Some(&old), None)?;
            self.set_ttl(wtxn, id, None)?;
            crate::columnar::failpoint("delete_after_ttl")?;
            self.record_history(wtxn, id, "delete", None)?;
            crate::columnar::failpoint("delete_after_history")?;
        }
        Ok(removed.len())
    }

    fn find_and_modify_columnar(
        &self,
        wtxn: &mut RwTxn<'_>,
        opts: &FindAndModifyOptions<'_>,
        default_id: Option<&Value>,
    ) -> Result<FindAndModifyOutcome> {
        validate_find_and_modify(opts)?;
        let query = FindQuery {
            filter: opts.filter.clone(),
            projection: None,
            sort: if opts.sort.is_empty() {
                vec![("_id".to_string(), 1)]
            } else {
                opts.sort.to_vec()
            },
            skip: 0,
            limit: Some(1),
            comment: None,
            max_time_ms: None,
            hint: None,
        };
        let selected = if let Some(id) = crate::columnar::exact_id_filter(opts.filter) {
            crate::columnar::find_by_id(&*wtxn, self.env, &self.name, &id)?
        } else {
            crate::columnar::find(&*wtxn, self.env, &self.name, &query)?
                .into_iter()
                .next()
        };
        if opts.remove {
            let Some(old) = selected else {
                return Ok(empty_find_and_modify());
            };
            let id_value = old.get("_id").cloned().ok_or(Error::InvalidId)?;
            let removed = self.delete_txn(wtxn, &serde_json::json!({"_id": id_value}), false)?;
            return Ok(FindAndModifyOutcome {
                value: Some(apply_projection(old, opts.projection, Some(opts.filter))),
                matched: removed,
                modified: removed,
                upserted_id: None,
                new_id: None,
            });
        }
        let update = opts
            .update
            .ok_or_else(|| Error::Query("find_and_modify requires `update` or `remove`".into()))?;
        if let Some(old) = selected {
            let id = id::key_from_document(&old).ok_or(Error::InvalidId)?;
            let id_value = old.get("_id").cloned().ok_or(Error::InvalidId)?;
            let outcome = self.update_opts_txn_filtered_with_id(
                wtxn,
                &serde_json::json!({"_id": id_value}),
                update,
                false,
                false,
                opts.array_filters,
                None,
            )?;
            let new = crate::columnar::find_by_id(&*wtxn, self.env, &self.name, &id)?
                .ok_or_else(|| Error::Query("updated document disappeared".into()))?;
            return Ok(FindAndModifyOutcome {
                value: Some(apply_projection(
                    if opts.new { new } else { old },
                    opts.projection,
                    Some(opts.filter),
                )),
                matched: outcome.matched,
                modified: outcome.modified,
                upserted_id: None,
                new_id: None,
            });
        }
        if !opts.upsert {
            return Ok(empty_find_and_modify());
        }
        let mut doc = seed_from_filter(opts.filter);
        let filter_id = doc.get("_id").cloned();
        apply_upsert_with_options(&mut doc, update, opts.filter, opts.array_filters)?;
        if let Some(id) = filter_id {
            doc["_id"] = id;
        }
        if doc.get("_id").is_none() {
            if let Some(id) = default_id {
                doc["_id"] = id.clone();
            }
        }
        let id = self.insert_one_txn(wtxn, doc)?;
        let value = if opts.new {
            crate::columnar::find_by_id(&*wtxn, self.env, &self.name, &id)?
                .map(|doc| apply_projection(doc, opts.projection, Some(opts.filter)))
        } else {
            None
        };
        Ok(FindAndModifyOutcome {
            value,
            matched: 0,
            modified: 0,
            upserted_id: Some(id),
            new_id: None,
        })
    }

    fn find_and_modify_timeseries(
        &self,
        wtxn: &mut RwTxn<'_>,
        opts: &FindAndModifyOptions<'_>,
    ) -> Result<FindAndModifyOutcome> {
        validate_find_and_modify(opts)?;
        let settings = self.timeseries_settings_in(&*wtxn)?.unwrap_or_default();
        let query = FindQuery {
            filter: opts.filter.clone(),
            projection: None,
            sort: opts.sort.to_vec(),
            skip: 0,
            limit: Some(1),
            comment: None,
            max_time_ms: None,
            hint: None,
        };
        let selected = if let Some(id) = crate::timeseries::exact_id_filter(opts.filter) {
            crate::timeseries::find_by_id(&*wtxn, self.env, &self.name, &settings, id)?
        } else {
            self.find_in(&*wtxn, &query)?.into_iter().next()
        };
        if opts.remove {
            let Some(old) = selected else {
                return Ok(empty_find_and_modify());
            };
            let id = old.get("_id").cloned().ok_or(Error::InvalidId)?;
            let removed = self.delete_txn(wtxn, &serde_json::json!({"_id": id}), false)?;
            return Ok(FindAndModifyOutcome {
                value: Some(apply_projection(old, opts.projection, Some(opts.filter))),
                matched: removed,
                modified: removed,
                upserted_id: None,
                new_id: None,
            });
        }
        let update = opts
            .update
            .ok_or_else(|| Error::Query("find_and_modify requires `update` or `remove`".into()))?;
        if let Some(old) = selected {
            let id = old
                .get("_id")
                .and_then(Value::as_str)
                .ok_or(Error::InvalidId)?
                .to_string();
            let mut check = old.clone();
            apply_with_options(&mut check, update, opts.filter, opts.array_filters)?;
            Self::normalize_for_index(&mut check)?;
            self.validate_unique(&*wtxn, &check, &id)?;
            let report = crate::timeseries::update(
                wtxn,
                self.env,
                &self.name,
                &settings,
                &serde_json::json!({"_id": id}),
                update,
                false,
                opts.array_filters,
                false,
            )?;
            let new_id = report
                .moved
                .last()
                .map(|(_, new_id)| new_id.clone())
                .unwrap_or_else(|| id.clone());
            let new =
                crate::timeseries::find_by_id(&*wtxn, self.env, &self.name, &settings, &new_id)?
                    .ok_or_else(|| {
                        Error::Query("updated time-series document disappeared".into())
                    })?;
            if new_id == id {
                self.validate_unique(&*wtxn, &new, &id)?;
                self.apply_all_index_entries(wtxn, Some(&old), Some(&new), &id)?;
            } else {
                self.apply_all_index_entries(wtxn, Some(&old), None, &id)?;
                self.validate_unique(&*wtxn, &new, &new_id)?;
                self.apply_all_index_entries(wtxn, None, Some(&new), &new_id)?;
            }
            return Ok(FindAndModifyOutcome {
                value: Some(apply_projection(
                    if opts.new { new } else { old },
                    opts.projection,
                    Some(opts.filter),
                )),
                matched: 1,
                modified: 1,
                upserted_id: None,
                new_id: Some(new_id),
            });
        }
        if !opts.upsert {
            return Ok(empty_find_and_modify());
        }
        let mut check = seed_from_filter(opts.filter);
        apply_upsert_with_options(&mut check, update, opts.filter, opts.array_filters)?;
        Self::normalize_for_index(&mut check)?;
        let check_id = check
            .get("_id")
            .and_then(id::key_from_value)
            .unwrap_or_default();
        self.validate_unique(&*wtxn, &check, &check_id)?;
        let report = crate::timeseries::update(
            wtxn,
            self.env,
            &self.name,
            &settings,
            opts.filter,
            update,
            false,
            opts.array_filters,
            true,
        )?;
        let id = report
            .upserted_id
            .ok_or_else(|| Error::Query("upserted time-series document disappeared".into()))?;
        let inserted = crate::timeseries::find_by_id(&*wtxn, self.env, &self.name, &settings, &id)?;
        if let Some(doc) = &inserted {
            self.validate_unique(&*wtxn, doc, &id)?;
            self.apply_all_index_entries(wtxn, None, Some(doc), &id)?;
        }
        let value = if opts.new {
            inserted.map(|doc| apply_projection(doc, opts.projection, Some(opts.filter)))
        } else {
            None
        };
        Ok(FindAndModifyOutcome {
            value,
            matched: 0,
            modified: 0,
            upserted_id: Some(id.clone()),
            new_id: Some(id),
        })
    }

    pub fn find_and_modify(&self, opts: &FindAndModifyOptions<'_>) -> Result<FindAndModifyOutcome> {
        let mut wtxn = self.write_txn()?;
        let out = self.find_and_modify_txn(&mut wtxn, opts)?;
        wtxn.commit()?;
        Ok(out)
    }

    /// Vault variant. Vault filters must identify exactly one document.
    #[allow(clippy::too_many_arguments)]
    pub fn find_and_modify_vault_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        opts: &FindAndModifyOptions<'_>,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<FindAndModifyOutcome> {
        self.ensure_vault()?;
        validate_find_and_modify(opts)?;
        vault::validate_projection(opts.projection)?;
        vault::validate_sort(opts.sort)?;
        let query = FindQuery {
            filter: opts.filter.clone(),
            projection: None,
            sort: opts.sort.to_vec(),
            skip: 0,
            limit: Some(1),
            comment: None,
            max_time_ms: None,
            hint: None,
        };
        let selected = self
            .find_vault_in(&*wtxn, &query, key, tenant, kid)?
            .into_iter()
            .next();
        if opts.remove {
            let Some(old) = selected else {
                return Ok(empty_find_and_modify());
            };
            let id = old
                .get("_id")
                .and_then(Value::as_str)
                .ok_or(Error::InvalidId)?
                .to_string();
            let filter = serde_json::json!({"_id": id});
            let removed = self.delete_vault_txn(wtxn, &filter, false, key, tenant, kid)?;
            return Ok(FindAndModifyOutcome {
                value: Some(old),
                matched: removed,
                modified: removed,
                upserted_id: None,
                new_id: None,
            });
        }
        let update = opts
            .update
            .ok_or_else(|| Error::Query("find_and_modify requires `update` or `remove`".into()))?;
        if let Some(old) = selected {
            let new = self.write_vault_update_one(
                wtxn,
                old.clone(),
                opts.filter,
                update,
                opts.array_filters,
                key,
                tenant,
                kid,
            )?;
            return Ok(FindAndModifyOutcome {
                value: Some(if opts.new { new } else { old }),
                matched: 1,
                modified: 1,
                upserted_id: None,
                new_id: None,
            });
        }
        if !opts.upsert {
            return Ok(empty_find_and_modify());
        }
        let id = vault::exact_id_filter(opts.filter, tenant)?;
        let mut doc = seed_from_filter(opts.filter);
        doc["_id"] = Value::String(id.clone());
        apply_upsert_with_options(&mut doc, update, opts.filter, opts.array_filters)?;
        let id = self.insert_vault_txn(wtxn, doc, key, tenant, kid)?;
        let value = if opts.new {
            self.find_one_vault_in(&*wtxn, &serde_json::json!({"_id": id}), key, tenant, kid)?
        } else {
            None
        };
        Ok(FindAndModifyOutcome {
            value,
            matched: 0,
            modified: 0,
            upserted_id: Some(id),
            new_id: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn find_and_modify_vault(
        &self,
        opts: &FindAndModifyOptions<'_>,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<FindAndModifyOutcome> {
        let mut wtxn = self.write_txn()?;
        let out = self.find_and_modify_vault_txn(&mut wtxn, opts, key, tenant, kid)?;
        wtxn.commit()?;
        Ok(out)
    }

    pub fn delete_txn(&self, wtxn: &mut RwTxn<'_>, filter: &Value, multi: bool) -> Result<usize> {
        if self.profile.is_columnar() {
            return self.delete_columnar_txn(wtxn, filter, multi);
        }
        if self.profile.is_timeseries() {
            let settings = self.timeseries_settings_in(&*wtxn)?.unwrap_or_default();
            // A pure `_id` equality can go straight to the locator encoded in
            // the synthetic id instead of decoding every bucket.
            let direct_id = (!multi)
                .then(|| filter.as_object())
                .flatten()
                .filter(|object| object.len() == 1)
                .and_then(|object| object.get("_id"))
                .and_then(Value::as_str);
            if let Some(id) = direct_id {
                let before =
                    crate::timeseries::find_by_id(&*wtxn, self.env, &self.name, &settings, id)?;
                let Some(before) = before else {
                    return Ok(0);
                };
                if !crate::timeseries::delete_by_id(wtxn, self.env, &self.name, &settings, id)? {
                    return Ok(0);
                }
                self.apply_index_entries(wtxn, Some(&before), None, id)?;
                self.apply_text_entries(wtxn, Some(&before), None, id)?;
                self.apply_fuzzy_entries(wtxn, Some(&before), None, id)?;
                self.apply_vector_entries(wtxn, Some(&before), None, id)?;
                return Ok(1);
            }
            let query = FindQuery {
                filter: filter.clone(),
                projection: None,
                sort: Vec::new(),
                skip: 0,
                limit: if multi { None } else { Some(1) },
                comment: None,
                max_time_ms: None,
                hint: None,
            };
            let candidate_ids = self.timeseries_index_ids(&*wtxn, filter, None)?;
            let before = if let Some(ids) = &candidate_ids {
                crate::timeseries::find_with_ids(
                    &*wtxn,
                    self.env,
                    &self.name,
                    &settings,
                    &query,
                    Some(ids),
                )?
            } else {
                crate::timeseries::find(&*wtxn, self.env, &self.name, &settings, &query)?
            };
            let removed = if let Some(ids) = &candidate_ids {
                crate::timeseries::delete_with_ids(
                    wtxn, self.env, &self.name, &settings, filter, multi, ids,
                )?
            } else {
                crate::timeseries::delete(wtxn, self.env, &self.name, &settings, filter, multi)?
            };
            for doc in before {
                if let Some(id) = doc.get("_id").and_then(Value::as_str) {
                    self.apply_index_entries(wtxn, Some(&doc), None, id)?;
                    self.apply_text_entries(wtxn, Some(&doc), None, id)?;
                    self.apply_fuzzy_entries(wtxn, Some(&doc), None, id)?;
                    self.apply_vector_entries(wtxn, Some(&doc), None, id)?;
                }
            }
            return Ok(removed);
        }
        self.ensure_standard()?;
        let keys = self.matching_keys(&*wtxn, filter, multi)?;
        let mut deleted_docs = Vec::with_capacity(keys.len());
        for key in &keys {
            let id = std::str::from_utf8(key)
                .map_err(|e| Error::Query(e.to_string()))?
                .to_string();
            let raw = self
                .db
                .get(&*wtxn, key.as_slice())?
                .ok_or_else(|| Error::Query("document disappeared".into()))?
                .to_vec();
            let old: Value = parse_json(&raw)?;
            self.db.delete(&mut *wtxn, key.as_slice())?;
            self.apply_index_entries(wtxn, Some(&old), None, &id)?;
            self.apply_graph_entries(wtxn, Some(&old), None)?;
            self.apply_vector_entries(wtxn, Some(&old), None, &id)?;
            self.set_ttl(wtxn, &id, None)?;
            self.record_history(wtxn, &id, "delete", None)?;
            deleted_docs.push((id, old));
        }
        self.apply_text_deletes(wtxn, &deleted_docs)?;
        self.apply_fuzzy_deletes(wtxn, &deleted_docs)?;
        Ok(keys.len())
    }

    pub fn delete(&self, filter: &Value, multi: bool) -> Result<usize> {
        let mut wtxn = self.write_txn()?;
        let n = self.delete_txn(&mut wtxn, filter, multi)?;
        wtxn.commit()?;
        Ok(n)
    }

    // ----------------------------- graph -----------------------------

    fn graph_adjacency_db(
        &self,
        txn: &RoTxn<'_>,
        direction: GraphDirection,
        generation: u64,
    ) -> Result<Option<Database<Bytes, Bytes>>> {
        let resource = graph::adjacency_resource_gen(&self.name, direction, generation);
        self.resource_db(txn, &resource, &resource)
    }

    fn graph_adjacency_db_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        direction: GraphDirection,
        generation: u64,
    ) -> Result<Database<Bytes, Bytes>> {
        let resource = graph::adjacency_resource_gen(&self.name, direction, generation);
        self.resource_db_in(wtxn, &resource, &resource)
    }

    fn graph_def_in(&self, txn: &RoTxn<'_>) -> Result<Option<GraphDef>> {
        let key = graph::node_def_key(&self.name);
        let Some(raw) = self.meta.get(txn, key.as_bytes())? else {
            return Ok(None);
        };
        Ok(Some(graph::parse_def(raw)?))
    }

    pub fn graph_rebuild_state_in(&self, txn: &RoTxn<'_>) -> Result<Option<GraphRebuildState>> {
        let key = graph::rebuild_key(&self.name);
        let Some(raw) = self.meta.get(txn, key.as_bytes())? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(raw)?))
    }

    pub fn graph_rebuild_state(&self) -> Result<Option<GraphRebuildState>> {
        let rtxn = self.read_txn()?;
        let state = self.graph_rebuild_state_in(&rtxn)?;
        rtxn.commit()?;
        Ok(state)
    }

    pub(crate) fn set_graph_rebuild_state_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        state: Option<&GraphRebuildState>,
    ) -> Result<()> {
        let key = graph::rebuild_key(&self.name);
        match state {
            Some(state) => {
                self.meta.put(
                    &mut *wtxn,
                    key.as_bytes(),
                    serde_json::to_vec(state)?.as_slice(),
                )?;
            }
            None => {
                self.meta.delete(&mut *wtxn, key.as_bytes())?;
            }
        }
        Ok(())
    }

    pub(crate) fn edge_graph_def_in(&self, txn: &RoTxn<'_>) -> Result<Option<GraphDef>> {
        let key = graph::edge_def_key(&self.name);
        let Some(raw) = self.meta.get(txn, key.as_bytes())? else {
            return Ok(None);
        };
        Ok(Some(graph::parse_def(raw)?))
    }

    /// Adds or replaces the graph definition rooted at this node collection.
    ///
    /// The adjacency index is rebuilt from the edge collection in the same
    /// transaction. Existing edges are validated; a malformed edge aborts the
    /// whole change.
    pub fn set_graph(&self, mut def: GraphDef) -> Result<GraphDef> {
        def.node_collection = self.name.clone();
        def.validate()?;
        if def.node_collection == def.edge_collection {
            return Err(Error::Query(
                "graph node and edge collections must differ".into(),
            ));
        }
        let mut wtxn = self.write_txn()?;
        self.set_graph_txn(&mut wtxn, &def)?;
        wtxn.commit()?;
        Ok(def)
    }

    pub fn set_graph_txn(&self, wtxn: &mut RwTxn<'_>, def: &GraphDef) -> Result<()> {
        def.validate()?;
        if self.profile.is_vault() {
            return Err(Error::Query(
                "graph operations are not supported on vault collections".into(),
            ));
        }
        let store = self.store.ok_or_else(|| {
            Error::Query("graph configuration requires a store-backed collection".into())
        })?;
        let edge = store
            .collection_at(&*wtxn, &def.edge_collection)?
            .ok_or_else(|| {
                Error::Query(format!(
                    "graph edge collection `{}` does not exist",
                    def.edge_collection
                ))
            })?;
        if edge.profile.is_vault() {
            return Err(Error::Query(
                "graph edge collection cannot be a vault collection".into(),
            ));
        }
        if let Some(existing) = edge.edge_graph_def_in(&*wtxn)? {
            if existing.node_collection != def.node_collection {
                return Err(Error::Query(format!(
                    "edge collection `{}` already belongs to graph `{}`",
                    def.edge_collection, existing.node_collection
                )));
            }
        }
        if let Some(existing) = self.graph_def_in(&*wtxn)? {
            edge.clear_graph_adjacency_txn(wtxn, &existing)?;
            edge.set_graph_rebuild_state_in(wtxn, None)?;
            if existing.edge_collection != def.edge_collection {
                self.meta.delete(
                    &mut *wtxn,
                    graph::edge_def_key(&existing.edge_collection).as_bytes(),
                )?;
            }
        }
        self.meta.put(
            &mut *wtxn,
            graph::node_def_key(&self.name).as_bytes(),
            serde_json::to_vec(def)?.as_slice(),
        )?;
        self.meta.put(
            &mut *wtxn,
            graph::edge_def_key(&edge.name).as_bytes(),
            serde_json::to_vec(def)?.as_slice(),
        )?;
        edge.rebuild_graph_adjacency_txn(wtxn, def)?;
        Ok(())
    }

    pub fn get_graph(&self) -> Result<Option<GraphDef>> {
        if self.profile.is_vault() {
            return Err(Error::Query(
                "graph operations are not supported on vault collections".into(),
            ));
        }
        let rtxn = self.read_txn()?;
        let out = self.graph_def_in(&rtxn)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn get_graph_in(&self, rtxn: &RoTxn<'_>) -> Result<Option<GraphDef>> {
        self.graph_def_in(rtxn)
    }

    /// Removes the graph definition and clears its adjacency index.
    pub fn drop_graph(&self) -> Result<bool> {
        let mut wtxn = self.write_txn()?;
        let dropped = self.drop_graph_txn(&mut wtxn)?;
        wtxn.commit()?;
        Ok(dropped)
    }

    pub fn drop_graph_txn(&self, wtxn: &mut RwTxn<'_>) -> Result<bool> {
        let Some(def) = self.graph_def_in(&*wtxn)? else {
            return Ok(false);
        };
        let store = self.store.ok_or_else(|| {
            Error::Query("graph configuration requires a store-backed collection".into())
        })?;
        if let Some(edge) = store.collection_at(&*wtxn, &def.edge_collection)? {
            edge.clear_graph_adjacency_txn(wtxn, &def)?;
            edge.set_graph_rebuild_state_in(wtxn, None)?;
        }
        self.meta
            .delete(&mut *wtxn, graph::node_def_key(&self.name).as_bytes())?;
        self.meta.delete(
            &mut *wtxn,
            graph::edge_def_key(&def.edge_collection).as_bytes(),
        )?;
        Ok(true)
    }

    pub(crate) fn clear_graph_adjacency_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        def: &GraphDef,
    ) -> Result<()> {
        for generation in 0..=def.generation.saturating_add(1) {
            for direction in [GraphDirection::Out, GraphDirection::In] {
                let resource =
                    graph::adjacency_resource_gen(&def.edge_collection, direction, generation);
                self.drop_resource_db(wtxn, &resource, &resource)?;
            }
        }
        Ok(())
    }

    fn rebuild_graph_adjacency_txn(&self, wtxn: &mut RwTxn<'_>, def: &GraphDef) -> Result<()> {
        let docs = self.find_in(
            &*wtxn,
            &FindQuery {
                filter: Value::Object(Map::new()),
                projection: None,
                sort: Vec::new(),
                skip: 0,
                limit: None,
                comment: None,
                max_time_ms: None,
                hint: None,
            },
        )?;
        let out = self.graph_adjacency_db_in(wtxn, GraphDirection::Out, def.generation)?;
        let incoming = self.graph_adjacency_db_in(wtxn, GraphDirection::In, def.generation)?;
        for doc in &docs {
            for (direction, key, value) in graph::adjacency_entries(def, doc)? {
                match direction {
                    GraphDirection::Out => out.put(&mut *wtxn, key.as_slice(), value.as_slice())?,
                    GraphDirection::In => {
                        incoming.put(&mut *wtxn, key.as_slice(), value.as_slice())?
                    }
                    GraphDirection::Both => unreachable!(),
                }
            }
        }
        Ok(())
    }

    pub fn reindex_graph(&self) -> Result<usize> {
        let mut wtxn = self.write_txn()?;
        let count = self.reindex_graph_txn(&mut wtxn)?;
        wtxn.commit()?;
        Ok(count)
    }

    pub fn reindex_graph_txn(&self, wtxn: &mut RwTxn<'_>) -> Result<usize> {
        let def = self
            .graph_def_in(&*wtxn)?
            .ok_or_else(|| Error::Query("graph is not configured for this collection".into()))?;
        let store = self.store.ok_or_else(|| {
            Error::Query("graph configuration requires a store-backed collection".into())
        })?;
        let edge = store
            .collection_at(&*wtxn, &def.edge_collection)?
            .ok_or_else(|| {
                Error::Query(format!(
                    "graph edge collection `{}` does not exist",
                    def.edge_collection
                ))
            })?;
        if edge.graph_rebuild_state_in(&*wtxn)?.is_some() {
            return Err(Error::Query(
                "an online graph rebuild is running; cancel it first".into(),
            ));
        }
        edge.clear_graph_adjacency_txn(wtxn, &def)?;
        edge.rebuild_graph_adjacency_txn(wtxn, &def)?;
        Ok(edge.documents_for_backfill(&*wtxn)?.len())
    }

    /// Starts an online adjacency rebuild. Readers keep using the active
    /// generation; edge writes are mirrored into the target generation until
    /// the maintenance worker atomically swaps it in.
    pub fn start_graph_rebuild(&self) -> Result<Value> {
        let mut wtxn = self.write_txn()?;
        let out = self.start_graph_rebuild_txn(&mut wtxn)?;
        wtxn.commit()?;
        Ok(out)
    }

    pub fn start_graph_rebuild_txn(&self, wtxn: &mut RwTxn<'_>) -> Result<Value> {
        if self.profile.is_vault() {
            return Err(Error::Query(
                "graph operations are not supported on vault collections".into(),
            ));
        }
        let def = self
            .graph_def_in(&*wtxn)?
            .ok_or_else(|| Error::Query("graph is not configured for this collection".into()))?;
        let store = self.store.ok_or_else(|| {
            Error::Query("graph configuration requires a store-backed collection".into())
        })?;
        let edge = store
            .collection_at(&*wtxn, &def.edge_collection)?
            .ok_or_else(|| {
                Error::Query(format!(
                    "graph edge collection `{}` does not exist",
                    def.edge_collection
                ))
            })?;
        if edge.graph_rebuild_state_in(&*wtxn)?.is_some() {
            return Err(Error::Query("graph rebuild is already running".into()));
        }
        let target = def.generation.saturating_add(1);
        if target > graph::MAX_GRAPH_GENERATION {
            return Err(Error::Query(format!(
                "graph rebuild would exceed the maximum generation {}",
                graph::MAX_GRAPH_GENERATION
            )));
        }
        for direction in [GraphDirection::Out, GraphDirection::In] {
            let resource = graph::adjacency_resource_gen(&edge.name, direction, target);
            edge.drop_resource_db(&mut *wtxn, &resource, &resource)?;
        }
        let state = GraphRebuildState {
            node_collection: self.name.clone(),
            edge_collection: edge.name.clone(),
            target_generation: target,
            after: None,
            scanned: 0,
        };
        edge.set_graph_rebuild_state_in(wtxn, Some(&state))?;
        Ok(json!({
            "active_generation": def.generation,
            "target_generation": target,
            "scanned": 0,
            "done": false,
        }))
    }

    pub fn graph_rebuild_status(&self) -> Result<Value> {
        let rtxn = self.read_txn()?;
        let def = self
            .graph_def_in(&rtxn)?
            .ok_or_else(|| Error::Query("graph is not configured for this collection".into()))?;
        let store = self.store.ok_or_else(|| {
            Error::Query("graph configuration requires a store-backed collection".into())
        })?;
        let Some(edge) = store.collection_at(&rtxn, &def.edge_collection)? else {
            return Ok(json!({
                "active_generation": def.generation,
                "target_generation": Value::Null,
                "scanned": 0,
                "done": true,
            }));
        };
        let state = edge.graph_rebuild_state_in(&rtxn)?;
        rtxn.commit()?;
        Ok(match state {
            Some(state) => json!({
                "active_generation": def.generation,
                "target_generation": state.target_generation,
                "scanned": state.scanned,
                "after": state.after,
                "done": false,
            }),
            None => json!({
                "active_generation": def.generation,
                "target_generation": Value::Null,
                "scanned": 0,
                "done": true,
            }),
        })
    }

    pub fn cancel_graph_rebuild(&self) -> Result<bool> {
        let mut wtxn = self.write_txn()?;
        let out = self.cancel_graph_rebuild_txn(&mut wtxn)?;
        wtxn.commit()?;
        Ok(out)
    }

    pub fn cancel_graph_rebuild_txn(&self, wtxn: &mut RwTxn<'_>) -> Result<bool> {
        let Some(def) = self.graph_def_in(&*wtxn)? else {
            return Ok(false);
        };
        let store = self.store.ok_or_else(|| {
            Error::Query("graph configuration requires a store-backed collection".into())
        })?;
        let Some(edge) = store.collection_at(&*wtxn, &def.edge_collection)? else {
            return Ok(false);
        };
        let Some(state) = edge.graph_rebuild_state_in(&*wtxn)? else {
            return Ok(false);
        };
        for direction in [GraphDirection::Out, GraphDirection::In] {
            let resource =
                graph::adjacency_resource_gen(&edge.name, direction, state.target_generation);
            edge.drop_resource_db(&mut *wtxn, &resource, &resource)?;
        }
        edge.set_graph_rebuild_state_in(wtxn, None)?;
        Ok(true)
    }

    /// Builds one bounded chunk of the online rebuild. Called by the daemon
    /// maintenance worker through the single writer thread.
    pub fn step_graph_rebuild(&self, limit: usize) -> Result<Value> {
        let mut wtxn = self.write_txn()?;
        let out = self.step_graph_rebuild_txn(&mut wtxn, limit)?;
        wtxn.commit()?;
        Ok(out)
    }

    pub fn step_graph_rebuild_txn(&self, wtxn: &mut RwTxn<'_>, limit: usize) -> Result<Value> {
        let limit = limit.clamp(1, 10_000);
        let Some(mut state) = self.graph_rebuild_state_in(&*wtxn)? else {
            return Ok(json!({"done": true, "scanned": 0}));
        };
        let store = self.store.ok_or_else(|| {
            Error::Query("graph configuration requires a store-backed collection".into())
        })?;
        let Some(node) = store.collection_at(&*wtxn, &state.node_collection)? else {
            return Err(Error::Query(
                "graph rebuild node collection is missing".into(),
            ));
        };
        let Some(def) = node.graph_def_in(&*wtxn)? else {
            return Err(Error::Query("graph rebuild definition is missing".into()));
        };
        if def.generation.saturating_add(1) != state.target_generation {
            return Err(Error::Query(
                "graph rebuild target generation is stale".into(),
            ));
        }
        if state.target_generation > graph::MAX_GRAPH_GENERATION {
            return Err(Error::Query(format!(
                "graph rebuild target exceeds the maximum generation {}",
                graph::MAX_GRAPH_GENERATION
            )));
        }
        let out =
            self.graph_adjacency_db_in(&mut *wtxn, GraphDirection::Out, state.target_generation)?;
        let incoming =
            self.graph_adjacency_db_in(&mut *wtxn, GraphDirection::In, state.target_generation)?;
        let mut docs = Vec::with_capacity(limit);
        if self.profile.is_columnar() {
            let mut all = self.documents_for_backfill(&*wtxn)?;
            all.sort_by(|a, b| a.0.cmp(&b.0));
            for (id, doc) in all {
                if state
                    .after
                    .as_deref()
                    .is_some_and(|after| id.as_str() <= after)
                {
                    continue;
                }
                docs.push((id, serde_json::to_vec(&doc)?));
                if docs.len() >= limit {
                    break;
                }
            }
        } else {
            let range = match state.after.as_deref() {
                Some(after) => (Bound::Excluded(after.as_bytes()), Bound::Unbounded),
                None => (Bound::Unbounded, Bound::Unbounded),
            };
            for item in self.db.range(&*wtxn, &range)? {
                let (id, raw) = item?;
                docs.push((String::from_utf8_lossy(id).into_owned(), raw.to_vec()));
                if docs.len() >= limit {
                    break;
                }
            }
        }
        let done_scan = docs.len() < limit;
        for (id, raw) in &docs {
            let doc: Value = parse_json(raw)?;
            for (direction, key, value) in graph::adjacency_entries(&def, &doc)? {
                match direction {
                    GraphDirection::Out => out.put(&mut *wtxn, key.as_slice(), value.as_slice())?,
                    GraphDirection::In => {
                        incoming.put(&mut *wtxn, key.as_slice(), value.as_slice())?
                    }
                    GraphDirection::Both => unreachable!(),
                }
            }
            state.after = Some(id.clone());
        }
        state.scanned = state.scanned.saturating_add(docs.len() as u64);
        if !done_scan {
            self.set_graph_rebuild_state_in(wtxn, Some(&state))?;
            return Ok(json!({
                "done": false,
                "scanned": state.scanned,
                "after": state.after,
            }));
        }
        // Atomic swap: bump both metadata copies, remove the state, and delete
        // the old generations in the same transaction.
        let mut new_def = def.clone();
        new_def.generation = state.target_generation;
        new_def.validate()?;
        let raw_def = serde_json::to_vec(&new_def)?;
        node.meta.put(
            &mut *wtxn,
            graph::node_def_key(&node.name).as_bytes(),
            raw_def.as_slice(),
        )?;
        self.meta.put(
            &mut *wtxn,
            graph::edge_def_key(&self.name).as_bytes(),
            raw_def.as_slice(),
        )?;
        for generation in 0..state.target_generation {
            for direction in [GraphDirection::Out, GraphDirection::In] {
                let resource = graph::adjacency_resource_gen(&self.name, direction, generation);
                self.drop_resource_db(&mut *wtxn, &resource, &resource)?;
            }
        }
        self.set_graph_rebuild_state_in(wtxn, None)?;
        Ok(json!({
            "done": true,
            "scanned": state.scanned,
            "active_generation": state.target_generation,
        }))
    }

    pub fn graph_traverse(&self, mut options: TraverseOptions) -> Result<Value> {
        if self.profile.is_vault() {
            return Err(Error::Query(
                "graph operations are not supported on vault collections".into(),
            ));
        }
        options.validate()?;
        let rtxn = self.read_txn()?;
        let def = self
            .graph_def_in(&rtxn)?
            .ok_or_else(|| Error::Query("graph is not configured for this collection".into()))?;
        if let Some(seed) = options.seed.as_ref() {
            if !options.start.is_empty() {
                return Err(Error::Query(
                    "traverse accepts either `start` or `seed.vector`, not both".into(),
                ));
            }
            let docs = self.vector_search_in(
                &rtxn,
                &seed.vector.field,
                &seed.vector.vector,
                &seed.vector.filter,
                0,
                Some(seed.vector.k),
            )?;
            options.start = docs
                .into_iter()
                .filter_map(|doc| doc.get("_id").cloned())
                .collect();
            if options.start.is_empty() {
                return Ok(json!({
                    "roots": [],
                    "nodes": 0,
                    "edges": 0,
                    "truncated": false,
                    "missing": [],
                }));
            }
        }
        let store = self.store.ok_or_else(|| {
            Error::Query("graph traversal requires a store-backed collection".into())
        })?;
        let view = GraphViewImpl::new(store, &rtxn, def)?;
        let out = graph::traverse(&view, &options)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn graph_shortest_path(&self, options: ShortestPathOptions) -> Result<Value> {
        if self.profile.is_vault() {
            return Err(Error::Query(
                "graph operations are not supported on vault collections".into(),
            ));
        }
        options.validate()?;
        let rtxn = self.read_txn()?;
        let def = self
            .graph_def_in(&rtxn)?
            .ok_or_else(|| Error::Query("graph is not configured for this collection".into()))?;
        let store = self.store.ok_or_else(|| {
            Error::Query("graph traversal requires a store-backed collection".into())
        })?;
        let view = GraphViewImpl::new(store, &rtxn, def)?;
        let out = graph::shortest_path(&view, &options)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn graph_cypher(&self, options: CypherOptions) -> Result<Value> {
        if self.profile.is_vault() {
            return Err(Error::Query(
                "graph operations are not supported on vault collections".into(),
            ));
        }
        options.validate()?;
        let rtxn = self.read_txn()?;
        let def = self
            .graph_def_in(&rtxn)?
            .ok_or_else(|| Error::Query("graph is not configured for this collection".into()))?;
        let store = self.store.ok_or_else(|| {
            Error::Query("graph traversal requires a store-backed collection".into())
        })?;
        let view = GraphViewImpl::new(store, &rtxn, def)?;
        let out = crate::cypher::execute(&view, &options)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn graph_all_shortest_paths(&self, options: AllShortestPathsOptions) -> Result<Value> {
        if self.profile.is_vault() {
            return Err(Error::Query(
                "graph operations are not supported on vault collections".into(),
            ));
        }
        options.validate()?;
        let rtxn = self.read_txn()?;
        let def = self
            .graph_def_in(&rtxn)?
            .ok_or_else(|| Error::Query("graph is not configured for this collection".into()))?;
        let store = self.store.ok_or_else(|| {
            Error::Query("graph traversal requires a store-backed collection".into())
        })?;
        let view = GraphViewImpl::new(store, &rtxn, def)?;
        let out = graph::all_shortest_paths(&view, &options)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn graph_match(&self, options: GraphMatchOptions) -> Result<Value> {
        if self.profile.is_vault() {
            return Err(Error::Query(
                "graph operations are not supported on vault collections".into(),
            ));
        }
        options.validate()?;
        let rtxn = self.read_txn()?;
        let def = self
            .graph_def_in(&rtxn)?
            .ok_or_else(|| Error::Query("graph is not configured for this collection".into()))?;
        let store = self.store.ok_or_else(|| {
            Error::Query("graph traversal requires a store-backed collection".into())
        })?;
        let view = GraphViewImpl::new(store, &rtxn, def)?;
        let out = graph::graph_match(&view, &options)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn graph_algorithm(&self, options: GraphAlgorithmOptions) -> Result<Value> {
        self.graph_algorithm_with_observer(options, None)
    }

    pub fn graph_algorithm_with_observer(
        &self,
        options: GraphAlgorithmOptions,
        observer: Option<&dyn GraphAlgorithmObserver>,
    ) -> Result<Value> {
        if self.profile.is_vault() {
            return Err(Error::Query(
                "graph operations are not supported on vault collections".into(),
            ));
        }
        options.validate()?;
        let rtxn = self.read_txn()?;
        let def = self
            .graph_def_in(&rtxn)?
            .ok_or_else(|| Error::Query("graph is not configured for this collection".into()))?;
        let store = self.store.ok_or_else(|| {
            Error::Query("graph traversal requires a store-backed collection".into())
        })?;
        let view = GraphViewImpl::new(store, &rtxn, def)?;
        let out = graph::graph_algorithm_with_observer(&view, &options, observer)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn graph_neighbors(&self, options: NeighborsOptions) -> Result<Value> {
        if self.profile.is_vault() {
            return Err(Error::Query(
                "graph operations are not supported on vault collections".into(),
            ));
        }
        options.validate()?;
        let rtxn = self.read_txn()?;
        let def = self
            .graph_def_in(&rtxn)?
            .ok_or_else(|| Error::Query("graph is not configured for this collection".into()))?;
        let store = self.store.ok_or_else(|| {
            Error::Query("graph traversal requires a store-backed collection".into())
        })?;
        let view = GraphViewImpl::new(store, &rtxn, def)?;
        let out = graph::neighbors(&view, &options)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn hybrid_search(&self, options: HybridOptions) -> Result<Value> {
        if self.profile.is_vault() {
            return Err(Error::Query(
                "graph operations are not supported on vault collections".into(),
            ));
        }
        options.validate()?;
        let rtxn = self.read_txn()?;
        let def = self
            .graph_def_in(&rtxn)?
            .ok_or_else(|| Error::Query("graph is not configured for this collection".into()))?;
        let seeds = self.vector_search_in(
            &rtxn,
            &options.vector.field,
            &options.vector.vector,
            &options.vector.filter,
            0,
            Some(options.vector.k),
        )?;
        let seed_ids: Vec<String> = seeds
            .iter()
            .filter_map(|doc| doc.get("_id").and_then(crate::id::key_from_value))
            .collect();
        if seed_ids.is_empty() {
            rtxn.commit()?;
            return Ok(json!({
                "items": [],
                "nodes": 0,
                "edges": 0,
                "truncated": false,
            }));
        }
        let def_key = vector::def_key(&self.name, &options.vector.field);
        let Some(raw_def) = self.meta.get(&rtxn, def_key.as_bytes())? else {
            return Err(Error::Query(format!(
                "no vector index on field `{}`",
                options.vector.field
            )));
        };
        let vector_def: VectorDef = serde_json::from_slice(raw_def)?;
        let store = self.store.ok_or_else(|| {
            Error::Query("graph traversal requires a store-backed collection".into())
        })?;
        let view = GraphViewImpl::new(store, &rtxn, def)?;
        let out = graph::hybrid(&view, &options, &seed_ids, vector_def.metric)?;
        rtxn.commit()?;
        Ok(out)
    }

    fn validate_graph_edge_txn(&self, txn: &RoTxn<'_>, doc: &Value) -> Result<()> {
        if self.profile.is_vault() {
            return Ok(());
        }
        if let Some(def) = self.edge_graph_def_in(txn)? {
            graph::adjacency_entries(&def, doc)?;
        }
        Ok(())
    }

    /// Maintains the native adjacency index for an edge collection.
    fn apply_graph_entries(
        &self,
        wtxn: &mut RwTxn<'_>,
        old: Option<&Value>,
        new: Option<&Value>,
    ) -> Result<()> {
        if self.profile.is_vault() {
            return Ok(());
        }
        let Some(def) = self.edge_graph_def_in(&*wtxn)? else {
            return Ok(());
        };
        let mut generations = vec![def.generation];
        if let Some(state) = self.graph_rebuild_state_in(&*wtxn)? {
            if state.target_generation != def.generation {
                generations.push(state.target_generation);
            }
        }
        for generation in generations {
            let out = self.graph_adjacency_db_in(wtxn, GraphDirection::Out, generation)?;
            let incoming = self.graph_adjacency_db_in(wtxn, GraphDirection::In, generation)?;
            for doc in [old, new].into_iter().flatten() {
                for (direction, key, _) in graph::adjacency_entries(&def, doc)? {
                    match direction {
                        GraphDirection::Out => {
                            out.delete(&mut *wtxn, key.as_slice())?;
                        }
                        GraphDirection::In => {
                            incoming.delete(&mut *wtxn, key.as_slice())?;
                        }
                        GraphDirection::Both => unreachable!(),
                    }
                }
            }
            if let Some(doc) = new {
                for (direction, key, value) in graph::adjacency_entries(&def, doc)? {
                    match direction {
                        GraphDirection::Out => {
                            out.put(&mut *wtxn, key.as_slice(), value.as_slice())?
                        }
                        GraphDirection::In => {
                            incoming.put(&mut *wtxn, key.as_slice(), value.as_slice())?
                        }
                        GraphDirection::Both => unreachable!(),
                    }
                }
            }
        }
        Ok(())
    }

    // ----------------------------- vault -----------------------------

    fn vault_stored(id: &str, tenant: &str, envelope: Value) -> Result<Value> {
        let enc = envelope.get("_enc").cloned().ok_or_else(|| Error::Vault {
            code: "encryption_failed",
            message: "encryption did not produce an envelope".into(),
        })?;
        Ok(serde_json::json!({
            "_id": id,
            "tenant_id": tenant,
            "_enc": enc,
        }))
    }

    fn decrypt_stored(
        &self,
        stored: &Value,
        key: &VaultKey,
        tenant: &str,
        id: &str,
        kid: &str,
    ) -> Result<Value> {
        if vault::stored_tenant(stored) != Some(tenant) {
            return Err(Error::Vault {
                code: "tenant_mismatch",
                message: "document belongs to a different tenant".into(),
            });
        }
        let doc = vault::decrypt_document(stored, key, &self.name, tenant, id, kid)?;
        let got_id = doc.get("_id").and_then(Value::as_str);
        let got_tenant = doc.get(vault::TENANT_FIELD).and_then(Value::as_str);
        if got_id != Some(id) || got_tenant != Some(tenant) {
            return Err(Error::Vault {
                code: "decryption_failed",
                message: "decrypted document identity does not match its envelope".into(),
            });
        }
        Ok(doc)
    }

    fn check_vault_size(stored: &Value) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec(stored)?;
        if bytes.len() > MAX_DOCUMENT_BYTES {
            return Err(Error::DocumentTooLarge(bytes.len(), MAX_DOCUMENT_BYTES));
        }
        Ok(bytes)
    }

    /// Reads the encrypted stored envelope for a vault document.
    pub fn vault_stored_txn(&self, wtxn: &RwTxn<'_>, id: &str) -> Result<Option<Value>> {
        self.ensure_vault()?;
        let Some(raw) = self.db.get(wtxn, id.as_bytes())? else {
            return Ok(None);
        };
        parse_json(raw).map(Some)
    }

    /// Applies an already-encrypted vault envelope from the replication log.
    pub fn apply_vault_replication_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        op: &str,
        id: &str,
        after: Option<&Value>,
        ttl_at: Option<u64>,
    ) -> Result<()> {
        self.ensure_vault()?;
        match op {
            "insert" | "update" | "upsert" => {
                let stored = after.ok_or_else(|| {
                    Error::Query("vault replication change is missing stored document".into())
                })?;
                if stored.get("_id").and_then(Value::as_str) != Some(id) {
                    return Err(Error::Query(
                        "vault replication stored document id does not match".into(),
                    ));
                }
                if vault::stored_tenant(stored).is_none() {
                    return Err(Error::Query(
                        "vault replication stored document has no tenant".into(),
                    ));
                }
                let bytes = Self::check_vault_size(stored)?;
                self.db.put(&mut *wtxn, id.as_bytes(), &bytes)?;
                self.set_ttl(wtxn, id, ttl_at)?;
                self.record_history(
                    wtxn,
                    id,
                    if op == "insert" { "insert" } else { "update" },
                    Some(stored.clone()),
                )?;
                Ok(())
            }
            "delete" => {
                self.db.delete(&mut *wtxn, id.as_bytes())?;
                self.set_ttl(wtxn, id, None)?;
                self.record_history(wtxn, id, "delete", None)?;
                Ok(())
            }
            other => Err(Error::Query(format!(
                "unsupported vault replication operation `{other}`"
            ))),
        }
    }

    /// Inserts a document into a vault collection in the given transaction.
    pub fn insert_vault_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        mut doc: Value,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<String> {
        self.ensure_vault()?;
        vault::validate_context(tenant, kid)?;
        let obj = doc
            .as_object_mut()
            .ok_or_else(|| Error::Query("document must be an object".into()))?;

        let id = match obj.get("_id") {
            Some(Value::String(s)) => s.clone(),
            Some(_) => return Err(Error::InvalidId),
            None => {
                let id = uuid::Uuid::new_v4().to_string();
                obj.insert("_id".to_string(), Value::String(id.clone()));
                id
            }
        };
        match obj.get(vault::TENANT_FIELD) {
            Some(Value::String(got)) if got == tenant => {}
            Some(Value::String(_)) => {
                return Err(Error::Vault {
                    code: "tenant_mismatch",
                    message: "document `tenant_id` does not match X-NonoDB-Tenant".into(),
                })
            }
            Some(_) => {
                return Err(Error::Vault {
                    code: "invalid_vault_document",
                    message: "`tenant_id` must be a string".into(),
                })
            }
            None => {
                obj.insert(
                    vault::TENANT_FIELD.to_string(),
                    Value::String(tenant.to_string()),
                );
            }
        }

        if self.db.get(&*wtxn, id.as_bytes())?.is_some() {
            return Err(Error::DuplicateKey(id));
        }
        let ttl_at = ttl::take_insert_ttl_with_field(
            &mut doc,
            self.ttl_default_seconds,
            self.ttl_field_pair(),
        )?;
        date::normalize_document(&mut doc)?;
        numeric::normalize_document(&mut doc)?;
        bson_types::normalize_document(&mut doc)?;
        let envelope = vault::encrypt_document(&doc, key, &self.name, tenant, &id, kid)?;
        let stored = Self::vault_stored(&id, tenant, envelope)?;
        let bytes = Self::check_vault_size(&stored)?;
        self.db.put(&mut *wtxn, id.as_bytes(), &bytes)?;
        self.set_ttl(wtxn, &id, ttl_at)?;
        self.record_history(wtxn, &id, "insert", Some(stored))?;
        Ok(id)
    }

    /// Inserts many documents into a vault collection. Not all-or-nothing.
    pub fn insert_many_vault_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        docs: &[Value],
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<Vec<String>> {
        let mut ids = Vec::with_capacity(docs.len());
        for doc in docs {
            ids.push(self.insert_vault_txn(wtxn, doc.clone(), key, tenant, kid)?);
        }
        Ok(ids)
    }

    /// Updates exactly one vault document by `_id`. `multi` is not supported.
    #[allow(clippy::too_many_arguments)]
    pub fn update_vault_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        filter: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<UpdateOutcome> {
        self.update_vault_txn_filtered(wtxn, filter, update, multi, upsert, key, tenant, kid, &[])
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_vault_txn_filtered(
        &self,
        wtxn: &mut RwTxn<'_>,
        filter: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
        array_filters: &[Value],
    ) -> Result<UpdateOutcome> {
        self.ensure_vault()?;
        if multi {
            return Err(Error::Vault {
                code: "invalid_vault_filter",
                message: "multi update is not supported for vault collections".into(),
            });
        }
        let id = vault::exact_id_filter(filter, tenant)?;
        let Some(raw) = self.db.get(&*wtxn, id.as_bytes())? else {
            if !upsert {
                return Ok(UpdateOutcome {
                    matched: 0,
                    modified: 0,
                    upserted_id: None,
                    moved: Vec::new(),
                });
            }
            let mut doc = seed_from_filter(filter);
            doc["_id"] = Value::String(id.clone());
            apply_upsert_with_options(&mut doc, update, filter, array_filters)?;
            let id = self.insert_vault_txn(wtxn, doc, key, tenant, kid)?;
            return Ok(UpdateOutcome {
                matched: 0,
                modified: 0,
                upserted_id: Some(id),
                moved: Vec::new(),
            });
        };
        let old_stored: Value = parse_json(raw)?;
        let old = self.decrypt_stored(&old_stored, key, tenant, &id, kid)?;
        self.write_vault_update_one(wtxn, old, filter, update, array_filters, key, tenant, kid)?;
        Ok(UpdateOutcome {
            matched: 1,
            modified: 1,
            upserted_id: None,
            moved: Vec::new(),
        })
    }

    /// Deletes exactly one vault document by `_id`.
    #[allow(clippy::too_many_arguments)]
    fn write_vault_update_one(
        &self,
        wtxn: &mut RwTxn<'_>,
        old: Value,
        filter: &Value,
        update: &Value,
        array_filters: &[Value],
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<Value> {
        let id = old
            .get("_id")
            .and_then(Value::as_str)
            .ok_or(Error::InvalidId)?
            .to_string();
        let old_ttl = self.ttl_at(&*wtxn, &id)?;
        let mut new = old.clone();
        apply_with_options(&mut new, update, filter, array_filters)?;
        let ttl_at = match ttl::take_update_ttl_with_field(&mut new, self.ttl_field_pair())? {
            TtlUpdate::Preserve => old_ttl,
            TtlUpdate::Clear => None,
            TtlUpdate::Set(at) => Some(at),
        };
        date::normalize_document(&mut new)?;
        numeric::normalize_document(&mut new)?;
        bson_types::normalize_document(&mut new)?;
        if new.get("_id").and_then(Value::as_str) != Some(id.as_str()) {
            return Err(Error::InvalidId);
        }
        if new.get(vault::TENANT_FIELD).and_then(Value::as_str) != Some(tenant) {
            return Err(Error::Vault {
                code: "tenant_mismatch",
                message: "update cannot change `tenant_id`".into(),
            });
        }
        let envelope = vault::encrypt_document(&new, key, &self.name, tenant, &id, kid)?;
        let stored = Self::vault_stored(&id, tenant, envelope)?;
        let bytes = Self::check_vault_size(&stored)?;
        self.db.put(&mut *wtxn, id.as_bytes(), &bytes)?;
        self.set_ttl(wtxn, &id, ttl_at)?;
        self.record_history(wtxn, &id, "update", Some(stored))?;
        Ok(new)
    }

    pub fn delete_vault_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        filter: &Value,
        multi: bool,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<usize> {
        self.ensure_vault()?;
        if multi {
            return Err(Error::Vault {
                code: "invalid_vault_filter",
                message: "multi delete is not supported for vault collections".into(),
            });
        }
        let id = vault::exact_id_filter(filter, tenant)?;
        let Some(raw) = self.db.get(&*wtxn, id.as_bytes())? else {
            return Ok(0);
        };
        let stored: Value = parse_json(raw)?;
        // Decrypt before deleting: this enforces the tenant/key and catches corruption.
        self.decrypt_stored(&stored, key, tenant, &id, kid)?;
        self.db.delete(&mut *wtxn, id.as_bytes())?;
        self.set_ttl(wtxn, &id, None)?;
        self.record_history(wtxn, &id, "delete", None)?;
        Ok(1)
    }

    /// Deletes an expired document without decrypting vault payloads.
    ///
    /// The expected expiry must still match the TTL entry; this protects a
    /// concurrent update that refreshed the TTL after the sweeper read it.
    /// Returns the stored document so the caller can emit the delete event
    /// after strict TTL reads have already hidden it.
    pub fn expire_txn(&self, wtxn: &mut RwTxn<'_>, id: &str, expected_ttl_at: u64) -> Result<bool> {
        Ok(self
            .expire_txn_with_doc(wtxn, id, expected_ttl_at)?
            .is_some())
    }

    /// Like [`Self::expire_txn`], but returns the expired stored document.
    pub fn expire_txn_with_doc(
        &self,
        wtxn: &mut RwTxn<'_>,
        id: &str,
        expected_ttl_at: u64,
    ) -> Result<Option<Value>> {
        let Some(current) = self.ttl_at(&*wtxn, id)? else {
            return Ok(None);
        };
        if current != expected_ttl_at {
            return Ok(None);
        }
        if self.profile.is_columnar() {
            let Some(old) = crate::columnar::find_by_id(&*wtxn, self.env, &self.name, id)? else {
                self.set_ttl(wtxn, id, None)?;
                return Ok(None);
            };
            let locator = crate::columnar::locator_for_id(&*wtxn, self.env, &self.name, id)?;
            let id_value = old
                .get("_id")
                .cloned()
                .unwrap_or_else(|| serde_json::Value::String(id.to_string()));
            if crate::columnar::delete(
                wtxn,
                self.env,
                &self.name,
                &serde_json::json!({"_id": id_value}),
                false,
            )? == 0
            {
                self.set_ttl(wtxn, id, None)?;
                return Ok(None);
            }
            crate::columnar::failpoint("ttl_after_delete")?;
            if let Some(locator) = locator {
                crate::columnar::drop_empty_bucket(wtxn, self.env, &self.name, locator)?;
            }
            self.apply_index_entries(wtxn, Some(&old), None, id)?;
            self.apply_graph_entries(wtxn, Some(&old), None)?;
            self.apply_text_entries(wtxn, Some(&old), None, id)?;
            self.apply_fuzzy_entries(wtxn, Some(&old), None, id)?;
            self.apply_vector_entries(wtxn, Some(&old), None, id)?;
            self.set_ttl(wtxn, id, None)?;
            if self.ttl_purge_history {
                history::purge(&self.history, wtxn, &self.name, id)?;
            } else {
                self.record_history(wtxn, id, "delete", None)?;
            }
            return Ok(Some(old));
        }
        let Some(raw) = self.db.get(&*wtxn, id.as_bytes())? else {
            self.set_ttl(wtxn, id, None)?;
            return Ok(None);
        };
        let old: Value = parse_json(raw)?;
        self.db.delete(&mut *wtxn, id.as_bytes())?;
        if !self.profile.is_vault() {
            self.apply_index_entries(wtxn, Some(&old), None, id)?;
            self.apply_graph_entries(wtxn, Some(&old), None)?;
            self.apply_text_entries(wtxn, Some(&old), None, id)?;
            self.apply_fuzzy_entries(wtxn, Some(&old), None, id)?;
            self.apply_vector_entries(wtxn, Some(&old), None, id)?;
        }
        self.set_ttl(wtxn, id, None)?;
        if self.ttl_purge_history {
            history::purge(&self.history, wtxn, &self.name, id)?;
        } else {
            self.record_history(wtxn, id, "delete", None)?;
        }
        Ok(Some(old))
    }

    /// Reads vault documents. Only `_id`/`tenant_id` filters and `_id` sorting are allowed.
    pub fn find_vault_in(
        &self,
        rtxn: &RoTxn<'_>,
        query: &FindQuery,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<Vec<Value>> {
        self.ensure_vault()?;
        vault::validate_projection(query.projection.as_ref())?;
        vault::validate_sort(&query.sort)?;
        let filter = vault::read_filter(&query.filter, tenant)?;
        let stored_query = FindQuery {
            filter,
            projection: None,
            sort: query.sort.clone(),
            skip: query.skip,
            limit: query.limit,
            comment: query.comment.clone(),
            max_time_ms: query.max_time_ms,
            hint: query.hint.clone(),
        };
        let stored = self.find_in(rtxn, &stored_query)?;
        let mut out = Vec::with_capacity(stored.len());
        for item in stored {
            let id = item
                .get("_id")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Vault {
                    code: "decryption_failed",
                    message: "stored vault document has no `_id`".into(),
                })?;
            out.push(self.decrypt_stored(&item, key, tenant, id, kid)?);
        }
        Ok(out)
    }

    pub fn find_one_vault_in(
        &self,
        rtxn: &RoTxn<'_>,
        filter: &Value,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<Option<Value>> {
        let query = FindQuery {
            filter: filter.clone(),
            projection: None,
            sort: Vec::new(),
            skip: 0,
            limit: Some(1),
            comment: None,
            max_time_ms: None,
            hint: None,
        };
        Ok(self.find_vault_in(rtxn, &query, key, tenant, kid)?.pop())
    }

    /// Counts vault documents without decrypting them.
    pub fn count_vault_in(&self, rtxn: &RoTxn<'_>, filter: &Value, tenant: &str) -> Result<usize> {
        self.ensure_vault()?;
        let filter = vault::read_filter(filter, tenant)?;
        self.count_in(rtxn, &filter)
    }

    /// History for a vault document, with snapshots decrypted on read.
    #[allow(clippy::too_many_arguments)]
    pub fn history_vault_in(
        &self,
        rtxn: &RoTxn<'_>,
        id: &str,
        limit: Option<usize>,
        include_docs: bool,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<Vec<HistoryEntry>> {
        self.ensure_vault()?;
        let mut entries = self.history_in(rtxn, id, limit, include_docs)?;
        if include_docs {
            for entry in &mut entries {
                if let Some(stored) = entry.doc.take() {
                    entry.doc = Some(self.decrypt_stored(&stored, key, tenant, id, kid)?);
                }
            }
        }
        Ok(entries)
    }

    pub fn get_version_vault_in(
        &self,
        rtxn: &RoTxn<'_>,
        id: &str,
        rev: u64,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<Option<HistoryEntry>> {
        self.ensure_vault()?;
        let mut entry = self.get_version_in(rtxn, id, rev)?;
        if let Some(entry) = &mut entry {
            if let Some(stored) = entry.doc.take() {
                entry.doc = Some(self.decrypt_stored(&stored, key, tenant, id, kid)?);
            }
        }
        Ok(entry)
    }

    pub(crate) fn events_enabled_in(&self, wtxn: &RwTxn<'_>) -> Result<Option<EventConfig>> {
        if self.name == EVENTS_COLLECTION || self.profile.is_vault() {
            return Ok(None);
        }
        let key = format!("events:{}", self.name);
        let config = self
            .meta
            .get(wtxn, key.as_bytes())?
            .and_then(|raw| serde_json::from_slice::<EventConfig>(raw).ok())
            .unwrap_or_default();
        Ok(config.active().then_some(config))
    }

    pub(crate) fn append_change_event(
        &self,
        wtxn: &mut RwTxn<'_>,
        op: &str,
        id: &str,
        before: Option<&Value>,
        after: Option<&Value>,
    ) -> Result<()> {
        let _ = self.evaluate_subscriptions(wtxn, op, id, before, after)?;
        crate::observer::notify(self, wtxn, op, id, before, after)?;
        let Some(config) = self.events_enabled_in(wtxn)? else {
            return Ok(());
        };
        let Some(event) = events::change_event(&self.name, op, id, before, after, &config) else {
            return Ok(());
        };
        self.append_outbox_event(wtxn, event)
    }

    pub fn append_outbox_event(&self, wtxn: &mut RwTxn<'_>, event: Value) -> Result<()> {
        let db = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(EVENTS_COLLECTION))?;
        let outbox = Collection::new(
            None,
            self.env,
            db,
            self.meta,
            self.history,
            self.ttl_db,
            0,
            EVENTS_COLLECTION.to_string(),
            crate::vault::CollectionProfile::Standard,
            None,
            None,
            false,
            self.txn_gate,
        );
        outbox.insert_one_txn(wtxn, event.clone())?;
        crate::observer::notify_outbox(&event);
        Ok(())
    }

    pub fn has_subscriptions_in(&self, wtxn: &RwTxn<'_>) -> Result<bool> {
        if self.name == EVENTS_COLLECTION || self.profile.is_vault() {
            return Ok(false);
        }
        Ok(self
            .meta
            .get(wtxn, subscriptions::meta_key(&self.name).as_bytes())?
            .is_some())
    }

    pub fn evaluate_subscriptions(
        &self,
        wtxn: &mut RwTxn<'_>,
        op: &str,
        id: &str,
        before: Option<&Value>,
        after: Option<&Value>,
    ) -> Result<Vec<Value>> {
        if self.name == EVENTS_COLLECTION || self.profile.is_vault() {
            return Ok(Vec::new());
        }
        let key = subscriptions::meta_key(&self.name);
        let Some(raw) = self.meta.get(&*wtxn, key.as_bytes())? else {
            return Ok(Vec::new());
        };
        let mut list: Vec<Subscription> =
            serde_json::from_slice(raw).map_err(|e| Error::Query(format!("subscriptions: {e}")))?;
        let now = subscriptions::now_ms();
        let original_len = list.len();
        list.retain(|subscription| !subscription.is_expired(now));
        let mut changed = list.len() != original_len;
        if list.is_empty() {
            self.meta.delete(&mut *wtxn, key.as_bytes())?;
            return Ok(Vec::new());
        }
        let mut events = Vec::new();
        for subscription in &mut list {
            if !subscription.watches_op(op) {
                continue;
            }
            let condition_doc = if op == "delete" { before } else { after };
            let doc_matches = subscription.filter.is_null()
                || subscription
                    .filter
                    .as_object()
                    .is_some_and(|object| object.is_empty())
                || condition_doc
                    .is_some_and(|doc| crate::query::matches(doc, &subscription.filter));
            let (count_matches, count) = match &subscription.count {
                Some(condition) => {
                    let count = self.count_in(wtxn, &condition.filter)? as u64;
                    (condition.matches(count), Some(count))
                }
                None => (true, None),
            };
            let matched = doc_matches && count_matches;
            let should_emit = matched && (!subscription.once || !subscription.last_matched);
            if should_emit {
                events.push(subscription.event(&self.name, op, id, before, after, count));
            }
            if subscription.last_matched != matched {
                subscription.last_matched = matched;
                changed = true;
            }
        }
        for event in &events {
            self.append_outbox_event(wtxn, event.clone())?;
        }
        if changed {
            let raw = serde_json::to_vec(&list)?;
            self.meta.put(&mut *wtxn, key.as_bytes(), &raw)?;
        }
        Ok(events)
    }

    fn record_history(
        &self,
        wtxn: &mut RwTxn<'_>,
        id: &str,
        op: &str,
        doc: Option<Value>,
    ) -> Result<()> {
        if self.history_max_versions == 0 {
            return Ok(());
        }
        history::append(&self.history, &self.meta, wtxn, &self.name, id, op, doc)?;
        Ok(())
    }

    /// Reads document revisions, newest first.
    pub fn history(
        &self,
        id: &str,
        limit: Option<usize>,
        include_docs: bool,
    ) -> Result<Vec<HistoryEntry>> {
        let rtxn = self.read_txn()?;
        let out = self.history_in(&rtxn, id, limit, include_docs)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn history_in(
        &self,
        rtxn: &RoTxn<'_>,
        id: &str,
        limit: Option<usize>,
        include_docs: bool,
    ) -> Result<Vec<HistoryEntry>> {
        history::list(&self.history, rtxn, &self.name, id, limit, include_docs)
    }

    /// Reads one exact global revision.
    pub fn get_version(&self, id: &str, rev: u64) -> Result<Option<HistoryEntry>> {
        let rtxn = self.read_txn()?;
        let out = self.get_version_in(&rtxn, id, rev)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn get_version_in(
        &self,
        rtxn: &RoTxn<'_>,
        id: &str,
        rev: u64,
    ) -> Result<Option<HistoryEntry>> {
        history::get(&self.history, rtxn, &self.name, id, rev)
    }

    /// Collects keys (`_id`) matching the filter.
    ///
    /// Uses the same fast paths as `find`: exact `_id`,
    /// secondary index, and only then a full scan.
    fn matching_keys(&self, txn: &RoTxn<'_>, filter: &Value, multi: bool) -> Result<Vec<Vec<u8>>> {
        crate::query::validate::validate_filter(filter)?;
        let mut keys = Vec::new();
        let raw = raw_prefilter(filter);

        if let Some(id) = direct_id(filter) {
            if self.db.get(txn, id.as_bytes())?.is_some() {
                keys.push(id.as_bytes().to_vec());
            }
            return Ok(keys);
        }

        if let Some(ids) = direct_ids_in(filter) {
            let residual = Self::residual_without_in(filter).unwrap_or_else(|| filter.clone());
            for id in ids {
                let Some(bytes) = self.db.get(txn, id.as_bytes())? else {
                    continue;
                };
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, &residual) {
                    keys.push(id.into_bytes());
                    if !multi {
                        break;
                    }
                }
            }
            return Ok(keys);
        }

        if let Some(plans) = self.or_index_plans(txn, filter)? {
            let residual = Self::residual_without_in(filter).unwrap_or_else(|| filter.clone());
            let mut seen = HashSet::new();
            'plans: for plan in plans {
                let (lower, upper) = &plan.bounds;
                let range = (as_bound(lower), as_bound(upper));
                for item in plan.db.range(txn, &range)? {
                    let (key, value) = item?;
                    let Some((id, _)) = index::decode_index_entry(key, value) else {
                        continue;
                    };
                    if !seen.insert(id.clone()) {
                        continue;
                    }
                    let Some(bytes) = self.db.get(txn, id.as_bytes())? else {
                        continue;
                    };
                    let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                        continue;
                    };
                    if matches(&doc, &residual) {
                        keys.push(id.into_bytes());
                        if !multi {
                            break 'plans;
                        }
                    }
                }
            }
            return Ok(keys);
        }

        if let Some(ids) = self.index_candidates(txn, filter, None)? {
            for id in ids {
                let Some(bytes) = self.db.get(txn, id.as_bytes())? else {
                    continue;
                };
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, filter) {
                    keys.push(id.into_bytes());
                    if !multi {
                        break;
                    }
                }
            }
            return Ok(keys);
        }

        if let Some((lower, upper)) = id_bounds(filter) {
            let range = (as_bound(&lower), as_bound(&upper));
            for item in self.db.range(txn, &range)? {
                let (key, bytes) = item?;
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, filter) {
                    keys.push(key.to_vec());
                    if !multi {
                        break;
                    }
                }
            }
            return Ok(keys);
        }

        for item in self.db.iter(txn)? {
            let (key, bytes) = item?;
            let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                continue;
            };
            if matches(&doc, filter) {
                keys.push(key.to_vec());
                if !multi {
                    break;
                }
            }
        }
        Ok(keys)
    }

    /// Streams documents matching a filter through `f`, using the same
    /// access paths as `find`/`matching_keys` but without collecting all
    /// results first. This keeps a leading aggregation `$match` index-backed
    /// while allowing a streaming `$group` to consume an unbounded input.
    fn for_each_matching<F>(
        &self,
        txn: &RoTxn<'_>,
        filter: &Value,
        deadline: Option<Instant>,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(Value) -> Result<()>,
    {
        crate::query::validate::validate_filter(filter)?;
        check_find_deadline(deadline)?;
        let raw = raw_prefilter(filter);
        let strict_ttl = self.ttl_filter_active(txn)?;
        let now = ttl::now_ms();
        let mut f = f;
        let mut emit = |doc: Value| -> Result<()> {
            if strict_ttl && !self.ttl_doc_visible(txn, &doc, now)? {
                return Ok(());
            }
            f(doc)
        };

        if let Some(id) = direct_id(filter) {
            if let Some(bytes) = self.db.get(txn, id.as_bytes())? {
                emit(parse_json(bytes)?)?;
            }
            return Ok(());
        }

        if let Some(ids) = direct_ids_in(filter) {
            let residual = Self::residual_without_in(filter).unwrap_or_else(|| filter.clone());
            for id in ids {
                check_find_deadline(deadline)?;
                let Some(bytes) = self.db.get(txn, id.as_bytes())? else {
                    continue;
                };
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, &residual) {
                    emit(doc)?;
                }
            }
            return Ok(());
        }

        if let Some(ids) = self.all_index_candidates(txn, filter)? {
            check_find_deadline(deadline)?;
            for id in ids {
                check_find_deadline(deadline)?;
                let Some(bytes) = self.db.get(txn, id.as_bytes())? else {
                    continue;
                };
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, filter) {
                    emit(doc)?;
                }
            }
            return Ok(());
        }

        if let Some(ids) = self.geo_index_candidates(txn, filter)? {
            check_find_deadline(deadline)?;
            for id in ids {
                check_find_deadline(deadline)?;
                let Some(bytes) = self.db.get(txn, id.as_bytes())? else {
                    continue;
                };
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, filter) {
                    emit(doc)?;
                }
            }
            return Ok(());
        }

        if let Some(plans) = self.or_index_plans(txn, filter)? {
            let residual = Self::residual_without_in(filter).unwrap_or_else(|| filter.clone());
            let mut seen = HashSet::new();
            for plan in plans {
                let (lower, upper) = &plan.bounds;
                let range = (as_bound(lower), as_bound(upper));
                for item in plan.db.range(txn, &range)? {
                    check_find_deadline(deadline)?;
                    let (key, value) = item?;
                    let Some((id, _)) = index::decode_index_entry(key, value) else {
                        continue;
                    };
                    if !seen.insert(id.clone()) {
                        continue;
                    }
                    let Some(bytes) = self.db.get(txn, id.as_bytes())? else {
                        continue;
                    };
                    let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                        continue;
                    };
                    if matches(&doc, &residual) {
                        emit(doc)?;
                    }
                }
            }
            return Ok(());
        }

        if let Some(plan) = self.index_plan(txn, filter)? {
            let (lower, upper) = &plan.bounds;
            let range = (as_bound(lower), as_bound(upper));
            let multikey = plan.def.is_multikey();
            let mut seen = HashSet::new();
            for item in plan.db.range(txn, &range)? {
                check_find_deadline(deadline)?;
                let (key, value) = item?;
                let Some((id, _)) = index::decode_index_entry(key, value) else {
                    continue;
                };
                if multikey && !seen.insert(id.clone()) {
                    continue;
                }
                let Some(bytes) = self.db.get(txn, id.as_bytes())? else {
                    continue;
                };
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, filter) {
                    emit(doc)?;
                }
            }
            return Ok(());
        }

        if let Some((lower, upper)) = id_bounds(filter) {
            let range = (as_bound(&lower), as_bound(&upper));
            for item in self.db.range(txn, &range)? {
                check_find_deadline(deadline)?;
                let (_key, bytes) = item?;
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, filter) {
                    emit(doc)?;
                }
            }
            return Ok(());
        }

        for item in self.db.iter(txn)? {
            check_find_deadline(deadline)?;
            let (_key, bytes) = item?;
            let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                continue;
            };
            if matches(&doc, filter) {
                emit(doc)?;
            }
        }
        Ok(())
    }

    // ----------------------------- read ------------------------------

    /// Runs a read-only aggregation pipeline over this collection.
    ///
    /// A safe prefix (`$match`, `$sort`, `$skip`, `$limit`, simple `$project`)
    /// is pushed into the normal find path so indexes, `_id` ranges, covering
    /// projections, and query-level TopK can be used. A lone `$count` becomes a
    /// `count` call instead of materializing documents. Intermediate results
    /// are capped by [`aggregate::MAX_AGGREGATE_DOCS`].
    pub fn aggregate(&self, pipeline: &[Value]) -> Result<Vec<Value>> {
        self.aggregate_with_options(pipeline, None)
    }

    pub fn aggregate_with_options(
        &self,
        pipeline: &[Value],
        max_time_ms: Option<u64>,
    ) -> Result<Vec<Value>> {
        let rtxn = self.read_txn()?;
        let out = self.aggregate_in_with_options(&rtxn, pipeline, max_time_ms);
        rtxn.commit()?;
        out
    }

    pub fn aggregate_in(&self, rtxn: &RoTxn<'_>, pipeline: &[Value]) -> Result<Vec<Value>> {
        self.aggregate_in_with_options(rtxn, pipeline, None)
    }

    fn timeseries_aggregate_streamable(pipeline: &[Value]) -> bool {
        !pipeline.is_empty()
            && pipeline.iter().all(|stage| {
                stage.as_object().is_some_and(|object| {
                    object.keys().all(|key| {
                        matches!(
                            key.as_str(),
                            "$match"
                                | "$project"
                                | "$addFields"
                                | "$set"
                                | "$unset"
                                | "$replaceRoot"
                                | "$replaceWith"
                                | "$lookup"
                                | "$unwind"
                        )
                    })
                })
            })
    }

    pub fn aggregate_in_with_options(
        &self,
        rtxn: &RoTxn<'_>,
        pipeline: &[Value],
        max_time_ms: Option<u64>,
    ) -> Result<Vec<Value>> {
        let deadline = operation_deadline(max_time_ms);
        self.aggregate_in_deadline(rtxn, pipeline, deadline)
    }

    fn try_timeseries_rollup(
        &self,
        rtxn: &RoTxn<'_>,
        pipeline: &[Value],
        settings: &crate::timeseries::Settings,
    ) -> Result<Option<Vec<Value>>> {
        if std::env::var("NONODB_TS_DISABLE_ROLLUPS").ok().as_deref() == Some("1") {
            return Ok(None);
        }
        let Some(group_at) = pipeline
            .iter()
            .position(|stage| stage.get("$group").is_some())
        else {
            return Ok(None);
        };
        let prefix = &pipeline[..group_at];
        let mut filter = Value::Object(Map::new());
        for stage in prefix {
            let Some(object) = stage.as_object() else {
                return Ok(None);
            };
            if object.len() != 1 {
                return Ok(None);
            }
            if let Some(matched) = object.get("$match") {
                filter = matched.clone();
            } else if object.get("$sort").is_none() {
                return Ok(None);
            }
        }
        if !crate::timeseries::filter_is_meta_time_only(&filter, settings) {
            return Ok(None);
        }
        let Some(group) = pipeline[group_at].get("$group").and_then(Value::as_object) else {
            return Ok(None);
        };
        let Some(trunc) = group.get("_id").and_then(|id| {
            id.get("bucket")
                .and_then(|bucket| bucket.get("$dateTrunc"))
                .or_else(|| id.get("$dateTrunc"))
        }) else {
            return Ok(None);
        };
        let Some((unit, _unit_byte, _window_size)) =
            crate::timeseries::rollup_spec_for_trunc(trunc)
        else {
            return Ok(None);
        };
        for (field, accumulator) in group {
            if field == "_id" {
                continue;
            }
            let Some(accumulator) = accumulator.as_object() else {
                return Ok(None);
            };
            if !accumulator
                .keys()
                .all(|op| matches!(op.as_str(), "$first" | "$last" | "$max" | "$min" | "$sum"))
            {
                return Ok(None);
            }
        }
        let Some((mut docs, partial)) =
            crate::timeseries::rollup_docs(rtxn, self.env, &self.name, settings, &filter, unit)?
        else {
            return Ok(None);
        };
        for (start, end) in partial {
            let partial_filter = json!({
                "$and": [
                    filter.clone(),
                    {"ts": {
                        "$gte": {"$date": {"$numberLong": start.to_string()}},
                        "$lte": {"$date": {"$numberLong": end.to_string()}}
                    }}
                ]
            });
            let mut hour_partial = Vec::new();
            if unit == "1d" {
                if let Some((hour_docs, partials)) = crate::timeseries::rollup_docs(
                    rtxn,
                    self.env,
                    &self.name,
                    settings,
                    &partial_filter,
                    "1h",
                )? {
                    docs.extend(hour_docs);
                    hour_partial = partials;
                    if hour_partial.is_empty() {
                        continue;
                    }
                }
            }
            let ranges = if hour_partial.is_empty() {
                vec![(start, end)]
            } else {
                hour_partial
            };
            for (start, end) in ranges {
                let partial_filter = json!({
                    "$and": [
                        filter.clone(),
                        {"ts": {
                            "$gte": {"$date": {"$numberLong": start.to_string()}},
                            "$lte": {"$date": {"$numberLong": end.to_string()}}
                        }}
                    ]
                });
                let raw = crate::timeseries::find(
                    rtxn,
                    self.env,
                    &self.name,
                    settings,
                    &FindQuery {
                        filter: partial_filter,
                        projection: None,
                        sort: Vec::new(),
                        skip: 0,
                        limit: None,
                        comment: None,
                        max_time_ms: None,
                        hint: None,
                    },
                )?;
                for mut doc in raw {
                    if let Some(object) = doc.as_object_mut() {
                        object.insert("n".into(), Value::from(1));
                    }
                    docs.push(doc);
                }
            }
        }
        docs.sort_by(|a, b| {
            let ta = crate::timeseries::extract_time_public(a, &settings.time_field).unwrap_or(0);
            let tb = crate::timeseries::extract_time_public(b, &settings.time_field).unwrap_or(0);
            ta.cmp(&tb)
        });
        let mut rewritten = Map::new();
        for (field, accumulator) in group {
            let mut accumulator = accumulator.clone();
            if field != "_id" {
                if let Some(object) = accumulator.as_object_mut() {
                    if object.get("$sum") == Some(&Value::from(1)) {
                        object.insert("$sum".into(), Value::String("$n".into()));
                    }
                }
            }
            rewritten.insert(field.clone(), accumulator);
        }
        let mut stages = vec![json!({"$group": Value::Object(rewritten)})];
        stages.extend_from_slice(&pipeline[group_at + 1..]);
        let host = LookupHost { coll: self, rtxn };
        Ok(Some(aggregate::run_with_host(
            docs,
            &stages,
            aggregate::max_docs(),
            Some(&host),
        )?))
    }

    fn timeseries_window_docs(
        &self,
        rtxn: &RoTxn<'_>,
        settings: &crate::timeseries::Settings,
        filter: &Value,
    ) -> Result<Vec<Value>> {
        let query = FindQuery {
            filter: filter.clone(),
            projection: None,
            sort: Vec::new(),
            skip: 0,
            limit: None,
            comment: None,
            max_time_ms: None,
            hint: None,
        };
        crate::timeseries::find_window_docs(rtxn, self.env, &self.name, settings, &query)
    }

    fn aggregate_in_deadline(
        &self,
        rtxn: &RoTxn<'_>,
        pipeline: &[Value],
        deadline: Option<Instant>,
    ) -> Result<Vec<Value>> {
        if self.profile.is_timeseries() {
            // Time-series buckets are decoded through the normal find path so
            // bucket pruning and residual filters still apply, then the regular
            // in-memory pipeline runs on the decoded documents. This enables
            // `$group`/`$bucket` downsampling without a second query engine.
            check_find_deadline(deadline)?;
            aggregate::validate_pipeline(pipeline)?;
            let settings = self.timeseries_settings_in(rtxn)?.unwrap_or_default();
            if let Some(out) = self.try_timeseries_rollup(rtxn, pipeline, &settings)? {
                return Ok(out);
            }
            if let Some(out) = crate::timeseries::try_columnar_group(
                rtxn, self.env, &self.name, &settings, pipeline,
            )? {
                return Ok(out);
            }
            let filter = pipeline
                .first()
                .and_then(|stage| stage.get("$match"))
                .cloned()
                .unwrap_or_else(|| Value::Object(Map::new()));
            let query = FindQuery {
                filter,
                projection: None,
                sort: Vec::new(),
                skip: 0,
                limit: None,
                comment: None,
                max_time_ms: None,
                hint: None,
            };
            if let Some(stage) = pipeline.first().and_then(Value::as_object) {
                let host = LookupHost { coll: self, rtxn };
                if let Some(spec) = stage.get("$search") {
                    let docs = crate::search_compat::search_stage_in(self, rtxn, spec, 1000)?;
                    let out = aggregate::run_with_host(
                        docs,
                        &pipeline[1..],
                        aggregate::max_docs(),
                        Some(&host),
                    )?;
                    return Ok(out);
                }
                if let Some(spec) = stage.get("$searchMeta") {
                    let meta = crate::search_compat::search_meta_stage_in(self, rtxn, spec)?;
                    let out = aggregate::run_with_host(
                        vec![meta],
                        &pipeline[1..],
                        aggregate::max_docs(),
                        Some(&host),
                    )?;
                    return Ok(out);
                }
                if let Some(spec) = stage.get("$vectorSearch") {
                    let docs = crate::search_compat::vector_search_stage_in(self, rtxn, spec)?;
                    let out = aggregate::run_with_host(
                        docs,
                        &pipeline[1..],
                        aggregate::max_docs(),
                        Some(&host),
                    )?;
                    return Ok(out);
                }
            }
            if let Some(group_at) = pipeline
                .iter()
                .position(|stage| stage.get("$group").is_some())
            {
                let prefix = &pipeline[..group_at];
                let tail = &pipeline[group_at + 1..];
                let streamable_prefix = prefix.iter().all(|stage| {
                    Self::timeseries_aggregate_streamable(std::slice::from_ref(stage))
                });
                if streamable_prefix {
                    if let Some(spec) = pipeline[group_at].get("$group") {
                        if let Ok(builder) = aggregate::GroupBuilder::new(spec) {
                            {
                                let mut builder = builder;
                                let batch_size = std::env::var("NONODB_TS_AGG_BATCH")
                                    .ok()
                                    .and_then(|value| value.parse::<usize>().ok())
                                    .unwrap_or(4096);
                                let host = LookupHost { coll: self, rtxn };
                                crate::timeseries::scan_batches(
                                    rtxn,
                                    self.env,
                                    &self.name,
                                    &settings,
                                    &query.filter,
                                    batch_size,
                                    |batch| {
                                        let docs = if prefix.is_empty() {
                                            batch
                                        } else {
                                            aggregate::run_with_host(
                                                batch,
                                                prefix,
                                                aggregate::max_docs(),
                                                Some(&host),
                                            )?
                                        };
                                        for doc in docs {
                                            builder.update(doc)?;
                                        }
                                        Ok(true)
                                    },
                                )?;
                                let groups = builder.finish()?;
                                return aggregate::run_with_host(
                                    groups,
                                    tail,
                                    aggregate::max_docs(),
                                    Some(&host),
                                );
                            }
                        }
                    }
                }
            }
            if Self::timeseries_aggregate_streamable(pipeline) {
                let batch_size = std::env::var("NONODB_TS_AGG_BATCH")
                    .ok()
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(4096);
                let host = LookupHost { coll: self, rtxn };
                let limit = aggregate::max_docs();
                let mut out = Vec::new();
                crate::timeseries::scan_batches(
                    rtxn,
                    self.env,
                    &self.name,
                    &settings,
                    &query.filter,
                    batch_size,
                    |batch| {
                        let mut part =
                            aggregate::run_with_host(batch, pipeline, limit, Some(&host))?;
                        if out.len() + part.len() > limit {
                            part.truncate(limit.saturating_sub(out.len()));
                        }
                        let full = out.len() + part.len() >= limit;
                        out.extend(part);
                        Ok(!full)
                    },
                )?;
                return Ok(out);
            }
            let host = LookupHost { coll: self, rtxn };
            let docs = if pipeline
                .iter()
                .any(|stage| stage.get("$setWindowFields").is_some())
            {
                self.timeseries_window_docs(rtxn, &settings, &query.filter)?
            } else {
                crate::timeseries::find(rtxn, self.env, &self.name, &settings, &query)?
            };
            return aggregate::run_with_host(docs, pipeline, aggregate::max_docs(), Some(&host));
        }
        check_find_deadline(deadline)?;
        aggregate::validate_pipeline(pipeline)?;
        if let Some(stage) = pipeline.first().and_then(Value::as_object) {
            if let Some(spec) = stage.get("$search") {
                let docs = crate::search_compat::search_stage_in(self, rtxn, spec, 1000)?;
                let host = LookupHost { coll: self, rtxn };
                let out = aggregate::run_with_host(
                    docs,
                    &pipeline[1..],
                    aggregate::max_docs(),
                    Some(&host),
                )?;
                check_find_deadline(deadline)?;
                return Ok(out);
            }
            if let Some(spec) = stage.get("$searchMeta") {
                let meta = crate::search_compat::search_meta_stage_in(self, rtxn, spec)?;
                let host = LookupHost { coll: self, rtxn };
                let out = aggregate::run_with_host(
                    vec![meta],
                    &pipeline[1..],
                    aggregate::max_docs(),
                    Some(&host),
                )?;
                check_find_deadline(deadline)?;
                return Ok(out);
            }
            if let Some(spec) = stage.get("$vectorSearch") {
                let docs = crate::search_compat::vector_search_stage_in(self, rtxn, spec)?;
                let host = LookupHost { coll: self, rtxn };
                let out = aggregate::run_with_host(
                    docs,
                    &pipeline[1..],
                    aggregate::max_docs(),
                    Some(&host),
                )?;
                check_find_deadline(deadline)?;
                return Ok(out);
            }
        }
        if self.profile.is_columnar() {
            let plan = aggregate::pushdown(pipeline)?;
            check_find_deadline(deadline)?;
            if let Some(field) = plan.count {
                let count = self.count_in_deadline(rtxn, &plan.filter, deadline)?;
                if count == 0 {
                    return Ok(Vec::new());
                }
                return Ok(vec![Value::Object(Map::from_iter([(
                    field,
                    Value::from(count as u64),
                )]))]);
            }
            if let Some(field) = aggregate::terminal_count_skippable(&pipeline[plan.next..])? {
                let count = self.count_in_deadline(rtxn, &plan.filter, deadline)?;
                let after_skip = count.saturating_sub(plan.skip);
                let count = plan.limit.map_or(after_skip, |limit| after_skip.min(limit));
                if count == 0 {
                    return Ok(Vec::new());
                }
                return Ok(vec![Value::Object(Map::from_iter([(
                    field,
                    Value::from(count as u64),
                )]))]);
            }
            let remaining = &pipeline[plan.next..];
            if plan.sort.is_empty()
                && plan.skip == 0
                && plan.limit.is_none()
                && plan.projection.is_none()
                && !self.ttl_filter_active(rtxn)?
            {
                if let Some(group_arg) = remaining
                    .first()
                    .and_then(Value::as_object)
                    .filter(|obj| obj.len() == 1)
                    .and_then(|obj| obj.get("$group"))
                {
                    let builder = aggregate::GroupBuilder::new(group_arg)?;
                    if builder.order_independent() {
                        let groups = crate::columnar::group_parallel(
                            rtxn,
                            self.env,
                            &self.name,
                            &plan.filter,
                            group_arg,
                            aggregate_workers(),
                        )?;
                        check_find_deadline(deadline)?;
                        let host = LookupHost { coll: self, rtxn };
                        let out = aggregate::run_with_host(
                            groups,
                            &remaining[1..],
                            aggregate::max_docs(),
                            Some(&host),
                        )?;
                        check_find_deadline(deadline)?;
                        return Ok(out);
                    }
                }
            }
            let max_docs = aggregate::max_docs();
            let limit = plan
                .limit
                .map(|n| n.min(max_docs.saturating_add(1)))
                .unwrap_or_else(|| max_docs.saturating_add(1));
            let docs = self.find_in(
                rtxn,
                &FindQuery {
                    filter: plan.filter,
                    projection: plan.projection,
                    sort: plan.sort,
                    skip: plan.skip,
                    limit: Some(limit),
                    comment: None,
                    max_time_ms: deadline_remaining_ms(deadline),
                    hint: None,
                },
            )?;
            if docs.len() > max_docs {
                return Err(Error::Query(format!(
                    "aggregation input exceeds {max_docs} documents"
                )));
            }
            let host = LookupHost { coll: self, rtxn };
            let out =
                aggregate::run_with_host(docs, &pipeline[plan.next..], max_docs, Some(&host))?;
            check_find_deadline(deadline)?;
            return Ok(out);
        }
        let plan = aggregate::pushdown(pipeline)?;
        check_find_deadline(deadline)?;

        if let Some(field) = plan.count {
            let count = self.count_in_deadline(rtxn, &plan.filter, deadline)?;
            if count == 0 {
                return Ok(Vec::new());
            }
            return Ok(vec![Value::Object(Map::from_iter([(
                field,
                Value::from(count as u64),
            )]))]);
        }

        if let Some(field) = aggregate::terminal_count_skippable(&pipeline[plan.next..])? {
            let count = self.count_in_deadline(rtxn, &plan.filter, deadline)?;
            let after_skip = count.saturating_sub(plan.skip);
            let count = plan.limit.map_or(after_skip, |limit| after_skip.min(limit));
            if count == 0 {
                return Ok(Vec::new());
            }
            return Ok(vec![Value::Object(Map::from_iter([(
                field,
                Value::from(count as u64),
            )]))]);
        }

        let max_docs = aggregate::max_docs();
        let host = LookupHost { coll: self, rtxn };
        let remaining = &pipeline[plan.next..];

        // A pipeline whose remaining prefix is just `$group` does not need an
        // intermediate vector of every input document. Feed documents directly
        // into the group accumulators in `_id` order.
        if plan.sort.is_empty()
            && plan.skip == 0
            && plan.limit.is_none()
            && plan.projection.is_none()
        {
            if let Some(group_arg) = remaining
                .first()
                .and_then(Value::as_object)
                .filter(|obj| obj.len() == 1)
                .and_then(|obj| obj.get("$group"))
            {
                let mut builder = aggregate::GroupBuilder::new(group_arg)?;
                let grouped = if builder.order_independent() {
                    let total = self.db.stat(rtxn)?.entries;
                    let parallel = !self.ttl_filter_active(rtxn)?
                        && total >= PARALLEL_SCAN_MIN_ENTRIES
                        && aggregate_workers() > 1
                        && !self.has_index_path(rtxn, &plan.filter)?;
                    if parallel {
                        self.parallel_group_scan(rtxn, &plan.filter, group_arg, deadline)?
                    } else {
                        self.for_each_matching(rtxn, &plan.filter, deadline, |doc| {
                            builder.update(doc)
                        })?;
                        builder.finish()?
                    }
                } else {
                    // `$first`/`$last`/`$push` depend on the natural `_id` order.
                    let strict_ttl = self.ttl_filter_active(rtxn)?;
                    let now = ttl::now_ms();
                    for item in self.db.iter(rtxn)? {
                        check_find_deadline(deadline)?;
                        let (_key, raw) = item?;
                        let doc = parse_json(raw)?;
                        if matches(&doc, &plan.filter)
                            && (!strict_ttl || self.ttl_doc_visible(rtxn, &doc, now)?)
                        {
                            builder.update(doc)?;
                        }
                    }
                    builder.finish()?
                };
                if grouped.len() > max_docs {
                    return Err(Error::Query(format!(
                        "aggregation intermediate result exceeds {max_docs} documents"
                    )));
                }
                let out =
                    aggregate::run_with_host(grouped, &remaining[1..], max_docs, Some(&host))?;
                check_find_deadline(deadline)?;
                return Ok(out);
            }
        }

        let limit = plan
            .limit
            .map(|n| n.min(max_docs.saturating_add(1)))
            .unwrap_or_else(|| max_docs.saturating_add(1));
        let query = FindQuery {
            filter: plan.filter,
            projection: plan.projection,
            sort: plan.sort,
            skip: plan.skip,
            limit: Some(limit),
            comment: None,
            max_time_ms: deadline_remaining_ms(deadline),
            hint: None,
        };
        let docs = self.find_in(rtxn, &query)?;
        if docs.len() > max_docs {
            return Err(Error::Query(format!(
                "aggregation input exceeds {max_docs} documents"
            )));
        }
        let out = aggregate::run_with_host(docs, &pipeline[plan.next..], max_docs, Some(&host))?;
        check_find_deadline(deadline)?;
        Ok(out)
    }

    fn lookup_stage_in(
        &self,
        rtxn: &RoTxn<'_>,
        docs: Vec<Value>,
        spec: &Value,
        vars: &BTreeMap<String, Value>,
    ) -> Result<Vec<Value>> {
        let Some(spec) = spec.as_object() else {
            return Err(Error::Query("$lookup requires an object".into()));
        };
        let let_vars = match spec.get("let") {
            Some(value) => Some(
                value
                    .as_object()
                    .ok_or_else(|| Error::Query("$lookup `let` must be an object".into()))?,
            ),
            None => None,
        };
        let from = spec
            .get("from")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Query("$lookup requires `from`".into()))?;
        let as_field = spec
            .get("as")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Query("$lookup requires `as`".into()))?;
        let local = spec.get("localField").and_then(Value::as_str);
        let foreign = spec.get("foreignField").and_then(Value::as_str);
        if local.is_some() != foreign.is_some() {
            return Err(Error::Query(
                "$lookup requires both localField and foreignField".into(),
            ));
        }
        let pipeline = spec.get("pipeline").and_then(Value::as_array);
        if local.is_none() && pipeline.is_none() {
            return Err(Error::Query(
                "$lookup requires localField/foreignField or a pipeline".into(),
            ));
        }
        let store = self
            .store
            .ok_or_else(|| Error::Query("$lookup requires a store-backed collection".into()))?;
        let foreign_coll = store.collection_at(rtxn, from)?;
        let max_docs = aggregate::max_docs();
        let host = LookupHost { coll: self, rtxn };
        let mut out = Vec::with_capacity(docs.len());
        for mut doc in docs {
            let mut doc_vars = vars.clone();
            if let Some(let_vars) = let_vars {
                for (name, expression) in let_vars {
                    doc_vars.insert(
                        name.clone(),
                        aggregate::eval_expr_vars(&doc, expression, vars)?,
                    );
                }
            }
            let filter = match (local, foreign) {
                (Some(local), Some(foreign)) => {
                    let mut values = Vec::new();
                    for value in path::get_values(&doc, local) {
                        match value {
                            Value::Array(items) => values.extend(items.iter().cloned()),
                            value => values.push(value.clone()),
                        }
                    }
                    let condition = if values.is_empty() {
                        Value::Null
                    } else if values.len() == 1 {
                        values.pop().expect("one value")
                    } else {
                        Value::Object(Map::from_iter([("$in".to_string(), Value::Array(values))]))
                    };
                    Value::Object(Map::from_iter([(foreign.to_string(), condition)]))
                }
                _ => Value::Object(Map::new()),
            };
            let mut matched = match &foreign_coll {
                Some(foreign_coll) => foreign_coll.find_in(
                    rtxn,
                    &FindQuery {
                        filter,
                        limit: Some(max_docs.saturating_add(1)),
                        ..Default::default()
                    },
                )?,
                None => Vec::new(),
            };
            if matched.len() > max_docs {
                return Err(Error::Query(format!(
                    "$lookup result exceeds {max_docs} documents"
                )));
            }
            if let Some(pipeline) = pipeline {
                matched = aggregate::run_with_host_vars(
                    matched,
                    pipeline,
                    max_docs,
                    Some(&host),
                    &doc_vars,
                )?;
            }
            path::set(&mut doc, as_field, Value::Array(matched))?;
            out.push(doc);
        }
        Ok(out)
    }

    fn graph_lookup_stage_in(
        &self,
        rtxn: &RoTxn<'_>,
        docs: Vec<Value>,
        spec: &Value,
        vars: &BTreeMap<String, Value>,
    ) -> Result<Vec<Value>> {
        let spec = spec
            .as_object()
            .ok_or_else(|| Error::Query("$graphLookup requires an object".into()))?;
        let from = spec
            .get("from")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Query("$graphLookup requires `from`".into()))?;
        let start_with = spec
            .get("startWith")
            .ok_or_else(|| Error::Query("$graphLookup requires `startWith`".into()))?;
        let connect_from = spec
            .get("connectFromField")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Query("$graphLookup requires `connectFromField`".into()))?;
        let connect_to = spec
            .get("connectToField")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Query("$graphLookup requires `connectToField`".into()))?;
        let as_field = spec
            .get("as")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Query("$graphLookup requires `as`".into()))?;
        let max_depth = match spec.get("maxDepth") {
            Some(value) => Some(
                value
                    .as_u64()
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or_else(|| {
                        Error::Query(
                            "$graphLookup `maxDepth` must be a non-negative integer".into(),
                        )
                    })?,
            ),
            None => None,
        };
        let depth_field = match spec.get("depthField") {
            Some(value) => Some(
                value
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        Error::Query("$graphLookup `depthField` must be a non-empty string".into())
                    })?,
            ),
            None => None,
        };
        let restrict = match spec.get("restrictSearchWithMatch") {
            Some(value) => Some(
                value
                    .as_object()
                    .ok_or_else(|| {
                        Error::Query(
                            "$graphLookup `restrictSearchWithMatch` must be an object".into(),
                        )
                    })?
                    .clone(),
            ),
            None => None,
        };
        let store = self.store.ok_or_else(|| {
            Error::Query("$graphLookup requires a store-backed collection".into())
        })?;
        let Some(foreign) = store.collection_at(rtxn, from)? else {
            let mut out = docs;
            for doc in &mut out {
                path::set(doc, as_field, Value::Array(Vec::new()))?;
            }
            return Ok(out);
        };
        let max_docs = aggregate::max_docs();
        if restrict.is_none() {
            let fast_def = match foreign.edge_graph_def_in(rtxn)? {
                Some(def)
                    if def.node_collection == self.name
                        && def.edge_collection == foreign.name
                        && def.directed
                        && def.from_field == connect_from
                        && def.to_field == connect_to =>
                {
                    Some(def)
                }
                _ => None,
            };
            if let Some(def) = fast_def {
                if let Some(out) = self.graph_lookup_adjacency(
                    rtxn,
                    &foreign,
                    &def,
                    docs.clone(),
                    start_with,
                    connect_from,
                    as_field,
                    max_depth,
                    depth_field,
                    vars,
                )? {
                    return Ok(out);
                }
            }
        }
        let mut out = Vec::with_capacity(docs.len());
        for mut doc in docs {
            let start = aggregate::eval_expr_vars(&doc, start_with, vars)?;
            let mut values = Vec::new();
            let mut value_keys = HashSet::new();
            graph_push_value(&start, &mut values, &mut value_keys);
            let mut results = Vec::new();
            let mut seen = HashSet::new();
            let mut depth = 0usize;
            loop {
                if values.is_empty() {
                    break;
                }
                let base = Value::Object(Map::from_iter([(
                    connect_to.to_string(),
                    Value::Object(Map::from_iter([("$in".to_string(), Value::Array(values))])),
                )]));
                let filter = if let Some(restrict) = &restrict {
                    Value::Object(Map::from_iter([(
                        "$and".to_string(),
                        Value::Array(vec![base, Value::Object(restrict.clone())]),
                    )]))
                } else {
                    base
                };
                let matched = foreign.find_in(
                    rtxn,
                    &FindQuery {
                        filter,
                        limit: Some(max_docs.saturating_add(1)),
                        ..Default::default()
                    },
                )?;
                let mut next_values = Vec::new();
                let mut next_keys = HashSet::new();
                for mut candidate in matched {
                    let key = graph_identity(&candidate);
                    if !seen.insert(key) {
                        continue;
                    }
                    let next = graph_collect_field(&candidate, connect_from);
                    if let Some(depth_field) = depth_field {
                        path::set(&mut candidate, depth_field, Value::from(depth as u64))?;
                    }
                    results.push(candidate);
                    if results.len() > max_docs {
                        return Err(Error::Query(format!(
                            "$graphLookup result exceeds {max_docs} documents"
                        )));
                    }
                    for value in next {
                        graph_push_value(&value, &mut next_values, &mut next_keys);
                    }
                }
                if max_depth.is_some_and(|max_depth| depth >= max_depth) {
                    break;
                }
                values = next_values;
                depth = depth.saturating_add(1);
            }
            path::set(&mut doc, as_field, Value::Array(results))?;
            out.push(doc);
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_lookup_adjacency(
        &self,
        rtxn: &RoTxn<'_>,
        foreign: &Collection<'_>,
        def: &GraphDef,
        docs: Vec<Value>,
        start_with: &Value,
        connect_from: &str,
        as_field: &str,
        max_depth: Option<usize>,
        depth_field: Option<&str>,
        vars: &BTreeMap<String, Value>,
    ) -> Result<Option<Vec<Value>>> {
        #[cfg(test)]
        GRAPH_LOOKUP_FAST_PATH_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let Some(store) = self.store else {
            return Ok(None);
        };
        let view = match GraphViewImpl::new(store, rtxn, def.clone()) {
            Ok(view) => view,
            Err(_) => return Ok(None),
        };
        let max_docs = aggregate::max_docs();
        let mut out = Vec::with_capacity(docs.len());
        for mut doc in docs {
            let start = aggregate::eval_expr_vars(&doc, start_with, vars)?;
            let mut values = Vec::new();
            let mut value_keys = HashSet::new();
            graph_push_value(&start, &mut values, &mut value_keys);
            if values.iter().any(|value| graph::node_key(value).is_err()) {
                return Ok(None);
            }
            let mut results = Vec::new();
            let mut seen = HashSet::new();
            let mut depth = 0usize;
            loop {
                if values.is_empty() {
                    break;
                }
                let mut next_values = Vec::new();
                let mut next_keys = HashSet::new();
                let mut level = Vec::new();
                let mut level_keys = HashSet::new();
                for value in &values {
                    let id = match graph::node_key(value) {
                        Ok(id) => id,
                        Err(_) => return Ok(None),
                    };
                    let neighbors = match view.neighbors(&id, GraphDirection::In, &[]) {
                        Ok(neighbors) => neighbors,
                        Err(_) => return Ok(None),
                    };
                    for edge in neighbors {
                        let Some(bytes) = foreign.db.get(rtxn, edge.edge_id.as_bytes())? else {
                            return Ok(None);
                        };
                        let candidate = match parse_json(bytes) {
                            Ok(candidate) => candidate,
                            Err(_) => return Ok(None),
                        };
                        let key = graph_identity(&candidate);
                        if seen.contains(&key) || !level_keys.insert(key.clone()) {
                            continue;
                        }
                        let sort_key = candidate
                            .get("_id")
                            .and_then(crate::id::key_from_value)
                            .unwrap_or_default();
                        level.push((sort_key, key, candidate));
                    }
                }
                // The fallback `find` path returns matches in `_id` order.
                // Keep that observable ordering even though adjacency keys are
                // sorted by label/target/edge id.
                level.sort_by(|a, b| a.0.cmp(&b.0));
                for (_, key, candidate) in level {
                    seen.insert(key);
                    let next = graph_collect_field(&candidate, connect_from);
                    let mut candidate = candidate;
                    if let Some(depth_field) = depth_field {
                        path::set(&mut candidate, depth_field, Value::from(depth as u64))?;
                    }
                    results.push(candidate);
                    if results.len() > max_docs {
                        return Err(Error::Query(format!(
                            "$graphLookup result exceeds {max_docs} documents"
                        )));
                    }
                    for value in next {
                        graph_push_value(&value, &mut next_values, &mut next_keys);
                    }
                }
                if max_depth.is_some_and(|max_depth| depth >= max_depth) {
                    break;
                }
                values = next_values;
                depth = depth.saturating_add(1);
            }
            path::set(&mut doc, as_field, Value::Array(results))?;
            out.push(doc);
        }
        Ok(Some(out))
    }

    fn union_stage_in(
        &self,
        rtxn: &RoTxn<'_>,
        spec: &Value,
        _vars: &BTreeMap<String, Value>,
    ) -> Result<Vec<Value>> {
        let (from, pipeline) = match spec {
            Value::String(from) => (from.as_str(), None),
            Value::Object(obj) => {
                let from = obj
                    .get("coll")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::Query("$unionWith requires `coll`".into()))?;
                let pipeline = match obj.get("pipeline") {
                    Some(value) => Some(value.as_array().ok_or_else(|| {
                        Error::Query("$unionWith `pipeline` must be an array".into())
                    })?),
                    None => None,
                };
                (from, pipeline)
            }
            _ => {
                return Err(Error::Query(
                    "$unionWith requires a collection name or object".into(),
                ));
            }
        };
        let store = self
            .store
            .ok_or_else(|| Error::Query("$unionWith requires a store-backed collection".into()))?;
        let Some(foreign) = store.collection_at(rtxn, from)? else {
            return Ok(Vec::new());
        };
        let max_docs = aggregate::max_docs();
        let docs = match pipeline {
            Some(pipeline) => foreign.aggregate_in(rtxn, pipeline)?,
            None => foreign.find_in(
                rtxn,
                &FindQuery {
                    limit: Some(max_docs.saturating_add(1)),
                    ..Default::default()
                },
            )?,
        };
        if docs.len() > max_docs {
            return Err(Error::Query(format!(
                "$unionWith collection `{from}` exceeds {max_docs} documents"
            )));
        }
        Ok(docs)
    }

    pub fn find(&self, query: &FindQuery) -> Result<Vec<Value>> {
        if self.profile.is_timeseries() {
            let rtxn = self.read_txn()?;
            let out = self.find_timeseries_in(&rtxn, query)?;
            rtxn.commit()?;
            return Ok(out);
        }
        if self.profile.is_columnar() {
            let rtxn = self.read_txn()?;
            let out = self.find_in(&rtxn, query)?;
            rtxn.commit()?;
            return Ok(out);
        }
        let normalized = normalized_find_query(query)?;
        let query = normalized.as_ref();
        crate::query::validate::validate_filter(&query.filter)?;
        let rtxn = self.read_txn()?;
        let limit = query.limit.unwrap_or(usize::MAX);
        let want = query.skip.saturating_add(limit);
        let strict_ttl = self.ttl_filter_active(&rtxn)?;

        // Sorting the whole set is CPU-bound. When the filter has no
        // index, JSON parsing and TopK can be spread across all cores.
        // The snapshot stays consistent: the main thread reads one `RoTxn` and sends
        // only raw bytes to workers (the LMDB cursor is not shared).
        let sorted = !query.sort.is_empty();
        let parallel_base = !strict_ttl
            && query.hint.is_none()
            && limit > 0
            && want <= PARALLEL_SCAN_MAX_WANT
            && self.db.stat(&rtxn)?.entries >= PARALLEL_SCAN_MIN_ENTRIES
            && direct_id(&query.filter).is_none()
            && direct_ids_in(&query.filter).is_none()
            && id_bounds(&query.filter).is_none()
            && id_sort_direction(&query.sort).is_none()
            && self.index_plan(&rtxn, &query.filter)?.is_none()
            && self.or_index_plans(&rtxn, &query.filter)?.is_none()
            && self.all_index_candidates(&rtxn, &query.filter)?.is_none()
            && !(is_match_all(&query.filter)
                && (self.sort_index_plan(&rtxn, &query.sort)?.is_some()
                    || self.multikey_sort_plan(&rtxn, &query.sort)?.is_some()));
        let parallel = parallel_base
            && (sorted || (!is_match_all(&query.filter) && want >= PARALLEL_UNSORTED_MIN_WANT));

        let out = if parallel {
            let docs = if sorted {
                self.parallel_sorted_scan(&rtxn, query, want)?
            } else {
                self.parallel_unsorted_scan(&rtxn, query, want)?
            };
            docs.into_iter()
                .skip(query.skip)
                .take(limit)
                .map(|doc| match &query.projection {
                    Some(projection) => project(&doc, projection, Some(&query.filter)),
                    None => doc,
                })
                .collect()
        } else {
            self.find_in(&rtxn, query)?
        };
        rtxn.commit()?;
        Ok(out)
    }

    /// Returns stored document bytes for a `find` that needs no projection,
    /// sorting, or index hint. `None` means the caller must use [`Self::find`].
    ///
    /// The fast path deliberately avoids `serde_json::Value` on match-all
    /// scans. Filtered scans still parse for matching, but reuse the original
    /// bytes for output instead of serializing the parsed value again.
    pub fn find_raw(&self, query: &FindQuery) -> Result<Option<Vec<Vec<u8>>>> {
        let normalized = normalized_find_query(query)?;
        let query = normalized.as_ref();
        crate::query::validate::validate_filter(&query.filter)?;
        if self.profile().is_vault()
            || self.profile().is_timeseries()
            || self.profile().is_columnar()
            || query.projection.is_some()
            || !query.sort.is_empty()
            || query.hint.is_some()
        {
            return Ok(None);
        }

        let limit = query.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Some(Vec::new()));
        }
        let deadline = find_deadline(query);
        check_find_deadline(deadline)?;
        if geo::extract_near(&query.filter).is_some() {
            return Ok(None);
        }

        let rtxn = self.read_txn()?;
        if self.ttl_filter_active(&rtxn)? {
            rtxn.commit()?;
            return Ok(None);
        }
        let match_all = is_match_all(&query.filter);
        if !match_all {
            // Prefer the existing indexed paths. This fast path is for scans;
            // replacing an index lookup with a full collection walk would be a
            // regression for selective filters.
            let indexed = direct_id(&query.filter).is_some()
                || direct_ids_in(&query.filter).is_some()
                || id_bounds(&query.filter).is_some()
                || self.all_index_candidates(&rtxn, &query.filter)?.is_some()
                || self.geo_index_candidates(&rtxn, &query.filter)?.is_some()
                || self.index_plan(&rtxn, &query.filter)?.is_some()
                || self.or_index_plans(&rtxn, &query.filter)?.is_some();
            if indexed {
                rtxn.commit()?;
                return Ok(None);
            }
        }

        let raw_prefilter = raw_prefilter(&query.filter);
        let raw_prefilter = raw_prefilter.as_ref();
        let mut skipped = 0usize;
        let mut out = Vec::new();
        for item in self.db.iter(&rtxn)? {
            check_find_deadline(deadline)?;
            let (_key, bytes) = item?;
            if !match_all {
                let Some(doc) = parse_prefiltered(bytes, raw_prefilter)? else {
                    continue;
                };
                if !matches(&doc, &query.filter) {
                    continue;
                }
            }
            if skipped < query.skip {
                skipped += 1;
                continue;
            }
            out.push(bytes.to_vec());
            if out.len() >= limit {
                break;
            }
        }
        rtxn.commit()?;
        Ok(Some(out))
    }

    /// Reads one bounded backfill chunk after the last processed `_id`.
    ///
    /// Using a fresh range/iterator per chunk releases the LMDB iterator borrow
    /// before the caller writes index entries, so backfill can stream instead of
    /// collecting every entry in memory.
    fn read_backfill_chunk(&self, txn: &RoTxn<'_>, after: Option<&[u8]>) -> Result<BackfillChunk> {
        let mut chunk = BackfillChunk {
            data: Vec::new(),
            docs: Vec::with_capacity(PARALLEL_SCAN_CHUNK),
        };
        if let Some(after) = after {
            let range = (Bound::Excluded(after), Bound::Unbounded);
            for item in self.db.range(txn, &range)? {
                let (id_bytes, doc_bytes) = item?;
                push_backfill_doc(&mut chunk, id_bytes, doc_bytes);
                if chunk.docs.len() >= PARALLEL_SCAN_CHUNK
                    || chunk.data.len() >= BACKFILL_CHUNK_BYTES
                {
                    break;
                }
            }
        } else {
            for item in self.db.iter(txn)? {
                let (id_bytes, doc_bytes) = item?;
                push_backfill_doc(&mut chunk, id_bytes, doc_bytes);
                if chunk.docs.len() >= PARALLEL_SCAN_CHUNK
                    || chunk.data.len() >= BACKFILL_CHUNK_BYTES
                {
                    break;
                }
            }
        }
        Ok(chunk)
    }

    /// Parallel index backfill with incremental application.
    ///
    /// The main thread reads one `RwTxn` and feeds bounded chunks to workers.
    /// Workers parse JSON and produce index entries. The main thread applies
    /// each completed chunk immediately instead of accumulating every entry in
    /// memory first. Chunks are bounded by both document count and raw bytes,
    /// so peak Rust heap usage is bounded by the worker queue rather than the
    /// collection size.
    fn parallel_backfill_apply<T, F, A>(
        &self,
        wtxn: &mut RwTxn<'_>,
        kind: &str,
        target: &str,
        f: F,
        mut apply: A,
    ) -> Result<()>
    where
        T: Send,
        F: Fn(String, Value) -> Result<Vec<T>> + Sync,
        A: FnMut(&mut RwTxn<'_>, Vec<T>) -> Result<()>,
    {
        let total = self.db.stat(wtxn)?.entries as u64;
        let progress = crate::progress::begin(&self.name, target, kind, total);
        let workers = backfill_workers();
        if workers <= 1 {
            return self.backfill_serial_apply(wtxn, f, &progress, apply);
        }

        let (job_tx, job_rx) = std::sync::mpsc::sync_channel::<BackfillChunk>(workers * 2);
        let job_rx = Arc::new(Mutex::new(job_rx));
        let (res_tx, res_rx) = std::sync::mpsc::channel::<Result<Vec<T>>>();
        let func = &f;

        std::thread::scope(|scope| -> Result<()> {
            let progress_ref = &progress;
            for _ in 0..workers {
                let rx = Arc::clone(&job_rx);
                let tx = res_tx.clone();
                scope.spawn(move || loop {
                    let job = {
                        let guard = rx.lock().expect("backfill queue");
                        guard.recv()
                    };
                    match job {
                        Ok(chunk) => {
                            let processed = chunk.docs.len() as u64;
                            let mut out = Vec::new();
                            for (id_start, id_len, doc_start, doc_len) in chunk.docs {
                                let id = std::str::from_utf8(
                                    &chunk.data[id_start as usize..][..id_len as usize],
                                )
                                .map_err(|e| Error::Query(e.to_string()))
                                .and_then(|id| {
                                    let doc = serde_json::from_slice::<Value>(
                                        &chunk.data[doc_start as usize..][..doc_len as usize],
                                    )?;
                                    func(id.to_string(), doc)
                                });
                                match id {
                                    Ok(entries) => out.extend(entries),
                                    Err(e) => {
                                        let _ = tx.send(Err(e));
                                        return;
                                    }
                                }
                            }
                            let _ = tx.send(Ok(out));
                            progress_ref.advance(processed);
                        }
                        Err(_) => break,
                    }
                });
            }
            drop(res_tx);

            // Keep at most the worker queue plus a small number of completed
            // chunks in memory. Apply results while producing jobs rather than
            // buffering all entries until the scan is over.
            let mut send_chunk = |wtxn: &mut RwTxn<'_>, mut chunk: BackfillChunk| -> Result<()> {
                loop {
                    match job_tx.try_send(chunk) {
                        Ok(()) => break,
                        Err(std::sync::mpsc::TrySendError::Full(returned)) => {
                            chunk = returned;
                            let result = res_rx.recv().map_err(|_| {
                                Error::Query("index backfill worker stopped unexpectedly".into())
                            })??;
                            apply(wtxn, result)?;
                        }
                        Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                            return Err(Error::Query(
                                "index backfill worker stopped unexpectedly".into(),
                            ));
                        }
                    }
                }
                while let Ok(result) = res_rx.try_recv() {
                    apply(wtxn, result?)?;
                }
                Ok(())
            };

            let scan_result = (|| -> Result<()> {
                let mut after: Option<Vec<u8>> = None;
                loop {
                    let chunk = self.read_backfill_chunk(&*wtxn, after.as_deref())?;
                    if chunk.docs.is_empty() {
                        break;
                    }
                    let (id_start, id_len, _, _) = *chunk.docs.last().expect("non-empty chunk");
                    after = Some(
                        chunk.data[id_start as usize..id_start as usize + id_len as usize].to_vec(),
                    );
                    send_chunk(&mut *wtxn, chunk)?;
                }
                Ok(())
            })();
            drop(send_chunk);
            drop(job_tx);

            if scan_result.is_err() {
                // Workers may still be blocked sending results. Drain them so
                // the scope can join instead of deadlocking on a full channel.
                for _ in res_rx {}
                return scan_result;
            }

            // Apply the remaining chunks. Keep draining after the first error
            // so workers never block on a receiver that is about to be dropped.
            let mut result = Ok(());
            for chunk in res_rx {
                match chunk {
                    Ok(entries) if result.is_ok() => {
                        if let Err(e) = apply(wtxn, entries) {
                            result = Err(e);
                        }
                    }
                    Err(e) if result.is_ok() => result = Err(e),
                    _ => {}
                }
            }
            result
        })
    }

    fn backfill_serial_apply<T, F, A>(
        &self,
        wtxn: &mut RwTxn<'_>,
        f: F,
        progress: &crate::progress::BackfillGuard,
        mut apply: A,
    ) -> Result<()>
    where
        F: Fn(String, Value) -> Result<Vec<T>>,
        A: FnMut(&mut RwTxn<'_>, Vec<T>) -> Result<()>,
    {
        let mut after: Option<Vec<u8>> = None;
        loop {
            let chunk = self.read_backfill_chunk(&*wtxn, after.as_deref())?;
            if chunk.docs.is_empty() {
                break;
            }
            let (id_start, id_len, _, _) = *chunk.docs.last().expect("non-empty chunk");
            after =
                Some(chunk.data[id_start as usize..id_start as usize + id_len as usize].to_vec());
            for (id_start, id_len, doc_start, doc_len) in chunk.docs {
                let id = std::str::from_utf8(&chunk.data[id_start as usize..][..id_len as usize])
                    .map_err(|e| Error::Query(e.to_string()))?
                    .to_string();
                let doc: Value = parse_json(&chunk.data[doc_start as usize..][..doc_len as usize])?;
                apply(wtxn, f(id, doc)?)?;
                progress.advance(1);
            }
        }
        Ok(())
    }

    /// Parallel full scan with sorting.
    ///
    /// The main thread iterates one `RoTxn` (consistent snapshot) and pushes chunks
    /// of raw bytes into a queue. Workers parse JSON, filter, and compute
    /// local TopK; finally the result is merged preserving `seq` order.
    fn parallel_sorted_scan(
        &self,
        rtxn: &RoTxn<'_>,
        query: &FindQuery,
        want: usize,
    ) -> Result<Vec<Value>> {
        let workers = scan_workers();
        if workers <= 1 || want == 0 {
            return self.scan_sorted_serial(rtxn, query, want);
        }

        let (job_tx, job_rx) = std::sync::mpsc::sync_channel::<ScanChunk>(workers * 2);
        let job_rx = Arc::new(Mutex::new(job_rx));
        let (res_tx, res_rx) = std::sync::mpsc::channel::<Result<Vec<(usize, Value)>>>();
        let sort = query.sort.as_slice();
        let filter = &query.filter;
        let raw = raw_prefilter(filter);
        let raw = raw.as_ref();
        let deadline = find_deadline(query);
        check_find_deadline(deadline)?;

        std::thread::scope(|scope| -> Result<Vec<Value>> {
            for _ in 0..workers {
                let rx = Arc::clone(&job_rx);
                let tx = res_tx.clone();
                scope.spawn(move || {
                    let mut top = TopK::new(sort, want);
                    let mut failed = false;
                    loop {
                        let job = {
                            let guard = rx.lock().expect("job queue");
                            guard.recv()
                        };
                        match job {
                            Ok(chunk) => {
                                for (seq, start, len) in chunk.docs {
                                    let bytes = &chunk.data[start as usize..][..len as usize];
                                    match parse_prefiltered(bytes, raw) {
                                        Ok(Some(doc)) => {
                                            if matches(&doc, filter) {
                                                top.push_seq(doc, seq);
                                            }
                                        }
                                        Ok(None) => {}
                                        Err(e) => {
                                            if !failed {
                                                failed = true;
                                                let _ = tx.send(Err(e));
                                            }
                                        }
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    if !failed {
                        let _ = tx.send(Ok(top.into_sorted_seq()));
                    }
                });
            }
            drop(res_tx);

            let mut chunk = ScanChunk {
                idx: 0,
                data: Vec::new(),
                docs: Vec::with_capacity(PARALLEL_SCAN_CHUNK),
            };
            for (seq, item) in self.db.iter(rtxn)?.enumerate() {
                check_find_deadline(deadline)?;
                let (_key, bytes) = item?;
                let start = chunk.data.len() as u32;
                chunk.data.extend_from_slice(bytes);
                chunk.docs.push((seq, start, bytes.len() as u32));
                if chunk.docs.len() >= PARALLEL_SCAN_CHUNK {
                    let full = std::mem::replace(
                        &mut chunk,
                        ScanChunk {
                            idx: 0,
                            data: Vec::new(),
                            docs: Vec::with_capacity(PARALLEL_SCAN_CHUNK),
                        },
                    );
                    let _ = job_tx.send(full);
                }
            }
            if !chunk.docs.is_empty() {
                let _ = job_tx.send(chunk);
            }
            drop(job_tx);

            let mut merged = TopK::new(sort, want);
            for res in res_rx {
                for (seq, doc) in res? {
                    merged.push_seq(doc, seq);
                }
            }
            Ok(merged.into_sorted())
        })
    }

    /// Returns true when a filter has an index-backed access path.
    fn has_index_path(&self, rtxn: &RoTxn<'_>, filter: &Value) -> Result<bool> {
        if direct_id(filter).is_some()
            || direct_ids_in(filter).is_some()
            || id_bounds(filter).is_some()
        {
            return Ok(true);
        }
        if self.all_index_candidates(rtxn, filter)?.is_some() {
            return Ok(true);
        }
        if self.geo_index_plan(rtxn, filter)?.is_some() {
            return Ok(true);
        }
        if self.or_index_plans(rtxn, filter)?.is_some() {
            return Ok(true);
        }
        Ok(self.index_plan(rtxn, filter)?.is_some())
    }

    /// Parallel streaming `$group` for order-independent accumulators.
    ///
    /// Each worker keeps a local group map and the coordinator merges the
    /// partial maps once. Order-sensitive accumulators stay on the sequential
    /// path so `$first`/`$last`/`$push` retain natural `_id` order semantics.
    fn parallel_group_scan(
        &self,
        rtxn: &RoTxn<'_>,
        filter: &Value,
        group_spec: &Value,
        deadline: Option<Instant>,
    ) -> Result<Vec<Value>> {
        let workers = aggregate_workers();
        if workers <= 1 {
            let mut builder = aggregate::GroupBuilder::new(group_spec)?;
            self.for_each_matching(rtxn, filter, deadline, |doc| builder.update(doc))?;
            return builder.finish();
        }
        check_find_deadline(deadline)?;

        let (job_tx, job_rx) = std::sync::mpsc::sync_channel::<ScanChunk>(workers * 2);
        let job_rx = Arc::new(Mutex::new(job_rx));
        let (res_tx, res_rx) = std::sync::mpsc::channel::<Result<aggregate::GroupBuilder>>();
        let raw = raw_prefilter(filter);
        let raw_ref = raw.as_ref();

        std::thread::scope(|scope| -> Result<Vec<Value>> {
            for _ in 0..workers {
                let rx = Arc::clone(&job_rx);
                let tx = res_tx.clone();
                scope.spawn(move || {
                    let mut builder = match aggregate::GroupBuilder::new(group_spec) {
                        Ok(builder) => builder,
                        Err(e) => {
                            let _ = tx.send(Err(e));
                            return;
                        }
                    };
                    let mut failed = false;
                    loop {
                        let job = {
                            let guard = rx.lock().expect("group scan queue");
                            guard.recv()
                        };
                        let Ok(chunk) = job else {
                            break;
                        };
                        if failed {
                            continue;
                        }
                        for (_seq, start, len) in chunk.docs {
                            let bytes = &chunk.data[start as usize..][..len as usize];
                            match parse_prefiltered(bytes, raw_ref) {
                                Ok(Some(doc)) => {
                                    if matches(&doc, filter) {
                                        if let Err(e) = builder.update(doc) {
                                            let _ = tx.send(Err(e));
                                            failed = true;
                                            break;
                                        }
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    let _ = tx.send(Err(e));
                                    failed = true;
                                    break;
                                }
                            }
                        }
                    }
                    if !failed {
                        let _ = tx.send(Ok(builder));
                    }
                });
            }
            drop(res_tx);

            let mut chunk = ScanChunk {
                idx: 0,
                data: Vec::new(),
                docs: Vec::with_capacity(PARALLEL_SCAN_CHUNK),
            };
            for item in self.db.iter(rtxn)? {
                check_find_deadline(deadline)?;
                let (_key, bytes) = item?;
                let start = chunk.data.len() as u32;
                chunk.data.extend_from_slice(bytes);
                chunk.docs.push((0, start, bytes.len() as u32));
                if chunk.docs.len() >= PARALLEL_SCAN_CHUNK {
                    let full = std::mem::replace(
                        &mut chunk,
                        ScanChunk {
                            idx: 0,
                            data: Vec::new(),
                            docs: Vec::with_capacity(PARALLEL_SCAN_CHUNK),
                        },
                    );
                    let _ = job_tx.send(full);
                }
            }
            if !chunk.docs.is_empty() {
                let _ = job_tx.send(chunk);
            }
            drop(job_tx);

            let mut merged = aggregate::GroupBuilder::new(group_spec)?;
            let mut first_error = None;
            for result in res_rx {
                match result {
                    Ok(partial) => {
                        if first_error.is_none() {
                            merged.merge(partial)?;
                        }
                    }
                    Err(e) => {
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                    }
                }
            }
            match first_error {
                Some(e) => Err(e),
                None => merged.finish(),
            }
        })
    }

    /// Parallel full scan without sorting.
    ///
    /// Chunks are sent in waves: one to each worker. The coordinator
    /// receives the whole wave and assembles results in `_id` order. Once it has
    /// `want` matches, it stops sending more chunks. This lets broad
    /// filters keep early exit while selective scans run in parallel.
    fn parallel_unsorted_scan(
        &self,
        rtxn: &RoTxn<'_>,
        query: &FindQuery,
        want: usize,
    ) -> Result<Vec<Value>> {
        let workers = scan_workers();
        if workers <= 1 || want == 0 {
            return self.find_in(rtxn, query);
        }

        let mut job_txs = Vec::with_capacity(workers);
        let mut job_rxs = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (tx, rx) = std::sync::mpsc::sync_channel::<ScanChunk>(1);
            job_txs.push(tx);
            job_rxs.push(rx);
        }
        let (res_tx, res_rx) = std::sync::mpsc::channel::<Result<(usize, Vec<(usize, Value)>)>>();
        let filter = &query.filter;
        let raw = raw_prefilter(filter);
        let raw = raw.as_ref();
        let deadline = find_deadline(query);
        check_find_deadline(deadline)?;
        // Create the iterator before starting workers: an LMDB error must not leave
        // workers waiting on a channel that is never closed.
        let mut iter = self.db.iter(rtxn)?;

        std::thread::scope(|scope| -> Result<Vec<Value>> {
            for rx in job_rxs {
                let tx = res_tx.clone();
                scope.spawn(move || {
                    let mut failed = false;
                    loop {
                        let job = rx.recv();
                        let Ok(chunk) = job else {
                            break;
                        };
                        let mut out = Vec::new();
                        if !failed {
                            for (seq, start, len) in chunk.docs {
                                let bytes = &chunk.data[start as usize..][..len as usize];
                                match parse_prefiltered(bytes, raw) {
                                    Ok(Some(doc)) => {
                                        if matches(&doc, filter) {
                                            out.push((seq, doc));
                                            if out.len() >= want {
                                                break;
                                            }
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(e) => {
                                        failed = true;
                                        let _ = tx.send(Err(e));
                                        break;
                                    }
                                }
                            }
                        }
                        if !failed {
                            let _ = tx.send(Ok((chunk.idx, out)));
                        }
                    }
                });
            }
            drop(res_tx);

            let mut merged = TopK::new(&[], want);
            let mut seq = 0usize;
            let mut next_chunk = 0usize;
            let mut expected = 0usize;
            let mut exhausted = false;
            let mut error: Option<Error> = None;

            // The first wave is small so a broad filter stops quickly.
            // Only when it is not enough do we enlarge the wave and amortize
            // synchronization on a selective full scan.
            let mut rounds = 1usize;
            'scan: loop {
                check_find_deadline(deadline)?;
                let mut sent = 0usize;
                'fill: for _ in 0..rounds {
                    for tx in &job_txs {
                        if exhausted {
                            break 'fill;
                        }
                        let mut chunk = ScanChunk {
                            idx: next_chunk,
                            data: Vec::new(),
                            docs: Vec::with_capacity(PARALLEL_SCAN_CHUNK),
                        };
                        while chunk.docs.len() < PARALLEL_SCAN_CHUNK {
                            check_find_deadline(deadline)?;
                            match iter.next() {
                                Some(item) => match item {
                                    Ok((_key, bytes)) => {
                                        let start = chunk.data.len() as u32;
                                        chunk.data.extend_from_slice(bytes);
                                        chunk.docs.push((seq, start, bytes.len() as u32));
                                        seq += 1;
                                    }
                                    Err(e) => {
                                        error = Some(e.into());
                                        break 'scan;
                                    }
                                },
                                None => {
                                    exhausted = true;
                                    break;
                                }
                            }
                        }
                        if chunk.docs.is_empty() {
                            break 'fill;
                        }
                        next_chunk += 1;
                        if tx.send(chunk).is_err() {
                            error = Some(Error::Query("parallel scan worker stopped".into()));
                            break 'scan;
                        }
                        sent += 1;
                    }
                }
                if sent == 0 {
                    break;
                }

                let mut slots: Vec<Option<Vec<(usize, Value)>>> = vec![None; sent];
                for _ in 0..sent {
                    let msg = match res_rx.recv() {
                        Ok(msg) => msg,
                        Err(_) => {
                            error = Some(Error::Query("parallel scan returned no result".into()));
                            break 'scan;
                        }
                    };
                    let (idx, docs) = match msg {
                        Ok(v) => v,
                        Err(e) => {
                            error = Some(e);
                            break 'scan;
                        }
                    };
                    let pos = idx.checked_sub(expected).unwrap_or(usize::MAX);
                    if pos >= sent {
                        error = Some(Error::Query("unexpected parallel scan chunk".into()));
                        break 'scan;
                    }
                    slots[pos] = Some(docs);
                }
                for docs in slots.into_iter().flatten() {
                    for (seq, doc) in docs {
                        merged.push_seq(doc, seq);
                    }
                    expected += 1;
                    if merged.heap.len() >= want {
                        break 'scan;
                    }
                }
                rounds = if want <= 1024 {
                    4
                } else if want <= 4096 {
                    2
                } else {
                    1
                };
            }
            drop(job_txs);
            match error {
                Some(e) => Err(e),
                None => Ok(merged.into_sorted()),
            }
        })
    }

    /// Single-threaded version used as a fallback (e.g. one core).
    fn scan_sorted_serial(
        &self,
        rtxn: &RoTxn<'_>,
        query: &FindQuery,
        want: usize,
    ) -> Result<Vec<Value>> {
        let mut top = TopK::new(&query.sort, want);
        let raw = raw_prefilter(&query.filter);
        let deadline = find_deadline(query);
        for (seq, item) in self.db.iter(rtxn)?.enumerate() {
            check_find_deadline(deadline)?;
            let (_key, bytes) = item?;
            let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                continue;
            };
            if matches(&doc, &query.filter) {
                top.push_seq(doc, seq);
            }
        }
        Ok(top.into_sorted())
    }

    /// `$near` returns documents ordered by distance. A geo index narrows the
    /// candidate set when `$maxDistance` is present; otherwise the collection
    /// scan still provides correct ordering.
    fn find_near(
        &self,
        rtxn: &RoTxn<'_>,
        query: &FindQuery,
        field: &str,
        near: &geo::GeoNear,
    ) -> Result<Vec<Value>> {
        if !query.sort.is_empty() {
            return Err(Error::Query(
                "$near cannot be combined with an explicit sort".into(),
            ));
        }
        let limit = query.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let raw = raw_prefilter(&query.filter);
        let mut hits: Vec<(f64, Value)> = Vec::new();
        let deadline = find_deadline(query);
        check_find_deadline(deadline)?;

        if let Some(ids) = self.geo_index_candidates(rtxn, &query.filter)? {
            for id in ids {
                check_find_deadline(deadline)?;
                let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                    continue;
                };
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if !matches(&doc, &query.filter) {
                    continue;
                }
                if let Some(distance) = geo::field_distance(&doc, field, near.center) {
                    hits.push((distance, doc));
                }
            }
        } else {
            for item in self.db.iter(rtxn)? {
                check_find_deadline(deadline)?;
                let (_key, bytes) = item?;
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if !matches(&doc, &query.filter) {
                    continue;
                }
                if let Some(distance) = geo::field_distance(&doc, field, near.center) {
                    hits.push((distance, doc));
                }
            }
        }

        hits.sort_by(|a, b| a.0.total_cmp(&b.0));
        Ok(hits
            .into_iter()
            .skip(query.skip)
            .take(limit)
            .map(|(_, doc)| match &query.projection {
                Some(projection) => project(&doc, projection, Some(&query.filter)),
                None => doc,
            })
            .collect())
    }

    pub fn find_in(&self, rtxn: &RoTxn<'_>, query: &FindQuery) -> Result<Vec<Value>> {
        if self.profile.is_timeseries() {
            return self.find_timeseries_in(rtxn, query);
        }
        if self.profile.is_columnar() {
            let normalized = normalized_find_query(query)?;
            let query = normalized.as_ref();
            crate::query::validate::validate_filter(&query.filter)?;
            let hint = query.hint.as_ref().map(parse_index_hint).transpose()?;
            let plan = self.query_index_plan(rtxn, &query.filter, hint.as_ref())?;
            let deadline = find_deadline(query);
            check_find_deadline(deadline)?;
            if let Some((field, near)) = geo::extract_near(&query.filter) {
                if !query.sort.is_empty() {
                    return Err(Error::Query(
                        "$near cannot be combined with an explicit sort".into(),
                    ));
                }
                let limit = query.limit.unwrap_or(usize::MAX);
                if limit == 0 {
                    return Ok(Vec::new());
                }
                let strict_ttl = self.ttl_filter_active(rtxn)?;
                let now = ttl::now_ms();
                let mut hits = Vec::new();
                for doc in crate::columnar::all_docs(rtxn, self.env, &self.name)? {
                    check_find_deadline(deadline)?;
                    if !crate::query::matcher::matches(&doc, &query.filter) {
                        continue;
                    }
                    if strict_ttl && !self.ttl_doc_visible(rtxn, &doc, now)? {
                        continue;
                    }
                    if let Some(distance) = geo::field_distance(&doc, &field, near.center) {
                        hits.push((distance, doc));
                    }
                }
                hits.sort_by(|a, b| a.0.total_cmp(&b.0));
                return Ok(hits
                    .into_iter()
                    .skip(query.skip)
                    .take(limit)
                    .map(|(_, doc)| project_document(&doc, query.projection.as_ref()))
                    .collect());
            }
            if let Some(plan) = plan {
                let ids = self.index_plan_candidates(rtxn, &plan, None)?;
                let strict_ttl = self.ttl_filter_active(rtxn)?;
                let now = ttl::now_ms();
                let mut docs = Vec::with_capacity(ids.len());
                for id in ids {
                    check_find_deadline(deadline)?;
                    let Some(doc) = crate::columnar::find_by_id(rtxn, self.env, &self.name, &id)?
                    else {
                        continue;
                    };
                    if !crate::query::matcher::matches(&doc, &query.filter) {
                        continue;
                    }
                    if strict_ttl && !self.ttl_doc_visible(rtxn, &doc, now)? {
                        continue;
                    }
                    docs.push(doc);
                }
                let limit = query.limit.unwrap_or(usize::MAX);
                if !query.sort.is_empty() {
                    docs = top_k_docs(docs, &query.sort, query.skip.saturating_add(limit));
                }
                return Ok(docs
                    .into_iter()
                    .skip(query.skip)
                    .take(limit)
                    .map(|doc| project_document(&doc, query.projection.as_ref()))
                    .collect());
            }
            let out = if self.ttl_filter_active(rtxn)? {
                let now = ttl::now_ms();
                crate::columnar::find_visible(rtxn, self.env, &self.name, query, &mut |doc| {
                    self.ttl_doc_visible(rtxn, doc, now)
                })?
            } else {
                crate::columnar::find(rtxn, self.env, &self.name, query)?
            };
            check_find_deadline(deadline)?;
            return Ok(out);
        }
        let normalized = normalized_find_query(query)?;
        let query = normalized.as_ref();
        crate::query::validate::validate_filter(&query.filter)?;
        let hint = query.hint.as_ref().map(parse_index_hint).transpose()?;
        let limit = query.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let deadline = find_deadline(query);
        check_find_deadline(deadline)?;
        let strict_ttl = self.ttl_filter_active(rtxn)?;
        let now = ttl::now_ms();
        if let Some((field, near)) = geo::extract_near(&query.filter) {
            let docs = self.find_near(rtxn, query, &field, &near)?;
            return if strict_ttl {
                self.retain_visible_docs(rtxn, docs)
            } else {
                Ok(docs)
            };
        }
        let sorted = !query.sort.is_empty();
        // Without sorting, `skip + limit` matches are enough (early exit).
        // With sorting, TopK keeps only the `want` best documents.
        let want = query.skip.saturating_add(limit);
        let mut acc = Acc::new(sorted, &query.sort, want);
        let mut post_skip = query.skip;
        let raw = raw_prefilter(&query.filter);

        // 0) Ordered match-all queries can walk primary-key or index order
        //    directly, stopping after `skip + limit` entries.
        if query.hint.is_none() && is_match_all(&query.filter) && sorted && !strict_ttl {
            if let Some(out) = self.find_sorted_match_all(rtxn, query)? {
                return Ok(out);
            }
        }

        // 0b) Top-level `$or` or a simple `$in`: when every branch has an
        //     index, merge the candidate ids and apply the full filter as a
        //     residual.
        if let Some(plans) = if query.hint.is_none() {
            self.or_index_plans(rtxn, &query.filter)?
        } else {
            None
        } {
            let residual =
                Self::residual_without_in(&query.filter).unwrap_or_else(|| query.filter.clone());
            let mut seen = HashSet::new();
            'plans: for plan in &plans {
                let range = (as_bound(&plan.bounds.0), as_bound(&plan.bounds.1));
                for item in plan.db.range(rtxn, &range)? {
                    check_find_deadline(deadline)?;
                    let (key, value) = item?;
                    let Some((id, _)) = index::decode_index_entry(key, value) else {
                        continue;
                    };
                    if !seen.insert(id.clone()) {
                        continue;
                    }
                    let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                        continue;
                    };
                    let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                        continue;
                    };
                    if !matches(&doc, &residual) {
                        continue;
                    }
                    if strict_ttl && !self.ttl_doc_visible(rtxn, &doc, now)? {
                        continue;
                    }
                    acc.push(doc);
                    if !sorted && acc.len() >= want {
                        break 'plans;
                    }
                }
            }
            let docs = acc.finish();
            return Ok(docs
                .into_iter()
                .skip(post_skip)
                .take(limit)
                .map(|doc| match &query.projection {
                    Some(projection) => project(&doc, projection, Some(&query.filter)),
                    None => doc,
                })
                .collect());
        }

        // 1) Exact `_id` -> direct get.
        if let Some(id) = if query.hint.is_none() {
            direct_id(&query.filter)
        } else {
            None
        } {
            if let Some(bytes) = self.db.get(rtxn, id.as_bytes())? {
                let doc = parse_json(bytes)?;
                if !strict_ttl || self.ttl_doc_visible(rtxn, &doc, now)? {
                    acc.push(doc);
                }
            }
        }
        // 1a) `_id: {$in: [...]}`: fetch the requested primary keys directly
        //     instead of scanning the collection once per value.
        else if let Some(ids) = if query.hint.is_none() {
            direct_ids_in(&query.filter)
        } else {
            None
        } {
            let residual =
                Self::residual_without_in(&query.filter).unwrap_or_else(|| query.filter.clone());
            for id in ids {
                check_find_deadline(deadline)?;
                if !sorted && acc.len() >= want {
                    break;
                }
                let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                    continue;
                };
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, &residual)
                    && (!strict_ttl || self.ttl_doc_visible(rtxn, &doc, now)?)
                {
                    acc.push(doc);
                }
            }
        }
        // 1b) Multikey `$all`: intersect the index ranges for every required
        //     value, then apply the full filter as a residual.
        else if let Some(ids) = if query.hint.is_none() {
            self.all_index_candidates(rtxn, &query.filter)?
        } else {
            None
        } {
            for id in ids {
                check_find_deadline(deadline)?;
                if !sorted && acc.len() >= want {
                    break;
                }
                let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                    continue;
                };
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, &query.filter)
                    && (!strict_ttl || self.ttl_doc_visible(rtxn, &doc, now)?)
                {
                    acc.push(doc);
                }
            }
        }
        // 1c) Geo index cell ranges for `$geoWithin` / `$geoIntersects`.
        else if let Some(ids) = if query.hint.is_none() {
            self.geo_index_candidates(rtxn, &query.filter)?
        } else {
            None
        } {
            for id in ids {
                check_find_deadline(deadline)?;
                if !sorted && acc.len() >= want {
                    break;
                }
                let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                    continue;
                };
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, &query.filter)
                    && (!strict_ttl || self.ttl_doc_visible(rtxn, &doc, now)?)
                {
                    acc.push(doc);
                }
            }
        }
        // 2) Secondary index. First the fast "index-only" path: if
        //    the sort exactly matches the index order, we walk the index
        //    and fetch only `skip + limit` documents instead of the whole range.
        else if let Some(plan) = self.query_index_plan(rtxn, &query.filter, hint.as_ref())? {
            let sort_match = if sorted && !plan.def.is_multikey() {
                index_matches_sort(&plan.def, &query.sort)
            } else {
                None
            };
            if let Some(reverse) = sort_match {
                let (lower, upper) = &plan.bounds;
                let range = (as_bound(lower), as_bound(upper));
                let mut skipped = 0usize;
                let mut out = Vec::new();
                let iter: IndexIter<'_> = if reverse {
                    Box::new(plan.db.rev_range(rtxn, &range)?)
                } else {
                    Box::new(plan.db.range(rtxn, &range)?)
                };
                for item in iter {
                    check_find_deadline(deadline)?;
                    let (key, value) = item?;
                    let Some((id, inc)) = index::decode_index_entry(key, value) else {
                        continue;
                    };
                    let doc = if let Some(doc) =
                        covering_doc(&id, inc.as_ref(), &query.filter, &query.projection)
                    {
                        doc
                    } else {
                        let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                            continue;
                        };
                        let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                            continue;
                        };
                        doc
                    };
                    if !matches(&doc, &query.filter) {
                        continue;
                    }
                    if strict_ttl && !self.ttl_doc_visible(rtxn, &doc, now)? {
                        continue;
                    }
                    if skipped < query.skip {
                        skipped += 1;
                        continue;
                    }
                    out.push(doc);
                    if out.len() >= limit {
                        break;
                    }
                }
                return Ok(out
                    .into_iter()
                    .map(|doc| match &query.projection {
                        Some(projection) => project(&doc, projection, Some(&query.filter)),
                        None => doc,
                    })
                    .collect());
            }

            let ids = self.index_plan_candidates(rtxn, &plan, (!sorted).then_some(want))?;
            check_find_deadline(deadline)?;
            for id in ids {
                check_find_deadline(deadline)?;
                if !sorted && acc.len() >= want {
                    break;
                }
                if let Some(bytes) = self.db.get(rtxn, id.as_bytes())? {
                    let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                        continue;
                    };
                    // Residual filter: the index may return extra entries.
                    if matches(&doc, &query.filter)
                        && (!strict_ttl || self.ttl_doc_visible(rtxn, &doc, now)?)
                    {
                        acc.push(doc);
                    }
                }
            }
        }
        // 3) `_id` range over the primary key. This is not a secondary index, but
        //    it narrows the full scan to a contiguous B+tree interval.
        else if let Some((lower, upper)) = if query.hint.is_none() {
            id_bounds(&query.filter)
        } else {
            None
        } {
            let range = (as_bound(&lower), as_bound(&upper));
            let reverse = id_sort_direction(&query.sort).unwrap_or(false);
            let iter: IndexIter<'_> = if reverse {
                Box::new(self.db.rev_range(rtxn, &range)?)
            } else {
                Box::new(self.db.range(rtxn, &range)?)
            };

            if !sorted || id_sort_direction(&query.sort).is_some() {
                let mut skipped = 0usize;
                let mut out = Vec::new();
                for item in iter {
                    check_find_deadline(deadline)?;
                    let (_key, bytes) = item?;
                    let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                        continue;
                    };
                    if !matches(&doc, &query.filter) {
                        continue;
                    }
                    if strict_ttl && !self.ttl_doc_visible(rtxn, &doc, now)? {
                        continue;
                    }
                    if skipped < query.skip {
                        skipped += 1;
                        continue;
                    }
                    out.push(doc);
                    if out.len() >= limit {
                        break;
                    }
                }
                return Ok(out
                    .into_iter()
                    .map(|doc| match &query.projection {
                        Some(projection) => project(&doc, projection, Some(&query.filter)),
                        None => doc,
                    })
                    .collect());
            }

            let mut top = TopK::new(&query.sort, want);
            for item in iter {
                check_find_deadline(deadline)?;
                let (_key, bytes) = item?;
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, &query.filter)
                    && (!strict_ttl || self.ttl_doc_visible(rtxn, &doc, now)?)
                {
                    top.push(doc);
                }
            }
            return Ok(top
                .into_sorted()
                .into_iter()
                .skip(query.skip)
                .take(limit)
                .map(|doc| match &query.projection {
                    Some(projection) => project(&doc, projection, Some(&query.filter)),
                    None => doc,
                })
                .collect());
        }
        // 4) Full scan without sorting: skip immediately, without collecting
        //     skipped documents or sorting the whole batch at the end.
        else if !sorted {
            post_skip = 0;
            if is_match_all(&query.filter) && !strict_ttl {
                // Natural order is `_id`; skipped documents do not need parsing.
                for item in self.db.iter(rtxn)?.skip(query.skip).take(limit) {
                    check_find_deadline(deadline)?;
                    let (_key, bytes) = item?;
                    acc.push(parse_json(bytes)?);
                }
            } else {
                let mut skipped = 0usize;
                for item in self.db.iter(rtxn)? {
                    check_find_deadline(deadline)?;
                    let (_key, bytes) = item?;
                    let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                        continue;
                    };
                    if matches(&doc, &query.filter) {
                        if strict_ttl && !self.ttl_doc_visible(rtxn, &doc, now)? {
                            continue;
                        }
                        if skipped < query.skip {
                            skipped += 1;
                            continue;
                        }
                        acc.push(doc);
                        if acc.len() >= limit {
                            break;
                        }
                    }
                }
            }
        }
        // 3b) Full scan with sorting: collect matches through TopK.
        else {
            for item in self.db.iter(rtxn)? {
                check_find_deadline(deadline)?;
                let (_key, bytes) = item?;
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, &query.filter)
                    && (!strict_ttl || self.ttl_doc_visible(rtxn, &doc, now)?)
                {
                    acc.push(doc);
                }
            }
        }

        let docs = acc.finish();
        let docs = if strict_ttl {
            self.retain_visible_docs(rtxn, docs)?
        } else {
            docs
        };
        Ok(docs
            .into_iter()
            .skip(post_skip)
            .take(limit)
            .map(|doc| match &query.projection {
                Some(projection) => project(&doc, projection, Some(&query.filter)),
                None => doc,
            })
            .collect())
    }

    pub fn find_one(&self, filter: &Value) -> Result<Option<Value>> {
        let query = FindQuery {
            filter: filter.clone(),
            sort: Vec::new(),
            skip: 0,
            limit: Some(1),
            projection: None,
            comment: None,
            max_time_ms: None,
            hint: None,
        };
        Ok(self.find(&query)?.into_iter().next())
    }

    pub fn find_one_in(
        &self,
        rtxn: &RoTxn<'_>,
        filter: &Value,
        projection: Option<&Value>,
    ) -> Result<Option<Value>> {
        let query = FindQuery {
            filter: filter.clone(),
            sort: Vec::new(),
            skip: 0,
            limit: Some(1),
            projection: projection.cloned(),
            comment: None,
            max_time_ms: None,
            hint: None,
        };
        Ok(self.find_in(rtxn, &query)?.into_iter().next())
    }

    /// Point lookup by `_id` or a unique single-field index.
    ///
    /// Results are returned in input order. A missing key produces `None`.
    /// `projection` uses the normal include/exclude rules. An `_id`-only
    /// projection is served from index metadata, and other covering projections
    /// can be served without reading the document.
    pub fn lookup(
        &self,
        by: &str,
        values: &[Value],
        projection: Option<&Value>,
    ) -> Result<Vec<Option<Value>>> {
        self.lookup_fields(&[by.to_string()], values, projection)
    }

    pub fn lookup_in(
        &self,
        rtxn: &RoTxn<'_>,
        by: &str,
        values: &[Value],
        projection: Option<&Value>,
    ) -> Result<Vec<Option<Value>>> {
        self.lookup_fields_in(rtxn, &[by.to_string()], values, projection)
    }

    /// Point lookup by one or more fields. A multi-field lookup requires an
    /// exact unique compound index and every value must be an array of the same
    /// length as `by`.
    pub fn lookup_fields(
        &self,
        by: &[String],
        values: &[Value],
        projection: Option<&Value>,
    ) -> Result<Vec<Option<Value>>> {
        let rtxn = self.read_txn()?;
        let out = self.lookup_fields_in(&rtxn, by, values, projection)?;
        rtxn.commit()?;
        Ok(out)
    }

    /// Ordered point lookup with execution counters.
    pub fn lookup_fields_report(
        &self,
        by: &[String],
        values: &[Value],
        projection: Option<&Value>,
    ) -> Result<(Vec<Option<Value>>, LookupStats)> {
        let rtxn = self.read_txn()?;
        let out = self.lookup_fields_report_in(&rtxn, by, values, projection)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn lookup_fields_in(
        &self,
        rtxn: &RoTxn<'_>,
        by: &[String],
        values: &[Value],
        projection: Option<&Value>,
    ) -> Result<Vec<Option<Value>>> {
        Ok(self
            .lookup_fields_report_in(rtxn, by, values, projection)?
            .0)
    }

    /// Ordered point lookup with execution counters for `explain_lookup`.
    pub fn lookup_fields_report_in(
        &self,
        rtxn: &RoTxn<'_>,
        by: &[String],
        values: &[Value],
        projection: Option<&Value>,
    ) -> Result<(Vec<Option<Value>>, LookupStats)> {
        if by.is_empty() || by.iter().any(String::is_empty) {
            return Err(Error::Query("lookup field must not be empty".into()));
        }
        if values.len() > MAX_LOOKUP_VALUES {
            return Err(Error::Query(format!(
                "lookup accepts at most {MAX_LOOKUP_VALUES} values"
            )));
        }
        let strict_ttl = self.ttl_filter_active(rtxn)?;
        let now = ttl::now_ms();
        let mut stats = LookupStats {
            requested_keys: values.len(),
            ..LookupStats::default()
        };
        if values.is_empty() {
            return Ok((Vec::new(), stats));
        }

        // Deduplicate key work while preserving the original result order.
        let mut seen: HashMap<LookupKey<'_>, usize> = HashMap::with_capacity(values.len());
        let mut positions = Vec::with_capacity(values.len());
        let mut unique_values = Vec::with_capacity(values.len());
        for value in values {
            let key = lookup_key(value)?;
            if let Some(&index) = seen.get(&key) {
                positions.push(index);
            } else {
                let index = unique_values.len();
                seen.insert(key, index);
                unique_values.push(value);
                positions.push(index);
            }
        }

        let mut unique_out = Vec::with_capacity(unique_values.len());
        let mut unique_sizes = Vec::with_capacity(unique_values.len());
        let mut unique_bytes = 0usize;
        let id_only = projection_is_id_only(projection);
        stats.unique_keys = unique_values.len();

        if self.profile.is_columnar() && by.len() == 1 && by[0] == "_id" {
            stats.selected_index = "_id_".to_string();
            stats.index_fields = by.to_vec();
            stats.primary = true;
            for &value in &unique_values {
                stats.index_entries_read = stats.index_entries_read.saturating_add(1);
                let (doc, size) = match value.as_str() {
                    Some(id) if strict_ttl && self.ttl_expired_at(rtxn, id, now)? => (None, 4),
                    Some(id) => {
                        match crate::columnar::find_by_id(rtxn, self.env, &self.name, id)? {
                            Some(doc) => {
                                if id_only {
                                    stats.bytes_read =
                                        stats.bytes_read.saturating_add(id.len() as u64);
                                    (Some(serde_json::json!({"_id": id})), id.len())
                                } else {
                                    stats.documents_read = stats.documents_read.saturating_add(1);
                                    let bytes = serde_json::to_vec(&doc)?;
                                    stats.bytes_read =
                                        stats.bytes_read.saturating_add(bytes.len() as u64);
                                    let size = bytes.len();
                                    let doc = match projection {
                                        Some(projection) => project(&doc, projection, None),
                                        None => doc,
                                    };
                                    (Some(doc), size)
                                }
                            }
                            None => (None, 4),
                        }
                    }
                    None => (None, 4),
                };
                push_lookup_result(
                    &mut unique_out,
                    &mut unique_sizes,
                    &mut unique_bytes,
                    doc,
                    size,
                )?;
            }
            stats.covering = id_only;
            finish_lookup_stats(
                values.len(),
                &unique_out,
                &unique_sizes,
                &positions,
                &mut stats,
            );
            let out = finish_lookup(values.len(), unique_out, unique_sizes, &positions)?;
            return Ok((out, stats));
        }

        if by.len() == 1 && by[0] == "_id" {
            stats.selected_index = "_id_".to_string();
            stats.index_fields = by.to_vec();
            stats.primary = true;
            for &value in &unique_values {
                stats.index_entries_read = stats.index_entries_read.saturating_add(1);
                let (doc, size) = match value.as_str() {
                    Some(id) if strict_ttl && self.ttl_expired_at(rtxn, id, now)? => (None, 4),
                    Some(id) => match self.db.get(rtxn, id.as_bytes())? {
                        Some(bytes) => {
                            if id_only {
                                stats.bytes_read = stats.bytes_read.saturating_add(id.len() as u64);
                                (Some(serde_json::json!({"_id": id})), id.len())
                            } else {
                                stats.documents_read = stats.documents_read.saturating_add(1);
                                stats.bytes_read =
                                    stats.bytes_read.saturating_add(bytes.len() as u64);
                                let size = bytes.len();
                                match projection {
                                    Some(projection) => {
                                        let doc: Value = parse_json(bytes)?;
                                        (Some(project(&doc, projection, None)), size)
                                    }
                                    None => (Some(parse_json(bytes)?), size),
                                }
                            }
                        }
                        None => (None, 4),
                    },
                    None => (None, 4),
                };
                push_lookup_result(
                    &mut unique_out,
                    &mut unique_sizes,
                    &mut unique_bytes,
                    doc,
                    size,
                )?;
            }
            stats.covering = id_only;
            finish_lookup_stats(
                values.len(),
                &unique_out,
                &unique_sizes,
                &positions,
                &mut stats,
            );
            let out = finish_lookup(values.len(), unique_out, unique_sizes, &positions)?;
            return Ok((out, stats));
        }

        let def = self
            .index_defs(rtxn)?
            .into_iter()
            .find(|def| {
                def.unique
                    && !def.geo
                    && !def.is_multikey()
                    && def.current_format()
                    && def.effective_fields().as_slice() == by
            })
            .ok_or_else(|| {
                Error::Query(format!(
                    "lookup on `{}` requires a unique single-field or compound index",
                    by.join("+")
                ))
            })?;
        let Some(db) = self.index_db(rtxn, &def)? else {
            return Err(Error::Query(format!(
                "lookup on `{}` requires a unique single-field or compound index",
                by.join("+")
            )));
        };
        stats.selected_index = def.name();
        stats.index_fields = def.effective_fields();
        stats.index_include = def.include.clone();

        if self.profile.is_columnar() {
            for &value in &unique_values {
                let parts = if by.len() == 1 {
                    std::slice::from_ref(value)
                } else {
                    let Some(parts) = value.as_array() else {
                        push_lookup_result(
                            &mut unique_out,
                            &mut unique_sizes,
                            &mut unique_bytes,
                            None,
                            4,
                        )?;
                        continue;
                    };
                    if parts.len() != by.len() {
                        push_lookup_result(
                            &mut unique_out,
                            &mut unique_sizes,
                            &mut unique_bytes,
                            None,
                            4,
                        )?;
                        continue;
                    }
                    parts.as_slice()
                };
                let Some((lower, upper)) = lookup_bounds(parts, &def) else {
                    push_lookup_result(
                        &mut unique_out,
                        &mut unique_sizes,
                        &mut unique_bytes,
                        None,
                        4,
                    )?;
                    continue;
                };
                let range = (as_bound(&lower), as_bound(&upper));
                let mut found = None;
                for item in db.range(rtxn, &range)? {
                    stats.index_entries_read = stats.index_entries_read.saturating_add(1);
                    let (key, raw) = item?;
                    let raw_size = raw.len();
                    let Some((id, _inc)) = index::decode_index_entry(key, raw) else {
                        continue;
                    };
                    if strict_ttl && self.ttl_expired_at(rtxn, &id, now)? {
                        continue;
                    }
                    let Some(doc) = crate::columnar::find_by_id(rtxn, self.env, &self.name, &id)?
                    else {
                        continue;
                    };
                    let (doc, size) = if id_only {
                        stats.bytes_read = stats.bytes_read.saturating_add(raw_size as u64);
                        (serde_json::json!({"_id": id}), raw_size)
                    } else {
                        stats.documents_read = stats.documents_read.saturating_add(1);
                        let bytes = serde_json::to_vec(&doc)?;
                        stats.bytes_read = stats.bytes_read.saturating_add(bytes.len() as u64);
                        let size = bytes.len();
                        let doc = match projection {
                            Some(projection) => project(&doc, projection, None),
                            None => doc,
                        };
                        (doc, size)
                    };
                    found = Some((doc, size));
                    break;
                }
                let (doc, size) = match found {
                    Some((doc, size)) => (Some(doc), size),
                    None => (None, 4),
                };
                push_lookup_result(
                    &mut unique_out,
                    &mut unique_sizes,
                    &mut unique_bytes,
                    doc,
                    size,
                )?;
            }
            stats.covering = id_only;
            finish_lookup_stats(
                values.len(),
                &unique_out,
                &unique_sizes,
                &positions,
                &mut stats,
            );
            let out = finish_lookup(values.len(), unique_out, unique_sizes, &positions)?;
            return Ok((out, stats));
        }

        let covering_projection = projection.cloned();
        for &value in &unique_values {
            let parts = if by.len() == 1 {
                std::slice::from_ref(value)
            } else {
                let Some(parts) = value.as_array() else {
                    push_lookup_result(
                        &mut unique_out,
                        &mut unique_sizes,
                        &mut unique_bytes,
                        None,
                        4,
                    )?;
                    continue;
                };
                if parts.len() != by.len() {
                    push_lookup_result(
                        &mut unique_out,
                        &mut unique_sizes,
                        &mut unique_bytes,
                        None,
                        4,
                    )?;
                    continue;
                }
                parts.as_slice()
            };
            let Some((lower, upper)) = lookup_bounds(parts, &def) else {
                push_lookup_result(
                    &mut unique_out,
                    &mut unique_sizes,
                    &mut unique_bytes,
                    None,
                    4,
                )?;
                continue;
            };
            let range = (as_bound(&lower), as_bound(&upper));
            let mut found = None;
            for item in db.range(rtxn, &range)? {
                stats.index_entries_read = stats.index_entries_read.saturating_add(1);
                let (key, raw) = item?;
                let raw_size = raw.len();
                let Some((id, inc)) = index::decode_index_entry(key, raw) else {
                    continue;
                };
                if strict_ttl && self.ttl_expired_at(rtxn, &id, now)? {
                    break;
                }
                let mut filter = Map::new();
                for (field, part) in by.iter().zip(parts) {
                    filter.insert(field.clone(), part.clone());
                }
                let filter = Value::Object(filter);
                let (doc, size) = if id_only {
                    stats.bytes_read = stats.bytes_read.saturating_add(raw_size as u64);
                    (serde_json::json!({"_id": id}), raw_size)
                } else if let Some(doc) =
                    covering_doc(&id, inc.as_ref(), &filter, &covering_projection)
                {
                    stats.bytes_read = stats.bytes_read.saturating_add(raw_size as u64);
                    (
                        match projection {
                            Some(projection) => project(&doc, projection, Some(&filter)),
                            None => doc,
                        },
                        raw_size,
                    )
                } else {
                    let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                        continue;
                    };
                    stats.documents_read = stats.documents_read.saturating_add(1);
                    stats.bytes_read = stats.bytes_read.saturating_add(bytes.len() as u64);
                    let size = bytes.len();
                    match projection {
                        Some(projection) => {
                            let doc: Value = parse_json(bytes)?;
                            (project(&doc, projection, None), size)
                        }
                        None => (parse_json(bytes)?, size),
                    }
                };
                found = Some((doc, size));
                break;
            }
            let (doc, size) = match found {
                Some((doc, size)) => (Some(doc), size),
                None => (None, 4),
            };
            push_lookup_result(
                &mut unique_out,
                &mut unique_sizes,
                &mut unique_bytes,
                doc,
                size,
            )?;
        }
        stats.covering = projection.is_some() && stats.documents_read == 0;
        finish_lookup_stats(
            values.len(),
            &unique_out,
            &unique_sizes,
            &positions,
            &mut stats,
        );
        let out = finish_lookup(values.len(), unique_out, unique_sizes, &positions)?;
        Ok((out, stats))
    }

    /// Explains the access path and actual counts for a `find` query.
    ///
    /// This executes the query so `actual_matched` and `actual_returned` are
    /// exact. Use small limits when explaining broad queries.
    pub fn explain_in(&self, rtxn: &RoTxn<'_>, query: &FindQuery) -> Result<Value> {
        if self.profile.is_timeseries() {
            let matched = self.count_in(rtxn, &query.filter)?;
            let hint = query.hint.as_ref().map(parse_index_hint).transpose()?;
            let plan = self.query_index_plan(rtxn, &query.filter, hint.as_ref())?;
            let mut prune_stats = None;
            let bucket_index = if plan.is_none() {
                match self.timeseries_settings_in(rtxn)? {
                    Some(settings) => {
                        let locators = crate::timeseries::lookup_indexed_buckets(
                            rtxn,
                            self.env,
                            &self.name,
                            &query.filter,
                            &settings,
                        )?;
                        if locators.is_some() {
                            prune_stats = crate::timeseries::bucket_prune_stats(
                                rtxn,
                                self.env,
                                &self.name,
                                &query.filter,
                                &settings,
                            )?;
                        }
                        locators.is_some()
                    }
                    None => false,
                }
            } else {
                false
            };
            let indexed = plan.is_some() || bucket_index;
            let (buckets_total, buckets_pruned) = prune_stats.unwrap_or((0, 0));
            return Ok(serde_json::json!({
                "collection": self.name,
                "profile": "timeseries",
                "access": if indexed { "index_scan" } else { "bucket_scan" },
                "index_used": indexed,
                "bucket_index_used": bucket_index,
                "index": if bucket_index { Some("bucket_locator".to_string()) } else { plan.as_ref().map(|plan| plan.def.name()) },
                "buckets_total": buckets_total,
                "buckets_pruned": buckets_pruned,
                "filter": query.filter,
                "matched": matched,
            }));
        }
        if self.profile.is_columnar() {
            let normalized = normalized_find_query(query)?;
            let query = normalized.as_ref();
            crate::query::validate::validate_filter(&query.filter)?;
            let hint = query.hint.as_ref().map(parse_index_hint).transpose()?;
            let plan = self.query_index_plan(rtxn, &query.filter, hint.as_ref())?;
            let stats = crate::columnar::stats(rtxn, self.env, &self.name)?;
            let matched = self.count_in(rtxn, &query.filter)?;
            let returned = self.find_in(rtxn, query)?.len();
            let candidates = match &plan {
                Some(plan) => self.index_count_plan(rtxn, plan)? as u64,
                None => stats.rows,
            };
            let sort_covered = query.sort.is_empty()
                || id_sort_direction(&query.sort).is_some()
                || plan
                    .as_ref()
                    .is_some_and(|plan| index_matches_sort(&plan.def, &query.sort).is_some());
            return Ok(serde_json::json!({
                "collection": self.name,
                "profile": "columnar",
                "access": if plan.is_some() { "index_scan" } else { "bucket_scan" },
                "access_path": if plan.is_some() { "index_scan" } else { "bucket_scan" },
                "index_used": plan.is_some(),
                "index": plan.as_ref().map(|plan| serde_json::json!({
                    "fields": plan.def.effective_fields(),
                    "unique": plan.def.unique,
                    "include": plan.def.include,
                    "multikey": plan.def.multikey_fields,
                })).unwrap_or(Value::Null),
                "bounds": Value::Null,
                "buckets_total": stats.buckets,
                "buckets_pruned": 0,
                "buckets_sealed": stats.sealed_buckets,
                "buckets_open": stats.open_buckets,
                "estimated_documents": stats.rows,
                "candidates": candidates,
                "documents_scanned": candidates,
                "sort_covered": sort_covered,
                "filter": query.filter,
                "sort": Value::Array(
                    query
                        .sort
                        .iter()
                        .map(|(field, dir)| serde_json::json!([field, dir]))
                        .collect(),
                ),
                "skip": query.skip,
                "limit": query.limit.map_or(Value::Null, Value::from),
                "actual_matched": matched,
                "actual_returned": returned,
            }));
        }
        let normalized = normalized_find_query(query)?;
        let query = normalized.as_ref();
        crate::query::validate::validate_filter(&query.filter)?;
        let mut out = self.explain_plan(rtxn, query)?;
        let matched = self.count_in(rtxn, &query.filter)?;
        let returned = self.find_in(rtxn, query)?.len();
        let obj = out
            .as_object_mut()
            .ok_or_else(|| Error::Query("internal explain error".into()))?;
        obj.insert("filter".into(), query.filter.clone());
        obj.insert(
            "sort".into(),
            Value::Array(
                query
                    .sort
                    .iter()
                    .map(|(field, dir)| serde_json::json!([field, dir]))
                    .collect(),
            ),
        );
        obj.insert("skip".into(), Value::from(query.skip));
        obj.insert("limit".into(), query.limit.map_or(Value::Null, Value::from));
        if let Some(comment) = &query.comment {
            obj.insert("comment".into(), Value::String(comment.clone()));
        }
        if let Some(max_time_ms) = query.max_time_ms {
            obj.insert("maxTimeMS".into(), Value::from(max_time_ms));
        }
        if let Some(hint) = &query.hint {
            obj.insert("hint".into(), hint.clone());
        }
        obj.insert(
            "estimated_documents".into(),
            Value::from(obj.get("candidates").and_then(Value::as_u64).unwrap_or(0)),
        );
        obj.insert("actual_matched".into(), Value::from(matched as u64));
        obj.insert("actual_returned".into(), Value::from(returned as u64));
        Ok(out)
    }

    pub fn explain(&self, query: &FindQuery) -> Result<Value> {
        let rtxn = self.read_txn()?;
        let out = self.explain_in(&rtxn, query)?;
        rtxn.commit()?;
        Ok(out)
    }

    fn explain_plan(&self, rtxn: &RoTxn<'_>, query: &FindQuery) -> Result<Value> {
        let sort_covered = |plan: Option<&IndexDef>| -> bool {
            if query.sort.is_empty() {
                return true;
            }
            if id_sort_direction(&query.sort).is_some() {
                return true;
            }
            plan.and_then(|def| index_matches_sort(def, &query.sort))
                .is_some()
        };
        let mut out = serde_json::Map::new();
        out.insert("collection".into(), Value::String(self.name.clone()));
        let hint = query.hint.as_ref().map(parse_index_hint).transpose()?;

        if query.hint.is_none() && is_match_all(&query.filter) {
            let candidates = self.db.stat(rtxn)?.entries as u64;
            if id_sort_direction(&query.sort).is_some() {
                out.insert("access_path".into(), Value::String("id_range".into()));
                out.insert("index_used".into(), Value::Bool(true));
                out.insert(
                    "index".into(),
                    serde_json::json!({"fields": ["_id"], "primary": true}),
                );
                out.insert("bounds".into(), Value::Null);
                out.insert("candidates".into(), Value::from(candidates));
                out.insert("sort_covered".into(), Value::Bool(true));
                return Ok(Value::Object(out));
            }
            let sort_plan = match self.sort_index_plan(rtxn, &query.sort)? {
                Some(plan) => Some(plan),
                None => self.multikey_sort_plan(rtxn, &query.sort)?,
            };
            if let Some(plan) = sort_plan {
                out.insert("access_path".into(), Value::String("index_scan".into()));
                out.insert("index_used".into(), Value::Bool(true));
                out.insert(
                    "index".into(),
                    serde_json::json!({
                        "fields": plan.def.effective_fields(),
                        "unique": plan.def.unique,
                        "include": plan.def.include,
                        "multikey": plan.def.multikey_fields,
                    }),
                );
                out.insert("bounds".into(), Value::Null);
                out.insert("candidates".into(), Value::from(candidates));
                out.insert("sort_covered".into(), Value::Bool(true));
                return Ok(Value::Object(out));
            }
            out.insert(
                "access_path".into(),
                Value::String("collection_scan".into()),
            );
            out.insert("index_used".into(), Value::Bool(false));
            out.insert("index".into(), Value::Null);
            out.insert("bounds".into(), Value::Null);
            out.insert("candidates".into(), Value::from(candidates));
            out.insert("sort_covered".into(), Value::Bool(sort_covered(None)));
            return Ok(Value::Object(out));
        }

        if let Some(id) = if query.hint.is_none() {
            direct_id(&query.filter)
        } else {
            None
        } {
            let exists = self.db.get(rtxn, id.as_bytes())?.is_some();
            out.insert("access_path".into(), Value::String("id_lookup".into()));
            out.insert("index_used".into(), Value::Bool(true));
            out.insert(
                "index".into(),
                serde_json::json!({"fields": ["_id"], "primary": true}),
            );
            out.insert("bounds".into(), Value::Null);
            out.insert("candidates".into(), Value::from(u64::from(exists)));
            out.insert("sort_covered".into(), Value::Bool(sort_covered(None)));
            return Ok(Value::Object(out));
        }

        if let Some(ids) = if query.hint.is_none() {
            direct_ids_in(&query.filter)
        } else {
            None
        } {
            let mut candidates = 0u64;
            for id in ids {
                candidates += u64::from(self.db.get(rtxn, id.as_bytes())?.is_some());
            }
            out.insert("access_path".into(), Value::String("id_lookup".into()));
            out.insert("index_used".into(), Value::Bool(true));
            out.insert(
                "index".into(),
                serde_json::json!({"fields": ["_id"], "primary": true}),
            );
            out.insert("bounds".into(), Value::Null);
            out.insert("candidates".into(), Value::from(candidates));
            out.insert("sort_covered".into(), Value::Bool(sort_covered(None)));
            return Ok(Value::Object(out));
        }

        if let Some(ids) = if query.hint.is_none() {
            self.all_index_candidates(rtxn, &query.filter)?
        } else {
            None
        } {
            out.insert("access_path".into(), Value::String("index_scan".into()));
            out.insert("index_used".into(), Value::Bool(true));
            out.insert(
                "index".into(),
                serde_json::json!({
                    "fields": [query.filter.as_object().and_then(|o| o.keys().next())],
                    "multikey": true,
                }),
            );
            out.insert("bounds".into(), Value::Null);
            out.insert("candidates".into(), Value::from(ids.len() as u64));
            out.insert("sort_covered".into(), Value::Bool(sort_covered(None)));
            return Ok(Value::Object(out));
        }

        if let Some(plan) = if query.hint.is_none() {
            self.geo_index_plan(rtxn, &query.filter)?
        } else {
            None
        } {
            let mut candidates = 0usize;
            for (lower, upper) in &plan.ranges {
                let range = (as_bound(lower), as_bound(upper));
                candidates = candidates.saturating_add(plan.db.range(rtxn, &range)?.count());
            }
            out.insert("access_path".into(), Value::String("geo_index_scan".into()));
            out.insert("index_used".into(), Value::Bool(true));
            out.insert(
                "index".into(),
                serde_json::json!({
                    "fields": plan.def.effective_fields(),
                    "geo": true,
                }),
            );
            out.insert(
                "bounds".into(),
                serde_json::json!({"ranges": plan.ranges.len()}),
            );
            out.insert("candidates".into(), Value::from(candidates as u64));
            out.insert("sort_covered".into(), Value::Bool(false));
            return Ok(Value::Object(out));
        }

        if let Some(plans) = if query.hint.is_none() {
            self.or_index_plans(rtxn, &query.filter)?
        } else {
            None
        } {
            let mut candidates = 0usize;
            let mut indexes = Vec::with_capacity(plans.len());
            for plan in &plans {
                candidates = candidates.saturating_add(self.index_count_plan(rtxn, plan)?);
                indexes.push(serde_json::json!({
                    "fields": plan.def.effective_fields(),
                    "unique": plan.def.unique,
                    "include": plan.def.include,
                }));
            }
            out.insert("access_path".into(), Value::String("index_union".into()));
            out.insert("index_used".into(), Value::Bool(true));
            out.insert("index".into(), Value::Array(indexes));
            out.insert("bounds".into(), Value::Null);
            out.insert("candidates".into(), Value::from(candidates as u64));
            out.insert("sort_covered".into(), Value::Bool(sort_covered(None)));
            return Ok(Value::Object(out));
        }

        if let Some(plan) = self.query_index_plan(rtxn, &query.filter, hint.as_ref())? {
            let candidates = if plan.def.is_multikey() {
                self.index_count_unique_plan(rtxn, &plan)?
            } else {
                self.index_count_plan(rtxn, &plan)?
            };
            let fields = plan.def.effective_fields();
            out.insert("access_path".into(), Value::String("index_scan".into()));
            out.insert("index_used".into(), Value::Bool(true));
            out.insert(
                "index".into(),
                serde_json::json!({
                    "fields": fields,
                    "unique": plan.def.unique,
                    "include": plan.def.include,
                    "multikey": plan.def.multikey_fields,
                }),
            );
            out.insert(
                "bounds".into(),
                serde_json::json!({
                    "lower": explain_bound(&plan.bounds.0, false),
                    "upper": explain_bound(&plan.bounds.1, false),
                }),
            );
            out.insert("candidates".into(), Value::from(candidates as u64));
            out.insert(
                "sort_covered".into(),
                Value::Bool(sort_covered(Some(&plan.def))),
            );
            return Ok(Value::Object(out));
        }

        if let Some((lower, upper)) = if query.hint.is_none() {
            id_bounds(&query.filter)
        } else {
            None
        } {
            let range = (as_bound(&lower), as_bound(&upper));
            let candidates = self.db.range(rtxn, &range)?.count();
            out.insert("access_path".into(), Value::String("id_range".into()));
            out.insert("index_used".into(), Value::Bool(true));
            out.insert(
                "index".into(),
                serde_json::json!({"fields": ["_id"], "primary": true}),
            );
            out.insert(
                "bounds".into(),
                serde_json::json!({
                    "lower": explain_bound(&lower, true),
                    "upper": explain_bound(&upper, true),
                }),
            );
            out.insert("candidates".into(), Value::from(candidates as u64));
            out.insert("sort_covered".into(), Value::Bool(sort_covered(None)));
            return Ok(Value::Object(out));
        }

        out.insert(
            "access_path".into(),
            Value::String("collection_scan".into()),
        );
        out.insert("index_used".into(), Value::Bool(false));
        out.insert("index".into(), Value::Null);
        out.insert("bounds".into(), Value::Null);
        out.insert(
            "candidates".into(),
            Value::from(self.db.stat(rtxn)?.entries as u64),
        );
        out.insert("sort_covered".into(), Value::Bool(sort_covered(None)));
        Ok(Value::Object(out))
    }

    pub fn count(&self, filter: &Value) -> Result<usize> {
        self.count_with_options(filter, None)
    }

    pub fn count_with_options(&self, filter: &Value, max_time_ms: Option<u64>) -> Result<usize> {
        let rtxn = self.read_txn()?;
        let n = self.count_in_with_options(&rtxn, filter, max_time_ms)?;
        rtxn.commit()?;
        Ok(n)
    }

    pub fn count_in(&self, rtxn: &RoTxn<'_>, filter: &Value) -> Result<usize> {
        self.count_in_with_options(rtxn, filter, None)
    }

    pub fn count_in_with_options(
        &self,
        rtxn: &RoTxn<'_>,
        filter: &Value,
        max_time_ms: Option<u64>,
    ) -> Result<usize> {
        let deadline = operation_deadline(max_time_ms);
        self.count_in_deadline(rtxn, filter, deadline)
    }

    fn count_in_deadline(
        &self,
        rtxn: &RoTxn<'_>,
        filter: &Value,
        deadline: Option<Instant>,
    ) -> Result<usize> {
        if self.profile.is_timeseries() {
            let settings = self.timeseries_settings_in(rtxn)?.unwrap_or_default();
            return crate::timeseries::count(rtxn, self.env, &self.name, &settings, filter);
        }
        if self.profile.is_columnar() {
            crate::query::validate::validate_filter(filter)?;
            check_find_deadline(deadline)?;
            if self.ttl_filter_active(rtxn)? {
                let query = FindQuery {
                    filter: filter.clone(),
                    ..Default::default()
                };
                let now = ttl::now_ms();
                return Ok(crate::columnar::find_visible(
                    rtxn,
                    self.env,
                    &self.name,
                    &query,
                    &mut |doc| self.ttl_doc_visible(rtxn, doc, now),
                )?
                .len());
            }
            return crate::columnar::count(rtxn, self.env, &self.name, filter);
        }
        crate::query::validate::validate_filter(filter)?;
        check_find_deadline(deadline)?;
        if self.ttl_filter_active(rtxn)? {
            let query = FindQuery {
                filter: filter.clone(),
                max_time_ms: deadline.map(|deadline| {
                    u64::try_from(
                        deadline
                            .saturating_duration_since(Instant::now())
                            .as_millis(),
                    )
                    .unwrap_or(u64::MAX)
                    .max(1)
                }),
                ..Default::default()
            };
            return Ok(self.find_in(rtxn, &query)?.len());
        }
        // `count({})` / `count(null)` -> O(1) from B+tree metadata.
        if is_match_all(filter) {
            return Ok(self.db.stat(rtxn)?.entries);
        }
        // Exact `_id` -> direct `get`.
        if let Some(id) = direct_id(filter) {
            return Ok(usize::from(self.db.get(rtxn, id.as_bytes())?.is_some()));
        }
        // `_id` range over the primary key. When the filter contains only conditions on
        // `_id`, counting range entries without parsing JSON is enough.
        if let Some((lower, upper)) = id_bounds(filter) {
            let range = (as_bound(&lower), as_bound(&upper));
            if filter.as_object().is_some_and(|o| o.len() == 1) {
                let mut n = 0usize;
                for item in self.db.range(rtxn, &range)? {
                    check_find_deadline(deadline)?;
                    item?;
                    n += 1;
                }
                return Ok(n);
            }
            let mut n = 0usize;
            let raw = raw_prefilter(filter);
            for item in self.db.range(rtxn, &range)? {
                check_find_deadline(deadline)?;
                let (_key, bytes) = item?;
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, filter) {
                    n += 1;
                }
            }
            return Ok(n);
        }
        // Multikey `$all`: the intersection already contains distinct ids.
        if let Some(ids) = self.all_index_candidates(rtxn, filter)? {
            check_find_deadline(deadline)?;
            return Ok(ids.len());
        }
        // Geo index cell ranges still need the residual matcher because a cell
        // can contain points outside the requested circle/polygon.
        if let Some(ids) = self.geo_index_candidates(rtxn, filter)? {
            let raw = raw_prefilter(filter);
            let mut n = 0;
            for id in ids {
                check_find_deadline(deadline)?;
                let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                    continue;
                };
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, filter) {
                    n += 1;
                }
            }
            return Ok(n);
        }
        // Filter fully covered by an index: count keys without parsing JSON.
        if let Some(plan) = self.index_plan(rtxn, filter)? {
            check_find_deadline(deadline)?;
            if index_filter_exact(&plan.def, filter) {
                return if plan.def.is_multikey() {
                    self.index_count_unique_plan(rtxn, &plan)
                } else {
                    self.index_count_plan(rtxn, &plan)
                };
            }
        }
        let mut n = 0;
        let raw = raw_prefilter(filter);
        for item in self.db.iter(rtxn)? {
            check_find_deadline(deadline)?;
            let (_key, bytes) = item?;
            let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                continue;
            };
            if matches(&doc, filter) {
                n += 1;
            }
        }
        Ok(n)
    }

    /// Deep-checks documents and all index structures in a read transaction.
    pub fn verify_in(&self, rtxn: &RoTxn<'_>) -> Result<CollectionVerification> {
        self.verify_in_options(rtxn, false)
    }

    pub fn verify_in_options(
        &self,
        rtxn: &RoTxn<'_>,
        allow_missing_search: bool,
    ) -> Result<CollectionVerification> {
        if self.profile.is_timeseries() {
            let settings = self.timeseries_settings_in(rtxn)?.unwrap_or_default();
            let (documents, bytes) =
                crate::timeseries::verify_in(rtxn, self.env, &self.name, &settings)?;
            return Ok(CollectionVerification {
                documents,
                bytes,
                indexes: Vec::new(),
                text_indexes: Vec::new(),
                fuzzy_indexes: Vec::new(),
                vector_indexes: Vec::new(),
            });
        }
        let defs = self.index_defs(rtxn)?;
        let search_defs = self.search_index_defs_in(rtxn)?;
        let text_names: Vec<String> = search_defs
            .iter()
            .filter(|def| def.kind == SearchKind::Text)
            .map(|def| def.name.clone())
            .collect();
        let fuzzy_names: Vec<String> = search_defs
            .iter()
            .filter(|def| def.kind == SearchKind::Fuzzy)
            .map(|def| def.name.clone())
            .collect();
        let vector_fields = self.vector_defs(rtxn)?;

        let mut documents = 0u64;
        let mut bytes = 0u64;
        let mut expected_index = vec![0u64; defs.len()];
        let mut expected_vectors = vec![0u64; vector_fields.len()];

        let docs_for_verify: Vec<(String, Value, u64)> = if self.profile.is_columnar() {
            let mut docs = Vec::new();
            crate::columnar::for_each(rtxn, self.env, &self.name, |doc| {
                let Some(obj) = doc.as_object() else {
                    return Err(Error::Query(format!(
                        "columnar document in `{}` is not a JSON object",
                        self.name
                    )));
                };
                let id = obj
                    .get("_id")
                    .and_then(id::key_from_value)
                    .ok_or(Error::InvalidId)?;
                let size = serde_json::to_vec(&doc)?.len() as u64;
                docs.push((id, doc, size));
                Ok(())
            })?;
            docs
        } else {
            let mut docs = Vec::new();
            for item in self.db.iter(rtxn)? {
                let (key, raw) = item?;
                let id = std::str::from_utf8(key).map_err(|e| {
                    Error::Query(format!("invalid document key in `{}`: {e}", self.name))
                })?;
                let doc: Value = serde_json::from_slice(raw).map_err(|e| {
                    Error::Query(format!("invalid JSON for `{}`/`{id}`: {e}", self.name))
                })?;
                let Some(obj) = doc.as_object() else {
                    return Err(Error::Query(format!(
                        "document `{}`/`{id}` is not a JSON object",
                        self.name
                    )));
                };
                if obj.get("_id").and_then(id::key_from_value).as_deref() != Some(id) {
                    return Err(Error::Query(format!(
                        "document `{}`/`{id}` has a missing or mismatched _id",
                        self.name
                    )));
                }
                docs.push((id.to_string(), doc, raw.len() as u64));
            }
            docs
        };

        for (id, doc, size) in docs_for_verify {
            let id = id.as_str();
            let key = id.as_bytes();
            documents += 1;
            bytes += size;

            for (i, def) in defs.iter().enumerate() {
                if !def.current_format() {
                    continue;
                }
                let keys = index::encode_index_keys(def, &doc)?;
                if keys.keys.is_empty() {
                    continue;
                }
                let db = self.index_db(rtxn, def)?.ok_or_else(|| {
                    Error::Query(format!(
                        "index `{}` on `{}` is defined but its database is missing",
                        def.name(),
                        self.name
                    ))
                })?;
                let expected = self.index_value_for(def, &doc, id);
                let compact = self.compact_index_ids(def);
                for value_bytes in keys.keys {
                    let mut index_key = value_bytes;
                    index::append_index_id(&mut index_key, id, compact);
                    let actual = db.get(rtxn, &index_key)?.ok_or_else(|| {
                        Error::Query(format!(
                            "index `{}` on `{}` is missing entry for `{id}`",
                            def.name(),
                            self.name
                        ))
                    })?;
                    if actual != expected.as_slice() {
                        return Err(Error::Query(format!(
                            "index `{}` on `{}` has a stale entry for `{id}`",
                            def.name(),
                            self.name
                        )));
                    }
                    expected_index[i] += 1;
                }
            }

            for (i, def) in vector_fields.iter().enumerate() {
                let Some(vector) = index::field_value(&doc, &def.field)
                    .and_then(|value| vector::parse_vector(value, def.dims))
                else {
                    continue;
                };
                let db = self.vector_db(rtxn, &def.field)?.ok_or_else(|| {
                    Error::Query(format!(
                        "vector index `{}` on `{}` is defined but its database is missing",
                        def.field, self.name
                    ))
                })?;
                let actual = db.get(rtxn, key)?.ok_or_else(|| {
                    Error::Query(format!(
                        "vector index `{}` on `{}` is missing entry for `{id}`",
                        def.field, self.name
                    ))
                })?;
                if actual != vector::encode_vector(&vector).as_slice() {
                    return Err(Error::Query(format!(
                        "vector index `{}` on `{}` has a stale entry for `{id}`",
                        def.field, self.name
                    )));
                }
                expected_vectors[i] += 1;
            }
        }

        let mut indexes = Vec::with_capacity(defs.len());
        for (i, def) in defs.iter().enumerate() {
            let db = self.index_db(rtxn, def)?.ok_or_else(|| {
                Error::Query(format!(
                    "index `{}` on `{}` is defined but its database is missing",
                    def.name(),
                    self.name
                ))
            })?;
            let entries = db.stat(rtxn)?.entries as u64;
            let checked = def.current_format();
            if checked && entries != expected_index[i] {
                return Err(Error::Query(format!(
                    "index `{}` on `{}` has {entries} entries, expected {}",
                    def.name(),
                    self.name,
                    expected_index[i]
                )));
            }
            indexes.push(IndexVerification {
                name: def.name(),
                fields: def.effective_fields(),
                unique: def.unique,
                include: def.include.clone(),
                format: def.format,
                entries,
                checked,
                multikey_fields: def.multikey_fields.clone(),
                multikey_all_docs: def.multikey_all_docs,
            });
        }

        for (i, def) in vector_fields.iter().enumerate() {
            let db = self.vector_db(rtxn, &def.field)?.ok_or_else(|| {
                Error::Query(format!(
                    "vector index `{}` on `{}` is defined but its database is missing",
                    def.field, self.name
                ))
            })?;
            let entries = db.stat(rtxn)?.entries as u64;
            if entries != expected_vectors[i] {
                return Err(Error::Query(format!(
                    "vector index `{}` on `{}` has {entries} entries, expected {}",
                    def.field, self.name, expected_vectors[i]
                )));
            }
        }

        let root = self.segment_root()?;
        for def in search_defs.iter() {
            match def.kind {
                SearchKind::Text => {
                    let Some(manifest) = self.segment_manifest_in(rtxn, &def.name)? else {
                        if documents == 0 || allow_missing_search {
                            continue;
                        }
                        return Err(Error::Query(format!(
                            "segment text index `{}` on `{}` has no manifest",
                            def.name, self.name
                        )));
                    };
                    for rel in &manifest.segments {
                        let path = root.join(rel);
                        if !path.is_file() {
                            if allow_missing_search {
                                continue;
                            }
                            return Err(Error::Query(format!(
                                "segment text index `{}` on `{}` is missing `{}`",
                                def.name,
                                self.name,
                                path.display()
                            )));
                        }
                        SegmentReader::open(&path).map_err(|error| {
                            Error::Query(format!(
                                "segment text index `{}` on `{}` cannot open `{}`: {error}",
                                def.name,
                                self.name,
                                path.display()
                            ))
                        })?;
                    }
                }
                SearchKind::Fuzzy => {
                    let Some(manifest) = self.fuzzy_segment_manifest_in(rtxn, &def.name)? else {
                        if documents == 0 || allow_missing_search {
                            continue;
                        }
                        return Err(Error::Query(format!(
                            "segment fuzzy index `{}` on `{}` has no manifest",
                            def.name, self.name
                        )));
                    };
                    for (segment, values) in manifest.segments.iter().zip(&manifest.values) {
                        let segment_path = root.join(segment);
                        let values_path = root.join(values);
                        if allow_missing_search
                            && (!segment_path.is_file() || !values_path.is_file())
                        {
                            continue;
                        }
                        for path in [&segment_path, &values_path] {
                            if !path.is_file() {
                                return Err(Error::Query(format!(
                                    "segment fuzzy index `{}` on `{}` is missing `{}`",
                                    def.name,
                                    self.name,
                                    path.display()
                                )));
                            }
                        }
                        FuzzySegmentReader::open(&segment_path, &values_path).map_err(|error| {
                            Error::Query(format!(
                                "segment fuzzy index `{}` on `{}` cannot open `{}`: {error}",
                                def.name,
                                self.name,
                                segment_path.display()
                            ))
                        })?;
                    }
                }
            }
        }

        Ok(CollectionVerification {
            documents,
            bytes,
            indexes,
            text_indexes: text_names,
            fuzzy_indexes: fuzzy_names,
            vector_indexes: vector_fields.into_iter().map(|def| def.field).collect(),
        })
    }

    pub(crate) fn read_txn(&self) -> Result<crate::store::ReadTxn<'_>> {
        let guard = self.txn_gate.enter();
        Ok(crate::store::ReadTxn::new(self.env.read_txn()?, guard))
    }

    fn write_txn(&self) -> Result<crate::store::WriteTxn<'_>> {
        let guard = self.txn_gate.enter();
        Ok(crate::store::WriteTxn::new(
            self.env.write_txn()?,
            guard,
            self.env,
            true,
        ))
    }
}

struct GraphViewImpl<'a, 't> {
    txn: &'a RoTxn<'t>,
    node: Collection<'a>,
    edge: Collection<'a>,
    def: GraphDef,
    out: Option<Database<Bytes, Bytes>>,
    incoming: Option<Database<Bytes, Bytes>>,
}

impl<'a, 't> GraphViewImpl<'a, 't> {
    fn new(store: &'a crate::store::Store, txn: &'a RoTxn<'t>, def: GraphDef) -> Result<Self> {
        let node = store
            .collection_at(txn, &def.node_collection)?
            .ok_or_else(|| {
                Error::Query(format!(
                    "graph node collection `{}` does not exist",
                    def.node_collection
                ))
            })?;
        let edge = store
            .collection_at(txn, &def.edge_collection)?
            .ok_or_else(|| {
                Error::Query(format!(
                    "graph edge collection `{}` does not exist",
                    def.edge_collection
                ))
            })?;
        let out = edge.graph_adjacency_db(txn, GraphDirection::Out, def.generation)?;
        let incoming = edge.graph_adjacency_db(txn, GraphDirection::In, def.generation)?;
        Ok(Self {
            txn,
            node,
            edge,
            def,
            out,
            incoming,
        })
    }

    fn scan(
        &self,
        db: Option<Database<Bytes, Bytes>>,
        node: &str,
        label: Option<&str>,
        seen: &mut HashSet<String>,
        out: &mut Vec<AdjEdge>,
    ) -> Result<()> {
        let Some(db) = db else {
            return Ok(());
        };
        let prefix = match label {
            Some(label) => graph::label_prefix(node, label),
            None => graph::node_prefix(node),
        };
        for item in db.prefix_iter(self.txn, prefix.as_slice())? {
            let (key, raw) = item?;
            let Some((_node, edge_label, target, edge_id)) = graph::decode_adjacency_key(key)
            else {
                continue;
            };
            if !seen.insert(edge_id.to_string()) {
                continue;
            }
            let summary = match graph::decode_adjacency_value(raw) {
                Ok(summary) => summary,
                Err(_) => {
                    // Releases before compact adjacency stored the full edge
                    // document. Read it transparently; the next reindex writes
                    // the compact form.
                    let doc: Value = parse_json(raw)?;
                    let parts = graph::edge_parts(&self.def, &doc)?;
                    graph::AdjacencyValue {
                        edge_id: graph::edge_id(&doc)?,
                        label: parts.label,
                        weight: parts.weight,
                        valid_from: parts.valid_from,
                        valid_to: parts.valid_to,
                    }
                }
            };
            if summary.edge_id != edge_id || summary.label != edge_label {
                return Err(Error::Query("corrupt graph adjacency value".into()));
            }
            out.push(AdjEdge {
                edge_id: summary.edge_id,
                label: summary.label,
                weight: summary.weight,
                target: target.to_string(),
                valid_from: summary.valid_from,
                valid_to: summary.valid_to,
            });
        }
        Ok(())
    }
}

impl GraphView for GraphViewImpl<'_, '_> {
    fn node(&self, id: &str) -> Result<Option<Value>> {
        self.node.document_in(self.txn, id)
    }

    fn edge(&self, id: &str) -> Result<Option<Value>> {
        self.edge.document_in(self.txn, id)
    }

    fn node_ids(&self) -> Result<Vec<String>> {
        Ok(self
            .node
            .documents_for_backfill(self.txn)?
            .into_iter()
            .map(|(id, _)| id)
            .collect())
    }

    fn neighbors(
        &self,
        id: &str,
        direction: GraphDirection,
        labels: &[String],
    ) -> Result<Vec<AdjEdge>> {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        let directions: &[GraphDirection] = if !self.def.directed {
            &[GraphDirection::Out, GraphDirection::In]
        } else {
            match direction {
                GraphDirection::Out => &[GraphDirection::Out],
                GraphDirection::In => &[GraphDirection::In],
                GraphDirection::Both => &[GraphDirection::Out, GraphDirection::In],
            }
        };
        for current in directions {
            let db = match current {
                GraphDirection::Out => self.out,
                GraphDirection::In => self.incoming,
                GraphDirection::Both => unreachable!(),
            };
            if labels.is_empty() {
                self.scan(db, id, None, &mut seen, &mut out)?;
            } else {
                for label in labels {
                    self.scan(db, id, Some(label), &mut seen, &mut out)?;
                }
            }
        }
        Ok(out)
    }
}

// ----------------------------- helpers -------------------------------

/// Equality value from a condition (`5` or `{"$eq": 5}`).
fn eq_value(cond: &Value) -> Option<&Value> {
    if date::is_date(cond) || numeric::is_numeric(cond) || bson_types::is_bson_value(cond) {
        return Some(cond);
    }
    if let Some(obj) = cond.as_object() {
        if obj.len() == 1 {
            if let Some(value) = obj.get("$eq") {
                return Some(value);
            }
            if let Some(values) = obj.get("$all").and_then(Value::as_array) {
                if values.len() == 1 {
                    return values.first();
                }
            }
        }
        return None;
    }
    Some(cond)
}

/// Builds an index range for the filter when all definition fields have
/// conditions and the prefix (everything except the last field) is equality-based.
fn nested_map_value<'a>(map: &'a Map<String, Value>, path: &str) -> Option<&'a Value> {
    let mut parts = path.split('.');
    let mut current = map.get(parts.next()?)?;
    for part in parts {
        current = current.as_object()?.get(part)?;
    }
    Some(current)
}

/// Returns the filter condition relevant to an index field. For a dotted field
/// such as `items.price`, a parent `$elemMatch` is translated into a condition
/// on that leaf path; the full filter is still applied as a residual.
fn index_condition<'a>(filter: &'a Map<String, Value>, field: &str) -> Option<&'a Value> {
    if let Some(cond) = filter.get(field) {
        if let Some(elem) = cond.as_object().and_then(|o| o.get("$elemMatch")) {
            if elem
                .as_object()
                .is_some_and(|o| o.keys().all(|k| k.starts_with('$')))
            {
                return Some(elem);
            }
        }
        return Some(cond);
    }
    let (parent, rest) = field.split_once('.')?;
    let elem = filter
        .get(parent)?
        .as_object()?
        .get("$elemMatch")?
        .as_object()?;
    nested_map_value(elem, rest)
}

fn validate_partial_filter(filter: Option<&Value>) -> Result<()> {
    let Some(filter) = filter else {
        return Ok(());
    };
    let Some(object) = filter.as_object() else {
        return Err(Error::Query(
            "partialFilterExpression must be a document".into(),
        ));
    };
    for key in object.keys() {
        if matches!(key.as_str(), "$where" | "$text" | "$expr" | "$jsonSchema") {
            return Err(Error::Query(format!(
                "partialFilterExpression does not support `{key}`"
            )));
        }
    }
    Ok(())
}

fn flatten_filter_conditions(filter: &Value) -> Option<Vec<(String, Value)>> {
    let obj = filter.as_object()?;
    if let Some(and) = obj.get("$and") {
        if obj.len() != 1 {
            return None;
        }
        let mut out = Vec::new();
        for branch in and.as_array()? {
            out.extend(flatten_filter_conditions(branch)?);
        }
        return Some(out);
    }
    if obj.keys().any(|key| key.starts_with('$')) {
        return None;
    }
    Some(
        obj.iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    )
}

fn comparable_range_values(left: &Value, right: &Value) -> bool {
    if left.is_number() && right.is_number() {
        return true;
    }
    if crate::date::is_date(left) && crate::date::is_date(right) {
        return true;
    }
    left.is_string() && right.is_string()
}

fn range_condition_implies(query: &Value, partial: &Value) -> bool {
    let (Some(query), Some(partial)) = (query.as_object(), partial.as_object()) else {
        return false;
    };
    if query.is_empty() || partial.is_empty() {
        return false;
    }
    let mut compared = false;
    for (partial_op, partial_value) in partial {
        let (query_value, strict) = match partial_op.as_str() {
            "$gte" => match (query.get("$gte"), query.get("$gt")) {
                (Some(value), _) => (value, false),
                (None, Some(value)) => (value, true),
                _ => return false,
            },
            "$gt" => match query.get("$gt") {
                Some(value) => (value, false),
                None => return false,
            },
            "$lte" => match (query.get("$lte"), query.get("$lt")) {
                (Some(value), _) => (value, false),
                (None, Some(value)) => (value, true),
                _ => return false,
            },
            "$lt" => match query.get("$lt") {
                Some(value) => (value, false),
                None => return false,
            },
            _ => return false,
        };
        if !comparable_range_values(query_value, partial_value) {
            return false;
        }
        let ordering = crate::aggregate::compare_value(query_value, partial_value);
        let valid = if partial_op == "$gte" || partial_op == "$gt" {
            ordering != Ordering::Less
        } else {
            ordering != Ordering::Greater
        };
        if !valid || (strict && ordering == Ordering::Equal) {
            return false;
        }
        compared = true;
    }
    compared
}

fn condition_implies(query: &Value, partial: &Value) -> bool {
    if query == partial {
        return true;
    }
    if let Some(value) = query.as_object().and_then(|object| object.get("$eq")) {
        if value == partial || range_condition_implies(&serde_json::json!({"$eq": value}), partial)
        {
            return true;
        }
    }
    if range_condition_implies(query, partial) {
        return true;
    }
    let query = query
        .as_object()
        .and_then(|object| object.get("$eq"))
        .unwrap_or(query);
    let partial = partial
        .as_object()
        .and_then(|object| object.get("$eq"))
        .unwrap_or(partial);
    if query == partial {
        return true;
    }
    query
        .as_object()
        .and_then(|object| object.get("$in"))
        .and_then(Value::as_array)
        .is_some_and(|values| values.iter().any(|value| value == partial))
}

/// A partial index may only serve a query when the query's top-level filter
/// explicitly includes the partial predicate. This is deliberately
/// conservative: it never assumes that an unrelated range implies a partial
/// filter, and it falls back to a collection scan when the implication is not
/// obvious.
fn partial_filter_allows(def: &IndexDef, query: &Value) -> bool {
    let Some(partial) = &def.partial_filter_expression else {
        return true;
    };
    let Some(partial_conditions) = flatten_filter_conditions(partial) else {
        return false;
    };
    if partial_conditions.is_empty() {
        return true;
    }
    let Some(query_conditions) = flatten_filter_conditions(query) else {
        return false;
    };
    for (field, partial_condition) in &partial_conditions {
        if !query_conditions
            .iter()
            .any(|(query_field, query_condition)| {
                query_field == field && condition_implies(query_condition, partial_condition)
            })
        {
            return false;
        }
    }
    true
}

fn bounds_for_index(def: &IndexDef, filter: &Map<String, Value>) -> Option<index::RangeBytes> {
    let fields = def.effective_fields();
    if fields.is_empty() {
        return None;
    }
    let directions = def.storage_directions();
    let mut prefix = Vec::new();
    for (i, field) in fields.iter().enumerate() {
        let cond = index_condition(filter, field)?;
        // `null` also matches a missing field. Plain indexes encode it as
        // null, but the planner remains conservative here.
        if cond.is_null()
            || cond
                .as_object()
                .and_then(|o| o.get("$eq"))
                .is_some_and(Value::is_null)
        {
            return None;
        }
        let direction = directions.get(i).copied().unwrap_or(1);
        if i + 1 == fields.len() {
            let bounds = index::bounds_from_cond_direction(cond, direction)?;
            return Some(if prefix.is_empty() {
                bounds
            } else {
                index::prefix_bounds(&prefix, bounds)
            });
        }
        let v = eq_value(cond)?;
        prefix.extend(index::encode_value_direction(v, direction)?);
    }
    Some(index::exact_prefix(&prefix))
}

fn explain_bound(bound: &Bound<Vec<u8>>, utf8: bool) -> Value {
    match bound {
        Bound::Unbounded => Value::Null,
        Bound::Included(bytes) => serde_json::json!({
            "value": explain_bytes(bytes, utf8),
            "inclusive": true,
        }),
        Bound::Excluded(bytes) => serde_json::json!({
            "value": explain_bytes(bytes, utf8),
            "inclusive": false,
        }),
    }
}

fn explain_bytes(bytes: &[u8], utf8: bool) -> String {
    if utf8 {
        return String::from_utf8_lossy(bytes).into_owned();
    }
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn is_match_all(filter: &Value) -> bool {
    filter.is_null() || filter.as_object().is_some_and(|o| o.is_empty())
}

/// Whether `count` can trust the index without reading documents.
///
/// We exclude `null` (the matcher treats a missing field as `null`, and missing fields
/// are not in the index) and operators the index does not support.
fn count_cond_safe(cond: &Value) -> bool {
    if date::is_date(cond) || numeric::is_numeric(cond) || bson_types::is_bson_value(cond) {
        return true;
    }
    match cond {
        Value::Null => false,
        Value::Object(ops) => {
            if ops.is_empty() || ops.values().any(Value::is_null) {
                return false;
            }
            // `$eq` only on its own; range operators may be combined (`$gte` + `$lt`).
            if ops.contains_key("$eq") && ops.len() != 1 {
                return false;
            }
            ops.keys()
                .all(|op| matches!(op.as_str(), "$eq" | "$gt" | "$gte" | "$lt" | "$lte"))
        }
        _ => true,
    }
}

/// The filter covers exactly all index fields (no extra conditions),
/// so the number of entries in the range equals the number of matching documents.
fn index_filter_exact(def: &IndexDef, filter: &Value) -> bool {
    let Some(obj) = filter.as_object() else {
        return false;
    };
    let fields = def.effective_fields();
    obj.len() == fields.len()
        && fields
            .iter()
            .all(|f| obj.get(f).is_some_and(count_cond_safe))
}

fn as_bound(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn value_at<'a>(doc: &'a Value, path_str: &str) -> Option<&'a Value> {
    path::get(doc, path_str)
}

/// Recognizes a filter equivalent to an `_id` lookup.
fn direct_id(filter: &Value) -> Option<String> {
    let obj = filter.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    let value = obj.get("_id")?;
    if let Some(id) = id::key_from_value(value) {
        return Some(id);
    }
    let op = value.as_object()?.get("$eq")?;
    id::key_from_value(op)
}

/// Recognizes a simple `{"_id": {"$in": [...]}}` filter and resolves each
/// value to its primary key. Duplicate values produce one result.
fn direct_ids_in(filter: &Value) -> Option<Vec<String>> {
    let branch = Collection::in_filter_branch(filter)?;
    let obj = branch.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    let ops = obj.get("_id")?.as_object()?;
    if ops.len() != 1 {
        return None;
    }
    let values = ops.get("$in")?.as_array()?;
    let mut seen = HashSet::new();
    let mut ids = Vec::with_capacity(values.len());
    for value in values {
        let id = id::key_from_value(value)?;
        if seen.insert(id.clone()) {
            ids.push(id);
        }
    }
    Some(ids)
}

/// Builds a primary-key `_id` range from `$gt/$gte/$lt/$lte` conditions.
/// An exact `_id` (string or `{"$eq": ...}`) is also a range, which allows
/// combining it with additional conditions without a full scan.
fn id_bounds(filter: &Value) -> Option<IdBounds> {
    let cond = filter.as_object()?.get("_id")?;
    if let Some(id) = id::key_from_value(cond) {
        let key = id.into_bytes();
        return Some((Bound::Included(key.clone()), Bound::Included(key)));
    }
    let ops = cond.as_object()?;
    if ops.is_empty() {
        return None;
    }
    if let Some(eq) = ops.get("$eq") {
        if ops.len() != 1 {
            return None;
        }
        let key = id::key_from_value(eq)?.into_bytes();
        return Some((Bound::Included(key.clone()), Bound::Included(key)));
    }

    let mut lower = Bound::Unbounded;
    let mut upper = Bound::Unbounded;
    let mut saw = false;
    for (op, value) in ops {
        let key = id::key_from_value(value)?.into_bytes();
        match op.as_str() {
            "$gt" => lower = tighter_lower(lower, Bound::Excluded(key)),
            "$gte" => lower = tighter_lower(lower, Bound::Included(key)),
            "$lt" => upper = tighter_upper(upper, Bound::Excluded(key)),
            "$lte" => upper = tighter_upper(upper, Bound::Included(key)),
            _ => return None,
        }
        saw = true;
    }
    saw.then_some((lower, upper))
}

fn tighter_lower(a: Bound<Vec<u8>>, b: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (a, b) {
        (Bound::Unbounded, b) => b,
        (a, Bound::Unbounded) => a,
        (Bound::Included(a), Bound::Included(b)) => Bound::Included(a.max(b)),
        (Bound::Excluded(a), Bound::Excluded(b)) => Bound::Excluded(a.max(b)),
        (Bound::Included(a), Bound::Excluded(b)) => {
            if a > b {
                Bound::Included(a)
            } else {
                Bound::Excluded(b)
            }
        }
        (Bound::Excluded(a), Bound::Included(b)) => {
            if b > a {
                Bound::Included(b)
            } else {
                Bound::Excluded(a)
            }
        }
    }
}

fn tighter_upper(a: Bound<Vec<u8>>, b: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (a, b) {
        (Bound::Unbounded, b) => b,
        (a, Bound::Unbounded) => a,
        (Bound::Included(a), Bound::Included(b)) => Bound::Included(a.min(b)),
        (Bound::Excluded(a), Bound::Excluded(b)) => Bound::Excluded(a.min(b)),
        (Bound::Included(a), Bound::Excluded(b)) => {
            if a < b {
                Bound::Included(a)
            } else {
                Bound::Excluded(b)
            }
        }
        (Bound::Excluded(a), Bound::Included(b)) => {
            if b < a {
                Bound::Included(b)
            } else {
                Bound::Excluded(a)
            }
        }
    }
}

/// Is the sort a single `_id`? Returns `Some(reverse)` for `-1`.
fn id_sort_direction(sort: &[(String, i32)]) -> Option<bool> {
    if sort.len() == 1 && sort[0].0 == "_id" {
        Some(sort[0].1 < 0)
    } else {
        None
    }
}

fn best_array_value(items: &[Value], descending: bool) -> Option<&Value> {
    let mut best: Option<&Value> = None;
    for item in items {
        let candidate = match item {
            Value::Array(inner) => best_array_value(inner, descending),
            _ => Some(item),
        };
        let Some(candidate) = candidate else {
            continue;
        };
        let replace = match best {
            None => true,
            Some(current) => {
                let ord = compare_scalar_values(candidate, current);
                if descending {
                    ord == Ordering::Greater
                } else {
                    ord == Ordering::Less
                }
            }
        };
        if replace {
            best = Some(candidate);
        }
    }
    best
}

fn sort_value(value: Option<&Value>, descending: bool) -> Option<&Value> {
    match value {
        Some(Value::Array(items)) => best_array_value(items, descending),
        other => other,
    }
}

fn compare_scalar_values(a: &Value, b: &Value) -> Ordering {
    if let Some(ord) = numeric::cmp_values(a, b) {
        return ord;
    }
    if let Some(ord) = bson_types::cmp_values(a, b) {
        return ord;
    }
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        _ => date::cmp(a, b).unwrap_or(Ordering::Equal),
    }
}

fn compare_values(a: Option<&Value>, b: Option<&Value>, descending: bool) -> Ordering {
    let a = sort_value(a, descending);
    let b = sort_value(b, descending);
    match (a, b) {
        (None, None) => Ordering::Equal,
        // MongoDB treats a missing field like an explicit null for sorting.
        (None, Some(Value::Null)) | (Some(Value::Null), None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less, // missing fields sort first, like MongoDB
        (Some(_), None) => Ordering::Greater,
        (Some(a), Some(b)) => compare_scalar_values(a, b),
    }
}

fn projection_is_id_only(projection: Option<&Value>) -> bool {
    projection
        .and_then(Value::as_object)
        .is_some_and(|projection| {
            projection.len() == 1 && projection.get("_id").is_some_and(truthy)
        })
}

#[derive(Hash, PartialEq, Eq)]
enum LookupKey<'a> {
    Str(&'a str),
    Json(String),
}

fn lookup_key(value: &Value) -> Result<LookupKey<'_>> {
    Ok(match value.as_str() {
        Some(value) => LookupKey::Str(value),
        None => LookupKey::Json(serde_json::to_string(value)?),
    })
}

fn push_lookup_result(
    out: &mut Vec<Option<Value>>,
    sizes: &mut Vec<usize>,
    bytes: &mut usize,
    doc: Option<Value>,
    size: usize,
) -> Result<()> {
    *bytes = bytes.saturating_add(size);
    if *bytes > MAX_LOOKUP_RESPONSE_BYTES {
        return Err(Error::ResultTooLarge(*bytes, MAX_LOOKUP_RESPONSE_BYTES));
    }
    out.push(doc);
    sizes.push(size);
    Ok(())
}

fn finish_lookup_stats(
    total: usize,
    unique_out: &[Option<Value>],
    sizes: &[usize],
    positions: &[usize],
    stats: &mut LookupStats,
) {
    let mut found = 0usize;
    let mut bytes = 0u64;
    for &index in positions {
        if unique_out.get(index).is_some_and(Option::is_some) {
            found = found.saturating_add(1);
        }
        if let Some(size) = sizes.get(index) {
            bytes = bytes.saturating_add(*size as u64);
        }
    }
    stats.found = found;
    stats.missing = total.saturating_sub(found);
    stats.result_bytes = bytes;
}

fn finish_lookup(
    total: usize,
    unique_out: Vec<Option<Value>>,
    unique_sizes: Vec<usize>,
    positions: &[usize],
) -> Result<Vec<Option<Value>>> {
    if unique_out.len() == total {
        return Ok(unique_out);
    }
    let mut out = Vec::with_capacity(total);
    let mut bytes = 0usize;
    for &index in positions {
        bytes = bytes.saturating_add(unique_sizes[index]);
        if bytes > MAX_LOOKUP_RESPONSE_BYTES {
            return Err(Error::ResultTooLarge(bytes, MAX_LOOKUP_RESPONSE_BYTES));
        }
        out.push(unique_out[index].clone());
    }
    Ok(out)
}

fn lookup_bounds(parts: &[Value], def: &IndexDef) -> Option<crate::index::RangeBytes> {
    let directions = def.storage_directions();
    let mut key = Vec::new();
    for (index, part) in parts.iter().enumerate() {
        let direction = directions.get(index).copied().unwrap_or(1);
        key.extend_from_slice(&index::encode_value_direction(part, direction)?);
    }
    Some((
        Bound::Included(key.clone()),
        Bound::Excluded(index::after_value(&key)),
    ))
}

/// Builds a document from covering-index fields when the filter and projection
/// fit within `inc`. Otherwise returns `None` (the document must be fetched).
fn covering_doc(
    id: &str,
    inc: Option<&Map<String, Value>>,
    filter: &Value,
    projection: &Option<Value>,
) -> Option<Value> {
    let inc = inc?;
    let proj = projection.as_ref()?.as_object()?;
    // Only inclusive projection: an exclusive projection needs the whole document.
    if !proj.iter().any(|(k, v)| k.as_str() != "_id" && truthy(v)) {
        return None;
    }
    for key in proj.keys().filter(|k| k.as_str() != "_id") {
        if key.contains('.') || key.starts_with('$') || !inc.contains_key(key) {
            return None;
        }
    }
    for key in filter.as_object()?.keys() {
        if key != "_id" && (key.contains('.') || !inc.contains_key(key)) {
            return None;
        }
    }
    let mut obj = inc.clone();
    obj.insert("_id".to_string(), Value::String(id.to_string()));
    Some(Value::Object(obj))
}

/// Does index order match the sort? Returns `Some(reverse)` when
/// the sort uses exactly the same fields (in the same order) and
/// all directions are ascending (`false`) or all descending (`true`).
fn index_matches_sort(def: &IndexDef, sort: &[(String, i32)]) -> Option<bool> {
    if def.geo {
        return None;
    }
    let fields = def.effective_fields();
    if fields.len() != sort.len() || sort.is_empty() {
        return None;
    }
    if fields.iter().zip(sort).any(|(f, (s, _))| f != s) {
        return None;
    }
    let directions = def.storage_directions();
    if sort
        .iter()
        .zip(&directions)
        .all(|((_, order), direction)| (*order >= 0) == (*direction >= 0))
    {
        return Some(false);
    }
    if sort
        .iter()
        .zip(&directions)
        .all(|((_, order), direction)| (*order >= 0) != (*direction >= 0))
    {
        return Some(true);
    }
    None
}

/// Precomputes the values used by `compare_docs` for a sort specification.
#[derive(Clone, Debug)]
pub(crate) struct WindowSortKey {
    value: Option<Value>,
    date_ms: Option<i64>,
}

pub(crate) fn window_sort_key(doc: &Value, sort: &[(String, i32)]) -> Vec<Option<WindowSortKey>> {
    sort.iter()
        .map(|(field, dir)| {
            sort_value(value_at(doc, field), *dir < 0)
                .cloned()
                .map(|value| WindowSortKey {
                    date_ms: date::parse_value(&value),
                    value: Some(value),
                })
        })
        .collect()
}

/// Compares two precomputed sort keys with the same semantics as
/// `compare_docs`.
pub(crate) fn compare_window_sort_keys(
    a: &[Option<WindowSortKey>],
    b: &[Option<WindowSortKey>],
    sort: &[(String, i32)],
) -> Ordering {
    for (index, (_, dir)) in sort.iter().enumerate() {
        let left = a.get(index).and_then(Option::as_ref);
        let right = b.get(index).and_then(Option::as_ref);
        let ord = match (
            left.and_then(|key| key.date_ms),
            right.and_then(|key| key.date_ms),
        ) {
            (Some(left), Some(right)) => left.cmp(&right),
            _ => compare_values(
                left.and_then(|key| key.value.as_ref()),
                right.and_then(|key| key.value.as_ref()),
                *dir < 0,
            ),
        };
        if ord != Ordering::Equal {
            return if *dir >= 0 { ord } else { ord.reverse() };
        }
    }
    Ordering::Equal
}

pub(crate) fn compare_docs(a: &Value, b: &Value, sort: &[(String, i32)]) -> Ordering {
    for (field, dir) in sort {
        let ord = compare_values(value_at(a, field), value_at(b, field), *dir < 0);
        if ord != Ordering::Equal {
            return if *dir >= 0 { ord } else { ord.reverse() };
        }
    }
    // The standard profile iterates LMDB keys in canonical `_id` order, so
    // ties after every sort field are deterministic by `_id`, regardless of
    // the requested direction. Columnar locators are hash ordered, so apply
    // the same tie-break explicitly.
    let a = a.get("_id").and_then(crate::id::key_from_value);
    let b = b.get("_id").and_then(crate::id::key_from_value);
    match (a, b) {
        (Some(a), Some(b)) => a.cmp(&b),
        _ => Ordering::Equal,
    }
}

/// `find` result accumulator: a plain list without sorting, TopK with sorting.
enum Acc<'a> {
    All(Vec<Value>),
    Top(TopK<'a>),
}

impl<'a> Acc<'a> {
    fn new(sorted: bool, sort: &'a [(String, i32)], want: usize) -> Self {
        if sorted {
            Acc::Top(TopK::new(sort, want))
        } else {
            Acc::All(Vec::new())
        }
    }

    fn push(&mut self, doc: Value) {
        match self {
            Acc::All(docs) => docs.push(doc),
            Acc::Top(top) => top.push(doc),
        }
    }

    fn len(&self) -> usize {
        match self {
            Acc::All(docs) => docs.len(),
            Acc::Top(top) => top.heap.len(),
        }
    }

    fn finish(self) -> Vec<Value> {
        match self {
            Acc::All(mut docs) => {
                // Deterministic order of the returned set.
                docs.sort_by(|a, b| {
                    let ka = id::key_from_document(a).unwrap_or_default();
                    let kb = id::key_from_document(b).unwrap_or_default();
                    ka.cmp(&kb)
                });
                docs
            }
            Acc::Top(top) => top.into_sorted(),
        }
    }
}

/// Selects the best `want` documents with the same comparator and heap
/// algorithm as the standard profile. Columnar reads use this instead of
/// `slice::sort_by` so inconsistent mixed-type comparators behave identically.
pub(crate) fn top_k_docs<I>(docs: I, sort: &[(String, i32)], want: usize) -> Vec<Value>
where
    I: IntoIterator<Item = Value>,
{
    let mut top = TopK::new(sort, want);
    for doc in docs {
        top.push(doc);
    }
    top.into_sorted()
}

/// Document in the TopK heap; `seq` preserves stability for equal keys.
struct TopDoc<'a> {
    doc: Value,
    seq: usize,
    sort: &'a [(String, i32)],
}

impl PartialEq for TopDoc<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for TopDoc<'_> {}

impl PartialOrd for TopDoc<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopDoc<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Larger = worse, so the worst candidate is at the top of the heap.
        match compare_docs(&self.doc, &other.doc, self.sort) {
            Ordering::Equal => self.seq.cmp(&other.seq),
            ord => ord,
        }
    }
}

/// Bounded selection of the `want` best documents without sorting everything.
struct TopK<'a> {
    sort: &'a [(String, i32)],
    want: usize,
    seq: usize,
    heap: BinaryHeap<TopDoc<'a>>,
}

impl<'a> TopK<'a> {
    fn new(sort: &'a [(String, i32)], want: usize) -> Self {
        Self {
            sort,
            want,
            seq: 0,
            heap: BinaryHeap::new(),
        }
    }

    fn push(&mut self, doc: Value) {
        self.seq += 1;
        self.push_seq(doc, self.seq);
    }

    /// Inserts a document with a global sequence number (sort stability).
    fn push_seq(&mut self, doc: Value, seq: usize) {
        if self.want == 0 {
            return;
        }
        let entry = TopDoc {
            doc,
            seq,
            sort: self.sort,
        };
        if self.heap.len() < self.want {
            self.heap.push(entry);
        } else if self
            .heap
            .peek()
            .is_some_and(|worst| entry.cmp(worst) == Ordering::Less)
        {
            self.heap.pop();
            self.heap.push(entry);
        }
    }

    fn into_sorted(self) -> Vec<Value> {
        self.into_sorted_seq()
            .into_iter()
            .map(|(_, doc)| doc)
            .collect()
    }

    fn into_sorted_seq(self) -> Vec<(usize, Value)> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|e| (e.seq, e.doc))
            .collect()
    }
}

fn truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Number(_) => numeric::parse_value(v)
            .is_some_and(|n| !matches!(n.value(), numeric::Numeric::Finite { len: 1, digits, .. } if digits[0] == 0)),
        Value::Null => false,
        _ => {
            if let Some(n) = numeric::parse_value(v) {
                !matches!(n.value(), numeric::Numeric::Finite { len: 1, digits, .. } if digits[0] == 0)
            } else {
                true
            }
        }
    }
}

/// Top-level projection: include or exclude mode (like MongoDB).
fn validate_find_and_modify(opts: &FindAndModifyOptions<'_>) -> Result<()> {
    if opts.remove && opts.update.is_some() {
        return Err(Error::Query(
            "find_and_modify cannot combine `remove` and `update`".into(),
        ));
    }
    if !opts.remove && opts.update.is_none() {
        return Err(Error::Query(
            "find_and_modify requires `update` or `remove`".into(),
        ));
    }
    if opts.remove && opts.upsert {
        return Err(Error::Query(
            "find_and_modify cannot combine `remove` and `upsert`".into(),
        ));
    }
    if opts.remove && opts.new {
        return Err(Error::Query(
            "find_and_modify cannot return a new document when removing".into(),
        ));
    }
    Ok(())
}

fn empty_find_and_modify() -> FindAndModifyOutcome {
    FindAndModifyOutcome {
        value: None,
        matched: 0,
        modified: 0,
        upserted_id: None,
        new_id: None,
    }
}

fn apply_projection(doc: Value, projection: Option<&Value>, filter: Option<&Value>) -> Value {
    match projection {
        Some(p) => project(&doc, p, filter),
        None => doc,
    }
}

/// Applies a normal `find` projection. Used by streaming cursor pages, which
/// need the raw `_id` for the next page before hiding it from the caller.
pub fn project_document(doc: &Value, projection: Option<&Value>) -> Value {
    match projection {
        Some(projection) => project(doc, projection, None),
        None => doc.clone(),
    }
}

fn projection_integer(value: &Value) -> Option<i64> {
    numeric::integer_value(value).and_then(|value| i64::try_from(value).ok())
}

fn slice_projection(value: &Value, arg: &Value) -> Value {
    let Some(items) = value.as_array() else {
        return value.clone();
    };
    let len = items.len();
    let (skip, limit) = if let Some(n) = projection_integer(arg) {
        if n >= 0 {
            (0usize, usize::try_from(n).unwrap_or(usize::MAX).min(len))
        } else {
            let take = usize::try_from(n.unsigned_abs())
                .unwrap_or(usize::MAX)
                .min(len);
            (len - take, take)
        }
    } else if let Some(pair) = arg.as_array() {
        if pair.len() != 2 {
            return value.clone();
        }
        let Some(skip) = projection_integer(&pair[0]) else {
            return value.clone();
        };
        let Some(limit) = projection_integer(&pair[1]) else {
            return value.clone();
        };
        if limit <= 0 {
            return Value::Array(Vec::new());
        }
        let start = if skip >= 0 {
            usize::try_from(skip).unwrap_or(usize::MAX).min(len)
        } else {
            len.saturating_sub(
                usize::try_from(skip.unsigned_abs())
                    .unwrap_or(usize::MAX)
                    .min(len),
            )
        };
        (
            start,
            usize::try_from(limit)
                .unwrap_or(usize::MAX)
                .min(len.saturating_sub(start)),
        )
    } else {
        return value.clone();
    };
    let end = skip.saturating_add(limit).min(len);
    Value::Array(items[skip..end].to_vec())
}

fn merge_projection_value(dst: &mut Value, src: Value) {
    match (dst, src) {
        (Value::Object(dst), Value::Object(src)) => merge_projection_object(dst, src),
        (Value::Array(dst), Value::Array(src)) if dst.len() == src.len() => {
            for (dst, src) in dst.iter_mut().zip(src) {
                merge_projection_value(dst, src);
            }
        }
        (dst, src) => *dst = src,
    }
}

fn merge_projection_object(dst: &mut Map<String, Value>, src: Map<String, Value>) {
    for (key, value) in src {
        match dst.get_mut(&key) {
            Some(existing) => merge_projection_value(existing, value),
            None => {
                dst.insert(key, value);
            }
        }
    }
}

fn merge_projection_fragment(out: &mut Map<String, Value>, fragment: Value) {
    if let Value::Object(fragment) = fragment {
        merge_projection_object(out, fragment);
    }
}

/// Applies a projection modifier such as `$slice`, `$elemMatch`, or `$meta`.
fn projection_modifier(value: &Value, modifier: &Value, root: &Value) -> Option<Value> {
    let Some(obj) = modifier.as_object() else {
        return Some(value.clone());
    };
    if let Some(slice) = obj.get("$slice") {
        return Some(slice_projection(value, slice));
    }
    if let Some(elem) = obj.get("$elemMatch") {
        let Value::Array(items) = value else {
            return None;
        };
        return items
            .iter()
            .find(|item| crate::query::matcher::elem_item_matches(item, elem))
            .map(|found| Value::Array(vec![found.clone()]));
    }
    if let Some(meta) = obj.get("$meta").and_then(Value::as_str) {
        return match meta {
            "textScore" | "searchScore" => Some(
                root.get("_score")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!(0.0)),
            ),
            "indexKey" => Some(
                root.get("_indexKey")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({})),
            ),
            "sortKey" => Some(
                root.get("_sortKey")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({})),
            ),
            _ => None,
        };
    }
    Some(value.clone())
}

/// Extracts a nested projection fragment. Arrays are traversed element-wise so
/// `items.name` becomes `{"items": [{"name": ...}, ...]}`.
fn extract_projection_path(
    value: &Value,
    parts: &[&str],
    modifier: &Value,
    root: &Value,
) -> Option<Value> {
    let Some((part, rest)) = parts.split_first() else {
        return projection_modifier(value, modifier, root);
    };

    if let Value::Object(obj) = value {
        let child = match obj.get(*part) {
            Some(child) => child,
            None if rest.is_empty()
                && modifier
                    .as_object()
                    .is_some_and(|obj| obj.contains_key("$meta")) =>
            {
                let projected = projection_modifier(&Value::Null, modifier, root)?;
                let mut out = Map::new();
                out.insert((*part).to_string(), projected);
                return Some(Value::Object(out));
            }
            None => return None,
        };
        let projected = extract_projection_path(child, rest, modifier, root)?;
        let mut out = Map::new();
        out.insert((*part).to_string(), projected);
        return Some(Value::Object(out));
    }

    if let Value::Array(items) = value {
        if let Ok(index) = part.parse::<usize>() {
            let child = items.get(index)?;
            let projected = extract_projection_path(child, rest, modifier, root)?;
            return Some(Value::Array(vec![projected]));
        }
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            out.push(
                extract_projection_path(item, parts, modifier, root)
                    .unwrap_or_else(|| Value::Object(Map::new())),
            );
        }
        return Some(Value::Array(out));
    }

    None
}

fn positional_element<'a>(
    doc: &Value,
    array_path: &str,
    items: &'a [Value],
    filter: Option<&Value>,
) -> Option<&'a Value> {
    if let Some(filter) = filter {
        for item in items {
            let mut probe = doc.clone();
            if crate::query::path::set(&mut probe, array_path, serde_json::json!([item.clone()]))
                .is_err()
            {
                break;
            }
            if crate::query::matcher::matches(&probe, filter) {
                return Some(item);
            }
        }
    }
    items.first()
}

fn positional_projection(
    doc: &Value,
    projection_path: &str,
    modifier: &Value,
    filter: Option<&Value>,
) -> Option<Value> {
    let array_path = projection_path.strip_suffix(".$")?;
    let items = crate::query::path::get(doc, array_path)?.as_array()?;
    let item = positional_element(doc, array_path, items, filter)?;
    let selected = projection_modifier(item, modifier, doc)?;
    let mut fragment = Value::Object(Map::new());
    crate::query::path::set(&mut fragment, array_path, Value::Array(vec![selected])).ok()?;
    Some(fragment)
}

fn exclude_projection_path(value: &mut Value, parts: &[&str]) {
    let Some((part, rest)) = parts.split_first() else {
        return;
    };
    match value {
        Value::Object(obj) => {
            if rest.is_empty() {
                obj.remove(*part);
            } else if let Some(child) = obj.get_mut(*part) {
                exclude_projection_path(child, rest);
            }
        }
        Value::Array(items) => {
            if let Ok(index) = part.parse::<usize>() {
                if let Some(child) = items.get_mut(index) {
                    exclude_projection_path(child, rest);
                }
            } else {
                for item in items {
                    exclude_projection_path(item, parts);
                }
            }
        }
        _ => {}
    }
}

fn project(doc: &Value, projection: &Value, filter: Option<&Value>) -> Value {
    let Some(p) = projection.as_object() else {
        return doc.clone();
    };
    if p.is_empty() {
        return doc.clone();
    }

    let include_mode = p.iter().any(|(k, v)| k.as_str() != "_id" && truthy(v));

    if include_mode {
        let mut out = Map::new();
        // `_id` is included by default unless the projection explicitly sets `_id: 0`.
        let include_id = p.get("_id").map(truthy).unwrap_or(true);
        if include_id {
            if let Some(id) = doc.get("_id") {
                out.insert("_id".to_string(), id.clone());
            }
        }
        for (k, v) in p {
            if k == "_id" || !truthy(v) {
                continue;
            }
            let fragment = if k.ends_with(".$") {
                positional_projection(doc, k, v, filter)
            } else {
                let parts: Vec<&str> = k.split('.').collect();
                extract_projection_path(doc, &parts, v, doc)
            };
            if let Some(fragment) = fragment {
                merge_projection_fragment(&mut out, fragment);
            }
        }
        Value::Object(out)
    } else {
        let mut out = doc.clone();
        for (k, v) in p {
            if !truthy(v) {
                let parts: Vec<&str> = k.split('.').collect();
                exclude_projection_path(&mut out, &parts);
            }
        }
        out
    }
}

fn graph_push_value(value: &Value, out: &mut Vec<Value>, seen: &mut HashSet<String>) {
    if let Value::Array(items) = value {
        for item in items {
            graph_push_value(item, out, seen);
        }
        return;
    }
    let key = serde_json::to_string(value).unwrap_or_default();
    if seen.insert(key) {
        out.push(value.clone());
    }
}

fn graph_collect_field(doc: &Value, field: &str) -> Vec<Value> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for value in path::get_values(doc, field) {
        graph_push_value(value, &mut out, &mut seen);
    }
    out
}

fn graph_identity(doc: &Value) -> String {
    match doc.get("_id") {
        Some(id) => format!("id:{}", serde_json::to_string(id).unwrap_or_default()),
        None => format!("doc:{}", serde_json::to_string(doc).unwrap_or_default()),
    }
}

struct LookupHost<'a, 'e, 'r> {
    coll: &'a Collection<'e>,
    rtxn: &'a RoTxn<'r>,
}

impl aggregate::AggregateHost for LookupHost<'_, '_, '_> {
    fn lookup(
        &self,
        docs: Vec<Value>,
        spec: &Value,
        vars: &BTreeMap<String, Value>,
    ) -> Result<Vec<Value>> {
        self.coll.lookup_stage_in(self.rtxn, docs, spec, vars)
    }

    fn union_with(&self, spec: &Value, vars: &BTreeMap<String, Value>) -> Result<Vec<Value>> {
        self.coll.union_stage_in(self.rtxn, spec, vars)
    }

    fn graph_lookup(
        &self,
        docs: Vec<Value>,
        spec: &Value,
        vars: &BTreeMap<String, Value>,
    ) -> Result<Vec<Value>> {
        self.coll.graph_lookup_stage_in(self.rtxn, docs, spec, vars)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;
    use serde_json::json;

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        (dir, store)
    }

    fn history_store(max_versions: usize) -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store
            .create_collection("users", crate::vault::CollectionProfile::Standard)
            .unwrap();
        store.set_collection_history("users", max_versions).unwrap();
        (dir, store)
    }

    #[test]
    fn history_records_versions_and_get_version() {
        let (_dir, store) = history_store(50);
        let c = store.collection("users").unwrap();

        let id = c.insert_one(json!({"name": "Ala", "n": 1})).unwrap();
        let first = c.history(&id, None, true).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].op, "insert");
        assert_eq!(first[0].doc.as_ref().unwrap()["n"], 1);
        let first_rev = first[0].rev;

        assert_eq!(
            c.update(&json!({"_id": id}), &json!({"$set": {"n": 2}}), false)
                .unwrap(),
            1
        );
        let versions = c.history(&id, None, true).unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].op, "update");
        assert_eq!(versions[0].doc.as_ref().unwrap()["n"], 2);
        assert!(versions[0].rev > first_rev);

        let old = c.get_version(&id, first_rev).unwrap().unwrap();
        assert_eq!(old.doc.as_ref().unwrap()["n"], 1);

        let metadata = c.history(&id, None, false).unwrap();
        assert_eq!(metadata.len(), 2);
        assert!(metadata.iter().all(|v| v.doc.is_none()));

        assert_eq!(c.delete(&json!({"_id": id}), false).unwrap(), 1);
        let deleted = c.history(&id, None, false).unwrap();
        assert_eq!(deleted[0].op, "delete");
        assert!(deleted[0].deleted);
        assert!(deleted[0].doc.is_none());
        assert_eq!(
            c.get_version(&id, versions[0].rev).unwrap().unwrap().op,
            "update"
        );
    }

    #[test]
    fn history_is_disabled_by_default() {
        let (_dir, store) = store();
        let c = store.collection("users").unwrap();
        let id = c.insert_one(json!({"name": "Ala"})).unwrap();
        assert!(c.history(&id, None, true).unwrap().is_empty());
    }

    #[test]
    fn history_is_configured_per_collection() {
        let (_dir, store) = store();
        let _ = store.collection("enabled").unwrap();
        let disabled = store.collection("disabled").unwrap();
        store.set_collection_history("enabled", 3).unwrap();
        let enabled = store.collection("enabled").unwrap();
        assert_eq!(store.collection_history("enabled").unwrap(), 3);
        assert_eq!(store.collection_history("disabled").unwrap(), 0);

        let eid = enabled.insert_one(json!({"n": 1})).unwrap();
        let did = disabled.insert_one(json!({"n": 1})).unwrap();
        assert_eq!(enabled.history(&eid, None, true).unwrap().len(), 1);
        assert!(disabled.history(&did, None, true).unwrap().is_empty());

        store.set_collection_history("enabled", 0).unwrap();
        let enabled = store.collection("enabled").unwrap();
        assert_eq!(store.collection_history("enabled").unwrap(), 0);
        enabled
            .update(&json!({"_id": eid}), &json!({"$set": {"n": 2}}), false)
            .unwrap();
        assert_eq!(enabled.history(&eid, None, true).unwrap().len(), 1);
    }

    #[test]
    fn history_prunes_old_versions() {
        let (_dir, store) = history_store(2);
        let c = store.collection("users").unwrap();
        let id = c.insert_one(json!({"n": 0})).unwrap();
        for n in 1..=4 {
            assert_eq!(
                c.update(&json!({"_id": id}), &json!({"$set": {"n": n}}), false)
                    .unwrap(),
                1
            );
        }
        assert_eq!(c.history(&id, None, false).unwrap().len(), 5);

        assert_eq!(store.prune_history(100).unwrap(), 3);
        let kept = c.history(&id, None, true).unwrap();
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].doc.as_ref().unwrap()["n"], 4);
        assert_eq!(kept[1].doc.as_ref().unwrap()["n"], 3);
    }

    #[test]
    fn history_prunes_with_per_collection_limits() {
        let (_dir, store) = store();
        for name in ["a", "b"] {
            store
                .create_collection(name, crate::vault::CollectionProfile::Standard)
                .unwrap();
        }
        store.set_collection_history("a", 2).unwrap();
        store.set_collection_history("b", 1).unwrap();
        let a = store.collection("a").unwrap();
        let b = store.collection("b").unwrap();
        let aid = a.insert_one(json!({"n": 0})).unwrap();
        let bid = b.insert_one(json!({"n": 0})).unwrap();
        for n in 1..=4 {
            a.update(&json!({"_id": aid}), &json!({"$set": {"n": n}}), false)
                .unwrap();
            b.update(&json!({"_id": bid}), &json!({"$set": {"n": n}}), false)
                .unwrap();
        }
        assert_eq!(store.prune_history(100).unwrap(), 7);
        assert_eq!(a.history(&aid, None, true).unwrap().len(), 2);
        assert_eq!(b.history(&bid, None, true).unwrap().len(), 1);
    }

    #[test]
    fn history_prunes_all_documents_across_passes() {
        let (_dir, store) = history_store(2);
        let c = store.collection("users").unwrap();
        let ids: Vec<String> = (0..3)
            .map(|n| c.insert_one(json!({"n": n, "v": 0})).unwrap())
            .collect();
        for id in &ids {
            for n in 1..=2 {
                c.update(&json!({"_id": id}), &json!({"$set": {"v": n}}), false)
                    .unwrap();
            }
        }

        // The deletion limit forces several bounded maintenance passes.
        while store.prune_history(1).unwrap() > 0 {}
        for id in &ids {
            let kept = c.history(id, None, true).unwrap();
            assert_eq!(kept.len(), 2);
            assert_eq!(kept[0].doc.as_ref().unwrap()["v"], 2);
            assert_eq!(kept[1].doc.as_ref().unwrap()["v"], 1);
        }
    }

    #[test]
    fn object_id_can_be_used_as_primary_key() {
        let (_dir, store) = store();
        let c = store.collection("object_ids").unwrap();
        let oid = json!({"$oid": "000000000000000000000001"});
        let key = c
            .insert_one(json!({"_id": oid.clone(), "value": 1}))
            .unwrap();
        assert!(crate::id::is_object_id_key(&key));

        let found = c.find_one(&json!({"_id": oid.clone()})).unwrap().unwrap();
        assert_eq!(found["_id"], oid);
        assert_eq!(found["value"], 1);

        let updated = c
            .update(
                &json!({"_id": {"$eq": oid.clone()}}),
                &json!({"$set": {"value": 2}}),
                false,
            )
            .unwrap();
        assert_eq!(updated, 1);
        assert_eq!(
            c.find_one(&json!({"_id": oid.clone()})).unwrap().unwrap()["value"],
            2
        );

        let removed = c.delete(&json!({"_id": oid.clone()}), false).unwrap();
        assert_eq!(removed, 1);
        assert!(c.find_one(&json!({"_id": oid})).unwrap().is_none());
    }

    #[test]
    fn insert_find_update_delete() {
        let (_dir, store) = store();
        let c = store.collection("users").unwrap();

        let id = c.insert_one(json!({"name": "Ala", "age": 30})).unwrap();
        c.insert_one(json!({"name": "Bob", "age": 17})).unwrap();
        assert!(c.insert_one(json!({"_id": id, "name": "dup"})).is_err());

        let q = FindQuery {
            filter: json!({"age": {"$gte": 18}}),
            ..Default::default()
        };
        let found = c.find(&q).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0]["name"], "Ala");

        let n = c
            .update(&json!({"name": "Ala"}), &json!({"$inc": {"age": 1}}), false)
            .unwrap();
        assert_eq!(n, 1);
        let ala = c.find_one(&json!({"name": "Ala"})).unwrap().unwrap();
        assert_eq!(ala["age"], 31);

        let d = c.delete(&json!({"age": {"$lt": 18}}), true).unwrap();
        assert_eq!(d, 1);
        assert_eq!(c.count(&serde_json::json!({})).unwrap(), 1);
    }

    #[test]
    fn expr_mod_and_bitwise_filters_run_through_find() {
        let (_dir, store) = store();
        let c = store.collection("exprs").unwrap();
        c.insert_one(json!({"_id": "a", "spent": 12, "budget": 10, "flags": 0b1010}))
            .unwrap();
        c.insert_one(json!({"_id": "b", "spent": 7, "budget": 10, "flags": 0b0101}))
            .unwrap();

        let over = c
            .find(&FindQuery {
                filter: json!({"$expr": {"$gt": ["$spent", "$budget"]}}),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(over.len(), 1);
        assert_eq!(over[0]["_id"], "a");
        assert_eq!(c.count(&json!({"spent": {"$mod": [5, 2]}})).unwrap(), 2);
        assert_eq!(
            c.count(&json!({"flags": {"$bitsAllSet": [1, 3]}})).unwrap(),
            1
        );
        assert!(c
            .find(&FindQuery {
                filter: json!({"spent": {"$mod": [0, 0]}}),
                ..Default::default()
            })
            .is_err());
        assert!(c
            .find(&FindQuery {
                filter: json!({"flags": {"$bitsAllSet": [64]}}),
                ..Default::default()
            })
            .is_err());
    }

    #[test]
    fn json_schema_filter_and_validation() {
        let (_dir, store) = store();
        let c = store.collection("schemas").unwrap();
        c.insert_one(json!({"_id": "a", "name": "Ala", "age": 30}))
            .unwrap();
        c.insert_one(json!({"_id": "b", "name": "Bob"})).unwrap();

        let found = c
            .find(&FindQuery {
                filter: json!({"$jsonSchema": {
                    "required": ["age"],
                    "properties": {
                        "name": {"bsonType": "string", "minLength": 2},
                        "age": {"bsonType": "number", "minimum": 18}
                    }
                }}),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0]["_id"], "a");

        assert!(c
            .find(&FindQuery {
                filter: json!({"$jsonSchema": {"required": "age"}}),
                ..Default::default()
            })
            .is_err());
        assert!(c
            .find(&FindQuery {
                filter: json!({"$jsonSchema": {"bsonType": "not-a-type"}}),
                ..Default::default()
            })
            .is_err());
    }

    #[test]
    fn raw_prefilter_avoids_false_negatives() {
        let filter = json!({"name": "a\"b\n", "active": true});
        let raw = raw_prefilter(&filter).unwrap();
        let matching = json!({"_id": "1", "name": "a\"b\n", "active": true, "junk": "x"});
        let bytes = serde_json::to_vec(&matching).unwrap();
        assert!(raw.matches(&bytes));

        let wrong_bool = json!({"_id": "1", "name": "a\"b\n", "active": false});
        assert!(!raw.matches(&serde_json::to_vec(&wrong_bool).unwrap()));

        let wrong_string = json!({"_id": "1", "name": "other", "active": true});
        assert!(!raw.matches(&serde_json::to_vec(&wrong_string).unwrap()));

        // `$or` cannot be used as a prefilter.
        assert!(raw_prefilter(&json!({"$or": [{"name": "a"}, {"name": "b"}]})).is_none());
        // A number on its own requires the parser (e.g. 1 vs 1.0).
        assert!(raw_prefilter(&json!({"n": 1})).is_none());
    }

    #[test]
    fn raw_prefilter_preserves_string_and_number_semantics() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        c.insert_one(json!({"_id": "a", "name": "a\"b\n", "n": 1, "tags": ["x"]}))
            .unwrap();
        c.insert_one(json!({"_id": "b", "name": "other", "n": 2, "tags": ["y"]}))
            .unwrap();

        assert_eq!(
            c.find_one(&json!({"name": "a\"b\n"})).unwrap().unwrap()["_id"],
            "a"
        );
        assert_eq!(
            c.find_one(&json!({"name": {"$eq": "other"}}))
                .unwrap()
                .unwrap()["_id"],
            "b"
        );
        assert_eq!(
            c.find_one(&json!({"tags": "x"})).unwrap().unwrap()["_id"],
            "a"
        );
        // The prefilter uses only the string; the number is still compared by the matcher.
        assert_eq!(
            c.find_one(&json!({"name": "a\"b\n", "n": 1.0}))
                .unwrap()
                .unwrap()["_id"],
            "a"
        );
        assert!(c
            .find_one(&json!({"name": "a\"b\n", "n": 2}))
            .unwrap()
            .is_none());
    }

    #[test]
    fn parallel_sorted_scan_matches_serial() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..1000 {
            c.insert_one(json!({
                "_id": format!("id{i:04}"),
                "g": i % 7,
                "n": i,
                "junk": "x",
            }))
            .unwrap();
        }
        let q = FindQuery {
            filter: json!({"n": {"$gte": 100}}),
            projection: Some(json!({"g": 1, "n": 1, "_id": 0})),
            sort: vec![("g".to_string(), 1), ("n".to_string(), -1)],
            skip: 7,
            limit: Some(31),
            comment: None,
            max_time_ms: None,
            hint: None,
        };
        // `find` takes the parallel path (1000 entries > test threshold).
        let parallel = c.find(&q).unwrap();
        // `find_in` is the same scan but single-threaded.
        let rtxn = store.read_txn().unwrap();
        let serial = c.find_in(&rtxn, &q).unwrap();
        rtxn.commit().unwrap();
        assert_eq!(parallel, serial);
        assert_eq!(parallel.len(), 31);
        for pair in parallel.windows(2) {
            let a = (
                pair[0]["g"].as_i64().unwrap(),
                -pair[0]["n"].as_i64().unwrap(),
            );
            let b = (
                pair[1]["g"].as_i64().unwrap(),
                -pair[1]["n"].as_i64().unwrap(),
            );
            assert!(a <= b, "niestabilna kolejność: {a:?} > {b:?}");
        }
    }

    #[test]
    fn parallel_unsorted_scan_matches_serial() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..1000 {
            c.insert_one(json!({
                "_id": format!("id{i:04}"),
                "g": i % 7,
                "n": i,
                "junk": "x",
            }))
            .unwrap();
        }
        let q = FindQuery {
            filter: json!({"n": {"$gte": 100}}),
            projection: Some(json!({"g": 1, "n": 1, "_id": 0})),
            skip: 7,
            limit: Some(31),
            ..Default::default()
        };
        // `find` takes the parallel scan without sorting.
        let parallel = c.find(&q).unwrap();
        let rtxn = store.read_txn().unwrap();
        let serial = c.find_in(&rtxn, &q).unwrap();
        rtxn.commit().unwrap();
        assert_eq!(parallel, serial);
        assert_eq!(parallel.len(), 31);
    }

    #[test]
    fn id_range_uses_primary_key_order() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..20 {
            c.insert_one(json!({"_id": format!("id{i:02}"), "n": i}))
                .unwrap();
        }

        // `_id` range plus an extra condition; natural `_id` order.
        let q = FindQuery {
            filter: json!({"_id": {"$gte": "id05", "$lt": "id09"}, "n": {"$gte": 6}}),
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        let ids: Vec<_> = got.iter().map(|d| d["_id"].as_str().unwrap()).collect();
        assert_eq!(ids, ["id06", "id07", "id08"]);

        // Descending `_id` sorting still uses the range.
        let q = FindQuery {
            filter: json!({"_id": {"$gt": "id05", "$lte": "id09"}}),
            sort: vec![("_id".to_string(), -1)],
            limit: Some(2),
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        let ids: Vec<_> = got.iter().map(|d| d["_id"].as_str().unwrap()).collect();
        assert_eq!(ids, ["id09", "id08"]);

        // An exact `_id` with an extra condition does not require a full scan.
        assert!(c
            .find_one(&json!({"_id": "id07", "n": 7}))
            .unwrap()
            .is_some());
        assert!(c
            .find_one(&json!({"_id": "id07", "n": 8}))
            .unwrap()
            .is_none());

        assert_eq!(
            c.count(&json!({"_id": {"$gte": "id05", "$lt": "id09"}}))
                .unwrap(),
            4
        );
        assert_eq!(
            c.count(&json!({"_id": {"$gte": "id05", "$lt": "id09"}, "n": {"$gte": 7}}))
                .unwrap(),
            2
        );

        // An empty or reversed range must not fall back to a full scan.
        assert!(c
            .find(&FindQuery {
                filter: json!({"_id": {"$gt": "id09", "$lt": "id05"}}),
                limit: Some(10),
                ..Default::default()
            })
            .unwrap()
            .is_empty());
        assert_eq!(
            c.count(&json!({"_id": {"$gt": "id09", "$lt": "id05"}}))
                .unwrap(),
            0
        );
    }

    #[test]
    fn id_range_update_delete() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..20 {
            c.insert_one(json!({"_id": format!("id{i:02}"), "n": i}))
                .unwrap();
        }
        let n = c
            .update(
                &json!({"_id": {"$gte": "id05", "$lt": "id09"}}),
                &json!({"$set": {"flag": true}}),
                true,
            )
            .unwrap();
        assert_eq!(n, 4);
        assert_eq!(c.count(&json!({"flag": true})).unwrap(), 4);

        let n = c
            .delete(&json!({"_id": {"$gte": "id06", "$lte": "id08"}}), true)
            .unwrap();
        assert_eq!(n, 3);
        assert_eq!(c.count(&serde_json::json!({})).unwrap(), 17);
    }

    #[test]
    fn sort_projection_pagination() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..5 {
            c.insert_one(json!({"i": i, "even": i % 2 == 0, "junk": "x"}))
                .unwrap();
        }
        let q = FindQuery {
            filter: serde_json::json!({}),
            projection: Some(json!({"i": 1, "_id": 0})),
            sort: vec![("i".to_string(), -1)],
            skip: 1,
            limit: Some(2),
            comment: None,
            max_time_ms: None,
            hint: None,
        };
        let got = c.find(&q).unwrap();
        assert_eq!(got, vec![json!({"i": 3}), json!({"i": 2})]);
    }

    #[test]
    fn projection_slice_and_elem_match() {
        let (_dir, store) = store();
        let c = store.collection("projected").unwrap();
        c.insert_one(json!({
            "_id": "a",
            "tags": ["a", "b", "c", "d"],
            "items": [{"sku": "x", "qty": 1}, {"sku": "y", "qty": 5}]
        }))
        .unwrap();

        let first = c
            .find(&FindQuery {
                filter: serde_json::json!({}),
                projection: Some(json!({
                    "tags": {"$slice": 2},
                    "items": {"$elemMatch": {"qty": {"$gt": 3}}},
                    "_id": 0
                })),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            first,
            vec![json!({
                "tags": ["a", "b"],
                "items": [{"sku": "y", "qty": 5}]
            })]
        );

        let last = c
            .find(&FindQuery {
                filter: serde_json::json!({}),
                projection: Some(json!({"tags": {"$slice": [-2, 1]}, "_id": 0})),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(last, vec![json!({"tags": ["c"]})]);

        let extended = c
            .find(&FindQuery {
                filter: serde_json::json!({}),
                projection: Some(json!({
                    "tags": {"$slice": {"$numberInt": "2"}},
                    "_id": 0
                })),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(extended, vec![json!({"tags": ["a", "b"]})]);
    }

    #[test]
    fn nested_positional_and_meta_projections() {
        let (_dir, store) = store();
        let c = store.collection("projected_nested").unwrap();
        c.insert_one(json!({
            "_id": "a",
            "user": {
                "name": "Ada",
                "password": "secret",
                "address": {"city": "Warsaw", "zip": "00-001"}
            },
            "items": [
                {"sku": "x", "qty": 1, "cost": 10},
                {"sku": "y", "qty": 5, "cost": 50}
            ],
            "tags": ["a", "b", "c"],
            "_score": 2.5
        }))
        .unwrap();

        let nested = c
            .find(&FindQuery {
                filter: json!({}),
                projection: Some(json!({
                    "user.name": 1,
                    "user.address.city": 1,
                    "items.sku": 1,
                    "items.qty": 1,
                    "_id": 0
                })),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            nested,
            vec![json!({
                "user": {"name": "Ada", "address": {"city": "Warsaw"}},
                "items": [
                    {"sku": "x", "qty": 1},
                    {"sku": "y", "qty": 5}
                ]
            })]
        );

        let positional = c
            .find(&FindQuery {
                filter: json!({"items.qty": {"$gt": 3}}),
                projection: Some(json!({
                    "user.name": 1,
                    "items.$": {"$slice": 1},
                    "score": {"$meta": "textScore"},
                    "_id": 0
                })),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            positional,
            vec![json!({
                "user": {"name": "Ada"},
                "items": [{"sku": "y", "qty": 5, "cost": 50}],
                "score": 2.5
            })]
        );

        let excluded = c
            .find(&FindQuery {
                filter: json!({}),
                projection: Some(json!({
                    "user.password": 0,
                    "user.address.zip": 0,
                    "items.cost": 0,
                    "_id": 0
                })),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            excluded,
            vec![json!({
                "user": {
                    "name": "Ada",
                    "address": {"city": "Warsaw"}
                },
                "items": [
                    {"sku": "x", "qty": 1},
                    {"sku": "y", "qty": 5}
                ],
                "tags": ["a", "b", "c"],
                "_score": 2.5
            })]
        );
    }

    #[test]
    fn secondary_index_equality_and_range() {
        let (_dir, store) = store();
        let c = store.collection("users").unwrap();
        for i in 0..10 {
            c.insert_one(json!({"name": format!("u{i}"), "age": i}))
                .unwrap();
        }
        c.create_index("age", false).unwrap();
        assert_eq!(c.list_indexes().unwrap(), vec![("age".to_string(), false)]);

        let eq = c.find_one(&json!({"age": 5})).unwrap().unwrap();
        assert_eq!(eq["name"], "u5");

        let q = FindQuery {
            filter: json!({"age": {"$gte": 7}}),
            sort: vec![("age".to_string(), 1)],
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0]["age"], 7);
        assert_eq!(got[2]["age"], 9);
    }

    #[test]
    fn sort_only_index_or_union_and_type_plans() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..10 {
            c.insert_one(json!({
                "_id": format!("d{i}"),
                "n": i,
                "name": format!("u{i}"),
            }))
            .unwrap();
        }
        c.create_index("n", false).unwrap();
        c.create_index("name", false).unwrap();

        let q = FindQuery {
            filter: serde_json::json!({}),
            sort: vec![("n".into(), -1)],
            limit: Some(3),
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        let ns: Vec<_> = got.iter().map(|d| d["n"].as_i64()).collect();
        assert_eq!(ns, vec![Some(9), Some(8), Some(7)]);
        let plan = c.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_scan");
        assert_eq!(plan["sort_covered"], true);

        // A plain index stores a missing sort field as null, so it still covers
        // the sort and places that document first, like MongoDB.
        c.insert_one(json!({"_id": "missing"})).unwrap();
        let q = FindQuery {
            filter: serde_json::json!({}),
            sort: vec![("n".into(), 1)],
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        assert_eq!(got.len(), 11);
        assert_eq!(got[0]["_id"], "missing");
        let plan = c.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_scan");
        assert_eq!(plan["sort_covered"], true);

        let q = FindQuery {
            filter: json!({"$or": [{"n": 3}, {"name": "u7"}]}),
            limit: Some(10),
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        assert_eq!(got.len(), 2);
        let plan = c.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_union");

        let q = FindQuery {
            filter: json!({"n": {"$type": "int"}}),
            limit: Some(3),
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 3);
        let plan = c.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_scan");
    }

    #[test]
    fn mongodb_null_sort_and_in_lookup_plans() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for (id, value) in [("1", Some(2)), ("2", None), ("3", Some(1)), ("4", Some(0))] {
            let mut doc = json!({"_id": id});
            if let Some(value) = value {
                doc["v"] = json!(value);
            }
            if id == "4" {
                doc["v"] = Value::Null;
            }
            c.insert_one(doc).unwrap();
        }
        let asc = FindQuery {
            filter: json!({}),
            sort: vec![("v".into(), 1)],
            ..Default::default()
        };
        let ids: Vec<_> = c
            .find(&asc)
            .unwrap()
            .into_iter()
            .map(|doc| doc["_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, ["2", "4", "3", "1"]);
        let desc = FindQuery {
            sort: vec![("v".into(), -1)],
            ..asc.clone()
        };
        let ids: Vec<_> = c
            .find(&desc)
            .unwrap()
            .into_iter()
            .map(|doc| doc["_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, ["1", "3", "2", "4"]);

        c.create_index("n", false).unwrap();
        for i in 0..6 {
            c.insert_one(json!({"_id": format!("n{i}"), "n": i}))
                .unwrap();
        }
        let primary = FindQuery {
            filter: json!({"_id": {"$in": ["n1", "n3", "n1"]}}),
            ..Default::default()
        };
        let ids: Vec<_> = c
            .find(&primary)
            .unwrap()
            .into_iter()
            .map(|doc| doc["_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, ["n1", "n3"]);
        assert_eq!(c.explain(&primary).unwrap()["access_path"], "id_lookup");

        let secondary = FindQuery {
            filter: json!({"n": {"$in": [1, 3, 5]}}),
            ..Default::default()
        };
        assert_eq!(c.find(&secondary).unwrap().len(), 3);
        assert_eq!(c.explain(&secondary).unwrap()["access_path"], "index_union");

        let paged = FindQuery {
            filter: json!({"$and": [{"n": {"$in": [1, 3, 5]}}, {"_id": {"$gt": "n1"}}]}),
            ..Default::default()
        };
        let ids: Vec<_> = c
            .find(&paged)
            .unwrap()
            .into_iter()
            .map(|doc| doc["_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, ["n3", "n5"]);
        assert_eq!(c.explain(&paged).unwrap()["access_path"], "index_union");
    }

    #[test]
    fn multikey_indexes_filter_unique_sort_and_verify() {
        let (_dir, store) = store();
        let c = store.collection("mk").unwrap();
        for (id, tags) in [
            ("a", json!(["b", "c"])),
            ("b", json!(["a", "z"])),
            ("c", json!(["b", "b"])),
            ("d", json!(["m"])),
        ] {
            c.insert_one(json!({"_id": id, "tags": tags})).unwrap();
        }
        c.create_index("tags", false).unwrap();
        let def = c.list_index_defs().unwrap().into_iter().next().unwrap();
        assert_eq!(def.multikey_fields, vec!["tags".to_string()]);
        assert!(def.multikey_all_docs);

        let q = FindQuery {
            filter: json!({"tags": "b"}),
            ..Default::default()
        };
        let mut ids: Vec<_> = c
            .find(&q)
            .unwrap()
            .into_iter()
            .map(|d| d["_id"].as_str().unwrap().to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["a", "c"]);
        assert_eq!(c.count(&json!({"tags": "b"})).unwrap(), 2);
        let plan = c.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_scan");
        assert_eq!(plan["index"]["multikey"], json!(["tags"]));

        let q = FindQuery {
            filter: json!({"tags": {"$all": ["b", "c"]}}),
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 1);
        assert_eq!(c.count(&json!({"tags": {"$all": ["b", "c"]}})).unwrap(), 1);
        let plan = c.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_scan");

        let q = FindQuery {
            filter: serde_json::json!({}),
            sort: vec![("tags".into(), 1)],
            ..Default::default()
        };
        let ids: Vec<_> = c
            .find(&q)
            .unwrap()
            .into_iter()
            .map(|d| d["_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, vec!["b", "a", "c", "d"]);
        let plan = c.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_scan");
        assert_eq!(plan["sort_covered"], true);

        let q = FindQuery {
            filter: serde_json::json!({}),
            sort: vec![("tags".into(), -1)],
            skip: 1,
            limit: Some(2),
            ..Default::default()
        };
        let ids: Vec<_> = c
            .find(&q)
            .unwrap()
            .into_iter()
            .map(|d| d["_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, vec!["d", "a"]);

        // A document without the indexed field cannot use index sort order;
        // the scan must place it first, like MongoDB.
        c.insert_one(json!({"_id": "e"})).unwrap();
        let q = FindQuery {
            filter: serde_json::json!({}),
            sort: vec![("tags".into(), 1)],
            ..Default::default()
        };
        let ids: Vec<_> = c
            .find(&q)
            .unwrap()
            .into_iter()
            .map(|d| d["_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, vec!["e", "b", "a", "c", "d"]);
        {
            let rtxn = c.env.read_txn().unwrap();
            c.verify_in(&rtxn).unwrap();
        }

        // Updating an array replaces all entries for the document.
        c.update(
            &json!({"_id": "a"}),
            &json!({"$set": {"tags": ["q"]}}),
            false,
        )
        .unwrap();
        assert_eq!(c.count(&json!({"tags": "c"})).unwrap(), 0);
        assert_eq!(c.count(&json!({"tags": "q"})).unwrap(), 1);
        {
            let rtxn = c.env.read_txn().unwrap();
            c.verify_in(&rtxn).unwrap();
        }

        // Unique multikey indexes allow duplicate elements in one document,
        // but reject the same value in a different document.
        let u = store.collection("mkuniq").unwrap();
        u.insert_one(json!({"_id": "u1", "tags": ["x", "x"]}))
            .unwrap();
        u.insert_one(json!({"_id": "u2", "tags": ["y"]})).unwrap();
        u.create_index("tags", true).unwrap();
        assert!(u
            .update(
                &json!({"_id": "u2"}),
                &json!({"$set": {"tags": ["x"]}}),
                false
            )
            .is_err());
        assert!(u
            .insert_one(json!({"_id": "u3", "tags": ["y", "y"]}))
            .is_err());
        u.insert_one(json!({"_id": "u3", "tags": ["z", "z"]}))
            .unwrap();
        u.update(
            &json!({"_id": "u2"}),
            &json!({"$set": {"tags": ["y", "w"]}}),
            false,
        )
        .unwrap();
        {
            let rtxn = u.env.read_txn().unwrap();
            u.verify_in(&rtxn).unwrap();
        }

        // An index created before any writes becomes multikey when arrays are
        // inserted and remains usable for sort order.
        let early = store.collection("mkearly").unwrap();
        early.create_index("tags", false).unwrap();
        early
            .insert_one(json!({"_id": "x", "tags": ["b", "d"]}))
            .unwrap();
        early
            .insert_one(json!({"_id": "y", "tags": ["a", "c"]}))
            .unwrap();
        let q = FindQuery {
            filter: serde_json::json!({}),
            sort: vec![("tags".into(), 1)],
            ..Default::default()
        };
        let ids: Vec<_> = early
            .find(&q)
            .unwrap()
            .into_iter()
            .map(|d| d["_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, vec!["y", "x"]);
        let plan = early.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_scan");

        let d = store.collection("mkdup").unwrap();
        d.insert_one(json!({"_id": "a", "tags": ["x"]})).unwrap();
        d.insert_one(json!({"_id": "b", "tags": ["x", "y"]}))
            .unwrap();
        assert!(d.create_index("tags", true).is_err());
    }

    #[test]
    fn multikey_elem_match_uses_nested_index() {
        let (_dir, store) = store();
        let c = store.collection("mk_elem").unwrap();
        c.insert_one(json!({"_id": "e1", "items": [{"price": 5}]}))
            .unwrap();
        c.insert_one(json!({"_id": "e2", "items": [{"price": 15}]}))
            .unwrap();
        c.insert_one(json!({"_id": "e3", "items": [{"price": 20}, {"price": 1}]}))
            .unwrap();
        c.create_index("items.price", false).unwrap();

        let q = FindQuery {
            filter: json!({"items": {"$elemMatch": {"price": {"$gte": 10, "$lt": 18}}}}),
            ..Default::default()
        };
        let ids: Vec<_> = c
            .find(&q)
            .unwrap()
            .into_iter()
            .map(|d| d["_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, vec!["e2"]);
        let plan = c.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_scan");
        assert_eq!(plan["index"]["multikey"], json!(["items.price"]));

        let q = FindQuery {
            filter: json!({"items": {"$elemMatch": {"price": 1}}}),
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 1);
        assert_eq!(
            c.count(&json!({"items": {"$elemMatch": {"price": 1}}}))
                .unwrap(),
            1
        );
        {
            let rtxn = c.env.read_txn().unwrap();
            c.verify_in(&rtxn).unwrap();
        }
    }

    #[test]
    fn mixed_direction_compound_index_serves_sorted_queries() {
        let (_dir, store) = store();
        let c = store.collection("mixed_events").unwrap();
        for g in 0..3 {
            for age in 0..10 {
                c.insert_one(json!({
                    "_id": format!("g{g}a{age}"),
                    "g": g,
                    "age": age,
                }))
                .unwrap();
            }
        }
        c.create_indexes(&[IndexSpec {
            fields: vec!["g".into(), "age".into()],
            name: "g_1_age_-1".into(),
            directions: vec![1, -1],
            unique: false,
            sparse: false,
            partial_filter_expression: None,
            include: vec![],
            geo: false,
        }])
        .unwrap();

        let q = FindQuery {
            filter: json!({}),
            sort: vec![("g".into(), 1), ("age".into(), -1)],
            limit: Some(5),
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        let pairs: Vec<_> = got
            .iter()
            .map(|doc| (doc["g"].as_i64(), doc["age"].as_i64()))
            .collect();
        assert_eq!(
            pairs,
            vec![
                (Some(0), Some(9)),
                (Some(0), Some(8)),
                (Some(0), Some(7)),
                (Some(0), Some(6)),
                (Some(0), Some(5)),
            ]
        );
        let plan = c.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_scan");
        assert_eq!(plan["sort_covered"], true);

        let q = FindQuery {
            filter: json!({"g": 1, "age": {"$gte": 4, "$lt": 8}}),
            sort: vec![("g".into(), 1), ("age".into(), -1)],
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        let ages: Vec<_> = got.iter().map(|doc| doc["age"].as_i64()).collect();
        assert_eq!(ages, vec![Some(7), Some(6), Some(5), Some(4)]);
        let plan = c.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_scan");
        assert_eq!(plan["sort_covered"], true);

        let q = FindQuery {
            filter: json!({}),
            sort: vec![("g".into(), -1), ("age".into(), 1)],
            limit: Some(4),
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        let pairs: Vec<_> = got
            .iter()
            .map(|doc| (doc["g"].as_i64(), doc["age"].as_i64()))
            .collect();
        assert_eq!(
            pairs,
            vec![
                (Some(2), Some(0)),
                (Some(2), Some(1)),
                (Some(2), Some(2)),
                (Some(2), Some(3)),
            ]
        );

        c.update(
            &json!({"_id": "g1a7"}),
            &json!({"$set": {"age": 10}}),
            false,
        )
        .unwrap();
        let q = FindQuery {
            filter: json!({"g": 1, "age": {"$gte": 8}}),
            sort: vec![("g".into(), 1), ("age".into(), -1)],
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        let ids: Vec<_> = got.iter().map(|doc| doc["_id"].clone()).collect();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[0], "g1a7");
        assert_eq!(ids[1], "g1a9");
        assert_eq!(ids[2], "g1a8");

        let rtxn = c.env.read_txn().unwrap();
        c.verify_in(&rtxn).unwrap();
    }

    #[test]
    fn compound_index_filters_sorts_and_maintains() {
        let (_dir, store) = store();
        let c = store.collection("events").unwrap();
        for g in 0..3 {
            for age in 0..10 {
                c.insert_one(json!({
                    "_id": format!("g{g}a{age}"),
                    "g": g,
                    "age": age,
                    "name": format!("{g}-{age}"),
                }))
                .unwrap();
            }
        }
        c.create_index_fields(&["g".to_string(), "age".to_string()], false, &[])
            .unwrap();
        assert!(c
            .list_indexes()
            .unwrap()
            .contains(&("g+age".to_string(), false)));

        // Range on the last field + sorting along the index.
        let q = FindQuery {
            filter: json!({"g": 1, "age": {"$gte": 4, "$lt": 8}}),
            sort: vec![("age".to_string(), 1)],
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        assert_eq!(got.len(), 4);
        assert_eq!(got[0]["age"], 4);
        assert_eq!(got[3]["age"], 7);

        // Equality on all fields + count through the index.
        let q = FindQuery {
            filter: json!({"g": 2, "age": 3}),
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 1);
        assert_eq!(
            c.count(&json!({"g": 1, "age": {"$gte": 4, "$lt": 8}}))
                .unwrap(),
            4
        );
        // An extra condition cannot use the index alone for count.
        assert_eq!(
            c.count(&json!({"g": 1, "age": {"$gte": 4, "$lt": 8}, "name": "1-4"}))
                .unwrap(),
            1
        );

        // Update moves the entry in the index.
        c.update(
            &json!({"_id": "g1a5"}),
            &json!({"$set": {"age": 99}}),
            false,
        )
        .unwrap();
        assert_eq!(
            c.count(&json!({"g": 1, "age": {"$gte": 4, "$lt": 8}}))
                .unwrap(),
            3
        );
        assert!(c.find_one(&json!({"g": 1, "age": 99})).unwrap().is_some());

        // Delete clears the entry.
        c.delete(&json!({"_id": "g2a3"}), false).unwrap();
        assert!(c.find_one(&json!({"g": 2, "age": 3})).unwrap().is_none());
    }

    #[test]
    fn covering_index_serves_projection_without_document_fetch() {
        let (_dir, store) = store();
        let c = store.collection("cover").unwrap();
        c.create_index_fields(&["g".to_string()], false, &["v".to_string()])
            .unwrap();
        c.insert_one(json!({"_id": "a", "g": 1, "v": 42, "other": "x"}))
            .unwrap();
        c.update(&json!({"_id": "a"}), &json!({"$set": {"v": 43}}), false)
            .unwrap();
        // An exclusive projection cannot be served from the index alone.
        let excl = FindQuery {
            filter: json!({"g": 1}),
            projection: Some(json!({"other": 0})),
            sort: vec![("g".to_string(), 1)],
            ..Default::default()
        };
        let got = c.find(&excl).unwrap();
        assert_eq!(got[0]["v"], 43);
        assert!(got[0].get("other").is_none());
        // Delete the document but leave the index entry: coverage must not
        // reach the collection, so the result proves the index value was used.
        let mut wtxn = c.write_txn().unwrap();
        c.db.delete(&mut wtxn, b"a").unwrap();
        wtxn.commit().unwrap();

        let q = FindQuery {
            filter: json!({"g": 1}),
            projection: Some(json!({"g": 1, "v": 1, "_id": 0})),
            sort: vec![("g".to_string(), 1)],
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        assert_eq!(got, vec![json!({"g": 1, "v": 43})]);
    }

    #[test]
    fn pipeline_update_maintains_indexes_and_upserts() {
        let (_dir, store) = store();
        let c = store.collection("pipeline_updates").unwrap();
        c.create_index("total", false).unwrap();
        c.insert_one(json!({"_id": "a", "a": 2, "b": 4, "remove": true}))
            .unwrap();

        let out = c
            .update_opts(
                &json!({"_id": "a"}),
                &json!([
                    {"$set": {"total": {"$add": ["$a", "$b"]}}},
                    {"$unset": "remove"},
                    {"$project": {"_id": 1, "total": 1}}
                ]),
                false,
                false,
            )
            .unwrap();
        assert_eq!((out.matched, out.modified), (1, 1));
        let doc = c.find_one(&json!({"_id": "a"})).unwrap().unwrap();
        assert_eq!(doc, json!({"_id": "a", "total": 6}));
        assert_eq!(c.count(&json!({"total": 6})).unwrap(), 1);
        {
            let rtxn = c.env.read_txn().unwrap();
            c.verify_in(&rtxn).unwrap();
        }

        let out = c
            .update_opts(
                &json!({"_id": "b", "tenant": "t"}),
                &json!([{"$set": {"total": 10}}]),
                false,
                true,
            )
            .unwrap();
        assert_eq!(out.upserted_id.as_deref(), Some("b"));
        assert_eq!(
            c.find_one(&json!({"_id": "b"})).unwrap().unwrap(),
            json!({"_id": "b", "tenant": "t", "total": 10})
        );

        assert!(c
            .update(&json!({"_id": "a"}), &json!([{"$match": {}}]), false)
            .is_err());
        assert!(c
            .update(
                &json!({"_id": "a"}),
                &json!([{"$set": {"_id": "changed"}}]),
                false
            )
            .is_err());
        assert_eq!(
            c.find_one(&json!({"_id": "a"})).unwrap().unwrap()["total"],
            6
        );
    }

    #[test]
    fn update_upsert_and_array_operators() {
        let (_dir, store) = store();
        let c = store.collection("cfg").unwrap();
        let out = c
            .update_opts(
                &json!({"tenant": "a", "key": "mail"}),
                &json!({
                    "$set": {"host": "smtp"},
                    "$setOnInsert": {"created": true},
                    "$addToSet": {"tags": {"$each": ["x", "x", "y"]}},
                    "$push": {"ports": {"$each": [25, 587]}},
                }),
                false,
                true,
            )
            .unwrap();
        assert_eq!(out.matched, 0);
        assert_eq!(out.modified, 0);
        let id = out.upserted_id.expect("upserted id");
        let doc = c.find_one(&json!({"_id": id})).unwrap().unwrap();
        assert_eq!(doc["tenant"], "a");
        assert_eq!(doc["key"], "mail");
        assert_eq!(doc["host"], "smtp");
        assert_eq!(doc["created"], true);
        assert_eq!(doc["tags"], json!(["x", "y"]));
        assert_eq!(doc["ports"], json!([25, 587]));

        // The second upsert matches an existing document: no new `_id`,
        // `$setOnInsert` does not apply.
        let out = c
            .update_opts(
                &json!({"tenant": "a", "key": "mail"}),
                &json!({
                    "$set": {"host": "smtp2"},
                    "$setOnInsert": {"created": false},
                    "$addToSet": {"tags": "z"},
                }),
                false,
                true,
            )
            .unwrap();
        assert_eq!(out.matched, 1);
        assert_eq!(out.modified, 1);
        assert!(out.upserted_id.is_none());
        let doc = c.find_one(&json!({"_id": id})).unwrap().unwrap();
        assert_eq!(doc["host"], "smtp2");
        assert_eq!(doc["created"], true);
        assert_eq!(doc["tags"], json!(["x", "y", "z"]));

        // Replacement upsert adds equalities from the filter.
        let out = c
            .update_opts(
                &json!({"tenant": "b", "key": "db"}),
                &json!({"value": 1}),
                false,
                true,
            )
            .unwrap();
        let doc = c
            .find_one(&json!({"_id": out.upserted_id.unwrap()}))
            .unwrap()
            .unwrap();
        assert_eq!(doc["tenant"], "b");
        assert_eq!(doc["key"], "db");
        assert_eq!(doc["value"], 1);
    }

    #[test]
    fn upsert_without_match_and_no_upsert_is_noop() {
        let (_dir, store) = store();
        let c = store.collection("cfg2").unwrap();
        let out = c
            .update_opts(&json!({"a": 1}), &json!({"$set": {"b": 2}}), false, false)
            .unwrap();
        assert_eq!(
            out,
            UpdateOutcome {
                matched: 0,
                modified: 0,
                upserted_id: None,
                moved: Vec::new()
            }
        );
        assert_eq!(c.count(&serde_json::json!({})).unwrap(), 0);
    }

    #[test]
    fn sparse_and_partial_indexes_enforce_unique_only_for_indexed_documents() {
        let (_dir, store) = store();
        let c = store.collection("sparse_partial").unwrap();
        c.insert_one(json!({"_id": "missing-a"})).unwrap();
        c.insert_one(json!({"_id": "missing-b"})).unwrap();
        c.insert_one(json!({"_id": "null", "email": null})).unwrap();
        c.create_indexes(&[IndexSpec {
            fields: vec!["email".into()],
            name: "email_sparse".into(),
            directions: Vec::new(),
            unique: true,
            sparse: true,
            partial_filter_expression: None,
            include: Vec::new(),
            geo: false,
        }])
        .unwrap();
        c.insert_one(json!({"_id": "missing-c"})).unwrap();
        assert!(matches!(
            c.insert_one(json!({"_id": "null-2", "email": null})),
            Err(Error::DuplicateKey(_))
        ));
        c.insert_one(json!({"_id": "one", "email": "a@example.test"}))
            .unwrap();
        assert!(matches!(
            c.insert_one(json!({"_id": "two", "email": "a@example.test"})),
            Err(Error::DuplicateKey(_))
        ));

        let plain = store.collection("plain_unique_missing").unwrap();
        plain.create_index("email", true).unwrap();
        plain.insert_one(json!({"_id": "plain-missing-a"})).unwrap();
        assert!(matches!(
            plain.insert_one(json!({"_id": "plain-missing-b"})),
            Err(Error::DuplicateKey(_))
        ));
        assert!(matches!(
            plain.insert_one(json!({"_id": "plain-null", "email": null})),
            Err(Error::DuplicateKey(_))
        ));

        let p = store.collection("partial_unique").unwrap();
        p.insert_one(json!({"_id": "old-inactive", "status": "inactive", "email": "same"}))
            .unwrap();
        p.insert_one(json!({"_id": "old-active", "status": "active", "email": "old"}))
            .unwrap();
        p.create_indexes(&[IndexSpec {
            fields: vec!["email".into()],
            name: "active_email".into(),
            directions: Vec::new(),
            unique: true,
            sparse: false,
            partial_filter_expression: Some(json!({"status": "active"})),
            include: Vec::new(),
            geo: false,
        }])
        .unwrap();
        // Inactive documents are not in the partial index, even with duplicate
        // keys. Active documents must be unique.
        p.insert_one(json!({"_id": "inactive-2", "status": "inactive", "email": "same"}))
            .unwrap();
        p.insert_one(json!({"_id": "active-new", "status": "active", "email": "new"}))
            .unwrap();
        assert!(matches!(
            p.insert_one(json!({"_id": "active-dup", "status": "active", "email": "old"})),
            Err(Error::DuplicateKey(_))
        ));

        // The partial index is used only when the query repeats its predicate.
        let filtered = FindQuery {
            filter: json!({"status": "active", "email": "old"}),
            ..Default::default()
        };
        let explain = p.explain(&filtered).unwrap();
        assert_eq!(explain["access_path"], "index_scan");
        assert_eq!(explain["index"]["fields"], json!(["email"]));
        let unfiltered = FindQuery {
            filter: json!({"email": "same"}),
            ..Default::default()
        };
        assert_eq!(
            p.explain(&unfiltered).unwrap()["access_path"],
            "collection_scan"
        );
        let hint = FindQuery {
            filter: json!({"email": "same"}),
            hint: Some(json!("active_email")),
            ..Default::default()
        };
        assert!(p.find(&hint).is_err());

        // Reindex keeps the sparse and partial definitions.
        p.reindex().unwrap();
        let defs = p.list_index_defs().unwrap();
        assert!(defs.iter().any(|def| def.name() == "active_email"
            && def.partial_filter_expression == Some(json!({"status": "active"}))));
    }

    #[test]
    fn partial_index_range_predicate_is_used() {
        let (_dir, store) = store();
        let c = store.collection("partial_range").unwrap();
        for score in [50, 100, 150, 200] {
            c.insert_one(json!({"score": score})).unwrap();
        }
        c.create_indexes(&[IndexSpec {
            fields: vec!["score".into()],
            name: "score_hot".into(),
            directions: Vec::new(),
            unique: false,
            sparse: false,
            partial_filter_expression: Some(json!({"score": {"$gte": 100}})),
            include: Vec::new(),
            geo: false,
        }])
        .unwrap();
        let stronger = FindQuery {
            filter: json!({"score": {"$gte": 150, "$lte": 250}}),
            ..Default::default()
        };
        let plan = c.explain(&stronger).unwrap();
        assert_eq!(plan["access_path"], "index_scan", "{plan}");
        assert_eq!(plan["index_used"], true);
        let weaker = FindQuery {
            filter: json!({"score": {"$gte": 50}}),
            ..Default::default()
        };
        assert_eq!(
            c.explain(&weaker).unwrap()["access_path"],
            "collection_scan"
        );
        assert_eq!(c.find(&stronger).unwrap().len(), 2);
    }

    #[test]
    fn compound_unique_index_checks_full_key() {
        let (_dir, store) = store();
        let c = store.collection("u").unwrap();
        c.create_index_fields(&["a".to_string(), "b".to_string()], true, &[])
            .unwrap();
        c.insert_one(json!({"a": 1, "b": 1})).unwrap();
        c.insert_one(json!({"a": 1, "b": 2})).unwrap();
        let err = c.insert_one(json!({"a": 1, "b": 1})).unwrap_err();
        assert!(matches!(err, Error::DuplicateKey(_)));
    }

    #[test]
    fn index_maintained_on_update_and_delete() {
        let (_dir, store) = store();
        let c = store.collection("users").unwrap();
        let id = c.insert_one(json!({"name": "Ala", "age": 30})).unwrap();
        c.create_index("age", true).unwrap();

        // Changing an indexed field updates the entry.
        c.update(&json!({"_id": id}), &json!({"$set": {"age": 31}}), false)
            .unwrap();
        assert!(c.find_one(&json!({"age": 30})).unwrap().is_none());
        assert_eq!(
            c.find_one(&json!({"age": 31})).unwrap().unwrap()["name"],
            "Ala"
        );

        // Uniqueness.
        c.insert_one(json!({"name": "Bob", "age": 40})).unwrap();
        assert!(c.insert_one(json!({"name": "Cezary", "age": 40})).is_err());

        // Delete removes the entry from the index.
        c.delete(&json!({"age": 40}), true).unwrap();
        assert!(c.find_one(&json!({"age": 40})).unwrap().is_none());
    }

    #[test]
    fn rejects_oversized_document() {
        let (_dir, store) = store();
        let c = store.collection("big").unwrap();
        let big = "x".repeat(crate::MAX_DOCUMENT_BYTES + 10);
        let err = c.insert_one(json!({ "payload": big })).unwrap_err();
        assert!(matches!(err, Error::DocumentTooLarge(_, _)));
    }

    #[test]
    fn unique_index_backfill_detects_duplicates() {
        let (_dir, store) = store();
        let c = store.collection("users").unwrap();
        c.insert_one(json!({"name": "Ala"})).unwrap();
        c.insert_one(json!({"name": "Ala"})).unwrap();
        assert!(c.create_index("name", true).is_err());
    }

    #[test]
    fn geo_index_supports_near_within_and_updates() {
        let (_dir, store) = store();
        let c = store.collection("places").unwrap();
        let near_id = c
            .insert_one(json!({
                "_id": "near",
                "loc": {"type": "Point", "coordinates": [19.94, 50.06]},
            }))
            .unwrap();
        c.insert_one(json!({
            "_id": "slightly-near",
            "loc": {"type": "Point", "coordinates": [19.941, 50.06]},
        }))
        .unwrap();
        c.insert_one(json!({
            "_id": "far",
            "loc": {"type": "Point", "coordinates": [21.01, 52.23]},
        }))
        .unwrap();
        c.create_geo_index("loc").unwrap();

        let near = FindQuery {
            filter: json!({
                "loc": {
                    "$near": {
                        "$geometry": {"type": "Point", "coordinates": [19.94, 50.06]},
                        "$maxDistance": 500
                    }
                }
            }),
            ..Default::default()
        };
        let found = c.find(&near).unwrap();
        assert_eq!(found.len(), 2);
        assert_eq!(found[0]["_id"], near_id);
        assert_eq!(found[1]["_id"], "slightly-near");

        let within = json!({"loc": {"$geoWithin": {"$box": [[19.0, 50.0], [20.0, 51.0]]}}});
        assert_eq!(c.count(&within).unwrap(), 2);
        let q = FindQuery {
            filter: within.clone(),
            ..Default::default()
        };
        let explained = c.explain(&q).unwrap();
        assert_eq!(explained["access_path"], "geo_index_scan");
        assert_eq!(explained["index_used"], true);
        assert_eq!(explained["index"]["geo"], true);

        // Updating the point must move its geo index entries.
        c.update(
            &json!({"_id": near_id}),
            &json!({"$set": {"loc": {"type": "Point", "coordinates": [21.0, 52.2]}}}),
            false,
        )
        .unwrap();
        assert_eq!(c.count(&within).unwrap(), 1);
        let remaining = c.find(&near).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0]["_id"], "slightly-near");
        let rtxn = store.read_txn().unwrap();
        c.verify_in(&rtxn).unwrap();
        rtxn.commit().unwrap();
    }

    #[test]
    fn segment_text_index_supports_crud() {
        let (_dir, store) = store();
        let c = store.collection("segment_text_crud").unwrap();
        for doc in [
            json!({"_id": "a", "body": "rust database"}),
            json!({"_id": "b", "body": "rust web"}),
            json!({"_id": "c", "body": "python database"}),
        ] {
            c.insert_one(doc).unwrap();
        }
        c.create_text_segment_index("body").unwrap();
        let ids = |docs: Vec<Value>| {
            let mut ids: Vec<String> = docs
                .into_iter()
                .map(|doc| doc["_id"].as_str().unwrap().to_string())
                .collect();
            ids.sort();
            ids
        };
        assert_eq!(
            ids(c.search("body", "rust", &json!({}), 0, Some(10)).unwrap()),
            vec!["a".to_string(), "b".to_string()]
        );
        c.update(
            &json!({"_id": "a"}),
            &json!({"$set": {"body": "python only"}}),
            false,
        )
        .unwrap();
        assert_eq!(
            ids(c.search("body", "rust", &json!({}), 0, Some(10)).unwrap()),
            vec!["b".to_string()]
        );
        c.delete(&json!({"_id": "b"}), false).unwrap();
        assert!(c
            .search("body", "rust", &json!({}), 0, Some(10))
            .unwrap()
            .is_empty());
        assert_eq!(
            ids(c.search("body", "python", &json!({}), 0, Some(10)).unwrap()),
            vec!["a".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn segment_text_tracks_exact_live_stats() {
        let (_dir, store) = store();
        let c = store.collection("segment_text_stats").unwrap();
        c.insert_one(json!({"_id": "a", "body": "alpha beta"}))
            .unwrap();
        c.insert_one(json!({"_id": "b", "body": "alpha"})).unwrap();
        c.create_text_segment_index("body").unwrap();

        c.update(
            &json!({"_id": "a"}),
            &json!({"$set": {"body": "gamma"}}),
            false,
        )
        .unwrap();
        c.delete(&json!({"_id": "b"}), false).unwrap();
        c.insert_one(json!({"_id": "c", "body": "gamma gamma"}))
            .unwrap();

        let rtxn = store.read_txn().unwrap();
        assert_eq!(c.segment_stats_in(&rtxn, "body").unwrap(), Some((2, 3)));
        assert_eq!(c.segment_df_in(&rtxn, "body", "alpha").unwrap(), 0);
        assert_eq!(c.segment_df_in(&rtxn, "body", "beta").unwrap(), 0);
        assert_eq!(c.segment_df_in(&rtxn, "body", "gamma").unwrap(), 2);
        rtxn.commit().unwrap();

        // Older segment indexes have no stats keys; an explicit rebuild
        // restores exact live statistics from the current documents.
        let mut wtxn = store.write_txn().unwrap();
        c.clear_segment_stats(&mut wtxn, "body").unwrap();
        wtxn.commit().unwrap();
        assert_eq!(c.rebuild_segment_text_stats().unwrap(), 1);
        let rtxn = store.read_txn().unwrap();
        assert_eq!(c.segment_stats_in(&rtxn, "body").unwrap(), Some((2, 3)));
        assert_eq!(c.segment_df_in(&rtxn, "body", "gamma").unwrap(), 2);
        rtxn.commit().unwrap();

        // Startup repair only touches indexes without statistics.
        let mut wtxn = store.write_txn().unwrap();
        c.clear_segment_stats(&mut wtxn, "body").unwrap();
        wtxn.commit().unwrap();
        assert_eq!(c.ensure_segment_text_stats().unwrap(), 1);
        assert_eq!(c.ensure_segment_text_stats().unwrap(), 0);
    }

    #[test]
    fn segment_text_compacts_after_three_flushes() {
        let (_dir, store) = store();
        let c = store.collection("segment_text_auto_compact").unwrap();
        c.insert_one(json!({"_id": "base", "body": "base rust"}))
            .unwrap();
        c.create_text_segment_index("body").unwrap();
        for i in 0..(SEGMENT_DELTA_FLUSH_DOCS * 3 + 10) {
            c.insert_one(json!({"_id": format!("d{i}"), "body": format!("doc{i} rust")}))
                .unwrap();
        }
        let rtxn = store.env().read_txn().unwrap();
        let manifest = c
            .segment_manifest_in(&rtxn, "body")
            .unwrap()
            .expect("segment manifest");
        assert_eq!(manifest.segments.len(), 2);
        assert!(manifest.segments[1].contains("compact-"));
    }

    #[test]
    fn segment_text_index_supports_indexed_filters() {
        let (_dir, store) = store();
        let c = store.collection("segment_text_filter").unwrap();
        for doc in [
            json!({"_id": "a", "body": "rust database", "cat": 1}),
            json!({"_id": "b", "body": "rust web", "cat": 2}),
            json!({"_id": "c", "body": "python database", "cat": 1}),
        ] {
            c.insert_one(doc).unwrap();
        }
        c.create_index("cat", false).unwrap();
        c.create_text_segment_index("body").unwrap();
        let mut ids: Vec<String> = c
            .search("body", "rust", &json!({"cat": 1}), 0, Some(10))
            .unwrap()
            .into_iter()
            .map(|doc| doc["_id"].as_str().unwrap().to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["a".to_string()]);
        let ids: Vec<String> = c
            .search("body", "rust", &json!({"cat": {"$gt": 1}}), 0, Some(10))
            .unwrap()
            .into_iter()
            .map(|doc| doc["_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, vec!["b".to_string()]);
    }

    #[test]
    fn segment_text_supports_phrases_negation_and_post_filters() {
        let (_dir, store) = store();
        let c = store.collection("segment_text_verify").unwrap();
        for doc in [
            json!({"_id": "a", "body": "quick brown fox", "tag": "x"}),
            json!({"_id": "b", "body": "quick red brown fox", "tag": "y"}),
        ] {
            c.insert_one(doc).unwrap();
        }
        c.create_text_segment_index("body").unwrap();
        let ids = |docs: Vec<Value>| {
            let mut ids: Vec<String> = docs
                .into_iter()
                .map(|doc| doc["_id"].as_str().unwrap().to_string())
                .collect();
            ids.sort();
            ids
        };
        assert_eq!(
            ids(c
                .search("body", "\"quick brown\"", &json!({}), 0, Some(10))
                .unwrap()),
            vec!["a".to_string()]
        );
        assert_eq!(
            ids(c
                .search("body", "quick brown -red", &json!({}), 0, Some(10))
                .unwrap()),
            vec!["a".to_string()]
        );
        assert_eq!(
            ids(c
                .search("body", "brown", &json!({"tag": "y"}), 0, Some(10))
                .unwrap()),
            vec!["b".to_string()]
        );
        let phrase_score = c
            .search("body", "\"quick brown\"", &json!({}), 0, Some(10))
            .unwrap()[0]["_score"]
            .as_f64()
            .unwrap();
        let term_score = c
            .search("body", "quick brown", &json!({}), 0, Some(10))
            .unwrap()[0]["_score"]
            .as_f64()
            .unwrap();
        assert!(phrase_score > term_score);
    }

    #[test]
    fn segment_text_full_compaction_removes_tombstones() {
        let (_dir, store) = store();
        let c = store.collection("segment_text_full_compact").unwrap();
        for i in 0..10 {
            c.insert_one(json!({"_id": format!("d{i}"), "body": "rust database"}))
                .unwrap();
        }
        c.create_text_segment_index("body").unwrap();
        for i in 0..4 {
            c.delete(&json!({"_id": format!("d{i}")}), false).unwrap();
        }
        let mut wtxn = store.env().write_txn().unwrap();
        let def = c
            .search_index_def(&wtxn, SearchKind::Text, "body")
            .unwrap()
            .unwrap();
        c.maybe_full_compact_text_segments(&mut wtxn, &def).unwrap();
        wtxn.commit().unwrap();
        let rtxn = store.env().read_txn().unwrap();
        let manifest = c.segment_manifest_in(&rtxn, "body").unwrap().unwrap();
        assert_eq!(manifest.segments.len(), 1);
        assert!(manifest.segments[0].contains("full-"));
        assert!(manifest.tombstones.is_empty());
        drop(rtxn);
        assert_eq!(
            c.search("body", "rust", &json!({}), 0, Some(20))
                .unwrap()
                .len(),
            6
        );
    }

    #[test]
    fn segment_fuzzy_full_compaction_removes_tombstones() {
        let (_dir, store) = store();
        let c = store.collection("segment_fuzzy_full_compact").unwrap();
        for (id, name) in [
            ("a", "Warszawa"),
            ("b", "Krakow"),
            ("c", "Wroclaw"),
            ("d", "Gdansk"),
            ("e", "Poznan"),
            ("f", "Lodz"),
            ("g", "Szczecin"),
            ("h", "Lublin"),
        ] {
            c.insert_one(json!({"_id": id, "name": name})).unwrap();
        }
        c.create_fuzzy_segment_index("name").unwrap();
        for id in ["b", "c", "d"] {
            c.delete(&json!({"_id": id}), false).unwrap();
        }
        let mut wtxn = store.env().write_txn().unwrap();
        let def = c
            .search_index_def(&wtxn, SearchKind::Fuzzy, "name")
            .unwrap()
            .unwrap();
        c.maybe_full_compact_fuzzy_segments(&mut wtxn, &def)
            .unwrap();
        wtxn.commit().unwrap();
        let rtxn = store.env().read_txn().unwrap();
        let manifest = c.fuzzy_segment_manifest_in(&rtxn, "name").unwrap().unwrap();
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.values.len(), 1);
        assert!(manifest.segments[0].contains("full-"));
        assert!(manifest.tombstones.is_empty());
        drop(rtxn);
        assert!(c
            .fuzzy_search("name", "Krakw", 1, &json!({}), Some(10))
            .unwrap()
            .is_empty());
        assert_eq!(
            c.fuzzy_search("name", "Warszwa", 1, &json!({}), Some(10))
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn segment_text_compaction_merges_flushed_suffix() {
        let (_dir, store) = store();
        let c = store.collection("segment_text_compact").unwrap();
        for (id, body) in [
            ("a", "rust database"),
            ("b", "rust web"),
            ("c", "python database"),
            ("d", "python web"),
        ] {
            c.insert_one(json!({"_id": id, "body": body})).unwrap();
        }
        c.create_text_segment_index("body").unwrap();
        let root = c.segment_root().unwrap();
        let dir = segment_index_dir(&c.name, "body");
        let manifest = {
            let rtxn = store.env().read_txn().unwrap();
            c.segment_manifest_in(&rtxn, "body").unwrap().unwrap()
        };
        let mut manifest = manifest;
        for (n, (id, body)) in [
            ("b", "rust web"),
            ("c", "python database"),
            ("d", "python web"),
        ]
        .into_iter()
        .enumerate()
        {
            let rel = format!("{dir}/seg-extra-{n}.seg");
            let mut builder = SegmentBuilder::new(root.join(&rel)).unwrap();
            builder.add_document(id, body).unwrap();
            builder.finish().unwrap();
            manifest.segments.push(rel);
        }
        let mut wtxn = store.env().write_txn().unwrap();
        c.put_segment_manifest(&mut wtxn, "body", &manifest)
            .unwrap();
        let def = c
            .search_index_def(&wtxn, SearchKind::Text, "body")
            .unwrap()
            .unwrap();
        c.maybe_compact_text_segments(&mut wtxn, &def).unwrap();
        wtxn.commit().unwrap();
        let rtxn = store.env().read_txn().unwrap();
        let compacted = c.segment_manifest_in(&rtxn, "body").unwrap().unwrap();
        assert_eq!(compacted.segments.len(), 2);
        drop(rtxn);
        let mut ids: Vec<String> = c
            .search("body", "python", &json!({}), 0, Some(10))
            .unwrap()
            .into_iter()
            .map(|doc| doc["_id"].as_str().unwrap().to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["c".to_string(), "d".to_string()]);
    }

    #[test]
    fn segment_fuzzy_index_supports_filters() {
        let (_dir, store) = store();
        let c = store.collection("segment_fuzzy_filter").unwrap();
        for doc in [
            json!({"_id": "a", "name": "Warszawa", "cat": 1, "tag": "x"}),
            json!({"_id": "b", "name": "Warszawa", "cat": 2, "tag": "y"}),
        ] {
            c.insert_one(doc).unwrap();
        }
        c.create_index("cat", false).unwrap();
        c.create_fuzzy_segment_index("name").unwrap();
        let hits = c
            .fuzzy_search("name", "Warszwa", 1, &json!({"cat": 2}), Some(10))
            .unwrap();
        let all = c
            .fuzzy_search("name", "Warszwa", 1, &json!({}), Some(10))
            .unwrap();
        assert_eq!(all.len(), 2, "unfiltered: {all:?}");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "b");
        let hits = c
            .fuzzy_search("name", "Warszwa", 1, &json!({"cat": {"$gt": 1}}), Some(10))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "b");
        let hits = c
            .fuzzy_search("name", "Warszwa", 1, &json!({"tag": "x"}), Some(10))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "a");
        let hits = c
            .fuzzy_search(
                "name",
                "Warszwa",
                1,
                &json!({"cat": 2, "tag": "y"}),
                Some(10),
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "b");
    }

    #[test]
    fn segment_fuzzy_compaction_merges_flushed_suffix() {
        let (_dir, store) = store();
        let c = store.collection("segment_fuzzy_compact").unwrap();
        for (id, name) in [
            ("a", "Warszawa"),
            ("b", "Krakow"),
            ("c", "Wroclaw"),
            ("d", "Gdansk"),
        ] {
            c.insert_one(json!({"_id": id, "name": name})).unwrap();
        }
        c.create_fuzzy_segment_index("name").unwrap();
        let root = c.segment_root().unwrap();
        let dir = fuzzy_segment_index_dir(&c.name, "name");
        let manifest = {
            let rtxn = store.env().read_txn().unwrap();
            c.fuzzy_segment_manifest_in(&rtxn, "name").unwrap().unwrap()
        };
        let mut manifest = manifest;
        for (n, (id, name)) in [("b", "Krakow"), ("c", "Wroclaw"), ("d", "Gdansk")]
            .into_iter()
            .enumerate()
        {
            let rel = format!("{dir}/seg-extra-{n}.seg");
            let values_rel = format!("{dir}/seg-extra-{n}.values");
            let mut builder = FuzzySegmentBuilder::new(root.join(&rel)).unwrap();
            builder.add_document(id, name).unwrap();
            builder.finish(root.join(&values_rel)).unwrap();
            manifest.segments.push(rel);
            manifest.values.push(values_rel);
        }
        let mut wtxn = store.env().write_txn().unwrap();
        c.put_fuzzy_segment_manifest(&mut wtxn, "name", &manifest)
            .unwrap();
        let def = c
            .search_index_def(&wtxn, SearchKind::Fuzzy, "name")
            .unwrap()
            .unwrap();
        c.maybe_compact_fuzzy_segments(&mut wtxn, &def).unwrap();
        wtxn.commit().unwrap();
        let rtxn = store.env().read_txn().unwrap();
        let compacted = c.fuzzy_segment_manifest_in(&rtxn, "name").unwrap().unwrap();
        assert_eq!(compacted.segments.len(), 2);
        assert_eq!(compacted.values.len(), 2);
        drop(rtxn);
        let hits = c
            .fuzzy_search("name", "Krakw", 1, &json!({}), Some(10))
            .unwrap();
        assert_eq!(hits[0]["_id"], "b");
    }

    #[test]
    fn segment_fuzzy_index_supports_crud() {
        let (_dir, store) = store();
        let c = store.collection("segment_fuzzy_crud").unwrap();
        for doc in [
            json!({"_id": "a", "name": "Warszawa"}),
            json!({"_id": "b", "name": "Krakow"}),
            json!({"_id": "c", "name": "Wroclaw"}),
        ] {
            c.insert_one(doc).unwrap();
        }
        c.create_fuzzy_segment_index("name").unwrap();
        let hits = c
            .fuzzy_search("name", "Warszwa", 2, &json!({}), Some(10))
            .unwrap();
        assert_eq!(hits[0]["_id"], "a");
        assert_eq!(hits[0]["_distance"], 1);
        c.update(
            &json!({"_id": "a"}),
            &json!({"$set": {"name": "Berlin"}}),
            false,
        )
        .unwrap();
        assert!(c
            .fuzzy_search("name", "Warszwa", 1, &json!({}), Some(10))
            .unwrap()
            .is_empty());
        c.delete(&json!({"_id": "b"}), false).unwrap();
        assert!(c
            .fuzzy_search("name", "Krakw", 1, &json!({}), Some(10))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn bulk_delete_batches_text_and_fuzzy_tombstones() {
        let (_dir, store) = store();
        let c = store.collection("bulk_delete_search").unwrap();
        for i in 0..1000 {
            c.insert_one(json!({
                "_id": format!("d{i:04}"),
                "body": "rust database search",
                "name": format!("City{i:04}"),
            }))
            .unwrap();
        }
        c.create_text_segment_index("body").unwrap();
        c.create_fuzzy_segment_index("name").unwrap();

        assert_eq!(c.delete(&json!({}), true).unwrap(), 1000);
        assert_eq!(c.count(&json!({})).unwrap(), 0);
        assert!(c
            .search("body", "rust", &json!({}), 0, Some(10))
            .unwrap()
            .is_empty());
        assert!(c
            .fuzzy_search("name", "city0500", 0, &json!({}), Some(10))
            .unwrap()
            .is_empty());

        let rtxn = store.env().read_txn().unwrap();
        let text = c.segment_manifest_in(&rtxn, "body").unwrap().unwrap();
        let fuzzy = c.fuzzy_segment_manifest_in(&rtxn, "name").unwrap().unwrap();
        assert_eq!(text.tombstones.len(), 1000);
        assert_eq!(fuzzy.tombstones.len(), 1000);
    }

    #[test]
    fn create_indexes_batch_builds_all_indexes_in_one_pass() {
        let (_dir, store) = store();
        let c = store.collection("batch_indexes").unwrap();
        for i in 0..300i64 {
            c.insert_one(json!({
                "grp": i % 10,
                "score": i,
                "tags": [format!("t{}", i % 3), format!("t{}", i % 5)],
            }))
            .unwrap();
        }
        c.create_indexes(&[
            IndexSpec {
                fields: vec!["grp".into()],
                name: String::new(),
                directions: Vec::new(),
                unique: false,
                sparse: false,
                partial_filter_expression: None,
                include: vec![],
                geo: false,
            },
            IndexSpec {
                fields: vec!["score".into()],
                name: String::new(),
                directions: Vec::new(),
                unique: false,
                sparse: false,
                partial_filter_expression: None,
                include: vec!["grp".into()],
                geo: false,
            },
            IndexSpec {
                fields: vec!["tags".into()],
                name: String::new(),
                directions: Vec::new(),
                unique: false,
                sparse: false,
                partial_filter_expression: None,
                include: vec![],
                geo: false,
            },
        ])
        .unwrap();
        let names: HashSet<_> = c
            .list_indexes()
            .unwrap()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert!(names.contains("grp"));
        assert!(names.contains("score"));
        assert!(names.contains("tags"));
        assert_eq!(c.count(&json!({"grp": 3})).unwrap(), 30);
        assert_eq!(c.count(&json!({"tags": "t1"})).unwrap(), 140);

        let dup = store.collection("batch_unique").unwrap();
        dup.insert_one(json!({"email": "a@example.com"})).unwrap();
        dup.insert_one(json!({"email": "a@example.com"})).unwrap();
        assert!(dup
            .create_indexes(&[IndexSpec {
                fields: vec!["email".into()],
                name: String::new(),
                directions: Vec::new(),
                unique: true,
                sparse: false,
                partial_filter_expression: None,
                include: vec![],
                geo: false,
            }])
            .is_err());
        assert!(dup.list_indexes().unwrap().is_empty());
    }

    #[test]
    fn reindex_rebuilds_every_index_type() {
        let (_dir, store) = store();
        let c = store.collection("reindexed").unwrap();
        c.insert_one(json!({
            "_id": "a", "grp": 1, "score": 10, "tags": ["x"],
            "body": "rust database engine", "name": "hello",
            "embedding": [1.0, 0.0], "email": "a@example.com"
        }))
        .unwrap();
        c.insert_one(json!({
            "_id": "b", "grp": 2, "score": 20, "tags": ["y"],
            "body": "python web framework", "name": "world",
            "embedding": [0.0, 1.0], "email": "b@example.com"
        }))
        .unwrap();
        c.create_index_fields(&["grp".into()], false, &["score".into()])
            .unwrap();
        c.create_text_index("body").unwrap();
        c.create_fuzzy_index("name").unwrap();
        c.create_vector_index("embedding", 2, VectorMetric::Cosine)
            .unwrap();

        let report = c.reindex().unwrap();
        assert_eq!(report.secondary, vec!["grp".to_string()]);
        assert_eq!(report.text, vec!["body".to_string()]);
        assert_eq!(report.fuzzy, vec!["name".to_string()]);
        assert_eq!(report.vector, vec!["embedding".to_string()]);

        assert_eq!(c.count(&json!({"grp": 1})).unwrap(), 1);
        assert_eq!(
            c.search("body", "rust", &serde_json::json!({}), 0, Some(10))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            c.fuzzy_search("name", "helo", 2, &serde_json::json!({}), Some(10))
                .unwrap()
                .len(),
            1
        );
        let vector = c
            .vector_search(
                "embedding",
                &[1.0, 0.0],
                &serde_json::json!({}),
                0,
                Some(10),
            )
            .unwrap();
        assert_eq!(vector[0]["_id"], "a");
    }

    #[test]
    fn text_search_ranks_and_stays_in_sync() {
        let (_dir, store) = store();
        let c = store.collection("docs").unwrap();
        c.insert_one(json!({"title": "rust database engine", "tag": "db"}))
            .unwrap();
        c.insert_one(json!({"title": "rust rust rust async runtime", "tag": "db"}))
            .unwrap();
        c.insert_one(json!({"title": "python web framework", "tag": "web"}))
            .unwrap();
        c.create_text_index("title").unwrap();
        c.create_index("tag", false).unwrap();
        assert_eq!(c.list_text_indexes().unwrap(), vec!["title".to_string()]);

        let hits = c
            .search("title", "rust", &serde_json::json!({}), 0, Some(10))
            .unwrap();
        assert_eq!(hits.len(), 2);
        // A document with higher `rust` frequency has a higher score.
        assert!(hits[0]["title"].as_str().unwrap().contains("rust rust"));
        assert!(hits[0]["_score"].as_f64().unwrap() > hits[1]["_score"].as_f64().unwrap());

        // The residual filter works.
        let only_db = c
            .search("title", "rust", &json!({"tag": "db"}), 0, Some(10))
            .unwrap();
        assert_eq!(only_db.len(), 2);

        // Update and delete keep the index in sync.
        c.update(
            &json!({"title": "python web framework"}),
            &json!({"$set": {"title": "rust web framework"}}),
            false,
        )
        .unwrap();
        let hits = c
            .search("title", "rust", &serde_json::json!({}), 0, Some(10))
            .unwrap();
        assert_eq!(hits.len(), 3);

        c.delete(&json!({"title": "rust database engine"}), false)
            .unwrap();
        let hits = c
            .search("title", "database", &serde_json::json!({}), 0, Some(10))
            .unwrap();
        assert!(hits.is_empty());

        c.drop_text_index("title").unwrap();
        assert!(c
            .search("title", "rust", &serde_json::json!({}), 0, None)
            .is_err());
    }

    #[test]
    fn parallel_text_and_vector_search_return_exact_results() {
        let (_dir, store) = store();
        let c = store.collection("parallel_search").unwrap();
        for i in 0..300 {
            let body = if i == 77 {
                "rust rust rust rust"
            } else {
                "rust filler"
            };
            c.insert_one(json!({
                "_id": format!("d{i:03}"),
                "body": body,
                "vec": [i as f64, 1.0]
            }))
            .unwrap();
        }
        c.create_text_index("body").unwrap();
        c.create_vector_index("vec", 2, VectorMetric::L2).unwrap();

        let hits = c.search("body", "rust", &json!({}), 0, Some(10)).unwrap();
        assert_eq!(hits.len(), 10);
        assert_eq!(hits[0]["_id"], "d077");

        let hits = c
            .vector_search("vec", &[299.0, 1.0], &json!({}), 0, Some(5))
            .unwrap();
        assert_eq!(hits.len(), 5);
        assert_eq!(hits[0]["_id"], "d299");
    }

    #[test]
    fn fuzzy_search_finds_typos_and_orders_by_distance() {
        let (_dir, store) = store();
        let c = store.collection("people").unwrap();
        c.insert_one(json!({"name": "Aleksandra"})).unwrap();
        c.insert_one(json!({"name": "Aleksander"})).unwrap();
        c.insert_one(json!({"name": "Bartosz"})).unwrap();
        c.create_fuzzy_index("name").unwrap();

        let hits = c
            .fuzzy_search("name", "aleksander", 2, &serde_json::json!({}), Some(10))
            .unwrap();
        assert!(hits.len() >= 2);
        assert_eq!(hits[0]["name"], "Aleksander");
        assert_eq!(hits[0]["_distance"], 0);

        let hits = c
            .fuzzy_search("name", "aleksndra", 2, &serde_json::json!({}), Some(10))
            .unwrap();
        assert!(hits.iter().any(|d| d["name"] == "Aleksandra"));

        let hits = c
            .fuzzy_search("name", "bartek", 1, &serde_json::json!({}), Some(10))
            .unwrap();
        assert!(hits.is_empty());

        c.update(
            &json!({"name": "Bartosz"}),
            &json!({"$set": {"name": "Bartlomiej"}}),
            false,
        )
        .unwrap();
        let hits = c
            .fuzzy_search("name", "bartlomiej", 2, &serde_json::json!({}), Some(10))
            .unwrap();
        assert_eq!(hits.len(), 1);

        c.delete(&json!({"name": "Aleksander"}), false).unwrap();
        let hits = c
            .fuzzy_search("name", "aleksander", 1, &serde_json::json!({}), Some(10))
            .unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn fuzzy_candidate_merge_keeps_exact_match_past_the_candidate_limit() {
        let (_dir, store) = store();
        let c = store.collection("fuzzy_many").unwrap();
        for i in 0..5200 {
            c.insert_one(json!({
                "_id": format!("f{i:05}"),
                "title": format!("w{i:05}")
            }))
            .unwrap();
        }
        c.create_fuzzy_index("title").unwrap();

        let hits = c
            .fuzzy_search("title", "w05199", 1, &json!({}), Some(5))
            .unwrap();
        assert_eq!(hits[0]["_id"], "f05199");
        assert_eq!(hits[0]["_distance"], 0);
    }

    #[test]
    fn count_fast_paths_are_correct() {
        let (_dir, store) = store();
        let c = store.collection("nums").unwrap();
        c.insert_one(json!({"v": 1})).unwrap();
        c.insert_one(json!({"v": 2})).unwrap();
        c.insert_one(serde_json::json!({})).unwrap(); // missing field
        c.insert_one(json!({"v": null})).unwrap();
        c.create_index("v", false).unwrap();

        assert_eq!(c.count(&serde_json::json!({})).unwrap(), 4);
        assert_eq!(c.count(&Value::Null).unwrap(), 4);
        assert_eq!(c.count(&json!({"v": 1})).unwrap(), 1);
        assert_eq!(c.count(&json!({"v": {"$gte": 2}})).unwrap(), 1);
        // `null` also matches a missing field. The planner stays conservative
        // and scans, even though plain indexes now encode missing as null.
        assert_eq!(c.count(&json!({"v": null})).unwrap(), 2);
        assert_eq!(c.count(&json!({"v": {"$eq": null}})).unwrap(), 2);

        let id = c.find_one(&json!({"v": 1})).unwrap().unwrap()["_id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(c.count(&json!({"_id": id})).unwrap(), 1);
        assert_eq!(c.count(&json!({"_id": "missing"})).unwrap(), 0);
    }

    #[test]
    fn find_limit_early_exit_and_sorted_queries() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..50 {
            c.insert_one(json!({"n": i, "grp": i % 5})).unwrap();
        }
        c.create_index("grp", false).unwrap();

        let q = FindQuery {
            filter: json!({"grp": 1}),
            limit: Some(3),
            skip: 2,
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 3);

        let q = FindQuery {
            filter: serde_json::json!({}),
            sort: vec![("n".into(), 1)],
            limit: Some(3),
            ..Default::default()
        };
        let ns: Vec<_> = c
            .find(&q)
            .unwrap()
            .iter()
            .map(|d| d["n"].as_i64())
            .collect();
        assert_eq!(ns, vec![Some(0), Some(1), Some(2)]);

        let q = FindQuery {
            filter: serde_json::json!({}),
            sort: vec![("n".into(), -1)],
            skip: 10,
            limit: Some(3),
            ..Default::default()
        };
        let ns: Vec<_> = c
            .find(&q)
            .unwrap()
            .iter()
            .map(|d| d["n"].as_i64())
            .collect();
        assert_eq!(ns, vec![Some(39), Some(38), Some(37)]);

        // `find_one` on an indexed field returns exactly one document.
        let one = c.find_one(&json!({"grp": 1})).unwrap().unwrap();
        assert_eq!(one["grp"], 1);
    }

    #[test]
    fn skip_and_sorted_topk_match_full_sort() {
        let (_dir, store) = store();
        let c = store.collection("skip").unwrap();
        for i in 0..100 {
            c.insert_one(json!({"n": i, "grp": i % 3})).unwrap();
        }

        // `skip` without a filter: natural `_id` order, without collecting skipped documents.
        let q = FindQuery {
            filter: serde_json::json!({}),
            skip: 90,
            limit: Some(5),
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 5);

        // `skip` with a non-match-all filter: only matching documents are skipped.
        let q = FindQuery {
            filter: json!({"grp": 0}),
            skip: 20,
            limit: Some(5),
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 5);

        // TopK with sorting, skip, and many equal keys.
        let all = c
            .find(&FindQuery {
                filter: serde_json::json!({}),
                ..Default::default()
            })
            .unwrap();
        let mut expected_docs = all.clone();
        expected_docs.sort_by_key(|d| d["grp"].as_i64().unwrap());
        let expected: Vec<_> = expected_docs
            .iter()
            .skip(50)
            .take(7)
            .map(|d| d["_id"].clone())
            .collect();

        let q = FindQuery {
            filter: serde_json::json!({}),
            sort: vec![("grp".into(), 1)],
            skip: 50,
            limit: Some(7),
            ..Default::default()
        };
        let got: Vec<_> = c
            .find(&q)
            .unwrap()
            .iter()
            .map(|d| d["_id"].clone())
            .collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn null_filter_with_index_falls_back_to_scan() {
        let (_dir, store) = store();
        let c = store.collection("nulls").unwrap();
        c.insert_one(json!({"a": null, "tag": "present"})).unwrap();
        c.insert_one(json!({"b": 1, "tag": "missing"})).unwrap();
        c.create_index("a", false).unwrap();

        // A missing field also matches `null`; the planner deliberately scans.
        assert_eq!(c.count(&json!({"a": null})).unwrap(), 2);
        let q = FindQuery {
            filter: json!({"a": null}),
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 2);
        assert_eq!(c.count(&json!({"a": {"$eq": null}})).unwrap(), 2);
    }

    #[test]
    fn range_index_respects_types() {
        let (_dir, store) = store();
        let c = store.collection("typed").unwrap();
        for v in [
            json!(1),
            json!(3),
            json!(10),
            json!("a"),
            json!("z"),
            json!(true),
        ] {
            c.insert_one(json!({"n": v})).unwrap();
        }
        c.create_index("n", false).unwrap();

        // The matcher does not compare different types: only numbers > 2.
        assert_eq!(c.count(&json!({"n": {"$gt": 2}})).unwrap(), 2);
        assert_eq!(c.count(&json!({"n": {"$lt": 2}})).unwrap(), 1);
        let mut ns: Vec<_> = c
            .find(&FindQuery {
                filter: json!({"n": {"$gt": 2}}),
                limit: Some(10),
                ..Default::default()
            })
            .unwrap()
            .iter()
            .map(|d| d["n"].as_i64().unwrap())
            .collect();
        ns.sort();
        assert_eq!(ns, vec![3, 10]);

        // Only strings < "m".
        assert_eq!(c.count(&json!({"n": {"$lt": "m"}})).unwrap(), 1);
    }

    #[test]
    fn combined_range_index_and_count() {
        let (_dir, store) = store();
        let c = store.collection("ranges").unwrap();
        for i in 0..100 {
            c.insert_one(json!({"n": i, "grp": i % 10})).unwrap();
        }
        c.create_index("n", false).unwrap();

        let q = FindQuery {
            filter: json!({"n": {"$gte": 30, "$lt": 35}}),
            limit: Some(100),
            ..Default::default()
        };
        let mut ns: Vec<_> = c
            .find(&q)
            .unwrap()
            .iter()
            .map(|d| d["n"].as_i64().unwrap())
            .collect();
        ns.sort();
        assert_eq!(ns, vec![30, 31, 32, 33, 34]);
        assert_eq!(c.count(&json!({"n": {"$gte": 30, "$lt": 35}})).unwrap(), 5);
        assert_eq!(c.count(&json!({"n": {"$gt": 30, "$lte": 35}})).unwrap(), 5);

        // Range + sorting + skip/limit.
        let q = FindQuery {
            filter: json!({"n": {"$gte": 30, "$lt": 35}}),
            sort: vec![("n".into(), -1)],
            skip: 1,
            limit: Some(2),
            ..Default::default()
        };
        let ns: Vec<_> = c
            .find(&q)
            .unwrap()
            .iter()
            .map(|d| d["n"].as_i64().unwrap())
            .collect();
        assert_eq!(ns, vec![33, 32]);
    }

    #[test]
    fn update_delete_use_index_and_residual_filter() {
        let (_dir, store) = store();
        let c = store.collection("groups").unwrap();
        c.insert_one(json!({"g": "a", "n": 1})).unwrap();
        c.insert_one(json!({"g": "a", "n": 2})).unwrap();
        c.insert_one(json!({"g": "b", "n": 3})).unwrap();
        c.create_index("g", false).unwrap();

        let updated = c
            .update(&json!({"g": "a"}), &json!({"$set": {"n": 0}}), true)
            .unwrap();
        assert_eq!(updated, 2);
        assert_eq!(c.count(&json!({"g": "a", "n": 0})).unwrap(), 2);

        let deleted = c.delete(&json!({"g": "b"}), false).unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(c.count(&serde_json::json!({})).unwrap(), 2);

        // Exact `_id` path in update/delete.
        let one = c.find_one(&json!({"g": "a"})).unwrap().unwrap();
        let id = one["_id"].as_str().unwrap().to_string();
        assert_eq!(
            c.update(&json!({"_id": id}), &json!({"$inc": {"n": 5}}), false)
                .unwrap(),
            1
        );
        assert_eq!(c.delete(&json!({"_id": id}), false).unwrap(), 1);
        assert_eq!(c.count(&serde_json::json!({})).unwrap(), 1);
    }

    #[test]
    fn lookup_stage_joins_and_filters() {
        let (_dir, store) = store();
        let suppliers = store.collection("lookup_suppliers").unwrap();
        suppliers
            .insert_one(json!({"_id": "s1", "supplier": "Acme"}))
            .unwrap();
        suppliers
            .insert_one(json!({"_id": "s2", "supplier": "Beta"}))
            .unwrap();

        let items = store.collection("lookup_items").unwrap();
        items
            .insert_one(json!({
                "_id": "i1", "sku": "a", "name": "A", "active": true, "supplier_id": "s1"
            }))
            .unwrap();
        items
            .insert_one(json!({
                "_id": "i2", "sku": "b", "name": "B", "active": false, "supplier_id": "s1"
            }))
            .unwrap();
        items
            .insert_one(json!({
                "_id": "i3", "sku": "c", "name": "C", "active": true, "supplier_id": "s2"
            }))
            .unwrap();
        items
            .insert_one(json!({"_id": "i4", "name": "missing sku"}))
            .unwrap();

        let orders = store.collection("lookup_orders").unwrap();
        orders
            .insert_one(json!({"_id": "o1", "skus": ["a", "b"], "sku": "a"}))
            .unwrap();
        orders
            .insert_one(json!({"_id": "o2", "sku": "c", "skus": ["c"]}))
            .unwrap();
        orders.insert_one(json!({"_id": "o3"})).unwrap();

        let out = orders
            .aggregate(&[
                json!({"$sort": {"_id": 1}}),
                json!({"$lookup": {
                    "from": "lookup_items",
                    "localField": "skus",
                    "foreignField": "sku",
                    "as": "items"
                }}),
            ])
            .unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0]["items"][0]["name"], "A");
        assert_eq!(out[0]["items"][1]["name"], "B");
        assert_eq!(out[1]["items"][0]["name"], "C");
        assert_eq!(out[2]["items"][0]["name"], "missing sku");

        let out = orders
            .aggregate(&[
                json!({"$sort": {"_id": 1}}),
                json!({"$lookup": {
                    "from": "lookup_items",
                    "localField": "sku",
                    "foreignField": "sku",
                    "as": "active",
                    "pipeline": [
                        {"$match": {"active": true}},
                        {"$project": {"_id": 0, "name": 1}}
                    ]
                }}),
            ])
            .unwrap();
        assert_eq!(out[0]["active"], json!([{"name": "A"}]));
        assert_eq!(out[1]["active"], json!([{"name": "C"}]));

        let out = orders
            .aggregate(&[
                json!({"$sort": {"_id": 1}}),
                json!({"$lookup": {
                    "from": "lookup_items",
                    "pipeline": [
                        {"$match": {"active": true}},
                        {"$project": {"_id": 0, "name": 1}}
                    ],
                    "as": "all_active"
                }}),
            ])
            .unwrap();
        assert_eq!(out[0]["all_active"], json!([{"name": "A"}, {"name": "C"}]));

        let out = orders
            .aggregate(&[
                json!({"$sort": {"_id": 1}}),
                json!({"$facet": {
                    "joined": [
                        {"$match": {"_id": "o1"}},
                        {"$lookup": {
                            "from": "lookup_items",
                            "localField": "skus",
                            "foreignField": "sku",
                            "as": "items"
                        }}
                    ]
                }}),
            ])
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["joined"][0]["items"][1]["name"], "B");

        let out = orders
            .aggregate(&[
                json!({"$sort": {"_id": 1}}),
                json!({"$lookup": {
                    "from": "lookup_items",
                    "let": {"wanted": "$sku"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$sku", "$$wanted"]}}},
                        {"$project": {"_id": 0, "name": 1, "wanted": "$$wanted"}}
                    ],
                    "as": "let_match"
                }}),
            ])
            .unwrap();
        assert_eq!(out[0]["let_match"], json!([{"name": "A", "wanted": "a"}]));
        assert_eq!(out[1]["let_match"], json!([{"name": "C", "wanted": "c"}]));
        assert_eq!(
            out[2]["let_match"],
            json!([{"name": "missing sku", "wanted": null}])
        );

        let out = orders
            .aggregate(&[
                json!({"$sort": {"_id": 1}}),
                json!({"$lookup": {
                    "from": "lookup_items",
                    "let": {"wanted": "$sku"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$sku", "$$wanted"]}}},
                        {"$group": {"_id": "$$wanted", "n": {"$sum": 1}}}
                    ],
                    "as": "let_group"
                }}),
            ])
            .unwrap();
        assert_eq!(out[0]["let_group"], json!([{"_id": "a", "n": 1}]));
        assert_eq!(out[1]["let_group"], json!([{"_id": "c", "n": 1}]));

        let out = orders
            .aggregate(&[
                json!({"$match": {"_id": "o1"}}),
                json!({"$lookup": {
                    "from": "lookup_items",
                    "let": {"wanted": "$sku"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$sku", "$$wanted"]}}},
                        {"$project": {
                            "_id": 0,
                            "name": 1,
                            "label": {"$convert": {"input": "$name", "to": "string"}},
                            "code": {"$toString": "$sku"}
                        }}
                    ],
                    "as": "converted"
                }}),
            ])
            .unwrap();
        assert_eq!(
            out[0]["converted"],
            json!([{"name": "A", "label": "A", "code": "a"}])
        );

        let out = orders
            .aggregate(&[
                json!({"$sort": {"_id": 1}}),
                json!({"$lookup": {
                    "from": "lookup_items",
                    "localField": "skus",
                    "foreignField": "sku",
                    "pipeline": [
                        {"$lookup": {
                            "from": "lookup_suppliers",
                            "localField": "supplier_id",
                            "foreignField": "_id",
                            "as": "supplier"
                        }},
                        {"$project": {"_id": 0, "name": 1, "supplier": 1}}
                    ],
                    "as": "nested"
                }}),
            ])
            .unwrap();
        assert_eq!(out[0]["nested"][0]["name"], "A");
        assert_eq!(out[0]["nested"][0]["supplier"][0]["supplier"], "Acme");
        assert_eq!(out[0]["nested"][1]["supplier"][0]["supplier"], "Acme");

        assert!(orders
            .aggregate(&[json!({"$lookup": {
                "from": "lookup_items",
                "localField": "sku",
                "foreignField": "sku",
                "as": "x",
                "let": 1
            }})])
            .is_err());
        assert!(orders
            .aggregate(&[json!({"$lookup": {"from": "lookup_items", "as": "x"}})])
            .is_err());
    }

    #[test]
    fn union_with_appends_collections_and_runs_subpipelines() {
        let (_dir, store) = store();
        let primary = store.collection("union_primary").unwrap();
        primary
            .insert_one(json!({"_id": "p1", "active": true, "value": 1}))
            .unwrap();
        primary
            .insert_one(json!({"_id": "p2", "active": false, "value": 2}))
            .unwrap();
        let archive = store.collection("union_archive").unwrap();
        archive
            .insert_one(json!({"_id": "a1", "active": true, "value": 3}))
            .unwrap();
        archive
            .insert_one(json!({"_id": "a2", "active": false, "value": 4}))
            .unwrap();

        let out = primary
            .aggregate(&[
                json!({"$match": {"active": true}}),
                json!({"$project": {"_id": 1, "value": 1}}),
                json!({"$unionWith": {
                    "coll": "union_archive",
                    "pipeline": [
                        {"$match": {"active": true}},
                        {"$project": {"_id": 1, "value": 1}}
                    ]
                }}),
                json!({"$sort": {"_id": 1}}),
            ])
            .unwrap();
        assert_eq!(
            out,
            vec![
                json!({"_id": "a1", "value": 3}),
                json!({"_id": "p1", "value": 1})
            ]
        );

        let out = primary
            .aggregate(&[
                json!({"$match": {"_id": "p1"}}),
                json!({"$unionWith": "union_archive"}),
                json!({"$sort": {"_id": 1}}),
            ])
            .unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0]["_id"], "a1");
        assert_eq!(out[1]["_id"], "a2");
        assert_eq!(out[2]["_id"], "p1");

        let out = primary
            .aggregate(&[
                json!({"$match": {"_id": "p1"}}),
                json!({"$unionWith": "union_missing"}),
            ])
            .unwrap();
        assert_eq!(out, vec![json!({"_id": "p1", "active": true, "value": 1})]);

        assert!(primary
            .aggregate(&[json!({"$unionWith": {"pipeline": []}})])
            .is_err());
        assert!(primary
            .aggregate(&[json!({"$unionWith": {"coll": "union_archive", "pipeline": 1}})])
            .is_err());
    }

    #[test]
    fn online_rebuild_rejects_generation_overflow() {
        let (_dir, store) = store();
        store
            .create_collection("nodes", crate::vault::CollectionProfile::Standard)
            .unwrap();
        store
            .create_collection("edges", crate::vault::CollectionProfile::Standard)
            .unwrap();
        let nodes = store.collection("nodes").unwrap();
        let def: crate::graph::GraphDef = serde_json::from_value(json!({
            "node_collection": "nodes",
            "edge_collection": "edges",
            "generation": crate::graph::MAX_GRAPH_GENERATION
        }))
        .unwrap();
        nodes.set_graph(def).unwrap();
        let error = nodes.start_graph_rebuild().unwrap_err().to_string();
        assert!(error.contains("maximum generation"), "{error}");
    }

    #[test]
    fn graph_lookup_uses_adjacency_fast_path() {
        let (_dir, store) = store();
        let nodes = store.collection("nodes").unwrap();
        let edges = store.collection("edges").unwrap();
        for id in ["n1", "n2", "n3"] {
            nodes.insert_one(json!({"_id": id})).unwrap();
        }
        let def: GraphDef = serde_json::from_value(json!({
            "node_collection": "nodes",
            "edge_collection": "edges"
        }))
        .unwrap();
        nodes.set_graph(def).unwrap();
        edges
            .insert_one(json!({"_id": "e1", "from": "n1", "to": "n2"}))
            .unwrap();
        edges
            .insert_one(json!({"_id": "e2", "from": "n2", "to": "n3"}))
            .unwrap();
        super::GRAPH_LOOKUP_FAST_PATH_HITS.store(0, std::sync::atomic::Ordering::Relaxed);
        let out = nodes
            .aggregate(&[json!({"$graphLookup": {
                "from": "edges",
                "startWith": "n3",
                "connectFromField": "from",
                "connectToField": "to",
                "as": "chain",
                "maxDepth": 2
            }})])
            .unwrap();
        assert!(super::GRAPH_LOOKUP_FAST_PATH_HITS.load(std::sync::atomic::Ordering::Relaxed) > 0);
        assert_eq!(out[0]["chain"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn graph_lookup_traverses_depth_restrictions_and_cycles() {
        let (_dir, store) = store();
        let employees = store.collection("graph_employees").unwrap();
        employees
            .insert_one(json!({"_id": "1", "name": "CEO", "reportsTo": null, "active": true}))
            .unwrap();
        employees
            .insert_one(json!({"_id": "2", "name": "VP", "reportsTo": "1", "active": true}))
            .unwrap();
        employees
            .insert_one(json!({"_id": "3", "name": "Eng", "reportsTo": "2", "active": true}))
            .unwrap();
        employees
            .insert_one(json!({"_id": "4", "name": "Old", "reportsTo": "1", "active": false}))
            .unwrap();
        employees
            .insert_one(json!({"_id": "5", "name": "Cycle", "reportsTo": "5", "active": true}))
            .unwrap();
        employees
            .insert_one(json!({"_id": "r1", "reportsTo": null, "active": true}))
            .unwrap();
        employees
            .insert_one(json!({"_id": "r2", "reportsTo": "r1", "active": false}))
            .unwrap();
        employees
            .insert_one(json!({"_id": "r3", "reportsTo": "r2", "active": true}))
            .unwrap();

        let inputs = store.collection("graph_inputs").unwrap();
        inputs
            .insert_one(json!({"_id": "q1", "reportsTo": "3"}))
            .unwrap();
        inputs
            .insert_one(json!({"_id": "q2", "reportsTo": "1"}))
            .unwrap();
        inputs
            .insert_one(json!({"_id": "q3", "reportsTo": "r3"}))
            .unwrap();

        let out = inputs
            .aggregate(&[
                json!({"$graphLookup": {
                    "from": "graph_employees",
                    "startWith": "$reportsTo",
                    "connectFromField": "reportsTo",
                    "connectToField": "_id",
                    "as": "chain",
                    "depthField": "depth",
                    "maxDepth": 3
                }}),
                json!({"$sort": {"_id": 1}}),
            ])
            .unwrap();
        let chain = out[0]["chain"].as_array().unwrap();
        assert_eq!(
            chain
                .iter()
                .map(|doc| doc["_id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["3", "2", "1"]
        );
        assert_eq!(
            chain
                .iter()
                .map(|doc| doc["depth"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );

        let out = inputs
            .aggregate(&[
                json!({"$match": {"_id": "q1"}}),
                json!({"$graphLookup": {
                    "from": "graph_employees",
                    "startWith": "$reportsTo",
                    "connectFromField": "reportsTo",
                    "connectToField": "_id",
                    "as": "chain",
                    "maxDepth": 1
                }}),
            ])
            .unwrap();
        assert_eq!(
            out[0]["chain"]
                .as_array()
                .unwrap()
                .iter()
                .map(|doc| doc["_id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["3", "2"]
        );

        let out = inputs
            .aggregate(&[
                json!({"$match": {"_id": "q3"}}),
                json!({"$graphLookup": {
                    "from": "graph_employees",
                    "startWith": "$reportsTo",
                    "connectFromField": "reportsTo",
                    "connectToField": "_id",
                    "as": "chain",
                    "maxDepth": 3,
                    "restrictSearchWithMatch": {"active": true}
                }}),
            ])
            .unwrap();
        assert_eq!(
            out[0]["chain"]
                .as_array()
                .unwrap()
                .iter()
                .map(|doc| doc["_id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["r3"]
        );

        let out = inputs
            .aggregate(&[
                json!({"$match": {"_id": "q1"}}),
                json!({"$graphLookup": {
                    "from": "graph_employees",
                    "startWith": "5",
                    "connectFromField": "reportsTo",
                    "connectToField": "_id",
                    "as": "cycle"
                }}),
            ])
            .unwrap();
        assert_eq!(out[0]["cycle"].as_array().unwrap().len(), 1);
        assert_eq!(out[0]["cycle"][0]["_id"], "5");

        let out = inputs
            .aggregate(&[
                json!({"$match": {"_id": "q1"}}),
                json!({"$graphLookup": {
                    "from": "graph_missing",
                    "startWith": "$reportsTo",
                    "connectFromField": "reportsTo",
                    "connectToField": "_id",
                    "as": "missing"
                }}),
            ])
            .unwrap();
        assert_eq!(out[0]["missing"], json!([]));

        assert!(inputs
            .aggregate(&[json!({"$graphLookup": {
                "from": "graph_employees",
                "startWith": "$reportsTo",
                "connectFromField": "reportsTo",
                "connectToField": "_id",
                "as": "bad",
                "maxDepth": -1
            }})])
            .is_err());
    }

    #[test]
    fn set_window_fields_partitions_ranks_and_accumulates() {
        let (_dir, store) = store();
        let c = store.collection("window_sales").unwrap();
        c.insert_one(json!({"_id": "a1", "region": "a", "day": 1, "amount": 10}))
            .unwrap();
        c.insert_one(json!({"_id": "a2", "region": "a", "day": 2, "amount": 20}))
            .unwrap();
        c.insert_one(json!({"_id": "a3", "region": "a", "day": 3, "amount": 5}))
            .unwrap();
        c.insert_one(json!({"_id": "b1", "region": "z", "day": 1, "amount": 7}))
            .unwrap();

        let out = c
            .aggregate(&[
                json!({"$setWindowFields": {
                    "partitionBy": "$region",
                    "sortBy": {"day": 1},
                    "output": {
                        "running": {"$sum": "$amount", "window": {"documents": ["unbounded", "current"]}},
                        "allTotal": {"$sum": "$amount"},
                        "sum3": {"$sum": "$amount", "window": {"documents": [-1, 1]}},
                        "rangeTotal": {"$sum": "$amount", "window": {"range": [-1, 1]}},
                        "rank": {"$rank": {}},
                        "dense": {"$denseRank": {}},
                        "row": {"$documentNumber": {}},
                        "prev": {"$shift": {"output": "$amount", "by": -1, "default": 0}},
                        "first": {"$first": "$amount"},
                        "count": {"$count": {}}
                    }
                }}),
                json!({"$sort": [["region", 1], ["day", 1]]}),
            ])
            .unwrap();
        assert_eq!(out.len(), 4);
        assert_eq!(out[0]["running"], 10);
        assert_eq!(out[0]["sum3"], 30);
        assert_eq!(out[0]["rangeTotal"], 30);
        assert_eq!(out[0]["rank"], 1);
        assert_eq!(out[0]["dense"], 1);
        assert_eq!(out[0]["row"], 1);
        assert_eq!(out[0]["prev"], 0);
        assert_eq!(out[0]["first"], 10);
        assert_eq!(out[0]["allTotal"], 35);
        assert_eq!(out[0]["count"], 3);

        assert_eq!(out[1]["running"], 30);
        assert_eq!(out[1]["sum3"], 35);
        assert_eq!(out[1]["rangeTotal"], 35);
        assert_eq!(out[1]["rank"], 2);
        assert_eq!(out[1]["dense"], 2);
        assert_eq!(out[1]["row"], 2);
        assert_eq!(out[1]["prev"], 10);
        assert_eq!(out[1]["first"], 10);
        assert_eq!(out[1]["allTotal"], 35);
        assert_eq!(out[1]["count"], 3);

        assert_eq!(out[2]["running"], 35);
        assert_eq!(out[2]["sum3"], 25);
        assert_eq!(out[2]["rangeTotal"], 25);
        assert_eq!(out[2]["rank"], 3);
        assert_eq!(out[2]["dense"], 3);
        assert_eq!(out[2]["row"], 3);
        assert_eq!(out[2]["prev"], 20);
        assert_eq!(out[2]["first"], 10);
        assert_eq!(out[2]["allTotal"], 35);
        assert_eq!(out[2]["count"], 3);

        assert_eq!(out[3]["running"], 7);
        assert_eq!(out[3]["sum3"], 7);
        assert_eq!(out[3]["rangeTotal"], 7);
        assert_eq!(out[3]["rank"], 1);
        assert_eq!(out[3]["row"], 1);
        assert_eq!(out[3]["prev"], 0);
        assert_eq!(out[3]["allTotal"], 7);
        assert_eq!(out[3]["count"], 1);

        assert!(c
            .aggregate(&[json!({"$setWindowFields": {
                "output": {"rank": {"$rank": {}}}
            }})])
            .is_err());
        assert!(c
            .aggregate(&[json!({"$setWindowFields": {
                "sortBy": {"day": 1},
                "output": {
                    "sum": {"$sum": "$amount", "window": {"range": [-1, 1], "unit": "day"}}
                }
            }})])
            .is_err());
        assert!(c
            .aggregate(&[json!({"$setWindowFields": {
                "sortBy": {"day": 1},
                "output": {"bad": {"$sum": "$amount", "$avg": "$amount"}}
            }})])
            .is_err());

        let ties = store.collection("window_ties").unwrap();
        ties.insert_one(json!({"_id": "t1", "k": 1})).unwrap();
        ties.insert_one(json!({"_id": "t2", "k": 1})).unwrap();
        ties.insert_one(json!({"_id": "t3", "k": 2})).unwrap();
        let out = ties
            .aggregate(&[
                json!({"$setWindowFields": {
                    "sortBy": {"k": 1},
                    "output": {
                        "rank": {"$rank": {}},
                        "dense": {"$denseRank": {}},
                        "row": {"$documentNumber": {}}
                    }
                }}),
                json!({"$sort": [["_id", 1]]}),
            ])
            .unwrap();
        assert_eq!(out[0]["rank"], 1);
        assert_eq!(out[1]["rank"], 1);
        assert_eq!(out[2]["rank"], 3);
        assert_eq!(out[0]["dense"], 1);
        assert_eq!(out[1]["dense"], 1);
        assert_eq!(out[2]["dense"], 2);
        assert_eq!(out[0]["row"], 1);
        assert_eq!(out[1]["row"], 2);
        assert_eq!(out[2]["row"], 3);
    }

    #[test]
    fn advanced_window_range_derivative_integral_and_ema() {
        let (_dir, store) = store();
        let c = store.collection("window_advanced").unwrap();
        for (id, x, value) in [("a", 1, 10), ("b", 2, 20), ("c", 4, 40), ("d", 8, 80)] {
            c.insert_one(json!({"_id": id, "x": x, "value": value}))
                .unwrap();
        }
        let out = c
            .aggregate(&[
                json!({"$setWindowFields": {
                    "sortBy": {"x": 1},
                    "output": {
                        "ema": {"$expMovingAvg": {"input": "$value", "N": 3}},
                        "derivative": {"$derivative": {"input": "$value"}},
                        "integral": {"$integral": {"input": "$value"}},
                        "rangeSum": {"$sum": "$value", "window": {"range": [-1, 1]}},
                        "rangeDerivative": {
                            "$derivative": {"input": "$value"},
                            "window": {"range": [-1, 1]}
                        }
                    }
                }}),
                json!({"$sort": {"_id": 1}}),
            ])
            .unwrap();
        assert_eq!(out.len(), 4);
        assert_eq!(out[0]["ema"], 10.0);
        assert_eq!(out[1]["ema"], 15.0);
        assert_eq!(out[2]["ema"], 27.5);
        assert_eq!(out[3]["ema"], 53.75);
        for row in &out {
            assert_eq!(row["derivative"], 10.0);
            assert_eq!(row["integral"], 315.0);
        }
        assert_eq!(out[0]["rangeSum"], 30);
        assert_eq!(out[1]["rangeSum"], 30);
        assert_eq!(out[2]["rangeSum"], 40);
        assert_eq!(out[3]["rangeSum"], 80);
        assert_eq!(out[0]["rangeDerivative"], 10.0);
        assert_eq!(out[1]["rangeDerivative"], 10.0);
        assert!(out[2]["rangeDerivative"].is_null());
        assert!(out[3]["rangeDerivative"].is_null());

        let dates = store.collection("window_dates").unwrap();
        for (id, ts, value) in [
            ("a", "2024-01-01T00:00:00Z", 1),
            ("b", "2024-01-01T01:00:00Z", 2),
            ("c", "2024-01-01T03:00:00Z", 4),
        ] {
            let date =
                crate::date::canonical(crate::date::parse_value(&json!({"$date": ts})).unwrap());
            dates
                .insert_one(json!({"_id": id, "ts": date, "value": value}))
                .unwrap();
        }
        let out = dates
            .aggregate(&[
                json!({"$setWindowFields": {
                    "sortBy": {"ts": 1},
                    "output": {
                        "rangeSum": {
                            "$sum": "$value",
                            "window": {"range": [-1, 1], "unit": "hour"}
                        }
                    }
                }}),
                json!({"$sort": {"_id": 1}}),
            ])
            .unwrap();
        assert_eq!(out[0]["rangeSum"], 3);
        assert_eq!(out[1]["rangeSum"], 3);
        assert_eq!(out[2]["rangeSum"], 4);

        assert!(c
            .aggregate(&[json!({"$setWindowFields": {
                "sortBy": {"x": 1},
                "output": {"bad": {"$expMovingAvg": {"input": "$value"}}}
            }})])
            .is_err());
        assert!(c
            .aggregate(&[json!({"$setWindowFields": {
                "sortBy": {"x": 1},
                "output": {"bad": {"$derivative": {"input": "$value"}, "window": {"documents": [0, 0]}}}
            }})])
            .is_ok());
    }

    #[test]
    fn densify_and_fill_time_series_gaps() {
        let (_dir, store) = store();
        let c = store.collection("densify").unwrap();
        c.insert_one(json!({"_id": "a", "p": "x", "t": 1})).unwrap();
        c.insert_one(json!({"_id": "b", "p": "x", "t": 3})).unwrap();
        c.insert_one(json!({"_id": "c", "p": "y", "t": 2})).unwrap();

        let out = c
            .aggregate(&[
                json!({"$densify": {
                    "field": "t",
                    "partitionByFields": ["p"],
                    "range": {"step": 1, "bounds": "partition"}
                }}),
                json!({"$sort": [["p", 1], ["t", 1]]}),
            ])
            .unwrap();
        assert_eq!(out.len(), 4);
        assert_eq!(out[0]["t"], 1);
        assert_eq!(out[1]["t"], 2);
        assert!(out[1].get("_id").is_none());
        assert_eq!(out[1]["p"], "x");
        assert_eq!(out[2]["t"], 3);
        assert_eq!(out[3]["t"], 2);

        let out = c
            .aggregate(&[
                json!({"$densify": {
                    "field": "t",
                    "partitionByFields": ["p"],
                    "range": {"step": 1, "bounds": "full"}
                }}),
                json!({"$sort": [["p", 1], ["t", 1]]}),
            ])
            .unwrap();
        assert_eq!(out.len(), 6);
        assert_eq!(out[0]["p"], "x");
        assert_eq!(out[0]["t"], 1);
        assert_eq!(out[1]["t"], 2);
        assert_eq!(out[2]["t"], 3);
        assert_eq!(out[3]["p"], "y");
        assert_eq!(out[3]["t"], 1);
        assert_eq!(out[5]["t"], 3);

        let dates = store.collection("densify_dates").unwrap();
        dates
            .insert_one(json!({
                "_id": "d1",
                "at": {"$date": "2026-01-01T00:00:00Z"}
            }))
            .unwrap();
        dates
            .insert_one(json!({
                "_id": "d2",
                "at": {"$date": "2026-01-03T00:00:00Z"}
            }))
            .unwrap();
        let out = dates
            .aggregate(&[
                json!({"$densify": {
                    "field": "at",
                    "range": {"step": 1, "unit": "day", "bounds": "partition"}
                }}),
                json!({"$sort": [["at", 1]]}),
            ])
            .unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(
            out[1]["at"],
            json!({"$date": {"$numberLong": "1767312000000"}})
        );

        let fill = store.collection("fill").unwrap();
        fill.insert_one(json!({"_id": "f1", "p": "x", "t": 1, "v": null, "w": null}))
            .unwrap();
        fill.insert_one(json!({"_id": "f2", "p": "x", "t": 2, "v": 10, "w": 10}))
            .unwrap();
        fill.insert_one(json!({"_id": "f3", "p": "x", "t": 3}))
            .unwrap();
        fill.insert_one(json!({"_id": "f4", "p": "x", "t": 4, "v": 20, "w": 20}))
            .unwrap();
        fill.insert_one(json!({"_id": "f5", "p": "x", "t": 5}))
            .unwrap();
        let out = fill
            .aggregate(&[
                json!({"$fill": {
                    "partitionBy": "$p",
                    "sortBy": {"t": 1},
                    "output": {
                        "v": {"value": 0},
                        "w": {"method": "locf"}
                    }
                }}),
                json!({"$sort": [["t", 1]]}),
            ])
            .unwrap();
        assert_eq!(out[0]["v"], 0);
        assert_eq!(out[1]["v"], 10);
        assert_eq!(out[2]["v"], 0);
        assert_eq!(out[3]["v"], 20);
        assert_eq!(out[4]["v"], 0);
        assert_eq!(out[0]["w"], json!(null));
        assert_eq!(out[2]["w"], 10);
        assert_eq!(out[4]["w"], 20);

        let linear = store.collection("fill_linear").unwrap();
        linear
            .insert_one(json!({"_id": "l1", "t": 1, "v": null}))
            .unwrap();
        linear
            .insert_one(json!({"_id": "l2", "t": 2, "v": 10}))
            .unwrap();
        linear.insert_one(json!({"_id": "l3", "t": 3})).unwrap();
        linear
            .insert_one(json!({"_id": "l4", "t": 4, "v": 20}))
            .unwrap();
        linear.insert_one(json!({"_id": "l5", "t": 5})).unwrap();
        let out = linear
            .aggregate(&[
                json!({"$fill": {
                    "sortBy": {"t": 1},
                    "output": {"v": {"method": "linear"}}
                }}),
                json!({"$sort": [["t", 1]]}),
            ])
            .unwrap();
        assert_eq!(out[2]["v"], 15);
        assert_eq!(out[0]["v"], json!(null));
        assert!(out[4].get("v").is_none());

        assert!(c
            .aggregate(&[json!({"$densify": {
                "field": "t",
                "range": {"step": 0}
            }})])
            .is_err());
        assert!(fill
            .aggregate(&[json!({"$fill": {
                "sortBy": {"t": 1},
                "output": {"v": {"method": "linear"}}
            }})])
            .is_ok());
        assert!(fill
            .aggregate(&[json!({"$fill": {
                "sortBy": {"t": 1},
                "output": {"v": {"value": 1, "method": "locf"}}
            }})])
            .is_err());
        assert!(linear
            .aggregate(&[json!({"$fill": {
                "output": {"v": {"method": "linear"}}
            }})])
            .is_err());
    }

    #[test]
    fn aggregate_uses_leading_match_and_common_stages() {
        let (_dir, store) = store();
        let c = store.collection("orders").unwrap();
        c.insert_one(json!({"tenant": "a", "amount": 10, "tags": ["x"]}))
            .unwrap();
        c.insert_one(json!({"tenant": "a", "amount": 20, "tags": ["x", "y"]}))
            .unwrap();
        c.insert_one(json!({"tenant": "b", "amount": 5, "tags": []}))
            .unwrap();

        let out = c
            .aggregate(&[
                json!({"$match": {"amount": {"$gte": 10}}}),
                json!({"$group": {
                    "_id": "$tenant",
                    "total": {"$sum": "$amount"},
                    "n": {"$sum": 1}
                }}),
                json!({"$sort": {"total": -1}}),
                json!({"$project": {"_id": 0, "tenant": "$_id", "total": 1, "n": 1}}),
            ])
            .unwrap();
        assert_eq!(out, vec![json!({"tenant": "a", "total": 30, "n": 2})]);

        let out = c
            .aggregate(&[json!({"$unwind": "$tags"}), json!({"$count": "rows"})])
            .unwrap();
        assert_eq!(out, vec![json!({"rows": 3})]);
    }

    #[test]
    fn aggregate_group_uses_index_and_preserves_order_sensitive_semantics() {
        let (_dir, store) = store();
        let c = store.collection("group_index").unwrap();
        for i in 0..300i64 {
            c.insert_one(json!({
                "_id": format!("d{i:04}"),
                "grp": i % 10,
                "score": i,
            }))
            .unwrap();
        }
        c.create_index("grp", false).unwrap();

        let out = c
            .aggregate(&[
                json!({"$match": {"grp": 3}}),
                json!({"$group": {
                    "_id": "$grp",
                    "n": {"$sum": 1},
                    "total": {"$sum": "$score"},
                    "lo": {"$min": "$score"},
                    "hi": {"$max": "$score"},
                }}),
            ])
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["n"], 30);
        assert_eq!(out[0]["lo"], 3);
        assert_eq!(out[0]["hi"], 293);
        assert_eq!(out[0]["total"], (3..300).step_by(10).sum::<i64>());

        // `$first`/`$last`/`$push` must keep natural `_id` order even when an
        // index on the matched field yields a different document order.
        let ordered = store.collection("group_order").unwrap();
        ordered.insert_one(json!({"_id": "a", "seq": 3})).unwrap();
        ordered.insert_one(json!({"_id": "b", "seq": 1})).unwrap();
        ordered.insert_one(json!({"_id": "c", "seq": 2})).unwrap();
        ordered.create_index("seq", false).unwrap();
        let out = ordered
            .aggregate(&[
                json!({"$match": {"seq": {"$gte": 0}}}),
                json!({"$group": {
                    "_id": null,
                    "first": {"$first": "$seq"},
                    "last": {"$last": "$seq"},
                    "pushed": {"$push": "$seq"},
                }}),
            ])
            .unwrap();
        assert_eq!(
            out,
            vec![json!({
                "_id": null,
                "first": 3,
                "last": 2,
                "pushed": [3, 1, 2],
            })]
        );
    }

    #[test]
    fn parallel_group_matches_expected_aggregates() {
        let (_dir, store) = store();
        let c = store.collection("group_parallel").unwrap();
        for i in 0..500i64 {
            c.insert_one(json!({
                "_id": format!("p{i:04}"),
                "grp": i % 7,
                "score": i,
                "flag": i % 3 == 0,
            }))
            .unwrap();
        }
        let out = c
            .aggregate(&[
                json!({"$match": {"flag": true}}),
                json!({"$group": {
                    "_id": "$grp",
                    "n": {"$sum": 1},
                    "total": {"$sum": "$score"},
                    "lo": {"$min": "$score"},
                    "hi": {"$max": "$score"},
                    "avg": {"$avg": "$score"},
                }}),
                json!({"$sort": {"_id": 1}}),
            ])
            .unwrap();
        assert_eq!(out.len(), 7);
        for row in out {
            let grp = row["_id"].as_i64().unwrap();
            let expected: Vec<i64> = (0..500).filter(|i| i % 7 == grp && i % 3 == 0).collect();
            let n = expected.len() as i64;
            let total: i64 = expected.iter().sum();
            let lo = *expected.iter().min().unwrap();
            let hi = *expected.iter().max().unwrap();
            assert_eq!(row["n"], n);
            assert_eq!(row["total"], total);
            assert_eq!(row["lo"], lo);
            assert_eq!(row["hi"], hi);
            assert_eq!(row["avg"].as_f64().unwrap(), total as f64 / n as f64);
        }
    }

    #[test]
    fn aggregate_pushdown_preserves_stage_order() {
        let (_dir, store) = store();
        let c = store.collection("pushdown").unwrap();
        c.insert_one(json!({"_id": "a", "amount": 30, "keep": true}))
            .unwrap();
        c.insert_one(json!({"_id": "b", "amount": 10, "keep": false}))
            .unwrap();
        c.insert_one(json!({"_id": "c", "amount": 20, "keep": true}))
            .unwrap();

        // `$limit` before `$sort`: sort only the first two documents.
        let out = c
            .aggregate(&[json!({"$limit": 2}), json!({"$sort": {"amount": -1}})])
            .unwrap();
        let amounts: Vec<_> = out.iter().map(|d| d["amount"].as_i64().unwrap()).collect();
        assert_eq!(amounts, vec![30, 10]);

        // `$sort`, `$skip`, `$limit` and a simple `$project` can move together.
        let out = c
            .aggregate(&[
                json!({"$sort": {"amount": -1}}),
                json!({"$skip": 1}),
                json!({"$limit": 1}),
                json!({"$project": {"_id": 0, "amount": 1}}),
            ])
            .unwrap();
        assert_eq!(out, vec![json!({"amount": 20})]);

        // A lone `$count` after `$match` uses the count fast path.
        let out = c
            .aggregate(&[json!({"$match": {"keep": true}}), json!({"$count": "rows"})])
            .unwrap();
        assert_eq!(out, vec![json!({"rows": 2})]);

        // `{_id: 1}` must not be pushed into `find`, which treats it as an
        // exclusion projection. Aggregation returns only `_id`.
        let out = c.aggregate(&[json!({"$project": {"_id": 1}})]).unwrap();
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|d| d.as_object().unwrap().len() == 1));

        // Consecutive leading `$match` stages are combined.
        let out = c
            .aggregate(&[
                json!({"$match": {"keep": true}}),
                json!({"$match": {"amount": {"$gt": 15}}}),
                json!({"$count": "rows"}),
            ])
            .unwrap();
        assert_eq!(out, vec![json!({"rows": 2})]);

        // A cardinality-preserving prefix before `$count` can be skipped.
        let out = c
            .aggregate(&[
                json!({"$sort": {"amount": -1}}),
                json!({"$project": {"_id": 0, "amount": 1}}),
                json!({"$count": "rows"}),
            ])
            .unwrap();
        assert_eq!(out, vec![json!({"rows": 3})]);

        // Computed stages are still evaluated before counting.
        let out = c
            .aggregate(&[
                json!({"$addFields": {"double": {"$multiply": ["$amount", 2]}}}),
                json!({"$count": "rows"}),
            ])
            .unwrap();
        assert_eq!(out, vec![json!({"rows": 3})]);
        assert!(c
            .aggregate(&[
                json!({"$project": {"bad": {"$divide": [1, 0]}}}),
                json!({"$count": "rows"}),
            ])
            .is_err());
    }

    #[test]
    fn find_query_comment_and_max_time_ms() {
        let (_dir, store) = store();
        let c = store.collection("queries").unwrap();
        c.insert_one(json!({"_id": "a", "n": 1})).unwrap();

        let q = FindQuery {
            filter: json!({"n": 1}),
            projection: None,
            sort: Vec::new(),
            skip: 0,
            limit: None,
            comment: Some("test query".into()),
            max_time_ms: Some(5_000),
            hint: None,
        };
        assert_eq!(c.find(&q).unwrap().len(), 1);
        let explained = c.explain(&q).unwrap();
        assert_eq!(explained["comment"], "test query");
        assert_eq!(explained["maxTimeMS"], 5_000);

        assert!(check_find_deadline(None).is_ok());
        assert!(matches!(
            check_find_deadline(Some(Instant::now())),
            Err(Error::Timeout(_))
        ));
        assert!(find_deadline(&q).is_some());
        assert!(find_deadline(&FindQuery {
            max_time_ms: Some(0),
            ..FindQuery::default()
        })
        .is_none());
    }

    #[test]
    fn index_hints_and_natural_sort() {
        let (_dir, store) = store();
        let c = store.collection("hinted").unwrap();
        for id in ["a", "b", "c"] {
            c.insert_one(json!({"_id": id, "x": 1, "y": id})).unwrap();
        }
        c.create_index("x", false).unwrap();

        let mut q = FindQuery {
            filter: json!({"y": {"$ne": "a"}}),
            projection: None,
            sort: Vec::new(),
            skip: 0,
            limit: None,
            comment: None,
            max_time_ms: None,
            hint: Some(json!({"x": 1})),
        };
        let ids = |docs: Vec<Value>| -> Vec<String> {
            docs.into_iter()
                .map(|doc| doc["_id"].as_str().unwrap().to_string())
                .collect()
        };
        assert_eq!(ids(c.find(&q).unwrap()), vec!["b", "c"]);

        q.hint = Some(json!("x"));
        assert_eq!(ids(c.find(&q).unwrap()), vec!["b", "c"]);
        let explained = c.explain(&q).unwrap();
        assert_eq!(explained["access_path"], "index_scan");
        assert_eq!(explained["index"]["fields"], json!(["x"]));

        let mut natural = FindQuery {
            filter: serde_json::json!({}),
            projection: None,
            sort: vec![("$natural".to_string(), -1)],
            skip: 0,
            limit: None,
            comment: None,
            max_time_ms: None,
            hint: Some(json!({"$natural": 1})),
        };
        assert_eq!(ids(c.find(&natural).unwrap()), vec!["c", "b", "a"]);
        assert_eq!(
            c.explain(&natural).unwrap()["access_path"],
            "collection_scan"
        );

        q.hint = Some(json!("missing-index"));
        assert!(c.find(&q).is_err());

        natural.sort = vec![("$natural".to_string(), 1), ("y".to_string(), 1)];
        assert!(c.find(&natural).is_err());
        natural.sort = vec![("$natural".to_string(), -1)];
        natural.hint = None;
        assert_eq!(ids(c.find(&natural).unwrap()), vec!["c", "b", "a"]);

        // A plain index includes a missing field as null, so a hinted residual
        // scan still visits documents without that field.
        c.insert_one(json!({"_id": "d", "y": "d"})).unwrap();
        q.filter = json!({"y": "d"});
        q.hint = Some(json!({"x": 1}));
        assert_eq!(ids(c.find(&q).unwrap()), vec!["d"]);
    }

    #[test]
    fn count_and_aggregate_max_time_ms() {
        let (_dir, store) = store();
        let c = store.collection("bounded").unwrap();
        for i in 0..3 {
            c.insert_one(json!({"_id": format!("id{i}"), "g": i % 2}))
                .unwrap();
        }

        assert_eq!(
            c.count_with_options(&json!({"g": 1}), Some(5_000)).unwrap(),
            1
        );
        let grouped = c
            .aggregate_with_options(
                &[json!({"$group": {"_id": "$g", "n": {"$sum": 1}}})],
                Some(5_000),
            )
            .unwrap();
        assert_eq!(grouped.len(), 2);

        let rtxn = store.read_txn().unwrap();
        assert!(matches!(
            c.count_in_deadline(&rtxn, &json!({"g": 1}), Some(Instant::now())),
            Err(Error::Timeout(_))
        ));
        assert!(matches!(
            c.aggregate_in_deadline(
                &rtxn,
                &[json!({"$group": {"_id": "$g", "n": {"$sum": 1}}})],
                Some(Instant::now())
            ),
            Err(Error::Timeout(_))
        ));
        rtxn.commit().unwrap();
    }

    #[test]
    fn find_raw_uses_stored_json_for_plain_finds() {
        let (_dir, store) = store();
        let c = store.collection("raw_find").unwrap();
        c.insert_one(json!({"_id": "a", "name": "Ala", "age": 30}))
            .unwrap();
        c.insert_one(json!({"_id": "b", "name": "Bob", "age": 20}))
            .unwrap();
        c.insert_one(json!({"_id": "c", "name": "Cid", "age": 25}))
            .unwrap();

        let raw = c
            .find_raw(&FindQuery {
                skip: 1,
                limit: Some(1),
                ..Default::default()
            })
            .unwrap()
            .unwrap();
        assert_eq!(raw.len(), 1);
        assert_eq!(
            serde_json::from_slice::<Value>(&raw[0]).unwrap()["_id"],
            "b"
        );

        let filtered = c
            .find_raw(&FindQuery {
                filter: json!({"name": "Cid"}),
                ..Default::default()
            })
            .unwrap()
            .unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(
            serde_json::from_slice::<Value>(&filtered[0]).unwrap()["age"],
            25
        );

        assert!(c
            .find_raw(&FindQuery {
                projection: Some(json!({"name": 1})),
                ..Default::default()
            })
            .unwrap()
            .is_none());
        assert!(c
            .find_raw(&FindQuery {
                sort: vec![("age".to_string(), 1)],
                ..Default::default()
            })
            .unwrap()
            .is_none());
    }
    fn expire_document(c: &Collection<'_>, store: &Store, id: &str) {
        let mut wtxn = store.write_txn().unwrap();
        c.set_ttl(&mut wtxn, id, Some(ttl::now_ms().saturating_sub(1)))
            .unwrap();
        wtxn.commit().unwrap();
    }

    #[test]
    fn strict_ttl_reads_hide_expired_documents() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        c.insert_one(json!({"_id": "live", "name": "live", "embedding": [0.9, 0.1]}))
            .unwrap();
        c.insert_one(json!({"_id": "expired", "name": "expired", "embedding": [1.0, 0.0]}))
            .unwrap();
        c.create_text_index("name").unwrap();
        c.create_fuzzy_index("name").unwrap();
        c.create_vector_index("embedding", 2, VectorMetric::Cosine)
            .unwrap();
        expire_document(&c, &store, "expired");

        assert_eq!(c.count(&json!({})).unwrap(), 1);
        assert!(c.find_one(&json!({"_id": "expired"})).unwrap().is_none());
        assert_eq!(
            c.find(&FindQuery {
                filter: json!({}),
                ..Default::default()
            })
            .unwrap()
            .len(),
            1
        );
        assert_eq!(
            c.lookup("_id", &[json!("expired"), json!("live")], None)
                .unwrap(),
            vec![
                None,
                Some(json!({"_id": "live", "name": "live", "embedding": [0.9, 0.1]}))
            ]
        );
        assert_eq!(
            c.search("name", "live", &json!({}), 0, Some(10))
                .unwrap()
                .len(),
            1
        );
        assert!(c
            .fuzzy_search("name", "expired", 0, &json!({}), Some(10))
            .unwrap()
            .is_empty());
        assert_eq!(
            c.vector_search("embedding", &[1.0, 0.0], &json!({}), 0, Some(1))
                .unwrap()[0]["_id"],
            "live"
        );
        assert_eq!(
            c.aggregate(&[json!({"$group": {"_id": null, "n": {"$sum": 1}}})])
                .unwrap()[0]["n"],
            1
        );
    }

    #[test]
    fn strict_ttl_boundary_update_and_clear() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        let id = c
            .insert_one(json!({"_id": "doc", "n": 1, "_ttl": 60}))
            .unwrap();
        let rtxn = store.read_txn().unwrap();
        let at = c.ttl_at(&rtxn, &id).unwrap().unwrap();
        assert!(at > ttl::now_ms());
        assert!(!c.ttl_expired_in(&rtxn, &id, at - 1).unwrap());
        assert!(c.ttl_expired_in(&rtxn, &id, at).unwrap());
        rtxn.commit().unwrap();

        c.update(&json!({"_id": id}), &json!({"$set": {"n": 2}}), false)
            .unwrap();
        let rtxn = store.read_txn().unwrap();
        assert_eq!(c.ttl_at(&rtxn, &id).unwrap(), Some(at));
        rtxn.commit().unwrap();

        c.update(&json!({"_id": id}), &json!({"$set": {"_ttl": 0}}), false)
            .unwrap();
        assert!(c.ttl_at(&store.read_txn().unwrap(), &id).unwrap().is_none());
        assert!(!c
            .ttl_expired_in(&store.read_txn().unwrap(), &id, u64::MAX)
            .unwrap());
    }
}

#[cfg(test)]
mod parse_json_tests {
    use super::*;

    #[test]
    fn sonic_matches_serde_for_edge_values() {
        let samples = [
            r#"{"a":1,"b":-2.5,"c":true,"d":null,"e":"x\\ny","u":"zażółć"}"#,
            r#"{"n":18446744073709551615,"i":-9223372036854775808}"#,
            r#"[1,2,3,{"x":"\\u00e9"}]"#,
            r#"0.1"#,
            r#""tekst""#,
        ];
        for raw in samples {
            let fast = parse_json(raw.as_bytes()).unwrap();
            let slow: Value = serde_json::from_slice(raw.as_bytes()).unwrap();
            assert_eq!(fast, slow, "raw={raw}");
        }
    }
}
