//! Simple daemon metrics (atomic counters, no external dependencies).

use std::collections::{BTreeMap, BTreeSet};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

pub static REQUESTS: AtomicU64 = AtomicU64::new(0);
pub static ERRORS: AtomicU64 = AtomicU64::new(0);
pub static READ_OPS: AtomicU64 = AtomicU64::new(0);
pub static CACHE_HITS: AtomicU64 = AtomicU64::new(0);
pub static CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
/// Automatic indexes: candidates, active indexes, and maintenance operations.
pub static AUTO_INDEX_CANDIDATES: AtomicU64 = AtomicU64::new(0);
pub static AUTO_INDEX_ACTIVE: AtomicU64 = AtomicU64::new(0);
pub static AUTO_INDEX_CREATED: AtomicU64 = AtomicU64::new(0);
pub static AUTO_INDEX_DROPPED: AtomicU64 = AtomicU64::new(0);
pub static AUTO_INDEX_BUILD_NS: AtomicU64 = AtomicU64::new(0);
pub static AUTO_INDEX_BUILD_ERRORS: AtomicU64 = AtomicU64::new(0);
/// Outbox watch/stream counters.
pub static WATCH_STREAMS: AtomicU64 = AtomicU64::new(0);
pub static WATCH_ACTIVE: AtomicU64 = AtomicU64::new(0);
pub static WATCH_EVENTS: AtomicU64 = AtomicU64::new(0);
pub static WATCH_TIMEOUTS: AtomicU64 = AtomicU64::new(0);
pub static WATCH_TRUNCATED: AtomicU64 = AtomicU64::new(0);
pub static WATCH_HEARTBEATS: AtomicU64 = AtomicU64::new(0);
pub static WATCH_THRESHOLD_EMITTED: AtomicU64 = AtomicU64::new(0);
pub static WATCH_THRESHOLD_SUPPRESSED: AtomicU64 = AtomicU64::new(0);
/// Stream consumer-group counters.
pub static STREAM_CLAIMS: AtomicU64 = AtomicU64::new(0);
pub static STREAM_REDELIVERIES: AtomicU64 = AtomicU64::new(0);
pub static STREAM_NACKS: AtomicU64 = AtomicU64::new(0);
pub static STREAM_DEAD_LETTERS: AtomicU64 = AtomicU64::new(0);
pub static STREAM_TRUNCATED: AtomicU64 = AtomicU64::new(0);
pub static STREAM_LEASE_RECOVERIES: AtomicU64 = AtomicU64::new(0);
/// SQL statement counters and total execution time.
pub static SQL_SELECT: AtomicU64 = AtomicU64::new(0);
pub static SQL_INSERT: AtomicU64 = AtomicU64::new(0);
pub static SQL_UPDATE: AtomicU64 = AtomicU64::new(0);
pub static SQL_DELETE: AtomicU64 = AtomicU64::new(0);
pub static SQL_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static SQL_DURATION_NS: AtomicU64 = AtomicU64::new(0);
/// Aggregate ordered point lookup counters.
pub static LOOKUP_REQUESTS: AtomicU64 = AtomicU64::new(0);
pub static LOOKUP_EXPLAINS: AtomicU64 = AtomicU64::new(0);
pub static LOOKUP_KEYS: AtomicU64 = AtomicU64::new(0);
pub static LOOKUP_UNIQUE_KEYS: AtomicU64 = AtomicU64::new(0);
pub static LOOKUP_FOUND: AtomicU64 = AtomicU64::new(0);
pub static LOOKUP_MISSING: AtomicU64 = AtomicU64::new(0);
pub static LOOKUP_DOCUMENTS_READ: AtomicU64 = AtomicU64::new(0);
pub static LOOKUP_INDEX_ENTRIES: AtomicU64 = AtomicU64::new(0);
pub static LOOKUP_BYTES_READ: AtomicU64 = AtomicU64::new(0);
pub static LOOKUP_BYTES_RETURNED: AtomicU64 = AtomicU64::new(0);
pub static LOOKUP_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static LOOKUP_DURATION_NS: AtomicU64 = AtomicU64::new(0);
/// Number of operations executed in `/v1/batch` requests.
pub static BATCH_OPS: AtomicU64 = AtomicU64::new(0);
pub static WRITE_OPS: AtomicU64 = AtomicU64::new(0);
pub static WRITE_BATCHES: AtomicU64 = AtomicU64::new(0);
/// Total `commit` time (including `fsync`) in nanoseconds.
pub static COMMIT_NS: AtomicU64 = AtomicU64::new(0);
/// TTL sweeps, expired documents, errors, and the last due backlog.
pub static TTL_SWEEPS: AtomicU64 = AtomicU64::new(0);
pub static TTL_EXPIRED: AtomicU64 = AtomicU64::new(0);
pub static TTL_SWEEP_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static TTL_BACKLOG: AtomicU64 = AtomicU64::new(0);
/// Approximate length of the write queue waiting for the writer.
pub static QUEUE_LEN: AtomicU64 = AtomicU64::new(0);
/// Peak queue length since process start.
pub static QUEUE_PEAK: AtomicU64 = AtomicU64::new(0);
/// 1 while the writer is executing a batch, 0 while idle.
pub static WRITER_BUSY: AtomicU64 = AtomicU64::new(0);
/// Current adaptive group-commit window in microseconds.
pub static WRITER_WINDOW_US: AtomicU64 = AtomicU64::new(0);
/// Window-selection counters and the last batch size.
pub static WRITER_IDLE_WINDOWS: AtomicU64 = AtomicU64::new(0);
pub static WRITER_BASE_WINDOWS: AtomicU64 = AtomicU64::new(0);
pub static WRITER_PRESSURE_WINDOWS: AtomicU64 = AtomicU64::new(0);
pub static WRITER_MAX_WINDOWS: AtomicU64 = AtomicU64::new(0);
pub static WRITER_LAST_BATCH_SIZE: AtomicU64 = AtomicU64::new(0);
pub static WRITER_WINDOW_NS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Authentication failures, lockouts, and rejected requests from blocked peers.
pub static AUTH_FAILURES: AtomicU64 = AtomicU64::new(0);
pub static AUTH_BLOCKS: AtomicU64 = AtomicU64::new(0);
pub static AUTH_BLOCKED_REQUESTS: AtomicU64 = AtomicU64::new(0);
pub static AUTH_BLOCKED_CLIENTS: AtomicU64 = AtomicU64::new(0);

pub static REPLICATION_TRANSPORT_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static REPLICATION_APPLY_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static REPLICATION_APPLIED_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static REPLICATION_LAST_SUCCESS_UNIX: AtomicU64 = AtomicU64::new(0);
pub static REPLICATION_RETENTION_RUNS: AtomicU64 = AtomicU64::new(0);
pub static REPLICATION_PRUNED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Time-series storage gauges and maintenance counters.
pub static TS_COLLECTIONS: AtomicU64 = AtomicU64::new(0);
pub static TS_ROWS: AtomicU64 = AtomicU64::new(0);
pub static TS_DELETED_ROWS: AtomicU64 = AtomicU64::new(0);
pub static TS_BUCKETS: AtomicU64 = AtomicU64::new(0);
pub static TS_ENCODED_BYTES: AtomicU64 = AtomicU64::new(0);
pub static TS_RAW_BYTES: AtomicU64 = AtomicU64::new(0);
pub static TS_RETENTION_RUNS: AtomicU64 = AtomicU64::new(0);
pub static TS_RETENTION_DROPPED_BUCKETS: AtomicU64 = AtomicU64::new(0);
pub static TS_RETENTION_BOUNDARY_BUCKETS: AtomicU64 = AtomicU64::new(0);
pub static TS_RETENTION_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static TS_COMPACTION_REQUESTS: AtomicU64 = AtomicU64::new(0);
pub static TS_COMPACTIONS: AtomicU64 = AtomicU64::new(0);
pub static TS_COMPACTED_BYTES: AtomicU64 = AtomicU64::new(0);
/// Client-certificate revocation refresh and decisions.
pub static TLS_REVOCATION_REFRESH_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static TLS_REVOCATION_REFRESH_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static TLS_REVOCATION_REVOKED_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static TLS_REVOCATION_UNAVAILABLE_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Upper bounds for request latency histograms in nanoseconds. The matching
/// Prometheus labels are expressed in seconds.
const LATENCY_BUCKETS_NS: [u64; 17] = [
    100_000,
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
    2_500_000_000,
    5_000_000_000,
    10_000_000_000,
    30_000_000_000,
    60_000_000_000,
];
const LATENCY_BUCKET_LABELS: [&str; 17] = [
    "0.0001", "0.0005", "0.001", "0.0025", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5",
    "1", "2.5", "5", "10", "30", "60",
];
const MAX_LATENCY_SERIES: usize = 128;
pub static LATENCY_SERIES_OVERFLOW: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Default)]
struct LatencyCounters {
    buckets: [u64; LATENCY_BUCKETS_NS.len()],
    count: u64,
    sum_ns: u64,
}

fn latency_map() -> &'static Mutex<BTreeMap<String, LatencyCounters>> {
    static MAP: OnceLock<Mutex<BTreeMap<String, LatencyCounters>>> = OnceLock::new();
    MAP.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// Records one end-to-end request latency for an operation name.
pub fn record_latency(operation: &str, elapsed: Duration) {
    if operation.is_empty() {
        return;
    }
    let mut map = latency_map()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if !map.contains_key(operation) && map.len() >= MAX_LATENCY_SERIES {
        inc(&LATENCY_SERIES_OVERFLOW);
        return;
    }
    let entry = map.entry(operation.to_string()).or_default();
    let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
    entry.count = entry.count.saturating_add(1);
    entry.sum_ns = entry.sum_ns.saturating_add(ns);
    for (index, upper) in LATENCY_BUCKETS_NS.iter().enumerate() {
        if ns <= *upper {
            entry.buckets[index] = entry.buckets[index].saturating_add(1);
            break;
        }
    }
}

fn latency_json() -> Vec<Value> {
    let map = latency_map()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    map.iter()
        .map(|(operation, counters)| {
            json!({
                "operation": operation,
                "buckets_ns": LATENCY_BUCKETS_NS,
                "buckets": counters.buckets,
                "count": counters.count,
                "sum_ns": counters.sum_ns,
            })
        })
        .collect()
}

fn lookup_latency_map() -> &'static Mutex<BTreeMap<String, LatencyCounters>> {
    static MAP: OnceLock<Mutex<BTreeMap<String, LatencyCounters>>> = OnceLock::new();
    MAP.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn lookup_latency_json() -> Vec<Value> {
    let map = lookup_latency_map()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    map.iter()
        .map(|(operation, counters)| {
            json!({
                "operation": operation,
                "buckets_ns": LATENCY_BUCKETS_NS,
                "buckets": counters.buckets,
                "count": counters.count,
                "sum_ns": counters.sum_ns,
            })
        })
        .collect()
}

/// Records one lookup and its aggregate counters.
pub fn record_lookup(
    elapsed: Duration,
    stats: &nonodb_core::LookupStats,
    bytes_returned: u64,
    ok: bool,
    explain: bool,
) {
    if explain {
        inc(&LOOKUP_EXPLAINS);
    } else {
        inc(&LOOKUP_REQUESTS);
    }
    add(&LOOKUP_KEYS, stats.requested_keys as u64);
    add(&LOOKUP_UNIQUE_KEYS, stats.unique_keys as u64);
    add(&LOOKUP_FOUND, stats.found as u64);
    add(&LOOKUP_MISSING, stats.missing as u64);
    add(&LOOKUP_DOCUMENTS_READ, stats.documents_read);
    add(&LOOKUP_INDEX_ENTRIES, stats.index_entries_read);
    add(&LOOKUP_BYTES_READ, stats.bytes_read);
    add(&LOOKUP_BYTES_RETURNED, bytes_returned);
    add(
        &LOOKUP_DURATION_NS,
        u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
    );
    if !ok {
        inc(&LOOKUP_ERRORS);
    }
    let operation = if explain { "explain_lookup" } else { "lookup" };
    let mut map = lookup_latency_map()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let entry = map.entry(operation.to_string()).or_default();
    let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
    entry.count = entry.count.saturating_add(1);
    entry.sum_ns = entry.sum_ns.saturating_add(ns);
    for (index, upper) in LATENCY_BUCKETS_NS.iter().enumerate() {
        if ns <= *upper {
            entry.buckets[index] = entry.buckets[index].saturating_add(1);
            break;
        }
    }
}

#[derive(Clone, Debug, Default)]
struct SnapshotCounters {
    success_total: u64,
    failures_total: u64,
    last_success_unix: u64,
    last_failure_unix: u64,
    last_duration_ms: u64,
}

fn snapshot_map() -> &'static Mutex<BTreeMap<String, SnapshotCounters>> {
    static MAP: OnceLock<Mutex<BTreeMap<String, SnapshotCounters>>> = OnceLock::new();
    MAP.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn certificate_map() -> &'static Mutex<BTreeMap<String, i64>> {
    static MAP: OnceLock<Mutex<BTreeMap<String, i64>>> = OnceLock::new();
    MAP.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// Records the leaf certificate expiry for one TLS role. The value is a Unix
/// timestamp in seconds so Prometheus can compare it directly with `time()`.
pub fn set_certificate_expiry(kind: &str, expires_at_unix: i64) {
    let mut map = certificate_map()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    map.insert(kind.to_string(), expires_at_unix);
}

/// Reads the first certificate from a PEM file and records its `notAfter`.
pub fn record_certificate_expiry(kind: &str, path: &Path) -> Result<(), String> {
    let file = std::fs::File::open(path)
        .map_err(|error| format!("open certificate {}: {error}", path.display()))?;
    let mut reader = BufReader::new(file);
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("parse certificate {}: {error}", path.display()))?;
    let leaf = certificates
        .first()
        .ok_or_else(|| format!("no certificate in {}", path.display()))?;
    use x509_parser::prelude::FromDer;
    let (_, certificate) = x509_parser::certificate::X509Certificate::from_der(leaf.as_ref())
        .map_err(|error| format!("decode certificate {}: {error}", path.display()))?;
    set_certificate_expiry(kind, certificate.validity().not_after.timestamp());
    Ok(())
}

fn certificates_json() -> Vec<Value> {
    let map = certificate_map()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    map.iter()
        .map(|(kind, expires_at_unix)| {
            json!({
                "kind": kind,
                "expires_at_unix": expires_at_unix,
            })
        })
        .collect()
}

/// Records a successful consistent snapshot for one database.
pub fn record_snapshot_success(database: &str, elapsed: Duration) {
    let mut map = snapshot_map()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let entry = map.entry(database.to_string()).or_default();
    entry.success_total = entry.success_total.saturating_add(1);
    entry.last_success_unix = unix_now();
    entry.last_duration_ms = elapsed.as_millis() as u64;
}

/// Records a failed snapshot attempt and keeps the previous success time so
/// alerting can show how stale the last usable backup is.
pub fn record_snapshot_failure(database: &str) {
    let mut map = snapshot_map()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let entry = map.entry(database.to_string()).or_default();
    entry.failures_total = entry.failures_total.saturating_add(1);
    entry.last_failure_unix = unix_now();
}

#[derive(Clone, Debug, Default)]
struct WalCounters {
    bytes: u64,
    oldest_unix: u64,
    last_seq: u64,
    full: bool,
    available: bool,
    pruned_bytes_total: u64,
}

fn wal_map() -> &'static Mutex<BTreeMap<String, WalCounters>> {
    static MAP: OnceLock<Mutex<BTreeMap<String, WalCounters>>> = OnceLock::new();
    MAP.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// Publishes the current PITR WAL state for one database.
pub fn record_wal(
    database: &str,
    bytes: u64,
    oldest_unix: u64,
    last_seq: u64,
    full: bool,
    available: bool,
) {
    let mut map = wal_map()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let entry = map.entry(database.to_string()).or_default();
    entry.bytes = bytes;
    entry.oldest_unix = oldest_unix;
    entry.last_seq = last_seq;
    entry.full = full;
    entry.available = available;
}

/// Adds bytes removed by WAL retention to the per-database counter.
pub fn record_wal_pruned(database: &str, bytes: u64) {
    let mut map = wal_map()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let entry = map.entry(database.to_string()).or_default();
    entry.pruned_bytes_total = entry.pruned_bytes_total.saturating_add(bytes);
}

fn wal_json() -> Vec<Value> {
    let now = unix_now();
    let map = wal_map()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    map.iter()
        .map(|(database, counters)| {
            json!({
                "database": database,
                "bytes": counters.bytes,
                "oldest_unix": counters.oldest_unix,
                "oldest_age_seconds": if counters.oldest_unix == 0 {
                    Value::Null
                } else {
                    json!(now.saturating_sub(counters.oldest_unix))
                },
                "last_seq": counters.last_seq,
                "full": u64::from(counters.full),
                "pitr_available": u64::from(counters.available),
                "pruned_bytes_total": counters.pruned_bytes_total,
            })
        })
        .collect()
}

fn snapshots_json() -> Vec<Value> {
    let now = unix_now();
    let map = snapshot_map()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    map.iter()
        .map(|(database, counters)| {
            json!({
                "database": database,
                "success_total": counters.success_total,
                "failures_total": counters.failures_total,
                "last_success_unix": counters.last_success_unix,
                "last_failure_unix": counters.last_failure_unix,
                "last_duration_ms": counters.last_duration_ms,
                "age_seconds": if counters.last_success_unix == 0 {
                    Value::Null
                } else {
                    json!(now.saturating_sub(counters.last_success_unix))
                },
            })
        })
        .collect()
}

#[derive(Clone, Debug)]
struct DiskPaths {
    data: PathBuf,
    snapshots: Option<PathBuf>,
}

static DISK_PATHS: OnceLock<Mutex<Option<DiskPaths>>> = OnceLock::new();

/// Records the paths used for filesystem capacity metrics. Called once at
/// startup after the data root exists.
pub fn set_disk_paths(data: PathBuf, snapshots: Option<PathBuf>) {
    *DISK_PATHS
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(DiskPaths { data, snapshots });
}

fn disk_for(path: &std::path::Path) -> Option<Value> {
    let total = fs2::total_space(path).ok()?;
    let free = fs2::available_space(path).ok()?;
    let used = total.saturating_sub(free);
    Some(json!({
        "total_bytes": total,
        "free_bytes": free,
        "used_bytes": used,
        "used_ratio": if total == 0 { 0.0 } else { used as f64 / total as f64 },
    }))
}

fn disk_json() -> Value {
    let Some(paths) = DISK_PATHS
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
    else {
        return json!({});
    };
    let mut out = serde_json::Map::new();
    if let Some(data) = disk_for(&paths.data) {
        out.insert("data".into(), data);
    }
    if let Some(snapshot_root) = paths.snapshots.as_deref() {
        if let Some(snapshots) = disk_for(snapshot_root) {
            out.insert("snapshots".into(), snapshots);
        }
    }
    Value::Object(out)
}

/// Highest number of distinct collection series kept in memory. Requests for
/// additional names are counted in `COLLECTION_METRICS_OVERFLOW` instead of
/// growing the map without bound.
const MAX_COLLECTION_METRICS: usize = 4096;
pub static COLLECTION_METRICS_OVERFLOW: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Default)]
struct CollectionCounters {
    requests_total: u64,
    read_ops_total: u64,
    write_requests_total: u64,
    cache_hits_total: u64,
    cache_misses_total: u64,
    errors_total: u64,
    request_ns_total: u64,
    last_request_ns: u64,
    /// Twelve five-second buckets covering roughly the last minute.
    traffic_buckets: [u64; 12],
    traffic_bucket: u64,
}

fn advance_traffic(counters: &mut CollectionCounters, bucket: u64) {
    if counters.traffic_bucket == 0 {
        counters.traffic_bucket = bucket;
        return;
    }
    if bucket <= counters.traffic_bucket {
        return;
    }
    let delta = (bucket - counters.traffic_bucket).min(12) as usize;
    if delta >= 12 {
        counters.traffic_buckets = [0; 12];
    } else {
        counters.traffic_buckets.rotate_left(delta);
        for slot in &mut counters.traffic_buckets[12 - delta..] {
            *slot = 0;
        }
    }
    counters.traffic_bucket = bucket;
}

fn traffic_last_minute(counters: &CollectionCounters) -> u64 {
    counters.traffic_buckets.iter().sum()
}

type CollectionKey = (String, String);

fn collection_map() -> &'static Mutex<BTreeMap<CollectionKey, CollectionCounters>> {
    static MAP: OnceLock<Mutex<BTreeMap<CollectionKey, CollectionCounters>>> = OnceLock::new();
    MAP.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn update_collection<F>(database: &str, collection: &str, update: F)
where
    F: FnOnce(&mut CollectionCounters),
{
    if collection.is_empty() {
        return;
    }
    let mut map = collection_map()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let key = (database.to_string(), collection.to_string());
    if !map.contains_key(&key) && map.len() >= MAX_COLLECTION_METRICS {
        inc(&COLLECTION_METRICS_OVERFLOW);
        return;
    }
    update(map.entry(key).or_default());
}

/// Records the outcome of one request for every collection it touched.
/// Batch requests count once per collection, not once per nested operation.
pub fn record_collection_request(
    database: &str,
    collections: &[String],
    read_only: bool,
    ok: bool,
    elapsed: Duration,
) {
    let unique: BTreeSet<&str> = collections.iter().map(String::as_str).collect();
    let elapsed_ns = elapsed.as_nanos() as u64;
    let bucket = unix_now() / 5;
    for collection in unique {
        update_collection(database, collection, |counters| {
            counters.requests_total += 1;
            counters.request_ns_total = counters.request_ns_total.saturating_add(elapsed_ns);
            counters.last_request_ns = elapsed_ns;
            advance_traffic(counters, bucket);
            counters.traffic_buckets[11] = counters.traffic_buckets[11].saturating_add(1);
            if ok {
                if read_only {
                    counters.read_ops_total += 1;
                } else {
                    counters.write_requests_total += 1;
                }
            } else {
                counters.errors_total += 1;
            }
        });
    }
}

/// Records a daemon read-cache hit or miss for every collection in a request.
pub fn record_collection_cache(database: &str, collections: &[String], hit: bool) {
    let unique: BTreeSet<&str> = collections.iter().map(String::as_str).collect();
    for collection in unique {
        update_collection(database, collection, |counters| {
            if hit {
                counters.cache_hits_total += 1;
            } else {
                counters.cache_misses_total += 1;
            }
        });
    }
}

fn collection_json(database: &str, collection: &str, counters: &CollectionCounters) -> Value {
    let requests = counters.requests_total;
    let last_minute = traffic_last_minute(counters);
    json!({
        "database": database,
        "collection": collection,
        "requests_total": requests,
        "read_ops_total": counters.read_ops_total,
        "write_requests_total": counters.write_requests_total,
        "cache_hits_total": counters.cache_hits_total,
        "cache_misses_total": counters.cache_misses_total,
        "errors_total": counters.errors_total,
        "request_ns_total": counters.request_ns_total,
        "last_request_us": counters.last_request_ns as f64 / 1000.0,
        "avg_request_us": if requests == 0 {
            0.0
        } else {
            counters.request_ns_total as f64 / requests as f64 / 1000.0
        },
        "requests_last_minute": last_minute,
        "avg_requests_per_second": last_minute as f64 / 60.0,
    })
}

/// Rolling one-minute traffic for one collection: request count and average
/// requests per second. Returns zeroes for an unseen collection.
pub fn collection_traffic(database: &str, collection: &str) -> (u64, f64) {
    let map = collection_map()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let last_minute = map
        .get(&(database.to_string(), collection.to_string()))
        .map(traffic_last_minute)
        .unwrap_or(0);
    (last_minute, last_minute as f64 / 60.0)
}

/// Returns counters for one collection. Missing collections report zeroes so a
/// newly created collection is immediately usable in dashboards.
pub fn collection_snapshot(database: &str, collection: &str) -> Value {
    let map = collection_map()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let counters = map
        .get(&(database.to_string(), collection.to_string()))
        .cloned()
        .unwrap_or_default();
    collection_json(database, collection, &counters)
}

pub fn inc(counter: &AtomicU64) {
    counter.fetch_add(1, Relaxed);
}

pub fn add(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Relaxed);
}

pub fn set(counter: &AtomicU64, n: u64) {
    counter.store(n, Relaxed);
}

/// Records the current queue length as a potential peak.
pub fn note_queue() {
    let _ = QUEUE_PEAK.fetch_max(QUEUE_LEN.load(Relaxed), Relaxed);
}

/// Safe (non-negative) counter decrement.
pub fn sub(counter: &AtomicU64, n: u64) {
    let _ = counter.try_update(Relaxed, Relaxed, |v| Some(v.saturating_sub(n)));
}

pub fn record_sql(kind: &str, elapsed: std::time::Duration, ok: bool) {
    let counter = match kind {
        "select" => &SQL_SELECT,
        "insert" => &SQL_INSERT,
        "update" => &SQL_UPDATE,
        "delete" => &SQL_DELETE,
        _ => &SQL_SELECT,
    };
    inc(counter);
    add(&SQL_DURATION_NS, elapsed.as_nanos() as u64);
    if !ok {
        inc(&SQL_ERRORS);
    }
}

pub fn snapshot() -> Value {
    let collection_metrics: Vec<Value> = {
        let map = collection_map()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        map.iter()
            .map(|((database, collection), counters)| {
                collection_json(database, collection, counters)
            })
            .collect()
    };
    let batches = WRITE_BATCHES.load(Relaxed).max(1);
    let commit_ns = COMMIT_NS.load(Relaxed);
    let write_ops = WRITE_OPS.load(Relaxed);
    let idle_windows = WRITER_IDLE_WINDOWS.load(Relaxed);
    let total_windows = idle_windows
        + WRITER_BASE_WINDOWS.load(Relaxed)
        + WRITER_PRESSURE_WINDOWS.load(Relaxed)
        + WRITER_MAX_WINDOWS.load(Relaxed);
    let fast_path_ratio = if total_windows == 0 {
        0.0
    } else {
        idle_windows as f64 / total_windows as f64
    };
    let avg_window_us = if total_windows == 0 {
        0.0
    } else {
        WRITER_WINDOW_NS_TOTAL.load(Relaxed) as f64 / total_windows as f64 / 1000.0
    };
    let (event_emitted, event_suppressed) = nonodb_core::events::stats();
    let latency = latency_json();
    let lookup_latency = lookup_latency_json();
    json!({
        "requests_total": REQUESTS.load(Relaxed),
        "errors_total": ERRORS.load(Relaxed),
        "read_ops_total": READ_OPS.load(Relaxed),
        "cache_hits_total": CACHE_HITS.load(Relaxed),
        "cache_misses_total": CACHE_MISSES.load(Relaxed),
        "auto_index_candidates": AUTO_INDEX_CANDIDATES.load(Relaxed),
        "auto_index_active": AUTO_INDEX_ACTIVE.load(Relaxed),
        "auto_index_created_total": AUTO_INDEX_CREATED.load(Relaxed),
        "auto_index_dropped_total": AUTO_INDEX_DROPPED.load(Relaxed),
        "auto_index_build_ns_total": AUTO_INDEX_BUILD_NS.load(Relaxed),
        "auto_index_build_errors_total": AUTO_INDEX_BUILD_ERRORS.load(Relaxed),
        "watch_streams_total": WATCH_STREAMS.load(Relaxed),
        "watch_active": WATCH_ACTIVE.load(Relaxed),
        "watch_events_total": WATCH_EVENTS.load(Relaxed),
        "watch_timeouts_total": WATCH_TIMEOUTS.load(Relaxed),
        "watch_truncated_total": WATCH_TRUNCATED.load(Relaxed),
        "watch_heartbeats_total": WATCH_HEARTBEATS.load(Relaxed),
        "watch_threshold_emitted_total": WATCH_THRESHOLD_EMITTED.load(Relaxed),
        "watch_threshold_suppressed_total": WATCH_THRESHOLD_SUPPRESSED.load(Relaxed),
        "collection_events_emitted_total": event_emitted,
        "collection_events_suppressed_total": event_suppressed,
        "sql_select_total": SQL_SELECT.load(Relaxed),
        "sql_insert_total": SQL_INSERT.load(Relaxed),
        "sql_update_total": SQL_UPDATE.load(Relaxed),
        "sql_delete_total": SQL_DELETE.load(Relaxed),
        "sql_errors_total": SQL_ERRORS.load(Relaxed),
        "sql_duration_ns_total": SQL_DURATION_NS.load(Relaxed),
        "lookup_requests_total": LOOKUP_REQUESTS.load(Relaxed),
        "lookup_explains_total": LOOKUP_EXPLAINS.load(Relaxed),
        "lookup_keys_total": LOOKUP_KEYS.load(Relaxed),
        "lookup_unique_keys_total": LOOKUP_UNIQUE_KEYS.load(Relaxed),
        "lookup_found_total": LOOKUP_FOUND.load(Relaxed),
        "lookup_missing_total": LOOKUP_MISSING.load(Relaxed),
        "lookup_documents_read_total": LOOKUP_DOCUMENTS_READ.load(Relaxed),
        "lookup_index_entries_read_total": LOOKUP_INDEX_ENTRIES.load(Relaxed),
        "lookup_bytes_read_total": LOOKUP_BYTES_READ.load(Relaxed),
        "lookup_bytes_returned_total": LOOKUP_BYTES_RETURNED.load(Relaxed),
        "lookup_errors_total": LOOKUP_ERRORS.load(Relaxed),
        "lookup_duration_ns_total": LOOKUP_DURATION_NS.load(Relaxed),
        "batch_ops_total": BATCH_OPS.load(Relaxed),
        "write_ops_total": write_ops,
        "write_batches_total": WRITE_BATCHES.load(Relaxed),
        "queue_len": QUEUE_LEN.load(Relaxed),
        "queue_peak": QUEUE_PEAK.load(Relaxed),
        "writer_busy": WRITER_BUSY.load(Relaxed),
        "writer_window_us": WRITER_WINDOW_US.load(Relaxed),
        "writer_avg_window_us": avg_window_us,
        "writer_last_batch_size": WRITER_LAST_BATCH_SIZE.load(Relaxed),
        "writer_idle_windows_total": WRITER_IDLE_WINDOWS.load(Relaxed),
        "writer_base_windows_total": WRITER_BASE_WINDOWS.load(Relaxed),
        "writer_pressure_windows_total": WRITER_PRESSURE_WINDOWS.load(Relaxed),
        "writer_max_windows_total": WRITER_MAX_WINDOWS.load(Relaxed),
        "writer_fast_path_ratio": fast_path_ratio,
        "auth_failures_total": AUTH_FAILURES.load(Relaxed),
        "auth_blocks_total": AUTH_BLOCKS.load(Relaxed),
        "auth_blocked_requests_total": AUTH_BLOCKED_REQUESTS.load(Relaxed),
        "auth_blocked_clients": AUTH_BLOCKED_CLIENTS.load(Relaxed),
        "replication_transport_errors_total": REPLICATION_TRANSPORT_ERRORS.load(Relaxed),
        "replication_apply_errors_total": REPLICATION_APPLY_ERRORS.load(Relaxed),
        "replication_applied_total": REPLICATION_APPLIED_TOTAL.load(Relaxed),
        "replication_last_success_unix": REPLICATION_LAST_SUCCESS_UNIX.load(Relaxed),
        "replication_retention_runs_total": REPLICATION_RETENTION_RUNS.load(Relaxed),
        "replication_pruned_total": REPLICATION_PRUNED_TOTAL.load(Relaxed),
        "ts_collections": TS_COLLECTIONS.load(Relaxed),
        "ts_rows": TS_ROWS.load(Relaxed),
        "ts_deleted_rows": TS_DELETED_ROWS.load(Relaxed),
        "ts_buckets": TS_BUCKETS.load(Relaxed),
        "ts_encoded_bytes": TS_ENCODED_BYTES.load(Relaxed),
        "ts_raw_bytes": TS_RAW_BYTES.load(Relaxed),
        "ts_retention_runs_total": TS_RETENTION_RUNS.load(Relaxed),
        "ts_retention_dropped_buckets_total": TS_RETENTION_DROPPED_BUCKETS.load(Relaxed),
        "ts_retention_boundary_buckets_total": TS_RETENTION_BOUNDARY_BUCKETS.load(Relaxed),
        "ts_retention_errors_total": TS_RETENTION_ERRORS.load(Relaxed),
        "ts_compaction_requests_total": TS_COMPACTION_REQUESTS.load(Relaxed),
        "ts_compactions_total": TS_COMPACTIONS.load(Relaxed),
        "ts_compacted_bytes_total": TS_COMPACTED_BYTES.load(Relaxed),
        "tls_revocation_refresh_total": TLS_REVOCATION_REFRESH_TOTAL.load(Relaxed),
        "tls_revocation_refresh_errors_total": TLS_REVOCATION_REFRESH_ERRORS.load(Relaxed),
        "tls_revocation_revoked_total": TLS_REVOCATION_REVOKED_TOTAL.load(Relaxed),
        "tls_revocation_unavailable_total": TLS_REVOCATION_UNAVAILABLE_TOTAL.load(Relaxed),
        "collection_metrics": collection_metrics,
        "collection_metrics_overflow_total": COLLECTION_METRICS_OVERFLOW.load(Relaxed),
        "latency": latency,
        "lookup_latency": lookup_latency,
        "latency_series_overflow_total": LATENCY_SERIES_OVERFLOW.load(Relaxed),
        "commit_ns_total": commit_ns,
        "ttl_sweeps_total": TTL_SWEEPS.load(Relaxed),
        "ttl_expired_total": TTL_EXPIRED.load(Relaxed),
        "ttl_sweep_errors_total": TTL_SWEEP_ERRORS.load(Relaxed),
        "ttl_backlog": TTL_BACKLOG.load(Relaxed),
        "snapshots": snapshots_json(),
        "wal": wal_json(),
        "certificates": certificates_json(),
        "disk": disk_json(),
        "index_builds": nonodb_core::progress::snapshot(),
        "avg_batch_size": write_ops as f64 / batches as f64,
        "avg_commit_us": commit_ns as f64 / batches as f64 / 1000.0,
    })
}

fn prom_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

fn prom_number(value: &Value) -> String {
    if let Some(value) = value.as_u64() {
        value.to_string()
    } else if let Some(value) = value.as_i64() {
        value.to_string()
    } else if let Some(value) = value.as_f64() {
        if value.is_finite() {
            format!("{value}")
        } else {
            "0".to_string()
        }
    } else {
        "0".to_string()
    }
}

fn prom_header(out: &mut String, name: &str, kind: &str, help: &str) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(kind);
    out.push('\n');
}

fn prom_sample(out: &mut String, name: &str, labels: &[(&str, &str)], value: &Value) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (index, (key, value)) in labels.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push_str(key);
            out.push_str("=\"");
            out.push_str(&prom_escape(value));
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    out.push_str(&prom_number(value));
    out.push('\n');
}

/// Native Prometheus/OpenMetrics-compatible text exposition. The endpoint is
/// intentionally generated from the same snapshot as `/metrics` so the two
/// representations cannot drift.
/// Appends per-database replication metrics gathered by the HTTP handler.
#[allow(clippy::too_many_arguments)]
pub fn append_replication_metrics(
    out: &mut String,
    enabled: bool,
    origin: Option<&str>,
    role: &str,
    epoch: u64,
    heads: &[(String, u64)],
    applied: &[(String, String, u64, u64, u64)],
    acks: &[(String, String, String, u64, u64)],
) {
    prom_header(
        out,
        "nonodb_replication_enabled",
        "gauge",
        "Whether durable replication-log capture is enabled.",
    );
    prom_sample(
        out,
        "nonodb_replication_enabled",
        &[],
        &json!(u8::from(enabled)),
    );
    prom_header(
        out,
        "nonodb_replication_role",
        "gauge",
        "Configured node replication role.",
    );
    prom_sample(out, "nonodb_replication_role", &[("role", role)], &json!(1));
    prom_header(
        out,
        "nonodb_replication_epoch",
        "gauge",
        "Current persisted replication role epoch.",
    );
    prom_sample(out, "nonodb_replication_epoch", &[], &json!(epoch));
    if let Some(origin) = origin {
        prom_header(
            out,
            "nonodb_replication_origin_info",
            "gauge",
            "Configured replication origin node identity.",
        );
        prom_sample(
            out,
            "nonodb_replication_origin_info",
            &[("origin", origin)],
            &json!(1),
        );
    }
    prom_header(
        out,
        "nonodb_replication_head",
        "gauge",
        "Highest replication sequence allocated in the database.",
    );
    for (database, head) in heads {
        prom_sample(
            out,
            "nonodb_replication_head",
            &[("database", database)],
            &json!(head),
        );
    }
    prom_header(
        out,
        "nonodb_replication_applied",
        "gauge",
        "Highest source sequence applied on this replica.",
    );
    for (database, origin, seq, _, _) in applied {
        prom_sample(
            out,
            "nonodb_replication_applied",
            &[("database", database), ("origin", origin)],
            &json!(seq),
        );
    }
    prom_header(
        out,
        "nonodb_replication_applied_epoch",
        "gauge",
        "Highest source role epoch applied on this replica.",
    );
    for (database, origin, _, _, epoch) in applied {
        prom_sample(
            out,
            "nonodb_replication_applied_epoch",
            &[("database", database), ("origin", origin)],
            &json!(epoch),
        );
    }
    prom_header(
        out,
        "nonodb_replication_lag",
        "gauge",
        "Source log head minus the applied sequence.",
    );
    for (database, origin, seq, source_head, _) in applied {
        prom_sample(
            out,
            "nonodb_replication_lag",
            &[("database", database), ("origin", origin)],
            &json!(source_head.saturating_sub(*seq)),
        );
    }
    prom_header(
        out,
        "nonodb_replication_min_ack",
        "gauge",
        "Minimum acknowledged source sequence across replicas.",
    );
    prom_header(
        out,
        "nonodb_replication_ack_nodes",
        "gauge",
        "Number of replicas that acknowledged the source.",
    );
    let mut groups: std::collections::BTreeMap<(String, String), Vec<u64>> =
        std::collections::BTreeMap::new();
    for (database, origin, _node, seq, _at_ms) in acks {
        groups
            .entry((database.clone(), origin.clone()))
            .or_default()
            .push(*seq);
    }
    for ((database, origin), values) in &groups {
        let labels = [("database", database.as_str()), ("origin", origin.as_str())];
        prom_sample(
            out,
            "nonodb_replication_min_ack",
            &labels,
            &json!(values.iter().copied().min().unwrap_or(0)),
        );
        prom_sample(
            out,
            "nonodb_replication_ack_nodes",
            &labels,
            &json!(values.len()),
        );
    }
    prom_header(
        out,
        "nonodb_replication_ack_seq",
        "gauge",
        "Applied sequence acknowledged by each replica.",
    );
    for (database, origin, node, seq, _at_ms) in acks {
        prom_sample(
            out,
            "nonodb_replication_ack_seq",
            &[("database", database), ("origin", origin), ("node", node)],
            &json!(seq),
        );
    }
    prom_header(
        out,
        "nonodb_replication_ack_age_seconds",
        "gauge",
        "Age of the last acknowledgement from each replica.",
    );
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0);
    for (database, origin, node, _seq, at_ms) in acks {
        prom_sample(
            out,
            "nonodb_replication_ack_age_seconds",
            &[("database", database), ("origin", origin), ("node", node)],
            &json!(now_ms.saturating_sub(*at_ms) as f64 / 1000.0),
        );
    }
}

/// One `(database, stream, group, committed, head, lag, inflight, dead)` row.
pub type StreamMetricRow = (String, String, String, u64, u64, u64, u64, u64);

/// Appends per-group stream metrics gathered from every open database.
pub fn append_stream_metrics(out: &mut String, rows: &[StreamMetricRow]) {
    prom_header(
        out,
        "nonodb_stream_committed_seq",
        "gauge",
        "Committed offset of a consumer group.",
    );
    for (database, stream, group, committed, _head, _lag, _inflight, _dead) in rows {
        prom_sample(
            out,
            "nonodb_stream_committed_seq",
            &[
                ("database", database.as_str()),
                ("stream", stream.as_str()),
                ("group", group.as_str()),
            ],
            &json!(committed),
        );
    }
    prom_header(
        out,
        "nonodb_stream_lag_events",
        "gauge",
        "Events between a group offset and the stream head.",
    );
    for (database, stream, group, _committed, _head, lag, _inflight, _dead) in rows {
        prom_sample(
            out,
            "nonodb_stream_lag_events",
            &[
                ("database", database.as_str()),
                ("stream", stream.as_str()),
                ("group", group.as_str()),
            ],
            &json!(lag),
        );
    }
    prom_header(
        out,
        "nonodb_stream_inflight_events",
        "gauge",
        "Events leased to members but not yet committed.",
    );
    for (database, stream, group, _committed, _head, _lag, inflight, _dead) in rows {
        prom_sample(
            out,
            "nonodb_stream_inflight_events",
            &[
                ("database", database.as_str()),
                ("stream", stream.as_str()),
                ("group", group.as_str()),
            ],
            &json!(inflight),
        );
    }
    prom_header(
        out,
        "nonodb_stream_dead_events",
        "gauge",
        "Dead-lettered events of a consumer group.",
    );
    for (database, stream, group, _committed, _head, _lag, _inflight, dead) in rows {
        prom_sample(
            out,
            "nonodb_stream_dead_events",
            &[
                ("database", database.as_str()),
                ("stream", stream.as_str()),
                ("group", group.as_str()),
            ],
            &json!(dead),
        );
    }
}

pub fn prometheus() -> String {
    let snapshot = snapshot();
    let mut out = String::with_capacity(16 * 1024);

    const COUNTERS: &[(&str, &str, &str)] = &[
        (
            "nonodb_requests_total",
            "requests_total",
            "HTTP data and watch requests.",
        ),
        ("nonodb_errors_total", "errors_total", "Error responses."),
        (
            "nonodb_read_ops_total",
            "read_ops_total",
            "Read operations handled.",
        ),
        (
            "nonodb_cache_hits_total",
            "cache_hits_total",
            "Read-cache hits.",
        ),
        (
            "nonodb_cache_misses_total",
            "cache_misses_total",
            "Read-cache misses.",
        ),
        (
            "nonodb_auto_index_created_total",
            "auto_index_created_total",
            "Automatic indexes created.",
        ),
        (
            "nonodb_auto_index_dropped_total",
            "auto_index_dropped_total",
            "Automatic indexes dropped.",
        ),
        (
            "nonodb_auto_index_build_ns_total",
            "auto_index_build_ns_total",
            "Time spent building automatic indexes.",
        ),
        (
            "nonodb_auto_index_build_errors_total",
            "auto_index_build_errors_total",
            "Failed automatic index builds.",
        ),
        (
            "nonodb_watch_streams_total",
            "watch_streams_total",
            "Watch streams opened.",
        ),
        (
            "nonodb_watch_events_total",
            "watch_events_total",
            "Events delivered by watch.",
        ),
        (
            "nonodb_watch_timeouts_total",
            "watch_timeouts_total",
            "Watch long-polls that timed out.",
        ),
        (
            "nonodb_watch_truncated_total",
            "watch_truncated_total",
            "Watch requests with an expired cursor.",
        ),
        (
            "nonodb_stream_claims_total",
            "stream_claims_total",
            "Consumer-group batches claimed.",
        ),
        (
            "nonodb_stream_redeliveries_total",
            "stream_redeliveries_total",
            "Consumer-group batches delivered more than once.",
        ),
        (
            "nonodb_stream_nacks_total",
            "stream_nacks_total",
            "Consumer-group batches released with nack.",
        ),
        (
            "nonodb_stream_dead_letters_total",
            "stream_dead_letters_total",
            "Consumer-group batches dead-lettered.",
        ),
        (
            "nonodb_stream_truncated_total",
            "stream_truncated_total",
            "Stream events dropped by offset retention.",
        ),
        (
            "nonodb_stream_lease_recoveries_total",
            "stream_lease_recoveries_total",
            "Expired stream leases requeued or dead-lettered by the janitor.",
        ),
        (
            "nonodb_watch_heartbeats_total",
            "watch_heartbeats_total",
            "Watch keepalive frames.",
        ),
        (
            "nonodb_watch_threshold_emitted_total",
            "watch_threshold_emitted_total",
            "Threshold events emitted.",
        ),
        (
            "nonodb_watch_threshold_suppressed_total",
            "watch_threshold_suppressed_total",
            "Threshold events suppressed.",
        ),
        (
            "nonodb_collection_events_emitted_total",
            "collection_events_emitted_total",
            "Automatic collection events written.",
        ),
        (
            "nonodb_collection_events_suppressed_total",
            "collection_events_suppressed_total",
            "Automatic collection events suppressed.",
        ),
        (
            "nonodb_sql_select_total",
            "sql_select_total",
            "SQL SELECT statements.",
        ),
        (
            "nonodb_sql_insert_total",
            "sql_insert_total",
            "SQL INSERT statements.",
        ),
        (
            "nonodb_sql_update_total",
            "sql_update_total",
            "SQL UPDATE statements.",
        ),
        (
            "nonodb_sql_delete_total",
            "sql_delete_total",
            "SQL DELETE statements.",
        ),
        ("nonodb_sql_errors_total", "sql_errors_total", "SQL errors."),
        (
            "nonodb_sql_duration_ns_total",
            "sql_duration_ns_total",
            "Total SQL execution time.",
        ),
        (
            "nonodb_lookup_requests_total",
            "lookup_requests_total",
            "Ordered point lookup requests.",
        ),
        (
            "nonodb_lookup_explains_total",
            "lookup_explains_total",
            "Lookup plan explanations.",
        ),
        (
            "nonodb_lookup_keys_total",
            "lookup_keys_total",
            "Requested lookup keys including duplicates.",
        ),
        (
            "nonodb_lookup_unique_keys_total",
            "lookup_unique_keys_total",
            "Distinct lookup keys after deduplication.",
        ),
        (
            "nonodb_lookup_found_total",
            "lookup_found_total",
            "Lookup results that found a document.",
        ),
        (
            "nonodb_lookup_missing_total",
            "lookup_missing_total",
            "Lookup results with no document.",
        ),
        (
            "nonodb_lookup_documents_read_total",
            "lookup_documents_read_total",
            "Documents read from the main collection during lookups.",
        ),
        (
            "nonodb_lookup_index_entries_read_total",
            "lookup_index_entries_read_total",
            "Index entries examined during lookups.",
        ),
        (
            "nonodb_lookup_bytes_read_total",
            "lookup_bytes_read_total",
            "Stored document and index bytes read during lookups.",
        ),
        (
            "nonodb_lookup_bytes_returned_total",
            "lookup_bytes_returned_total",
            "Stored result bytes returned by lookups.",
        ),
        (
            "nonodb_lookup_errors_total",
            "lookup_errors_total",
            "Failed lookup requests.",
        ),
        (
            "nonodb_lookup_duration_ns_total",
            "lookup_duration_ns_total",
            "Total lookup execution time.",
        ),
        (
            "nonodb_batch_ops_total",
            "batch_ops_total",
            "Operations inside read batches.",
        ),
        (
            "nonodb_write_ops_total",
            "write_ops_total",
            "Write operations executed.",
        ),
        (
            "nonodb_write_batches_total",
            "write_batches_total",
            "Write transactions committed.",
        ),
        (
            "nonodb_writer_idle_windows_total",
            "writer_idle_windows_total",
            "Adaptive writer idle windows.",
        ),
        (
            "nonodb_writer_base_windows_total",
            "writer_base_windows_total",
            "Adaptive writer base windows.",
        ),
        (
            "nonodb_writer_pressure_windows_total",
            "writer_pressure_windows_total",
            "Adaptive writer pressure windows.",
        ),
        (
            "nonodb_writer_max_windows_total",
            "writer_max_windows_total",
            "Adaptive writer maximum windows.",
        ),
        (
            "nonodb_auth_failures_total",
            "auth_failures_total",
            "Failed authentication attempts.",
        ),
        (
            "nonodb_auth_blocks_total",
            "auth_blocks_total",
            "IP lockouts started.",
        ),
        (
            "nonodb_auth_blocked_requests_total",
            "auth_blocked_requests_total",
            "Requests rejected by IP lockout.",
        ),
        (
            "nonodb_collection_metrics_overflow_total",
            "collection_metrics_overflow_total",
            "Collection metric series rejected by the cap.",
        ),
        (
            "nonodb_latency_series_overflow_total",
            "latency_series_overflow_total",
            "Latency operation series rejected by the cap.",
        ),
        (
            "nonodb_commit_ns_total",
            "commit_ns_total",
            "Total commit time including fsync.",
        ),
        (
            "nonodb_ttl_sweeps_total",
            "ttl_sweeps_total",
            "TTL sweeps performed.",
        ),
        (
            "nonodb_ts_retention_runs_total",
            "ts_retention_runs_total",
            "Automatic time-series retention passes.",
        ),
        (
            "nonodb_ts_retention_dropped_buckets_total",
            "ts_retention_dropped_buckets_total",
            "Whole time-series buckets dropped by retention.",
        ),
        (
            "nonodb_ts_retention_boundary_buckets_total",
            "ts_retention_boundary_buckets_total",
            "Time-series boundary buckets marked by retention.",
        ),
        (
            "nonodb_ts_retention_errors_total",
            "ts_retention_errors_total",
            "Failed time-series retention passes.",
        ),
        (
            "nonodb_ts_compaction_requests_total",
            "ts_compaction_requests_total",
            "Offline time-series compaction requests written.",
        ),
        (
            "nonodb_ts_compactions_total",
            "ts_compactions_total",
            "Time-series compactions completed at startup.",
        ),
        (
            "nonodb_ts_compacted_bytes_total",
            "ts_compacted_bytes_total",
            "Bytes reclaimed by time-series compaction.",
        ),
        (
            "nonodb_ttl_expired_total",
            "ttl_expired_total",
            "Documents deleted by TTL.",
        ),
        (
            "nonodb_ttl_sweep_errors_total",
            "ttl_sweep_errors_total",
            "Failed TTL sweeps.",
        ),
    ];
    for (name, key, help) in COUNTERS {
        prom_header(&mut out, name, "counter", help);
        prom_sample(&mut out, name, &[], &snapshot[key]);
    }

    const GAUGES: &[(&str, &str, &str)] = &[
        (
            "nonodb_auto_index_candidates",
            "auto_index_candidates",
            "Automatic-index candidates above threshold.",
        ),
        (
            "nonodb_auto_index_active",
            "auto_index_active",
            "Automatic indexes currently tracked.",
        ),
        (
            "nonodb_watch_active",
            "watch_active",
            "Currently open watch streams.",
        ),
        ("nonodb_queue_len", "queue_len", "Writer queue length."),
        (
            "nonodb_queue_peak",
            "queue_peak",
            "Peak writer queue length since start.",
        ),
        (
            "nonodb_writer_busy",
            "writer_busy",
            "1 while the writer is executing a batch.",
        ),
        (
            "nonodb_writer_window_us",
            "writer_window_us",
            "Current adaptive writer window.",
        ),
        (
            "nonodb_writer_avg_window_us",
            "writer_avg_window_us",
            "Average adaptive writer window.",
        ),
        (
            "nonodb_writer_last_batch_size",
            "writer_last_batch_size",
            "Last writer batch size.",
        ),
        (
            "nonodb_writer_fast_path_ratio",
            "writer_fast_path_ratio",
            "Share of idle fast-path windows.",
        ),
        (
            "nonodb_auth_blocked_clients",
            "auth_blocked_clients",
            "Currently blocked client IPs.",
        ),
        (
            "nonodb_replication_transport_errors_total",
            "replication_transport_errors_total",
            "Replication pull transport or decode errors.",
        ),
        (
            "nonodb_replication_apply_errors_total",
            "replication_apply_errors_total",
            "Replication apply errors.",
        ),
        (
            "nonodb_replication_applied_total",
            "replication_applied_total",
            "Replication transactions applied successfully.",
        ),
        (
            "nonodb_replication_last_success_unix",
            "replication_last_success_unix",
            "Unix time of the last successful replication pull.",
        ),
        (
            "nonodb_replication_retention_runs_total",
            "replication_retention_runs_total",
            "Automatic retention passes with at least one replica ack.",
        ),
        (
            "nonodb_replication_pruned_total",
            "replication_pruned_total",
            "Replication log entries removed by automatic retention.",
        ),
        (
            "nonodb_tls_revocation_refresh_total",
            "tls_revocation_refresh_total",
            "CRL refresh passes.",
        ),
        (
            "nonodb_tls_revocation_refresh_errors_total",
            "tls_revocation_refresh_errors_total",
            "CRL refresh passes with at least one failed source.",
        ),
        (
            "nonodb_tls_revocation_revoked_total",
            "tls_revocation_revoked_total",
            "Client certificates rejected as revoked.",
        ),
        (
            "nonodb_tls_revocation_unavailable_total",
            "tls_revocation_unavailable_total",
            "Client connections whose revocation status could not be determined.",
        ),
        (
            "nonodb_ttl_backlog",
            "ttl_backlog",
            "Due TTL entries observed by the last sweep.",
        ),
        (
            "nonodb_ts_collections",
            "ts_collections",
            "Time-series collections observed by the last sweep.",
        ),
        (
            "nonodb_ts_rows",
            "ts_rows",
            "Live rows across time-series collections.",
        ),
        (
            "nonodb_ts_deleted_rows",
            "ts_deleted_rows",
            "Deleted time-series rows still occupying bucket payloads.",
        ),
        (
            "nonodb_ts_buckets",
            "ts_buckets",
            "Time-series buckets across all collections.",
        ),
        (
            "nonodb_ts_encoded_bytes",
            "ts_encoded_bytes",
            "Encoded time-series bucket bytes.",
        ),
        (
            "nonodb_ts_raw_bytes",
            "ts_raw_bytes",
            "Raw JSON bytes represented by time-series buckets.",
        ),
        (
            "nonodb_avg_batch_size",
            "avg_batch_size",
            "Write ops per committed write transaction.",
        ),
        (
            "nonodb_avg_commit_us",
            "avg_commit_us",
            "Average commit time in microseconds.",
        ),
    ];
    for (name, key, help) in GAUGES {
        prom_header(&mut out, name, "gauge", help);
        prom_sample(&mut out, name, &[], &snapshot[key]);
    }

    if let Some(rows) = snapshot["certificates"].as_array() {
        if !rows.is_empty() {
            prom_header(
                &mut out,
                "nonodb_tls_certificate_expiry_timestamp_seconds",
                "gauge",
                "Leaf TLS certificate expiry as a Unix timestamp.",
            );
            for row in rows {
                let kind = row["kind"].as_str().unwrap_or("unknown");
                prom_sample(
                    &mut out,
                    "nonodb_tls_certificate_expiry_timestamp_seconds",
                    &[("kind", kind)],
                    &row["expires_at_unix"],
                );
            }
        }
    }

    if let Some(rows) = snapshot["latency"].as_array() {
        prom_header(
            &mut out,
            "nonodb_request_duration_seconds",
            "histogram",
            "End-to-end request latency by operation.",
        );
        for row in rows {
            let operation = row["operation"].as_str().unwrap_or("");
            let labels = [("op", operation)];
            let mut cumulative = 0u64;
            if let Some(buckets) = row["buckets"].as_array() {
                for (index, label) in LATENCY_BUCKET_LABELS.iter().enumerate() {
                    cumulative = cumulative.saturating_add(
                        buckets
                            .get(index)
                            .and_then(Value::as_u64)
                            .unwrap_or_default(),
                    );
                    prom_sample(
                        &mut out,
                        "nonodb_request_duration_seconds_bucket",
                        &[("op", operation), ("le", label)],
                        &json!(cumulative),
                    );
                }
            }
            prom_sample(
                &mut out,
                "nonodb_request_duration_seconds_bucket",
                &[("op", operation), ("le", "+Inf")],
                &row["count"],
            );
            let sum_seconds = row["sum_ns"].as_u64().unwrap_or_default() as f64 / 1_000_000_000.0;
            prom_sample(
                &mut out,
                "nonodb_request_duration_seconds_sum",
                &labels,
                &json!(sum_seconds),
            );
            prom_sample(
                &mut out,
                "nonodb_request_duration_seconds_count",
                &labels,
                &row["count"],
            );
        }
    }

    if let Some(rows) = snapshot["lookup_latency"].as_array() {
        prom_header(
            &mut out,
            "nonodb_lookup_duration_seconds",
            "histogram",
            "Lookup latency by operation.",
        );
        for row in rows {
            let operation = row["operation"].as_str().unwrap_or("");
            let labels = [("op", operation)];
            let mut cumulative = 0u64;
            if let Some(buckets) = row["buckets"].as_array() {
                for (index, label) in LATENCY_BUCKET_LABELS.iter().enumerate() {
                    cumulative = cumulative.saturating_add(
                        buckets
                            .get(index)
                            .and_then(Value::as_u64)
                            .unwrap_or_default(),
                    );
                    prom_sample(
                        &mut out,
                        "nonodb_lookup_duration_seconds_bucket",
                        &[("op", operation), ("le", label)],
                        &json!(cumulative),
                    );
                }
            }
            prom_sample(
                &mut out,
                "nonodb_lookup_duration_seconds_bucket",
                &[("op", operation), ("le", "+Inf")],
                &row["count"],
            );
            let sum_seconds = row["sum_ns"].as_u64().unwrap_or_default() as f64 / 1_000_000_000.0;
            prom_sample(
                &mut out,
                "nonodb_lookup_duration_seconds_sum",
                &labels,
                &json!(sum_seconds),
            );
            prom_sample(
                &mut out,
                "nonodb_lookup_duration_seconds_count",
                &labels,
                &row["count"],
            );
        }
    }

    if let Some(rows) = snapshot["collection_metrics"].as_array() {
        const COLLECTION_METRICS: &[(&str, &str, &str)] = &[
            (
                "nonodb_collection_requests_total",
                "requests_total",
                "Requests that touched a collection.",
            ),
            (
                "nonodb_collection_read_ops_total",
                "read_ops_total",
                "Successful read requests for a collection.",
            ),
            (
                "nonodb_collection_write_requests_total",
                "write_requests_total",
                "Successful write requests for a collection.",
            ),
            (
                "nonodb_collection_cache_hits_total",
                "cache_hits_total",
                "Read-cache hits for a collection.",
            ),
            (
                "nonodb_collection_cache_misses_total",
                "cache_misses_total",
                "Read-cache misses for a collection.",
            ),
            (
                "nonodb_collection_errors_total",
                "errors_total",
                "Failed requests for a collection.",
            ),
            (
                "nonodb_collection_request_ns_total",
                "request_ns_total",
                "Total request time for a collection.",
            ),
            (
                "nonodb_collection_last_request_us",
                "last_request_us",
                "Last request duration for a collection.",
            ),
            (
                "nonodb_collection_avg_request_us",
                "avg_request_us",
                "Average request duration for a collection.",
            ),
            (
                "nonodb_collection_requests_last_minute",
                "requests_last_minute",
                "Requests in the rolling last minute.",
            ),
            (
                "nonodb_collection_avg_requests_per_second",
                "avg_requests_per_second",
                "Average requests per second over the last minute.",
            ),
        ];
        for (name, key, help) in COLLECTION_METRICS {
            let kind = if key.ends_with("_total") {
                "counter"
            } else {
                "gauge"
            };
            prom_header(&mut out, name, kind, help);
            for row in rows {
                let database = row["database"].as_str().unwrap_or("");
                let collection = row["collection"].as_str().unwrap_or("");
                prom_sample(
                    &mut out,
                    name,
                    &[("database", database), ("collection", collection)],
                    &row[*key],
                );
            }
        }
    }

    if let Some(builds) = snapshot["index_builds"].as_array() {
        prom_header(
            &mut out,
            "nonodb_index_build_active",
            "gauge",
            "1 while an index backfill is active.",
        );
        for build in builds {
            let labels = [
                ("collection", build["collection"].as_str().unwrap_or("")),
                ("target", build["target"].as_str().unwrap_or("")),
                ("kind", build["kind"].as_str().unwrap_or("")),
            ];
            let active = if build["finished"].as_bool().unwrap_or(false) {
                json!(0)
            } else {
                json!(1)
            };
            prom_sample(&mut out, "nonodb_index_build_active", &labels, &active);
        }
        prom_header(
            &mut out,
            "nonodb_index_build_processed",
            "gauge",
            "Documents processed by an index backfill.",
        );
        for build in builds {
            let labels = [
                ("collection", build["collection"].as_str().unwrap_or("")),
                ("target", build["target"].as_str().unwrap_or("")),
                ("kind", build["kind"].as_str().unwrap_or("")),
            ];
            prom_sample(
                &mut out,
                "nonodb_index_build_processed",
                &labels,
                &build["processed"],
            );
        }
        prom_header(
            &mut out,
            "nonodb_index_build_total",
            "gauge",
            "Documents planned for an index backfill.",
        );
        for build in builds {
            let labels = [
                ("collection", build["collection"].as_str().unwrap_or("")),
                ("target", build["target"].as_str().unwrap_or("")),
                ("kind", build["kind"].as_str().unwrap_or("")),
            ];
            prom_sample(
                &mut out,
                "nonodb_index_build_total",
                &labels,
                &build["total"],
            );
        }
        prom_header(
            &mut out,
            "nonodb_index_build_elapsed_ms",
            "gauge",
            "Elapsed index backfill time.",
        );
        for build in builds {
            let labels = [
                ("collection", build["collection"].as_str().unwrap_or("")),
                ("target", build["target"].as_str().unwrap_or("")),
                ("kind", build["kind"].as_str().unwrap_or("")),
            ];
            prom_sample(
                &mut out,
                "nonodb_index_build_elapsed_ms",
                &labels,
                &build["elapsed_ms"],
            );
        }
    }

    if let Some(disk) = snapshot["disk"].as_object() {
        const DISK_METRICS: &[(&str, &str, &str, &str)] = &[
            (
                "nonodb_disk_total_bytes",
                "total_bytes",
                "gauge",
                "Filesystem capacity.",
            ),
            (
                "nonodb_disk_free_bytes",
                "free_bytes",
                "gauge",
                "Filesystem free space.",
            ),
            (
                "nonodb_disk_used_bytes",
                "used_bytes",
                "gauge",
                "Filesystem used space.",
            ),
            (
                "nonodb_disk_used_ratio",
                "used_ratio",
                "gauge",
                "Filesystem used ratio.",
            ),
        ];
        for (name, key, kind, help) in DISK_METRICS {
            prom_header(&mut out, name, kind, help);
            for (mount, values) in disk {
                prom_sample(&mut out, name, &[("mount", mount)], &values[*key]);
            }
        }
    }

    if let Some(wal) = snapshot["wal"].as_array() {
        const WAL_METRICS: &[(&str, &str, &str, &str)] = &[
            (
                "nonodb_wal_bytes",
                "bytes",
                "gauge",
                "Bytes used by retained PITR WAL segments.",
            ),
            (
                "nonodb_wal_oldest_age_seconds",
                "oldest_age_seconds",
                "gauge",
                "Age of the oldest retained WAL segment.",
            ),
            (
                "nonodb_wal_last_sequence",
                "last_seq",
                "gauge",
                "Highest sequence archived in the WAL.",
            ),
            (
                "nonodb_wal_full",
                "full",
                "gauge",
                "1 when the WAL hard limit blocks writes.",
            ),
            (
                "nonodb_pitr_available",
                "pitr_available",
                "gauge",
                "1 when a snapshot plus WAL can serve a PITR restore.",
            ),
            (
                "nonodb_wal_pruned_bytes_total",
                "pruned_bytes_total",
                "counter",
                "WAL bytes removed by snapshot-covered retention.",
            ),
        ];
        for (name, key, kind, help) in WAL_METRICS {
            prom_header(&mut out, name, kind, help);
            for row in wal {
                if row[*key].is_null() {
                    continue;
                }
                prom_sample(
                    &mut out,
                    name,
                    &[("database", row["database"].as_str().unwrap_or(""))],
                    &row[*key],
                );
            }
        }
    }

    if let Some(snapshots) = snapshot["snapshots"].as_array() {
        const SNAPSHOT_METRICS: &[(&str, &str, &str, &str)] = &[
            (
                "nonodb_snapshot_success_total",
                "success_total",
                "counter",
                "Successful snapshots.",
            ),
            (
                "nonodb_snapshot_failures_total",
                "failures_total",
                "counter",
                "Failed snapshots.",
            ),
            (
                "nonodb_snapshot_last_success_timestamp_seconds",
                "last_success_unix",
                "gauge",
                "Unix time of the last successful snapshot.",
            ),
            (
                "nonodb_snapshot_last_failure_timestamp_seconds",
                "last_failure_unix",
                "gauge",
                "Unix time of the last failed snapshot.",
            ),
            (
                "nonodb_snapshot_age_seconds",
                "age_seconds",
                "gauge",
                "Age of the last successful snapshot.",
            ),
            (
                "nonodb_snapshot_last_duration_ms",
                "last_duration_ms",
                "gauge",
                "Duration of the last successful snapshot.",
            ),
        ];
        for (name, key, kind, help) in SNAPSHOT_METRICS {
            prom_header(&mut out, name, kind, help);
            for row in snapshots {
                if row[*key].is_null() {
                    continue;
                }
                prom_sample(
                    &mut out,
                    name,
                    &[("database", row["database"].as_str().unwrap_or(""))],
                    &row[*key],
                );
            }
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_metrics_are_exposed_in_snapshot_and_prometheus() {
        let database = format!("wal-metrics-{}", std::process::id());
        record_wal(
            &database,
            2048,
            unix_now().saturating_sub(30),
            7,
            true,
            false,
        );
        record_wal_pruned(&database, 512);
        let snapshot = snapshot();
        let row = snapshot["wal"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["database"] == database.as_str())
            .expect("wal row");
        assert_eq!(row["bytes"], 2048);
        assert_eq!(row["full"], 1);
        assert_eq!(row["pitr_available"], 0);
        assert_eq!(row["pruned_bytes_total"], 512);
        let text = prometheus();
        assert!(text.contains("nonodb_wal_bytes{database=\""));
        assert!(text.contains("nonodb_wal_full{database=\""));
        assert!(text.contains("nonodb_pitr_available{database=\""));
        assert!(text.contains("nonodb_wal_pruned_bytes_total{database=\""));
    }

    #[test]
    fn collection_metrics_are_isolated_and_include_cache_and_errors() {
        let database = format!("metrics-test-{}", std::process::id());
        let collection = "orders";

        record_collection_request(
            &database,
            &[collection.to_string()],
            true,
            true,
            Duration::from_micros(25),
        );
        record_collection_request(
            &database,
            &[collection.to_string()],
            false,
            true,
            Duration::from_micros(75),
        );
        record_collection_request(
            &database,
            &[collection.to_string()],
            true,
            false,
            Duration::from_micros(5),
        );
        record_collection_cache(&database, &[collection.to_string()], true);
        record_collection_cache(&database, &[collection.to_string()], false);

        let snapshot = collection_snapshot(&database, collection);
        assert_eq!(snapshot["requests_total"], 3);
        assert_eq!(snapshot["read_ops_total"], 1);
        assert_eq!(snapshot["write_requests_total"], 1);
        assert_eq!(snapshot["errors_total"], 1);
        assert_eq!(snapshot["cache_hits_total"], 1);
        assert_eq!(snapshot["cache_misses_total"], 1);
        assert_eq!(snapshot["avg_request_us"], 35.0);
        assert_eq!(snapshot["requests_last_minute"], 3);
        assert_eq!(snapshot["avg_requests_per_second"], 0.05);

        let other = collection_snapshot(&database, "missing");
        assert_eq!(other["requests_total"], 0);
        assert_eq!(other["avg_request_us"], 0.0);
    }

    #[test]
    fn prometheus_exposition_has_global_collection_and_snapshot_metrics() {
        let database = format!("prometheus-test-{}", std::process::id());
        let collection = "prom_orders";
        record_collection_request(
            &database,
            &[collection.to_string()],
            true,
            true,
            Duration::from_micros(50),
        );
        record_collection_cache(&database, &[collection.to_string()], true);
        record_snapshot_success(&database, Duration::from_millis(12));
        let text = prometheus();
        assert!(text.contains("# TYPE nonodb_requests_total counter"));
        assert!(text.contains("# TYPE nonodb_queue_len gauge"));
        assert!(text.contains("nonodb_collection_requests_total{database=\""));
        assert!(text.contains(&format!("collection=\"{collection}\"")));
        assert!(text.contains("nonodb_snapshot_success_total{database=\""));
        assert!(text.contains("nonodb_snapshot_age_seconds{database=\""));
    }

    #[test]
    fn certificate_expiry_is_exported_as_a_timestamp() {
        let kind = format!("test-{}", std::process::id());
        set_certificate_expiry(&kind, 1_900_000_000);
        let snapshot = snapshot();
        assert!(snapshot["certificates"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["kind"] == kind.as_str() && row["expires_at_unix"] == 1_900_000_000));
        let text = prometheus();
        assert!(text.contains("# TYPE nonodb_tls_certificate_expiry_timestamp_seconds gauge"));
        assert!(text.contains(&format!(
            "nonodb_tls_certificate_expiry_timestamp_seconds{{kind=\"{kind}\"}} 1900000000"
        )));
    }

    #[test]
    fn request_latency_histograms_are_bounded_and_exported() {
        let operation = format!("latency-test-{}", std::process::id());
        record_latency(&operation, Duration::from_millis(1));
        record_latency(&operation, Duration::from_millis(20));
        record_latency(&operation, Duration::from_secs(2));

        let snapshot = snapshot();
        let row = snapshot["latency"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["operation"] == operation)
            .unwrap();
        assert_eq!(row["count"], 3);
        assert_eq!(row["sum_ns"], 2_021_000_000u64);
        assert_eq!(row["buckets"][2], 1);
        assert_eq!(row["buckets"][6], 1);
        assert_eq!(row["buckets"][12], 1);

        let text = prometheus();
        assert!(text.contains("# TYPE nonodb_request_duration_seconds histogram"));
        assert!(text.contains(&format!(
            "nonodb_request_duration_seconds_bucket{{op=\"{operation}\",le=\"1\"}} 2"
        )));
        assert!(text.contains(&format!(
            "nonodb_request_duration_seconds_bucket{{op=\"{operation}\",le=\"+Inf\"}} 3"
        )));
        assert!(text.contains(&format!(
            "nonodb_request_duration_seconds_count{{op=\"{operation}\"}} 3"
        )));
    }

    #[test]
    fn lookup_metrics_and_histograms_are_exported() {
        let stats = nonodb_core::LookupStats {
            selected_index: "email".into(),
            index_fields: vec!["email".into()],
            index_include: vec!["name".into()],
            requested_keys: 3,
            unique_keys: 2,
            found: 2,
            missing: 1,
            index_entries_read: 2,
            documents_read: 0,
            bytes_read: 64,
            result_bytes: 32,
            covering: true,
            primary: false,
        };
        record_lookup(Duration::from_millis(2), &stats, 32, true, false);
        record_lookup(Duration::from_millis(3), &stats, 32, true, true);

        let snapshot = snapshot();
        assert!(snapshot["lookup_requests_total"].as_u64().unwrap() >= 1);
        assert!(snapshot["lookup_explains_total"].as_u64().unwrap() >= 1);
        assert!(snapshot["lookup_found_total"].as_u64().unwrap() >= 2);
        assert!(snapshot["lookup_latency"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["operation"] == "lookup"));
        assert!(snapshot["lookup_latency"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["operation"] == "explain_lookup"));
        let text = prometheus();
        assert!(text.contains("nonodb_lookup_requests_total"));
        assert!(text.contains("# TYPE nonodb_lookup_duration_seconds histogram"));
        assert!(text.contains("nonodb_lookup_duration_seconds_bucket{op=\"lookup\",le=\"+Inf\"}"));
    }

    #[test]
    fn duplicate_collection_names_in_a_batch_count_once() {
        let database = format!("metrics-batch-test-{}", std::process::id());
        let collection = "events";
        record_collection_request(
            &database,
            &[collection.to_string(), collection.to_string()],
            false,
            true,
            Duration::from_micros(10),
        );
        let snapshot = collection_snapshot(&database, collection);
        assert_eq!(snapshot["requests_total"], 1);
        assert_eq!(snapshot["write_requests_total"], 1);
    }
}
