//! Simple daemon metrics (atomic counters, no external dependencies).

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
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
/// SQL statement counters and total execution time.
pub static SQL_SELECT: AtomicU64 = AtomicU64::new(0);
pub static SQL_INSERT: AtomicU64 = AtomicU64::new(0);
pub static SQL_UPDATE: AtomicU64 = AtomicU64::new(0);
pub static SQL_DELETE: AtomicU64 = AtomicU64::new(0);
pub static SQL_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static SQL_DURATION_NS: AtomicU64 = AtomicU64::new(0);
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
        "collection_metrics": collection_metrics,
        "collection_metrics_overflow_total": COLLECTION_METRICS_OVERFLOW.load(Relaxed),
        "commit_ns_total": commit_ns,
        "ttl_sweeps_total": TTL_SWEEPS.load(Relaxed),
        "ttl_expired_total": TTL_EXPIRED.load(Relaxed),
        "ttl_sweep_errors_total": TTL_SWEEP_ERRORS.load(Relaxed),
        "ttl_backlog": TTL_BACKLOG.load(Relaxed),
        "snapshots": snapshots_json(),
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
            "nonodb_ttl_backlog",
            "ttl_backlog",
            "Due TTL entries observed by the last sweep.",
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
