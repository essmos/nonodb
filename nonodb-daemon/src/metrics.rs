//! Simple daemon metrics (atomic counters, no external dependencies).

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

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
/// Authentication failures, lockouts, and rejected requests from blocked peers.
pub static AUTH_FAILURES: AtomicU64 = AtomicU64::new(0);
pub static AUTH_BLOCKS: AtomicU64 = AtomicU64::new(0);
pub static AUTH_BLOCKED_REQUESTS: AtomicU64 = AtomicU64::new(0);
pub static AUTH_BLOCKED_CLIENTS: AtomicU64 = AtomicU64::new(0);

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
    let batches = WRITE_BATCHES.load(Relaxed).max(1);
    let commit_ns = COMMIT_NS.load(Relaxed);
    let write_ops = WRITE_OPS.load(Relaxed);
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
        "auth_failures_total": AUTH_FAILURES.load(Relaxed),
        "auth_blocks_total": AUTH_BLOCKS.load(Relaxed),
        "auth_blocked_requests_total": AUTH_BLOCKED_REQUESTS.load(Relaxed),
        "auth_blocked_clients": AUTH_BLOCKED_CLIENTS.load(Relaxed),
        "commit_ns_total": commit_ns,
        "ttl_sweeps_total": TTL_SWEEPS.load(Relaxed),
        "ttl_expired_total": TTL_EXPIRED.load(Relaxed),
        "ttl_sweep_errors_total": TTL_SWEEP_ERRORS.load(Relaxed),
        "ttl_backlog": TTL_BACKLOG.load(Relaxed),
        "index_builds": nonodb_core::progress::snapshot(),
        "avg_batch_size": write_ops as f64 / batches as f64,
        "avg_commit_us": commit_ns as f64 / batches as f64 / 1000.0,
    })
}
