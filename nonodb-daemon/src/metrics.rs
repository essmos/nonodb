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
/// Number of operations executed in `/v1/batch` requests.
pub static BATCH_OPS: AtomicU64 = AtomicU64::new(0);
pub static WRITE_OPS: AtomicU64 = AtomicU64::new(0);
pub static WRITE_BATCHES: AtomicU64 = AtomicU64::new(0);
/// Total `commit` time (including `fsync`) in nanoseconds.
pub static COMMIT_NS: AtomicU64 = AtomicU64::new(0);
/// Approximate length of the write queue waiting for the writer.
pub static QUEUE_LEN: AtomicU64 = AtomicU64::new(0);
/// Peak queue length since process start.
pub static QUEUE_PEAK: AtomicU64 = AtomicU64::new(0);

pub fn inc(counter: &AtomicU64) {
    counter.fetch_add(1, Relaxed);
}

pub fn add(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Relaxed);
}

/// Records the current queue length as a potential peak.
pub fn note_queue() {
    let _ = QUEUE_PEAK.fetch_max(QUEUE_LEN.load(Relaxed), Relaxed);
}

/// Safe (non-negative) counter decrement.
pub fn sub(counter: &AtomicU64, n: u64) {
    let _ = counter.try_update(Relaxed, Relaxed, |v| Some(v.saturating_sub(n)));
}

pub fn snapshot() -> Value {
    let batches = WRITE_BATCHES.load(Relaxed).max(1);
    let commit_ns = COMMIT_NS.load(Relaxed);
    let write_ops = WRITE_OPS.load(Relaxed);
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
        "batch_ops_total": BATCH_OPS.load(Relaxed),
        "write_ops_total": write_ops,
        "write_batches_total": WRITE_BATCHES.load(Relaxed),
        "queue_len": QUEUE_LEN.load(Relaxed),
        "queue_peak": QUEUE_PEAK.load(Relaxed),
        "commit_ns_total": commit_ns,
        "avg_batch_size": write_ops as f64 / batches as f64,
        "avg_commit_us": commit_ns as f64 / batches as f64 / 1000.0,
    })
}
