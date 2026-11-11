//! Process-local progress for long-running index backfills.
//!
//! LMDB writes happen in one transaction, so in-progress counters cannot be
//! read from the database by other requests. This registry exposes them to the
//! daemon metrics endpoint instead. It is intentionally small and process-local:
//! a restart loses only the progress display, never index data.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct BackfillSnapshot {
    pub collection: String,
    pub target: String,
    pub kind: String,
    pub processed: u64,
    pub total: u64,
    pub finished: bool,
    pub started_at_ms: u64,
    pub elapsed_ms: u64,
}

struct BackfillState {
    collection: String,
    target: String,
    kind: String,
    total: u64,
    processed: AtomicU64,
    finished: AtomicBool,
    started_at_ms: u64,
}

impl BackfillState {
    fn snapshot(&self) -> BackfillSnapshot {
        let now = now_ms();
        BackfillSnapshot {
            collection: self.collection.clone(),
            target: self.target.clone(),
            kind: self.kind.clone(),
            processed: self.processed.load(Ordering::Relaxed),
            total: self.total,
            finished: self.finished.load(Ordering::Relaxed),
            started_at_ms: self.started_at_ms,
            elapsed_ms: now.saturating_sub(self.started_at_ms),
        }
    }
}

static REGISTRY: OnceLock<Mutex<Vec<Arc<BackfillState>>>> = OnceLock::new();

fn registry() -> &'static Mutex<Vec<Arc<BackfillState>>> {
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Starts progress tracking for one backfill. The guard marks the build as
/// finished when it is dropped.
pub struct BackfillGuard {
    state: Arc<BackfillState>,
}

impl BackfillGuard {
    pub fn advance(&self, docs: u64) {
        self.state.processed.fetch_add(docs, Ordering::Relaxed);
    }
}

impl Drop for BackfillGuard {
    fn drop(&mut self) {
        self.state.finished.store(true, Ordering::Relaxed);
    }
}

pub fn begin(collection: &str, target: &str, kind: &str, total: u64) -> BackfillGuard {
    let state = Arc::new(BackfillState {
        collection: collection.to_string(),
        target: target.to_string(),
        kind: kind.to_string(),
        total,
        processed: AtomicU64::new(0),
        finished: AtomicBool::new(false),
        started_at_ms: now_ms(),
    });
    let mut entries = registry().lock().expect("index progress lock");
    if entries.len() >= 64 {
        entries.retain(|entry| !entry.finished.load(Ordering::Relaxed));
        if entries.len() >= 64 {
            let keep = entries.len() - 63;
            entries.drain(..keep);
        }
    }
    entries.push(Arc::clone(&state));
    BackfillGuard { state }
}

/// Current snapshot of active and recently finished backfills.
pub fn snapshot() -> Vec<BackfillSnapshot> {
    let entries = registry().lock().expect("index progress lock");
    let mut out: Vec<_> = entries.iter().map(|entry| entry.snapshot()).collect();
    out.sort_by_key(|entry| entry.started_at_ms);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_tracks_processed_and_finishes_on_drop() {
        let guard = begin("progress-test", "field", "index", 10);
        guard.advance(4);
        let entry = snapshot()
            .into_iter()
            .find(|entry| entry.collection == "progress-test")
            .unwrap();
        assert_eq!(entry.processed, 4);
        assert_eq!(entry.total, 10);
        assert!(!entry.finished);
        drop(guard);
        let entry = snapshot()
            .into_iter()
            .find(|entry| entry.collection == "progress-test")
            .unwrap();
        assert!(entry.finished);
    }
}
