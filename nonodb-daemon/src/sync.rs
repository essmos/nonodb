//! Background `force_sync` for interval durability.
//!
//! With `NONODB_SYNC_INTERVAL_MS` set, [`nonodb_core::Store`] opens LMDB with
//! `NO_SYNC`, so a commit copies pages into the memory map but does not touch
//! the disk. This thread turns that into a bounded-loss window: it syncs at most
//! every `N` milliseconds, and only when a commit actually happened since the
//! last sync, so an idle daemon never writes.
//!
//! `Durability::Full` requests keep calling `force_sync` themselves; this thread
//! is only responsible for writes that asked for less.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use nonodb_core::Store;

/// Starts the syncer when interval durability is enabled.
pub fn start(
    store: Arc<Store>,
    database: String,
    stop: Arc<AtomicBool>,
) -> Option<std::thread::JoinHandle<()>> {
    let interval_ms = nonodb_core::sync_interval_ms();
    if interval_ms == 0
        || nonodb_core::sync_policy(false, interval_ms) != nonodb_core::SyncPolicy::Interval
    {
        return None;
    }
    std::thread::Builder::new()
        .name(format!("nonodb-sync-{database}"))
        .spawn(move || run(store, database, stop, Duration::from_millis(interval_ms)))
        .ok()
}

/// Syncs once if a commit since the last call asked for less than `full`.
///
/// Returns `true` when a sync was performed. Split out from [`run`] so the
/// "only sync when there is something to sync" rule is testable without timers.
fn sync_once(store: &Store) -> bool {
    if crate::writer::take_unsynced_writes() == 0 {
        return false;
    }
    let started = std::time::Instant::now();
    match store.force_sync() {
        Ok(()) => {
            crate::metrics::inc(&crate::metrics::SYNC_INTERVAL_RUNS);
            crate::metrics::add(
                &crate::metrics::SYNC_INTERVAL_NS,
                started.elapsed().as_nanos() as u64,
            );
            true
        }
        Err(error) => {
            tracing::warn!(error = %error, "interval sync failed");
            false
        }
    }
}

fn run(store: Arc<Store>, database: String, stop: Arc<AtomicBool>, interval: Duration) {
    // Sleep in slices so shutdown does not wait a full interval.
    let slice = interval.min(Duration::from_millis(50));
    let mut slept = Duration::ZERO;
    while !stop.load(Ordering::Relaxed) {
        std::thread::sleep(slice);
        slept += slice;
        if slept < interval {
            continue;
        }
        slept = Duration::ZERO;
        let _ = &database;
        sync_once(&store);
    }
    // A clean shutdown should not leave committed-but-unsynced writes behind.
    sync_once(&store);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::TempDir::new().unwrap();
        let store = Store::open(dir.path()).unwrap();
        (dir, store)
    }

    #[test]
    fn idle_daemon_does_not_sync() {
        let (_dir, store) = store();
        let _ = crate::writer::take_unsynced_writes();
        assert!(!sync_once(&store), "no writes since the last sync");
    }

    #[test]
    fn unsynced_commits_trigger_one_sync() {
        let (_dir, store) = store();
        let _ = crate::writer::take_unsynced_writes();
        crate::writer::note_unsynced_for_test();
        crate::writer::note_unsynced_for_test();
        assert!(sync_once(&store));
        assert!(
            !sync_once(&store),
            "the counter is drained, so a second pass has nothing to do"
        );
    }
}
