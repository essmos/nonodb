//! PITR WAL archiver.
//!
//! The writer records every durable transaction in the replication log. This
//! thread copies that log into segmented WAL files, enforces hard size limits,
//! and requests a snapshot before the limit is reached. Snapshots and WAL
//! pruning are coordinated by `spawn_snapshots` in `main.rs`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nonodb_core::pitr::{self, WalWriter};
use nonodb_core::Store;

use crate::metrics;

const DEFAULT_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_MAX_BYTES: u64 = 20 * 1024 * 1024 * 1024;
const DEFAULT_WINDOW_SECS: u64 = 7 * 24 * 60 * 60;
const COPY_BATCH: usize = 512;

#[derive(Clone, Debug, Default)]
struct WalState {
    database: String,
    bytes: u64,
    oldest_unix: u64,
    last_seq: u64,
    full: bool,
    snapshot_requested: bool,
    prune_through: Option<u64>,
    error: Option<String>,
}

fn states() -> &'static Mutex<HashMap<usize, WalState>> {
    static STATES: OnceLock<Mutex<HashMap<usize, WalState>>> = OnceLock::new();
    STATES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn env_bool(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_size(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| nonodb_core::parse_size(&value))
        .map(|value| value as u64)
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// PITR is opt-in so existing deployments keep their current retention
/// behaviour and disk usage.
pub fn enabled() -> bool {
    env_bool("NONODB_PITR_ENABLED")
}

pub fn segment_bytes() -> u64 {
    env_size("NONODB_WAL_SEGMENT_BYTES", DEFAULT_SEGMENT_BYTES)
}

pub fn max_bytes() -> u64 {
    env_size("NONODB_WAL_MAX_BYTES", DEFAULT_MAX_BYTES)
}

pub fn max_age_secs() -> u64 {
    env_u64("NONODB_WAL_MAX_AGE_SECS", window_secs())
}

/// Effective retention window. A shorter hard age cap also shortens the PITR
/// window rather than pretending older history is still recoverable.
pub fn retention_secs() -> u64 {
    window_secs().min(max_age_secs()).max(1)
}

pub fn window_secs() -> u64 {
    std::env::var("NONODB_PITR_WINDOW_SECS")
        .or_else(|_| std::env::var("NONODB_PITR_WINDOW"))
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_WINDOW_SECS)
}

pub fn dir(database_path: &Path) -> PathBuf {
    database_path.join(pitr::WAL_DIR)
}

/// True when the WAL hard limit has been reached for this store. The writer
/// rejects new writes with `wal_full` until snapshot retention frees space.
pub fn is_full(store: &Store) -> bool {
    states()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&store.id())
        .is_some_and(|state| state.full)
}

/// Consumed by the snapshot loop. A request is raised at 80% of the hard
/// limit and when the limit is reached.
pub fn take_snapshot_request(store: &Store) -> bool {
    let mut map = states()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(state) = map.get_mut(&store.id()) else {
        return false;
    };
    let requested = state.snapshot_requested;
    state.snapshot_requested = false;
    requested
}

fn update_state<F>(store_id: usize, update: F)
where
    F: FnOnce(&mut WalState),
{
    let mut map = states()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let state = map.entry(store_id).or_default();
    update(state);
}

/// Asks the WAL thread to prune sealed segments through `through`. The WAL
/// thread owns the active file handle, so it rotates a fully-covered active
/// segment before deleting it.
pub fn request_prune(store: &Store, through: u64) {
    if through == 0 {
        return;
    }
    update_state(store.id(), |state| {
        state.prune_through = Some(state.prune_through.map_or(through, |old| old.max(through)));
    });
}

fn take_prune(store_id: usize) -> Option<u64> {
    let mut map = states()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    map.get_mut(&store_id)
        .and_then(|state| state.prune_through.take())
}

fn remove_state(store_id: usize) {
    states()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&store_id);
}

/// Starts the WAL archiver for one database. Returns `None` when PITR is
/// disabled or the thread cannot be created.
pub fn start(
    store: Arc<Store>,
    database_path: PathBuf,
    database: String,
    stop: Arc<AtomicBool>,
) -> Option<std::thread::JoinHandle<()>> {
    if !enabled() {
        return None;
    }
    update_state(store.id(), |state| {
        state.database = database.clone();
    });
    std::thread::Builder::new()
        .name(format!("nonodb-wal-{database}"))
        .spawn(move || run(store, database_path, database, stop))
        .ok()
}

fn run(store: Arc<Store>, database_path: PathBuf, database: String, stop: Arc<AtomicBool>) {
    let wal_dir = dir(&database_path);
    if let Err(error) = prepare_dir(&wal_dir, &store) {
        tracing::error!(database, error = %error, "PITR WAL could not start");
        update_state(store.id(), |state| {
            state.error = Some(error);
            state.full = true;
        });
        return;
    }
    let max_bytes = max_bytes();
    let max_age_secs = max_age_secs();
    let soft_bytes = max_bytes.saturating_mul(4) / 5;
    let mut writer = match WalWriter::open(&wal_dir, segment_bytes()) {
        Ok(writer) => writer,
        Err(error) => {
            tracing::error!(database, error = %error, "PITR WAL open failed");
            update_state(store.id(), |state| {
                state.error = Some(error.to_string());
                state.full = true;
            });
            return;
        }
    };
    let mut last_log_prune = Instant::now();
    let mut catchup_error: Option<String> = None;
    while !stop.load(Ordering::Relaxed) {
        let mut copied = 0usize;
        let mut caught_up = false;
        if catchup_error.is_none() {
            loop {
                match store.replication_after(writer.last_seq(), COPY_BATCH) {
                    Ok(batch) if batch.is_empty() => {
                        caught_up = true;
                        break;
                    }
                    Ok(batch) => {
                        let len = batch.len();
                        match writer.append_batch(&batch) {
                            Ok(appended) => copied = copied.saturating_add(appended),
                            Err(error) => {
                                catchup_error = Some(error.to_string());
                                tracing::error!(database, error = %error, "PITR WAL append failed");
                                break;
                            }
                        }
                        if len < COPY_BATCH {
                            caught_up = true;
                            break;
                        }
                    }
                    Err(error) => {
                        catchup_error = Some(error.to_string());
                        tracing::error!(database, error = %error, "PITR WAL log read failed");
                        break;
                    }
                }
            }
        }
        if copied > 0 {
            if let Err(error) = writer.sync() {
                catchup_error = Some(error.to_string());
                tracing::error!(database, error = %error, "PITR WAL fsync failed");
            }
        }

        if let Some(through) = take_prune(store.id()) {
            if writer.last_seq() <= through {
                // Seal the active segment so it can be removed. The writer's
                // new segment starts after the current head; `last_seq` is
                // unchanged.
                if let Err(error) = writer.rotate_after_current() {
                    tracing::warn!(database, error = %error, "PITR WAL rotate for prune failed");
                }
            }
            match pitr::prune_through(&wal_dir, through) {
                Ok(removed) if removed > 0 => {
                    metrics::record_wal_pruned(&database, removed);
                    tracing::info!(database, through, removed, "PITR WAL retention pruned");
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(database, error = %error, "PITR WAL prune failed");
                }
            }
        }

        let bytes = pitr::bytes_used(&wal_dir);
        let (oldest_unix, last_seq) = (
            pitr::oldest_created_at_ms(&wal_dir)
                .map(|ms| ms / 1000)
                .unwrap_or(0),
            writer.last_seq(),
        );
        let now_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        let age_secs = now_unix.saturating_sub(oldest_unix);
        let too_old = oldest_unix > 0 && age_secs >= max_age_secs;
        let full = bytes >= max_bytes || too_old;
        let available = catchup_error.is_none() && last_seq > 0 && !full;
        update_state(store.id(), |state| {
            state.database = database.clone();
            state.bytes = bytes;
            state.oldest_unix = oldest_unix;
            state.last_seq = last_seq;
            state.full = full;
            state.error = catchup_error.clone();
            if bytes >= soft_bytes || too_old {
                state.snapshot_requested = true;
            }
        });
        metrics::record_wal(&database, bytes, oldest_unix, last_seq, full, available);
        if full {
            tracing::warn!(
                database,
                bytes,
                limit = max_bytes,
                age_secs,
                max_age_secs,
                "PITR WAL limit reached; writes will be rejected until a snapshot prunes retained segments"
            );
        }

        // Without replicas, the durable replication log only exists to feed
        // this WAL. Prune only after the WAL has been fsynced above.
        if !replication_configured() && last_log_prune.elapsed() >= Duration::from_secs(30) {
            last_log_prune = Instant::now();
            let through = writer.last_seq().saturating_sub(1024);
            if through > 0 {
                if let Err(error) = store.prune_replication_through(through) {
                    tracing::warn!(database, error = %error, "log retention prune failed");
                }
            }
        }

        let _ = caught_up;
        if copied == 0 {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    let _ = writer.sync();
    remove_state(store.id());
    tracing::info!(database, "PITR WAL archiver stopped");
}

fn prepare_dir(wal_dir: &Path, store: &Store) -> Result<(), String> {
    std::fs::create_dir_all(wal_dir)
        .map_err(|error| format!("create {}: {error}", wal_dir.display()))?;
    let head = store
        .replication_head()
        .map_err(|error| format!("read replication head: {error}"))?;
    let wal_last = pitr::last_seq(wal_dir);
    if wal_last > head {
        // The database was restored to an older point. Keeping a WAL that is
        // newer than the live data would make new sequences collide, so move
        // the superseded history aside instead of silently deleting it.
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0);
        let superseded = wal_dir.with_file_name(format!("{}.superseded-{stamp}", pitr::WAL_DIR));
        std::fs::rename(wal_dir, &superseded).map_err(|error| {
            format!(
                "move superseded WAL {} -> {}: {error}",
                wal_dir.display(),
                superseded.display()
            )
        })?;
        std::fs::create_dir_all(wal_dir)
            .map_err(|error| format!("recreate {}: {error}", wal_dir.display()))?;
        tracing::warn!(
            wal = %superseded.display(),
            head,
            wal_last,
            "PITR WAL was ahead of restored data; moved aside"
        );
    }
    Ok(())
}

fn replication_configured() -> bool {
    std::env::var("NONODB_REPLICATION_ENABLED")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}
