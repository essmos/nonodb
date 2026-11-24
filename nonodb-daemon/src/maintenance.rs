//! Background maintenance worker.
//!
//! The worker never opens its own LMDB write transaction. TTL expiry, history
//! pruning, and auto-index builds are submitted to the single writer thread so
//! the daemon remains the only LMDB writer.

use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nonodb_core::{ttl, Store};
use serde_json::Value;
use tokio::sync::oneshot;

use crate::auto_index::AutoIndexer;
use crate::metrics;
use crate::protocol::{ApiError, ExpireEntry, WriteReq};
use crate::writer::WriteJob;

const HISTORY_PRUNE_INTERVAL: Duration = Duration::from_secs(30);
const HISTORY_PRUNE_LIMIT: usize = 1000;
const SUBSCRIPTION_PRUNE_INTERVAL: Duration = Duration::from_secs(60);
/// Marker written when deleted time-series pages exceed the compaction threshold.
/// The actual file swap happens before the environment is opened on the next
/// startup, so no live connection is interrupted.
pub const COMPACT_REQUEST_FILE: &str = ".timeseries-compact-requested";

#[derive(Debug, Clone)]
struct Config {
    interval: Duration,
    ttl_interval: Duration,
    ttl_limit: usize,
    ts_retention_interval: Duration,
    ts_compact_threshold: u64,
}

impl Config {
    fn from_env() -> Self {
        Self {
            interval: Duration::from_millis(env_u64("NONODB_MAINTENANCE_INTERVAL_MS", 1000).max(1)),
            ttl_interval: Duration::from_secs(env_u64("NONODB_TTL_SWEEP_INTERVAL", 5).max(1)),
            ttl_limit: env_usize("NONODB_TTL_SWEEP_LIMIT", 256).clamp(1, 10_000),
            ts_retention_interval: Duration::from_secs(env_u64(
                "NONODB_TS_RETENTION_INTERVAL_SECS",
                60,
            )),
            ts_compact_threshold: env_u64("NONODB_TS_COMPACT_THRESHOLD_MB", 256)
                .saturating_mul(1024 * 1024),
        }
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

pub fn start(
    store: Arc<Store>,
    write_tx: mpsc::Sender<WriteJob>,
    auto_index: Arc<AutoIndexer>,
    path: PathBuf,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> std::thread::JoinHandle<()> {
    let config = Config::from_env();
    std::thread::Builder::new()
        .name("nonodb-maintenance".into())
        .spawn(move || run(store, write_tx, auto_index, path, config, stop))
        .expect("failed to start maintenance worker")
}

fn run(
    store: Arc<Store>,
    write_tx: mpsc::Sender<WriteJob>,
    auto_index: Arc<AutoIndexer>,
    path: PathBuf,
    config: Config,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    // Sweep once shortly after startup, then at the configured cadence.
    let mut last_ttl: Option<Instant> = None;
    let mut last_prune: Option<Instant> = None;
    let mut last_subscription_prune: Option<Instant> = None;
    let mut last_ts_retention: Option<Instant> = None;
    let mut ts_deletions_seen = false;

    loop {
        let deadline = Instant::now() + config.interval;
        while Instant::now() < deadline {
            if stop.load(std::sync::atomic::Ordering::Relaxed) {
                return;
            }
            std::thread::sleep(
                Duration::from_millis(100).min(deadline.saturating_duration_since(Instant::now())),
            );
        }
        if stop.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        if !crate::replicator::writable() {
            // Replicas receive TTL deletes and metadata from the primary. Local
            // maintenance writes would be rejected by the read-only writer.
            continue;
        }

        if last_ttl.is_none_or(|at| at.elapsed() >= config.ttl_interval) {
            last_ttl = Some(Instant::now());
            if let Err(e) = sweep_ttl(&store, &write_tx, config.ttl_limit) {
                metrics::inc(&metrics::TTL_SWEEP_ERRORS);
                tracing::warn!(?e, "TTL sweep failed");
            }
        }

        if last_prune.is_none_or(|at| at.elapsed() >= HISTORY_PRUNE_INTERVAL) {
            last_prune = Some(Instant::now());
            if let Err(e) = submit(
                &write_tx,
                WriteReq::PruneHistory {
                    limit: HISTORY_PRUNE_LIMIT,
                },
                false,
            ) {
                tracing::warn!(?e, "history pruning failed");
            }
        }

        if last_subscription_prune.is_none_or(|at| at.elapsed() >= SUBSCRIPTION_PRUNE_INTERVAL) {
            last_subscription_prune = Some(Instant::now());
            if let Err(e) = submit(&write_tx, WriteReq::PruneSubscriptions, false) {
                tracing::warn!(?e, "subscription pruning failed");
            }
        }

        if !config.ts_retention_interval.is_zero()
            && last_ts_retention.is_none_or(|at| at.elapsed() >= config.ts_retention_interval)
        {
            last_ts_retention = Some(Instant::now());
            match sweep_timeseries_retention(&store, &write_tx) {
                Ok(changed) => ts_deletions_seen |= changed,
                Err(e) => {
                    metrics::inc(&metrics::TS_RETENTION_ERRORS);
                    tracing::warn!(?e, "time-series retention failed");
                }
            }
            if config.ts_compact_threshold > 0 && ts_deletions_seen {
                if let Some(bytes) = data_file_bytes(&path) {
                    if should_request_compaction(bytes, config.ts_compact_threshold, true) {
                        if let Err(e) = request_compaction(&path) {
                            tracing::warn!(?e, "time-series compaction request failed");
                        }
                    }
                }
            }
        }

        auto_index.tick_due();
    }
}

/// Runs one automatic time-series retention pass over every collection that
/// configured `expireAfterSeconds`. Returns true when at least one deleted row
/// was observed, which is the signal for offline compaction accounting.
fn sweep_timeseries_retention(
    store: &Store,
    write_tx: &mpsc::Sender<WriteJob>,
) -> Result<bool, String> {
    let now = ttl::now_ms() as i64;
    let mut changed = false;
    let mut collections = 0u64;
    let mut rows = 0u64;
    let mut deleted_rows = 0u64;
    let mut buckets = 0u64;
    let mut encoded_bytes = 0u64;
    let mut raw_bytes = 0u64;
    for name in store.collections().map_err(|e| e.to_string())? {
        let Some(settings) = store
            .timeseries_settings(&name)
            .map_err(|e| e.to_string())?
        else {
            continue;
        };
        collections += 1;
        if let Some(expire) = settings.expire_after_seconds.filter(|value| *value > 0) {
            let before_ms = now.saturating_sub((expire as i64).saturating_mul(1000));
            metrics::inc(&metrics::TS_RETENTION_RUNS);
            let value = submit(
                write_tx,
                WriteReq::RetainTimeSeries {
                    coll: name.clone(),
                    before_ms,
                },
                false,
            )?;
            if let Some(report) = value.get("report") {
                let dropped = report
                    .get("dropped_buckets")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let boundary = report
                    .get("tombstoned_buckets")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let dropped_rows = report
                    .get("dropped_rows")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                metrics::add(&metrics::TS_RETENTION_DROPPED_BUCKETS, dropped);
                metrics::add(&metrics::TS_RETENTION_BOUNDARY_BUCKETS, boundary);
                changed |= dropped > 0 || boundary > 0 || dropped_rows > 0;
            }
        }
        // Read stats after retention so the gauges reflect the post-pass state.
        if let Some(stats) = store.timeseries_stats(&name).map_err(|e| e.to_string())? {
            rows += stats.rows;
            deleted_rows += stats.deleted_rows;
            buckets += stats.buckets;
            encoded_bytes += stats.encoded_bytes;
            raw_bytes += stats.raw_bytes;
            if stats.deleted_rows > 0 {
                changed = true;
            }
        }
    }
    metrics::set(&metrics::TS_COLLECTIONS, collections);
    metrics::set(&metrics::TS_ROWS, rows);
    metrics::set(&metrics::TS_DELETED_ROWS, deleted_rows);
    metrics::set(&metrics::TS_BUCKETS, buckets);
    metrics::set(&metrics::TS_ENCODED_BYTES, encoded_bytes);
    metrics::set(&metrics::TS_RAW_BYTES, raw_bytes);
    Ok(changed)
}

fn data_file_bytes(path: &Path) -> Option<u64> {
    std::fs::metadata(path.join("data.mdb"))
        .ok()
        .map(|meta| meta.len())
}

fn should_request_compaction(file_bytes: u64, threshold: u64, deletions_seen: bool) -> bool {
    deletions_seen && threshold > 0 && file_bytes >= threshold
}

/// Writes the offline-compaction marker consumed by the next startup.
fn request_compaction(path: &Path) -> Result<(), String> {
    let marker = path.join(COMPACT_REQUEST_FILE);
    if marker.exists() {
        return Ok(());
    }
    let payload = format!(
        "{{\"requested_at_ms\":{},\"data_bytes\":{}}}\n",
        ttl::now_ms(),
        data_file_bytes(path).unwrap_or(0)
    );
    std::fs::write(&marker, payload).map_err(|e| e.to_string())?;
    metrics::inc(&metrics::TS_COMPACTION_REQUESTS);
    tracing::warn!(
        path = %path.display(),
        "time-series deletions exceeded the compaction threshold; data.mdb will be compacted on next startup"
    );
    Ok(())
}

/// Compacts a database directory before it is opened when the previous run left
/// a compaction request. Safe by construction: no environment is open and the
/// caller has not started the writer or listeners yet.
pub fn compact_if_requested(
    path: &Path,
) -> Result<Option<nonodb_core::store::CompactReport>, String> {
    let marker = path.join(COMPACT_REQUEST_FILE);
    if !marker.exists() {
        return Ok(None);
    }
    let report = nonodb_core::store::compact_data_dir(path).map_err(|e| e.to_string())?;
    std::fs::remove_file(&marker).map_err(|e| e.to_string())?;
    metrics::inc(&metrics::TS_COMPACTIONS);
    metrics::add(
        &metrics::TS_COMPACTED_BYTES,
        report.before_bytes.saturating_sub(report.after_bytes),
    );
    tracing::info!(
        path = %path.display(),
        before = report.before_bytes,
        after = report.after_bytes,
        "compacted time-series database on startup"
    );
    Ok(Some(report))
}

fn sweep_ttl(store: &Store, write_tx: &mpsc::Sender<WriteJob>, limit: usize) -> Result<(), String> {
    metrics::inc(&metrics::TTL_SWEEPS);
    let due = store
        .ttl_due(ttl::now_ms(), limit)
        .map_err(|e| e.to_string())?;
    metrics::set(&metrics::TTL_BACKLOG, due.len() as u64);
    if due.is_empty() {
        return Ok(());
    }
    let entries = due
        .into_iter()
        .map(|entry| ExpireEntry {
            coll: entry.coll,
            id: entry.id,
            expected_ttl_at: entry.expire_at,
        })
        .collect();
    let value = submit(write_tx, WriteReq::Expire { entries }, false)?;
    let expired = value.get("expired").and_then(Value::as_u64).unwrap_or(0);
    metrics::add(&metrics::TTL_EXPIRED, expired);
    Ok(())
}

fn submit(
    write_tx: &mpsc::Sender<WriteJob>,
    req: WriteReq,
    isolated: bool,
) -> Result<Value, String> {
    let (resp, rx) = oneshot::channel();
    metrics::inc(&metrics::QUEUE_LEN);
    metrics::note_queue();
    if write_tx
        .send(WriteJob {
            req,
            resp,
            isolated,
            durability: crate::protocol::Durability::Full,
        })
        .is_err()
    {
        metrics::sub(&metrics::QUEUE_LEN, 1);
        return Err("writer unavailable".into());
    }
    metrics::inc(&metrics::WRITE_OPS);
    match rx.blocking_recv() {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(ApiError { code, message })) => Err(format!("{code}: {message}")),
        Err(_) => Err("writer dropped maintenance request".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn compaction_threshold_requires_deletions() {
        assert!(!should_request_compaction(1024, 512, false));
        assert!(!should_request_compaction(1024, 0, true));
        assert!(!should_request_compaction(511, 512, true));
        assert!(should_request_compaction(512, 512, true));
    }

    #[test]
    fn scheduled_retention_deletes_expired_rows_through_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path()).unwrap());
        let settings = nonodb_core::timeseries::Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            expire_after_seconds: Some(3600),
            ..Default::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        let now = ttl::now_ms() as i64;
        for idx in 0..20i64 {
            let ts = if idx < 10 {
                now - 7_200_000 - idx * 1000
            } else {
                now - idx * 1000
            };
            c.insert_one(json!({
                "ts": ts,
                "device": {"id": "a"},
                "value": idx,
            }))
            .unwrap();
        }
        let (tx, rx) = mpsc::channel();
        let cache = Arc::new(crate::cache::QueryCache::from_env());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer_stop = stop.clone();
        let writer_store = store.clone();
        let writer = std::thread::spawn(move || {
            crate::writer::run(writer_store, rx, cache, writer_stop);
        });

        let changed = sweep_timeseries_retention(&store, &tx).unwrap();
        assert!(changed);
        assert_eq!(c.count(&json!({})).unwrap(), 10);
        assert_eq!(
            metrics::TS_COLLECTIONS.load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            metrics::TS_ROWS.load(std::sync::atomic::Ordering::Relaxed),
            10
        );
        assert!(metrics::TS_RETENTION_RUNS.load(std::sync::atomic::Ordering::Relaxed) > 0);
        assert_eq!(c.count(&json!({"missing": true})).unwrap(), 0);

        drop(tx);
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        writer.join().unwrap();
        let store = Arc::try_unwrap(store).ok().unwrap();
        store.close();
    }

    #[test]
    fn marker_compacts_before_open_and_preserves_rows() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let settings = nonodb_core::timeseries::Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            ..Default::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        for idx in 0..2000i64 {
            c.insert_one(json!({
                "ts": idx * 1000,
                "device": {"id": format!("dev-{}", idx % 10)},
                "value": idx,
                "payload": "x".repeat(200),
            }))
            .unwrap();
        }
        store.seal_timeseries("metrics").unwrap();
        assert_eq!(
            c.delete(&json!({"value": {"$lt": 1900}}), true).unwrap(),
            1900
        );
        store.close();

        let before = data_file_bytes(dir.path()).unwrap();
        request_compaction(dir.path()).unwrap();
        assert!(dir.path().join(COMPACT_REQUEST_FILE).exists());
        let report = compact_if_requested(dir.path()).unwrap().unwrap();
        assert!(report.after_bytes < before);
        assert!(!dir.path().join(COMPACT_REQUEST_FILE).exists());

        let store = Store::open(dir.path()).unwrap();
        let c = store.collection("metrics").unwrap();
        assert_eq!(c.count(&json!({})).unwrap(), 100);
        assert_eq!(c.count(&json!({"value": 1999})).unwrap(), 1);
        assert!(c.find_by_key("missing").unwrap().is_none());
    }
}
