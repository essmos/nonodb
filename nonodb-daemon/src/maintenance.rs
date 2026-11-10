//! Background maintenance worker.
//!
//! The worker never opens its own LMDB write transaction. TTL expiry, history
//! pruning, and auto-index builds are submitted to the single writer thread so
//! the daemon remains the only LMDB writer.

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

#[derive(Debug, Clone)]
struct Config {
    interval: Duration,
    ttl_interval: Duration,
    ttl_limit: usize,
}

impl Config {
    fn from_env() -> Self {
        Self {
            interval: Duration::from_millis(env_u64("NONODB_MAINTENANCE_INTERVAL_MS", 1000).max(1)),
            ttl_interval: Duration::from_secs(env_u64("NONODB_TTL_SWEEP_INTERVAL", 5).max(1)),
            ttl_limit: env_usize("NONODB_TTL_SWEEP_LIMIT", 256).clamp(1, 10_000),
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

pub fn start(store: Arc<Store>, write_tx: mpsc::Sender<WriteJob>, auto_index: Arc<AutoIndexer>) {
    let config = Config::from_env();
    std::thread::Builder::new()
        .name("nonodb-maintenance".into())
        .spawn(move || run(store, write_tx, auto_index, config))
        .expect("failed to start maintenance worker");
}

fn run(
    store: Arc<Store>,
    write_tx: mpsc::Sender<WriteJob>,
    auto_index: Arc<AutoIndexer>,
    config: Config,
) {
    // Sweep once shortly after startup, then at the configured cadence.
    let mut last_ttl: Option<Instant> = None;
    let mut last_prune: Option<Instant> = None;

    loop {
        std::thread::sleep(config.interval);

        if last_ttl.is_none_or(|at| at.elapsed() >= config.ttl_interval) {
            last_ttl = Some(Instant::now());
            if let Err(e) = sweep_ttl(&store, &write_tx, config.ttl_limit) {
                metrics::inc(&metrics::TTL_SWEEP_ERRORS);
                tracing::warn!(?e, "TTL sweep failed");
            }
        }

        if last_prune.is_none_or(|at| at.elapsed() >= HISTORY_PRUNE_INTERVAL)
            && store.history_config().active()
        {
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

        auto_index.tick_due();
    }
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
