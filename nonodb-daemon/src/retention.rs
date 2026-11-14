//! Automatic replication-log retention based on replica acknowledgements.

use std::{sync::Arc, time::Duration};

use tokio::sync::watch;

use crate::{databases::Databases, metrics, replicator, writer};

#[derive(Clone, Copy, Debug)]
struct Config {
    interval: Duration,
    keep: u64,
}

impl Config {
    fn from_env() -> Result<Option<Self>, String> {
        let interval = std::env::var("NONODB_REPLICATION_PRUNE_INTERVAL")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        if interval == 0 {
            return Ok(None);
        }
        let keep = std::env::var("NONODB_REPLICATION_RETENTION_KEEP")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        Ok(Some(Self {
            interval: Duration::from_secs(interval),
            keep,
        }))
    }
}

pub fn start(databases: Arc<Databases>, shutdown: watch::Receiver<bool>) -> Result<(), String> {
    let Some(config) = Config::from_env()? else {
        return Ok(());
    };
    tokio::spawn(async move {
        let mut shutdown = shutdown;
        loop {
            if *shutdown.borrow() {
                break;
            }
            if replicator::writable() {
                run_once(&databases, config).await;
            }
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = tokio::time::sleep(config.interval) => {}
            }
        }
        tracing::info!("replication retention loop stopped");
    });
    Ok(())
}

async fn run_once(databases: &Databases, config: Config) {
    let Some(origin) = writer::replication_origin() else {
        return;
    };
    for name in databases.names().await {
        let runtime = match databases.get(&name).await {
            Ok(runtime) => runtime,
            Err(error) => {
                tracing::warn!(database = name, error = %error.message, "retention database open failed");
                continue;
            }
        };
        let acks = match runtime.store.replica_acks(&origin) {
            Ok(acks) => acks,
            Err(error) => {
                tracing::warn!(database = name, error = %error, "retention ack read failed");
                continue;
            }
        };
        if acks.is_empty() {
            continue;
        }
        metrics::inc(&metrics::REPLICATION_RETENTION_RUNS);
        let min_ack = acks.iter().map(|(_, seq)| *seq).min().unwrap_or(0);
        let head = match runtime.store.replication_head() {
            Ok(head) => head,
            Err(error) => {
                tracing::warn!(database = name, error = %error, "retention head read failed");
                continue;
            }
        };
        let through = min_ack.saturating_sub(config.keep).min(head);
        if through == 0 {
            continue;
        }
        match runtime.store.prune_replication_through(through) {
            Ok(deleted) if deleted > 0 => {
                metrics::add(&metrics::REPLICATION_PRUNED_TOTAL, deleted as u64);
                tracing::info!(
                    database = name,
                    origin,
                    through,
                    deleted,
                    "replication log retention pruned"
                );
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(database = name, error = %error, "retention prune failed");
            }
        }
    }
}
