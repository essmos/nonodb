//! Stream maintenance: the lease-expiry janitor and offset-driven retention.
//!
//! Every tick the loop first recovers expired leases (requeue or dead-letter)
//! for every group, then drops events below the slowest consumer's committed
//! offset (minus `min_seq_keep`) and older than `max_age_ms`. Pruning runs
//! through the writer so deletions are replicated and observable like any other
//! write.

use std::{sync::Arc, time::Duration};

use tokio::sync::{oneshot, watch};

use crate::{
    databases::Databases,
    metrics,
    protocol::{Durability, WriteReq},
    writer::WriteJob,
};

#[derive(Clone, Copy, Debug)]
struct Config {
    interval: Duration,
    batch: usize,
}

impl Config {
    fn from_env() -> Result<Option<Self>, String> {
        // Retention and the lease janitor run by default; `0` disables them.
        let interval = std::env::var("NONODB_STREAM_PRUNE_INTERVAL")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(60);
        if interval == 0 {
            tracing::info!("stream maintenance disabled by NONODB_STREAM_PRUNE_INTERVAL=0");
            return Ok(None);
        }
        let batch = std::env::var("NONODB_STREAM_PRUNE_BATCH")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1_000)
            .clamp(1, 10_000);
        Ok(Some(Self {
            interval: Duration::from_secs(interval.max(1)),
            batch,
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
            run_once(&databases, config).await;
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = tokio::time::sleep(config.interval) => {}
            }
        }
        tracing::info!("stream retention loop stopped");
    });
    Ok(())
}

async fn run_once(databases: &Databases, config: Config) {
    for name in databases.names().await {
        // Replicas run no maintenance: everything below writes.
        if !crate::replicator::writable() {
            continue;
        }
        let runtime = match databases.get(&name).await {
            Ok(runtime) => runtime,
            Err(error) => {
                tracing::warn!(database = name, error = %error.message, "stream retention open failed");
                continue;
            }
        };
        for group in runtime.store.stream_groups(None).unwrap_or_default() {
            // Requeue/dead-letter through the writer: lease state is node-local,
            // but the resulting committed-offset advance and dead letters must
            // reach replicas, and only the primary may act on them at all.
            match recover(&runtime.write_tx, &group.stream, &group.group).await {
                Ok((0, 0)) => {}
                Ok((recovered, dead)) => {
                    metrics::add(&metrics::STREAM_LEASE_RECOVERIES, recovered);
                    tracing::info!(
                        database = name,
                        stream = group.stream,
                        group = group.group,
                        recovered,
                        dead,
                        "stream lease janitor recovered expired batches"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        database = name,
                        stream = group.stream,
                        group = group.group,
                        error = %error.message,
                        "stream lease janitor failed"
                    );
                }
            }
        }
        let streams = match runtime.store.streams() {
            Ok(streams) => streams,
            Err(error) => {
                tracing::warn!(database = name, error = %error, "stream retention list failed");
                continue;
            }
        };
        for def in streams {
            if def.retention.is_empty() {
                continue;
            }
            // Drain in batches until the retention limit stops moving.
            loop {
                match prune(&runtime.write_tx, &def.name, config.batch).await {
                    Ok(deleted) if deleted > 0 => {
                        metrics::add(&metrics::STREAM_PRUNED_EVENTS, deleted as u64);
                        tracing::info!(
                            database = name,
                            stream = def.name,
                            deleted,
                            "stream retention pruned"
                        );
                        if deleted < config.batch {
                            break;
                        }
                    }
                    Ok(_) => break,
                    Err(error) => {
                        tracing::warn!(
                            database = name,
                            stream = def.name,
                            error = %error.message,
                            "stream retention prune failed"
                        );
                        break;
                    }
                }
            }
        }
    }
}

async fn recover(
    write_tx: &std::sync::mpsc::Sender<WriteJob>,
    stream: &str,
    group: &str,
) -> Result<(u64, u64), crate::protocol::ApiError> {
    let (resp, rx) = oneshot::channel();
    write_tx
        .send(WriteJob {
            req: WriteReq::RecoverStreamLeases {
                stream: stream.to_string(),
                group: group.to_string(),
            },
            resp,
            isolated: false,
            durability: Durability::Full,
        })
        .map_err(|_| crate::protocol::ApiError {
            code: "writer_closed".into(),
            message: "writer stopped".into(),
        })?;
    let out = rx.await.map_err(|_| crate::protocol::ApiError {
        code: "writer_closed".into(),
        message: "writer stopped".into(),
    })??;
    Ok((
        out.get("recovered")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        out.get("dead")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
    ))
}

async fn prune(
    write_tx: &std::sync::mpsc::Sender<WriteJob>,
    coll: &str,
    batch: usize,
) -> Result<usize, crate::protocol::ApiError> {
    let (resp, rx) = oneshot::channel();
    write_tx
        .send(WriteJob {
            req: WriteReq::PruneStream {
                coll: coll.to_string(),
                batch,
            },
            resp,
            isolated: false,
            durability: Durability::Relaxed,
        })
        .map_err(|_| crate::protocol::ApiError {
            code: "writer_closed".into(),
            message: "writer stopped".into(),
        })?;
    let out = rx.await.map_err(|_| crate::protocol::ApiError {
        code: "writer_closed".into(),
        message: "writer stopped".into(),
    })??;
    Ok(out
        .get("deleted")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0) as usize)
}
