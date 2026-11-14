//! Automatic candidate election after the primary lease is lost.

use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::replicator;

#[derive(Clone, Debug)]
struct Config {
    interval: Duration,
    timeout: Duration,
    priority: Duration,
}

impl Config {
    fn from_env() -> Option<Self> {
        if !std::env::var("NONODB_REPLICATION_AUTO_FAILOVER")
            .ok()
            .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
        {
            return None;
        }
        if !std::env::var("NONODB_REPLICATION_CANDIDATE")
            .ok()
            .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
        {
            return None;
        }
        let interval = std::env::var("NONODB_REPLICATION_ELECTION_INTERVAL")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(5);
        let timeout = std::env::var("NONODB_REPLICATION_FAILOVER_TIMEOUT")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(30);
        let priority = std::env::var("NONODB_REPLICATION_CANDIDATE_PRIORITY")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        Some(Self {
            interval: Duration::from_secs(interval),
            timeout: Duration::from_secs(timeout),
            priority: Duration::from_secs(priority),
        })
    }
}

pub fn start(data_root: std::path::PathBuf, shutdown: watch::Receiver<bool>) -> Result<(), String> {
    let Some(config) = Config::from_env() else {
        return Ok(());
    };
    if replicator::advertise_url().is_none() || replicator::voter_urls().is_empty() {
        return Ok(());
    }
    tokio::spawn(async move {
        let mut shutdown = shutdown;
        let mut failed_since: Option<Instant> = None;
        loop {
            if *shutdown.borrow() {
                break;
            }
            if replicator::writable() {
                failed_since = None;
            } else {
                match replicator::source_health(&data_root).await {
                    Ok(Some(health)) if health.role == "primary" => {
                        failed_since = None;
                    }
                    Ok(Some(health)) if health.successor.is_some() => {
                        failed_since = None;
                    }
                    Ok(_) | Err(_) => {
                        let since = *failed_since.get_or_insert_with(Instant::now);
                        if since.elapsed() >= config.timeout + config.priority {
                            if !replicator::promotion_ready() {
                                tracing::warn!(
                                    "candidate is not caught up with the last observed source head; refusing automatic promotion"
                                );
                                failed_since = Some(Instant::now());
                                continue;
                            }
                            tracing::warn!(
                                timeout_secs = config.timeout.as_secs(),
                                priority_secs = config.priority.as_secs(),
                                "primary appears unavailable; starting candidate election"
                            );
                            match replicator::promote_with_fence(
                                &data_root,
                                replicator::epoch(),
                                true,
                            )
                            .await
                            {
                                Ok(role) => {
                                    tracing::warn!(
                                        epoch = role.epoch,
                                        "candidate won quorum and became primary"
                                    );
                                    failed_since = None;
                                }
                                Err((code, message)) => {
                                    tracing::warn!(
                                        code,
                                        message,
                                        "candidate election did not win quorum"
                                    );
                                    failed_since = Some(Instant::now());
                                }
                            }
                        }
                    }
                }
            }
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = tokio::time::sleep(config.interval) => {}
            }
        }
        tracing::info!("replication election loop stopped");
    });
    Ok(())
}
