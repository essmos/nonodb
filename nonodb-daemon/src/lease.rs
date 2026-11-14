//! Quorum lease renewal for primary self-fencing.

use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::watch;

use crate::replicator;

#[derive(Clone, Debug)]
struct Config {
    interval: Duration,
    ttl: Duration,
    voters: Vec<String>,
    advertise: String,
    token: Option<String>,
}

impl Config {
    fn from_env() -> Option<Self> {
        let interval = std::env::var("NONODB_REPLICATION_LEASE_INTERVAL")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(5);
        let ttl = std::env::var("NONODB_REPLICATION_LEASE_TTL")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(15);
        let voters = replicator::voter_urls();
        let advertise = replicator::advertise_url()?;
        if voters.is_empty() {
            return None;
        }
        Some(Self {
            interval: Duration::from_secs(interval),
            ttl: Duration::from_secs(ttl),
            voters,
            advertise,
            token: replicator::voter_token(),
        })
    }
}

pub fn start(data_root: std::path::PathBuf, shutdown: watch::Receiver<bool>) -> Result<(), String> {
    let Some(config) = Config::from_env() else {
        return Ok(());
    };
    tokio::spawn(async move {
        let mut shutdown = shutdown;
        let mut last_ok = Instant::now();
        loop {
            if *shutdown.borrow() {
                break;
            }
            if replicator::writable() {
                let epoch = replicator::epoch();
                let majority = config.voters.len() / 2 + 1;
                let mut granted = 0usize;
                if config.voters.iter().any(|voter| voter == &config.advertise)
                    && replicator::cast_lease(
                        &data_root,
                        epoch,
                        &config.advertise,
                        config.ttl.as_secs(),
                    )
                    .unwrap_or(false)
                {
                    granted += 1;
                }
                let client = reqwest::Client::builder()
                    .connect_timeout(Duration::from_secs(3))
                    .timeout(Duration::from_secs(5))
                    .build();
                if let Ok(client) = client {
                    for voter in config
                        .voters
                        .iter()
                        .filter(|voter| *voter != &config.advertise)
                    {
                        match request_lease(&client, voter, &config, epoch).await {
                            Ok(true) => granted += 1,
                            Ok(false) => {}
                            Err(error) => {
                                tracing::warn!(voter, error = %error, "lease voter failed");
                            }
                        }
                    }
                }
                if granted >= majority {
                    last_ok = Instant::now();
                } else if last_ok.elapsed() >= config.ttl {
                    tracing::error!(
                        granted,
                        majority,
                        voters = config.voters.len(),
                        "primary lost quorum lease; self-demoting to replica"
                    );
                    match replicator::self_demote() {
                        Ok(role) => tracing::warn!(
                            epoch = role.epoch,
                            "primary self-demoted after losing quorum"
                        ),
                        Err((code, message)) => {
                            tracing::error!(code, message, "primary self-demotion failed");
                        }
                    }
                    last_ok = Instant::now();
                }
            } else {
                last_ok = Instant::now();
            }
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = tokio::time::sleep(config.interval) => {}
            }
        }
        tracing::info!("replication lease loop stopped");
    });
    Ok(())
}

async fn request_lease(
    client: &reqwest::Client,
    voter: &str,
    config: &Config,
    epoch: u64,
) -> Result<bool, String> {
    let path = "/v1/replication/lease";
    let body = serde_json::to_vec(&serde_json::json!({
        "epoch": epoch,
        "candidate": config.advertise,
        "ttl_seconds": config.ttl.as_secs(),
    }))
    .map_err(|error| error.to_string())?;
    let mut request = client
        .post(format!("{voter}{path}"))
        .header(reqwest::header::ACCEPT, "application/json")
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body);
    if let Some(token) = &config.token {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .map_err(|error| format!("request {path}: {error}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|error| format!("read {path} response: {error}"))?;
    if !status.is_success() {
        return Err(format!("{path} -> HTTP {}: {body}", status.as_u16()));
    }
    let payload: Value =
        serde_json::from_str(&body).map_err(|error| format!("decode {path}: {error}"))?;
    Ok(payload
        .pointer("/result/granted")
        .and_then(Value::as_bool)
        .unwrap_or(false))
}
