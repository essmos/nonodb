//! Pull-based replication from a primary or standby source.
//!
//! The source exposes its durable log through the admin replication API. A
//! replica polls status to discover the source origin, then pulls contiguous
//! transactions and applies them atomically. Writes are rejected while the
//! node role is not `primary`.

use std::{fs, sync::Arc, time::Duration};

use futures_util::StreamExt;
use nonodb_core::{replication::ReplicationTxn, Store};
use serde_json::Value;
use tokio::{io::AsyncWriteExt, sync::watch};

use crate::{
    databases::{Databases, DEFAULT_DATABASE},
    metrics,
};

#[derive(Clone, Debug)]
struct Config {
    source: String,
    token: Option<String>,
    databases: Vec<String>,
    interval: Duration,
    limit: usize,
    ca_file: Option<String>,
    cert_file: Option<String>,
    key_file: Option<String>,
    bootstrap: String,
}

impl Config {
    fn from_env() -> Result<Option<Self>, String> {
        let role = role();
        if role != "replica" && role != "standby" {
            return Ok(None);
        }
        let source = std::env::var("NONODB_REPLICATION_SOURCE")
            .ok()
            .map(|value| value.trim().trim_end_matches('/').to_string())
            .filter(|value| !value.is_empty())
            .ok_or("NONODB_REPLICATION_SOURCE is required for replica/standby role")?;
        if !source.starts_with("http://") && !source.starts_with("https://") {
            return Err("NONODB_REPLICATION_SOURCE must start with http:// or https://".into());
        }
        let databases = std::env::var("NONODB_REPLICATION_DATABASES")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|item| !item.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .filter(|items| !items.is_empty())
            .unwrap_or_else(|| vec![DEFAULT_DATABASE.to_string()]);
        let interval = std::env::var("NONODB_REPLICATION_POLL_INTERVAL")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(1));
        let limit = std::env::var("NONODB_REPLICATION_BATCH")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(500)
            .clamp(1, 1000);
        let ca_file = non_empty_env("NONODB_REPLICATION_CA_FILE");
        let cert_file = non_empty_env("NONODB_REPLICATION_CERT_FILE");
        let key_file = non_empty_env("NONODB_REPLICATION_KEY_FILE");
        if cert_file.is_some() != key_file.is_some() {
            return Err(
                "provide both NONODB_REPLICATION_CERT_FILE and NONODB_REPLICATION_KEY_FILE".into(),
            );
        }
        let bootstrap = std::env::var("NONODB_REPLICATION_BOOTSTRAP")
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .filter(|value| matches!(value.as_str(), "auto" | "always" | "never"))
            .unwrap_or_else(|| "auto".to_string());
        Ok(Some(Self {
            source,
            token: non_empty_env("NONODB_REPLICATION_SOURCE_TOKEN"),
            databases,
            interval,
            limit,
            ca_file,
            cert_file,
            key_file,
            bootstrap,
        }))
    }

    fn client(&self) -> Result<reqwest::Client, String> {
        let mut builder = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60));
        if let Some(path) = &self.ca_file {
            let pem = fs::read(path).map_err(|error| format!("read {path}: {error}"))?;
            let certs = reqwest::Certificate::from_pem_bundle(&pem)
                .map_err(|error| format!("parse CA certificate {path}: {error}"))?;
            if certs.is_empty() {
                return Err(format!("no certificates in {path}"));
            }
            for cert in certs {
                builder = builder.add_root_certificate(cert);
            }
        }
        if let (Some(cert), Some(key)) = (&self.cert_file, &self.key_file) {
            let mut pem = fs::read(cert).map_err(|error| format!("read {cert}: {error}"))?;
            pem.extend_from_slice(&fs::read(key).map_err(|error| format!("read {key}: {error}"))?);
            let identity = reqwest::Identity::from_pem(&pem)
                .map_err(|error| format!("parse client identity: {error}"))?;
            builder = builder.identity(identity);
        }
        builder
            .build()
            .map_err(|error| format!("build replication HTTP client: {error}"))
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub fn role() -> String {
    std::env::var("NONODB_REPLICATION_ROLE")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "primary".to_string())
}

/// Client writes are accepted only on the primary. Standbys receive writes
/// through the replication apply path, which bypasses the writer queue.
pub fn writable() -> bool {
    role() == "primary"
}

/// Optionally installs a source snapshot before the daemon opens its default
/// runtime. This is the only safe point to replace a live database directory.
pub async fn bootstrap_startup(data_root: &std::path::Path) -> Result<(), String> {
    let Some(config) = Config::from_env()? else {
        return Ok(());
    };
    if config.bootstrap == "never" {
        return Ok(());
    }
    let client = config.client()?;
    for database in &config.databases {
        if config.bootstrap == "auto" && !bootstrap_fresh(data_root, database).await? {
            continue;
        }
        let status = match get_json(&client, &config, database, "status", &[]).await {
            Ok(status) => status,
            Err(error) => {
                tracing::warn!(database, error = %error, "replication bootstrap status failed");
                continue;
            }
        };
        let origin = status
            .get("origin")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or_default()
            .to_string();
        if origin.is_empty() {
            continue;
        }
        match bootstrap_database(&client, &config, data_root, database, &origin).await {
            Ok(head) => {
                tracing::info!(database, origin, head, "replication snapshot installed");
            }
            Err(error) => {
                tracing::warn!(database, error = %error, "replication bootstrap failed");
                if config.bootstrap == "always" {
                    return Err(error);
                }
            }
        }
    }
    Ok(())
}

async fn bootstrap_fresh(data_root: &std::path::Path, database: &str) -> Result<bool, String> {
    let path = data_root.join(database);
    if !path.join("data.mdb").is_file() {
        return Ok(true);
    }
    let path_for_open = path.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<(bool, bool), String> {
        let store = Store::open(&path_for_open).map_err(|error| error.to_string())?;
        let applied = store
            .replication_applied_origins()
            .map_err(|error| error.to_string())?;
        let collections = store.collections().map_err(|error| error.to_string())?;
        let env = store.env().clone();
        drop(store);
        env.prepare_for_closing().wait();
        Ok((
            !applied.is_empty(),
            collections.iter().any(|name| !name.starts_with('_')),
        ))
    })
    .await
    .map_err(|error| format!("inspect existing replica data: {error}"))??;
    Ok(!result.0 && !result.1)
}

async fn bootstrap_database(
    client: &reqwest::Client,
    config: &Config,
    data_root: &std::path::Path,
    database: &str,
    origin: &str,
) -> Result<u64, String> {
    let temp = std::env::temp_dir().join(format!(
        "nonodb-replication-bootstrap-{}-{}-{}",
        std::process::id(),
        database,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0)
    ));
    tokio::fs::create_dir_all(&temp)
        .await
        .map_err(|error| format!("create bootstrap temp dir: {error}"))?;
    let data_file = temp.join("data.mdb");
    let result = async {
        let path = if database == DEFAULT_DATABASE {
            "/v1/replication/snapshot".to_string()
        } else {
            format!("/{database}/v1/replication/snapshot")
        };
        let mut request = client
            .get(format!("{}{}", config.source, path))
            .header(reqwest::header::ACCEPT, "application/octet-stream");
        if let Some(token) = &config.token {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|error| format!("request {path}: {error}"))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format!(
                "{path} -> HTTP {}: {}",
                status.as_u16(),
                body.chars().take(500).collect::<String>()
            ));
        }
        let mut file = tokio::fs::File::create(&data_file)
            .await
            .map_err(|error| format!("create snapshot file: {error}"))?;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| format!("read snapshot stream: {error}"))?;
            file.write_all(&chunk)
                .await
                .map_err(|error| format!("write snapshot file: {error}"))?;
        }
        file.flush()
            .await
            .map_err(|error| format!("flush snapshot file: {error}"))?;
        drop(file);

        let snapshot_dir = temp.clone();
        let snapshot_head = tokio::task::spawn_blocking(move || -> Result<u64, String> {
            let store = Store::open(&snapshot_dir).map_err(|error| error.to_string())?;
            let head = store
                .replication_head()
                .map_err(|error| error.to_string())?;
            let env = store.env().clone();
            drop(store);
            env.prepare_for_closing().wait();
            Ok(head)
        })
        .await
        .map_err(|error| format!("open snapshot: {error}"))??;
        if snapshot_head == 0 {
            return Err("source snapshot has no replication head".into());
        }
        install_bootstrap_snapshot(data_root, database, origin, snapshot_head, &data_file).await?;
        Ok(snapshot_head)
    }
    .await;
    let _ = tokio::fs::remove_dir_all(&temp).await;
    result
}

async fn install_bootstrap_snapshot(
    data_root: &std::path::Path,
    database: &str,
    origin: &str,
    head: u64,
    source: &std::path::Path,
) -> Result<(), String> {
    let target = data_root.join(database);
    let backup = data_root.join(format!(".bootstrap-{database}-{}", std::process::id()));
    let _ = tokio::fs::remove_dir_all(&backup).await;
    if target.exists() {
        tokio::fs::rename(&target, &backup)
            .await
            .map_err(|error| format!("move old replica database aside: {error}"))?;
    }
    tokio::fs::create_dir_all(&target)
        .await
        .map_err(|error| format!("create replica database directory: {error}"))?;
    if let Err(error) = tokio::fs::copy(source, target.join("data.mdb")).await {
        let _ = tokio::fs::remove_dir_all(&target).await;
        let _ = tokio::fs::rename(&backup, &target).await;
        return Err(format!("install snapshot data: {error}"));
    }
    let target_for_open = target.clone();
    let origin = origin.to_string();
    let result = tokio::task::spawn_blocking(move || -> Result<(), String> {
        let store = Store::open(&target_for_open).map_err(|error| error.to_string())?;
        store
            .set_replication_state(&origin, head, head)
            .map_err(|error| error.to_string())?;
        let env = store.env().clone();
        drop(store);
        env.prepare_for_closing().wait();
        Ok(())
    })
    .await
    .map_err(|error| format!("finalize snapshot: {error}"))?;
    if let Err(error) = result {
        let _ = tokio::fs::remove_dir_all(&target).await;
        let _ = tokio::fs::rename(&backup, &target).await;
        return Err(format!("finalize snapshot: {error}"));
    }
    let _ = tokio::fs::remove_dir_all(&backup).await;
    Ok(())
}

pub fn start(databases: Arc<Databases>, shutdown: watch::Receiver<bool>) -> Result<(), String> {
    let Some(config) = Config::from_env()? else {
        return Ok(());
    };
    let client = config.client()?;
    tracing::info!(
        role = %role(),
        source = %config.source,
        databases = ?config.databases,
        interval_secs = config.interval.as_secs(),
        "replication pull loop started"
    );
    tokio::spawn(async move {
        let mut shutdown = shutdown;
        loop {
            if *shutdown.borrow() {
                break;
            }
            for database in &config.databases {
                if let Err(error) = pull_database(&client, &config, &databases, database).await {
                    tracing::warn!(database, error = %error, "replication pull failed");
                }
            }
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = tokio::time::sleep(config.interval) => {}
            }
        }
        tracing::info!("replication pull loop stopped");
    });
    Ok(())
}

async fn pull_database(
    client: &reqwest::Client,
    config: &Config,
    databases: &Databases,
    database: &str,
) -> Result<(), String> {
    let runtime = match databases.get(database).await {
        Ok(runtime) => runtime,
        Err(_) => {
            databases
                .create(database)
                .await
                .map_err(|error| format!("create replica database: {}", error.message))?;
            databases
                .get(database)
                .await
                .map_err(|error| format!("open replica database: {}", error.message))?
        }
    };

    for _ in 0..100 {
        let status = get_json(client, config, database, "status", &[]).await?;
        let origin = status
            .get("origin")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or("source replication capture is disabled")?
            .to_string();
        let applied = runtime
            .store
            .replication_applied(&origin)
            .map_err(|error| error.to_string())?;
        let status_head = status.get("head").and_then(Value::as_u64).unwrap_or(0);
        runtime
            .store
            .set_replication_source_head(&origin, status_head)
            .map_err(|error| error.to_string())?;
        if applied >= status_head {
            return Ok(());
        }
        let changes = get_json(
            client,
            config,
            database,
            "changes",
            &[
                ("after", applied.to_string()),
                ("limit", config.limit.to_string()),
            ],
        )
        .await?;
        let batch = changes
            .get("changes")
            .and_then(Value::as_array)
            .ok_or("source changes response is missing `changes`")?;
        if batch.is_empty() {
            return Ok(());
        }
        let mut applied_any = false;
        for value in batch {
            let txn: ReplicationTxn = serde_json::from_value(value.clone())
                .map_err(|error| format!("decode replication transaction: {error}"))?;
            match runtime.store.apply_replication_txn(&txn) {
                Ok(applied) => {
                    if applied {
                        applied_any = true;
                        metrics::inc(&metrics::REPLICATION_APPLIED_TOTAL);
                    }
                }
                Err(error) => {
                    metrics::inc(&metrics::REPLICATION_APPLY_ERRORS);
                    return Err(format!(
                        "apply replication transaction {}: {error}",
                        txn.seq
                    ));
                }
            }
        }
        if applied_any {
            runtime.cache.invalidate();
            metrics::set(&metrics::REPLICATION_LAST_SUCCESS_UNIX, unix_now());
        }
        if batch.len() < config.limit {
            return Ok(());
        }
    }
    Err("replication catch-up exceeded 100 batches".into())
}

async fn get_json(
    client: &reqwest::Client,
    config: &Config,
    database: &str,
    action: &str,
    query: &[(&str, String)],
) -> Result<Value, String> {
    let path = if database == DEFAULT_DATABASE {
        format!("/v1/replication/{action}")
    } else {
        format!("/{database}/v1/replication/{action}")
    };
    let mut url = format!("{}{}", config.source, path);
    if !query.is_empty() {
        url.push('?');
        for (index, (key, value)) in query.iter().enumerate() {
            if index > 0 {
                url.push('&');
            }
            url.push_str(key);
            url.push('=');
            url.push_str(value);
        }
    }
    let mut request = client
        .get(url)
        .header(reqwest::header::ACCEPT, "application/json");
    if let Some(token) = &config.token {
        request = request.bearer_auth(token);
    }
    let response = request.send().await.map_err(|error| {
        metrics::inc(&metrics::REPLICATION_TRANSPORT_ERRORS);
        format!("request {path}: {error}")
    })?;
    let status = response.status();
    let body = response.text().await.map_err(|error| {
        metrics::inc(&metrics::REPLICATION_TRANSPORT_ERRORS);
        format!("read {path} response: {error}")
    })?;
    let payload: Value = serde_json::from_str(&body).map_err(|error| {
        metrics::inc(&metrics::REPLICATION_TRANSPORT_ERRORS);
        format!("decode {path} response: {error}")
    })?;
    if !status.is_success() || payload.get("ok").and_then(Value::as_bool) == Some(false) {
        metrics::inc(&metrics::REPLICATION_TRANSPORT_ERRORS);
        let code = payload
            .pointer("/error/code")
            .and_then(Value::as_str)
            .unwrap_or("replication_error");
        let message = payload
            .pointer("/error/message")
            .and_then(Value::as_str)
            .unwrap_or("replication request failed");
        return Err(format!(
            "{path} -> HTTP {} {code}: {message}",
            status.as_u16()
        ));
    }
    payload
        .get("result")
        .cloned()
        .ok_or_else(|| format!("{path} response is missing `result`"))
}
