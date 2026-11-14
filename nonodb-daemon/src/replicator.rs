//! Pull-based replication from a primary or standby source.
//!
//! The source exposes its durable log through the admin replication API. A
//! replica polls status to discover the source origin, then pulls contiguous
//! transactions and applies them atomically. Writes are rejected while the
//! node role is not `primary`.

use std::{
    fs,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use futures_util::StreamExt;
use nonodb_core::{replication::ReplicationTxn, Store};
use serde::{Deserialize, Serialize};
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedSource {
    source: String,
    #[serde(default = "default_databases")]
    databases: Vec<String>,
    #[serde(default = "default_interval")]
    interval_secs: u64,
    #[serde(default = "default_batch")]
    batch: usize,
    #[serde(default = "default_bootstrap")]
    bootstrap: String,
}

fn default_databases() -> Vec<String> {
    vec![DEFAULT_DATABASE.to_string()]
}

fn default_interval() -> u64 {
    1
}

fn default_batch() -> usize {
    500
}

fn default_bootstrap() -> String {
    "auto".to_string()
}

fn source_config_path(data_root: &std::path::Path) -> std::path::PathBuf {
    data_root.join("replication-source.json")
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let source = std::env::var("NONODB_REPLICATION_SOURCE")
            .ok()
            .map(|value| value.trim().trim_end_matches('/').to_string())
            .unwrap_or_default();
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
            .unwrap_or_else(default_databases);
        let interval = std::env::var("NONODB_REPLICATION_POLL_INTERVAL")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or_else(default_interval);
        let limit = std::env::var("NONODB_REPLICATION_BATCH")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or_else(default_batch)
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
            .unwrap_or_else(default_bootstrap);
        Ok(Self {
            source,
            token: non_empty_env("NONODB_REPLICATION_SOURCE_TOKEN"),
            databases,
            interval: Duration::from_secs(interval),
            limit,
            ca_file,
            cert_file,
            key_file,
            bootstrap,
        })
    }

    fn load(data_root: &std::path::Path) -> Result<Option<Self>, String> {
        let mut config = Self::from_env()?;
        let path = source_config_path(data_root);
        if path.is_file() {
            let raw = fs::read(&path).map_err(|error| format!("read source config: {error}"))?;
            let persisted: PersistedSource = serde_json::from_slice(&raw)
                .map_err(|error| format!("parse source config: {error}"))?;
            config.source = persisted.source.trim().trim_end_matches('/').to_string();
            config.databases = persisted.databases;
            config.interval = Duration::from_secs(persisted.interval_secs.max(1));
            config.limit = persisted.batch.clamp(1, 1000);
            if matches!(persisted.bootstrap.as_str(), "auto" | "always" | "never") {
                config.bootstrap = persisted.bootstrap;
            }
        }
        if config.source.is_empty() {
            return Ok(None);
        }
        if !config.source.starts_with("http://") && !config.source.starts_with("https://") {
            return Err("replication source must start with http:// or https://".into());
        }
        if config.databases.is_empty() {
            config.databases = default_databases();
        }
        Ok(Some(config))
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedRole {
    role: String,
    epoch: u64,
}

struct RoleCell {
    role: String,
    epoch: u64,
    path: std::path::PathBuf,
}

fn role_cell() -> &'static Mutex<RoleCell> {
    static ROLE: OnceLock<Mutex<RoleCell>> = OnceLock::new();
    ROLE.get_or_init(|| {
        Mutex::new(RoleCell {
            role: std::env::var("NONODB_REPLICATION_ROLE")
                .ok()
                .map(|value| value.trim().to_ascii_lowercase())
                .filter(|value| matches!(value.as_str(), "primary" | "replica" | "standby"))
                .unwrap_or_else(|| "primary".to_string()),
            epoch: 0,
            path: std::path::PathBuf::new(),
        })
    })
}

fn valid_role(value: &str) -> bool {
    matches!(value, "primary" | "replica" | "standby")
}

fn write_role(path: &std::path::Path, role: &str, epoch: u64) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| format!("create role state dir: {error}"))?;
    }
    let temp = path.with_extension(format!("tmp-{}", std::process::id()));
    let encoded = serde_json::to_vec_pretty(&PersistedRole {
        role: role.to_string(),
        epoch,
    })
    .map_err(|error| error.to_string())?;
    fs::write(&temp, encoded).map_err(|error| format!("write role state: {error}"))?;
    fs::rename(&temp, path).map_err(|error| format!("replace role state: {error}"))?;
    Ok(())
}

/// Loads the persisted role before any database writer starts. The role file
/// wins over `NONODB_REPLICATION_ROLE`; env seeds a brand-new installation.
pub fn init_role(data: &std::path::Path) -> Result<(), String> {
    let path = data.join("replication-role.json");
    let env_role = std::env::var("NONODB_REPLICATION_ROLE")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| valid_role(value))
        .unwrap_or_else(|| "primary".to_string());
    let state = if path.is_file() {
        let raw = fs::read(&path).map_err(|error| format!("read role state: {error}"))?;
        let persisted: PersistedRole =
            serde_json::from_slice(&raw).map_err(|error| format!("parse role state: {error}"))?;
        if !valid_role(&persisted.role) {
            return Err(format!(
                "invalid persisted replication role `{}`",
                persisted.role
            ));
        }
        persisted
    } else {
        let state = PersistedRole {
            role: env_role,
            epoch: 0,
        };
        write_role(&path, &state.role, state.epoch)?;
        state
    };
    let mut cell = role_cell()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cell.role = state.role;
    cell.epoch = state.epoch;
    cell.path = path;
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub struct RoleView {
    pub role: String,
    pub epoch: u64,
    pub read_only: bool,
}

fn role_view(cell: &RoleCell) -> RoleView {
    RoleView {
        role: cell.role.clone(),
        epoch: cell.epoch,
        read_only: cell.role != "primary",
    }
}

fn change_role(next: &str, expected_epoch: u64) -> Result<RoleView, (String, String)> {
    if !valid_role(next) {
        return Err((
            "invalid_role".to_string(),
            format!("role must be primary, replica, or standby, got `{next}`"),
        ));
    }
    let mut cell = role_cell()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if cell.role == next {
        return Ok(role_view(&cell));
    }
    if expected_epoch != cell.epoch {
        return Err((
            "epoch_mismatch".to_string(),
            format!(
                "expected epoch {expected_epoch}, current epoch is {}",
                cell.epoch
            ),
        ));
    }
    let previous = cell.role.clone();
    let epoch = cell.epoch.saturating_add(1);
    if let Err(error) = write_role(&cell.path, next, epoch) {
        cell.role = previous;
        return Err(("role_state_error".to_string(), error));
    }
    cell.role = next.to_string();
    cell.epoch = epoch;
    Ok(role_view(&cell))
}

pub fn promote(expected_epoch: u64) -> Result<RoleView, (String, String)> {
    change_role("primary", expected_epoch)
}

pub fn demote(expected_epoch: u64, role: &str) -> Result<RoleView, (String, String)> {
    if role == "primary" {
        return Err((
            "invalid_role".to_string(),
            "demote requires replica or standby".to_string(),
        ));
    }
    change_role(role, expected_epoch)
}

pub fn epoch() -> u64 {
    role_cell()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .epoch
}

#[derive(Clone, Debug, Default)]
struct RuntimeStatus {
    source: Option<String>,
    databases: Vec<String>,
    interval_secs: u64,
    bootstrap: String,
    last_success_unix: u64,
    last_error_unix: u64,
    last_transport_error: Option<String>,
    last_apply_error: Option<String>,
}

fn runtime_status() -> &'static Mutex<RuntimeStatus> {
    static STATUS: OnceLock<Mutex<RuntimeStatus>> = OnceLock::new();
    STATUS.get_or_init(|| Mutex::new(RuntimeStatus::default()))
}

fn record_config(config: &Config) {
    let mut status = runtime_status()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    status.source = Some(config.source.clone());
    status.databases = config.databases.clone();
    status.interval_secs = config.interval.as_secs();
    status.bootstrap = config.bootstrap.clone();
}

fn record_source(source: &PersistedSource) {
    let mut status = runtime_status()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    status.source = Some(source.source.clone());
    status.databases = source.databases.clone();
    status.interval_secs = source.interval_secs;
    status.bootstrap = source.bootstrap.clone();
}

fn record_success() {
    let mut status = runtime_status()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    status.last_success_unix = unix_now();
}

fn record_transport_error(message: &str) {
    let mut status = runtime_status()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    status.last_error_unix = unix_now();
    status.last_transport_error = Some(message.chars().take(500).collect());
}

fn record_apply_error(message: &str) {
    let mut status = runtime_status()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    status.last_error_unix = unix_now();
    status.last_apply_error = Some(message.chars().take(500).collect());
}

/// Returns the current replication runtime view for the admin status API/GUI.
pub fn status_json() -> Value {
    let status = runtime_status()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    serde_json::json!({
        "role": role(),
        "epoch": epoch(),
        "read_only": !writable(),
        "source": status.source,
        "databases": status.databases,
        "interval_secs": status.interval_secs,
        "bootstrap": status.bootstrap,
        "last_success_unix": status.last_success_unix,
        "last_error_unix": status.last_error_unix,
        "last_transport_error": status.last_transport_error,
        "last_apply_error": status.last_apply_error,
    })
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
    role_cell()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .role
        .clone()
}

/// Client writes are accepted only on the primary. Standbys receive writes
/// through the replication apply path, which bypasses the writer queue.
pub fn writable() -> bool {
    role() == "primary"
}

/// Persists a new pull source. The active loop reloads it on the next cycle.
pub fn set_source(
    data_root: &std::path::Path,
    source: &str,
    databases: &[String],
    interval_secs: u64,
    batch: usize,
    bootstrap: &str,
) -> Result<Value, String> {
    let source = source.trim().trim_end_matches('/');
    if !source.starts_with("http://") && !source.starts_with("https://") {
        return Err("source must start with http:// or https://".into());
    }
    let databases = if databases.is_empty() {
        default_databases()
    } else {
        databases.to_vec()
    };
    if !matches!(bootstrap, "auto" | "always" | "never") {
        return Err("bootstrap must be auto, always, or never".into());
    }
    let persisted = PersistedSource {
        source: source.to_string(),
        databases,
        interval_secs: interval_secs.max(1),
        batch: batch.clamp(1, 1000),
        bootstrap: bootstrap.to_string(),
    };
    let encoded = serde_json::to_vec_pretty(&persisted).map_err(|error| error.to_string())?;
    let path = source_config_path(data_root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| format!("create data dir: {error}"))?;
    }
    let temp = path.with_extension(format!("tmp-{}", std::process::id()));
    fs::write(&temp, encoded).map_err(|error| format!("write source config: {error}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temp, fs::Permissions::from_mode(0o600))
            .map_err(|error| format!("chmod source config: {error}"))?;
    }
    fs::rename(&temp, &path).map_err(|error| format!("replace source config: {error}"))?;
    record_source(&persisted);
    Ok(serde_json::json!({
        "source": persisted.source,
        "databases": persisted.databases,
        "interval_secs": persisted.interval_secs,
        "batch": persisted.batch,
        "bootstrap": persisted.bootstrap,
    }))
}

/// Optionally installs a source snapshot before the daemon opens its default
/// runtime. This is the only safe point to replace a live database directory.
pub async fn bootstrap_startup(data_root: &std::path::Path) -> Result<(), String> {
    if !matches!(role().as_str(), "replica" | "standby") {
        return Ok(());
    }
    let Some(config) = Config::load(data_root)? else {
        return Ok(());
    };
    if config.bootstrap == "never" {
        return Ok(());
    }
    record_config(&config);
    let client = config.client()?;
    for database in &config.databases {
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
        let source_epoch = status.get("epoch").and_then(Value::as_u64).unwrap_or(0);
        let head = status.get("head").and_then(Value::as_u64).unwrap_or(0);
        if origin.is_empty() || head == 0 {
            continue;
        }
        if config.bootstrap == "auto" {
            let (fresh, applied) = bootstrap_state(data_root, database, &origin).await?;
            if !fresh {
                if applied >= head {
                    continue;
                }
                let changes = get_json(
                    &client,
                    &config,
                    database,
                    "changes",
                    &[("after", applied.to_string()), ("limit", "1".into())],
                )
                .await?;
                let first = changes
                    .get("changes")
                    .and_then(Value::as_array)
                    .and_then(|items| items.first())
                    .and_then(|txn| txn.get("seq"))
                    .and_then(Value::as_u64);
                if first == Some(applied + 1) {
                    continue;
                }
                tracing::warn!(
                    database,
                    applied,
                    head,
                    "replication log gap detected; resyncing from snapshot"
                );
            }
        }
        match bootstrap_database(&client, &config, data_root, database, &origin, source_epoch).await
        {
            Ok(head) => {
                record_success();
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

async fn bootstrap_state(
    data_root: &std::path::Path,
    database: &str,
    origin: &str,
) -> Result<(bool, u64), String> {
    let path = data_root.join(database);
    if !path.join("data.mdb").is_file() {
        return Ok((true, 0));
    }
    let path_for_open = path.clone();
    let origin = origin.to_string();
    tokio::task::spawn_blocking(move || -> Result<(bool, u64), String> {
        let store = Store::open(&path_for_open).map_err(|error| error.to_string())?;
        let origins = store
            .replication_applied_origins()
            .map_err(|error| error.to_string())?;
        let collections = store.collections().map_err(|error| error.to_string())?;
        let applied = store
            .replication_applied(&origin)
            .map_err(|error| error.to_string())?;
        let env = store.env().clone();
        drop(store);
        env.prepare_for_closing().wait();
        let fresh = origins.is_empty() && !collections.iter().any(|name| !name.starts_with('_'));
        Ok((fresh, applied))
    })
    .await
    .map_err(|error| format!("inspect existing replica data: {error}"))?
}

async fn bootstrap_database(
    client: &reqwest::Client,
    config: &Config,
    data_root: &std::path::Path,
    database: &str,
    origin: &str,
    source_epoch: u64,
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
        install_bootstrap_snapshot(
            data_root,
            database,
            origin,
            snapshot_head,
            source_epoch,
            &data_file,
        )
        .await?;
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
    epoch: u64,
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
            .set_replication_state(&origin, head, head, epoch)
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

pub fn start(
    databases: Arc<Databases>,
    data_root: std::path::PathBuf,
    shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    tokio::spawn(async move {
        let mut shutdown = shutdown;
        loop {
            if *shutdown.borrow() {
                break;
            }
            let config = match Config::load(&data_root) {
                Ok(Some(config)) => config,
                Ok(None) => {
                    tokio::select! {
                        _ = shutdown.changed() => break,
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                    continue;
                }
                Err(error) => {
                    tracing::warn!(error = %error, "replication source config reload failed");
                    tokio::select! {
                        _ = shutdown.changed() => break,
                        _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                    }
                    continue;
                }
            };
            record_config(&config);
            if !writable() {
                let client = match config.client() {
                    Ok(client) => client,
                    Err(error) => {
                        tracing::warn!(error = %error, "replication client config failed");
                        tokio::select! {
                            _ = shutdown.changed() => break,
                            _ = tokio::time::sleep(config.interval) => {}
                        }
                        continue;
                    }
                };
                for database in &config.databases {
                    if let Err(error) = pull_database(&client, &config, &databases, database).await
                    {
                        tracing::warn!(database, error = %error, "replication pull failed");
                    }
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
        if writable() {
            return Ok(());
        }
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
            if writable() {
                return Ok(());
            }
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
                    let message = format!("apply replication transaction {}: {error}", txn.seq);
                    record_apply_error(&message);
                    return Err(message);
                }
            }
        }
        if applied_any {
            runtime.cache.invalidate();
            metrics::set(&metrics::REPLICATION_LAST_SUCCESS_UNIX, unix_now());
            record_success();
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
        let message = format!("request {path}: {error}");
        record_transport_error(&message);
        message
    })?;
    let status = response.status();
    let body = response.text().await.map_err(|error| {
        metrics::inc(&metrics::REPLICATION_TRANSPORT_ERRORS);
        let message = format!("read {path} response: {error}");
        record_transport_error(&message);
        message
    })?;
    let payload: Value = serde_json::from_str(&body).map_err(|error| {
        metrics::inc(&metrics::REPLICATION_TRANSPORT_ERRORS);
        let message = format!("decode {path} response: {error}");
        record_transport_error(&message);
        message
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
