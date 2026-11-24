//! Pull-based replication from a primary or standby source.
//!
//! The source exposes its durable log through the admin replication API. A
//! replica polls status to discover the source origin, then pulls contiguous
//! transactions and applies them atomically. Writes are rejected while the
//! node role is not `primary`.

use std::{
    fs,
    net::ToSocketAddrs,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};

use futures_util::StreamExt;
use nonodb_core::{replication::ReplicationTxn, Store};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use tokio::{io::AsyncWriteExt, sync::watch};

use crate::{
    databases::{Databases, DEFAULT_DATABASE},
    metrics,
};

static PROMOTION_READY: AtomicBool = AtomicBool::new(false);
static LEASE_VALID: AtomicBool = AtomicBool::new(false);
static RESTART_REQUESTED: AtomicBool = AtomicBool::new(false);
static CLUSTER_PEER_TOKEN: OnceLock<std::sync::RwLock<Option<String>>> = OnceLock::new();
/// Request a controlled process restart after the current request finishes.
///
/// A join needs to run snapshot bootstrap before the database is opened, so the
/// daemon exits with a restart status and lets its supervisor start it again.
pub fn request_restart() {
    RESTART_REQUESTED.store(true, Ordering::Relaxed);
}

pub fn restart_requested() -> bool {
    RESTART_REQUESTED.load(Ordering::Relaxed)
}

fn cluster_peer_slot() -> &'static std::sync::RwLock<Option<String>> {
    CLUSTER_PEER_TOKEN.get_or_init(|| std::sync::RwLock::new(None))
}

static SOURCE_CONFIG_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn source_config_lock() -> &'static Mutex<()> {
    SOURCE_CONFIG_LOCK.get_or_init(|| Mutex::new(()))
}

/// Stores the URL a replica advertises in its ACK. This is node-local: a new
/// primary learns the roster again from fresh ACKs after failover.
pub fn record_replica_url(node: &str, url: &str) -> Result<(), String> {
    let node = node.trim();
    if node.is_empty() || node.len() > 128 {
        return Err("replica node id must be 1..=128 characters".into());
    }
    let url = url.trim().trim_end_matches('/');
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err("replica advertise URL must start with http:// or https://".into());
    }
    if url.len() > 2048 || url.chars().any(char::is_whitespace) {
        return Err("replica advertise URL is invalid".into());
    }
    let mut urls = crate::node_state::get_json::<std::collections::HashMap<String, String>>(
        "replication:replica_urls",
    )?
    .unwrap_or_default();
    urls.insert(node.to_string(), url.to_string());
    crate::node_state::put_json("replication:replica_urls", &urls)
}

pub fn forget_replica_url(node: &str) -> Result<(), String> {
    let mut urls = crate::node_state::get_json::<std::collections::HashMap<String, String>>(
        "replication:replica_urls",
    )?
    .unwrap_or_default();
    if urls.remove(node).is_some() {
        crate::node_state::put_json("replication:replica_urls", &urls)?;
    }
    Ok(())
}

pub fn replica_url(node: &str) -> Option<String> {
    crate::node_state::get_json::<std::collections::HashMap<String, String>>(
        "replication:replica_urls",
    )
    .ok()
    .flatten()
    .and_then(|urls| urls.get(node).cloned())
}

const BLOCKED_REPLICAS_KEY: &str = "replication:blocked_replicas";

fn blocked_replicas() -> std::collections::HashSet<String> {
    crate::node_state::get_json::<Vec<String>>(BLOCKED_REPLICAS_KEY)
        .ok()
        .flatten()
        .unwrap_or_default()
        .into_iter()
        .collect()
}

pub fn replica_blocked(node: &str) -> bool {
    blocked_replicas().contains(node)
}

pub fn block_replica(node: &str) -> Result<(), String> {
    let mut blocked = blocked_replicas();
    if blocked.insert(node.to_string()) {
        let mut values: Vec<_> = blocked.into_iter().collect();
        values.sort();
        crate::node_state::put_json(BLOCKED_REPLICAS_KEY, &values)?;
    }
    Ok(())
}

pub fn unblock_replica(node: &str) -> Result<(), String> {
    let mut blocked = blocked_replicas();
    if blocked.remove(node) {
        let mut values: Vec<_> = blocked.into_iter().collect();
        values.sort();
        crate::node_state::put_json(BLOCKED_REPLICAS_KEY, &values)?;
    }
    Ok(())
}

const PAUSED_KEY: &str = "replication:paused";

pub fn paused() -> bool {
    crate::node_state::get_json::<bool>(PAUSED_KEY)
        .ok()
        .flatten()
        .unwrap_or(false)
}

pub fn set_paused(value: bool) -> Result<(), String> {
    crate::node_state::put_json(PAUSED_KEY, &value)
}

/// Stops pulling and removes the source configuration. The node remains
/// read-only until explicitly promoted.
pub fn disconnect(data_root: &std::path::Path) -> Result<(), String> {
    set_paused(true)?;
    crate::node_state::remove("replication:source")?;
    clear_source_status();
    let _ = data_root;
    Ok(())
}

/// Best-effort URL for the replica's own HTTP API. Explicit
/// `NONODB_REPLICATION_ADVERTISE_URL` always wins. Otherwise use the local
/// interface that routes to the primary plus the configured TCP listen port.
/// This works on a flat LAN but cannot guess NAT/port-forward mappings.
fn inferred_advertise_url(config: &Config) -> Option<String> {
    if let Some(url) = non_empty_env("NONODB_REPLICATION_ADVERTISE_URL") {
        return Some(url.trim().trim_end_matches('/').to_string());
    }
    let tcp = crate::server_info::snapshot()
        .tcp
        .map(|listener| listener.bound)
        .or_else(|| {
            std::env::var("NONODB_TCP")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })?;
    let tls = std::env::var_os("NONODB_TLS_CERT").is_some()
        && std::env::var_os("NONODB_TLS_KEY").is_some();
    inferred_advertise_url_for(&config.source, &tcp, tls)
}

fn inferred_advertise_url_for(source_url: &str, tcp: &str, tls: bool) -> Option<String> {
    let (host, port) = tcp.rsplit_once(':')?;
    let port = port.parse::<u16>().ok().filter(|port| *port > 0)?;
    let host = host.trim_matches(['[', ']']);
    if matches!(host, "127.0.0.1" | "localhost" | "::1") {
        return None;
    }
    let host = if host.is_empty() || matches!(host, "0.0.0.0" | "::") {
        let source = source_url
            .strip_prefix("http://")
            .or_else(|| source_url.strip_prefix("https://"))?;
        let authority = source.split('/').next()?;
        let default_port = if source_url.starts_with("https://") {
            443
        } else {
            80
        };
        let (source_host, source_port) = if let Some(rest) = authority.strip_prefix('[') {
            match rest.split_once("]:") {
                Some((host, port)) => (host.to_string(), port.parse::<u16>().ok()?),
                None => (rest.trim_end_matches(']').to_string(), default_port),
            }
        } else {
            match authority.rsplit_once(':') {
                Some((host, port)) => (host.to_string(), port.parse::<u16>().ok()?),
                None => (authority.to_string(), default_port),
            }
        };
        let target = format!("{source_host}:{source_port}")
            .to_socket_addrs()
            .ok()?
            .find(|addr| addr.is_ipv4())?;
        let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
        socket.connect(target).ok()?;
        let ip = socket.local_addr().ok()?.ip();
        if ip.is_loopback() || ip.is_unspecified() {
            return None;
        }
        ip.to_string()
    } else {
        host.to_string()
    };
    let scheme = if tls { "https" } else { "http" };
    let host = if host.contains(':') {
        format!("[{host}]")
    } else {
        host
    };
    Some(format!("{scheme}://{host}:{port}"))
}

pub fn set_cluster_peer_token(token: Option<String>) {
    *cluster_peer_slot()
        .write()
        .unwrap_or_else(|p| p.into_inner()) = token;
}

fn cluster_peer_token() -> Option<String> {
    cluster_peer_slot()
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
}

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
    restart_on_gap: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedSource {
    source: String,
    #[serde(default)]
    token: Option<String>,
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

fn read_state<T: DeserializeOwned + Serialize>(
    key: &str,
    legacy_name: &str,
    data_root: &std::path::Path,
) -> Result<Option<T>, String> {
    if let Some(value) = crate::node_state::get_json(key)? {
        return Ok(Some(value));
    }
    let path = crate::node_state::legacy_path(data_root, legacy_name);
    if !path.is_file() {
        return Ok(None);
    }
    let raw = fs::read(&path).map_err(|error| format!("read legacy {legacy_name}: {error}"))?;
    let value: T = serde_json::from_slice(&raw)
        .map_err(|error| format!("parse legacy {legacy_name}: {error}"))?;
    crate::node_state::put_json(key, &value)?;
    let _ = fs::remove_file(path);
    Ok(Some(value))
}

fn write_state<T: Serialize + ?Sized>(key: &str, value: &T) -> Result<(), String> {
    crate::node_state::put_json(key, value)
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
        let restart_on_gap = std::env::var("NONODB_REPLICATION_EXIT_ON_GAP")
            .ok()
            .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"));
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
            restart_on_gap,
        })
    }

    fn load(data_root: &std::path::Path) -> Result<Option<Self>, String> {
        let mut config = Self::from_env()?;
        if let Some(persisted) = read_state::<PersistedSource>(
            "replication:source",
            "replication-source.json",
            data_root,
        )? {
            config.source = persisted.source.trim().trim_end_matches('/').to_string();
            if persisted
                .token
                .as_deref()
                .is_some_and(|token| !token.trim().is_empty())
            {
                config.token = persisted.token;
            }
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
            if let Err(error) =
                metrics::record_certificate_expiry("replication", std::path::Path::new(cert))
            {
                tracing::warn!(%error, "replication certificate expiry metric unavailable");
            }
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
    #[serde(default)]
    successor: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct PersistedVote {
    epoch: u64,
    #[serde(default)]
    candidate: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct PersistedLease {
    epoch: u64,
    #[serde(default)]
    candidate: Option<String>,
    expires_at_ms: u64,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

/// Records a lease after a successful vote. The lease is advisory fencing:
/// voters only grant renewals to the same candidate/epoch until a higher epoch
/// wins a new vote.
pub fn cast_lease(
    data_root: &std::path::Path,
    epoch: u64,
    candidate: &str,
    ttl_seconds: u64,
) -> Result<bool, String> {
    if !cast_vote(data_root, epoch, candidate)? {
        return Ok(false);
    }
    write_state(
        "replication:lease",
        &PersistedLease {
            epoch,
            candidate: Some(candidate.to_string()),
            expires_at_ms: now_ms().saturating_add(ttl_seconds.max(1) * 1000),
        },
    )?;
    Ok(true)
}

fn load_vote(data_root: &std::path::Path) -> Result<PersistedVote, String> {
    Ok(read_state("replication:vote", "replication-vote.json", data_root)?.unwrap_or_default())
}

pub fn cast_vote(data_root: &std::path::Path, epoch: u64, candidate: &str) -> Result<bool, String> {
    if !candidate.starts_with("http://") && !candidate.starts_with("https://") {
        return Err("candidate must start with http:// or https://".into());
    }
    let current = load_vote(data_root)?;
    if epoch < current.epoch {
        return Ok(false);
    }
    if epoch == current.epoch
        && current
            .candidate
            .as_deref()
            .is_some_and(|value| value != candidate)
    {
        return Ok(false);
    }
    write_state(
        "replication:vote",
        &PersistedVote {
            epoch,
            candidate: Some(candidate.to_string()),
        },
    )?;
    Ok(true)
}

pub(crate) fn voter_urls() -> Vec<String> {
    std::env::var("NONODB_REPLICATION_VOTERS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .map(|item| item.trim_end_matches('/').to_string())
                .filter(|item| !item.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn follower_urls() -> Vec<String> {
    std::env::var("NONODB_REPLICATION_FOLLOWERS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .map(|item| item.trim_end_matches('/').to_string())
                .filter(|item| !item.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

/// Stable addresses used to rediscover the current primary after a source
/// address becomes unreachable. Seeds are configured with
/// `NONODB_REPLICATION_SEEDS`; voters and followers are also queried. In
/// production these should be DNS names or a load balancer, not fixed IPs.
pub(crate) fn discovery_urls() -> Vec<String> {
    let mut urls = std::env::var("NONODB_REPLICATION_SEEDS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .map(|item| item.trim_end_matches('/').to_string())
                .filter(|item| !item.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    urls.extend(voter_urls());
    urls.extend(follower_urls());
    urls.sort();
    urls.dedup();
    urls
}

pub(crate) fn voter_token() -> Option<String> {
    non_empty_env("NONODB_REPLICATION_VOTER_TOKEN").or_else(cluster_peer_token)
}

struct RoleCell {
    role: String,
    epoch: u64,
    successor: Option<String>,
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
            successor: None,
        })
    })
}

fn valid_role(value: &str) -> bool {
    matches!(value, "primary" | "replica" | "standby")
}

fn write_role(role: &str, epoch: u64, successor: Option<&str>) -> Result<(), String> {
    write_state(
        "replication:role",
        &PersistedRole {
            role: role.to_string(),
            epoch,
            successor: successor.map(str::to_string),
        },
    )
}

/// Loads the persisted role before any database writer starts. The role file
/// wins over `NONODB_REPLICATION_ROLE`; env seeds a brand-new installation.
pub fn init_role(data: &std::path::Path) -> Result<(), String> {
    let env_role = std::env::var("NONODB_REPLICATION_ROLE")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| valid_role(value))
        .unwrap_or_else(|| "primary".to_string());
    let state: PersistedRole =
        match read_state::<PersistedRole>("replication:role", "replication-role.json", data)? {
            Some(persisted) => {
                if !valid_role(&persisted.role) {
                    return Err(format!(
                        "invalid persisted replication role `{}`",
                        persisted.role
                    ));
                }
                persisted
            }
            None => {
                let state = PersistedRole {
                    role: env_role,
                    epoch: 0,
                    successor: None,
                };
                write_role(&state.role, state.epoch, None)?;
                state
            }
        };
    let mut cell = role_cell()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cell.role = state.role;
    cell.epoch = state.epoch;
    cell.successor = state.successor;
    Ok(())
}
#[derive(Debug, Clone, Serialize)]
pub struct RoleView {
    pub role: String,
    pub epoch: u64,
    pub read_only: bool,
    pub successor: Option<String>,
}

fn role_view(cell: &RoleCell) -> RoleView {
    RoleView {
        role: cell.role.clone(),
        epoch: cell.epoch,
        read_only: cell.role != "primary",
        successor: cell.successor.clone(),
    }
}

fn change_role(
    next: &str,
    expected_epoch: u64,
    successor: Option<&str>,
) -> Result<RoleView, (String, String)> {
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
    let previous_successor = cell.successor.clone();
    let epoch = cell.epoch.saturating_add(1);
    let successor = if next == "primary" {
        None
    } else {
        successor.map(str::to_string)
    };
    if let Err(error) = write_role(next, epoch, successor.as_deref()) {
        cell.role = previous;
        cell.successor = previous_successor;
        return Err(("role_state_error".to_string(), error));
    }
    cell.role = next.to_string();
    cell.epoch = epoch;
    cell.successor = successor;
    if next == "primary" {
        set_lease_valid(false);
        clear_replication_errors();
    }
    Ok(role_view(&cell))
}

#[derive(Debug, Clone)]
pub struct SourceHealth {
    pub role: String,
    pub successor: Option<String>,
}

/// Reads the configured source status for the auto-election worker.
pub async fn source_health(data_root: &std::path::Path) -> Result<Option<SourceHealth>, String> {
    let Some(config) = Config::load(data_root)? else {
        return Ok(None);
    };
    let client = config.client()?;
    let status = get_json(&client, &config, DEFAULT_DATABASE, "status", &[]).await?;
    Ok(Some(SourceHealth {
        role: status
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        successor: status
            .get("successor")
            .and_then(Value::as_str)
            .map(str::to_string),
    }))
}

async fn acquire_quorum(
    data_root: &std::path::Path,
    expected_epoch: u64,
    advertise: &str,
) -> Result<(), (String, String)> {
    let voters = voter_urls();
    if voters.is_empty() {
        return Ok(());
    }
    let proposal = expected_epoch.saturating_add(1);
    let majority = voters.len() / 2 + 1;
    let mut granted = 0usize;
    if voters.iter().any(|voter| voter == advertise)
        && cast_vote(data_root, proposal, advertise)
            .map_err(|error| ("quorum_vote".to_string(), error))?
    {
        granted += 1;
    }
    let token = voter_token();
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|error| ("quorum_vote".to_string(), error.to_string()))?;
    for voter in voters.iter().filter(|voter| *voter != advertise) {
        let path = "/v1/replication/vote";
        let body = serde_json::to_vec(&serde_json::json!({
            "epoch": proposal,
            "candidate": advertise,
        }))
        .map_err(|error| ("quorum_vote".to_string(), error.to_string()))?;
        let mut request = client
            .post(format!("{voter}{path}"))
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body);
        if let Some(token) = &token {
            request = request.bearer_auth(token);
        }
        match request.send().await {
            Ok(response) if response.status().is_success() => {
                let body = response.text().await.unwrap_or_default();
                let granted_vote = serde_json::from_str::<Value>(&body)
                    .ok()
                    .and_then(|payload| payload.pointer("/result/granted").and_then(Value::as_bool))
                    .unwrap_or(false);
                if granted_vote {
                    granted += 1;
                }
            }
            Ok(response) => {
                tracing::warn!(
                    voter,
                    status = response.status().as_u16(),
                    "quorum voter rejected request"
                );
            }
            Err(error) => {
                tracing::warn!(voter, error = %error, "quorum voter unreachable");
            }
        }
    }
    if granted < majority {
        return Err((
            "quorum_not_reached".to_string(),
            format!(
                "promotion needs {majority} votes, received {granted} from {} voters",
                voters.len()
            ),
        ));
    }
    Ok(())
}

/// Pushes the new primary URL to every configured voter.
async fn propagate_source(data_root: &std::path::Path, advertise: &str) {
    let mut targets = voter_urls();
    for follower in follower_urls() {
        if !targets.contains(&follower) {
            targets.push(follower);
        }
    }
    if targets.is_empty() {
        return;
    }
    let config = match Config::load(data_root) {
        Ok(Some(config)) => config,
        _ => return,
    };
    let token = voter_token();
    let client = match reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            tracing::warn!(error = %error, "source propagation client failed");
            return;
        }
    };
    for voter in targets.iter().filter(|voter| *voter != advertise) {
        let path = "/v1/replication/source";
        let body = serde_json::to_vec(&serde_json::json!({
            "source": advertise,
            "databases": config.databases,
            "interval_secs": config.interval.as_secs(),
            "batch": config.limit,
            "bootstrap": config.bootstrap,
        }));
        let body = match body {
            Ok(body) => body,
            Err(error) => {
                tracing::warn!(error = %error, "source propagation body failed");
                return;
            }
        };
        let mut request = client
            .put(format!("{voter}{path}"))
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body);
        if let Some(token) = &token {
            request = request.bearer_auth(token);
        }
        match request.send().await {
            Ok(response) if response.status().is_success() => {
                tracing::info!(voter, source = advertise, "replication source propagated");
            }
            Ok(response) => {
                tracing::warn!(
                    voter,
                    status = response.status().as_u16(),
                    "replication source propagation rejected"
                );
            }
            Err(error) => {
                tracing::warn!(voter, error = %error, "replication source propagation failed");
            }
        }
    }
}

/// Promotes this node after attempting to demote the current source and point
/// it at this node's advertised replication URL.
pub async fn promote_with_fence(
    data_root: &std::path::Path,
    expected_epoch: u64,
    force: bool,
) -> Result<RoleView, (String, String)> {
    let advertise = non_empty_env("NONODB_REPLICATION_ADVERTISE_URL")
        .map(|value| value.trim().trim_end_matches('/').to_string());
    if let Some(advertise) = advertise.clone() {
        if !advertise.starts_with("http://") && !advertise.starts_with("https://") {
            return Err((
                "invalid_advertise_url".to_string(),
                "NONODB_REPLICATION_ADVERTISE_URL must start with http:// or https://".to_string(),
            ));
        }
        acquire_quorum(data_root, expected_epoch, &advertise).await?;
        match Config::load(data_root) {
            Ok(Some(config)) if config.source != advertise => {
                let client = config
                    .client()
                    .map_err(|error| ("replication_client".to_string(), error))?;
                let status = get_json(&client, &config, DEFAULT_DATABASE, "status", &[]).await;
                match status {
                    Ok(status) => {
                        let source_role = status.get("role").and_then(Value::as_str).unwrap_or("");
                        if source_role == "primary" {
                            let source_epoch =
                                status.get("epoch").and_then(Value::as_u64).unwrap_or(0);
                            let body = serde_json::json!({
                                "expected_epoch": source_epoch,
                                "role": "replica",
                                "successor": advertise,
                            });
                            if let Err(error) =
                                post_json(&client, &config, DEFAULT_DATABASE, "demote", &body).await
                            {
                                if !force {
                                    return Err(("source_fence_failed".to_string(), error));
                                }
                                tracing::warn!(error = %error, "source fence failed; forcing promotion");
                            }
                        }
                    }
                    Err(error) => {
                        if !force {
                            return Err(("source_fence_failed".to_string(), error));
                        }
                        tracing::warn!(error = %error, "source fence status failed; forcing promotion");
                    }
                }
            }
            Ok(_) => {}
            Err(error) => {
                if !force {
                    return Err(("source_fence_failed".to_string(), error));
                }
            }
        }
    }
    let role = promote(expected_epoch)?;
    // The promotion path already acquired a voter majority (or has no voters).
    // Treat that as the initial lease so clients can use the new writer at once;
    // the lease loop extends it or fences this node after the TTL.
    set_lease_valid(true);
    if let Some(advertise) = advertise {
        propagate_source(data_root, &advertise).await;
    }
    Ok(role)
}

pub fn promote(expected_epoch: u64) -> Result<RoleView, (String, String)> {
    change_role("primary", expected_epoch, None)
}

pub fn demote(
    expected_epoch: u64,
    role: &str,
    successor: Option<&str>,
) -> Result<RoleView, (String, String)> {
    if role == "primary" {
        return Err((
            "invalid_role".to_string(),
            "demote requires replica or standby".to_string(),
        ));
    }
    if let Some(successor) = successor {
        if !successor.starts_with("http://") && !successor.starts_with("https://") {
            return Err((
                "invalid_successor".to_string(),
                "successor must start with http:// or https://".to_string(),
            ));
        }
    }
    change_role(role, expected_epoch, successor)
}

/// Best-known write endpoint for load balancers and clients.
pub fn write_url(data_root: &std::path::Path) -> Result<Option<String>, String> {
    if writable() {
        return Ok(advertise_url());
    }
    if let Some(successor) = successor() {
        return Ok(Some(successor));
    }
    Ok(Config::load(data_root)?.map(|config| config.source))
}

pub(crate) fn advertise_url() -> Option<String> {
    non_empty_env("NONODB_REPLICATION_ADVERTISE_URL")
        .map(|value| value.trim().trim_end_matches('/').to_string())
}

/// Locally fences a primary that has lost its quorum lease.
pub fn self_demote() -> Result<RoleView, (String, String)> {
    set_lease_valid(false);
    let epoch = epoch();
    change_role("replica", epoch, None)
}

pub fn successor() -> Option<String> {
    role_cell()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .successor
        .clone()
}

pub fn epoch() -> u64 {
    role_cell()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .epoch
}

pub fn promotion_ready() -> bool {
    role_is_primary() || (PROMOTION_READY.load(Ordering::Relaxed) && crate::control_plane::ready())
}

/// Role alone, without the quorum-lease gate. Used by role owners such as the
/// lease loop and election loop; request writers must use [`writable`].
pub fn role_is_primary() -> bool {
    role() == "primary"
}

pub fn lease_valid() -> bool {
    voter_urls().is_empty() || LEASE_VALID.load(Ordering::Relaxed)
}

pub fn set_lease_valid(valid: bool) {
    LEASE_VALID.store(valid, Ordering::Relaxed);
}

/// Advances the local role epoch after observing a newer epoch from the
/// replication stream. Replicas must remember the source epoch so a later
/// promotion proposes an epoch higher than every vote already cast.
pub fn observe_epoch(epoch: u64) -> Result<(), String> {
    if epoch == 0 {
        return Ok(());
    }
    let mut cell = role_cell()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if epoch <= cell.epoch {
        return Ok(());
    }
    write_role(&cell.role, epoch, cell.successor.as_deref())?;
    cell.epoch = epoch;
    Ok(())
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
    status.last_success_unix = 0;
    status.last_error_unix = 0;
    status.last_transport_error = None;
    status.last_apply_error = None;
}

fn clear_source_status() {
    let mut status = runtime_status()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    status.source = None;
    status.databases.clear();
    status.interval_secs = 0;
    status.bootstrap.clear();
    status.last_success_unix = 0;
    status.last_error_unix = 0;
    status.last_transport_error = None;
    status.last_apply_error = None;
}

fn record_success() {
    let mut status = runtime_status()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    status.last_success_unix = unix_now();
    status.last_error_unix = 0;
    status.last_transport_error = None;
    status.last_apply_error = None;
}

fn clear_replication_errors() {
    let mut status = runtime_status()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    status.last_error_unix = 0;
    status.last_transport_error = None;
    status.last_apply_error = None;
}

fn record_transport_error(message: &str) {
    let mut status = runtime_status()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    status.last_error_unix = unix_now();
    status.last_transport_error = Some(message.chars().take(500).collect());
}

fn record_apply_error(message: &str) {
    PROMOTION_READY.store(false, Ordering::Relaxed);
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
        "lease_valid": lease_valid(),
        "promotion_ready": promotion_ready(),
        "paused": paused(),
        "successor": role_cell()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .successor
            .clone(),
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

pub(crate) fn node_id() -> String {
    non_empty_env("NONODB_REPLICATION_NODE_ID")
        .or_else(|| non_empty_env("NONODB_NODE_ID"))
        .or_else(|| non_empty_env("NONODB_REPLICATION_ORIGIN"))
        .unwrap_or_else(|| "local".to_string())
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
    role_is_primary() && lease_valid()
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
    set_source_with_token(
        data_root,
        source,
        None,
        databases,
        interval_secs,
        batch,
        bootstrap,
    )
}

/// Replaces the bearer token in an existing source configuration.
pub fn set_source_token(data_root: &std::path::Path, token: &str) -> Result<(), String> {
    let Some(persisted) =
        read_state::<PersistedSource>("replication:source", "replication-source.json", data_root)?
    else {
        return Ok(());
    };
    set_source_with_token(
        data_root,
        &persisted.source,
        Some(token.to_string()),
        &persisted.databases,
        persisted.interval_secs,
        persisted.batch,
        &persisted.bootstrap,
    )?;
    Ok(())
}

/// Adds database names discovered in the replicated catalog to the active
/// source configuration, preserving the existing source/token/interval.
pub fn ensure_source_databases(
    data_root: &std::path::Path,
    names: &[String],
) -> Result<(), String> {
    // The initial source may come from environment variables and never have
    // been persisted. Load the effective configuration so catalog databases
    // are added to that source too, then persist the merged list.
    let Some(config) = Config::load(data_root)? else {
        return Ok(());
    };
    let mut desired = vec![DEFAULT_DATABASE.to_string()];
    for name in names {
        if name != DEFAULT_DATABASE && !desired.iter().any(|item| item == name) {
            desired.push(name.clone());
        }
    }
    desired.sort();
    desired.dedup();
    let mut current = config.databases.clone();
    current.sort();
    current.dedup();
    if current == desired {
        return Ok(());
    }
    set_source_with_token(
        data_root,
        &config.source,
        config.token.clone(),
        &desired,
        config.interval.as_secs(),
        config.limit,
        &config.bootstrap,
    )?;
    Ok(())
}

pub fn set_source_with_token(
    data_root: &std::path::Path,
    source: &str,
    token: Option<String>,
    databases: &[String],
    interval_secs: u64,
    batch: usize,
    bootstrap: &str,
) -> Result<Value, String> {
    let _guard = source_config_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
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
    // A source update (for example catalog/source propagation) must not erase
    // an existing credential. Preserve the current token when the caller does
    // not supply one.
    let token = token.filter(|value| !value.trim().is_empty()).or_else(|| {
        read_state::<PersistedSource>("replication:source", "replication-source.json", data_root)
            .ok()
            .flatten()
            .and_then(|persisted| persisted.token)
    });
    let persisted = PersistedSource {
        source: source.to_string(),
        token,
        databases,
        interval_secs: interval_secs.max(1),
        batch: batch.clamp(1, 1000),
        bootstrap: bootstrap.to_string(),
    };
    write_state("replication:source", &persisted)?;
    let _ = set_paused(false);
    record_source(&persisted);
    Ok(serde_json::json!({
        "source": persisted.source,
        "databases": persisted.databases,
        "interval_secs": persisted.interval_secs,
        "batch": persisted.batch,
        "bootstrap": persisted.bootstrap,
    }))
}

/// Redeems an 8-character pairing code on a remote primary and installs the
/// returned source configuration locally. Used by `nonodb-cli replication join`
/// through the local daemon, so the CLI does not need its own TLS client.
pub async fn join_remote(
    data_root: &std::path::Path,
    url: &str,
    code: &str,
    node_id: Option<&str>,
    advertise_url: Option<&str>,
) -> Result<Value, String> {
    let url = url.trim().trim_end_matches('/');
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err("pairing URL must start with http:// or https://".into());
    }
    if code.trim().is_empty() {
        return Err("pairing code is required".into());
    }
    let config = Config::from_env()?;
    let client = config.client()?;
    let node_id = node_id
        .map(str::to_string)
        .or_else(|| non_empty_env("NONODB_REPLICATION_NODE_ID"))
        .or_else(|| non_empty_env("NONODB_REPLICATION_ORIGIN"))
        .unwrap_or_else(|| "local".to_string());
    let advertise_url = advertise_url
        .map(str::to_string)
        .or_else(|| non_empty_env("NONODB_REPLICATION_ADVERTISE_URL"));
    let body = serde_json::json!({
        "code": code.trim(),
        "node_id": node_id,
        "advertise_url": advertise_url,
    });
    let response = client
        .post(format!("{url}/v1/replication/pair"))
        .header(reqwest::header::ACCEPT, "application/json")
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .timeout(Duration::from_secs(20))
        .body(serde_json::to_vec(&body).map_err(|error| error.to_string())?)
        .send()
        .await
        .map_err(|error| format!("pair request: {error}"))?;
    let status = response.status();
    let raw = response
        .text()
        .await
        .map_err(|error| format!("read pair response: {error}"))?;
    if !status.is_success() {
        return Err(format!(
            "pair request failed: HTTP {}: {raw}",
            status.as_u16()
        ));
    }
    let payload: Value =
        serde_json::from_str(&raw).map_err(|error| format!("decode pair response: {error}"))?;
    let result = payload
        .get("result")
        .ok_or("pair response is missing result")?;
    let token = result
        .get("token")
        .and_then(Value::as_str)
        .ok_or("pair response is missing token")?
        .to_string();
    let source = result
        .get("source")
        .and_then(Value::as_str)
        .ok_or("pair response is missing source")?
        .to_string();
    let databases: Vec<String> = result
        .get("databases")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .filter(|items: &Vec<String>| !items.is_empty())
        .unwrap_or_else(default_databases);
    let interval = result
        .get("interval_secs")
        .and_then(Value::as_u64)
        .unwrap_or(1);
    let batch = result.get("batch").and_then(Value::as_u64).unwrap_or(500) as usize;
    let bootstrap = result
        .get("bootstrap")
        .and_then(Value::as_str)
        .unwrap_or("auto");
    let configured = set_source_with_token(
        data_root,
        &source,
        Some(token),
        &databases,
        interval,
        batch,
        bootstrap,
    )?;
    if writable() {
        demote(epoch(), "replica", None).map_err(|(code, message)| format!("{code}: {message}"))?;
    }
    Ok(serde_json::json!({
        "ok": true,
        "source": configured,
        "node_id": node_id,
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
            let (fresh, applied) = bootstrap_state(data_root, database).await?;
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
                if let Err(error) = send_ack(&client, &config, database, &origin, head).await {
                    tracing::warn!(database, error = %error, "replication bootstrap ack failed");
                }
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
) -> Result<(bool, u64), String> {
    let path = data_root.join(database);
    if !path.join("data.mdb").is_file() {
        return Ok((true, 0));
    }
    let path_for_open = path.clone();
    tokio::task::spawn_blocking(move || -> Result<(bool, u64), String> {
        let store = Store::open(&path_for_open).map_err(|error| error.to_string())?;
        let origins = store
            .replication_applied_origins()
            .map_err(|error| error.to_string())?;
        let collections = store.collections().map_err(|error| error.to_string())?;
        let applied = store
            .replication_applied_global()
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
    let temp_root = std::env::var_os("NONODB_REPLICATION_BOOTSTRAP_TMP")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    tokio::fs::create_dir_all(&temp_root).await.map_err(|error| {
        format!(
            "create replication bootstrap temp root {}: {error}; set NONODB_REPLICATION_BOOTSTRAP_TMP to a writable directory",
            temp_root.display()
        )
    })?;
    let temp = temp_root.join(format!(
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
        .map_err(|error| {
            format!(
                "create bootstrap temp dir {}: {error}; set NONODB_REPLICATION_BOOTSTRAP_TMP to a directory with more space",
                temp.display()
            )
        })?;
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
        if let Some(required) = response.content_length() {
            let available = fs2::available_space(&temp)
                .map_err(|error| format!("check bootstrap temp space: {error}"))?;
            if required > available {
                return Err(format!(
                    "insufficient space for replication snapshot in {}: snapshot is {required} bytes, {available} bytes available; set NONODB_REPLICATION_BOOTSTRAP_TMP to a directory with more space",
                    temp.display()
                ));
            }
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
        let database_path = data_root.join(database);
        match copy_bootstrap_sidecars(client, config, database, &database_path).await {
            Ok(copied) if copied > 0 => {
                tracing::info!(database, copied, "copied replica search sidecars");
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(
                    database,
                    error = %error,
                    "sidecar transfer failed; rebuilding from snapshot data"
                );
                match rebuild_bootstrap_search(&database_path).await {
                    Ok(rebuilt) if rebuilt > 0 => {
                        tracing::info!(database, rebuilt, "rebuilt replica search sidecars");
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(
                            database,
                            error = %error,
                            "snapshot installed but search sidecars were not rebuilt; restart with NONODB_REBUILD_MISSING_SEARCH=1"
                        );
                    }
                }
            }
        }
        Ok(snapshot_head)
    }
    .await;
    let _ = tokio::fs::remove_dir_all(&temp).await;
    result
}

async fn bootstrap_sidecar_paths(database_path: &std::path::Path) -> Result<Vec<String>, String> {
    let database_path = database_path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<Vec<String>, String> {
        let store = Store::open(&database_path).map_err(|error| error.to_string())?;
        let files = store
            .search_sidecar_files()
            .map_err(|error| error.to_string())?;
        let env = store.env().clone();
        drop(store);
        env.prepare_for_closing().wait();
        Ok(files)
    })
    .await
    .map_err(|error| format!("list search sidecars join error: {error}"))?
}

fn safe_sidecar_path(relative: &str) -> bool {
    let path = std::path::Path::new(relative);
    if path.is_absolute()
        || !path.components().all(|part| {
            matches!(
                part,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            )
        })
    {
        return false;
    }
    let text = path.to_string_lossy();
    text.starts_with("search/") || text.starts_with("vector-ann/")
}

async fn copy_bootstrap_sidecars(
    client: &reqwest::Client,
    config: &Config,
    database: &str,
    database_path: &std::path::Path,
) -> Result<usize, String> {
    let files = bootstrap_sidecar_paths(database_path).await?;
    if files.is_empty() {
        return Ok(0);
    }
    let route = if database == DEFAULT_DATABASE {
        "/v1/replication/snapshot-sidecar".to_string()
    } else {
        format!("/{database}/v1/replication/snapshot-sidecar")
    };
    let mut copied = 0usize;
    for relative in files {
        if !safe_sidecar_path(&relative) {
            return Err(format!("refusing unsafe sidecar path `{relative}`"));
        }
        let mut request = client.get(format!("{}{}?path={}", config.source, route, relative));
        if let Some(token) = &config.token {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|error| format!("request sidecar `{relative}`: {error}"))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format!(
                "sidecar `{relative}` -> HTTP {}: {}",
                status.as_u16(),
                body.chars().take(300).collect::<String>()
            ));
        }
        let destination = database_path.join(&relative);
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| format!("create sidecar directory: {error}"))?;
        }
        let temp = destination.with_extension(format!(
            "tmp-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0)
        ));
        let mut file = match tokio::fs::File::create(&temp).await {
            Ok(file) => file,
            Err(error) => return Err(format!("create sidecar `{relative}`: {error}")),
        };
        let mut stream = response.bytes_stream();
        let mut write_error = None;
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(chunk) => {
                    if let Err(error) = file.write_all(&chunk).await {
                        write_error = Some(format!("write sidecar `{relative}`: {error}"));
                        break;
                    }
                }
                Err(error) => {
                    write_error = Some(format!("read sidecar `{relative}`: {error}"));
                    break;
                }
            }
        }
        if write_error.is_none() {
            if let Err(error) = file.flush().await {
                write_error = Some(format!("flush sidecar `{relative}`: {error}"));
            }
        }
        if write_error.is_none() {
            if let Err(error) = file.sync_all().await {
                write_error = Some(format!("sync sidecar `{relative}`: {error}"));
            }
        }
        drop(file);
        if let Some(error) = write_error {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(error);
        }
        if let Err(error) = tokio::fs::rename(&temp, &destination).await {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(format!("install sidecar `{relative}`: {error}"));
        }
        copied += 1;
    }
    Ok(copied)
}

async fn rebuild_bootstrap_search(database_path: &std::path::Path) -> Result<usize, String> {
    let database_path = database_path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<usize, String> {
        let store = Store::open(&database_path).map_err(|error| error.to_string())?;
        let mut rebuilt = 0usize;
        for name in store.collections().map_err(|error| error.to_string())? {
            let collection = store.collection(&name).map_err(|error| error.to_string())?;
            rebuilt += collection
                .rebuild_missing_search_sidecars()
                .map_err(|error| error.to_string())?;
        }
        let env = store.env().clone();
        drop(store);
        env.prepare_for_closing().wait();
        Ok(rebuilt)
    })
    .await
    .map_err(|error| format!("rebuild search sidecars join error: {error}"))?
}

async fn install_bootstrap_snapshot(
    data_root: &std::path::Path,
    database: &str,
    origin: &str,
    head: u64,
    epoch: u64,
    source: &std::path::Path,
) -> Result<(), String> {
    let source_size = tokio::fs::metadata(source)
        .await
        .map_err(|error| format!("stat bootstrap snapshot: {error}"))?
        .len();
    let available = fs2::available_space(data_root)
        .map_err(|error| format!("check data root space: {error}"))?;
    if source_size > available {
        return Err(format!(
            "insufficient space to install replication snapshot in {}: snapshot is {source_size} bytes, {available} bytes available",
            data_root.display()
        ));
    }
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
    shutdown_tx: watch::Sender<bool>,
) -> Result<(), String> {
    tokio::spawn(async move {
        let mut shutdown = shutdown;
        let mut consecutive_failures = 0u32;
        loop {
            if *shutdown.borrow() {
                break;
            }
            let config = match Config::load(&data_root) {
                Ok(Some(config)) => config,
                Ok(None) => {
                    if !role_is_primary() && !paused() && !discovery_urls().is_empty() {
                        if let Ok(Some(source)) = discover_primary(&data_root).await {
                            if let Some(token) = voter_token() {
                                let databases = default_databases();
                                match set_source_with_token(
                                    &data_root,
                                    &source,
                                    Some(token),
                                    &databases,
                                    1,
                                    500,
                                    "auto",
                                ) {
                                    Ok(_) => tracing::warn!(
                                        source = %source,
                                        "discovered current primary for automatic rejoin"
                                    ),
                                    Err(error) => tracing::warn!(
                                        error = %error,
                                        "automatic rejoin source install failed"
                                    ),
                                }
                            }
                        }
                    }
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
            if paused() {
                tokio::select! {
                    _ = shutdown.changed() => break,
                    _ = tokio::time::sleep(config.interval) => {}
                }
                continue;
            }
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
                let mut cycle_failed = false;
                for database in &config.databases {
                    if let Err(error) = pull_database(
                        &client,
                        &config,
                        &data_root,
                        &databases,
                        database,
                        &shutdown_tx,
                    )
                    .await
                    {
                        cycle_failed = true;
                        tracing::warn!(database, error = %error, "replication pull failed");
                    }
                }
                if cycle_failed {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                } else {
                    consecutive_failures = 0;
                }
                if consecutive_failures >= 3 {
                    consecutive_failures = 0;
                    match discover_primary(&data_root).await {
                        Ok(Some(source)) if source != config.source => {
                            match set_source(
                                &data_root,
                                &source,
                                &config.databases,
                                config.interval.as_secs(),
                                config.limit,
                                &config.bootstrap,
                            ) {
                                Ok(_) => tracing::warn!(
                                    old_source = %config.source,
                                    new_source = %source,
                                    "rediscovered primary using configured seeds/voters"
                                ),
                                Err(error) => tracing::warn!(
                                    error = %error,
                                    "rediscovered primary could not be installed"
                                ),
                            }
                        }
                        Ok(_) => {}
                        Err(error) => {
                            tracing::warn!(error = %error, "primary discovery failed");
                        }
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

async fn discover_primary(data_root: &std::path::Path) -> Result<Option<String>, String> {
    let token = voter_token()
        .or_else(|| {
            Config::load(data_root)
                .ok()
                .flatten()
                .and_then(|config| config.token)
        })
        .ok_or("no voter, cluster peer, or source token configured")?;
    let config = Config::from_env()?;
    let client = config.client()?;
    let self_url = advertise_url();
    for voter in discovery_urls() {
        let response = client
            .get(format!("{voter}/v1/replication/primary"))
            .bearer_auth(&token)
            .timeout(Duration::from_secs(5))
            .send()
            .await;
        let Ok(response) = response else {
            continue;
        };
        if !response.status().is_success() {
            continue;
        }
        let Ok(payload) = response.json::<Value>().await else {
            continue;
        };
        let Some(url) = payload
            .pointer("/result/write_url")
            .and_then(Value::as_str)
            .map(|url| url.trim_end_matches('/'))
            .filter(|url| !url.is_empty())
        else {
            continue;
        };
        if self_url.as_deref() == Some(url) {
            continue;
        }
        let role = payload
            .pointer("/result/role")
            .and_then(Value::as_str)
            .unwrap_or("");
        if role == "primary" {
            return Ok(Some(url.to_string()));
        }
    }
    Ok(None)
}

/// Hard-resync a replica database whose local log diverged from the current
/// source. The data is recreated and the full source log is pulled again.
async fn reset_replicated_database(databases: &Databases, database: &str) -> Result<(), String> {
    let runtime = databases
        .get(database)
        .await
        .map_err(|error| format!("{}: {}", error.code, error.message))?;
    runtime
        .store
        .reset_replica_state()
        .map_err(|error| error.to_string())
}

async fn pull_database(
    client: &reqwest::Client,
    config: &Config,
    data_root: &std::path::Path,
    databases: &Databases,
    database: &str,
    shutdown_tx: &watch::Sender<bool>,
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
        if role_is_primary() {
            return Ok(());
        }
        let status = get_json(client, config, database, "status", &[]).await?;
        let source_role = status.get("role").and_then(Value::as_str).unwrap_or("");
        let successor = status
            .get("successor")
            .and_then(Value::as_str)
            .map(|value| value.trim().trim_end_matches('/'))
            .filter(|value| !value.is_empty());
        if source_role != "primary" {
            if let Some(successor) = successor {
                if successor != config.source {
                    set_source(
                        data_root,
                        successor,
                        &config.databases,
                        config.interval.as_secs(),
                        config.limit,
                        &config.bootstrap,
                    )?;
                    tracing::warn!(
                        old_source = %config.source,
                        new_source = successor,
                        "source was demoted; switching replication source to successor"
                    );
                    return Ok(());
                }
            }
        }
        let origin = status
            .get("origin")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or("source replication capture is disabled")?
            .to_string();
        let applied = runtime
            .store
            .replication_applied_global()
            .map_err(|error| error.to_string())?;
        let status_head = status.get("head").and_then(Value::as_u64).unwrap_or(0);
        runtime
            .store
            .set_replication_source_head(&origin, status_head)
            .map_err(|error| error.to_string())?;
        if applied >= status_head {
            PROMOTION_READY.store(true, Ordering::Relaxed);
            if let Err(error) = send_ack(client, config, database, &origin, applied).await {
                tracing::warn!(database, error = %error, "replication idle ack failed");
            }
            return Ok(());
        }
        PROMOTION_READY.store(false, Ordering::Relaxed);
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
            if applied < status_head {
                let message = format!(
                    "replication gap: no changes available after {applied}, source head {status_head}"
                );
                metrics::inc(&metrics::REPLICATION_APPLY_ERRORS);
                record_apply_error(&message);
                maybe_restart_for_gap(config, &message, shutdown_tx);
                return Err(message);
            }
            return Ok(());
        }
        let mut applied_any = false;
        let mut observed_epoch = 0u64;
        for value in batch {
            if role_is_primary() {
                return Ok(());
            }
            let txn: ReplicationTxn = serde_json::from_value(value.clone())
                .map_err(|error| format!("decode replication transaction: {error}"))?;
            observed_epoch = observed_epoch.max(txn.epoch);
            match runtime.store.apply_replication_txn(&txn) {
                Ok(applied) => {
                    if applied {
                        applied_any = true;
                        metrics::inc(&metrics::REPLICATION_APPLIED_TOTAL);
                        if let Err(error) =
                            crate::live::notify_applied(&runtime.store, &txn.changes)
                        {
                            tracing::warn!(seq = txn.seq, error = %error, "live apply notification failed");
                        }
                    }
                }
                Err(error) => {
                    metrics::inc(&metrics::REPLICATION_APPLY_ERRORS);
                    let message = format!("apply replication transaction {}: {error}", txn.seq);
                    record_apply_error(&message);
                    if message.contains("sequence mismatch")
                        || message.contains("stale replication epoch")
                    {
                        tracing::error!(
                            database,
                            error = %message,
                            "local replication log diverged from source; resetting replica database"
                        );
                        drop(runtime);
                        if let Err(reset) = reset_replicated_database(databases, database).await {
                            tracing::error!(database, error = %reset, "replica database reset failed");
                        }
                        return Err(message);
                    }
                    maybe_restart_for_gap(config, &message, shutdown_tx);
                    return Err(message);
                }
            }
        }
        if observed_epoch > 0 {
            observe_epoch(observed_epoch).map_err(|error| {
                metrics::inc(&metrics::REPLICATION_APPLY_ERRORS);
                let message = format!("persist observed replication epoch: {error}");
                record_apply_error(&message);
                message
            })?;
        }
        if applied_any {
            runtime.cache.invalidate();
            if let Err(error) = crate::control_plane::apply_remote().await {
                metrics::inc(&metrics::REPLICATION_APPLY_ERRORS);
                let message = format!("apply replicated control plane: {error}");
                record_apply_error(&message);
                return Err(message);
            }
            metrics::set(&metrics::REPLICATION_LAST_SUCCESS_UNIX, unix_now());
            record_success();
            let acked = runtime
                .store
                .replication_applied_global()
                .map_err(|error| error.to_string())?;
            if let Err(error) = send_ack(client, config, database, &origin, acked).await {
                tracing::warn!(database, error = %error, "replication ack failed");
            }
        }
        let local = runtime
            .store
            .replication_applied_global()
            .map_err(|error| error.to_string())?;
        PROMOTION_READY.store(local >= status_head, Ordering::Relaxed);
        if batch.len() < config.limit {
            return Ok(());
        }
    }
    Err("replication catch-up exceeded 100 batches".into())
}

fn maybe_restart_for_gap(config: &Config, error: &str, shutdown_tx: &watch::Sender<bool>) {
    if !config.restart_on_gap
        || !(error.contains("replication gap")
            || error.contains("sequence mismatch")
            || error.contains("stale replication epoch"))
    {
        return;
    }
    tracing::error!(
        error,
        "replication needs snapshot resync; requesting graceful restart"
    );
    request_restart();
    let _ = shutdown_tx.send(true);
}

async fn post_json(
    client: &reqwest::Client,
    config: &Config,
    database: &str,
    action: &str,
    body: &Value,
) -> Result<Value, String> {
    let path = if database == DEFAULT_DATABASE {
        format!("/v1/replication/{action}")
    } else {
        format!("/{database}/v1/replication/{action}")
    };
    let encoded = serde_json::to_vec(body).map_err(|error| error.to_string())?;
    let mut request = client
        .post(format!("{}{}", config.source, path))
        .header(reqwest::header::ACCEPT, "application/json")
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(encoded);
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
        let code = payload
            .pointer("/error/code")
            .and_then(Value::as_str)
            .unwrap_or("replication_error");
        let message = payload
            .pointer("/error/message")
            .and_then(Value::as_str)
            .unwrap_or("replication request failed");
        let message = format!("{path} -> HTTP {} {code}: {message}", status.as_u16());
        record_transport_error(&message);
        return Err(message);
    }
    payload
        .get("result")
        .cloned()
        .ok_or_else(|| format!("{path} response is missing `result`"))
}

async fn send_ack(
    client: &reqwest::Client,
    config: &Config,
    database: &str,
    origin: &str,
    seq: u64,
) -> Result<(), String> {
    let path = if database == DEFAULT_DATABASE {
        "/v1/replication/ack".to_string()
    } else {
        format!("/{database}/v1/replication/ack")
    };
    let mut payload = serde_json::json!({
        "node": node_id(),
        "origin": origin,
        "seq": seq,
    });
    if let Some(advertise) = inferred_advertise_url(config) {
        crate::server_info::set_advertise_url(Some(advertise.clone()));
        payload["advertise_url"] = advertise.into();
    }
    let body = serde_json::to_vec(&payload).map_err(|error| error.to_string())?;
    let mut request = client
        .post(format!("{}{}", config.source, path))
        .header(reqwest::header::ACCEPT, "application/json")
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body);
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
    if !status.is_success() {
        metrics::inc(&metrics::REPLICATION_TRANSPORT_ERRORS);
        let message = format!("{path} -> HTTP {}: {}", status.as_u16(), body);
        record_transport_error(&message);
        return Err(message);
    }
    Ok(())
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

#[cfg(test)]
mod advertise_tests {
    use super::inferred_advertise_url_for;

    #[test]
    fn explicit_lan_address_is_advertised() {
        assert_eq!(
            inferred_advertise_url_for("http://10.0.0.10:7902", "10.0.0.50:7912", false),
            Some("http://10.0.0.50:7912".into())
        );
        assert_eq!(
            inferred_advertise_url_for("https://10.0.0.10:7902", "10.0.0.50:7912", true),
            Some("https://10.0.0.50:7912".into())
        );
    }

    #[test]
    fn loopback_is_not_advertised() {
        assert_eq!(
            inferred_advertise_url_for("http://10.0.0.10:7902", "127.0.0.1:7912", false),
            None
        );
        assert_eq!(
            inferred_advertise_url_for("http://127.0.0.1:7902", "0.0.0.0:7912", false),
            None
        );
    }
}
