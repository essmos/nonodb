//! Cluster control-plane replication.
//!
//! The current primary publishes one `__nonodb_control` document containing
//! the auth registry snapshot (token hashes only), backup jobs/state, and the
//! database catalog. Replicas apply it locally but keep all write-side services
//! dormant until they are promoted. The document travels through the normal
//! durable replication log, so it is covered by snapshots, acks, retention and
//! epoch fencing without a separate transport.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nonodb_core::CONTROL_DB;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{oneshot, watch};

use crate::{
    auth::AuthState,
    backups::BackupManager,
    databases::Databases,
    protocol::{Durability, WriteReq},
    replicator,
    writer::WriteJob,
};

pub const CONTROL_ID: &str = "snapshot";
pub const CONTROL_VERSION: u32 = 1;

static GLOBAL: std::sync::OnceLock<Arc<ControlPlane>> = std::sync::OnceLock::new();
static CONTROL_READY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub fn ready() -> bool {
    CONTROL_READY.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn install(manager: Arc<ControlPlane>) {
    let _ = GLOBAL.set(manager);
}

/// Called by the replica apply loop after a committed batch. Reads the default
/// database control document and materializes auth/backups/catalog locally.
pub async fn apply_remote() -> Result<(), String> {
    let Some(manager) = GLOBAL.get() else {
        return Ok(());
    };
    manager.apply_from_store().await
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ClusterSnapshot {
    pub version: u32,
    pub revision: u64,
    #[serde(default)]
    pub auth: Option<Value>,
    #[serde(default)]
    pub backups: Option<Value>,
    #[serde(default)]
    pub databases: Vec<String>,
    #[serde(default)]
    pub peer_token: Option<String>,
}

impl ClusterSnapshot {
    fn canonical(&self) -> Value {
        json!({
            "version": self.version,
            "auth": self.auth,
            "backups": self.backups,
            "databases": self.databases,
            "peer_token": self.peer_token,
        })
    }
}

pub struct ControlPlane {
    databases: Arc<Databases>,
    auth: Option<Arc<AuthState>>,
    backups: Arc<BackupManager>,
    last_hash: Mutex<Option<Vec<u8>>>,
    last_revision: Mutex<u64>,
    last_applied: Mutex<Option<u64>>,
    peer_token: Mutex<Option<String>>,
}

impl ControlPlane {
    pub fn new(
        databases: Arc<Databases>,
        auth: Option<Arc<AuthState>>,
        backups: Arc<BackupManager>,
    ) -> Arc<Self> {
        Arc::new(Self {
            databases,
            auth,
            backups,
            last_hash: Mutex::new(None),
            last_revision: Mutex::new(0),
            last_applied: Mutex::new(None),
            peer_token: Mutex::new(None),
        })
    }

    pub fn start(self: &Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let manager = self.clone();
        tokio::spawn(async move {
            // Establish the cluster peer token before the lease loop starts so
            // a primary without an explicit voter token can still participate
            // in its own quorum when its auth registry is shared/replicated.
            if replicator::role_is_primary() {
                if let Err(error) = manager.ensure_peer_token().await {
                    tracing::warn!(error = %error, "initial cluster peer token failed");
                }
            }
            // Materialize the last replicated control snapshot at startup. A
            // fresh replica may have received it through bootstrap before the
            // auth/backup managers were opened.
            if !replicator::role_is_primary() {
                if let Err(error) = manager.apply_from_store().await {
                    tracing::warn!(error = %error, "control-plane startup apply failed");
                }
            }
            loop {
                if *shutdown.borrow() {
                    break;
                }
                if replicator::writable() {
                    if let Err(error) = manager.publish().await {
                        tracing::warn!(error = %error, "control-plane publish failed");
                    }
                }
                tokio::select! {
                    _ = shutdown.changed() => break,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
            tracing::info!("control-plane loop stopped");
        });
    }

    async fn ensure_peer_token(&self) -> Result<Option<String>, String> {
        {
            let current = self.peer_token.lock().unwrap_or_else(|p| p.into_inner());
            if current.is_some() {
                return Ok(current.clone());
            }
        }
        let path = self.databases.root().join("cluster-peer-token");
        let legacy = std::fs::read_to_string(&path)
            .ok()
            .map(|token| token.trim().to_string())
            .filter(|token| !token.is_empty());
        let token = crate::node_state::get_json::<String>("replication:peer_token")
            .ok()
            .flatten()
            .or(legacy);
        if let Some(token) = token {
            if path.is_file() {
                let _ = crate::node_state::put_json("replication:peer_token", &token);
                let _ = std::fs::remove_file(&path);
            }
            replicator::set_cluster_peer_token(Some(token.clone()));
            *self.peer_token.lock().unwrap_or_else(|p| p.into_inner()) = Some(token.clone());
            return Ok(Some(token));
        }
        let Some(auth) = &self.auth else {
            return Ok(None);
        };
        let token = auth.mutate(|file| {
            file.principals
                .entry("cluster-peer".into())
                .or_insert_with(|| nonodb_auth::PrincipalDef {
                    scopes: vec!["admin:db".into()],
                });
            let (_kid, token, hash) = nonodb_auth::generate_token(Some("clusterpeer"))?;
            if file.tokens.iter().any(|entry| entry.kid == "clusterpeer") {
                // Rotate the deterministic kid if an old peer token exists.
                file.tokens.retain(|entry| entry.kid != "clusterpeer");
            }
            file.tokens.push(nonodb_auth::TokenEntry {
                kid: "clusterpeer".into(),
                sha256: hash,
                principal: "cluster-peer".into(),
                disabled: false,
            });
            Ok(token)
        })?;
        crate::node_state::put_json("replication:peer_token", &token)?;
        replicator::set_cluster_peer_token(Some(token.clone()));
        *self.peer_token.lock().unwrap_or_else(|p| p.into_inner()) = Some(token.clone());
        Ok(Some(token))
    }

    async fn build_snapshot(&self) -> Result<ClusterSnapshot, String> {
        let peer_token = self.ensure_peer_token().await?;
        let auth = match &self.auth {
            Some(auth) => Some(
                serde_json::to_value(auth.registry().file())
                    .map_err(|error| format!("serialize auth snapshot: {error}"))?,
            ),
            None => None,
        };
        let mut databases = self.databases.names().await;
        databases.retain(|name| name != crate::databases::DEFAULT_DATABASE);
        databases.sort();
        databases.dedup();
        let mut revision = unix_ms();
        {
            let mut last = self.last_revision.lock().unwrap_or_else(|p| p.into_inner());
            if revision <= *last {
                revision = last.saturating_add(1);
            }
            *last = revision;
        }
        Ok(ClusterSnapshot {
            version: CONTROL_VERSION,
            revision,
            auth,
            backups: Some(self.backups.control_snapshot()),
            databases,
            peer_token,
        })
    }

    async fn publish(&self) -> Result<(), String> {
        let snapshot = self.build_snapshot().await?;
        let canonical = serde_json::to_vec(&snapshot.canonical())
            .map_err(|error| format!("serialize control snapshot: {error}"))?;
        {
            let last = self.last_hash.lock().unwrap_or_else(|p| p.into_inner());
            if last.as_ref() == Some(&canonical) {
                return Ok(());
            }
        }
        let runtime = self
            .databases
            .default_runtime()
            .await
            .map_err(|error| format!("open default database: {}", error.message))?;
        let mut doc = serde_json::to_value(&snapshot)
            .map_err(|error| format!("encode control document: {error}"))?;
        if let Some(object) = doc.as_object_mut() {
            object.insert("_id".into(), Value::String(CONTROL_ID.into()));
        }
        // Delete+insert is a replacement, so removed auth/backup entries do not
        // linger. Both operations share one writer transaction.
        let request = WriteReq::Batch {
            ops: vec![
                WriteReq::Delete {
                    coll: CONTROL_DB.into(),
                    filter: json!({"_id": CONTROL_ID}),
                    multi: false,
                    vault: None,
                },
                WriteReq::Insert {
                    coll: CONTROL_DB.into(),
                    doc,
                    vault: None,
                },
            ],
        };
        let (resp, rx) = oneshot::channel();
        runtime
            .write_tx
            .send(WriteJob {
                req: request,
                resp,
                isolated: false,
                durability: Durability::Full,
            })
            .map_err(|_| "default writer unavailable".to_string())?;
        match rx.await {
            Ok(Ok(_)) => {
                let mut last = self.last_hash.lock().unwrap_or_else(|p| p.into_inner());
                *last = Some(canonical);
                CONTROL_READY.store(true, std::sync::atomic::Ordering::Relaxed);
                tracing::info!(
                    revision = snapshot.revision,
                    databases = snapshot.databases.len(),
                    "cluster control-plane snapshot published"
                );
                Ok(())
            }
            Ok(Err(error)) => Err(format!("{}: {}", error.code, error.message)),
            Err(_) => Err("writer dropped control-plane request".into()),
        }
    }

    /// Applies the replicated control document to local auth/backup/catalog
    /// state. Services that perform writes remain gated by the replication role.
    pub async fn apply_from_store(&self) -> Result<(), String> {
        let runtime = self
            .databases
            .default_runtime()
            .await
            .map_err(|error| format!("open default database: {}", error.message))?;
        let Some(doc) = read_control_doc(&runtime.store)? else {
            return Ok(());
        };
        let snapshot: ClusterSnapshot = serde_json::from_value(doc)
            .map_err(|error| format!("parse replicated control snapshot: {error}"))?;
        if snapshot.version != CONTROL_VERSION {
            CONTROL_READY.store(false, std::sync::atomic::Ordering::Relaxed);
            return Err(format!(
                "unsupported control-plane version {} (expected {})",
                snapshot.version, CONTROL_VERSION
            ));
        }
        {
            let mut applied = self.last_applied.lock().unwrap_or_else(|p| p.into_inner());
            if applied.is_some_and(|revision| revision == snapshot.revision) {
                return Ok(());
            }
            *applied = Some(snapshot.revision);
        }
        if let Some(auth) = &snapshot.auth {
            match &self.auth {
                Some(state) => state.apply_control_snapshot(auth)?,
                None => tracing::warn!(
                    "replicated auth snapshot is present but NONODB_AUTH_FILE is not configured"
                ),
            }
        }
        if let Some(token) = &snapshot.peer_token {
            crate::node_state::put_json("replication:peer_token", token)?;
            replicator::set_cluster_peer_token(Some(token.clone()));
            *self.peer_token.lock().unwrap_or_else(|p| p.into_inner()) = Some(token.clone());
            if !replicator::role_is_primary() {
                if let Err(error) = replicator::set_source_token(self.databases.root(), token) {
                    tracing::warn!(error = %error, "control peer token source update failed");
                }
            }
        }
        if let Some(backups) = &snapshot.backups {
            self.backups.apply_control_snapshot(backups)?;
        }
        for name in &snapshot.databases {
            match self.databases.create(name).await {
                Ok(()) => {}
                Err(error)
                    if error.code == "invalid_query"
                        && error.message.contains("already exists") => {}
                Err(error) => {
                    tracing::warn!(database = name, error = %error.message, "control catalog create failed");
                }
            }
        }
        if !replicator::role_is_primary() {
            if let Err(error) =
                replicator::ensure_source_databases(self.databases.root(), &snapshot.databases)
            {
                tracing::warn!(error = %error, "control catalog source update failed");
            }
        }
        if env_flag_default("NONODB_CLUSTER_PRUNE_DATABASES", true) {
            for name in self.databases.names().await {
                if name == crate::databases::DEFAULT_DATABASE
                    || snapshot
                        .databases
                        .iter()
                        .any(|candidate| candidate == &name)
                {
                    continue;
                }
                if let Err(error) = self.databases.drop_database(&name).await {
                    tracing::warn!(database = name, error = %error.message, "control catalog drop failed");
                }
            }
        }
        CONTROL_READY.store(true, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(
            revision = snapshot.revision,
            "cluster control-plane snapshot applied"
        );
        Ok(())
    }
}

fn read_control_doc(store: &nonodb_core::Store) -> Result<Option<Value>, String> {
    let rtxn = store.read_txn().map_err(|error| error.to_string())?;
    let collection = match store
        .collection_at(&rtxn, CONTROL_DB)
        .map_err(|error| error.to_string())?
    {
        Some(collection) => collection,
        None => {
            rtxn.commit().map_err(|error| error.to_string())?;
            return Ok(None);
        }
    };
    let doc = collection
        .find_one_in(&rtxn, &json!({"_id": CONTROL_ID}), None)
        .map_err(|error| error.to_string());
    rtxn.commit().map_err(|error| error.to_string())?;
    doc
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn env_flag_default(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(value) => matches!(value.as_str(), "1" | "true" | "yes" | "on"),
        Err(_) => default,
    }
}
