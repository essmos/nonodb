#![recursion_limit = "512"]

//! nonodb daemon: HTTP/JSON over a Unix socket.
//!
//! One process is the sole owner of the LMDB file. Clients (e.g. uvicorn
//! processes) connect through `/run/nonodb.sock` (paths configurable via
//! `NONODB_SOCK` i `NONODB_DATA`).

mod analytics;
mod audit;
mod auth;
mod auth_api;
mod authz;
mod auto_index;
mod backups;
mod backups_api;
mod cache;
mod control_plane;
mod cursor;
mod databases;
mod election;
mod imports;
mod imports_api;
mod instance;
mod lease;
mod live;
mod login_guard;
mod maintenance;
mod metrics;
mod mongo_wire;
mod network;
mod network_api;
mod node_state;
mod pairing;
mod pairing_api;
mod profile;
mod protocol;
mod quota;
mod replication_api;
mod replicator;
mod retention;
mod revocation;
mod s3_import;
mod server_info;
mod wal;
mod webhooks;
mod writer;

use std::collections::HashMap;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, OriginalUri, State},
    http::{
        header::{ACCEPT, CACHE_CONTROL, CONTENT_TYPE},
        HeaderMap, HeaderValue, StatusCode, Uri,
    },
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post, put},
    Extension, Json, Router,
};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use ring::digest::{digest, SHA256};
use serde_json::{json, value::RawValue, Value};

use nonodb_core::heed::RoTxn;
use nonodb_core::{
    aggregate, project_document, vault, CollectionProfile, FindQuery, Store, VaultKey,
    DEFAULT_FUZZY_DISTANCE, PAGE_SIZE,
};
use protocol::{ApiError, ApiResponse, Durability, LookupBy, Request, VaultAuth, WriteReq};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_rustls::rustls::pki_types::CertificateDer;
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;
use writer::WriteJob;

/// Maximum request body size (4 MiB).
const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;
/// Timeout for a single API request.
/// Timeout for index backfills. Building an index over tens of millions of
/// documents can take minutes; it must not be aborted by the normal 30s request
/// timeout. Non-positive values fall back to the default.
fn index_build_timeout() -> Duration {
    static TIMEOUT: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *TIMEOUT.get_or_init(|| {
        std::env::var("NONODB_INDEX_TIMEOUT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&secs| secs > 0)
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(3600))
    })
}

fn sql_slow_threshold() -> Duration {
    static THRESHOLD: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        std::env::var("NONODB_SQL_SLOW_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&ms| ms > 0)
            .map(Duration::from_millis)
            .unwrap_or_default()
    })
}

fn request_timeout() -> Duration {
    static TIMEOUT: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *TIMEOUT.get_or_init(|| {
        std::env::var("NONODB_REQUEST_TIMEOUT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&secs| secs > 0)
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(30))
    })
}

#[derive(Clone)]
struct AppState {
    store: Arc<Store>,
    write_tx: mpsc::Sender<WriteJob>,
    cache: Arc<cache::QueryCache>,
    auto_index: Arc<auto_index::AutoIndexer>,
    quota: Arc<quota::Quota>,
    analytics: Arc<analytics::AnalyticsManager>,
    _runtime: Arc<databases::DatabaseRuntime>,
    databases: Arc<databases::Databases>,
    pub(crate) backups: Arc<backups::BackupManager>,
    pub(crate) imports: Arc<imports::ImportManager>,
    pub(crate) auth: Option<Arc<auth::AuthState>>,
    pub(crate) audit: Option<Arc<audit::AuditLog>>,
    pub(crate) pairings: Arc<pairing::PairingManager>,
    cursors: Arc<cursor::Registry>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl AppState {
    async fn for_database(&self, name: &str) -> Result<Self, ApiError> {
        let runtime = self.databases.get(name).await?;
        Ok(Self {
            store: runtime.store.clone(),
            write_tx: runtime.write_tx.clone(),
            cache: runtime.cache.clone(),
            auto_index: runtime.auto_index.clone(),
            quota: runtime.quota.clone(),
            analytics: runtime.analytics.clone(),
            _runtime: runtime,
            databases: self.databases.clone(),
            backups: self.backups.clone(),
            imports: self.imports.clone(),
            auth: self.auth.clone(),
            audit: self.audit.clone(),
            pairings: self.pairings.clone(),
            cursors: self.cursors.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
        })
    }
}

fn database_from_uri(uri: &Uri) -> String {
    let first = uri
        .path()
        .trim_start_matches('/')
        .split('/')
        .next()
        .unwrap_or("");
    if first.is_empty() || first == "v1" {
        databases::DEFAULT_DATABASE.to_string()
    } else {
        first.to_string()
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let result = run();
    if replicator::restart_requested() {
        tracing::warn!("restart requested; exiting with status 75");
        std::process::exit(75);
    }
    result
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    profile::apply().map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    // Explicit runtime: limit the blocking thread pool (lower peak VSZ),
    // because LMDB operations run through `spawn_blocking`.
    let blocking_threads = std::env::var("NONODB_BLOCKING_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(64);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .max_blocking_threads(blocking_threads)
        .enable_all()
        .build()?;
    rt.block_on(run_async())
}

async fn run_async() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    if let Ok(profile) = std::env::var("NONODB_PROFILE") {
        if !profile.is_empty() {
            tracing::info!(profile, "deployment profile active");
        }
    }

    let data = PathBuf::from(std::env::var("NONODB_DATA").unwrap_or_else(|_| "./data".into()));
    node_state::open(&data)?;
    #[cfg(unix)]
    let sock =
        PathBuf::from(std::env::var("NONODB_SOCK").unwrap_or_else(|_| "/run/nonodb.sock".into()));
    let tcp_addr = std::env::var("NONODB_TCP").ok();
    let mongo_addr = std::env::var("NONODB_MONGO_ADDR").ok();
    let mongo_auth = if mongo_addr.is_some() {
        mongo_wire::MongoAuth::from_env()?
    } else {
        None
    };
    let mongo_tls = if mongo_addr.is_some() {
        mongo_wire::tls_from_env()?
    } else {
        None
    };
    let mongo_limits = if mongo_addr.is_some() {
        Some(mongo_wire::WireLimits::from_env()?)
    } else {
        None
    };
    server_info::set_advertise_url(
        std::env::var("NONODB_REPLICATION_ADVERTISE_URL")
            .ok()
            .map(|value| value.trim().trim_end_matches('/').to_string())
            .filter(|value| !value.is_empty()),
    );

    let auth = match std::env::var_os("NONODB_AUTH_FILE") {
        Some(path) if !path.is_empty() => Some(auth::AuthState::open(PathBuf::from(path))?),
        _ => None,
    };
    if tcp_addr.is_some() && auth.is_none() {
        return Err("NONODB_TCP requires NONODB_AUTH_FILE".into());
    }
    if let Some(auth) = &auth {
        auth.spawn_watcher();
    }
    let audit = audit::AuditLog::from_env()?;
    replicator::init_role(&data).map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    replicator::bootstrap_startup(&data)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    let databases = databases::Databases::open(&data)
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    databases.start_online_compactor();
    metrics::set_disk_paths(
        data.clone(),
        std::env::var_os("NONODB_SNAPSHOT_DIR").map(PathBuf::from),
    );
    let backups = backups::BackupManager::open(&data, databases.clone())
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    backups.start_scheduler();
    let imports = imports::ImportManager::open(&data, databases.clone())
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    live::install_observer();
    live::registry().start_janitor();
    let default = databases
        .default_runtime()
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.message.into() })?;

    if std::env::var("NONODB_REBUILD_MISSING_SEARCH")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
    {
        for db_name in databases.names().await {
            let runtime = databases
                .get(&db_name)
                .await
                .map_err(|e| -> Box<dyn std::error::Error> { e.message.into() })?;
            let store = runtime.store.clone();
            for name in store.collections()? {
                let collection = store.collection(&name)?;
                let rebuilt = collection.rebuild_missing_search_sidecars()?;
                if rebuilt > 0 {
                    tracing::info!(
                        database = %db_name,
                        collection = %name,
                        rebuilt,
                        "rebuilt missing search sidecars"
                    );
                }
            }
        }
    }

    let stats_mode = std::env::var("NONODB_REBUILD_SEARCH_STATS")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let stats_disabled = matches!(stats_mode.as_str(), "0" | "false" | "no" | "off");
    if !stats_disabled {
        let force = matches!(stats_mode.as_str(), "force" | "all");
        let only: Vec<String> = std::env::var("NONODB_REBUILD_SEARCH_STATS_COLLECTIONS")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        for db_name in databases.names().await {
            let runtime = databases
                .get(&db_name)
                .await
                .map_err(|e| -> Box<dyn std::error::Error> { e.message.into() })?;
            let store = runtime.store.clone();
            for name in store.collections()? {
                if !only.is_empty() && !only.iter().any(|wanted| wanted == &name) {
                    continue;
                }
                let collection = store.collection(&name)?;
                let rebuilt = if force {
                    collection.rebuild_segment_text_stats()?
                } else {
                    collection.ensure_segment_text_stats()?
                };
                if rebuilt > 0 {
                    tracing::info!(
                        database = %db_name,
                        collection = %name,
                        rebuilt,
                        force,
                        "rebuilt segment text statistics"
                    );
                }
            }
        }
    }

    if std::env::var("NONODB_BUILD_VECTOR_ANN")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
    {
        let nprobe = std::env::var("NONODB_VECTOR_ANN_PROBE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(16);
        let only: Vec<String> = std::env::var("NONODB_VECTOR_ANN_COLLECTIONS")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let store = default.store.clone();
        for name in store.collections()? {
            if !only.is_empty() && !only.iter().any(|wanted| wanted == &name) {
                continue;
            }
            let collection = store.collection(&name)?;
            for def in collection.list_vector_indexes()? {
                if def.ann.is_none() || !collection.vector_ann_sidecar_exists(&def.field) {
                    let stats = collection.build_vector_ann_index(&def.field, nprobe)?;
                    tracing::info!(
                        collection = %name,
                        field = %def.field,
                        entries = stats.entries,
                        clusters = stats.clusters,
                        nprobe = stats.nprobe,
                        bytes = stats.bytes,
                        "built vector ANN sidecar"
                    );
                }
            }
        }
    }

    let drain_timeout = drain_timeout();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let state_shutdown = shutdown_tx.clone();
    let replication_shutdown = shutdown_tx.clone();
    let control =
        control_plane::ControlPlane::new(databases.clone(), auth.clone(), backups.clone());
    control_plane::install(control.clone());
    control.start(shutdown_rx.clone());
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!("shutdown signal received; draining");
        let _ = shutdown_tx.send(true);
    });
    replicator::start(
        databases.clone(),
        data.clone(),
        shutdown_rx.clone(),
        replication_shutdown,
    )
    .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    retention::start(databases.clone(), shutdown_rx.clone())
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    lease::start(data.clone(), shutdown_rx.clone())
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    election::start(data.clone(), shutdown_rx.clone())
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    #[cfg(unix)]
    let instance_socket = Some(sock.as_path());
    #[cfg(not(unix))]
    let instance_socket = None;
    instance::start(&data, instance_socket, tcp_addr.as_deref())
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    let state = AppState {
        store: default.store.clone(),
        write_tx: default.write_tx.clone(),
        cache: default.cache.clone(),
        auto_index: default.auto_index.clone(),
        quota: default.quota.clone(),
        analytics: default.analytics.clone(),
        _runtime: default.clone(),
        databases: databases.clone(),
        backups,
        imports,
        auth: auth.clone(),
        audit: audit.clone(),
        pairings: Arc::new(pairing::PairingManager::new()),
        cursors: Arc::new(cursor::Registry::new()),
        shutdown_tx: state_shutdown,
    };
    let auth_routes = Router::new()
        .route("/v1/auth", get(auth_api::summary))
        .route("/v1/auth/principals", post(auth_api::create_principal))
        .route(
            "/v1/auth/principals/:name",
            put(auth_api::update_principal).delete(auth_api::delete_principal),
        )
        .route("/v1/auth/tokens", post(auth_api::create_token))
        .route("/v1/auth/tokens/:kid", patch(auth_api::update_token))
        .route("/v1/auth/certificates", post(auth_api::create_certificate))
        .route(
            "/v1/auth/certificates/:fingerprint",
            patch(auth_api::update_certificate),
        )
        .layer(axum::middleware::from_fn(no_store));
    let redirect_state = state.clone();
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/v1/capabilities", get(capabilities))
        .route("/v1/server", get(server))
        .route("/v1/network", get(network_api::view))
        .route(
            "/v1/network/firewall",
            put(network_api::save_firewall).delete(network_api::disable_firewall),
        )
        .route(
            "/v1/network/firewall/apply",
            post(network_api::apply_firewall),
        )
        .route("/readyz", get(ready))
        .route("/readyz/write", get(ready_write))
        .route("/metrics", get(metrics_handler))
        .route("/metrics/prometheus", get(prometheus_handler))
        .merge(auth_routes)
        .route("/v1/backups", get(backups_api::list))
        .route("/v1/backups/jobs", post(backups_api::upsert))
        .route(
            "/v1/backups/jobs/:id",
            axum::routing::delete(backups_api::delete),
        )
        .route("/v1/backups/jobs/:id/run", post(backups_api::run))
        .route("/v1/backups/keygen", post(backups_api::keygen))
        .route("/v1/backups/secret", post(backups_api::secret))
        .route("/v1/import", get(imports_api::view))
        .route("/v1/import/browse", post(imports_api::browse))
        .route("/v1/import/run", post(imports_api::run))
        .route("/v1/import/status/:id", get(imports_api::status))
        .route(
            "/v1/restore/preflight",
            post(imports_api::restore_preflight),
        )
        .route("/v1/restore", post(imports_api::restore))
        .route(
            "/v1/replication/status",
            get(replication_api::status_default),
        )
        .route(
            "/v1/replication/changes",
            get(replication_api::changes_default),
        )
        .route(
            "/v1/replication/snapshot",
            get(replication_api::snapshot_default),
        )
        .route(
            "/v1/replication/snapshot-sidecar",
            get(replication_api::sidecar_default),
        )
        .route(
            "/v1/replication/prune",
            post(replication_api::prune_default),
        )
        .route("/v1/replication/ack", post(replication_api::ack_default))
        .route("/v1/replication/acks", get(replication_api::acks_default))
        .route(
            "/v1/replication/acks/:node",
            delete(replication_api::forget_ack_default),
        )
        .route("/v1/replication/vote", post(replication_api::vote))
        .route("/v1/replication/primary", get(replication_api::primary))
        .route(
            "/v1/replication/pairings",
            get(pairing_api::list).post(pairing_api::create),
        )
        .route(
            "/v1/replication/pairings/:code",
            axum::routing::delete(pairing_api::revoke),
        )
        .route("/v1/replication/pair", post(pairing_api::pair))
        .route("/v1/replication/join", post(pairing_api::join))
        .route("/v1/replication/lease", post(replication_api::lease))
        .route("/v1/replication/pause", post(replication_api::pause))
        .route("/v1/replication/resume", post(replication_api::resume))
        .route(
            "/v1/replication/disconnect",
            post(replication_api::disconnect),
        )
        .route("/v1/replication/promote", post(replication_api::promote))
        .route("/v1/replication/demote", post(replication_api::demote))
        .route(
            "/v1/replication/source",
            axum::routing::put(replication_api::update_source),
        )
        .route(
            "/:database/v1/replication/status",
            get(replication_api::status),
        )
        .route(
            "/:database/v1/replication/changes",
            get(replication_api::changes),
        )
        .route(
            "/:database/v1/replication/snapshot",
            get(replication_api::snapshot),
        )
        .route(
            "/:database/v1/replication/snapshot-sidecar",
            get(replication_api::sidecar),
        )
        .route(
            "/:database/v1/replication/prune",
            post(replication_api::prune),
        )
        .route("/:database/v1/replication/ack", post(replication_api::ack))
        .route("/:database/v1/replication/acks", get(replication_api::acks))
        .route(
            "/:database/v1/replication/acks/:node",
            delete(replication_api::forget_ack),
        )
        .route("/v1/exec", post(exec))
        .route("/v1/watch", post(watch_stream))
        .route("/:database/v1/exec", post(exec))
        .route("/:database/v1/watch", post(watch_stream))
        .route("/:database/v1/capabilities", get(capabilities))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .layer(axum::middleware::from_fn(http_access_log))
        .layer(axum::middleware::from_fn_with_state(
            redirect_state,
            write_location,
        ))
        .with_state(state.clone());

    // Optional TCP listener (with TLS when a certificate and key are provided).
    let mut tcp_task: Option<tokio::task::JoinHandle<()>> = None;
    if let Some(addr) = tcp_addr.as_deref() {
        let auth = auth.clone().ok_or("NONODB_TCP requires NONODB_AUTH_FILE")?;
        let tls = match (
            std::env::var("NONODB_TLS_CERT"),
            std::env::var("NONODB_TLS_KEY"),
        ) {
            (Ok(cert), Ok(key)) => Some(tls_acceptor(&cert, &key, auth.clone(), audit.clone())?),
            (Err(_), Err(_)) => {
                if std::env::var_os("NONODB_TLS_CLIENT_CA").is_some() {
                    return Err(
                        "NONODB_TLS_CLIENT_CA requires NONODB_TLS_CERT and NONODB_TLS_KEY".into(),
                    );
                }
                tracing::warn!("NONODB_TCP without TLS (missing NONODB_TLS_CERT/KEY)");
                None
            }
            _ => return Err("provide both NONODB_TLS_CERT and NONODB_TLS_KEY".into()),
        };
        if let Some(setup) = &tls {
            spawn_tls_reload(setup.clone(), shutdown_rx.clone());
        }
        let app_tcp = app.clone();
        let addr = addr.to_string();
        let shutdown = shutdown_rx.clone();
        tcp_task = Some(tokio::spawn(async move {
            if let Err(e) = serve_tcp(&addr, tls, app_tcp, shutdown, drain_timeout).await {
                tracing::error!(?e, %addr, "TCP listener stopped");
            }
        }));
    }

    let mut mongo_task: Option<tokio::task::JoinHandle<()>> = None;
    if let Some(addr) = mongo_addr.as_deref() {
        let addr = addr.to_string();
        let shutdown = shutdown_rx.clone();
        let mongo_state = state.clone();
        let limits = mongo_limits.ok_or("NONODB_MONGO_ADDR requires MongoDB wire limits")?;
        mongo_task = Some(tokio::spawn(async move {
            if let Err(e) = mongo_wire::serve(
                &addr,
                mongo_state,
                mongo_auth,
                mongo_tls,
                shutdown,
                drain_timeout,
                limits,
            )
            .await
            {
                tracing::error!(?e, %addr, "Mongo wire listener stopped");
            }
        }));
    }

    #[cfg(not(unix))]
    {
        // Windows and other non-Unix platforms do not have Unix domain sockets
        // in tokio. TCP is therefore required there.
        if tcp_addr.is_none() {
            return Err(
                "NONODB_TCP is required on this platform (Unix sockets are unavailable)".into(),
            );
        }
        tracing::info!(
            version = env!("CARGO_PKG_VERSION"),
            data = %data.display(),
            tcp = tcp_addr.as_deref().unwrap_or(""),
            mongo = mongo_addr.as_deref().unwrap_or(""),
            "nonodb daemon listening"
        );
        let mut shutdown = shutdown_rx.clone();
        wait_for_shutdown(&mut shutdown).await;
        if let Some(task) = tcp_task {
            if let Err(e) = task.await {
                tracing::warn!(?e, "TCP listener task failed during shutdown");
            }
        }
        if let Some(task) = mongo_task {
            if let Err(e) = task.await {
                tracing::warn!(?e, "Mongo wire listener task failed during shutdown");
            }
        }
        wait_for_writer(drain_timeout).await;
        instance::stop();
        tracing::info!("nonodb daemon stopped");
        return Ok(());
    }

    #[cfg(unix)]
    {
        if let Some(parent) = sock.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        // The daemon is the sole owner of the socket - remove any orphaned file.
        let _ = std::fs::remove_file(&sock);

        let listener = tokio::net::UnixListener::bind(&sock)?;
        server_info::set_unix(&sock);
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o660))?;
        }

        tracing::info!(
            version = env!("CARGO_PKG_VERSION"),
            data = %data.display(),
            sock = %sock.display(),
            mongo = mongo_addr.as_deref().unwrap_or(""),
            "nonodb daemon listening"
        );

        let mut shutdown = shutdown_rx.clone();
        let mut connections = JoinSet::new();
        loop {
            let stream = tokio::select! {
                _ = wait_for_shutdown(&mut shutdown) => break,
                accepted = listener.accept() => accepted?.0,
            };

            if !peer_allowed(&stream) {
                tracing::warn!("rejected UDS connection: disallowed uid/gid");
                continue;
            }

            let app = app.clone().layer(Extension(auth::ConnectionAuth::unix()));
            let shutdown = shutdown_rx.clone();
            connections.spawn(async move {
                serve_stream(stream, app, shutdown).await;
            });
        }
        drop(listener);
        let _ = std::fs::remove_file(&sock);

        drain_connections(connections, drain_timeout, "uds").await;
        if let Some(task) = tcp_task {
            if let Err(e) = task.await {
                tracing::warn!(?e, "TCP listener task failed during shutdown");
            }
        }
        if let Some(task) = mongo_task {
            if let Err(e) = task.await {
                tracing::warn!(?e, "Mongo wire listener task failed during shutdown");
            }
        }
        wait_for_writer(drain_timeout).await;
        instance::stop();
        tracing::info!("nonodb daemon stopped");
        Ok(())
    }
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "info,nonodb=debug".into());
    let json = std::env::var("NONODB_LOG_FORMAT")
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "json"))
        .unwrap_or(false);
    if json {
        tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}

fn drain_timeout() -> Duration {
    Duration::from_secs(
        std::env::var("NONODB_DRAIN_TIMEOUT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30),
    )
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    let _ = shutdown.changed().await;
}

async fn drain_connections(mut connections: JoinSet<()>, timeout: Duration, listener: &str) {
    if connections.is_empty() {
        return;
    }
    let started = std::time::Instant::now();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match tokio::time::timeout_at(deadline, connections.join_next()).await {
            Ok(Some(Ok(()))) => {}
            Ok(Some(Err(err))) if err.is_cancelled() => {}
            Ok(Some(Err(err))) => tracing::debug!(?err, listener, "connection task failed"),
            Ok(None) => break,
            Err(_) => {
                let remaining = connections.len();
                tracing::warn!(
                    listener,
                    remaining,
                    timeout_secs = timeout.as_secs(),
                    "graceful drain timed out; aborting connections"
                );
                connections.shutdown().await;
                break;
            }
        }
    }
    tracing::info!(
        listener,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "connections drained"
    );
}

async fn wait_for_writer(timeout: Duration) {
    use std::sync::atomic::Ordering::Relaxed;
    let started = std::time::Instant::now();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut idle_since = None;
    loop {
        let queue = metrics::QUEUE_LEN.load(Relaxed);
        let busy = metrics::WRITER_BUSY.load(Relaxed);
        if queue == 0 && busy == 0 {
            let since = idle_since.get_or_insert_with(std::time::Instant::now);
            if since.elapsed() >= Duration::from_millis(100) {
                tracing::info!(
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "writer drained"
                );
                return;
            }
        } else {
            idle_since = None;
        }
        if tokio::time::Instant::now() >= deadline {
            tracing::warn!(
                queue,
                busy,
                timeout_secs = timeout.as_secs(),
                "writer drain timed out"
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[derive(Clone)]
struct TlsSetup {
    acceptor: Arc<RwLock<TlsAcceptor>>,
    cert: String,
    key: String,
    client_ca: Option<String>,
    auth: Arc<auth::AuthState>,
    client_auth: bool,
    audit: Option<Arc<audit::AuditLog>>,
    revocation: Option<Arc<revocation::RevocationState>>,
}

fn fingerprint(cert: &CertificateDer<'_>) -> [u8; 32] {
    let digest = digest(&SHA256, cert.as_ref());
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_ref());
    out
}

fn read_client_ca(path: &Path) -> Result<Arc<RootCertStore>, Box<dyn std::error::Error>> {
    let mut reader = BufReader::new(std::fs::File::open(path)?);
    let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err(format!("no CA certificates in {}", path.display()).into());
    }
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots.add(cert)?;
    }
    Ok(Arc::new(roots))
}

fn tls_acceptor(
    cert: &str,
    key: &str,
    auth: Arc<auth::AuthState>,
    audit: Option<Arc<audit::AuditLog>>,
) -> Result<TlsSetup, Box<dyn std::error::Error>> {
    let client_ca = std::env::var("NONODB_TLS_CLIENT_CA")
        .ok()
        .filter(|value| !value.is_empty());
    let (acceptor, client_auth) =
        build_tls_acceptor(cert, key, client_ca.as_deref(), auth.clone(), audit.clone())?;
    let revocation = revocation::RevocationState::from_env(client_ca.as_deref())?;
    Ok(TlsSetup {
        acceptor: Arc::new(RwLock::new(acceptor)),
        cert: cert.to_string(),
        key: key.to_string(),
        client_ca,
        auth,
        client_auth,
        audit,
        revocation,
    })
}

fn build_tls_acceptor(
    cert: &str,
    key: &str,
    client_ca: Option<&str>,
    _auth: Arc<auth::AuthState>,
    _audit: Option<Arc<audit::AuditLog>>,
) -> Result<(TlsAcceptor, bool), Box<dyn std::error::Error>> {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let mut cert_reader = BufReader::new(std::fs::File::open(cert)?);
    let certs = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;
    let mut key_reader = BufReader::new(std::fs::File::open(key)?);
    let key =
        rustls_pemfile::private_key(&mut key_reader)?.ok_or("no private key in NONODB_TLS_KEY")?;

    let client_auth_mode = std::env::var("NONODB_TLS_CLIENT_AUTH")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| {
            if client_ca.is_some() {
                "required".into()
            } else {
                "off".into()
            }
        })
        .to_ascii_lowercase();
    if !matches!(client_auth_mode.as_str(), "off" | "required" | "optional") {
        return Err("NONODB_TLS_CLIENT_AUTH must be one of: off, required, optional".into());
    }
    if client_ca.is_some() && client_auth_mode == "off" {
        return Err(
            "NONODB_TLS_CLIENT_AUTH=off cannot be combined with NONODB_TLS_CLIENT_CA".into(),
        );
    }

    let builder = ServerConfig::builder();
    let config = if let Some(ca) = client_ca {
        let roots = read_client_ca(Path::new(ca))?;
        let mut verifier = WebPkiClientVerifier::builder(roots);
        if client_auth_mode == "optional" {
            verifier = verifier.allow_unauthenticated();
        }
        let verifier = verifier.build()?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)?
    } else {
        if client_auth_mode != "off" {
            return Err("NONODB_TLS_CLIENT_AUTH requires NONODB_TLS_CLIENT_CA".into());
        }
        builder.with_no_client_auth().with_single_cert(certs, key)?
    };
    if let Err(error) = metrics::record_certificate_expiry("server", Path::new(cert)) {
        tracing::warn!(%error, "TLS certificate expiry metric unavailable");
    }
    Ok((TlsAcceptor::from(Arc::new(config)), client_ca.is_some()))
}

fn tls_signature(setup: &TlsSetup) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for path in std::iter::once(setup.cert.as_str())
        .chain(std::iter::once(setup.key.as_str()))
        .chain(setup.client_ca.as_deref())
    {
        path.hash(&mut hasher);
        if let Ok(metadata) = std::fs::metadata(path) {
            metadata.len().hash(&mut hasher);
            if let Ok(modified) = metadata.modified() {
                if let Ok(duration) = modified.duration_since(std::time::UNIX_EPOCH) {
                    duration.as_nanos().hash(&mut hasher);
                }
            }
        }
    }
    hasher.finish()
}

fn spawn_tls_reload(setup: TlsSetup, mut shutdown: watch::Receiver<bool>) {
    if let Some(revocation) = setup.revocation.clone() {
        let mut revocation_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let interval = revocation.refresh_interval();
            loop {
                tokio::select! {
                    _ = wait_for_shutdown(&mut revocation_shutdown) => break,
                    _ = tokio::time::sleep(interval) => {}
                }
                revocation.refresh().await;
            }
            tracing::info!("revocation refresh watcher stopped");
        });
    }
    let interval = std::env::var("NONODB_TLS_RELOAD_INTERVAL")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30);
    if interval == 0 {
        return;
    }
    tokio::spawn(async move {
        let mut signature = tls_signature(&setup);
        loop {
            tokio::select! {
                _ = wait_for_shutdown(&mut shutdown) => break,
                _ = tokio::time::sleep(Duration::from_secs(interval)) => {}
            }
            let next = tls_signature(&setup);
            if next == signature {
                continue;
            }
            match build_tls_acceptor(
                &setup.cert,
                &setup.key,
                setup.client_ca.as_deref(),
                setup.auth.clone(),
                setup.audit.clone(),
            ) {
                Ok((acceptor, _client_auth)) => {
                    *setup
                        .acceptor
                        .write()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = acceptor;
                    signature = next;
                    tracing::info!("TLS certificate reloaded");
                }
                Err(error) => {
                    tracing::error!(
                        ?error,
                        "TLS certificate reload failed; keeping previous certificate"
                    );
                }
            }
        }
        tracing::info!("TLS reload watcher stopped");
    });
}

async fn serve_tcp(
    addr: &str,
    tls: Option<TlsSetup>,
    app: Router,
    mut shutdown: watch::Receiver<bool>,
    drain_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?.to_string();
    server_info::set_tcp(addr, &bound, tls.is_some());
    tracing::info!(
        %addr,
        tls = tls.is_some(),
        mtls = tls.as_ref().is_some_and(|t| t.client_auth),
        "TCP listener"
    );
    let mut connections = JoinSet::new();
    loop {
        let (stream, peer) = tokio::select! {
            _ = wait_for_shutdown(&mut shutdown) => break,
            accepted = listener.accept() => accepted?,
        };
        let app = app.clone();
        let tls = tls.clone();
        let shutdown = shutdown.clone();
        connections.spawn(async move {
            let Some(tls) = tls else {
                let app = app.clone().layer(Extension(auth::ConnectionAuth::tcp(
                    None,
                    Some(peer.to_string()),
                    false,
                )));
                serve_stream(stream, app, shutdown).await;
                return;
            };
            let connection = auth::ConnectionAuth::tcp(None, Some(peer.to_string()), true);
            let now = Instant::now();
            if let Some(block) = tls.auth.block_info(&connection, now) {
                metrics::inc(&metrics::AUTH_BLOCKED_REQUESTS);
                let retry_after = block.retry_after.as_secs().max(1);
                audit_tls(
                    tls.audit.as_deref(),
                    peer,
                    "rate_limited",
                    Some(audit::AuthAudit {
                        failures: block.failures,
                        blocked_for_secs: Some(retry_after),
                    }),
                );
                tracing::warn!(%peer, retry_after, "TLS client blocked after failed logins");
                return;
            }
            let acceptor = tls
                .acceptor
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            let mut handshake_shutdown = shutdown.clone();
            let stream = tokio::select! {
                _ = wait_for_shutdown(&mut handshake_shutdown) => return,
                accepted = acceptor.accept(stream) => match accepted {
                    Ok(stream) => stream,
                    Err(e) => {
                        tracing::debug!(?e, %peer, "TLS handshake failed");
                        return;
                    }
                },
            };
            let cert_fingerprint = match stream
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|certs| certs.first())
            {
                Some(cert) => {
                    if let Some(revocation) = &tls.revocation {
                        if let Err(reason) = revocation.check_peer(cert.as_ref()).await {
                            audit_tls(tls.audit.as_deref(), peer, "unauthorized", None);
                            tracing::warn!(%peer, %reason, "mTLS client rejected by revocation policy");
                            return;
                        }
                    }
                    let fingerprint = fingerprint(cert);
                    match tls.auth.registry().authenticate_certificate(&fingerprint) {
                        Ok(principal) => {
                            tracing::debug!(
                                %peer,
                                principal = principal.name(),
                                "mTLS client accepted"
                            );
                            Some(fingerprint)
                        }
                        Err(reason) => {
                            let failure = tls.auth.record_auth_failure(&connection, Instant::now());
                            let audit = failure.map(|failure| audit::AuthAudit {
                                failures: failure.failures,
                                blocked_for_secs: failure.blocked_for.map(|d| d.as_secs().max(1)),
                            });
                            if let Some(failure) = failure {
                                if let Some(blocked_for) = failure.blocked_for {
                                    metrics::inc(&metrics::AUTH_BLOCKED_REQUESTS);
                                    audit_tls(tls.audit.as_deref(), peer, "rate_limited", audit);
                                    tracing::warn!(
                                        %peer,
                                        %reason,
                                        retry_after = blocked_for.as_secs().max(1),
                                        "mTLS client rejected and blocked"
                                    );
                                    return;
                                }
                            }
                            audit_tls(tls.audit.as_deref(), peer, "unauthorized", audit);
                            tracing::warn!(%peer, %reason, "mTLS client rejected");
                            return;
                        }
                    }
                }
                None => None,
            };
            let app = app.clone().layer(Extension(auth::ConnectionAuth::tcp(
                cert_fingerprint,
                Some(peer.to_string()),
                true,
            )));
            serve_stream(stream, app, shutdown).await;
        });
    }
    drop(listener);
    drain_connections(connections, drain_timeout, "tcp").await;
    Ok(())
}

async fn serve_stream<I>(io: I, app: Router, mut shutdown: watch::Receiver<bool>)
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let io = TokioIo::new(io);
    let service = TowerToHyperService::new(app);
    let mut connection = Box::pin(http1::Builder::new().serve_connection(io, service));
    tokio::select! {
        result = &mut connection => {
            if let Err(err) = result {
                tracing::debug!(?err, "connection closed");
            }
        }
        _ = wait_for_shutdown(&mut shutdown) => {
            connection.as_mut().graceful_shutdown();
            if let Err(err) = connection.await {
                tracing::debug!(?err, "connection closed during drain");
            }
        }
    }
}

/// Periodic LMDB snapshots. With PITR enabled, snapshots are the verified
/// base for WAL retention: only a snapshot whose metadata was fsynced may
/// allow pruning older WAL segments.
pub(crate) fn spawn_snapshots(
    store: Arc<Store>,
    dir: PathBuf,
    wal_dir: PathBuf,
    database: String,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> Option<std::thread::JoinHandle<()>> {
    let interval = std::env::var("NONODB_SNAPSHOT_INTERVAL")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(300));
    let keep = std::env::var("NONODB_SNAPSHOT_KEEP")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(24);
    std::thread::Builder::new()
        .name(format!("nonodb-snapshots-{database}"))
        .spawn(move || {
            if let Err(e) = std::fs::create_dir_all(&dir) {
                metrics::record_snapshot_failure(&database);
                tracing::error!(?e, dir = %dir.display(), "cannot create snapshot directory");
                return;
            }
            let mut last_run = std::time::Instant::now()
                .checked_sub(interval)
                .unwrap_or_else(std::time::Instant::now);
            loop {
                if stop.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                let requested = crate::wal::take_snapshot_request(&store);
                if requested || last_run.elapsed() >= interval {
                    snapshot_once(&store, &dir, &wal_dir, &database, keep);
                    last_run = std::time::Instant::now();
                }
                // Wake quickly for WAL pressure and shutdown; the actual
                // snapshot cadence is still governed by `interval`.
                std::thread::sleep(Duration::from_millis(500));
            }
        })
        .ok()
}

fn snapshot_once(
    store: &Store,
    dir: &std::path::Path,
    wal_dir: &std::path::Path,
    database: &str,
    keep: usize,
) {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let path = dir.join(format!("data-{stamp}.mdb"));
    let head = store.replication_head().unwrap_or(0);
    let started = std::time::Instant::now();
    match store.copy_to_file(&path, nonodb_core::heed::CompactionOption::Enabled) {
        Ok(file) => {
            if let Err(e) = file.sync_all() {
                metrics::record_snapshot_failure(database);
                tracing::warn!(?e, "snapshot fsync failed");
                let _ = std::fs::remove_file(&path);
                return;
            }
            drop(file);
            if let Err(error) = nonodb_core::pitr::write_snapshot_meta(&path, database, head) {
                metrics::record_snapshot_failure(database);
                tracing::warn!(path = %path.display(), error = %error, "snapshot metadata write failed");
                let _ = std::fs::remove_file(&path);
                nonodb_core::pitr::remove_snapshot_meta(&path);
                return;
            }
            metrics::record_snapshot_success(database, started.elapsed());
            tracing::info!(path = %path.display(), head, "snapshot zapisany");
            let floor = prune_snapshots(dir, keep);
            if let Some(through) = floor {
                // The WAL thread owns the active file handle and performs the
                // actual prune safely after rotating if necessary.
                let _ = wal_dir;
                crate::wal::request_prune(store, through);
            }
        }
        Err(e) => {
            metrics::record_snapshot_failure(database);
            tracing::error!(?e, "snapshot failed");
        }
    }
}

/// Prunes old snapshots. Returns the replication head of the snapshot that
/// still covers the oldest retained restore point. `None` means no WAL may be
/// pruned (legacy/missing metadata or PITR disabled).
fn prune_snapshots(dir: &std::path::Path, keep: usize) -> Option<u64> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return None;
    };
    let mut files: Vec<(u64, PathBuf)> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("data-") && n.ends_with(".mdb"))
        })
        .filter_map(|path| snapshot_timestamp(&path).map(|stamp| (stamp, path)))
        .collect();
    files.sort_by_key(|(stamp, _)| *stamp);
    if files.is_empty() {
        return None;
    }
    if !crate::wal::enabled() {
        let excess = files.len().saturating_sub(keep);
        for (_, old) in files.into_iter().take(excess) {
            if let Err(e) = std::fs::remove_file(&old) {
                tracing::warn!(?e, path = %old.display(), "cannot remove old snapshot");
            }
            nonodb_core::pitr::remove_snapshot_meta(&old);
        }
        return None;
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let window_ms = crate::wal::retention_secs().saturating_mul(1000);
    let newest = files.len() - 1;
    // Keep the oldest retained base and the newest recovery point. The base is
    // the newest snapshot at or before the retention window; before a full
    // window has elapsed it is the oldest snapshot available.
    let floor = files
        .iter()
        .rposition(|(stamp, _)| stamp.saturating_add(window_ms) <= now)
        .unwrap_or(0);
    for (index, (_, path)) in files.iter().enumerate() {
        if index == floor || index == newest {
            continue;
        }
        if let Err(e) = std::fs::remove_file(path) {
            tracing::warn!(?e, path = %path.display(), "cannot remove old snapshot");
            continue;
        }
        nonodb_core::pitr::remove_snapshot_meta(path);
    }
    let meta = nonodb_core::pitr::read_snapshot_meta(&files[floor].1)
        .ok()
        .flatten()?;
    Some(meta.replication_head)
}

fn snapshot_timestamp(path: &std::path::Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let value = name.strip_prefix("data-")?.strip_suffix(".mdb")?;
    value.parse::<u64>().ok()
}

async fn no_store(request: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn http_access_log(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let started = std::time::Instant::now();
    let response = next.run(request).await;
    if !matches!(path.as_str(), "/healthz" | "/metrics") {
        tracing::info!(
            http.method = %method,
            http.path = %path,
            http.status = response.status().as_u16(),
            duration_ms = started.elapsed().as_secs_f64() * 1000.0,
            "http request"
        );
    }
    response
}

async fn health() -> Json<Value> {
    Json(json!({ "ok": true }))
}

/// Public capability contract used by clients and migration tooling.
async fn capabilities() -> Json<Value> {
    Json(server_info::capabilities())
}

/// Returns the actual listener addresses of this daemon (admin only).
async fn server(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    let (principal, started) =
        match auth_api::start_admin(&state, &connection, &headers, "server_info") {
            Ok(value) => value,
            Err(response) => return response,
        };
    audit_control(
        &state,
        &connection,
        principal.name(),
        "server_info",
        true,
        None,
        started,
        None,
    );
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "result": server_info::snapshot() })),
    )
}

/// Readiness checks the writer and a real LMDB read transaction. Liveness stays
/// cheap and public at `/healthz`; orchestrators should use `/readyz`.
async fn ready(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    let role = replicator::role();
    let writable = replicator::writable();
    let lease_valid = replicator::lease_valid();
    let read_only = !writable;
    let writer_required = replicator::role_is_primary();
    let writer_ok = !writer_required || (writable && state._runtime.writer_alive());
    let store = state.store.clone();
    let storage = tokio::task::spawn_blocking(move || {
        let txn = store.read_txn().map_err(|err| err.to_string())?;
        txn.commit().map_err(|err| err.to_string())
    })
    .await;
    let (storage_ok, storage_error) = match storage {
        Ok(Ok(())) => (true, None),
        Ok(Err(err)) => (false, Some(err)),
        Err(err) => (false, Some(format!("readiness task failed: {err}"))),
    };
    let ok = writer_ok && storage_ok;
    let status = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({
            "ok": ok,
            "checks": {
                "writer": writer_ok,
                "writer_required": writer_required,
                "lease_valid": lease_valid,
                "storage": storage_ok,
                "storage_error": storage_error,
            },
            "replication": {
                "role": role,
                "read_only": read_only,
                "write_url": replicator::write_url(state.databases.root()).unwrap_or(None),
            }
        })),
    )
}

async fn write_location(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let response = next.run(request).await;
    if response.status() != StatusCode::FORBIDDEN || replicator::writable() {
        return response;
    }
    let write_url = match replicator::write_url(state.databases.root()) {
        Ok(Some(write_url)) => write_url,
        _ => return response,
    };
    let (parts, body) = response.into_parts();
    let bytes = match axum::body::to_bytes(body, 64 * 1024).await {
        Ok(bytes) => bytes,
        Err(_) => return Response::from_parts(parts, Body::empty()),
    };
    let mut payload = serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null);
    let read_only = payload
        .pointer("/error/code")
        .and_then(Value::as_str)
        .is_some_and(|code| code == "read_only");
    if read_only {
        if let Some(object) = payload.get_mut("error").and_then(Value::as_object_mut) {
            object.insert("write_url".to_string(), Value::String(write_url.clone()));
        } else if let Some(object) = payload.as_object_mut() {
            object.insert("write_url".to_string(), Value::String(write_url.clone()));
        }
    }
    let body = if read_only {
        serde_json::to_vec(&payload).unwrap_or_else(|_| bytes.to_vec())
    } else {
        bytes.to_vec()
    };
    let mut response = Response::from_parts(parts, Body::from(body));
    if read_only {
        if let Ok(value) = HeaderValue::from_str(&write_url) {
            response
                .headers_mut()
                .insert(axum::http::header::LOCATION, value);
        }
        response
            .headers_mut()
            .remove(axum::http::header::CONTENT_LENGTH);
    }
    response
}

async fn ready_write(State(state): State<AppState>) -> axum::response::Response {
    use axum::response::IntoResponse;
    let writable = replicator::writable();
    let write_url = replicator::write_url(state.databases.root()).unwrap_or(None);
    let status = if writable {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let mut response = (
        status,
        Json(json!({
            "ok": writable,
            "role": replicator::role(),
            "epoch": replicator::epoch(),
            "write_url": write_url,
        })),
    )
        .into_response();
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-store"),
    );
    if let Some(write_url) = write_url {
        if let Ok(value) = HeaderValue::from_str(&write_url) {
            response
                .headers_mut()
                .insert(axum::http::header::LOCATION, value);
        }
    }
    response
}

async fn metrics_handler(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    let started = Instant::now();
    let principal = match authenticate_with_guard(&state, &connection, &headers) {
        Ok(principal) => principal,
        Err((err, auth_audit)) => {
            let status = error_status(&err);
            audit_control(
                &state,
                &connection,
                "anonymous",
                "metrics",
                false,
                Some(&err.message),
                started,
                auth_audit,
            );
            return (status, Json(json!({ "ok": false, "error": err })));
        }
    };
    if !principal.allows_metrics() {
        let err = ApiError::forbidden();
        audit_control(
            &state,
            &connection,
            principal.name(),
            "metrics",
            false,
            Some(&err.message),
            started,
            None,
        );
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "ok": false, "error": err })),
        );
    }
    audit_control(
        &state,
        &connection,
        principal.name(),
        "metrics",
        true,
        None,
        started,
        None,
    );
    if let Some(auth) = &state.auth {
        auth.refresh_blocked_clients();
    }
    (StatusCode::OK, Json(metrics::snapshot()))
}

async fn prometheus_handler(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
) -> Response {
    let started = Instant::now();
    let principal = match authenticate_with_guard(&state, &connection, &headers) {
        Ok(principal) => principal,
        Err((err, auth_audit)) => {
            let status = error_status(&err);
            audit_control(
                &state,
                &connection,
                "anonymous",
                "metrics_prometheus",
                false,
                Some(&err.message),
                started,
                auth_audit,
            );
            return (status, Json(json!({ "ok": false, "error": err }))).into_response();
        }
    };
    if !principal.allows_metrics() {
        let err = ApiError::forbidden();
        audit_control(
            &state,
            &connection,
            principal.name(),
            "metrics_prometheus",
            false,
            Some(&err.message),
            started,
            None,
        );
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "ok": false, "error": err })),
        )
            .into_response();
    }
    audit_control(
        &state,
        &connection,
        principal.name(),
        "metrics_prometheus",
        true,
        None,
        started,
        None,
    );
    if let Some(auth) = &state.auth {
        auth.refresh_blocked_clients();
    }
    let mut heads = Vec::new();
    let mut applied = Vec::new();
    let mut acks = Vec::new();
    let origin = writer::replication_origin();
    for name in state.databases.names().await {
        if let Ok(runtime) = state.databases.get(&name).await {
            if let Ok(head) = runtime.store.replication_head() {
                heads.push((name.clone(), head));
            }
            if let Ok(origins) = runtime.store.replication_applied_origins() {
                for (origin, seq) in origins {
                    let source_head = runtime.store.replication_source_head(&origin).unwrap_or(0);
                    let epoch = runtime
                        .store
                        .replication_applied_epoch(&origin)
                        .unwrap_or(0);
                    applied.push((name.clone(), origin, seq, source_head, epoch));
                }
            }
            if let Some(origin) = &origin {
                if let Ok(replica_acks) = runtime.store.replica_ack_details(origin) {
                    for (node, seq, at_ms) in replica_acks {
                        acks.push((name.clone(), origin.clone(), node, seq, at_ms));
                    }
                }
            }
        }
    }
    let role = replicator::role();
    let mut body = metrics::prometheus();
    metrics::append_replication_metrics(
        &mut body,
        origin.is_some(),
        origin.as_deref(),
        &role,
        replicator::epoch(),
        &heads,
        &applied,
        &acks,
    );
    (
        [(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
        )],
        body,
    )
        .into_response()
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn audit_control(
    state: &AppState,
    connection: &auth::ConnectionAuth,
    principal: &str,
    op: &str,
    ok: bool,
    error: Option<&str>,
    started: Instant,
    auth_audit: Option<audit::AuthAudit>,
) {
    if let Some(audit) = &state.audit {
        audit.record(
            principal,
            connection.source.as_str(),
            connection.peer.as_deref(),
            op,
            &[],
            ok,
            error,
            started.elapsed().as_secs_f64() * 1000.0,
            auth_audit,
        );
    }
}

fn audit_tls(
    audit: Option<&audit::AuditLog>,
    peer: std::net::SocketAddr,
    error: &str,
    auth_audit: Option<audit::AuthAudit>,
) {
    if let Some(audit) = audit {
        audit.record(
            "anonymous",
            "tcp-tls",
            Some(&peer.to_string()),
            "tls_handshake",
            &[],
            false,
            Some(error),
            0.0,
            auth_audit,
        );
    }
}

/// Authenticate one HTTP request and apply the per-IP failed-login guard.
pub(crate) fn authenticate_with_guard(
    state: &AppState,
    connection: &auth::ConnectionAuth,
    headers: &HeaderMap,
) -> Result<nonodb_auth::Principal, (ApiError, Option<audit::AuthAudit>)> {
    let now = Instant::now();
    if let Some(block) = state
        .auth
        .as_ref()
        .and_then(|auth| auth.block_info(connection, now))
    {
        metrics::inc(&metrics::AUTH_BLOCKED_REQUESTS);
        let retry_after = block.retry_after.as_secs().max(1);
        return Err((
            ApiError::rate_limited(retry_after),
            Some(audit::AuthAudit {
                failures: block.failures,
                blocked_for_secs: Some(retry_after),
            }),
        ));
    }

    match auth::authenticate(state.auth.as_deref(), connection, headers) {
        Ok(principal) => Ok(principal),
        Err(err) => {
            let failure = state
                .auth
                .as_ref()
                .and_then(|auth| auth.record_auth_failure(connection, now));
            let audit = failure.map(|failure| audit::AuthAudit {
                failures: failure.failures,
                blocked_for_secs: failure.blocked_for.map(|d| d.as_secs().max(1)),
            });
            if let Some(failure) = failure {
                if let Some(blocked_for) = failure.blocked_for {
                    metrics::inc(&metrics::AUTH_BLOCKED_REQUESTS);
                    return Err((ApiError::rate_limited(blocked_for.as_secs().max(1)), audit));
                }
            }
            Err((err, audit))
        }
    }
}

fn header_value(headers: &HeaderMap, name: &str) -> Result<Option<String>, ApiError> {
    match headers.get(name) {
        None => Ok(None),
        Some(value) => value
            .to_str()
            .map(|s| Some(s.to_string()))
            .map_err(|_| ApiError {
                code: "invalid_vault_key".into(),
                message: format!("header `{name}` is not valid UTF-8"),
            }),
    }
}

/// Parses the optional vault headers. All three must be present together.
fn vault_auth(headers: &HeaderMap) -> Result<Option<VaultAuth>, ApiError> {
    let tenant = header_value(headers, "x-nonodb-tenant")?;
    let kid = header_value(headers, "x-nonodb-key-id")?;
    let key = header_value(headers, "x-nonodb-key")?;
    if tenant.is_none() && kid.is_none() && key.is_none() {
        return Ok(None);
    }
    let (Some(tenant), Some(kid), Some(key)) = (tenant, kid, key) else {
        return Err(vault::key_required().into());
    };
    vault::validate_context(&tenant, &kid)?;
    let key = zeroize::Zeroizing::new(key);
    let key = VaultKey::from_base64(key.as_str())?;
    Ok(Some(VaultAuth { key, kid, tenant }))
}

fn normalize_filter(filter: Value) -> Value {
    if filter.is_null() {
        json!({})
    } else {
        filter
    }
}

fn cursor_sort_direction(sort: &[(String, i32)]) -> Result<bool, ApiError> {
    match sort {
        [] => Ok(true),
        [(field, direction)]
            if (field == "_id" || field == "$natural") && (*direction == 1 || *direction == -1) =>
        {
            Ok(*direction > 0)
        }
        _ => Err(ApiError {
            code: "invalid_query".to_string(),
            message: "cursor find requires an empty sort or a single _id/$natural sort".to_string(),
        }),
    }
}

fn cursor_pipeline(pipeline: &[Value]) -> Result<(Value, Vec<Value>), ApiError> {
    let (filter, stages) = match pipeline.first().and_then(|stage| {
        stage
            .as_object()
            .filter(|obj| obj.len() == 1)
            .and_then(|obj| obj.get("$match"))
    }) {
        Some(arg) => (normalize_filter(arg.clone()), &pipeline[1..]),
        None => (json!({}), pipeline),
    };
    for stage in stages {
        let obj = stage.as_object().ok_or_else(|| ApiError {
            code: "invalid_query".to_string(),
            message: "cursor aggregation stage must be an object".to_string(),
        })?;
        if obj.len() != 1 {
            return Err(ApiError {
                code: "invalid_query".to_string(),
                message: "cursor aggregation stage must contain exactly one operator".to_string(),
            });
        }
        let name = obj.keys().next().expect("one entry");
        if !matches!(name.as_str(), "$project" | "$addFields" | "$set" | "$unset") {
            return Err(ApiError {
                code: "invalid_query".to_string(),
                message: format!(
                    "cursor aggregation does not support stage `{name}`; use cursor_find or a regular aggregate"
                ),
            });
        }
    }
    Ok((filter, stages.to_vec()))
}

fn cursor_api_error(message: String) -> ApiError {
    let code = if message.contains("not found") || message.contains("expired") {
        "cursor_not_found"
    } else if message.contains("already") {
        "cursor_busy"
    } else {
        "cursor"
    };
    ApiError {
        code: code.to_string(),
        message,
    }
}

fn cursor_filter(base: &Value, last_id: Option<&Value>, ascending: bool) -> Value {
    let mut filters = vec![base.clone()];
    if let Some(last_id) = last_id {
        let op = if ascending { "$gt" } else { "$lt" };
        filters.push(json!({"_id": {op: last_id.clone()}}));
    }
    if filters.len() == 1 {
        filters.pop().expect("one filter")
    } else {
        json!({"$and": filters})
    }
}

fn cursor_page(
    store: &Store,
    auth: Option<&VaultAuth>,
    state: &mut cursor::State,
    batch_override: Option<usize>,
) -> Result<(Vec<Value>, bool), ApiError> {
    if let Some(batch_size) = batch_override {
        state.batch_size = cursor::normalize_batch(Some(batch_size));
    }
    let fetch = state.remaining.map_or(state.batch_size, |remaining| {
        remaining.min(state.batch_size)
    });
    if fetch == 0 {
        return Ok((Vec::new(), false));
    }
    if let cursor::Kind::MaterializedAggregate { coll, docs, offset } = &mut state.kind {
        let c = read_collection(store, coll, auth)?;
        let mut out = Vec::with_capacity(fetch);
        while out.len() < fetch && *offset < docs.len() {
            let start = *offset;
            let take = fetch.min(docs.len() - start);
            let page = c.retain_visible(docs[start..start + take].to_vec())?;
            *offset = start + take;
            out.extend(page);
        }
        let visible = out.len();
        let has_more =
            *offset < docs.len() && state.remaining.is_none_or(|remaining| remaining > visible);
        if let Some(remaining) = &mut state.remaining {
            *remaining = remaining.saturating_sub(visible);
        }
        return Ok((out, has_more));
    }

    let kind = state.kind.clone();
    let coll = kind.coll().to_string();
    let c = read_collection(store, &coll, auth)?;
    if c.profile().is_vault() {
        return Err(ApiError {
            code: "unsupported_encrypted_operation".to_string(),
            message: "cursors are not supported for vault collections".to_string(),
        });
    }
    let ascending = state.ascending();
    let filter = match &kind {
        cursor::Kind::Find { filter, sort, .. } => {
            let id_only = sort.is_empty()
                || (sort.len() == 1 && matches!(sort[0].0.as_str(), "_id" | "$natural"));
            if id_only {
                cursor_filter(filter, state.last_id.as_ref(), ascending)
            } else {
                filter.clone()
            }
        }
        cursor::Kind::Aggregate { filter, .. } => {
            cursor_filter(filter, state.last_id.as_ref(), ascending)
        }
        cursor::Kind::MaterializedAggregate { .. } => {
            unreachable!("materialized cursors are paged before reading the collection")
        }
    };
    let (comment, max_time_ms, hint, sort) = match &kind {
        cursor::Kind::Find {
            comment,
            max_time_ms,
            hint,
            sort,
            ..
        } => (comment.clone(), *max_time_ms, hint.clone(), sort.clone()),
        cursor::Kind::Aggregate {
            comment,
            max_time_ms,
            ..
        } => (comment.clone(), *max_time_ms, None, Vec::new()),
        cursor::Kind::MaterializedAggregate { .. } => {
            unreachable!("materialized cursors are paged before reading the collection")
        }
    };
    let id_only =
        sort.is_empty() || (sort.len() == 1 && matches!(sort[0].0.as_str(), "_id" | "$natural"));
    let sort = if id_only {
        if ascending {
            // The collection's natural order is ascending `_id`; leaving the
            // sort empty lets indexed filters stop after `skip + limit` instead
            // of materializing the whole candidate set for TopK.
            Vec::new()
        } else {
            vec![("_id".to_string(), -1)]
        }
    } else {
        sort
    };
    let deadline = max_time_ms
        .filter(|ms| *ms > 0)
        .and_then(|ms| Instant::now().checked_add(Duration::from_millis(ms)));
    check_cursor_deadline(deadline)?;
    let query = FindQuery {
        filter,
        projection: None,
        sort,
        skip: state.skip,
        limit: Some(fetch),
        comment,
        max_time_ms,
        hint,
    };
    let docs = c.find(&query)?;
    if docs.is_empty() {
        state.skip = 0;
        return Ok((Vec::new(), false));
    }
    if id_only {
        state.skip = 0;
        state.last_id = docs.last().and_then(|doc| doc.get("_id").cloned());
    } else {
        state.skip = state.skip.saturating_add(docs.len());
    }
    if let Some(remaining) = &mut state.remaining {
        *remaining = remaining.saturating_sub(docs.len());
    }
    let has_more = docs.len() == fetch && state.remaining.is_none_or(|remaining| remaining > 0);

    let out = match kind {
        cursor::Kind::Find { projection, .. } => docs
            .iter()
            .map(|doc| project_document(doc, projection.as_ref()))
            .collect(),
        cursor::Kind::Aggregate { stages, .. } => {
            let out = aggregate::run(docs, &stages, aggregate::max_docs())?;
            check_cursor_deadline(deadline)?;
            out
        }
        cursor::Kind::MaterializedAggregate { .. } => {
            unreachable!("materialized cursors are paged before running aggregation")
        }
    };
    Ok((out, has_more))
}

fn check_cursor_deadline(deadline: Option<Instant>) -> Result<(), ApiError> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        Err(nonodb_core::Error::Timeout("operation exceeded maxTimeMS".into()).into())
    } else {
        Ok(())
    }
}

async fn open_cursor(
    state: &AppState,
    auth: Option<VaultAuth>,
    mut cursor_state: cursor::State,
    batch_override: Option<usize>,
) -> (StatusCode, Json<ApiResponse>) {
    let registry = state.cursors.clone();
    let cursor_id = match registry.open(cursor_state.clone()) {
        Ok(cursor_id) => cursor_id,
        Err(message) => return respond(Err(cursor_api_error(message))),
    };
    let store = state.store.clone();
    let joined = tokio::task::spawn_blocking(move || {
        let result = cursor_page(&store, auth.as_ref(), &mut cursor_state, batch_override);
        let has_more = result.as_ref().map(|(_, more)| *more).unwrap_or(false);
        registry.finish(cursor_id, cursor_state, has_more);
        result.map(|(docs, has_more)| {
            json!({
                "cursor_id": cursor_id,
                "docs": docs,
                "has_more": has_more,
            })
        })
    });
    read_result(joined).await
}

/// One frame for the streaming `/v1/watch` endpoint.
fn watch_frame(sse: bool, event: &Value) -> Bytes {
    let mut out = Vec::with_capacity(256);
    append_watch_frame(&mut out, sse, event);
    Bytes::from(out)
}

/// Appends one frame to an existing buffer so a poll page is sent as a single
/// channel message instead of one message per event.
fn append_watch_frame(out: &mut Vec<u8>, sse: bool, event: &Value) {
    if sse {
        let id = event
            .get("_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .replace(['\r', '\n'], "");
        out.extend_from_slice(b"event: message\nid: ");
        out.extend_from_slice(id.as_bytes());
        out.extend_from_slice(b"\ndata: ");
    }
    if serde_json::to_writer(&mut *out, event).is_err() {
        out.extend_from_slice(b"{}");
    }
    out.extend_from_slice(if sse { b"\n\n" } else { b"\n" });
}

fn watch_control(sse: bool, name: &str, value: &Value) -> Bytes {
    if sse {
        let data = serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string());
        return Bytes::from(format!("event: {name}\ndata: {data}\n\n"));
    }
    let value = if name == "ready" {
        json!({"stream": value})
    } else {
        value.clone()
    };
    let data = serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string());
    Bytes::from(format!("{data}\n"))
}

struct ThresholdWindow {
    started: Instant,
    count: usize,
    emitted: bool,
}

struct ThresholdState {
    spec: Option<protocol::WatchThreshold>,
    groups: HashMap<String, ThresholdWindow>,
}

impl ThresholdState {
    fn new(spec: Option<protocol::WatchThreshold>) -> Self {
        Self {
            spec: spec.filter(|spec| spec.count > 0),
            groups: HashMap::new(),
        }
    }

    fn apply(&mut self, event: &Value) -> Option<Value> {
        let Some(spec) = &self.spec else {
            return Some(event.clone());
        };
        let now = Instant::now();
        let window = Duration::from_millis(spec.window_ms.unwrap_or(60_000).max(1));
        let mut key_parts = Vec::new();
        for field in &spec.group_by {
            key_parts.push(
                nonodb_core::query::path::get(event, field)
                    .cloned()
                    .unwrap_or(Value::Null),
            );
        }
        let key = serde_json::to_string(&key_parts).unwrap_or_else(|_| "[]".to_string());
        let window_state = self
            .groups
            .entry(key.clone())
            .or_insert_with(|| ThresholdWindow {
                started: now,
                count: 0,
                emitted: false,
            });
        if now.duration_since(window_state.started) >= window {
            *window_state = ThresholdWindow {
                started: now,
                count: 0,
                emitted: false,
            };
        }
        window_state.count += 1;
        if window_state.emitted || window_state.count < spec.count {
            metrics::inc(&metrics::WATCH_THRESHOLD_SUPPRESSED);
            return None;
        }
        window_state.emitted = true;
        metrics::inc(&metrics::WATCH_THRESHOLD_EMITTED);
        let mut out = event.clone();
        if let Some(object) = out.as_object_mut() {
            object.insert(
                "threshold".into(),
                json!({
                    "count": window_state.count,
                    "window_ms": spec.window_ms.unwrap_or(60_000),
                    "group_by": spec.group_by,
                    "group": key_parts,
                }),
            );
        }
        Some(out)
    }
}

#[allow(clippy::too_many_arguments)]
async fn live_stream_loop(
    mut live_rx: tokio::sync::mpsc::Receiver<Value>,
    live_id: u64,
    heartbeat: Duration,
    deadline: Option<Instant>,
    sse: bool,
    tx: tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> Result<(), ApiError> {
    let ready = json!({"mode": "live", "subscription": live_id});
    if tx
        .send(Ok(watch_control(sse, "ready", &ready)))
        .await
        .is_err()
    {
        return Ok(());
    }
    let mut last_sent = Instant::now();
    loop {
        if tx.is_closed() {
            return Ok(());
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Ok(());
        }
        let wait = heartbeat.saturating_sub(last_sent.elapsed());
        tokio::select! {
            item = live_rx.recv() => {
                let Some(event) = item else {
                    return Ok(());
                };
                if tx.send(Ok(watch_frame(sse, &event))).await.is_err() {
                    return Ok(());
                }
                metrics::inc(&metrics::WATCH_EVENTS);
                last_sent = Instant::now();
            }
            _ = tokio::time::sleep(wait) => {
                let value = json!({"heartbeat": true, "ts_ms": now_ms()});
                if tx.send(Ok(watch_control(sse, "heartbeat", &value))).await.is_err() {
                    return Ok(());
                }
                live::registry().touch(live_id);
                metrics::inc(&metrics::WATCH_HEARTBEATS);
                last_sent = Instant::now();
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn watch_stream_loop(
    store: Arc<Store>,
    coll: String,
    auth: Option<VaultAuth>,
    mut after: String,
    limit: usize,
    types: Vec<String>,
    filter: Value,
    projection: Option<Value>,
    heartbeat: Duration,
    overlap: Duration,
    deadline: Option<Instant>,
    threshold: Option<protocol::WatchThreshold>,
    sse: bool,
    truncated: bool,
    oldest: Option<String>,
    tx: tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> Result<(), ApiError> {
    let mut ready = serde_json::Map::new();
    ready.insert("truncated".into(), Value::Bool(truncated));
    ready.insert(
        "oldest".into(),
        serde_json::to_value(oldest).unwrap_or(Value::Null),
    );
    if tx
        .send(Ok(watch_control(sse, "ready", &Value::Object(ready))))
        .await
        .is_err()
    {
        return Ok(());
    }

    let overlap_ms = overlap.as_millis() as u64;
    let seen_ttl = overlap * 2 + Duration::from_secs(5);
    let mut seen: HashMap<String, Instant> = HashMap::new();
    let mut last_late_scan = Instant::now() - Duration::from_secs(1);
    let mut last_write = Instant::now();
    let mut thresholds = ThresholdState::new(threshold);
    loop {
        if tx.is_closed() {
            return Ok(());
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Ok(());
        }
        let store_now = store.clone();
        let coll_now = coll.clone();
        let auth_now = auth.clone();
        let after_now = after.clone();
        let types_now = types.clone();
        let filter_now = filter.clone();
        let projection_now = projection.clone();
        let joined = tokio::task::spawn_blocking(move || {
            watch_once(
                &store_now,
                &coll_now,
                auth_now.as_ref(),
                &after_now,
                limit,
                &types_now,
                Some(&filter_now),
                projection_now.as_ref(),
            )
        });
        let (main_events, next, has_more) = match joined.await {
            Ok(Ok(value)) => value,
            Ok(Err(err)) => {
                let _ = tx
                    .send(Ok(watch_control(sse, "error", &json!({"error": err}))))
                    .await;
                return Err(err);
            }
            Err(e) => {
                let err = ApiError::internal(format!("join error: {e}"));
                let _ = tx
                    .send(Ok(watch_control(sse, "error", &json!({"error": err}))))
                    .await;
                return Err(err);
            }
        };
        let main_len = main_events.len();
        let mut events = main_events;

        // Concurrent producers can commit lower event ids after a higher id was
        // already delivered. Rescan a short recent window and suppress repeats.
        if overlap_ms > 0
            && !after.is_empty()
            && last_late_scan.elapsed() >= Duration::from_millis(200)
        {
            let after_ts = event_timestamp_ms(&after).unwrap_or(now_ms() as u64);
            let floor = event_id_from_ms(after_ts.saturating_sub(overlap_ms));
            let store_now = store.clone();
            let coll_now = coll.clone();
            let auth_now = auth.clone();
            let after_now = after.clone();
            let types_now = types.clone();
            let filter_now = filter.clone();
            let projection_now = projection.clone();
            let joined = tokio::task::spawn_blocking(move || {
                watch_late_scan(
                    &store_now,
                    &coll_now,
                    auth_now.as_ref(),
                    &floor,
                    &after_now,
                    10_000,
                    &types_now,
                    Some(&filter_now),
                    projection_now.as_ref(),
                )
            });
            match joined.await {
                Ok(Ok(late)) => events.extend(late),
                Ok(Err(err)) => {
                    let _ = tx
                        .send(Ok(watch_control(sse, "error", &json!({"error": err}))))
                        .await;
                    return Err(err);
                }
                Err(e) => {
                    let err = ApiError::internal(format!("join error: {e}"));
                    let _ = tx
                        .send(Ok(watch_control(sse, "error", &json!({"error": err}))))
                        .await;
                    return Err(err);
                }
            }
            last_late_scan = Instant::now();
        }

        let now = Instant::now();
        seen.retain(|_, expires| *expires > now);
        events.sort_by(|left, right| {
            left.get("_id")
                .and_then(Value::as_str)
                .cmp(&right.get("_id").and_then(Value::as_str))
        });
        events.retain(|event| {
            let Some(event_id) = event.get("_id").and_then(Value::as_str) else {
                return false;
            };
            if seen.contains_key(event_id) {
                return false;
            }
            seen.insert(event_id.to_string(), now + seen_ttl);
            true
        });
        if !events.is_empty() {
            let mut sent = 0usize;
            let mut batch: Vec<u8> = Vec::with_capacity(events.len() * 160);
            for event in &events {
                let Some(event) = thresholds.apply(event) else {
                    continue;
                };
                append_watch_frame(&mut batch, sse, &event);
                sent += 1;
            }
            if sent > 0 {
                if tx.send(Ok(Bytes::from(batch))).await.is_err() {
                    return Ok(());
                }
                metrics::add(&metrics::WATCH_EVENTS, sent as u64);
                last_write = Instant::now();
            }
        }
        if main_len > 0 {
            after = next;
            if has_more {
                continue;
            }
        }

        if last_write.elapsed() >= heartbeat {
            let value = json!({"heartbeat": true, "ts_ms": now_ms()});
            if tx
                .send(Ok(watch_control(sse, "heartbeat", &value)))
                .await
                .is_err()
            {
                return Ok(());
            }
            metrics::inc(&metrics::WATCH_HEARTBEATS);
            last_write = Instant::now();
        }
        // Wake as soon as the writer commits, with the poll interval as an
        // upper bound so a missed notification cannot stall the stream.
        tokio::select! {
            _ = live::stream_notify().notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

/// Streaming sibling of the long-poll `watch` operation.
///
/// `Accept: text/event-stream` selects SSE; otherwise the response is NDJSON.
/// The stream stays open until the client disconnects or `timeout_ms` expires.
async fn watch_stream(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Json(req): Json<Request>,
) -> Response {
    metrics::inc(&metrics::REQUESTS);
    let started = Instant::now();
    let database = database_from_uri(&uri);
    let collections = req.collections();
    let response = watch_stream_inner(
        State(state),
        Extension(connection),
        OriginalUri(uri),
        headers,
        Json(req),
    )
    .await;
    if response.status() != StatusCode::UNAUTHORIZED && response.status() != StatusCode::FORBIDDEN {
        metrics::record_collection_request(
            &database,
            &collections,
            true,
            response.status().is_success(),
            started.elapsed(),
        );
    }
    response
}

async fn watch_stream_inner(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Json(req): Json<Request>,
) -> Response {
    let started = Instant::now();
    let principal = match authenticate_with_guard(&state, &connection, &headers) {
        Ok(principal) => principal,
        Err((err, auth_audit)) => {
            audit_denied(
                &state,
                &connection,
                "anonymous",
                &req,
                &err,
                started,
                auth_audit,
            );
            return respond(Err(err)).into_response();
        }
    };
    let database = database_from_uri(&uri);
    if let Err(err) = authz::authorize_request(&principal, &req, &database) {
        audit_denied(
            &state,
            &connection,
            principal.name(),
            &req,
            &err,
            started,
            None,
        );
        return respond(Err(err)).into_response();
    }
    let state = match state.for_database(&database).await {
        Ok(state) => state,
        Err(err) => return respond(Err(err)).into_response(),
    };
    let audit_needed = state.audit.as_ref().is_some_and(|audit| audit.log_reads());
    let audit_op = req.op_name();
    let audit_collections: Vec<String> = req.collections();
    let vault_auth = match vault_auth(&headers) {
        Ok(auth) => auth,
        Err(err) => {
            audit_error(
                &state,
                &connection,
                principal.name(),
                audit_op,
                &audit_collections,
                &err,
                started,
                audit_needed,
            );
            return respond(Err(err)).into_response();
        }
    };
    let Request::Watch {
        coll,
        after,
        limit,
        timeout_ms,
        types,
        filter,
        projection,
        heartbeat_ms,
        overlap_ms,
        threshold,
        mode,
        ops,
        count,
        once,
        event_type,
        name,
    } = req
    else {
        let err = ApiError {
            code: "invalid_request".to_string(),
            message: "/v1/watch accepts only the `watch` operation".to_string(),
        };
        audit_error(
            &state,
            &connection,
            principal.name(),
            audit_op,
            &audit_collections,
            &err,
            started,
            audit_needed,
        );
        return respond(Err(err)).into_response();
    };

    if mode.as_deref() == Some("live") {
        if after.is_some() || types.is_some() || threshold.is_some() {
            let err = ApiError {
                code: "invalid_request".into(),
                message: "live mode does not support after/types/threshold".into(),
            };
            audit_error(
                &state,
                &connection,
                principal.name(),
                audit_op,
                &audit_collections,
                &err,
                started,
                audit_needed,
            );
            return respond(Err(err)).into_response();
        }
        let profile = match state.store.collection_profile(&coll) {
            Ok(Some(profile)) => profile,
            Ok(None) => {
                let err = ApiError {
                    code: "invalid_request".into(),
                    message: format!("collection `{coll}` does not exist"),
                };
                return respond(Err(err)).into_response();
            }
            Err(err) => return respond(Err(ApiError::from(err))).into_response(),
        };
        if profile.is_vault() {
            let err = ApiError {
                code: "invalid_request".into(),
                message: "live subscriptions are not supported for vault collections".into(),
            };
            return respond(Err(err)).into_response();
        }
        let mut subscription = nonodb_core::subscriptions::Subscription {
            name: name.unwrap_or_else(|| "live".into()),
            ops: ops.unwrap_or_default(),
            filter: filter.unwrap_or_else(|| json!({})),
            count,
            once: once.unwrap_or(false),
            event_type,
            last_matched: false,
            lease_seconds: 0,
            expires_at: 0,
        };
        if subscription.filter.is_null() {
            subscription.filter = json!({});
        }
        if let Some(count) = &mut subscription.count {
            if count.filter.is_null() {
                count.filter = json!({});
            }
        }
        if let Err(err) = subscription.validate() {
            let err = ApiError::from(err);
            audit_error(
                &state,
                &connection,
                principal.name(),
                audit_op,
                &audit_collections,
                &err,
                started,
                audit_needed,
            );
            return respond(Err(err)).into_response();
        }
        let heartbeat = Duration::from_millis(heartbeat_ms.unwrap_or(10_000).clamp(100, 10_000));
        let deadline = timeout_ms
            .filter(|timeout| *timeout > 0)
            .and_then(|timeout| Instant::now().checked_add(Duration::from_millis(timeout)));
        let sse = headers
            .get(ACCEPT)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("text/event-stream"));
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);
        let (live_tx, live_rx) = tokio::sync::mpsc::channel::<Value>(64);
        let (live_id, guard) =
            live::registry().register(state.store.id(), coll.clone(), subscription, live_tx);
        let stream = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });
        metrics::inc(&metrics::READ_OPS);
        metrics::inc(&metrics::WATCH_STREAMS);
        metrics::inc(&metrics::WATCH_ACTIVE);
        tokio::spawn(async move {
            let result = live_stream_loop(live_rx, live_id, heartbeat, deadline, sse, tx).await;
            drop(guard);
            metrics::sub(&metrics::WATCH_ACTIVE, 1);
            if audit_needed {
                if let Some(audit) = &state.audit {
                    audit.record(
                        principal.name(),
                        connection.source.as_str(),
                        connection.peer.as_deref(),
                        "watch_stream",
                        &[&coll],
                        result.is_ok(),
                        result.as_ref().err().map(|err| err.code.as_str()),
                        started.elapsed().as_secs_f64() * 1000.0,
                        None,
                    );
                }
            }
        });
        let content_type = if sse {
            "text/event-stream"
        } else {
            "application/x-ndjson"
        };
        let mut response = Body::from_stream(stream).into_response();
        response.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static(content_type),
        );
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response.headers_mut().insert(
            axum::http::header::HeaderName::from_static("x-accel-buffering"),
            HeaderValue::from_static("no"),
        );
        return response;
    }

    let limit = limit.unwrap_or(100).clamp(1, 1_000);
    let heartbeat = Duration::from_millis(heartbeat_ms.unwrap_or(15_000).clamp(100, 60_000));
    let overlap = Duration::from_millis(overlap_ms.unwrap_or(2_000).min(60_000));
    let deadline = timeout_ms
        .filter(|timeout| *timeout > 0)
        .and_then(|timeout| Instant::now().checked_add(Duration::from_millis(timeout)));
    let types = types.unwrap_or_default();
    let after = after.unwrap_or_default();
    let filter = filter.unwrap_or_else(|| json!({}));

    let status_store = state.store.clone();
    let status_coll = coll.clone();
    let status_auth = vault_auth.clone();
    let status_after = after.clone();
    let status = tokio::task::spawn_blocking(move || {
        watch_status(
            &status_store,
            &status_coll,
            status_auth.as_ref(),
            &status_after,
        )
    })
    .await;
    let (truncated, oldest) = match status {
        Ok(Ok(value)) => value,
        Ok(Err(err)) => {
            audit_error(
                &state,
                &connection,
                principal.name(),
                audit_op,
                &audit_collections,
                &err,
                started,
                audit_needed,
            );
            return respond(Err(err)).into_response();
        }
        Err(e) => {
            let err = ApiError::internal(format!("join error: {e}"));
            return respond(Err(err)).into_response();
        }
    };
    if truncated {
        metrics::inc(&metrics::WATCH_TRUNCATED);
    }

    let sse = headers
        .get(ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("text/event-stream"));
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });

    metrics::inc(&metrics::READ_OPS);
    metrics::inc(&metrics::WATCH_STREAMS);
    metrics::inc(&metrics::WATCH_ACTIVE);
    let task_state = state.clone();
    let task_coll = coll.clone();
    let audit_connection = connection.clone();
    let audit_principal = principal.name().to_string();
    tokio::spawn(async move {
        let result = watch_stream_loop(
            task_state.store.clone(),
            coll,
            vault_auth,
            after,
            limit,
            types,
            filter,
            projection,
            heartbeat,
            overlap,
            deadline,
            threshold,
            sse,
            truncated,
            oldest,
            tx,
        )
        .await;
        metrics::sub(&metrics::WATCH_ACTIVE, 1);
        if audit_needed {
            if let Some(audit) = &task_state.audit {
                audit.record(
                    &audit_principal,
                    audit_connection.source.as_str(),
                    audit_connection.peer.as_deref(),
                    "watch_stream",
                    &[&task_coll],
                    result.is_ok(),
                    result.as_ref().err().map(|err| err.code.as_str()),
                    started.elapsed().as_secs_f64() * 1000.0,
                    None,
                );
            }
        }
    });

    let content_type = if sse {
        "text/event-stream"
    } else {
        "application/x-ndjson"
    };
    let mut response = Body::from_stream(stream).into_response();
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static(content_type),
    );
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        axum::http::header::HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    response
}

async fn exec(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Json(req): Json<Request>,
) -> (StatusCode, Json<ApiResponse>) {
    metrics::inc(&metrics::REQUESTS);
    let started = std::time::Instant::now();
    let database = database_from_uri(&uri);
    let collections = req.collections();
    let read_only = req.is_read_only();
    let operation = req.op_name();
    let response = exec_inner(
        State(state),
        Extension(connection),
        OriginalUri(uri),
        headers,
        Json(req),
    )
    .await;
    // Do not create per-collection series for unauthenticated/unauthorized
    // names. The global request/error counters still see those attempts.
    if response.0 != StatusCode::UNAUTHORIZED && response.0 != StatusCode::FORBIDDEN {
        metrics::record_collection_request(
            &database,
            &collections,
            read_only,
            response.1.ok,
            started.elapsed(),
        );
    }
    // Long-poll and streaming watches are intentionally excluded: their
    // duration reflects client waiting time, not request execution cost.
    if operation != "watch" {
        metrics::record_latency(operation, started.elapsed());
    }
    response
}

async fn exec_inner(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Json(req): Json<Request>,
) -> (StatusCode, Json<ApiResponse>) {
    let started = std::time::Instant::now();
    let principal = match authenticate_with_guard(&state, &connection, &headers) {
        Ok(principal) => principal,
        Err((err, auth_audit)) => {
            audit_denied(
                &state,
                &connection,
                "anonymous",
                &req,
                &err,
                started,
                auth_audit,
            );
            return respond(Err(err));
        }
    };
    let database = database_from_uri(&uri);
    if let Err(err) = authz::authorize_request(&principal, &req, &database) {
        audit_denied(
            &state,
            &connection,
            principal.name(),
            &req,
            &err,
            started,
            None,
        );
        return respond(Err(err));
    }
    let state = match state.for_database(&database).await {
        Ok(state) => state,
        Err(err) => return respond(Err(err)),
    };
    if let Err(err) = authorize_graph_edges(&state.store, &principal, &req, &database) {
        audit_denied(
            &state,
            &connection,
            principal.name(),
            &req,
            &err,
            started,
            None,
        );
        return respond(Err(err));
    }
    let audit_needed = state
        .audit
        .as_ref()
        .is_some_and(|audit| audit.log_reads() || !req.is_read_only());
    let audit_op = req.op_name();
    let audit_collections: Vec<String> = req.collections();
    let vault_auth = match vault_auth(&headers) {
        Ok(auth) => auth,
        Err(err) => {
            audit_error(
                &state,
                &connection,
                principal.name(),
                audit_op,
                &audit_collections,
                &err,
                started,
                audit_needed,
            );
            return respond(Err(err));
        }
    };
    let auth_generation = state.auth.as_ref().map_or(0, |auth| auth.generation());
    let cache_key = if vault_auth.is_none() && cacheable(&req) {
        state.cache.key(&(principal.name(), auth_generation, &req))
    } else {
        None
    };
    let cache_version = state.cache.version();
    if let Some(key) = cache_key.as_deref() {
        if let Some(value) = state.cache.get(key) {
            if vault_auth.is_none() {
                state.auto_index.record(&req, false);
            }
            metrics::record_collection_cache(&database, &audit_collections, true);
            metrics::inc(&metrics::READ_OPS);
            let response = respond(Ok(value));
            audit_response(
                &state,
                &connection,
                principal.name(),
                audit_op,
                &audit_collections,
                &response,
                started,
                audit_needed,
            );
            return response;
        }
    }
    if cache_key.is_some() {
        metrics::record_collection_cache(&database, &audit_collections, false);
    }
    if vault_auth.is_none() {
        state.auto_index.record(&req, true);
    }

    let response = match req {
        Request::Find {
            coll,
            filter,
            projection,
            sort,
            skip,
            limit,
            comment,
            max_time_ms,
            hint,
        } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                let query = FindQuery {
                    filter,
                    projection,
                    sort,
                    skip,
                    limit,
                    comment,
                    max_time_ms,
                    hint,
                };
                if c.profile().is_vault() {
                    let auth = auth.as_ref().ok_or_else(vault::key_required)?;
                    let rtxn = store.read_txn()?;
                    let out = c.find_vault_in(&rtxn, &query, &auth.key, &auth.tenant, &auth.kid)?;
                    rtxn.commit().map_err(nonodb_core::Error::from)?;
                    Ok::<FindOutput, ApiError>(FindOutput::Value(Value::Array(out)))
                } else {
                    if auth.is_some() {
                        return Err(vault::key_unexpected().into());
                    }
                    match c.find_raw(&query)? {
                        Some(docs) => Ok(FindOutput::Raw(docs)),
                        None => Ok(FindOutput::Value(Value::Array(c.find(&query)?))),
                    }
                }
            });
            read_find_result(joined).await
        }

        Request::Watch {
            coll,
            after,
            limit,
            timeout_ms,
            types,
            filter,
            projection,
            heartbeat_ms: _,
            overlap_ms: _,
            threshold,
            mode,
            ..
        } => {
            if mode.as_deref() == Some("live") {
                return respond(Err(ApiError {
                    code: "invalid_request".into(),
                    message: "live mode requires POST /v1/watch".into(),
                }));
            }
            if threshold.is_some() {
                return respond(Err(ApiError {
                    code: "invalid_query".into(),
                    message: "threshold is only supported by /v1/watch streaming".into(),
                }));
            }
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let limit = limit.unwrap_or(100).clamp(1, 1_000);
            let timeout_ms = timeout_ms.unwrap_or(1_000).min(25_000);
            let types = types.unwrap_or_default();
            let after = after.unwrap_or_default();
            let status_store = store.clone();
            let status_coll = coll.clone();
            let status_auth = auth.clone();
            let status_after = after.clone();
            let status = tokio::task::spawn_blocking(move || {
                watch_status(
                    &status_store,
                    &status_coll,
                    status_auth.as_ref(),
                    &status_after,
                )
            })
            .await;
            let (truncated, oldest) = match status {
                Ok(Ok(value)) => value,
                Ok(Err(err)) => return respond(Err(err)),
                Err(e) => return respond(Err(ApiError::internal(format!("join error: {e}")))),
            };
            if truncated {
                metrics::inc(&metrics::WATCH_TRUNCATED);
            }
            let deadline = Instant::now() + Duration::from_millis(timeout_ms);
            loop {
                let store = store.clone();
                let coll = coll.clone();
                let auth = auth.clone();
                let after_now = after.clone();
                let types_now = types.clone();
                let filter_now = filter.clone();
                let projection_now = projection.clone();
                let joined = tokio::task::spawn_blocking(move || {
                    watch_once(
                        &store,
                        &coll,
                        auth.as_ref(),
                        &after_now,
                        limit,
                        &types_now,
                        filter_now.as_ref(),
                        projection_now.as_ref(),
                    )
                });
                let result = match joined.await {
                    Ok(inner) => inner,
                    Err(e) => return respond(Err(ApiError::internal(format!("join error: {e}")))),
                };
                let (events, next, has_more) = match result {
                    Ok(value) => value,
                    Err(err) => return respond(Err(err)),
                };
                if !events.is_empty() || has_more || Instant::now() >= deadline {
                    metrics::inc(&metrics::READ_OPS);
                    let timed_out = events.is_empty() && !has_more;
                    if timed_out {
                        metrics::inc(&metrics::WATCH_TIMEOUTS);
                    }
                    metrics::add(&metrics::WATCH_EVENTS, events.len() as u64);
                    return respond(Ok(json!({
                        "events": events,
                        "next": next,
                        "timed_out": timed_out,
                        "has_more": has_more,
                        "truncated": truncated,
                        "oldest": oldest,
                    })));
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        Request::FindOne {
            coll,
            filter,
            projection,
            comment,
            max_time_ms,
            hint,
        } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                let query = FindQuery {
                    filter,
                    projection,
                    sort: Vec::new(),
                    skip: 0,
                    limit: Some(1),
                    comment,
                    max_time_ms,
                    hint,
                };
                if c.profile().is_vault() {
                    let auth = auth.as_ref().ok_or_else(vault::key_required)?;
                    let rtxn = store.read_txn()?;
                    let out = c.find_vault_in(&rtxn, &query, &auth.key, &auth.tenant, &auth.kid)?;
                    rtxn.commit().map_err(nonodb_core::Error::from)?;
                    Ok::<Value, ApiError>(out.into_iter().next().unwrap_or(Value::Null))
                } else {
                    if auth.is_some() {
                        return Err(vault::key_unexpected().into());
                    }
                    Ok::<Value, ApiError>(c.find(&query)?.into_iter().next().unwrap_or(Value::Null))
                }
            });
            read_result(joined).await
        }

        Request::Lookup {
            coll,
            by,
            values,
            fields,
            projection,
        } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let started = std::time::Instant::now();
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "lookup on vault collections")?;
                let by = by
                    .unwrap_or(LookupBy::Field("_id".to_string()))
                    .into_fields();
                let projection = lookup_projection(fields.as_deref(), projection.as_ref())?;
                let report = c.lookup_fields_report(&by, &values, projection.as_ref());
                lookup_response(&coll, &by, report, started.elapsed(), false)
            });
            read_result(joined).await
        }

        Request::ExplainLookup {
            coll,
            by,
            values,
            fields,
            projection,
        } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let started = std::time::Instant::now();
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "explain_lookup on vault collections")?;
                let by = by
                    .unwrap_or(LookupBy::Field("_id".to_string()))
                    .into_fields();
                let projection = lookup_projection(fields.as_deref(), projection.as_ref())?;
                let report = c.lookup_fields_report(&by, &values, projection.as_ref());
                lookup_response(&coll, &by, report, started.elapsed(), true)
            });
            read_result(joined).await
        }

        Request::CursorFind {
            coll,
            filter,
            projection,
            sort,
            skip,
            limit,
            batch_size,
            comment,
            max_time_ms,
            hint,
        } => {
            let ascending = match cursor_sort_direction(&sort) {
                Ok(direction) => direction,
                Err(err) => return respond(Err(err)),
            };
            let kind = cursor::Kind::Find {
                coll,
                filter: normalize_filter(filter),
                projection,
                ascending,
                sort,
                skip,
                comment,
                max_time_ms,
                hint,
            };
            let cursor_state = cursor::State::new(
                principal.name().to_string(),
                database.clone(),
                kind,
                limit,
                batch_size,
            );
            open_cursor(&state, vault_auth.clone(), cursor_state, None).await
        }

        Request::CursorAggregate {
            coll,
            pipeline,
            limit,
            batch_size,
            comment,
            max_time_ms,
        } => {
            let (filter, stages) = match cursor_pipeline(&pipeline) {
                Ok(parts) => parts,
                Err(err) => return respond(Err(err)),
            };
            let kind = cursor::Kind::Aggregate {
                coll,
                filter,
                stages,
                comment,
                max_time_ms,
            };
            let cursor_state = cursor::State::new(
                principal.name().to_string(),
                database.clone(),
                kind,
                limit,
                batch_size,
            );
            open_cursor(&state, vault_auth.clone(), cursor_state, None).await
        }

        Request::GetMore {
            cursor_id,
            batch_size,
        } => {
            let owner = principal.name().to_string();
            let cursors = state.cursors.clone();
            let mut cursor_state = match cursors.begin(cursor_id, &owner) {
                Ok(state) => state,
                Err(err) => return respond(Err(cursor_api_error(err))),
            };
            let runtime = match state.databases.get(&cursor_state.database).await {
                Ok(runtime) => runtime,
                Err(err) => {
                    cursors.finish(cursor_id, cursor_state, true);
                    return respond(Err(err));
                }
            };
            let store = runtime.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let result = cursor_page(&store, auth.as_ref(), &mut cursor_state, batch_size);
                let has_more = result.as_ref().map(|(_, more)| *more).unwrap_or(false);
                cursors.finish(cursor_id, cursor_state, has_more);
                result.map(|(docs, has_more)| {
                    json!({
                        "cursor_id": cursor_id,
                        "docs": docs,
                        "has_more": has_more,
                    })
                })
            });
            read_result(joined).await
        }

        Request::CloseCursor { cursor_id } => {
            let closed = state.cursors.close(cursor_id, principal.name());
            return respond(Ok(json!({"cursor_id": cursor_id, "closed": closed})));
        }

        Request::Explain {
            coll,
            filter,
            sort,
            skip,
            limit,
            comment,
            max_time_ms,
            hint,
        } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "explain")?;
                let query = FindQuery {
                    filter,
                    sort,
                    skip,
                    limit,
                    projection: None,
                    comment,
                    max_time_ms,
                    hint,
                };
                Ok::<Value, ApiError>(c.explain(&query)?)
            });
            read_result(joined).await
        }

        Request::Count {
            coll,
            filter,
            comment: _,
            max_time_ms,
        } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                if c.profile().is_vault() {
                    let auth = auth.as_ref().ok_or_else(vault::key_required)?;
                    let rtxn = store.read_txn()?;
                    let n = c.count_vault_in(&rtxn, &filter, &auth.tenant)?;
                    rtxn.commit().map_err(nonodb_core::Error::from)?;
                    Ok::<Value, ApiError>(json!({ "count": n }))
                } else {
                    if auth.is_some() {
                        return Err(vault::key_unexpected().into());
                    }
                    Ok::<Value, ApiError>(
                        json!({ "count": c.count_with_options(&filter, max_time_ms)? }),
                    )
                }
            });
            read_result(joined).await
        }

        Request::Aggregate {
            coll,
            pipeline,
            comment: _,
            max_time_ms,
            durability,
        } => {
            let spec = match nonodb_core::aggregate::write_stage_spec(&pipeline) {
                Ok(spec) => spec,
                Err(err) => return respond(Err(err.into())),
            };
            if spec.is_some() {
                if vault_auth.is_some() {
                    return respond(Err(
                        vault::unsupported("$out/$merge on vault collections").into()
                    ));
                }
                let req = WriteReq::Aggregate {
                    coll,
                    pipeline,
                    max_time_ms,
                    database: database.clone(),
                };
                return submit_write(&state, req, durability.unwrap_or_default()).await;
            }
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "aggregation")?;
                Ok::<Value, ApiError>(
                    json!({ "results": c.aggregate_with_options(&pipeline, max_time_ms)? }),
                )
            });
            read_result(joined).await
        }

        Request::Sql {
            query,
            params,
            durability,
        } => {
            let started = Instant::now();
            let prepared = match nonodb_core::sql::PreparedSql::parse(&query) {
                Ok(prepared) => prepared,
                Err(err) => {
                    metrics::inc(&metrics::SQL_ERRORS);
                    return respond(Err(ApiError::from(err)));
                }
            };
            let kind = prepared.kind();
            let collection = prepared.collection().to_string();
            let response = if !prepared.is_read_only() {
                submit_write(
                    &state,
                    WriteReq::Sql { query, params },
                    durability.unwrap_or_default(),
                )
                .await
            } else {
                let store = state.store.clone();
                let auth = vault_auth.clone();
                let joined = tokio::task::spawn_blocking(move || {
                    let coll = prepared.collection().to_string();
                    let c = read_collection(&store, &coll, auth.as_ref())?;
                    reject_vault_read(&c, auth.as_ref(), "SQL")?;
                    Ok::<Value, ApiError>(json!({
                        "results": prepared.execute(&c, &params)?,
                    }))
                });
                read_result(joined).await
            };
            let elapsed = started.elapsed();
            let ok = response.1 .0.ok;
            metrics::record_sql(kind.as_str(), elapsed, ok);
            let slow = sql_slow_threshold();
            if !slow.is_zero() && elapsed >= slow {
                tracing::warn!(
                    sql_kind = kind.as_str(),
                    collection = %collection,
                    duration_ms = elapsed.as_millis(),
                    ok,
                    "slow SQL statement"
                );
            }
            response
        }

        Request::ExplainSql { query, params } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let coll = nonodb_core::sql::collection_name(&query)?;
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "SQL explain")?;
                Ok::<Value, ApiError>(nonodb_core::sql::explain(&c, &query, &params)?)
            });
            read_result(joined).await
        }

        Request::ListIndexes { coll } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "index listing")?;
                let list: Vec<Value> = c
                    .list_index_defs()?
                    .into_iter()
                    .map(|d| {
                        json!({
                            "field": d.name(),
                            "fields": d.effective_fields(),
                            "unique": d.unique,
                            "sparse": d.sparse,
                            "partial_filter_expression": d.partial_filter_expression,
                            "include": d.include,
                            "format": d.format,
                            "geo": d.geo,
                            "multikey_fields": d.multikey_fields,
                            "multikey_all_docs": d.multikey_all_docs,
                        })
                    })
                    .collect();
                Ok::<Value, ApiError>(json!({ "indexes": list }))
            });
            read_result(joined).await
        }

        Request::ListTextIndexes { coll } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "text index listing")?;
                Ok::<Value, ApiError>(json!({ "text_indexes": c.list_text_indexes()? }))
            });
            read_result(joined).await
        }

        Request::IndexStatus { coll } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "index status")?;
                let builds: Vec<_> = nonodb_core::progress::snapshot()
                    .into_iter()
                    .filter(|build| build.collection == coll)
                    .collect();
                Ok::<Value, ApiError>(json!({ "builds": builds }))
            });
            read_result(joined).await
        }

        Request::ListFuzzyIndexes { coll } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "fuzzy index listing")?;
                Ok::<Value, ApiError>(json!({ "fuzzy_indexes": c.list_fuzzy_indexes()? }))
            });
            read_result(joined).await
        }

        Request::ListVectorIndexes { coll } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "vector index listing")?;
                let list: Vec<Value> = c
                    .list_vector_indexes()?
                    .into_iter()
                    .map(|d| {
                        json!({
                            "field": d.field,
                            "dims": d.dims,
                            "metric": d.metric.as_str(),
                            "format": d.format,
                            "ann": d.ann,
                        })
                    })
                    .collect();
                Ok::<Value, ApiError>(json!({ "vector_indexes": list }))
            });
            read_result(joined).await
        }

        Request::VectorSearch {
            coll,
            field,
            vector,
            filter,
            skip,
            limit,
        } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "vector search")?;
                Ok::<Value, ApiError>(json!({
                    "results": c.vector_search(&field, &vector, &filter, skip, limit)?
                }))
            });
            read_result(joined).await
        }

        Request::GetGraph { coll } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "graph")?;
                Ok::<Value, ApiError>(json!({ "graph": c.get_graph()? }))
            });
            read_result(joined).await
        }

        Request::GraphRebuildStatus { coll } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                c.graph_rebuild_status().map_err(ApiError::from)
            });
            read_result(joined).await
        }

        Request::Traverse { coll, options } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "graph traversal")?;
                c.graph_traverse(options).map_err(ApiError::from)
            });
            read_result(joined).await
        }

        Request::ShortestPath { coll, options } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "graph traversal")?;
                c.graph_shortest_path(options).map_err(ApiError::from)
            });
            read_result(joined).await
        }

        Request::Cypher { coll, options } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "graph traversal")?;
                c.graph_cypher(options).map_err(ApiError::from)
            });
            read_result(joined).await
        }

        Request::AllShortestPaths { coll, options } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "graph traversal")?;
                c.graph_all_shortest_paths(options).map_err(ApiError::from)
            });
            read_result(joined).await
        }

        Request::GraphMatch { coll, options } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "graph traversal")?;
                c.graph_match(options).map_err(ApiError::from)
            });
            read_result(joined).await
        }

        Request::GraphAlgorithm { coll, options } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "graph traversal")?;
                c.graph_algorithm(options).map_err(ApiError::from)
            });
            read_result(joined).await
        }

        Request::StartGraphJob { coll, options } => {
            let collection = match read_collection(&state.store, &coll, vault_auth.as_ref()) {
                Ok(collection) => collection,
                Err(err) => return respond(Err(err)),
            };
            if let Err(err) = reject_vault_read(&collection, vault_auth.as_ref(), "graph traversal")
            {
                return respond(Err(err));
            }
            match state
                .analytics
                .start(principal.name(), &coll, options, state.store.clone())
            {
                Ok(value) => respond(Ok(value)),
                Err(err) => respond(Err(err)),
            }
        }

        Request::GraphJobStatus { job_id } => {
            match state.analytics.status(principal.name(), &job_id) {
                Ok(value) => respond(Ok(value)),
                Err(err) => respond(Err(err)),
            }
        }

        Request::GraphJobResult { job_id } => {
            match state.analytics.result(principal.name(), &job_id) {
                Ok(value) => respond(Ok(value)),
                Err(err) => respond(Err(err)),
            }
        }

        Request::CancelGraphJob { job_id } => {
            match state.analytics.cancel(principal.name(), &job_id) {
                Ok(value) => respond(Ok(value)),
                Err(err) => respond(Err(err)),
            }
        }

        Request::ExportGraphJob { job_id, coll } => {
            let manager = state.analytics.clone();
            let owner = principal.name().to_string();
            let joined =
                tokio::task::spawn_blocking(move || manager.export(&owner, &job_id, &coll));
            read_result(joined).await
        }

        Request::HybridSearch { coll, options } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "graph traversal")?;
                c.hybrid_search(options).map_err(ApiError::from)
            });
            read_result(joined).await
        }

        Request::Neighbors { coll, options } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "graph traversal")?;
                c.graph_neighbors(options).map_err(ApiError::from)
            });
            read_result(joined).await
        }

        Request::Search {
            coll,
            field,
            query,
            filter,
            skip,
            limit,
        } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "full-text search")?;
                Ok::<Value, ApiError>(json!({
                    "results": c.search(&field, &query, &filter, skip, limit)?
                }))
            });
            read_result(joined).await
        }

        Request::Fuzzy {
            coll,
            field,
            query,
            max_distance,
            filter,
            limit,
        } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "fuzzy search")?;
                let distance = max_distance.unwrap_or(DEFAULT_FUZZY_DISTANCE);
                Ok::<Value, ApiError>(json!({
                    "results": c.fuzzy_search(&field, &query, distance, &filter, limit)?
                }))
            });
            read_result(joined).await
        }

        Request::SearchAll { request } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let coll = request.coll.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "unified search")?;
                let out = c.search_all(&request)?;
                Ok::<Value, ApiError>(search_all_response(out))
            });
            read_result(joined).await
        }

        Request::History {
            coll,
            id,
            limit,
            include_docs,
        } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                if c.profile().is_vault() {
                    let auth = auth.as_ref().ok_or_else(vault::key_required)?;
                    let rtxn = store.read_txn()?;
                    let revisions = c.history_vault_in(
                        &rtxn,
                        &id,
                        limit,
                        include_docs,
                        &auth.key,
                        &auth.tenant,
                        &auth.kid,
                    )?;
                    rtxn.commit().map_err(nonodb_core::Error::from)?;
                    Ok::<Value, ApiError>(json!({ "revisions": revisions }))
                } else {
                    if auth.is_some() {
                        return Err(vault::key_unexpected().into());
                    }
                    Ok::<Value, ApiError>(json!({
                        "revisions": c.history(&id, limit, include_docs)?
                    }))
                }
            });
            read_result(joined).await
        }

        Request::GetVersion { coll, id, version } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                let entry = if c.profile().is_vault() {
                    let auth = auth.as_ref().ok_or_else(vault::key_required)?;
                    let rtxn = store.read_txn()?;
                    let entry = c.get_version_vault_in(
                        &rtxn,
                        &id,
                        version,
                        &auth.key,
                        &auth.tenant,
                        &auth.kid,
                    )?;
                    rtxn.commit().map_err(nonodb_core::Error::from)?;
                    entry
                } else {
                    if auth.is_some() {
                        return Err(vault::key_unexpected().into());
                    }
                    c.get_version(&id, version)?
                };
                let value = serde_json::to_value(entry)
                    .map_err(nonodb_core::Error::from)
                    .map_err(ApiError::from)?;
                Ok::<Value, ApiError>(value)
            });
            read_result(joined).await
        }

        Request::Batch { ops } => {
            if vault_auth.is_some() {
                return respond(Err(
                    vault::unsupported("batch reads on vault collections").into()
                ));
            }
            let store = state.store.clone();
            let principal = principal.clone();
            let n = ops.len();
            metrics::add(&metrics::BATCH_OPS, n as u64);
            let joined = tokio::task::spawn_blocking(move || {
                let rtxn = store.read_txn()?;
                let mut results = Vec::with_capacity(ops.len());
                for op in ops {
                    results.push(batch_read(&store, &rtxn, op, &principal, &database));
                }
                rtxn.commit().map_err(nonodb_core::Error::from)?;
                Ok::<Value, ApiError>(json!({ "results": results }))
            });
            read_result(joined).await
        }

        Request::GetCollectionTtl { coll } => {
            let store = state.store.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let ttl = store.collection_ttl(&coll)?;
                let ttl_field = store.collection_ttl_after(&coll)?.map(|(field, _)| field);
                let purge = store.collection_ttl_purge(&coll)?;
                Ok::<Value, ApiError>(json!({
                    "coll": coll,
                    "ttl_seconds": ttl,
                    "ttl_field": ttl_field,
                    "purge_history": purge,
                }))
            });
            read_result(joined).await
        }

        Request::GetCollectionTimeSeries { coll } => {
            let store = state.store.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let settings = store.timeseries_settings(&coll)?;
                let Some(settings) = settings else {
                    return Err(ApiError::from(nonodb_core::Error::Query(format!(
                        "collection `{coll}` is not a time-series collection"
                    ))));
                };
                let stats = store.timeseries_stats(&coll)?;
                Ok::<Value, ApiError>(json!({
                    "coll": coll,
                    "profile": "timeseries",
                    "timeseries": settings,
                    "stats": stats,
                }))
            });
            read_result(joined).await
        }

        Request::GetCollectionHistory { coll } => {
            let store = state.store.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let max_versions = store.collection_history(&coll)?;
                Ok::<Value, ApiError>(json!({
                    "coll": coll,
                    "max_versions": max_versions,
                }))
            });
            read_result(joined).await
        }

        Request::GetCollectionEvents { coll } => {
            let store = state.store.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let config = store.collection_events(&coll)?;
                Ok::<Value, ApiError>(json!({
                    "coll": coll,
                    "enabled": config.enabled,
                    "fields": config.fields,
                    "ops": config.ops,
                    "filter": config.filter,
                    "include_before": config.include_before,
                    "include_after": config.include_after,
                    "projection": config.projection,
                }))
            });
            read_result(joined).await
        }

        Request::GetCollectionMetrics { coll } => {
            respond(Ok(metrics::collection_snapshot(&database, &coll)))
        }

        Request::DatabaseStats => {
            let store = state.store.clone();
            let quota = state.quota.clone();
            let principal = principal.clone();
            let database = database.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let quota = quota.report(&store)?;
                let mut stats = store.storage_stats()?;
                stats
                    .collections
                    .retain(|item| principal.can_see_collection(&database, &item.collection));
                for item in &mut stats.collections {
                    let (last_minute, avg_per_second) =
                        metrics::collection_traffic(&database, &item.collection);
                    item.requests_last_minute = last_minute;
                    item.avg_requests_per_second = avg_per_second;
                }
                stats.total_records = stats.collections.iter().map(|item| item.records).sum();
                stats.collection_bytes =
                    stats.collections.iter().map(|item| item.total_bytes).sum();
                let mut value = serde_json::to_value(stats).map_err(nonodb_core::Error::from)?;
                value["database"] = json!(database);
                value["quota"] = serde_json::to_value(quota).map_err(nonodb_core::Error::from)?;
                Ok::<Value, ApiError>(value)
            });
            read_result(joined).await
        }

        Request::GetQuota => {
            let store = state.store.clone();
            let quota = state.quota.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let report = quota.report(&store)?;
                Ok::<Value, ApiError>(
                    serde_json::to_value(report).map_err(nonodb_core::Error::from)?,
                )
            });
            read_result(joined).await
        }

        Request::SetQuota { limits } => {
            let store = state.store.clone();
            let quota = state.quota.clone();
            let joined = tokio::task::spawn_blocking(move || {
                quota.set_limits(limits)?;
                let report = quota.report(&store)?;
                Ok::<Value, ApiError>(
                    serde_json::to_value(report).map_err(nonodb_core::Error::from)?,
                )
            });
            read_result(joined).await
        }

        Request::ListSubscriptions { coll } => {
            let store = state.store.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let list = store.list_subscriptions(&coll)?;
                Ok::<Value, ApiError>(json!({ "subscriptions": list }))
            });
            read_result(joined).await
        }

        Request::ListWebhooks => {
            let store = state.store.clone();
            let joined = tokio::task::spawn_blocking(move || {
                Ok::<Value, ApiError>(json!({
                    "webhooks": crate::webhooks::list_rules(&store).map_err(ApiError::internal)?
                }))
            });
            read_result(joined).await
        }

        Request::ListWebhookDeliveries { status, limit } => {
            let store = state.store.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let mut deliveries =
                    crate::webhooks::list_deliveries(&store).map_err(ApiError::internal)?;
                if let Some(status) = status.as_deref().filter(|value| !value.is_empty()) {
                    deliveries.retain(|delivery| {
                        delivery.get("status").and_then(Value::as_str) == Some(status)
                    });
                }
                let limit = limit.unwrap_or(100).min(1000);
                deliveries.truncate(limit);
                Ok::<Value, ApiError>(json!({ "deliveries": deliveries }))
            });
            read_result(joined).await
        }

        Request::ListCollections => {
            let store = state.store.clone();
            let principal = principal.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let mut collections = store.collections()?;
                collections.retain(|coll| principal.can_see_collection(&database, coll));
                Ok::<Value, ApiError>(json!({ "collections": collections }))
            });
            read_result(joined).await
        }

        Request::ListDatabases => {
            let mut names = state.databases.names().await;
            if !names.iter().any(|name| name == databases::DEFAULT_DATABASE) {
                names.push(databases::DEFAULT_DATABASE.to_string());
                names.sort();
            }
            names.retain(|name| principal.can_see_database(name));
            respond(Ok(json!({
                "default": databases::DEFAULT_DATABASE,
                "databases": names,
            })))
        }

        Request::CreateDatabase { name } => {
            if name == database {
                return respond(Err(ApiError {
                    code: "invalid_query".into(),
                    message: "cannot create the currently selected database".into(),
                }));
            }
            match state.databases.create(&name).await {
                Ok(()) => respond(Ok(json!({
                    "name": name,
                    "created": true,
                }))),
                Err(err) => respond(Err(err)),
            }
        }

        Request::DropDatabase { name } => {
            if name == database {
                return respond(Err(ApiError {
                    code: "invalid_query".into(),
                    message: "cannot drop the currently selected database".into(),
                }));
            }
            match state.databases.drop_database(&name).await {
                Ok(dropped) => respond(Ok(json!({
                    "name": name,
                    "dropped": dropped,
                }))),
                Err(err) => respond(Err(err)),
            }
        }

        Request::Sync => {
            let store = state.store.clone();
            let joined = tokio::task::spawn_blocking(move || {
                store.force_sync()?;
                Ok::<Value, ApiError>(json!({ "synced": true }))
            });
            read_result(joined).await
        }

        Request::MapInfo => {
            let store = state.store.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let map_size = store.map_size();
                let used = store.map_used_bytes();
                Ok::<Value, ApiError>(json!({
                    "map_size": map_size,
                    "used_bytes": used,
                    "free_bytes": map_size.saturating_sub(used),
                    "page_size": PAGE_SIZE,
                }))
            });
            read_result(joined).await
        }

        Request::ResizeMap { size } => {
            let store = state.store.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let size = usize::try_from(size).map_err(|_| ApiError {
                    code: "invalid_query".into(),
                    message: "map size does not fit in usize".into(),
                })?;
                let old_size = store.resize_map_size(size)?;
                Ok::<Value, ApiError>(json!({
                    "old_size": old_size,
                    "map_size": store.map_size(),
                }))
            });
            read_result(joined).await
        }

        Request::WriteBatch { ops, durability } => {
            if vault_auth.is_some() {
                return respond(Err(
                    vault::unsupported("write_batch on vault collections").into()
                ));
            }
            let req = WriteReq::Batch {
                ops: ops.into_iter().map(WriteReq::from).collect(),
            };
            submit_write(&state, req, durability.unwrap_or_default()).await
        }

        write_req => {
            let durability = write_req.write_durability();
            let req = match write_req {
                Request::Insert { coll, doc, .. } => WriteReq::Insert {
                    coll,
                    doc,
                    vault: vault_auth.clone(),
                },
                Request::InsertMany { coll, docs, .. } => WriteReq::InsertMany {
                    coll,
                    docs,
                    vault: vault_auth.clone(),
                },
                Request::Update {
                    coll,
                    filter,
                    update,
                    multi,
                    upsert,
                    array_filters,
                    ..
                } => WriteReq::Update {
                    coll,
                    filter,
                    update,
                    multi,
                    upsert,
                    array_filters,
                    default_id: None,
                    vault: vault_auth.clone(),
                },
                Request::FindAndModify {
                    coll,
                    filter,
                    update,
                    sort,
                    new,
                    upsert,
                    remove,
                    array_filters,
                    projection,
                    ..
                } => WriteReq::FindAndModify {
                    coll,
                    filter,
                    update,
                    sort,
                    new,
                    upsert,
                    remove,
                    array_filters,
                    projection,
                    default_id: None,
                    vault: vault_auth.clone(),
                },
                Request::Delete {
                    coll,
                    filter,
                    multi,
                    ..
                } => WriteReq::Delete {
                    coll,
                    filter,
                    multi,
                    vault: vault_auth.clone(),
                },
                Request::CreateCollection {
                    coll,
                    profile,
                    ttl_seconds,
                    ttl_purge_history,
                    timeseries,
                } => {
                    let profile = match profile {
                        Some(raw) => match CollectionProfile::parse(&raw) {
                            Ok(p) => p,
                            Err(e) => return respond(Err(e.into())),
                        },
                        None => CollectionProfile::Standard,
                    };
                    let timeseries = match timeseries {
                        Some(settings) => {
                            if !profile.is_timeseries() {
                                return respond(Err(nonodb_core::Error::Query(
                                    "`timeseries` settings require profile `timeseries`".into(),
                                )
                                .into()));
                            }
                            if let Err(err) = settings.validate() {
                                return respond(Err(err.into()));
                            }
                            Some(settings)
                        }
                        None if profile.is_timeseries() => {
                            return respond(Err(nonodb_core::Error::Query(
                                "profile `timeseries` requires a `timeseries` settings object with `timeField`"
                                    .into(),
                            )
                            .into()));
                        }
                        None => None,
                    };
                    WriteReq::CreateCollection {
                        coll,
                        profile,
                        ttl_seconds,
                        ttl_purge_history,
                        timeseries,
                    }
                }
                Request::DropCollection { coll } => WriteReq::DropCollection { coll },
                Request::SetCollectionTimeSeries { coll, settings } => {
                    WriteReq::SetCollectionTimeSeries { coll, settings }
                }
                Request::SetCollectionTtl {
                    coll,
                    ttl_seconds,
                    purge_history,
                } => WriteReq::SetCollectionTtl {
                    coll,
                    ttl_seconds,
                    purge_history,
                },
                Request::RetainTimeSeries { coll, before } => {
                    let before_ms = match nonodb_core::date::parse_value(&before) {
                        Some(ms) => ms,
                        None => match before.as_i64() {
                            Some(ms) => ms,
                            None => {
                                return respond(Err(nonodb_core::Error::Query(
                                    "`before` must be a date or epoch milliseconds".into(),
                                )
                                .into()))
                            }
                        },
                    };
                    WriteReq::RetainTimeSeries { coll, before_ms }
                }
                Request::SetCollectionTtlAfter {
                    coll,
                    field,
                    ttl_seconds,
                    purge_history,
                } => WriteReq::SetCollectionTtlAfter {
                    coll,
                    field,
                    ttl_seconds,
                    purge_history,
                },
                Request::BackfillCollectionTtl {
                    coll,
                    field,
                    ttl_seconds,
                    after,
                    limit,
                    dry_run,
                } => WriteReq::BackfillCollectionTtl {
                    coll,
                    field,
                    ttl_seconds,
                    after,
                    limit: limit.unwrap_or(1000).clamp(1, 10_000),
                    dry_run: dry_run.unwrap_or(false),
                },
                Request::SetCollectionHistory { coll, max_versions } => {
                    WriteReq::SetCollectionHistory { coll, max_versions }
                }
                Request::SetCollectionEvents {
                    coll,
                    enabled,
                    fields,
                    ops,
                    filter,
                    include_before,
                    include_after,
                    projection,
                } => WriteReq::SetCollectionEvents {
                    coll,
                    enabled,
                    fields,
                    ops,
                    filter,
                    include_before,
                    include_after,
                    projection,
                },
                Request::CreateSubscription {
                    coll,
                    name,
                    ops,
                    filter,
                    count,
                    once,
                    event_type,
                    lease_seconds,
                } => WriteReq::CreateSubscription {
                    coll,
                    subscription: nonodb_core::subscriptions::Subscription {
                        name,
                        ops,
                        filter,
                        count,
                        once,
                        event_type,
                        last_matched: false,
                        lease_seconds: lease_seconds.unwrap_or(600),
                        expires_at: 0,
                    },
                },
                Request::RenewSubscription {
                    coll,
                    name,
                    lease_seconds,
                } => WriteReq::RenewSubscription {
                    coll,
                    name,
                    lease_seconds,
                },
                Request::DeleteSubscription { coll, name } => {
                    WriteReq::DeleteSubscription { coll, name }
                }
                Request::CreateWebhook { rule } => WriteReq::CreateWebhook { rule },
                Request::DeleteWebhook { name } => WriteReq::DeleteWebhook { name },
                Request::RetryWebhookDelivery { id } => WriteReq::RetryWebhookDelivery { id },
                Request::CreateIndex {
                    coll,
                    field,
                    fields,
                    unique,
                    sparse,
                    partial_filter_expression,
                    include,
                    geo,
                } => {
                    let fields = match (fields, field) {
                        (Some(fields), _) => fields,
                        (None, Some(field)) => vec![field],
                        (None, None) => {
                            return respond(Err(ApiError {
                                code: "invalid_query".into(),
                                message: "create_index requires `field` or `fields`".into(),
                            }));
                        }
                    };
                    WriteReq::CreateIndex {
                        coll,
                        fields,
                        unique,
                        sparse,
                        partial_filter_expression,
                        include,
                        geo,
                    }
                }
                Request::CreateIndexes { coll, indexes } => {
                    let mut specs = Vec::with_capacity(indexes.len());
                    for index in indexes {
                        let fields = match (index.fields, index.field) {
                            (Some(fields), _) => fields,
                            (None, Some(field)) => vec![field],
                            (None, None) => {
                                return respond(Err(ApiError {
                                    code: "invalid_query".into(),
                                    message:
                                        "each create_indexes entry requires `field` or `fields`"
                                            .into(),
                                }));
                            }
                        };
                        specs.push(nonodb_core::IndexSpec {
                            fields,
                            name: index.name,
                            directions: index.directions,
                            unique: index.unique,
                            sparse: index.sparse,
                            partial_filter_expression: index.partial_filter_expression,
                            include: index.include,
                            geo: index.geo,
                        });
                    }
                    WriteReq::CreateIndexes {
                        coll,
                        indexes: specs,
                    }
                }
                Request::DropIndex { coll, field } => WriteReq::DropIndex { coll, field },
                Request::CreateTextIndex { coll, field } => {
                    WriteReq::CreateTextIndex { coll, field }
                }
                Request::DropTextIndex { coll, field } => WriteReq::DropTextIndex { coll, field },
                Request::CreateFuzzyIndex { coll, field } => {
                    WriteReq::CreateFuzzyIndex { coll, field }
                }
                Request::DropFuzzyIndex { coll, field } => WriteReq::DropFuzzyIndex { coll, field },
                Request::CreateVectorIndex {
                    coll,
                    field,
                    dims,
                    metric,
                } => WriteReq::CreateVectorIndex {
                    coll,
                    field,
                    dims,
                    metric: metric.unwrap_or_default(),
                },
                Request::Reindex { coll } => WriteReq::Reindex { coll },
                Request::SetGraph { coll, def } => WriteReq::SetGraph { coll, def },
                Request::DropGraph { coll } => WriteReq::DropGraph { coll },
                Request::ReindexGraph { coll, online } => {
                    if online {
                        WriteReq::StartGraphRebuild { coll }
                    } else {
                        WriteReq::ReindexGraph { coll }
                    }
                }
                Request::CancelGraphRebuild { coll } => WriteReq::CancelGraphRebuild { coll },
                Request::DropVectorIndex { coll, field } => {
                    WriteReq::DropVectorIndex { coll, field }
                }
                _ => unreachable!("read requests handled above"),
            };

            submit_write(&state, req, durability).await
        }
    };

    if response.0 == StatusCode::OK {
        if let Some(key) = cache_key {
            let cached: Option<Value> = response.1 .0.result.clone().or_else(|| {
                response
                    .1
                     .0
                    .raw_result
                    .as_ref()
                    .filter(|raw| state.cache.can_store_bytes(raw.get().len()))
                    .and_then(|raw| serde_json::from_str(raw.get()).ok())
            });
            if let Some(value) = cached {
                state.cache.put_until(
                    key,
                    cache_version,
                    value,
                    ttl_cache_deadline(state.store.as_ref()),
                );
            }
        }
    }
    audit_response(
        &state,
        &connection,
        principal.name(),
        audit_op,
        &audit_collections,
        &response,
        started,
        audit_needed,
    );
    response
}

fn audit_denied(
    state: &AppState,
    connection: &auth::ConnectionAuth,
    principal: &str,
    req: &Request,
    err: &ApiError,
    started: std::time::Instant,
    auth_audit: Option<audit::AuthAudit>,
) {
    let collections: Vec<String> = req.collections();
    let refs: Vec<&str> = collections.iter().map(String::as_str).collect();
    if let Some(audit) = &state.audit {
        audit.record(
            principal,
            connection.source.as_str(),
            connection.peer.as_deref(),
            req.op_name(),
            &refs,
            false,
            Some(&err.code),
            started.elapsed().as_secs_f64() * 1000.0,
            auth_audit,
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn audit_error(
    state: &AppState,
    connection: &auth::ConnectionAuth,
    principal: &str,
    op: &str,
    collections: &[String],
    err: &ApiError,
    started: std::time::Instant,
    enabled: bool,
) {
    if !enabled {
        return;
    }
    let refs: Vec<&str> = collections.iter().map(String::as_str).collect();
    if let Some(audit) = &state.audit {
        audit.record(
            principal,
            connection.source.as_str(),
            connection.peer.as_deref(),
            op,
            &refs,
            false,
            Some(&err.code),
            started.elapsed().as_secs_f64() * 1000.0,
            None,
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn audit_response(
    state: &AppState,
    connection: &auth::ConnectionAuth,
    principal: &str,
    op: &str,
    collections: &[String],
    response: &(StatusCode, Json<ApiResponse>),
    started: std::time::Instant,
    enabled: bool,
) {
    if !enabled {
        return;
    }
    let refs: Vec<&str> = collections.iter().map(String::as_str).collect();
    let error = response.1 .0.error.as_ref().map(|e| e.code.as_str());
    if let Some(audit) = &state.audit {
        audit.record(
            principal,
            connection.source.as_str(),
            connection.peer.as_deref(),
            op,
            &refs,
            response.0 == StatusCode::OK,
            error,
            started.elapsed().as_secs_f64() * 1000.0,
            None,
        );
    }
}

fn validate_lookup_input(by: &[String], values: &[Value]) -> Result<(), ApiError> {
    if by.is_empty() || by.iter().any(String::is_empty) {
        return Err(ApiError {
            code: "invalid_query".to_string(),
            message: "lookup field must not be empty".to_string(),
        });
    }
    if values.len() > nonodb_core::MAX_LOOKUP_VALUES {
        return Err(ApiError {
            code: "invalid_query".to_string(),
            message: format!(
                "lookup accepts at most {} values",
                nonodb_core::MAX_LOOKUP_VALUES
            ),
        });
    }
    Ok(())
}

fn lookup_response(
    coll: &str,
    by: &[String],
    report: nonodb_core::Result<(Vec<Option<Value>>, nonodb_core::LookupStats)>,
    elapsed: std::time::Duration,
    explain: bool,
) -> Result<Value, ApiError> {
    match report {
        Ok((results, stats)) => {
            let bytes_returned = if explain {
                u64::try_from(
                    serde_json::to_vec(&results)
                        .map(|bytes| bytes.len())
                        .unwrap_or_default(),
                )
                .unwrap_or_default()
            } else {
                stats.result_bytes
            };
            metrics::record_lookup(elapsed, &stats, bytes_returned, true, explain);
            if explain {
                let by = Value::Array(by.iter().cloned().map(Value::String).collect());
                Ok(json!({
                    "collection": coll,
                    "by": by,
                    "selected_index": stats.selected_index,
                    "index": {
                        "name": stats.selected_index,
                        "fields": stats.index_fields,
                        "include": stats.index_include,
                        "primary": stats.primary,
                        "unique": true,
                    },
                    "covering": stats.covering,
                    "key_counts": {
                        "requested": stats.requested_keys,
                        "unique": stats.unique_keys,
                        "duplicates": stats.requested_keys.saturating_sub(stats.unique_keys),
                    },
                    "found": stats.found,
                    "missing": stats.missing,
                    "documents_read": stats.documents_read,
                    "index_entries_read": stats.index_entries_read,
                    "bytes_read": stats.bytes_read,
                    "bytes_returned": bytes_returned,
                    "elapsed_ms": elapsed.as_secs_f64() * 1000.0,
                }))
            } else {
                let found = results.iter().filter(|doc| doc.is_some()).count();
                Ok(json!({ "results": results, "found": found }))
            }
        }
        Err(err) => {
            metrics::record_lookup(
                elapsed,
                &nonodb_core::LookupStats::default(),
                0,
                false,
                explain,
            );
            Err(err.into())
        }
    }
}

fn lookup_projection(
    fields: Option<&[String]>,
    projection: Option<&Value>,
) -> Result<Option<Value>, ApiError> {
    if fields.is_some() && projection.is_some() {
        return Err(ApiError {
            code: "invalid_query".to_string(),
            message: "lookup accepts either `fields` or `projection`, not both".to_string(),
        });
    }
    if let Some(projection) = projection {
        return Ok(Some(projection.clone()));
    }
    let Some(fields) = fields else {
        return Ok(None);
    };
    if fields.is_empty() {
        return Ok(None);
    }
    let mut projection = serde_json::Map::new();
    for field in fields {
        projection.insert(field.clone(), Value::from(1));
    }
    Ok(Some(Value::Object(projection)))
}

fn watch_filter(after: &str, types: &[String], filter: Option<&Value>) -> Value {
    let mut clauses = Vec::new();
    if !after.is_empty() {
        clauses.push(json!({"_id": {"$gt": after}}));
    }
    if !types.is_empty() {
        clauses.push(json!({"type": {"$in": types}}));
    }
    if let Some(filter) = filter {
        if !filter.as_object().is_some_and(|object| object.is_empty()) && !filter.is_null() {
            clauses.push(filter.clone());
        }
    }
    match clauses.len() {
        0 => json!({}),
        1 => clauses.pop().unwrap_or_else(|| json!({})),
        _ => json!({"$and": clauses}),
    }
}

/// `watch` always needs `_id` to advance its resume token.
fn watch_projection(projection: Option<&Value>) -> Option<Value> {
    let mut projection = projection?.clone();
    if let Some(object) = projection.as_object_mut() {
        object.insert("_id".to_string(), Value::from(1));
        Some(projection)
    } else {
        None
    }
}

#[allow(clippy::too_many_arguments)]
const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Extracts the millisecond timestamp from a 26-character ULID-style id.
fn event_timestamp_ms(event_id: &str) -> Option<u64> {
    if event_id.len() != 26 {
        return None;
    }
    let mut value: u64 = 0;
    for byte in event_id.as_bytes().iter().take(10) {
        let digit = CROCKFORD
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(byte))?;
        value = (value << 5) | digit as u64;
    }
    Some(value)
}

/// Encodes the lowest ULID-style id for a millisecond timestamp.
fn event_id_from_ms(milliseconds: u64) -> String {
    let mut value = ((milliseconds & ((1u64 << 48) - 1)) as u128) << 80;
    let mut chars = [b'0'; 26];
    for index in (0..26).rev() {
        chars[index] = CROCKFORD[(value & 31) as usize];
        value >>= 5;
    }
    String::from_utf8_lossy(&chars).into_owned()
}

#[allow(clippy::too_many_arguments)]
fn watch_late_scan(
    store: &Store,
    coll: &str,
    auth: Option<&VaultAuth>,
    floor: &str,
    after: &str,
    limit: usize,
    types: &[String],
    filter: Option<&Value>,
    projection: Option<&Value>,
) -> Result<Vec<Value>, ApiError> {
    let c = read_collection(store, coll, auth)?;
    reject_vault_read(&c, auth, "watch on vault collections")?;
    let mut clauses = vec![json!({"_id": {"$gt": floor, "$lte": after}})];
    if !types.is_empty() {
        clauses.push(json!({"type": {"$in": types}}));
    }
    if let Some(filter) = filter {
        if !filter.as_object().is_some_and(|object| object.is_empty()) && !filter.is_null() {
            clauses.push(filter.clone());
        }
    }
    let query = FindQuery {
        filter: if clauses.len() == 1 {
            clauses.pop().unwrap_or_else(|| json!({}))
        } else {
            json!({"$and": clauses})
        },
        projection: watch_projection(projection),
        sort: vec![("_id".to_string(), 1)],
        skip: 0,
        limit: Some(limit),
        comment: None,
        max_time_ms: None,
        hint: None,
    };
    Ok(c.find(&query)?)
}

#[allow(clippy::too_many_arguments)]
fn watch_once(
    store: &Store,
    coll: &str,
    auth: Option<&VaultAuth>,
    after: &str,
    limit: usize,
    types: &[String],
    filter: Option<&Value>,
    projection: Option<&Value>,
) -> Result<(Vec<Value>, String, bool), ApiError> {
    let c = read_collection(store, coll, auth)?;
    reject_vault_read(&c, auth, "watch on vault collections")?;
    let query = FindQuery {
        filter: watch_filter(after, types, filter),
        projection: watch_projection(projection),
        sort: vec![("_id".to_string(), 1)],
        skip: 0,
        limit: Some(limit + 1),
        comment: None,
        max_time_ms: None,
        hint: None,
    };
    let mut events = c.find(&query)?;
    let has_more = events.len() > limit;
    if has_more {
        events.truncate(limit);
    }
    let next = events
        .last()
        .and_then(|event| event.get("_id"))
        .and_then(Value::as_str)
        .unwrap_or(after)
        .to_string();
    Ok((events, next, has_more))
}

/// Returns `(truncated, oldest_retained_id)` when `after` is no longer present
/// and events older than it may have expired or been deleted.
fn watch_status(
    store: &Store,
    coll: &str,
    auth: Option<&VaultAuth>,
    after: &str,
) -> Result<(bool, Option<String>), ApiError> {
    let c = read_collection(store, coll, auth)?;
    reject_vault_read(&c, auth, "watch on vault collections")?;
    if after.is_empty() {
        return Ok((false, None));
    }
    let exists = c.find(&FindQuery {
        filter: json!({"_id": after}),
        projection: Some(json!({"_id": 1})),
        sort: Vec::new(),
        skip: 0,
        limit: Some(1),
        comment: None,
        max_time_ms: None,
        hint: None,
    })?;
    if !exists.is_empty() {
        return Ok((false, None));
    }
    let oldest = c.find(&FindQuery {
        filter: json!({}),
        projection: Some(json!({"_id": 1})),
        sort: vec![("_id".to_string(), 1)],
        skip: 0,
        limit: Some(1),
        comment: None,
        max_time_ms: None,
        hint: None,
    })?;
    let oldest = oldest
        .first()
        .and_then(|event| event.get("_id"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    Ok((oldest.as_deref().is_some_and(|id| id > after), oldest))
}

fn ttl_cache_deadline(store: &Store) -> Option<Instant> {
    let expiry = store.next_ttl_expiry().ok().flatten()?;
    let now = nonodb_core::ttl::now_ms();
    Some(Instant::now() + Duration::from_millis(expiry.saturating_sub(now)))
}

fn cacheable(req: &Request) -> bool {
    if let Request::Sql { query, .. } = req {
        if !nonodb_core::sql::is_read_only(query).unwrap_or(false) {
            return false;
        }
    }
    if let Request::Aggregate { pipeline, .. } = req {
        if nonodb_core::aggregate::write_stage_spec(pipeline)
            .ok()
            .flatten()
            .is_some()
        {
            return false;
        }
    }
    matches!(
        req,
        Request::Find { .. }
            | Request::FindOne { .. }
            | Request::Lookup { .. }
            | Request::ExplainLookup { .. }
            | Request::Explain { .. }
            | Request::Count { .. }
            | Request::Aggregate { .. }
            | Request::Sql { .. }
            | Request::ExplainSql { .. }
            | Request::Search { .. }
            | Request::SearchAll { .. }
            | Request::Fuzzy { .. }
            | Request::ListIndexes { .. }
            | Request::ListTextIndexes { .. }
            | Request::IndexStatus { .. }
            | Request::ListFuzzyIndexes { .. }
            | Request::ListVectorIndexes { .. }
            | Request::VectorSearch { .. }
            | Request::ListCollections
            | Request::GetCollectionTtl { .. }
            | Request::GetCollectionTimeSeries { .. }
            | Request::GetCollectionHistory { .. }
            | Request::GetCollectionEvents { .. }
            | Request::ListSubscriptions { .. }
            | Request::History { .. }
            | Request::GetVersion { .. }
    )
}

fn search_all_response(out: nonodb_core::SearchAllResponse) -> Value {
    json!({
        "results": out.results,
        "meta": {
            "mode": out.mode,
            "rrf_k": out.rrf_k,
            "candidate_count": out.candidate_count,
            "sources": out.sources,
        }
    })
}

fn authorize_graph_edges(
    store: &nonodb_core::Store,
    principal: &nonodb_auth::Principal,
    req: &Request,
    database: &str,
) -> Result<(), ApiError> {
    let coll = match req {
        Request::GetGraph { coll }
        | Request::Traverse { coll, .. }
        | Request::ShortestPath { coll, .. }
        | Request::Cypher { coll, .. }
        | Request::AllShortestPaths { coll, .. }
        | Request::GraphMatch { coll, .. }
        | Request::GraphAlgorithm { coll, .. }
        | Request::StartGraphJob { coll, .. }
        | Request::Neighbors { coll, .. }
        | Request::HybridSearch { coll, .. } => coll,
        _ => return Ok(()),
    };
    let rtxn = store.read_txn()?;
    let Some(collection) = store.collection_at(&rtxn, coll)? else {
        return Ok(());
    };
    let Some(def) = collection.get_graph_in(&rtxn)? else {
        return Ok(());
    };
    rtxn.commit()
        .map_err(|err| ApiError::from(nonodb_core::Error::from(err)))?;
    authz::allow_collection(
        principal,
        nonodb_auth::Action::Read,
        database,
        &def.edge_collection,
        req.op_name(),
    )
}

fn read_collection<'a>(
    store: &'a Store,
    coll: &str,
    auth: Option<&VaultAuth>,
) -> Result<nonodb_core::Collection<'a>, ApiError> {
    if auth.is_some() {
        match store.collection_profile(coll)? {
            Some(CollectionProfile::Vault) => {}
            Some(_) => return Err(vault::key_unexpected().into()),
            None => return Err(vault::not_a_vault_collection().into()),
        }
    }
    Ok(store.collection(coll)?)
}

fn reject_vault_read(
    c: &nonodb_core::Collection<'_>,
    auth: Option<&VaultAuth>,
    operation: &str,
) -> Result<(), ApiError> {
    if c.profile().is_vault() {
        return Err(vault::unsupported(operation).into());
    }
    if auth.is_some() {
        return Err(vault::key_unexpected().into());
    }
    Ok(())
}

fn unsafe_writes_enabled() -> bool {
    std::env::var("NONODB_ALLOW_UNSAFE_WRITES")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}

async fn submit_write(
    state: &AppState,
    req: WriteReq,
    durability: Durability,
) -> (StatusCode, Json<ApiResponse>) {
    if durability == Durability::Unsafe && !unsafe_writes_enabled() {
        return respond(Err(ApiError {
            code: "unsafe_durability_disabled".into(),
            message: "unsafe durability requires NONODB_ALLOW_UNSAFE_WRITES=1".into(),
        }));
    }
    let index_build = req.is_index_build();
    let timeout = if index_build {
        index_build_timeout()
    } else {
        request_timeout()
    };
    let (tx, rx) = tokio::sync::oneshot::channel();
    metrics::inc(&metrics::WRITE_OPS);
    metrics::inc(&metrics::QUEUE_LEN);
    metrics::note_queue();
    if state
        .write_tx
        .send(WriteJob {
            req,
            resp: tx,
            // A long index backfill must not share a transaction with user
            // writes; it gets its own commit and cannot roll them back.
            isolated: index_build,
            durability,
        })
        .is_err()
    {
        metrics::sub(&metrics::QUEUE_LEN, 1);
        return respond(Err(ApiError::internal("writer unavailable")));
    }
    match tokio::time::timeout(timeout, rx).await {
        Ok(Ok(res)) => respond(res),
        Ok(Err(_)) => respond(Err(ApiError::internal("writer dropped request"))),
        Err(_) => respond(Err(ApiError::timeout())),
    }
}

fn batch_read(
    store: &Store,
    rtxn: &RoTxn<'_>,
    req: Request,
    principal: &nonodb_auth::Principal,
    database: &str,
) -> Value {
    match execute_read(store, rtxn, req, principal, database) {
        Ok(value) => json!({ "ok": true, "result": value }),
        Err(e) => json!({ "ok": false, "error": { "code": e.code, "message": e.message } }),
    }
}

fn execute_read(
    store: &Store,
    rtxn: &RoTxn<'_>,
    req: Request,
    principal: &nonodb_auth::Principal,
    database: &str,
) -> Result<Value, ApiError> {
    match req {
        Request::Watch { .. } => Err(ApiError {
            code: "invalid_query".to_string(),
            message: "watch is not supported inside a read batch".to_string(),
        }),
        Request::Find {
            coll,
            filter,
            projection,
            sort,
            skip,
            limit,
            comment,
            max_time_ms,
            hint,
        } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(json!([]));
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            let query = FindQuery {
                filter,
                projection,
                sort,
                skip,
                limit,
                comment,
                max_time_ms,
                hint,
            };
            Ok(json!(c.find_in(rtxn, &query)?))
        }
        Request::FindOne {
            coll,
            filter,
            projection,
            comment,
            max_time_ms,
            hint,
        } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(Value::Null);
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            let query = FindQuery {
                filter,
                projection,
                sort: Vec::new(),
                skip: 0,
                limit: Some(1),
                comment,
                max_time_ms,
                hint,
            };
            Ok(c.find_in(rtxn, &query)?
                .into_iter()
                .next()
                .unwrap_or(Value::Null))
        }
        Request::Lookup {
            coll,
            by,
            values,
            fields,
            projection,
        } => {
            let started = std::time::Instant::now();
            let by = by
                .unwrap_or(LookupBy::Field("_id".to_string()))
                .into_fields();
            let projection = lookup_projection(fields.as_deref(), projection.as_ref())?;
            validate_lookup_input(&by, &values)?;
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                metrics::record_lookup(
                    started.elapsed(),
                    &nonodb_core::LookupStats::default(),
                    0,
                    true,
                    false,
                );
                return Ok(json!({
                    "results": vec![Value::Null; values.len()],
                    "found": 0,
                }));
            };
            reject_vault_read(&c, None, "lookup on vault collections")?;
            let report = c.lookup_fields_report_in(rtxn, &by, &values, projection.as_ref());
            lookup_response(&coll, &by, report, started.elapsed(), false)
        }
        Request::ExplainLookup {
            coll,
            by,
            values,
            fields,
            projection,
        } => {
            let started = std::time::Instant::now();
            let by = by
                .unwrap_or(LookupBy::Field("_id".to_string()))
                .into_fields();
            let projection = lookup_projection(fields.as_deref(), projection.as_ref())?;
            validate_lookup_input(&by, &values)?;
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                let stats = nonodb_core::LookupStats {
                    requested_keys: values.len(),
                    missing: values.len(),
                    selected_index: "_id_".to_string(),
                    index_fields: by.clone(),
                    primary: true,
                    ..nonodb_core::LookupStats::default()
                };
                metrics::record_lookup(started.elapsed(), &stats, 0, true, true);
                return Ok(json!({
                    "collection": coll,
                    "by": by,
                    "selected_index": stats.selected_index,
                    "index": {
                        "name": stats.selected_index,
                        "fields": stats.index_fields,
                        "include": stats.index_include,
                        "primary": stats.primary,
                        "unique": true,
                    },
                    "covering": false,
                    "key_counts": {
                        "requested": stats.requested_keys,
                        "unique": 0,
                        "duplicates": 0,
                    },
                    "found": 0,
                    "missing": stats.missing,
                    "documents_read": 0,
                    "index_entries_read": 0,
                    "bytes_read": 0,
                    "bytes_returned": 0,
                    "elapsed_ms": started.elapsed().as_secs_f64() * 1000.0,
                }));
            };
            reject_vault_read(&c, None, "explain_lookup on vault collections")?;
            let report = c.lookup_fields_report_in(rtxn, &by, &values, projection.as_ref());
            lookup_response(&coll, &by, report, started.elapsed(), true)
        }
        Request::Explain {
            coll,
            filter,
            sort,
            skip,
            limit,
            comment,
            max_time_ms,
            hint,
        } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(json!({
                    "collection": coll,
                    "access_path": "missing_collection",
                    "candidates": 0,
                    "actual_matched": 0,
                    "actual_returned": 0,
                }));
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            let query = FindQuery {
                filter,
                sort,
                skip,
                limit,
                projection: None,
                comment,
                max_time_ms,
                hint,
            };
            Ok(c.explain_in(rtxn, &query)?)
        }
        Request::Count {
            coll,
            filter,
            comment: _,
            max_time_ms,
        } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(json!({ "count": 0 }));
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            Ok(json!({ "count": c.count_in_with_options(rtxn, &filter, max_time_ms)? }))
        }
        Request::Aggregate {
            coll,
            pipeline,
            comment: _,
            max_time_ms,
            ..
        } => {
            if nonodb_core::aggregate::write_stage_spec(&pipeline)?.is_some() {
                return Err(ApiError {
                    code: "invalid_query".into(),
                    message: "$out/$merge are not allowed in a read batch".into(),
                });
            }
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(json!({ "results": [] }));
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            Ok(json!({ "results": c.aggregate_in_with_options(rtxn, &pipeline, max_time_ms)? }))
        }
        Request::Sql { query, params, .. } => {
            let prepared = nonodb_core::sql::PreparedSql::parse(&query)?;
            if !prepared.is_read_only() {
                return Err(ApiError {
                    code: "invalid_sql".into(),
                    message: "SQL writes are not allowed in a read batch".into(),
                });
            }
            let coll = prepared.collection().to_string();
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(json!({ "results": [] }));
            };
            reject_vault_read(&c, None, "SQL in a read batch")?;
            Ok(json!({ "results": prepared.execute_in(&c, rtxn, &params)? }))
        }
        Request::ExplainSql { query, params } => {
            let coll = nonodb_core::sql::collection_name(&query)?;
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(json!({
                    "collection": coll,
                    "access_path": "missing_collection",
                    "candidates": 0,
                    "actual_matched": 0,
                    "actual_returned": 0,
                }));
            };
            reject_vault_read(&c, None, "SQL explain in a read batch")?;
            Ok(nonodb_core::sql::explain_in(&c, rtxn, &query, &params)?)
        }
        Request::GetCollectionHistory { coll } => {
            let max_versions = store.collection_history(&coll)?;
            Ok(json!({ "coll": coll, "max_versions": max_versions }))
        }
        Request::GetCollectionEvents { coll } => {
            let config = store.collection_events(&coll)?;
            Ok(json!({
                "coll": coll,
                "enabled": config.enabled,
                "fields": config.fields,
                "ops": config.ops,
                "filter": config.filter,
                "include_before": config.include_before,
                "include_after": config.include_after,
                "projection": config.projection,
            }))
        }
        Request::GetCollectionMetrics { coll } => Ok(metrics::collection_snapshot(database, &coll)),
        Request::DatabaseStats => Err(ApiError {
            code: "invalid_query".into(),
            message: "database_stats is not supported inside a read batch".into(),
        }),
        Request::GetQuota | Request::SetQuota { .. } => Err(ApiError {
            code: "invalid_query".into(),
            message: "quota operations are not supported inside a read batch".into(),
        }),
        Request::ListSubscriptions { coll } => {
            let list = store.list_subscriptions(&coll)?;
            Ok(json!({ "subscriptions": list }))
        }
        Request::ListIndexes { coll } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(json!({ "indexes": [] }));
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            let list: Vec<Value> = c
                .index_defs_in(rtxn)?
                .into_iter()
                .map(|d| {
                    json!({
                        "field": d.name(),
                        "fields": d.effective_fields(),
                        "unique": d.unique,
                        "sparse": d.sparse,
                        "partial_filter_expression": d.partial_filter_expression,
                        "include": d.include,
                        "format": d.format,
                        "geo": d.geo,
                        "multikey_fields": d.multikey_fields,
                        "multikey_all_docs": d.multikey_all_docs,
                    })
                })
                .collect();
            Ok(json!({ "indexes": list }))
        }
        Request::ListTextIndexes { coll } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(json!({ "text_indexes": [] }));
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            Ok(json!({ "text_indexes": c.list_text_indexes_in(rtxn)? }))
        }
        Request::IndexStatus { coll } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(json!({ "builds": [] }));
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            let builds: Vec<_> = nonodb_core::progress::snapshot()
                .into_iter()
                .filter(|build| build.collection == coll)
                .collect();
            Ok(json!({ "builds": builds }))
        }
        Request::ListFuzzyIndexes { coll } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(json!({ "fuzzy_indexes": [] }));
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            Ok(json!({ "fuzzy_indexes": c.list_fuzzy_indexes_in(rtxn)? }))
        }
        Request::ListVectorIndexes { coll } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(json!({ "vector_indexes": [] }));
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            let list: Vec<Value> = c
                .list_vector_indexes_in(rtxn)?
                .into_iter()
                .map(|d| {
                    json!({
                        "field": d.field,
                        "dims": d.dims,
                        "metric": d.metric.as_str(),
                        "format": d.format,
                        "ann": d.ann,
                    })
                })
                .collect();
            Ok(json!({ "vector_indexes": list }))
        }
        Request::VectorSearch {
            coll,
            field,
            vector,
            filter,
            skip,
            limit,
        } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Err(nonodb_core::Error::Query(format!(
                    "collection `{coll}` does not exist"
                ))
                .into());
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            Ok(json!({
                "results": c.vector_search_in(rtxn, &field, &vector, &filter, skip, limit)?
            }))
        }
        Request::Search {
            coll,
            field,
            query,
            filter,
            skip,
            limit,
        } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Err(nonodb_core::Error::Query(format!(
                    "collection `{coll}` does not exist"
                ))
                .into());
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            Ok(json!({
                "results": c.search_in(rtxn, &field, &query, &filter, skip, limit)?
            }))
        }
        Request::Fuzzy {
            coll,
            field,
            query,
            max_distance,
            filter,
            limit,
        } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Err(nonodb_core::Error::Query(format!(
                    "collection `{coll}` does not exist"
                ))
                .into());
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            let distance = max_distance.unwrap_or(DEFAULT_FUZZY_DISTANCE);
            Ok(json!({
                "results": c.fuzzy_search_in(rtxn, &field, &query, distance, &filter, limit)?
            }))
        }
        Request::SearchAll { request } => {
            let Some(c) = store.collection_at(rtxn, &request.coll)? else {
                return Err(nonodb_core::Error::Query(format!(
                    "collection `{}` does not exist",
                    request.coll
                ))
                .into());
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            Ok(search_all_response(c.search_all_in(rtxn, &request)?))
        }
        Request::History {
            coll,
            id,
            limit,
            include_docs,
        } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(json!({ "revisions": [] }));
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            Ok(json!({
                "revisions": c.history_in(rtxn, &id, limit, include_docs)?
            }))
        }
        Request::GetVersion { coll, id, version } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(Value::Null);
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            let entry = c.get_version_in(rtxn, &id, version)?;
            Ok(serde_json::to_value(entry).map_err(nonodb_core::Error::from)?)
        }
        Request::ListCollections => {
            let mut collections = store.collections_in(rtxn)?;
            collections.retain(|coll| principal.can_see_collection(database, coll));
            Ok(json!({ "collections": collections }))
        }
        _ => Err(ApiError {
            code: "invalid_query".to_string(),
            message: "only read ops are allowed in batch".to_string(),
        }),
    }
}

enum FindOutput {
    Value(Value),
    Raw(Vec<Vec<u8>>),
}

async fn read_result(
    joined: tokio::task::JoinHandle<Result<Value, ApiError>>,
) -> (StatusCode, Json<ApiResponse>) {
    metrics::inc(&metrics::READ_OPS);
    match tokio::time::timeout(request_timeout(), joined).await {
        Ok(joined) => respond(flatten_join(joined)),
        Err(_) => respond(Err(ApiError::timeout())),
    }
}

async fn read_find_result(
    joined: tokio::task::JoinHandle<Result<FindOutput, ApiError>>,
) -> (StatusCode, Json<ApiResponse>) {
    metrics::inc(&metrics::READ_OPS);
    match tokio::time::timeout(request_timeout(), joined).await {
        Ok(Ok(Ok(FindOutput::Value(value)))) => respond(Ok(value)),
        Ok(Ok(Ok(FindOutput::Raw(docs)))) => respond_raw_docs(docs),
        Ok(Ok(Err(err))) => respond(Err(err)),
        Ok(Err(e)) => respond(Err(ApiError::internal(format!("join error: {e}")))),
        Err(_) => respond(Err(ApiError::timeout())),
    }
}

fn respond_raw_docs(docs: Vec<Vec<u8>>) -> (StatusCode, Json<ApiResponse>) {
    let capacity = docs.iter().map(Vec::len).sum::<usize>() + docs.len() + 2;
    let mut out = String::with_capacity(capacity);
    out.push('[');
    for (index, doc) in docs.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        match std::str::from_utf8(doc) {
            Ok(text) => out.push_str(text),
            Err(e) => {
                return respond(Err(ApiError::internal(format!(
                    "stored document is not valid UTF-8: {e}"
                ))));
            }
        }
    }
    out.push(']');
    match RawValue::from_string(out) {
        Ok(raw) => (StatusCode::OK, Json(ApiResponse::ok_raw(raw))),
        Err(e) => respond(Err(ApiError::internal(format!("invalid stored JSON: {e}")))),
    }
}

fn flatten_join(
    joined: Result<Result<Value, ApiError>, tokio::task::JoinError>,
) -> Result<Value, ApiError> {
    match joined {
        Ok(inner) => inner,
        Err(e) => Err(ApiError::internal(format!("join error: {e}"))),
    }
}

pub(crate) fn error_status(err: &ApiError) -> StatusCode {
    match err.code.as_str() {
        "duplicate_key"
        | "invalid_id"
        | "invalid_query"
        | "invalid_sql"
        | "invalid_json"
        | "invalid_collection"
        | "vault_key_required"
        | "vault_key_unexpected"
        | "invalid_vault_key"
        | "tenant_mismatch"
        | "unsupported_encrypted_operation"
        | "decryption_failed"
        | "not_a_vault_collection"
        | "invalid_vault_filter"
        | "invalid_vault_document"
        | "vault_profile_immutable"
        | "invalid_collection_profile" => StatusCode::BAD_REQUEST,
        "not_found" => StatusCode::NOT_FOUND,
        "database_limit" => StatusCode::SERVICE_UNAVAILABLE,
        "database_busy" => StatusCode::CONFLICT,
        "wal_full" | "quota_exceeded" => StatusCode::INSUFFICIENT_STORAGE,
        "unsafe_durability_disabled" => StatusCode::BAD_REQUEST,
        "unauthorized" => StatusCode::UNAUTHORIZED,
        "forbidden" | "read_only" => StatusCode::FORBIDDEN,
        "rate_limited" => StatusCode::TOO_MANY_REQUESTS,
        "document_too_large" => StatusCode::PAYLOAD_TOO_LARGE,
        "result_too_large" => StatusCode::PAYLOAD_TOO_LARGE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn respond(res: Result<Value, ApiError>) -> (StatusCode, Json<ApiResponse>) {
    match res {
        Ok(value) => (StatusCode::OK, Json(ApiResponse::ok(value))),
        Err(err) => {
            metrics::inc(&metrics::ERRORS);
            (error_status(&err), Json(ApiResponse::err(err)))
        }
    }
}

#[cfg(unix)]
fn peer_allowed(stream: &tokio::net::UnixStream) -> bool {
    static ALLOWED: std::sync::OnceLock<Option<(Vec<u32>, Vec<u32>)>> = std::sync::OnceLock::new();
    let allowed = ALLOWED.get_or_init(|| {
        let uids = parse_ids("NONODB_ALLOWED_UIDS");
        let gids = parse_ids("NONODB_ALLOWED_GIDS");
        (!uids.is_empty() || !gids.is_empty()).then_some((uids, gids))
    });
    let Some((uids, gids)) = allowed else {
        return true;
    };
    let Ok(cred) = stream.peer_cred() else {
        return false;
    };
    let uid = cred.uid();
    let gid = cred.gid();
    uids.contains(&uid) || gids.contains(&gid)
}

#[cfg(unix)]
fn parse_ids(name: &str) -> Vec<u32> {
    std::env::var(name)
        .ok()
        .map(|raw| {
            raw.split(',')
                .filter_map(|s| s.trim().parse::<u32>().ok())
                .collect()
        })
        .unwrap_or_default()
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let term = async {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut sig) = signal(SignalKind::terminate()) {
            sig.recv().await;
        }
    };

    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = term => {},
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn drain_waits_for_inflight_connections() {
        let mut connections = JoinSet::new();
        connections.spawn(async {
            tokio::time::sleep(Duration::from_millis(25)).await;
        });
        let started = Instant::now();
        drain_connections(connections, Duration::from_secs(1), "test").await;
        assert!(started.elapsed() >= Duration::from_millis(20));
    }

    #[tokio::test]
    async fn drain_aborts_after_timeout() {
        let mut connections = JoinSet::new();
        connections.spawn(std::future::pending::<()>());
        let started = Instant::now();
        drain_connections(connections, Duration::from_millis(25), "test").await;
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
