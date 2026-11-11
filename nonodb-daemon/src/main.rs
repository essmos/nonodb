//! nonodb daemon: HTTP/JSON over a Unix socket.
//!
//! One process is the sole owner of the LMDB file. Clients (e.g. uvicorn
//! processes) connect through `/run/nonodb.sock` (paths configurable via
//! `NONODB_SOCK` i `NONODB_DATA`).

mod audit;
mod auth;
mod authz;
mod auto_index;
mod cache;
mod cursor;
mod login_guard;
mod maintenance;
mod metrics;
mod protocol;
mod writer;

use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Extension, Json, Router,
};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use ring::digest::{digest, SHA256};
use serde_json::{json, Value};

use nonodb_core::heed::RoTxn;
use nonodb_core::{
    aggregate, project_document, vault, CollectionProfile, FindQuery, HistoryConfig, Store,
    VaultKey, DEFAULT_FUZZY_DISTANCE, PAGE_SIZE,
};
use protocol::{ApiError, ApiResponse, Request, VaultAuth, WriteReq};
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
    auth: Option<Arc<auth::AuthState>>,
    audit: Option<Arc<audit::AuditLog>>,
    cursors: Arc<cursor::Registry>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Explicit runtime: limit the blocking thread pool (lower peak VSZ),
    // because LMDB operations run through `spawn_blocking`.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .max_blocking_threads(64)
        .enable_all()
        .build()?;
    rt.block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let data = PathBuf::from(std::env::var("NONODB_DATA").unwrap_or_else(|_| "./data".into()));
    #[cfg(unix)]
    let sock =
        PathBuf::from(std::env::var("NONODB_SOCK").unwrap_or_else(|_| "/run/nonodb.sock".into()));
    let tcp_addr = std::env::var("NONODB_TCP").ok();

    let auth = match std::env::var_os("NONODB_AUTH_FILE") {
        Some(path) if !path.is_empty() => Some(auth::AuthState::open(PathBuf::from(path))?),
        _ => None,
    };
    if tcp_addr.is_some() && auth.is_none() {
        return Err("NONODB_TCP wymaga NONODB_AUTH_FILE".into());
    }
    if let Some(auth) = &auth {
        auth.spawn_watcher();
    }
    let audit = audit::AuditLog::from_env()?;

    let store = Arc::new(Store::open_with_history(&data, HistoryConfig::from_env())?);

    // One writer thread, independent of the tokio runtime (LMDB blocks on fsync).
    let (write_tx, write_rx) = mpsc::channel::<WriteJob>();
    let cache = Arc::new(cache::QueryCache::from_env());
    let writer_store = store.clone();
    let writer_cache = cache.clone();
    std::thread::Builder::new()
        .name("nonodb-writer".into())
        .spawn(move || writer::run(writer_store, write_rx, writer_cache))?;
    let auto_index = auto_index::AutoIndexer::start(store.clone(), write_tx.clone(), &data);
    maintenance::start(store.clone(), write_tx.clone(), auto_index.clone());

    if let Some(dir) = std::env::var_os("NONODB_SNAPSHOT_DIR") {
        spawn_snapshots(store.clone(), PathBuf::from(dir));
    }

    let drain_timeout = drain_timeout();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!("shutdown signal received; draining");
        let _ = shutdown_tx.send(true);
    });

    let state = AppState {
        store,
        write_tx,
        cache,
        auto_index,
        auth: auth.clone(),
        audit: audit.clone(),
        cursors: Arc::new(cursor::Registry::new()),
    };
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/metrics", get(metrics_handler))
        .route("/v1/exec", post(exec))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .layer(axum::middleware::from_fn(http_access_log))
        .with_state(state);

    // Optional TCP listener (with TLS when a certificate and key are provided).
    let mut tcp_task: Option<tokio::task::JoinHandle<()>> = None;
    if let Some(addr) = tcp_addr.as_deref() {
        let auth = auth.clone().ok_or("NONODB_TCP wymaga NONODB_AUTH_FILE")?;
        let tls = match (
            std::env::var("NONODB_TLS_CERT"),
            std::env::var("NONODB_TLS_KEY"),
        ) {
            (Ok(cert), Ok(key)) => Some(tls_acceptor(&cert, &key, auth.clone(), audit.clone())?),
            (Err(_), Err(_)) => {
                if std::env::var_os("NONODB_TLS_CLIENT_CA").is_some() {
                    return Err(
                        "NONODB_TLS_CLIENT_CA wymaga NONODB_TLS_CERT i NONODB_TLS_KEY".into(),
                    );
                }
                tracing::warn!("NONODB_TCP bez TLS (brak NONODB_TLS_CERT/KEY)");
                None
            }
            _ => return Err("podaj oba: NONODB_TLS_CERT i NONODB_TLS_KEY".into()),
        };
        let app_tcp = app.clone();
        let addr = addr.to_string();
        let shutdown = shutdown_rx.clone();
        tcp_task = Some(tokio::spawn(async move {
            if let Err(e) = serve_tcp(&addr, tls, app_tcp, shutdown, drain_timeout).await {
                tracing::error!(?e, %addr, "listener TCP zakończony");
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
            "nonodb daemon listening"
        );
        let mut shutdown = shutdown_rx.clone();
        wait_for_shutdown(&mut shutdown).await;
        if let Some(task) = tcp_task {
            if let Err(e) = task.await {
                tracing::warn!(?e, "TCP listener task failed during shutdown");
            }
        }
        wait_for_writer(drain_timeout).await;
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
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o660))?;
        }

        tracing::info!(
            version = env!("CARGO_PKG_VERSION"),
            data = %data.display(),
            sock = %sock.display(),
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
                tracing::warn!("odrzucono połączenie UDS: niedozwolony uid/gid");
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
        wait_for_writer(drain_timeout).await;
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
    acceptor: TlsAcceptor,
    auth: Arc<auth::AuthState>,
    client_auth: bool,
    audit: Option<Arc<audit::AuditLog>>,
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
        return Err(format!("brak certyfikatów CA w {}", path.display()).into());
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
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let mut cert_reader = BufReader::new(std::fs::File::open(cert)?);
    let certs = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;
    let mut key_reader = BufReader::new(std::fs::File::open(key)?);
    let key = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or("brak klucza prywatnego w NONODB_TLS_KEY")?;

    let client_ca = std::env::var("NONODB_TLS_CLIENT_CA")
        .ok()
        .filter(|value| !value.is_empty());
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
        return Err("NONODB_TLS_CLIENT_AUTH musi być: off, required lub optional".into());
    }

    if client_ca.is_some() && client_auth_mode == "off" {
        return Err(
            "NONODB_TLS_CLIENT_AUTH=off nie może być łączone z NONODB_TLS_CLIENT_CA".into(),
        );
    }

    let builder = ServerConfig::builder();
    let client_auth = client_ca.is_some();
    let config = if let Some(ca) = client_ca {
        let roots = read_client_ca(Path::new(&ca))?;
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
            return Err("NONODB_TLS_CLIENT_AUTH wymaga NONODB_TLS_CLIENT_CA".into());
        }
        builder.with_no_client_auth().with_single_cert(certs, key)?
    };

    Ok(TlsSetup {
        acceptor: TlsAcceptor::from(Arc::new(config)),
        auth,
        client_auth,
        audit,
    })
}

async fn serve_tcp(
    addr: &str,
    tls: Option<TlsSetup>,
    app: Router,
    mut shutdown: watch::Receiver<bool>,
    drain_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
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
            let mut handshake_shutdown = shutdown.clone();
            let stream = tokio::select! {
                _ = wait_for_shutdown(&mut handshake_shutdown) => return,
                accepted = tls.acceptor.accept(stream) => match accepted {
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

/// Periodic LMDB snapshots (consistent copies without stopping writes).
/// This is not full PITR with a WAL - it provides restore points every N seconds.
fn spawn_snapshots(store: Arc<Store>, dir: PathBuf) {
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
        .name("nonodb-snapshots".into())
        .spawn(move || {
            if let Err(e) = std::fs::create_dir_all(&dir) {
                tracing::error!(?e, dir = %dir.display(), "nie można utworzyć katalogu snapshotów");
                return;
            }
            loop {
                let stamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis())
                    .unwrap_or(0);
                let path = dir.join(format!("data-{stamp}.mdb"));
                match store.copy_to_file(&path, nonodb_core::heed::CompactionOption::Enabled) {
                    Ok(file) => {
                        if let Err(e) = file.sync_all() {
                            tracing::warn!(?e, "fsync snapshotu nie powiódł się");
                        }
                        tracing::info!(path = %path.display(), "snapshot zapisany");
                        prune_snapshots(&dir, keep);
                    }
                    Err(e) => tracing::error!(?e, "snapshot nie powiódł się"),
                }
                std::thread::sleep(interval);
            }
        })
        .ok();
}

fn prune_snapshots(dir: &std::path::Path, keep: usize) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut files: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("data-") && n.ends_with(".mdb"))
        })
        .collect();
    files.sort();
    let excess = files.len().saturating_sub(keep);
    for old in files.into_iter().take(excess) {
        if let Err(e) = std::fs::remove_file(&old) {
            tracing::warn!(?e, path = %old.display(), "nie można usunąć starego snapshotu");
        }
    }
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
    if !principal.allows_admin_database() {
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

#[allow(clippy::too_many_arguments)]
fn audit_control(
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
fn authenticate_with_guard(
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
        [(field, direction)] if field == "_id" && (*direction == 1 || *direction == -1) => {
            Ok(*direction > 0)
        }
        _ => Err(ApiError {
            code: "invalid_query".to_string(),
            message: "cursor find requires an empty sort or a single _id sort".to_string(),
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

fn cursor_filter(base: &Value, last_id: Option<&str>, ascending: bool) -> Value {
    let mut filters = vec![base.clone()];
    if let Some(last_id) = last_id {
        let op = if ascending { "$gt" } else { "$lt" };
        filters.push(json!({"_id": {op: last_id}}));
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
        cursor::Kind::Find { filter, .. } => {
            cursor_filter(filter, state.last_id.as_deref(), ascending)
        }
        cursor::Kind::Aggregate { filter, .. } => {
            cursor_filter(filter, state.last_id.as_deref(), ascending)
        }
    };
    let query = FindQuery {
        filter,
        projection: None,
        sort: vec![("_id".to_string(), if ascending { 1 } else { -1 })],
        skip: state.skip,
        limit: Some(fetch),
    };
    let docs = c.find(&query)?;
    state.skip = 0;
    if docs.is_empty() {
        return Ok((Vec::new(), false));
    }
    state.last_id = docs
        .last()
        .and_then(|doc| doc.get("_id"))
        .and_then(Value::as_str)
        .map(str::to_owned);
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
            aggregate::run(docs, &stages, aggregate::max_docs())?
        }
    };
    Ok((out, has_more))
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

async fn exec(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(req): Json<Request>,
) -> (StatusCode, Json<ApiResponse>) {
    metrics::inc(&metrics::REQUESTS);
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
    if let Err(err) = authz::authorize_request(&principal, &req) {
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
    let audit_collections: Vec<String> = req.collections().into_iter().map(str::to_owned).collect();
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
                };
                if c.profile().is_vault() {
                    let auth = auth.as_ref().ok_or_else(vault::key_required)?;
                    let rtxn = store.read_txn()?;
                    let out = c.find_vault_in(&rtxn, &query, &auth.key, &auth.tenant, &auth.kid)?;
                    rtxn.commit().map_err(nonodb_core::Error::from)?;
                    Ok::<Value, ApiError>(Value::Array(out))
                } else {
                    if auth.is_some() {
                        return Err(vault::key_unexpected().into());
                    }
                    Ok::<Value, ApiError>(Value::Array(c.find(&query)?))
                }
            });
            read_result(joined).await
        }

        Request::FindOne {
            coll,
            filter,
            projection,
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

        Request::CursorFind {
            coll,
            filter,
            projection,
            sort,
            skip,
            limit,
            batch_size,
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
                skip,
            };
            let cursor_state =
                cursor::State::new(principal.name().to_string(), kind, limit, batch_size);
            open_cursor(&state, vault_auth.clone(), cursor_state, None).await
        }

        Request::CursorAggregate {
            coll,
            pipeline,
            limit,
            batch_size,
        } => {
            let (filter, stages) = match cursor_pipeline(&pipeline) {
                Ok(parts) => parts,
                Err(err) => return respond(Err(err)),
            };
            let kind = cursor::Kind::Aggregate {
                coll,
                filter,
                stages,
            };
            let cursor_state =
                cursor::State::new(principal.name().to_string(), kind, limit, batch_size);
            open_cursor(&state, vault_auth.clone(), cursor_state, None).await
        }

        Request::GetMore {
            cursor_id,
            batch_size,
        } => {
            let owner = principal.name().to_string();
            let cursors = state.cursors.clone();
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let mut cursor_state =
                    cursors.begin(cursor_id, &owner).map_err(cursor_api_error)?;
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
                };
                Ok::<Value, ApiError>(c.explain(&query)?)
            });
            read_result(joined).await
        }

        Request::Count { coll, filter } => {
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
                    Ok::<Value, ApiError>(json!({ "count": c.count(&filter)? }))
                }
            });
            read_result(joined).await
        }

        Request::Aggregate { coll, pipeline } => {
            let store = state.store.clone();
            let auth = vault_auth.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = read_collection(&store, &coll, auth.as_ref())?;
                reject_vault_read(&c, auth.as_ref(), "aggregation")?;
                Ok::<Value, ApiError>(json!({ "results": c.aggregate(&pipeline)? }))
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
                            "include": d.include,
                            "format": d.format,
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
                    results.push(batch_read(&store, &rtxn, op, &principal));
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
                let purge = store.collection_ttl_purge(&coll)?;
                Ok::<Value, ApiError>(json!({
                    "coll": coll,
                    "ttl_seconds": ttl,
                    "purge_history": purge,
                }))
            });
            read_result(joined).await
        }

        Request::ListCollections => {
            let store = state.store.clone();
            let principal = principal.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let mut collections = store.collections()?;
                collections.retain(|coll| principal.can_see_collection(coll));
                Ok::<Value, ApiError>(json!({ "collections": collections }))
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

        Request::WriteBatch { ops } => {
            if vault_auth.is_some() {
                return respond(Err(
                    vault::unsupported("write_batch on vault collections").into()
                ));
            }
            let req = WriteReq::Batch {
                ops: ops.into_iter().map(WriteReq::from).collect(),
            };
            submit_write(&state, req).await
        }

        write_req => {
            let req = match write_req {
                Request::Insert { coll, doc } => WriteReq::Insert {
                    coll,
                    doc,
                    vault: vault_auth.clone(),
                },
                Request::InsertMany { coll, docs } => WriteReq::InsertMany {
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
                } => WriteReq::Update {
                    coll,
                    filter,
                    update,
                    multi,
                    upsert,
                    array_filters,
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
                    vault: vault_auth.clone(),
                },
                Request::Delete {
                    coll,
                    filter,
                    multi,
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
                } => {
                    let profile = match profile {
                        Some(raw) => match CollectionProfile::parse(&raw) {
                            Ok(p) => p,
                            Err(e) => return respond(Err(e.into())),
                        },
                        None => CollectionProfile::Standard,
                    };
                    WriteReq::CreateCollection {
                        coll,
                        profile,
                        ttl_seconds,
                        ttl_purge_history,
                    }
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
                Request::CreateIndex {
                    coll,
                    field,
                    fields,
                    unique,
                    include,
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
                        include,
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
                _ => unreachable!("read requests handled above"),
            };

            submit_write(&state, req).await
        }
    };

    if response.0 == StatusCode::OK {
        if let (Some(key), Some(value)) = (cache_key, response.1 .0.result.clone()) {
            state.cache.put(key, cache_version, value);
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
    let collections: Vec<String> = req.collections().into_iter().map(str::to_owned).collect();
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

fn cacheable(req: &Request) -> bool {
    matches!(
        req,
        Request::Find { .. }
            | Request::FindOne { .. }
            | Request::Explain { .. }
            | Request::Count { .. }
            | Request::Aggregate { .. }
            | Request::Search { .. }
            | Request::Fuzzy { .. }
            | Request::ListIndexes { .. }
            | Request::ListTextIndexes { .. }
            | Request::ListFuzzyIndexes { .. }
            | Request::ListCollections
            | Request::GetCollectionTtl { .. }
            | Request::History { .. }
            | Request::GetVersion { .. }
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
            Some(CollectionProfile::Standard) => return Err(vault::key_unexpected().into()),
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

async fn submit_write(state: &AppState, req: WriteReq) -> (StatusCode, Json<ApiResponse>) {
    let timeout = if req.is_index_build() {
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
            isolated: false,
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
) -> Value {
    match execute_read(store, rtxn, req, principal) {
        Ok(value) => json!({ "ok": true, "result": value }),
        Err(e) => json!({ "ok": false, "error": { "code": e.code, "message": e.message } }),
    }
}

fn execute_read(
    store: &Store,
    rtxn: &RoTxn<'_>,
    req: Request,
    principal: &nonodb_auth::Principal,
) -> Result<Value, ApiError> {
    match req {
        Request::Find {
            coll,
            filter,
            projection,
            sort,
            skip,
            limit,
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
            };
            Ok(json!(c.find_in(rtxn, &query)?))
        }
        Request::FindOne {
            coll,
            filter,
            projection,
        } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(Value::Null);
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            Ok(json!(c.find_one_in(rtxn, &filter, projection.as_ref())?))
        }
        Request::Explain {
            coll,
            filter,
            sort,
            skip,
            limit,
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
            };
            Ok(c.explain_in(rtxn, &query)?)
        }
        Request::Count { coll, filter } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(json!({ "count": 0 }));
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            Ok(json!({ "count": c.count_in(rtxn, &filter)? }))
        }
        Request::Aggregate { coll, pipeline } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(json!({ "results": [] }));
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            Ok(json!({ "results": c.aggregate_in(rtxn, &pipeline)? }))
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
                        "include": d.include,
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
        Request::ListFuzzyIndexes { coll } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(json!({ "fuzzy_indexes": [] }));
            };
            if c.profile().is_vault() {
                return Err(vault::unsupported("batch reads on vault collections").into());
            }
            Ok(json!({ "fuzzy_indexes": c.list_fuzzy_indexes_in(rtxn)? }))
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
            collections.retain(|coll| principal.can_see_collection(coll));
            Ok(json!({ "collections": collections }))
        }
        _ => Err(ApiError {
            code: "invalid_query".to_string(),
            message: "only read ops are allowed in batch".to_string(),
        }),
    }
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

fn flatten_join(
    joined: Result<Result<Value, ApiError>, tokio::task::JoinError>,
) -> Result<Value, ApiError> {
    match joined {
        Ok(inner) => inner,
        Err(e) => Err(ApiError::internal(format!("join error: {e}"))),
    }
}

fn error_status(err: &ApiError) -> StatusCode {
    match err.code.as_str() {
        "duplicate_key"
        | "invalid_id"
        | "invalid_query"
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
        "unauthorized" => StatusCode::UNAUTHORIZED,
        "forbidden" => StatusCode::FORBIDDEN,
        "rate_limited" => StatusCode::TOO_MANY_REQUESTS,
        "document_too_large" => StatusCode::PAYLOAD_TOO_LARGE,
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
