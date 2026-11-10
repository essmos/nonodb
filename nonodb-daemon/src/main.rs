//! nonodb daemon: HTTP/JSON over a Unix socket.
//!
//! One process is the sole owner of the LMDB file. Clients (e.g. uvicorn
//! processes) connect through `/run/nonodb.sock` (paths configurable via
//! `NONODB_SOCK` i `NONODB_DATA`).

mod auto_index;
mod cache;
mod maintenance;
mod metrics;
mod protocol;
mod writer;

use std::collections::HashMap;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use ring::digest::{digest, SHA256};
use serde_json::{json, Value};

use nonodb_core::heed::RoTxn;
use nonodb_core::{
    vault, CollectionProfile, FindQuery, HistoryConfig, Store, VaultKey, DEFAULT_FUZZY_DISTANCE,
};
use protocol::{ApiError, ApiResponse, Request, VaultAuth, WriteReq};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::rustls::pki_types::CertificateDer;
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;
use writer::WriteJob;

/// Maximum request body size (4 MiB).
const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;
/// Timeout for a single API request.
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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,nonodb=debug".into()),
        )
        .init();

    let data = PathBuf::from(std::env::var("NONODB_DATA").unwrap_or_else(|_| "./data".into()));
    #[cfg(unix)]
    let sock =
        PathBuf::from(std::env::var("NONODB_SOCK").unwrap_or_else(|_| "/run/nonodb.sock".into()));

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

    let state = AppState {
        store,
        write_tx,
        cache,
        auto_index,
    };
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/metrics", get(metrics_handler))
        .route("/v1/exec", post(exec))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(state);

    // Optional TCP listener (with TLS when a certificate and key are provided).
    let tcp_addr = std::env::var("NONODB_TCP").ok();
    if let Some(addr) = tcp_addr.as_deref() {
        if std::env::var("NONODB_AUTH_TOKEN")
            .ok()
            .filter(|t| !t.is_empty())
            .is_none()
        {
            return Err("NONODB_TCP wymaga NONODB_AUTH_TOKEN".into());
        }
        let tls = match (
            std::env::var("NONODB_TLS_CERT"),
            std::env::var("NONODB_TLS_KEY"),
        ) {
            (Ok(cert), Ok(key)) => Some(tls_acceptor(&cert, &key)?),
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
        tokio::spawn(async move {
            if let Err(e) = serve_tcp(&addr, tls, app_tcp).await {
                tracing::error!(?e, %addr, "listener TCP zakończony");
            }
        });
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
            data = %data.display(),
            tcp = tcp_addr.as_deref().unwrap_or(""),
            "nonodb daemon listening"
        );
        shutdown_signal().await;
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
            data = %data.display(),
            sock = %sock.display(),
            "nonodb daemon listening"
        );

        let mut shutdown = Box::pin(shutdown_signal());
        loop {
            let stream = tokio::select! {
                _ = &mut shutdown => break,
                accepted = listener.accept() => accepted?.0,
            };

            if !peer_allowed(&stream) {
                tracing::warn!("odrzucono połączenie UDS: niedozwolony uid/gid");
                continue;
            }

            let io = TokioIo::new(stream);
            let service = TowerToHyperService::new(app.clone());
            tokio::spawn(async move {
                if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                    tracing::debug!(?err, "connection closed");
                }
            });
        }

        let _ = std::fs::remove_file(&sock);
        tracing::info!("nonodb daemon stopped");
        Ok(())
    }
}

#[derive(Clone)]
struct TlsSetup {
    acceptor: TlsAcceptor,
    allowlist: Option<Arc<ClientAllowList>>,
    client_auth: bool,
}

/// Optional fingerprint allow-list. The file is re-read for every TLS
/// handshake, so removing an entry revokes that certificate immediately.
struct ClientAllowList {
    path: PathBuf,
}

impl ClientAllowList {
    fn load(&self) -> Result<HashMap<[u8; 32], String>, String> {
        let raw = std::fs::read_to_string(&self.path).map_err(|e| {
            format!(
                "nie można odczytać NONODB_TLS_CLIENT_ALLOW {}: {e}",
                self.path.display()
            )
        })?;
        let mut out = HashMap::new();
        for (line_no, line) in raw.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut parts = line.split_whitespace();
            let raw_fp = parts.next().ok_or_else(|| {
                format!("{}:{}: brak fingerprintu", self.path.display(), line_no + 1)
            })?;
            let raw_fp = raw_fp.strip_prefix("sha256:").unwrap_or(raw_fp);
            let hex: String = raw_fp.chars().filter(|c| *c != ':').collect();
            if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(format!(
                    "{}:{}: fingerprint musi mieć 64 znaki hex",
                    self.path.display(),
                    line_no + 1
                ));
            }
            let mut fp = [0u8; 32];
            for (i, byte) in fp.iter_mut().enumerate() {
                *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
                    .map_err(|e| format!("{}:{}: {e}", self.path.display(), line_no + 1))?;
            }
            let label = parts.collect::<Vec<_>>().join(" ");
            let label = if label.is_empty() {
                format!("sha256:{hex}")
            } else {
                label
            };
            out.insert(fp, label);
        }
        Ok(out)
    }

    fn authorize(&self, cert: &CertificateDer<'_>) -> Result<String, String> {
        let fingerprint = fingerprint(cert);
        let clients = self.load()?;
        clients.get(&fingerprint).cloned().ok_or_else(|| {
            format!(
                "fingerprint sha256:{} nie jest na allow-liście",
                hex(&fingerprint)
            )
        })
    }
}

fn fingerprint(cert: &CertificateDer<'_>) -> [u8; 32] {
    let digest = digest(&SHA256, cert.as_ref());
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_ref());
    out
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
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

fn tls_acceptor(cert: &str, key: &str) -> Result<TlsSetup, Box<dyn std::error::Error>> {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let mut cert_reader = BufReader::new(std::fs::File::open(cert)?);
    let certs = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;
    let mut key_reader = BufReader::new(std::fs::File::open(key)?);
    let key = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or("brak klucza prywatnego w NONODB_TLS_KEY")?;

    let client_ca = std::env::var("NONODB_TLS_CLIENT_CA")
        .ok()
        .filter(|value| !value.is_empty());
    let auth = std::env::var("NONODB_TLS_CLIENT_AUTH")
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
    if !matches!(auth.as_str(), "off" | "required" | "optional") {
        return Err("NONODB_TLS_CLIENT_AUTH musi być: off, required lub optional".into());
    }

    let allowlist = std::env::var_os("NONODB_TLS_CLIENT_ALLOW")
        .map(PathBuf::from)
        .map(|path| Arc::new(ClientAllowList { path }));
    if allowlist.is_some() && client_ca.is_none() {
        return Err("NONODB_TLS_CLIENT_ALLOW wymaga NONODB_TLS_CLIENT_CA".into());
    }

    if client_ca.is_some() && auth == "off" {
        return Err(
            "NONODB_TLS_CLIENT_AUTH=off nie może być łączone z NONODB_TLS_CLIENT_CA".into(),
        );
    }

    let builder = ServerConfig::builder();
    let client_auth = client_ca.is_some();
    let config = if let Some(ca) = client_ca {
        let roots = read_client_ca(Path::new(&ca))?;
        let mut verifier = WebPkiClientVerifier::builder(roots);
        if auth == "optional" {
            verifier = verifier.allow_unauthenticated();
        }
        let verifier = verifier.build()?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)?
    } else {
        if auth != "off" {
            return Err("NONODB_TLS_CLIENT_AUTH wymaga NONODB_TLS_CLIENT_CA".into());
        }
        builder.with_no_client_auth().with_single_cert(certs, key)?
    };

    Ok(TlsSetup {
        acceptor: TlsAcceptor::from(Arc::new(config)),
        allowlist,
        client_auth,
    })
}

async fn serve_tcp(
    addr: &str,
    tls: Option<TlsSetup>,
    app: Router,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(
        %addr,
        tls = tls.is_some(),
        mtls = tls.as_ref().is_some_and(|t| t.client_auth),
        allowlist = tls.as_ref().is_some_and(|t| t.allowlist.is_some()),
        "TCP listener"
    );
    loop {
        let (stream, peer) = listener.accept().await?;
        let app = app.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            let Some(tls) = tls else {
                serve_stream(stream, app).await;
                return;
            };
            match tls.acceptor.accept(stream).await {
                Ok(stream) => {
                    if let Some(allowlist) = &tls.allowlist {
                        let cert = stream
                            .get_ref()
                            .1
                            .peer_certificates()
                            .and_then(|certs| certs.first())
                            .cloned();
                        match cert {
                            Some(cert) => match allowlist.authorize(&cert) {
                                Ok(client) => {
                                    tracing::debug!(%peer, %client, "mTLS client accepted");
                                }
                                Err(reason) => {
                                    tracing::warn!(%peer, %reason, "mTLS client rejected");
                                    return;
                                }
                            },
                            None => {
                                tracing::warn!(%peer, "mTLS client rejected: brak certyfikatu");
                                return;
                            }
                        }
                    }
                    serve_stream(stream, app).await;
                }
                Err(e) => tracing::debug!(?e, %peer, "TLS handshake failed"),
            }
        });
    }
}

async fn serve_stream<I>(io: I, app: Router)
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let io = TokioIo::new(io);
    let service = TowerToHyperService::new(app);
    if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
        tracing::debug!(?err, "connection closed");
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
                match store
                    .env()
                    .copy_to_file(&path, nonodb_core::heed::CompactionOption::Enabled)
                {
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

async fn health() -> Json<Value> {
    Json(json!({ "ok": true }))
}

async fn metrics_handler(headers: HeaderMap) -> (StatusCode, Json<Value>) {
    if !auth_ok(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(
                json!({ "ok": false, "error": { "code": "unauthorized", "message": "invalid token" } }),
            ),
        );
    }
    (StatusCode::OK, Json(metrics::snapshot()))
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

async fn exec(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<Request>,
) -> (StatusCode, Json<ApiResponse>) {
    metrics::inc(&metrics::REQUESTS);
    if !auth_ok(&headers) {
        return respond(Err(ApiError {
            code: "unauthorized".into(),
            message: "missing or invalid bearer token".into(),
        }));
    }
    let vault_auth = match vault_auth(&headers) {
        Ok(auth) => auth,
        Err(err) => return respond(Err(err)),
    };
    let cache_key = if vault_auth.is_none() && cacheable(&req) {
        state.cache.key(&req)
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
            return respond(Ok(value));
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
            let n = ops.len();
            metrics::add(&metrics::BATCH_OPS, n as u64);
            let joined = tokio::task::spawn_blocking(move || {
                let rtxn = store.read_txn()?;
                let mut results = Vec::with_capacity(ops.len());
                for op in ops {
                    results.push(batch_read(&store, &rtxn, op));
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
            let joined = tokio::task::spawn_blocking(move || {
                Ok::<Value, ApiError>(json!({ "collections": store.collections()? }))
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
                } => WriteReq::Update {
                    coll,
                    filter,
                    update,
                    multi,
                    upsert,
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
    response
}

fn cacheable(req: &Request) -> bool {
    matches!(
        req,
        Request::Find { .. }
            | Request::FindOne { .. }
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
    match tokio::time::timeout(request_timeout(), rx).await {
        Ok(Ok(res)) => respond(res),
        Ok(Err(_)) => respond(Err(ApiError::internal("writer dropped request"))),
        Err(_) => respond(Err(ApiError::timeout())),
    }
}

fn batch_read(store: &Store, rtxn: &RoTxn<'_>, req: Request) -> Value {
    match execute_read(store, rtxn, req) {
        Ok(value) => json!({ "ok": true, "result": value }),
        Err(e) => json!({ "ok": false, "error": { "code": e.code, "message": e.message } }),
    }
}

fn execute_read(store: &Store, rtxn: &RoTxn<'_>, req: Request) -> Result<Value, ApiError> {
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
        Request::ListCollections => Ok(json!({ "collections": store.collections_in(rtxn)? })),
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

fn respond(res: Result<Value, ApiError>) -> (StatusCode, Json<ApiResponse>) {
    match res {
        Ok(value) => (StatusCode::OK, Json(ApiResponse::ok(value))),
        Err(err) => {
            metrics::inc(&metrics::ERRORS);
            let status = match err.code.as_str() {
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
                "document_too_large" => StatusCode::PAYLOAD_TOO_LARGE,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(ApiResponse::err(err)))
        }
    }
}

fn auth_ok(headers: &HeaderMap) -> bool {
    static TOKEN: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    let expected = TOKEN.get_or_init(|| {
        std::env::var("NONODB_AUTH_TOKEN")
            .ok()
            .filter(|t| !t.is_empty())
    });
    let Some(expected) = expected.as_deref() else {
        return true;
    };
    let Some(value) = headers.get(axum::http::header::AUTHORIZATION) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let Some(provided) = value.strip_prefix("Bearer ") else {
        return false;
    };
    constant_time_eq(provided.as_bytes(), expected.as_bytes())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
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
