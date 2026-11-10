//! nonodb daemon: HTTP/JSON po gnieździe uniksowym.
//!
//! Jeden proces jest wyłącznym właścicielem pliku LMDB. Klienci (np. procesy
//! uvicorn) łączą się przez `/run/nonodb.sock` (ścieżki konfigurowalne przez
//! `NONODB_SOCK` i `NONODB_DATA`).

mod auto_index;
mod cache;
mod metrics;
mod protocol;
mod writer;

use std::io::BufReader;
use std::path::PathBuf;
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
use serde_json::{json, Value};

use nonodb_core::heed::RoTxn;
use nonodb_core::{FindQuery, Store, DEFAULT_FUZZY_DISTANCE};
use protocol::{ApiError, ApiResponse, Request, WriteReq};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;
use writer::WriteJob;

/// Maksymalny rozmiar ciała żądania (4 MiB).
const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;
/// Timeout pojedynczego żądania API.
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
    // Jawny runtime: ograniczamy pulę wątków blokujących (mniej szczytowego VSZ),
    // bo operacje LMDB wykonują się przez `spawn_blocking`.
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

    let store = Arc::new(Store::open(&data)?);

    // Jeden wątek piszący, niezależny od runtime'u tokio (LMDB blokuje na fsync).
    let (write_tx, write_rx) = mpsc::channel::<WriteJob>();
    let cache = Arc::new(cache::QueryCache::from_env());
    let writer_store = store.clone();
    let writer_cache = cache.clone();
    std::thread::Builder::new()
        .name("nonodb-writer".into())
        .spawn(move || writer::run(writer_store, write_rx, writer_cache))?;
    let auto_index = auto_index::AutoIndexer::start(store.clone(), write_tx.clone(), &data);

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

    // Opcjonalny listener TCP (z TLS, jeśli podano certyfikat i klucz).
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
        // Daemon jest jedynym właścicielem socketa — usuwamy ewentualny osierocony plik.
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

fn tls_acceptor(cert: &str, key: &str) -> Result<TlsAcceptor, Box<dyn std::error::Error>> {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let mut cert_reader = BufReader::new(std::fs::File::open(cert)?);
    let certs = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;
    let mut key_reader = BufReader::new(std::fs::File::open(key)?);
    let key = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or("brak klucza prywatnego w NONODB_TLS_KEY")?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

async fn serve_tcp(
    addr: &str,
    tls: Option<TlsAcceptor>,
    app: Router,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, tls = tls.is_some(), "TCP listener");
    loop {
        let (stream, peer) = listener.accept().await?;
        let app = app.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            match tls {
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(stream) => serve_stream(stream, app).await,
                    Err(e) => tracing::debug!(?e, %peer, "TLS handshake failed"),
                },
                None => serve_stream(stream, app).await,
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

/// Okresowe snapshoty LMDB (kopie spójne, bez zatrzymywania zapisów).
/// To nie jest pełny PITR z WAL — daje punkty odtworzenia co N sekund.
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
    let cache_key = if cacheable(&req) {
        state.cache.key(&req)
    } else {
        None
    };
    let cache_version = state.cache.version();
    if let Some(key) = cache_key.as_deref() {
        if let Some(value) = state.cache.get(key) {
            state.auto_index.record(&req, false);
            metrics::inc(&metrics::READ_OPS);
            return respond(Ok(value));
        }
    }
    state.auto_index.record(&req, true);

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
            let joined = tokio::task::spawn_blocking(move || {
                let c = store.collection(&coll)?;
                let query = FindQuery {
                    filter,
                    projection,
                    sort,
                    skip,
                    limit,
                };
                Ok::<Value, ApiError>(Value::Array(c.find(&query)?))
            });
            read_result(joined).await
        }

        Request::FindOne {
            coll,
            filter,
            projection,
        } => {
            let store = state.store.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = store.collection(&coll)?;
                let query = FindQuery {
                    filter,
                    projection,
                    sort: Vec::new(),
                    skip: 0,
                    limit: Some(1),
                };
                Ok::<Value, ApiError>(c.find(&query)?.into_iter().next().unwrap_or(Value::Null))
            });
            read_result(joined).await
        }

        Request::Count { coll, filter } => {
            let store = state.store.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = store.collection(&coll)?;
                Ok::<Value, ApiError>(json!({ "count": c.count(&filter)? }))
            });
            read_result(joined).await
        }

        Request::ListIndexes { coll } => {
            let store = state.store.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = store.collection(&coll)?;
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
            let joined = tokio::task::spawn_blocking(move || {
                let c = store.collection(&coll)?;
                Ok::<Value, ApiError>(json!({ "text_indexes": c.list_text_indexes()? }))
            });
            read_result(joined).await
        }

        Request::ListFuzzyIndexes { coll } => {
            let store = state.store.clone();
            let joined = tokio::task::spawn_blocking(move || {
                let c = store.collection(&coll)?;
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
            let joined = tokio::task::spawn_blocking(move || {
                let c = store.collection(&coll)?;
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
            let joined = tokio::task::spawn_blocking(move || {
                let c = store.collection(&coll)?;
                let distance = max_distance.unwrap_or(DEFAULT_FUZZY_DISTANCE);
                Ok::<Value, ApiError>(json!({
                    "results": c.fuzzy_search(&field, &query, distance, &filter, limit)?
                }))
            });
            read_result(joined).await
        }

        Request::Batch { ops } => {
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

        Request::ListCollections => {
            let store = state.store.clone();
            let joined = tokio::task::spawn_blocking(move || {
                Ok::<Value, ApiError>(json!({ "collections": store.collections()? }))
            });
            read_result(joined).await
        }

        Request::WriteBatch { ops } => {
            let req = WriteReq::Batch {
                ops: ops.into_iter().map(WriteReq::from).collect(),
            };
            submit_write(&state, req).await
        }

        write_req => {
            let req = match write_req {
                Request::Insert { coll, doc } => WriteReq::Insert { coll, doc },
                Request::InsertMany { coll, docs } => WriteReq::InsertMany { coll, docs },
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
                },
                Request::Delete {
                    coll,
                    filter,
                    multi,
                } => WriteReq::Delete {
                    coll,
                    filter,
                    multi,
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
            | Request::Search { .. }
            | Request::Fuzzy { .. }
            | Request::ListIndexes { .. }
            | Request::ListTextIndexes { .. }
            | Request::ListFuzzyIndexes { .. }
            | Request::ListCollections
    )
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
            Ok(json!(c.find_one_in(rtxn, &filter, projection.as_ref())?))
        }
        Request::Count { coll, filter } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(json!({ "count": 0 }));
            };
            Ok(json!({ "count": c.count_in(rtxn, &filter)? }))
        }
        Request::ListIndexes { coll } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(json!({ "indexes": [] }));
            };
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
            Ok(json!({ "text_indexes": c.list_text_indexes_in(rtxn)? }))
        }
        Request::ListFuzzyIndexes { coll } => {
            let Some(c) = store.collection_at(rtxn, &coll)? else {
                return Ok(json!({ "fuzzy_indexes": [] }));
            };
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
            let distance = max_distance.unwrap_or(DEFAULT_FUZZY_DISTANCE);
            Ok(json!({
                "results": c.fuzzy_search_in(rtxn, &field, &query, distance, &filter, limit)?
            }))
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
                "duplicate_key" | "invalid_id" | "invalid_query" | "invalid_json"
                | "invalid_collection" => StatusCode::BAD_REQUEST,
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
