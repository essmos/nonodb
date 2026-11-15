//! Admin-only replication status and change-log inspection.

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::Deserialize;
use tokio_util::io::ReaderStream;

use crate::{
    audit_control,
    auth::{self},
    auth_api::{failure, ok, start_admin, AuthResponse},
    AppState,
};

#[derive(Debug, Deserialize)]
pub struct ChangesQuery {
    #[serde(default)]
    pub after: u64,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    100
}

#[derive(Debug, Deserialize)]
pub struct PruneBody {
    pub through: u64,
    #[serde(default)]
    pub require_acks: bool,
}

#[derive(Debug, Deserialize)]
pub struct AckBody {
    pub node: String,
    pub origin: String,
    pub seq: u64,
    #[serde(default)]
    pub advertise_url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct AcksQuery {
    #[serde(default)]
    pub origin: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct VoteBody {
    pub epoch: u64,
    pub candidate: String,
}

#[derive(Debug, Deserialize)]
pub struct LeaseBody {
    pub epoch: u64,
    pub candidate: String,
    #[serde(default = "default_lease_ttl")]
    pub ttl_seconds: u64,
}

fn default_lease_ttl() -> u64 {
    15
}

#[derive(Debug, Deserialize)]
pub struct RoleBody {
    pub expected_epoch: u64,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub successor: Option<String>,
    #[serde(default)]
    pub force: bool,
}

#[derive(Debug, Deserialize)]
pub struct SourceBody {
    pub source: String,
    #[serde(default)]
    pub databases: Option<Vec<String>>,
    #[serde(default)]
    pub interval_secs: Option<u64>,
    #[serde(default)]
    pub batch: Option<usize>,
    #[serde(default)]
    pub bootstrap: Option<String>,
}

pub async fn status(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Path(database): Path<String>,
) -> AuthResponse {
    let (principal, started) =
        match start_admin(&state, &connection, &headers, "replication_status") {
            Ok(value) => value,
            Err(response) => return response,
        };
    let database = if database.is_empty() {
        crate::databases::DEFAULT_DATABASE.to_string()
    } else {
        database
    };
    let store = match state.databases.get(&database).await {
        Ok(runtime) => runtime.store.clone(),
        Err(error) => {
            return failure(
                axum::http::StatusCode::BAD_REQUEST,
                &error.code,
                error.message,
            )
        }
    };
    match (
        store.replication_head(),
        store.replication_applied_origins(),
    ) {
        (Ok(head), Ok(applied)) => {
            let origin = crate::writer::replication_origin();
            let ack_details = origin
                .as_deref()
                .and_then(|origin| store.replica_ack_details(origin).ok())
                .unwrap_or_default();
            let min_ack = ack_details
                .iter()
                .map(|(_, seq, _)| *seq)
                .min()
                .unwrap_or(0);
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or(0);
            let acks = ack_details
                .into_iter()
                .map(|(node, seq, at_ms)| {
                    let url = crate::replicator::replica_url(&node);
                    serde_json::json!({
                        "node": node,
                        "seq": seq,
                        "at_ms": at_ms,
                        "age_seconds": now_ms.saturating_sub(at_ms) as f64 / 1000.0,
                        "url": url,
                    })
                })
                .collect::<Vec<_>>();
            let applied = applied
                .into_iter()
                .map(|(origin, seq)| {
                    let source_head = store.replication_source_head(&origin).unwrap_or(0);
                    let epoch = store.replication_applied_epoch(&origin).unwrap_or(0);
                    serde_json::json!({
                        "origin": origin,
                        "applied": seq,
                        "source_head": source_head,
                        "lag": source_head.saturating_sub(seq),
                        "epoch": epoch,
                    })
                })
                .collect::<Vec<_>>();
            audit_control(
                &state,
                &connection,
                principal.name(),
                "replication_status",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::json!({
                "database": database,
                "enabled": origin.is_some(),
                "origin": origin,
                "role": crate::replicator::role(),
                "epoch": crate::replicator::epoch(),
                "read_only": !crate::replicator::writable(),
                "lease_valid": crate::replicator::lease_valid(),
                "successor": crate::replicator::successor(),
                "write_url": crate::replicator::write_url(state.databases.root()).unwrap_or(None),
                "head": head,
                "applied": applied,
                "acks": acks,
                "min_ack": min_ack,
                "replication": crate::replicator::status_json(),
            }))
        }
        (Err(error), _) | (_, Err(error)) => failure(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "replication_error",
            error.to_string(),
        ),
    }
}

pub async fn changes(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Path(database): Path<String>,
    Query(query): Query<ChangesQuery>,
) -> AuthResponse {
    let (principal, started) =
        match start_admin(&state, &connection, &headers, "replication_changes") {
            Ok(value) => value,
            Err(response) => return response,
        };
    let database = if database.is_empty() {
        crate::databases::DEFAULT_DATABASE.to_string()
    } else {
        database
    };
    let store = match state.databases.get(&database).await {
        Ok(runtime) => runtime.store.clone(),
        Err(error) => {
            return failure(
                axum::http::StatusCode::BAD_REQUEST,
                &error.code,
                error.message,
            )
        }
    };
    let limit = query.limit.clamp(1, 1000);
    match store.replication_after(query.after, limit) {
        Ok(changes) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "replication_changes",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::json!({
                "database": database,
                "after": query.after,
                "changes": changes,
            }))
        }
        Err(error) => failure(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "replication_error",
            error.to_string(),
        ),
    }
}

pub async fn lease(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(body): Json<LeaseBody>,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "replication_lease")
    {
        Ok(value) => value,
        Err(response) => return response,
    };
    match crate::replicator::cast_lease(
        state.databases.root(),
        body.epoch,
        &body.candidate,
        body.ttl_seconds,
    ) {
        Ok(granted) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "replication_lease",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::json!({
                "granted": granted,
                "epoch": body.epoch,
                "candidate": body.candidate,
            }))
        }
        Err(message) => failure(StatusCode::BAD_REQUEST, "invalid_lease", message),
    }
}

pub async fn primary(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
) -> AuthResponse {
    let (principal, started) =
        match start_admin(&state, &connection, &headers, "replication_primary") {
            Ok(value) => value,
            Err(response) => return response,
        };
    let write_url = crate::replicator::write_url(state.databases.root());
    match write_url {
        Ok(write_url) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "replication_primary",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::json!({
                "role": crate::replicator::role(),
                "epoch": crate::replicator::epoch(),
                "write_url": write_url,
            }))
        }
        Err(message) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "replication_error",
            message,
        ),
    }
}

pub async fn vote(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(body): Json<VoteBody>,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "replication_vote")
    {
        Ok(value) => value,
        Err(response) => return response,
    };
    match crate::replicator::cast_vote(state.databases.root(), body.epoch, &body.candidate) {
        Ok(granted) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "replication_vote",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::json!({
                "granted": granted,
                "epoch": body.epoch,
                "candidate": body.candidate,
            }))
        }
        Err(message) => failure(StatusCode::BAD_REQUEST, "invalid_vote", message),
    }
}

pub async fn update_source(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(body): Json<SourceBody>,
) -> AuthResponse {
    let (principal, started) =
        match start_admin(&state, &connection, &headers, "replication_source_update") {
            Ok(value) => value,
            Err(response) => return response,
        };
    let databases = body.databases.unwrap_or_default();
    let result = crate::replicator::set_source(
        state.databases.root(),
        &body.source,
        &databases,
        body.interval_secs.unwrap_or(1),
        body.batch.unwrap_or(500),
        body.bootstrap.as_deref().unwrap_or("auto"),
    );
    match result {
        Ok(source) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "replication_source_update",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::json!({ "source": source }))
        }
        Err(message) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "replication_source_update",
                false,
                Some(&message),
                started,
                None,
            );
            failure(StatusCode::BAD_REQUEST, "invalid_source", message)
        }
    }
}

async fn set_paused(
    state: &AppState,
    connection: &auth::ConnectionAuth,
    headers: &HeaderMap,
    op: &str,
    value: bool,
) -> AuthResponse {
    let (principal, started) = match start_admin(state, connection, headers, op) {
        Ok(value) => value,
        Err(response) => return response,
    };
    match crate::replicator::set_paused(value) {
        Ok(()) => {
            audit_control(
                state,
                connection,
                principal.name(),
                op,
                true,
                None,
                started,
                None,
            );
            ok(serde_json::json!({
                "paused": value,
                "role": crate::replicator::role(),
                "source": crate::replicator::status_json().get("source").cloned(),
            }))
        }
        Err(message) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "replication_pause",
            message,
        ),
    }
}

pub async fn pause(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
) -> AuthResponse {
    set_paused(&state, &connection, &headers, "replication_pause", true).await
}

pub async fn resume(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
) -> AuthResponse {
    set_paused(&state, &connection, &headers, "replication_resume", false).await
}

pub async fn disconnect(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
) -> AuthResponse {
    let (principal, started) =
        match start_admin(&state, &connection, &headers, "replication_disconnect") {
            Ok(value) => value,
            Err(response) => return response,
        };
    match crate::replicator::disconnect(state.databases.root()) {
        Ok(()) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "replication_disconnect",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::json!({
                "disconnected": true,
                "paused": true,
                "role": crate::replicator::role(),
                "source": null,
                "read_only": !crate::replicator::writable(),
            }))
        }
        Err(message) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "replication_disconnect",
            message,
        ),
    }
}

pub async fn promote(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(body): Json<RoleBody>,
) -> AuthResponse {
    let (principal, started) =
        match start_admin(&state, &connection, &headers, "replication_promote") {
            Ok(value) => value,
            Err(response) => return response,
        };
    match crate::replicator::promote_with_fence(
        state.databases.root(),
        body.expected_epoch,
        body.force,
    )
    .await
    {
        Ok(role) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "replication_promote",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::json!({ "role": role }))
        }
        Err((code, message)) => role_failure(
            &state,
            &connection,
            principal.name(),
            "replication_promote",
            started,
            code,
            message,
        ),
    }
}

pub async fn demote(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(body): Json<RoleBody>,
) -> AuthResponse {
    let (principal, started) =
        match start_admin(&state, &connection, &headers, "replication_demote") {
            Ok(value) => value,
            Err(response) => return response,
        };
    let role = body.role.as_deref().unwrap_or("replica");
    match crate::replicator::demote(body.expected_epoch, role, body.successor.as_deref()) {
        Ok(role) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "replication_demote",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::json!({ "role": role }))
        }
        Err((code, message)) => role_failure(
            &state,
            &connection,
            principal.name(),
            "replication_demote",
            started,
            code,
            message,
        ),
    }
}

fn role_failure(
    state: &AppState,
    connection: &auth::ConnectionAuth,
    principal: &str,
    op: &str,
    started: std::time::Instant,
    code: String,
    message: String,
) -> AuthResponse {
    let status = if code == "epoch_mismatch" {
        StatusCode::CONFLICT
    } else if code == "role_state_error" {
        StatusCode::INTERNAL_SERVER_ERROR
    } else {
        StatusCode::BAD_REQUEST
    };
    audit_control(
        state,
        connection,
        principal,
        op,
        false,
        Some(&message),
        started,
        None,
    );
    failure(status, &code, message)
}

pub async fn snapshot(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Path(database): Path<String>,
) -> Response {
    let (principal, started) =
        match start_admin(&state, &connection, &headers, "replication_snapshot") {
            Ok(value) => value,
            Err(response) => return response.into_response(),
        };
    let database = if database.is_empty() {
        crate::databases::DEFAULT_DATABASE.to_string()
    } else {
        database
    };
    let runtime = match state.databases.get(&database).await {
        Ok(runtime) => runtime,
        Err(error) => {
            return failure(StatusCode::BAD_REQUEST, &error.code, error.message).into_response()
        }
    };
    let store = runtime.store.clone();
    if let Err(error) = store.force_sync() {
        return failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "replication_error",
            format!("sync source before snapshot: {error}"),
        )
        .into_response();
    }
    let head = store.replication_head().unwrap_or(0);
    let origin = crate::writer::replication_origin().unwrap_or_default();
    let path = std::env::temp_dir().join(format!(
        "nonodb-replication-snapshot-{}-{}.mdb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0)
    ));
    let copy_path = path.clone();
    let copy = tokio::task::spawn_blocking(move || {
        store.copy_to_file(&copy_path, nonodb_core::heed::CompactionOption::Enabled)
    })
    .await;
    let copy = match copy {
        Ok(Ok(file)) => file,
        Ok(Err(error)) => {
            let _ = std::fs::remove_file(&path);
            return failure(
                StatusCode::INTERNAL_SERVER_ERROR,
                "replication_error",
                error.to_string(),
            )
            .into_response();
        }
        Err(error) => {
            let _ = std::fs::remove_file(&path);
            return failure(
                StatusCode::INTERNAL_SERVER_ERROR,
                "replication_error",
                format!("snapshot task: {error}"),
            )
            .into_response();
        }
    };
    drop(copy);
    let file = match tokio::fs::File::open(&path).await {
        Ok(file) => file,
        Err(error) => {
            let _ = tokio::fs::remove_file(&path).await;
            return failure(
                StatusCode::INTERNAL_SERVER_ERROR,
                "replication_error",
                format!("open snapshot: {error}"),
            )
            .into_response();
        }
    };
    #[cfg(unix)]
    let _ = tokio::fs::remove_file(&path).await;
    audit_control(
        &state,
        &connection,
        principal.name(),
        "replication_snapshot",
        true,
        None,
        started,
        None,
    );
    let mut response = Response::new(Body::from_stream(ReaderStream::new(file)));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_static("attachment; filename=\"data.mdb\""),
    );
    if let Ok(value) = HeaderValue::from_str(&origin) {
        response
            .headers_mut()
            .insert("x-nonodb-replication-origin", value);
    }
    if let Ok(value) = HeaderValue::from_str(&head.to_string()) {
        response
            .headers_mut()
            .insert("x-nonodb-replication-head", value);
    }
    response
}

pub async fn ack(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Path(database): Path<String>,
    Json(body): Json<AckBody>,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "replication_ack") {
        Ok(value) => value,
        Err(response) => return response,
    };
    let database = if database.is_empty() {
        crate::databases::DEFAULT_DATABASE.to_string()
    } else {
        database
    };
    let store = match state.databases.get(&database).await {
        Ok(runtime) => runtime.store.clone(),
        Err(error) => return failure(StatusCode::BAD_REQUEST, &error.code, error.message),
    };
    if crate::replicator::replica_blocked(&body.node) {
        return failure(
            StatusCode::FORBIDDEN,
            "replica_removed",
            "this replica was removed on the primary; generate a new pairing code to re-add it",
        );
    }
    if let Some(url) = body
        .advertise_url
        .as_deref()
        .map(str::trim)
        .filter(|url| !url.is_empty())
    {
        if let Err(message) = crate::replicator::record_replica_url(&body.node, url) {
            return failure(StatusCode::BAD_REQUEST, "invalid_advertise_url", message);
        }
    }
    match store.record_replica_ack(&body.origin, &body.node, body.seq) {
        Ok(()) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "replication_ack",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::json!({
                "database": database,
                "node": body.node,
                "origin": body.origin,
                "seq": body.seq,
            }))
        }
        Err(error) => failure(
            StatusCode::BAD_REQUEST,
            "replication_error",
            error.to_string(),
        ),
    }
}

pub async fn forget_ack(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Path((database, node)): Path<(String, String)>,
) -> AuthResponse {
    let (principal, started) =
        match start_admin(&state, &connection, &headers, "replication_ack_forget") {
            Ok(value) => value,
            Err(response) => return response,
        };
    let node = node.trim().to_string();
    if node.is_empty() || node.len() > 128 {
        return failure(
            StatusCode::BAD_REQUEST,
            "invalid_replica",
            "replica node id must be 1..=128 characters",
        );
    }
    let database = if database.is_empty() {
        crate::databases::DEFAULT_DATABASE.to_string()
    } else {
        database
    };
    let store = match state.databases.get(&database).await {
        Ok(runtime) => runtime.store.clone(),
        Err(error) => return failure(StatusCode::BAD_REQUEST, &error.code, error.message),
    };
    let Some(origin) = crate::writer::replication_origin() else {
        return failure(
            StatusCode::BAD_REQUEST,
            "replication_disabled",
            "this node is not a replication source",
        );
    };
    match store.remove_replica_ack(&origin, &node) {
        Ok(removed) => {
            if let Err(error) = crate::replicator::block_replica(&node) {
                tracing::warn!(node = %node, error = %error, "block removed replica failed");
            }
            if let Err(error) = crate::replicator::forget_replica_url(&node) {
                tracing::warn!(node = %node, error = %error, "forget replica URL failed");
            }
            audit_control(
                &state,
                &connection,
                principal.name(),
                "replication_ack_forget",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::json!({
                "database": database,
                "node": node,
                "removed": removed,
                "blocked": true,
            }))
        }
        Err(error) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "replication_error",
            error.to_string(),
        ),
    }
}

pub async fn forget_ack_default(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Path(node): Path<String>,
) -> AuthResponse {
    forget_ack(
        State(state),
        Extension(connection),
        headers,
        Path((crate::databases::DEFAULT_DATABASE.to_string(), node)),
    )
    .await
}

pub async fn acks(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Path(database): Path<String>,
    Query(query): Query<AcksQuery>,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "replication_acks")
    {
        Ok(value) => value,
        Err(response) => return response,
    };
    let database = if database.is_empty() {
        crate::databases::DEFAULT_DATABASE.to_string()
    } else {
        database
    };
    let store = match state.databases.get(&database).await {
        Ok(runtime) => runtime.store.clone(),
        Err(error) => return failure(StatusCode::BAD_REQUEST, &error.code, error.message),
    };
    let origin = query
        .origin
        .or_else(crate::writer::replication_origin)
        .unwrap_or_default();
    if origin.is_empty() {
        return failure(
            StatusCode::BAD_REQUEST,
            "replication_disabled",
            "replication origin is not configured",
        );
    }
    match (store.replica_ack_details(&origin), store.replication_head()) {
        (Ok(acks), Ok(head)) => {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or(0);
            let min = acks.iter().map(|(_, seq, _)| *seq).min().unwrap_or(0);
            audit_control(
                &state,
                &connection,
                principal.name(),
                "replication_acks",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::json!({
                "database": database,
                "origin": origin,
                "head": head,
                "min_ack": min,
                "acks": acks
                    .into_iter()
                    .map(|(node, seq, at_ms)| serde_json::json!({
                        "node": node,
                        "seq": seq,
                        "at_ms": at_ms,
                        "age_seconds": now_ms.saturating_sub(at_ms) as f64 / 1000.0,
                    }))
                    .collect::<Vec<_>>(),
            }))
        }
        (Err(error), _) | (_, Err(error)) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "replication_error",
            error.to_string(),
        ),
    }
}

pub async fn prune(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Path(database): Path<String>,
    Json(body): Json<PruneBody>,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "replication_prune")
    {
        Ok(value) => value,
        Err(response) => return response,
    };
    let database = if database.is_empty() {
        crate::databases::DEFAULT_DATABASE.to_string()
    } else {
        database
    };
    let store = match state.databases.get(&database).await {
        Ok(runtime) => runtime.store.clone(),
        Err(error) => return failure(StatusCode::BAD_REQUEST, &error.code, error.message),
    };
    let mut through = body.through.min(store.replication_head().unwrap_or(0));
    let mut min_ack = None;
    if body.require_acks {
        let origin = match crate::writer::replication_origin() {
            Some(origin) => origin,
            None => {
                return failure(
                    StatusCode::BAD_REQUEST,
                    "replication_disabled",
                    "replication origin is not configured",
                )
            }
        };
        let acks = match store.replica_acks(&origin) {
            Ok(acks) => acks,
            Err(error) => {
                return failure(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "replication_error",
                    error.to_string(),
                )
            }
        };
        if acks.is_empty() {
            return failure(
                StatusCode::CONFLICT,
                "no_replica_acks",
                "no replica has acknowledged this source yet",
            );
        }
        let ack = acks.iter().map(|(_, seq)| *seq).min().unwrap_or(0);
        min_ack = Some(ack);
        through = through.min(ack);
    }
    match store.prune_replication_through(through) {
        Ok(deleted) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "replication_prune",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::json!({
                "database": database,
                "through": through,
                "deleted": deleted,
                "require_acks": body.require_acks,
                "min_ack": min_ack,
            }))
        }
        Err(error) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "replication_error",
            error.to_string(),
        ),
    }
}

pub async fn ack_default(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(body): Json<AckBody>,
) -> AuthResponse {
    ack(
        State(state),
        Extension(connection),
        headers,
        Path(crate::databases::DEFAULT_DATABASE.to_string()),
        Json(body),
    )
    .await
}

pub async fn acks_default(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Query(query): Query<AcksQuery>,
) -> AuthResponse {
    acks(
        State(state),
        Extension(connection),
        headers,
        Path(crate::databases::DEFAULT_DATABASE.to_string()),
        Query(query),
    )
    .await
}

pub async fn snapshot_default(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
) -> Response {
    snapshot(
        State(state),
        Extension(connection),
        headers,
        Path(crate::databases::DEFAULT_DATABASE.to_string()),
    )
    .await
}

pub async fn prune_default(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(body): Json<PruneBody>,
) -> AuthResponse {
    prune(
        State(state),
        Extension(connection),
        headers,
        Path(crate::databases::DEFAULT_DATABASE.to_string()),
        Json(body),
    )
    .await
}

pub async fn status_default(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
) -> AuthResponse {
    status(
        State(state),
        Extension(connection),
        headers,
        Path(crate::databases::DEFAULT_DATABASE.to_string()),
    )
    .await
}

pub async fn changes_default(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Query(query): Query<ChangesQuery>,
) -> AuthResponse {
    changes(
        State(state),
        Extension(connection),
        headers,
        Path(crate::databases::DEFAULT_DATABASE.to_string()),
        Query(query),
    )
    .await
}
