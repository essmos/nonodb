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
}

#[derive(Debug, Deserialize)]
pub struct RoleBody {
    pub expected_epoch: u64,
    #[serde(default)]
    pub role: Option<String>,
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
            let applied = applied
                .into_iter()
                .map(|(origin, seq)| {
                    let source_head = store.replication_source_head(&origin).unwrap_or(0);
                    serde_json::json!({
                        "origin": origin,
                        "applied": seq,
                        "source_head": source_head,
                        "lag": source_head.saturating_sub(seq),
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
                "head": head,
                "applied": applied,
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
    match crate::replicator::promote(body.expected_epoch) {
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
    match crate::replicator::demote(body.expected_epoch, role) {
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
    let through = body.through.min(store.replication_head().unwrap_or(0));
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
            }))
        }
        Err(error) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "replication_error",
            error.to_string(),
        ),
    }
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
