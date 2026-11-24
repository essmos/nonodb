//! Admin-only HTTP API for staged backup import.

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Extension, Json,
};

use crate::{
    audit_control,
    auth::{self},
    auth_api::{failure, ok, start_admin, AuthResponse},
    imports::{BrowseRequest, ImportSource, RestoreRequest, RunImportRequest},
    AppState,
};

pub async fn view(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "imports_view") {
        Ok(value) => value,
        Err(response) => return response,
    };
    audit_control(
        &state,
        &connection,
        principal.name(),
        "imports_view",
        true,
        None,
        started,
        None,
    );
    ok(state.imports.view())
}

pub async fn browse(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(request): Json<BrowseRequest>,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "imports_browse") {
        Ok(value) => value,
        Err(response) => return response,
    };
    match state.imports.browse(request.source).await {
        Ok(backups) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "imports_browse",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::json!({ "backups": backups }))
        }
        Err(error) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "imports_browse",
                false,
                Some(&error),
                started,
                None,
            );
            failure(StatusCode::BAD_REQUEST, "import_error", error)
        }
    }
}

pub async fn run(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(request): Json<RunImportRequest>,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "imports_run") {
        Ok(value) => value,
        Err(response) => return response,
    };
    match state.imports.start(request) {
        Ok(status) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "imports_run",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::json!(status))
        }
        Err(error) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "imports_run",
                false,
                Some(&error),
                started,
                None,
            );
            failure(StatusCode::BAD_REQUEST, "import_error", error)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn audit_restore(
    state: &AppState,
    connection: &auth::ConnectionAuth,
    principal: &str,
    op: &str,
    ok: bool,
    error: Option<&str>,
    started: std::time::Instant,
    details: serde_json::Value,
) {
    if let Some(audit) = &state.audit {
        audit.record_detail(
            principal,
            connection.source.as_str(),
            connection.peer.as_deref(),
            op,
            &[],
            ok,
            error,
            started.elapsed().as_secs_f64() * 1000.0,
            None,
            Some(&details),
        );
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct RestorePreflightRequest {
    pub source: ImportSource,
    pub location: String,
    pub database: String,
}

pub async fn restore_preflight(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(request): Json<RestorePreflightRequest>,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "restore_preflight")
    {
        Ok(value) => value,
        Err(response) => return response,
    };
    let details = serde_json::json!({
        "database": request.database,
        "location": request.location,
    });
    match state
        .imports
        .preflight_restore(request.source, request.location, request.database)
        .await
    {
        Ok(value) => {
            audit_restore(
                &state,
                &connection,
                principal.name(),
                "restore_preflight",
                true,
                None,
                started,
                details,
            );
            ok(value)
        }
        Err(error) => {
            audit_restore(
                &state,
                &connection,
                principal.name(),
                "restore_preflight",
                false,
                Some(&error),
                started,
                details,
            );
            failure(StatusCode::BAD_REQUEST, "restore_error", error)
        }
    }
}

pub async fn restore(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(request): Json<RestoreRequest>,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "restore_in_place")
    {
        Ok(value) => value,
        Err(response) => return response,
    };
    let details = serde_json::json!({
        "database": request.database,
        "location": request.location,
    });
    match state.imports.restore_in_place(request).await {
        Ok(value) => {
            audit_restore(
                &state,
                &connection,
                principal.name(),
                "restore_in_place",
                true,
                None,
                started,
                details,
            );
            ok(value)
        }
        Err(error) => {
            audit_restore(
                &state,
                &connection,
                principal.name(),
                "restore_in_place",
                false,
                Some(&error),
                started,
                details,
            );
            failure(StatusCode::BAD_REQUEST, "restore_error", error)
        }
    }
}

pub async fn status(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "imports_status") {
        Ok(value) => value,
        Err(response) => return response,
    };
    match state.imports.status(&id) {
        Some(status) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "imports_status",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::json!(status))
        }
        None => failure(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("unknown import job `{id}`"),
        ),
    }
}
