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
    imports::{BrowseRequest, RunImportRequest},
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
