//! Admin-only replication status and change-log inspection.

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    Extension,
};
use serde::Deserialize;

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
    match store.replication_head() {
        Ok(head) => {
            let origin = crate::writer::replication_origin();
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
                "role": std::env::var("NONODB_REPLICATION_ROLE").unwrap_or_else(|_| "primary".into()),
                "head": head,
            }))
        }
        Err(error) => failure(
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
