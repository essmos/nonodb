//! Replica pairing API.

use std::time::{Duration, Instant};

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Extension, Json,
};
use nonodb_auth::{PrincipalDef, TokenEntry};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    audit_control, auth, auth_api::start_admin, error_status, protocol::ApiError, AppState,
};

type ApiResponse = (StatusCode, Json<Value>);

#[derive(Debug, Deserialize)]
pub struct CreatePairingBody {
    pub name: String,
    #[serde(default)]
    pub databases: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct PairBody {
    pub code: String,
    #[serde(default)]
    pub node_id: Option<String>,
    #[serde(default)]
    pub advertise_url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct JoinBody {
    pub url: String,
    pub code: String,
    #[serde(default)]
    pub node_id: Option<String>,
    #[serde(default)]
    pub advertise_url: Option<String>,
}

fn ok(result: Value) -> ApiResponse {
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "result": result })),
    )
}

fn failure(status: StatusCode, code: &str, message: impl Into<String>) -> ApiResponse {
    (
        status,
        Json(json!({ "ok": false, "error": { "code": code, "message": message.into() } })),
    )
}

fn auth_state(state: &AppState) -> Result<std::sync::Arc<auth::AuthState>, ApiResponse> {
    state.auth.clone().ok_or_else(|| {
        failure(
            StatusCode::BAD_REQUEST,
            "auth_disabled",
            "pairing requires NONODB_AUTH_FILE on this node",
        )
    })
}

pub async fn create(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(body): Json<CreatePairingBody>,
) -> ApiResponse {
    let (principal, started) =
        match start_admin(&state, &connection, &headers, "replication_pairing_create") {
            Ok(value) => value,
            Err(response) => return response,
        };
    let databases = if body.databases.is_empty() {
        default_databases()
    } else {
        body.databases
    };
    match state.pairings.create(&body.name, databases) {
        Ok(view) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "replication_pairing_create",
                true,
                None,
                started,
                None,
            );
            ok(json!({
                "code": view.code,
                "name": view.name,
                "databases": view.databases,
                "expires_at_ms": view.expires_at_ms,
                "join_command": "nonodb-cli replication join <this-primary-url>",
            }))
        }
        Err(message) => failure(StatusCode::BAD_REQUEST, "invalid_pairing", message),
    }
}

pub async fn list(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
) -> ApiResponse {
    let (principal, started) =
        match start_admin(&state, &connection, &headers, "replication_pairing_list") {
            Ok(value) => value,
            Err(response) => return response,
        };
    let pending = state.pairings.list();
    audit_control(
        &state,
        &connection,
        principal.name(),
        "replication_pairing_list",
        true,
        None,
        started,
        None,
    );
    ok(json!({ "pending": pending }))
}

pub async fn revoke(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Path(code): Path<String>,
) -> ApiResponse {
    let (principal, started) =
        match start_admin(&state, &connection, &headers, "replication_pairing_revoke") {
            Ok(value) => value,
            Err(response) => return response,
        };
    let removed = state.pairings.revoke(&code);
    audit_control(
        &state,
        &connection,
        principal.name(),
        "replication_pairing_revoke",
        removed,
        None,
        started,
        None,
    );
    ok(json!({ "revoked": removed }))
}

/// Public endpoint protected only by the short-lived, single-use code.
pub async fn pair(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(body): Json<PairBody>,
) -> ApiResponse {
    let started = Instant::now();
    let peer = connection
        .peer
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    let pending = match state.pairings.redeem(&body.code, &peer) {
        Ok(pending) => pending,
        Err(message) => {
            tracing::warn!(peer = %peer, "replica pairing rejected");
            return failure(StatusCode::UNAUTHORIZED, "invalid_pairing_code", message);
        }
    };
    if let Some(node_id) = body.node_id.as_deref() {
        if node_id.is_empty()
            || node_id.len() > 64
            || !node_id
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
        {
            return failure(
                StatusCode::BAD_REQUEST,
                "invalid_node_id",
                "node_id must be 1..=64 alphanumeric, '-', '_' or '.' characters",
            );
        }
    }
    if let Some(node_id) = body.node_id.as_deref() {
        if let Err(error) = crate::replicator::unblock_replica(node_id) {
            tracing::warn!(node_id = %node_id, error = %error, "unblock replica failed");
        }
    }
    let auth = match auth_state(&state) {
        Ok(auth) => auth,
        Err(response) => return response,
    };
    let principal_name = format!("replica-{}", pending.name());
    let token_result = auth.mutate(|file| {
        file.principals
            .entry(principal_name.clone())
            .or_insert_with(|| PrincipalDef {
                scopes: vec!["admin:db".into()],
            });
        let (kid, token, hash) = nonodb_auth::generate_token(None)?;
        file.tokens.push(TokenEntry {
            kid: kid.clone(),
            sha256: hash,
            principal: principal_name.clone(),
            disabled: false,
        });
        Ok((kid, token))
    });
    let (kid, token) = match token_result {
        Ok(value) => value,
        Err(message) => {
            tracing::error!(error = %message, "pairing credential creation failed");
            return failure(
                StatusCode::INTERNAL_SERVER_ERROR,
                "pairing_credential_failed",
                message,
            );
        }
    };
    let source = advertise_url(&state, &headers);
    let databases = if pending.databases().is_empty() {
        default_databases()
    } else {
        pending.databases().to_vec()
    };
    audit_control(
        &state,
        &connection,
        &principal_name,
        "replication_pair",
        true,
        None,
        started,
        None,
    );
    tracing::info!(
        peer = %peer,
        node_id = body.node_id.as_deref().unwrap_or(""),
        principal = %principal_name,
        "replica paired"
    );
    ok(json!({
        "token": token,
        "kid": kid,
        "principal": principal_name,
        "source": source,
        "databases": databases,
        "interval_secs": env_u64("NONODB_REPLICATION_POLL_INTERVAL", 1),
        "batch": env_u64("NONODB_REPLICATION_BATCH", 500),
        "bootstrap": std::env::var("NONODB_REPLICATION_BOOTSTRAP").unwrap_or_else(|_| "auto".into()),
        "advertise_url": body.advertise_url,
    }))
}

/// Local-only endpoint used by `nonodb-cli replication join`.
pub async fn join(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(body): Json<JoinBody>,
) -> ApiResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "replication_join")
    {
        Ok(value) => value,
        Err(response) => return response,
    };
    match crate::replicator::join_remote(
        state.databases.root(),
        &body.url,
        &body.code,
        body.node_id.as_deref(),
        body.advertise_url.as_deref(),
    )
    .await
    {
        Ok(mut result) => {
            let restart = std::env::var("NONODB_REPLICATION_JOIN_RESTART")
                .ok()
                .is_none_or(|value| !matches!(value.trim(), "0" | "false" | "no" | "off"));
            if restart {
                if let Some(object) = result.as_object_mut() {
                    object.insert("restart_requested".into(), Value::Bool(true));
                }
                let shutdown = state.shutdown_tx.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    crate::replicator::request_restart();
                    let _ = shutdown.send(true);
                });
            }
            audit_control(
                &state,
                &connection,
                principal.name(),
                "replication_join",
                true,
                None,
                started,
                None,
            );
            ok(result)
        }
        Err(message) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "replication_join",
                false,
                Some(&message),
                started,
                None,
            );
            let api = ApiError {
                code: "pairing_failed".into(),
                message,
            };
            failure(error_status(&api), &api.code, api.message)
        }
    }
}

fn default_databases() -> Vec<String> {
    std::env::var("NONODB_REPLICATION_DATABASES")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|items| !items.is_empty())
        .unwrap_or_else(|| vec![crate::databases::DEFAULT_DATABASE.to_string()])
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn advertise_url(state: &AppState, headers: &HeaderMap) -> String {
    if let Ok(value) = std::env::var("NONODB_REPLICATION_ADVERTISE_URL") {
        if !value.trim().is_empty() {
            return value.trim().trim_end_matches('/').to_string();
        }
    }
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("127.0.0.1:7878");
    let scheme = if std::env::var_os("NONODB_TLS_CERT").is_some() {
        "https"
    } else {
        "http"
    };
    let _ = state;
    format!("{scheme}://{host}")
}
