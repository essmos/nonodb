//! Network/firewall API for the GUI Network tab.

use std::time::Instant;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Extension, Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    audit_control, auth, auth_api::start_admin, error_status, network, protocol::ApiError, AppState,
};

type ApiResponse = (StatusCode, Json<Value>);

#[derive(Debug, Deserialize)]
pub struct FirewallBody {
    #[serde(default)]
    pub allowed: Vec<String>,
    #[serde(default)]
    pub ports: Vec<u16>,
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

fn admin(
    state: &AppState,
    connection: &auth::ConnectionAuth,
    headers: &HeaderMap,
    op: &str,
) -> Result<(nonodb_auth::Principal, Instant), ApiResponse> {
    start_admin(state, connection, headers, op)
}

pub async fn view(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
) -> ApiResponse {
    let (principal, started) = match admin(&state, &connection, &headers, "network_view") {
        Ok(value) => value,
        Err(response) => return response,
    };
    audit_control(
        &state,
        &connection,
        principal.name(),
        "network_view",
        true,
        None,
        started,
        None,
    );
    ok(serde_json::to_value(network::view()).unwrap_or(Value::Null))
}

pub async fn save_firewall(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(body): Json<FirewallBody>,
) -> ApiResponse {
    let (principal, started) = match admin(&state, &connection, &headers, "network_firewall_save") {
        Ok(value) => value,
        Err(response) => return response,
    };
    let config = match network::validate(network::FirewallConfig {
        allowed: body.allowed,
        ports: body.ports,
    }) {
        Ok(config) => config,
        Err(message) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "network_firewall_save",
                false,
                Some(&message),
                started,
                None,
            );
            return failure(StatusCode::BAD_REQUEST, "invalid_firewall", message);
        }
    };
    if let Err(message) = network::save(&config) {
        audit_control(
            &state,
            &connection,
            principal.name(),
            "network_firewall_save",
            false,
            Some(&message),
            started,
            None,
        );
        return failure(StatusCode::INTERNAL_SERVER_ERROR, "firewall_state", message);
    }
    audit_control(
        &state,
        &connection,
        principal.name(),
        "network_firewall_save",
        true,
        None,
        started,
        None,
    );
    ok(serde_json::to_value(network::view()).unwrap_or(Value::Null))
}

pub async fn apply_firewall(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
) -> ApiResponse {
    let (principal, started) = match admin(&state, &connection, &headers, "network_firewall_apply")
    {
        Ok(value) => value,
        Err(response) => return response,
    };
    let config = network::load();
    let config = network::FirewallConfig {
        allowed: config.allowed,
        ports: if config.ports.is_empty() {
            network::default_ports()
        } else {
            config.ports
        },
    };
    let result = tokio::task::spawn_blocking(move || network::apply(&config))
        .await
        .map_err(|error| format!("firewall task failed: {error}"))
        .and_then(|result| result);
    match result {
        Ok(()) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "network_firewall_apply",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::to_value(network::view()).unwrap_or(Value::Null))
        }
        Err(message) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "network_firewall_apply",
                false,
                Some(&message),
                started,
                None,
            );
            let api = ApiError {
                code: "firewall_apply_failed".into(),
                message,
            };
            failure(error_status(&api), &api.code, api.message)
        }
    }
}

pub async fn disable_firewall(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
) -> ApiResponse {
    let (principal, started) =
        match admin(&state, &connection, &headers, "network_firewall_disable") {
            Ok(value) => value,
            Err(response) => return response,
        };
    let result = tokio::task::spawn_blocking(network::disable)
        .await
        .map_err(|error| format!("firewall task failed: {error}"))
        .and_then(|result| result);
    match result {
        Ok(()) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "network_firewall_disable",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::to_value(network::view()).unwrap_or(Value::Null))
        }
        Err(message) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "network_firewall_disable",
                false,
                Some(&message),
                started,
                None,
            );
            let api = ApiError {
                code: "firewall_disable_failed".into(),
                message,
            };
            failure(error_status(&api), &api.code, api.message)
        }
    }
}
