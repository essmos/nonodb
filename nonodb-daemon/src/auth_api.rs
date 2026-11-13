//! HTTP API for the file-based authorization registry.
//!
//! All routes require an instance-wide `admin:db` principal. Mutations are
//! serialized by the auth-file lock, reloaded immediately, and reject changes
//! that would remove the last enabled administrator credential. Plaintext
//! tokens are returned only from the create-token response and are never
//! logged or stored by the daemon.

use std::time::Instant;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Extension, Json,
};
use nonodb_auth::{PrincipalDef, TokenEntry};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    audit_control, auth, authenticate_with_guard, error_status, protocol::ApiError, AppState,
};

pub(crate) type AuthResponse = (StatusCode, Json<Value>);

#[derive(Debug, Deserialize)]
pub struct PrincipalBody {
    pub name: String,
    pub scopes: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ScopesBody {
    pub scopes: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct TokenBody {
    pub principal: String,
    #[serde(default)]
    pub kid: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CertificateBody {
    pub sha256: String,
    pub principal: String,
}

#[derive(Debug, Deserialize)]
pub struct ToggleBody {
    pub disabled: bool,
}

pub(crate) fn ok(result: Value) -> AuthResponse {
    (
        StatusCode::OK,
        Json(json!({ "ok": true, "result": result })),
    )
}

pub(crate) fn failure(status: StatusCode, code: &str, message: impl Into<String>) -> AuthResponse {
    (
        status,
        Json(json!({
            "ok": false,
            "error": { "code": code, "message": message.into() }
        })),
    )
}

pub(crate) fn mutation_failure(message: String) -> AuthResponse {
    let status = if message.contains("last enabled administrator") {
        StatusCode::CONFLICT
    } else {
        StatusCode::BAD_REQUEST
    };
    failure(status, "invalid_auth_request", message)
}

fn auth_state(state: &AppState) -> Result<std::sync::Arc<auth::AuthState>, AuthResponse> {
    state.auth.clone().ok_or_else(|| {
        failure(
            StatusCode::BAD_REQUEST,
            "auth_disabled",
            "authorization is not enabled; start the daemon with NONODB_AUTH_FILE",
        )
    })
}

pub(crate) fn start_admin(
    state: &AppState,
    connection: &auth::ConnectionAuth,
    headers: &HeaderMap,
    op: &str,
) -> Result<(nonodb_auth::Principal, Instant), AuthResponse> {
    let started = Instant::now();
    let principal = match authenticate_with_guard(state, connection, headers) {
        Ok(principal) => principal,
        Err((err, auth_audit)) => {
            audit_control(
                state,
                connection,
                "anonymous",
                op,
                false,
                Some(&err.message),
                started,
                auth_audit,
            );
            return Err(failure(error_status(&err), &err.code, err.message));
        }
    };
    if !principal.allows_admin_database() {
        let err = ApiError::forbidden();
        audit_control(
            state,
            connection,
            principal.name(),
            op,
            false,
            Some(&err.message),
            started,
            None,
        );
        return Err(failure(StatusCode::FORBIDDEN, &err.code, err.message));
    }
    Ok((principal, started))
}

fn finish<T>(
    state: &AppState,
    connection: &auth::ConnectionAuth,
    principal: &nonodb_auth::Principal,
    op: &str,
    started: Instant,
    result: Result<T, String>,
    map: impl FnOnce(T) -> Value,
) -> AuthResponse {
    match result {
        Ok(value) => {
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
            ok(map(value))
        }
        Err(err) => {
            audit_control(
                state,
                connection,
                principal.name(),
                op,
                false,
                Some(&err),
                started,
                None,
            );
            mutation_failure(err)
        }
    }
}

pub async fn summary(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "auth_summary") {
        Ok(value) => value,
        Err(response) => return response,
    };
    let auth = match auth_state(&state) {
        Ok(auth) => auth,
        Err(response) => return response,
    };
    let registry = auth.registry();
    let file = registry.file();
    let principals: Vec<Value> = file
        .principals
        .iter()
        .map(|(name, def)| {
            json!({
                "name": name,
                "scopes": def.scopes,
                "tokens": file.tokens.iter().filter(|token| token.principal == *name).count(),
                "certificates": file.certificates.iter().filter(|cert| cert.principal == *name).count(),
            })
        })
        .collect();
    let tokens: Vec<Value> = file
        .tokens
        .iter()
        .map(|token| {
            json!({
                "kid": token.kid,
                "principal": token.principal,
                "disabled": token.disabled,
            })
        })
        .collect();
    let certificates: Vec<Value> = file
        .certificates
        .iter()
        .map(|cert| {
            json!({
                "sha256": cert.sha256,
                "principal": cert.principal,
                "disabled": cert.disabled,
            })
        })
        .collect();
    audit_control(
        &state,
        &connection,
        principal.name(),
        "auth_summary",
        true,
        None,
        started,
        None,
    );
    ok(json!({
        "generation": auth.generation(),
        "principals": principals,
        "tokens": tokens,
        "certificates": certificates,
    }))
}

pub async fn create_principal(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(body): Json<PrincipalBody>,
) -> AuthResponse {
    let (principal, started) =
        match start_admin(&state, &connection, &headers, "auth_principal_create") {
            Ok(value) => value,
            Err(response) => return response,
        };
    let auth = match auth_state(&state) {
        Ok(auth) => auth,
        Err(response) => return response,
    };
    let name = body.name;
    let scopes = body.scopes;
    let result = auth.mutate(|file| {
        if file.principals.contains_key(&name) {
            return Err(format!("principal `{name}` already exists"));
        }
        nonodb_auth::Principal::new(name.clone(), &scopes)?;
        file.principals.insert(
            name.clone(),
            PrincipalDef {
                scopes: scopes.clone(),
            },
        );
        Ok(json!({ "name": name, "scopes": scopes }))
    });
    finish(
        &state,
        &connection,
        &principal,
        "auth_principal_create",
        started,
        result,
        |value| value,
    )
}

pub async fn update_principal(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<ScopesBody>,
) -> AuthResponse {
    let (principal, started) =
        match start_admin(&state, &connection, &headers, "auth_principal_update") {
            Ok(value) => value,
            Err(response) => return response,
        };
    let auth = match auth_state(&state) {
        Ok(auth) => auth,
        Err(response) => return response,
    };
    let scopes = body.scopes;
    let result = auth.mutate(|file| {
        let def = file
            .principals
            .get_mut(&name)
            .ok_or_else(|| format!("unknown principal `{name}`"))?;
        nonodb_auth::Principal::new(name.clone(), &scopes)?;
        def.scopes = scopes.clone();
        Ok(json!({ "name": name, "scopes": scopes }))
    });
    finish(
        &state,
        &connection,
        &principal,
        "auth_principal_update",
        started,
        result,
        |value| value,
    )
}

pub async fn delete_principal(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> AuthResponse {
    let (principal, started) =
        match start_admin(&state, &connection, &headers, "auth_principal_delete") {
            Ok(value) => value,
            Err(response) => return response,
        };
    let auth = match auth_state(&state) {
        Ok(auth) => auth,
        Err(response) => return response,
    };
    let result = auth.mutate(|file| {
        if !file.principals.contains_key(&name) {
            return Err(format!("unknown principal `{name}`"));
        }
        if file.tokens.iter().any(|token| token.principal == name) {
            return Err(format!(
                "principal `{name}` still has tokens; disable or remove them first"
            ));
        }
        if file.certificates.iter().any(|cert| cert.principal == name) {
            return Err(format!(
                "principal `{name}` still has certificates; disable or remove them first"
            ));
        }
        file.principals.remove(&name);
        Ok(json!({ "name": name, "deleted": true }))
    });
    finish(
        &state,
        &connection,
        &principal,
        "auth_principal_delete",
        started,
        result,
        |value| value,
    )
}

pub async fn create_token(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(body): Json<TokenBody>,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "auth_token_create")
    {
        Ok(value) => value,
        Err(response) => return response,
    };
    let auth = match auth_state(&state) {
        Ok(auth) => auth,
        Err(response) => return response,
    };
    let token_principal = body.principal;
    let requested_kid = body.kid;
    let result = auth.mutate(|file| {
        if !file.principals.contains_key(&token_principal) {
            return Err(format!("unknown principal `{token_principal}`"));
        }
        let (kid, token, hash) = nonodb_auth::generate_token(requested_kid.as_deref())?;
        if file.tokens.iter().any(|entry| entry.kid == kid) {
            return Err(format!("token kid `{kid}` already exists"));
        }
        file.tokens.push(TokenEntry {
            kid: kid.clone(),
            sha256: hash,
            principal: token_principal.clone(),
            disabled: false,
        });
        Ok(json!({
            "kid": kid,
            "principal": token_principal,
            "token": token,
        }))
    });
    finish(
        &state,
        &connection,
        &principal,
        "auth_token_create",
        started,
        result,
        |value| value,
    )
}

pub async fn update_token(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Path(kid): Path<String>,
    Json(body): Json<ToggleBody>,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "auth_token_update")
    {
        Ok(value) => value,
        Err(response) => return response,
    };
    let auth = match auth_state(&state) {
        Ok(auth) => auth,
        Err(response) => return response,
    };
    let disabled = body.disabled;
    let result = auth.mutate(|file| {
        let entry = file
            .tokens
            .iter_mut()
            .find(|entry| entry.kid == kid)
            .ok_or_else(|| format!("unknown token kid `{kid}`"))?;
        entry.disabled = disabled;
        Ok(json!({ "kid": kid, "disabled": disabled }))
    });
    finish(
        &state,
        &connection,
        &principal,
        "auth_token_update",
        started,
        result,
        |value| value,
    )
}

pub async fn create_certificate(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(body): Json<CertificateBody>,
) -> AuthResponse {
    let (principal, started) =
        match start_admin(&state, &connection, &headers, "auth_certificate_create") {
            Ok(value) => value,
            Err(response) => return response,
        };
    let auth = match auth_state(&state) {
        Ok(auth) => auth,
        Err(response) => return response,
    };
    let fingerprint = body.sha256;
    let cert_principal = body.principal;
    let result = auth.mutate(|file| {
        let fingerprint = nonodb_auth::normalize_fingerprint(&fingerprint)?;
        if !file.principals.contains_key(&cert_principal) {
            return Err(format!("unknown principal `{cert_principal}`"));
        }
        if file
            .certificates
            .iter()
            .any(|entry| entry.sha256 == fingerprint)
        {
            return Err(format!("certificate `{fingerprint}` is already registered"));
        }
        file.certificates.push(nonodb_auth::CertificateEntry {
            sha256: fingerprint.clone(),
            principal: cert_principal.clone(),
            disabled: false,
        });
        Ok(json!({
            "sha256": fingerprint,
            "principal": cert_principal,
        }))
    });
    finish(
        &state,
        &connection,
        &principal,
        "auth_certificate_create",
        started,
        result,
        |value| value,
    )
}

pub async fn update_certificate(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Path(fingerprint): Path<String>,
    Json(body): Json<ToggleBody>,
) -> AuthResponse {
    let (principal, started) =
        match start_admin(&state, &connection, &headers, "auth_certificate_update") {
            Ok(value) => value,
            Err(response) => return response,
        };
    let auth = match auth_state(&state) {
        Ok(auth) => auth,
        Err(response) => return response,
    };
    let disabled = body.disabled;
    let result = auth.mutate(|file| {
        let fingerprint = nonodb_auth::normalize_fingerprint(&fingerprint)?;
        let entry = file
            .certificates
            .iter_mut()
            .find(|entry| entry.sha256 == fingerprint)
            .ok_or_else(|| format!("unknown certificate `{fingerprint}`"))?;
        entry.disabled = disabled;
        Ok(json!({ "sha256": fingerprint, "disabled": disabled }))
    });
    finish(
        &state,
        &connection,
        &principal,
        "auth_certificate_update",
        started,
        result,
        |value| value,
    )
}
