//! Admin-only HTTP API for scheduled backup jobs.

use std::io::Write;
use std::path::{Path as StdPath, PathBuf};

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Extension, Json,
};
use serde::Deserialize;

use crate::{
    audit_control,
    auth::{self},
    auth_api::{failure, mutation_failure, ok, start_admin, AuthResponse},
    backups::BackupJob,
    AppState,
};

#[derive(Debug, Deserialize)]
pub struct KeygenRequest {
    pub identity_file: String,
    pub recipient_file: String,
}

#[derive(Debug, Deserialize)]
pub struct SecretRequest {
    pub path: String,
}

pub async fn list(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "backups_list") {
        Ok(value) => value,
        Err(response) => return response,
    };
    audit_control(
        &state,
        &connection,
        principal.name(),
        "backups_list",
        true,
        None,
        started,
        None,
    );
    ok(state.backups.view())
}

pub async fn upsert(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(job): Json<BackupJob>,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "backups_upsert") {
        Ok(value) => value,
        Err(response) => return response,
    };
    let result = state.backups.upsert(job);
    match result {
        Ok(value) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "backups_upsert",
                true,
                None,
                started,
                None,
            );
            ok(value)
        }
        Err(error) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "backups_upsert",
                false,
                Some(&error),
                started,
                None,
            );
            mutation_failure(error)
        }
    }
}

pub async fn delete(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "backups_delete") {
        Ok(value) => value,
        Err(response) => return response,
    };
    match state.backups.delete(&id) {
        Ok(()) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "backups_delete",
                true,
                None,
                started,
                None,
            );
            ok(serde_json::json!({ "id": id, "deleted": true }))
        }
        Err(error) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "backups_delete",
                false,
                Some(&error),
                started,
                None,
            );
            mutation_failure(error)
        }
    }
}

pub async fn run(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "backups_run") {
        Ok(value) => value,
        Err(response) => return response,
    };
    match state.backups.run_now(&id) {
        Ok(value) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "backups_run",
                true,
                None,
                started,
                None,
            );
            ok(value)
        }
        Err(error) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "backups_run",
                false,
                Some(&error),
                started,
                None,
            );
            failure(axum::http::StatusCode::BAD_REQUEST, "backup_error", error)
        }
    }
}

fn ensure_parent(path: &StdPath) -> Result<(), String> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() || parent.exists() {
        return Ok(());
    }
    std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
    }
    Ok(())
}

fn write_new(path: &StdPath, contents: &[u8], mode: u32) -> Result<(), String> {
    ensure_parent(path)?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(mode);
    }
    let mut file = options
        .open(path)
        .map_err(|e| format!("create {}: {e}", path.display()))?;
    file.write_all(contents)
        .map_err(|e| format!("write {}: {e}", path.display()))?;
    file.sync_all()
        .map_err(|e| format!("sync {}: {e}", path.display()))
}

fn generate_age_key(identity_file: PathBuf, recipient_file: PathBuf) -> Result<String, String> {
    if identity_file == recipient_file {
        return Err("identity and recipient files must differ".into());
    }
    let (identity, recipient) = nonodb_core::backup_crypto::generate_keypair();
    write_new(&identity_file, format!("{identity}\n").as_bytes(), 0o600)?;
    if let Err(error) = write_new(&recipient_file, format!("{recipient}\n").as_bytes(), 0o644) {
        let _ = std::fs::remove_file(&identity_file);
        return Err(error);
    }
    Ok(recipient)
}

fn generate_restic_password(path: PathBuf) -> Result<(), String> {
    use ring::rand::{SecureRandom, SystemRandom};
    let mut bytes = [0u8; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| "secure random generator failed".to_string())?;
    let mut password = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(password, "{byte:02x}");
    }
    write_new(&path, format!("{password}\n").as_bytes(), 0o600)
}

pub async fn keygen(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(request): Json<KeygenRequest>,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "backups_keygen") {
        Ok(value) => value,
        Err(response) => return response,
    };
    let identity_file = PathBuf::from(request.identity_file);
    let recipient_file = PathBuf::from(request.recipient_file);
    let result = tokio::task::spawn_blocking(move || {
        let recipient = generate_age_key(identity_file.clone(), recipient_file.clone())?;
        Ok::<_, String>(serde_json::json!({
            "identity_file": identity_file,
            "recipient_file": recipient_file,
            "recipient": recipient,
        }))
    })
    .await
    .map_err(|e| format!("keygen task: {e}"))
    .and_then(|inner| inner);
    match result {
        Ok(value) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "backups_keygen",
                true,
                None,
                started,
                None,
            );
            ok(value)
        }
        Err(error) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "backups_keygen",
                false,
                Some(&error),
                started,
                None,
            );
            failure(StatusCode::BAD_REQUEST, "backup_error", error)
        }
    }
}

pub async fn secret(
    State(state): State<AppState>,
    Extension(connection): Extension<auth::ConnectionAuth>,
    headers: HeaderMap,
    Json(request): Json<SecretRequest>,
) -> AuthResponse {
    let (principal, started) = match start_admin(&state, &connection, &headers, "backups_secret") {
        Ok(value) => value,
        Err(response) => return response,
    };
    let path = PathBuf::from(request.path);
    let result = tokio::task::spawn_blocking(move || {
        generate_restic_password(path.clone())?;
        Ok::<_, String>(serde_json::json!({ "path": path }))
    })
    .await
    .map_err(|e| format!("secret task: {e}"))
    .and_then(|inner| inner);
    match result {
        Ok(value) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "backups_secret",
                true,
                None,
                started,
                None,
            );
            ok(value)
        }
        Err(error) => {
            audit_control(
                &state,
                &connection,
                principal.name(),
                "backups_secret",
                false,
                Some(&error),
                started,
                None,
            );
            failure(StatusCode::BAD_REQUEST, "backup_error", error)
        }
    }
}
