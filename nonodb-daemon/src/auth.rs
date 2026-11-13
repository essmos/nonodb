//! Runtime authentication and auth-file reload.

use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

use axum::http::{header::AUTHORIZATION, HeaderMap};
use nonodb_auth::{AuthFile, Principal, Registry};

use crate::login_guard::{BlockInfo, FailureInfo, LoginGuard};
use crate::protocol::ApiError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    Unix,
    Tcp,
    TcpTls,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unix => "unix",
            Self::Tcp => "tcp",
            Self::TcpTls => "tcp-tls",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ConnectionAuth {
    pub local_admin: bool,
    pub cert_fingerprint: Option<[u8; 32]>,
    pub source: Source,
    pub peer: Option<String>,
}

impl ConnectionAuth {
    pub fn unix() -> Self {
        Self {
            local_admin: true,
            cert_fingerprint: None,
            source: Source::Unix,
            peer: None,
        }
    }

    pub fn tcp(cert_fingerprint: Option<[u8; 32]>, peer: Option<String>, tls: bool) -> Self {
        Self {
            local_admin: false,
            cert_fingerprint,
            source: if tls { Source::TcpTls } else { Source::Tcp },
            peer,
        }
    }

    /// Socket peer IP, used for failed-login lockout. Proxy headers are
    /// intentionally ignored; only the actual TCP peer is trusted.
    pub fn peer_ip(&self) -> Option<IpAddr> {
        self.peer
            .as_deref()
            .and_then(|peer| peer.parse::<SocketAddr>().ok())
            .map(|peer| peer.ip())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FileStamp {
    modified: Option<SystemTime>,
    len: u64,
}

pub struct AuthState {
    path: PathBuf,
    registry: RwLock<Arc<Registry>>,
    stamp: Mutex<Option<FileStamp>>,
    generation: AtomicU64,
    guard: LoginGuard,
}

impl AuthState {
    pub fn open(path: PathBuf) -> Result<Arc<Self>, String> {
        check_permissions(&path);
        let stamp = file_stamp(&path)?;
        let registry = Registry::load(&path)?;
        let guard = LoginGuard::from_env()?;
        tracing::info!(enabled = guard.enabled(), "failed-login lockout configured");
        tracing::info!(
            path = %path.display(),
            principals = registry.principals().count(),
            tokens = registry.file().tokens.len(),
            certificates = registry.file().certificates.len(),
            "auth registry loaded"
        );
        Ok(Arc::new(Self {
            path,
            registry: RwLock::new(Arc::new(registry)),
            stamp: Mutex::new(Some(stamp)),
            generation: AtomicU64::new(0),
            guard,
        }))
    }

    pub fn registry(&self) -> Arc<Registry> {
        self.registry.read().expect("auth registry lock").clone()
    }

    /// Monotonically increasing registry version. Cache keys include it so a
    /// scope or credential change cannot be served from an older authorization
    /// state.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn block_info(
        &self,
        connection: &ConnectionAuth,
        now: std::time::Instant,
    ) -> Option<BlockInfo> {
        if connection.local_admin {
            return None;
        }
        self.guard.block_info(connection.peer_ip()?, now)
    }

    pub fn refresh_blocked_clients(&self) {
        self.guard.blocked_count();
    }

    pub fn record_auth_failure(
        &self,
        connection: &ConnectionAuth,
        now: std::time::Instant,
    ) -> Option<FailureInfo> {
        if connection.local_admin {
            return None;
        }
        Some(self.guard.record_failure(connection.peer_ip()?, now))
    }

    /// Reloads the file and swaps the registry only when parsing/validation succeeds.
    pub fn reload(&self) -> bool {
        match Registry::load(&self.path) {
            Ok(registry) => {
                let principals = registry.principals().count();
                *self.registry.write().expect("auth registry lock") = Arc::new(registry);
                let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
                if let Ok(stamp) = file_stamp(&self.path) {
                    *self.stamp.lock().expect("auth stamp lock") = Some(stamp);
                }
                tracing::info!(
                    path = %self.path.display(),
                    principals,
                    generation,
                    "auth registry reloaded"
                );
                true
            }
            Err(err) => {
                tracing::error!(
                    path = %self.path.display(),
                    error = %err,
                    "auth registry reload failed; keeping last known good configuration"
                );
                false
            }
        }
    }

    /// Atomically mutates the registry file, reloads it, and refuses changes
    /// that would leave no enabled administrator credential.
    pub fn mutate<T, F>(&self, mutate: F) -> Result<T, String>
    where
        F: FnOnce(&mut AuthFile) -> Result<T, String>,
    {
        let output = nonodb_auth::update_atomic(&self.path, |file| {
            let output = mutate(file)?;
            if !has_enabled_admin(file) {
                return Err(
                    "operation would remove the last enabled administrator credential".into(),
                );
            }
            Ok(output)
        })?;
        if !self.reload() {
            return Err("auth file updated but could not be reloaded".into());
        }
        Ok(output)
    }

    fn reload_if_changed(&self) {
        let Ok(stamp) = file_stamp(&self.path) else {
            return;
        };
        let mut current = self.stamp.lock().expect("auth stamp lock");
        if current.as_ref() == Some(&stamp) {
            return;
        }
        *current = Some(stamp);
        drop(current);
        self.reload();
    }

    pub fn spawn_watcher(self: &Arc<Self>) {
        let state = self.clone();
        std::thread::Builder::new()
            .name("nonodb-auth-watch".into())
            .spawn(move || loop {
                std::thread::sleep(Duration::from_secs(1));
                state.reload_if_changed();
            })
            .expect("failed to start auth watcher");
    }
}

pub fn authenticate(
    state: Option<&AuthState>,
    connection: &ConnectionAuth,
    headers: &HeaderMap,
) -> Result<Principal, ApiError> {
    if connection.local_admin {
        return Ok(Principal::local_admin());
    }

    // Re-resolve the certificate on every request so revocation, disabling, and
    // scope changes apply to existing keep-alive connections.
    let cert_principal = match connection.cert_fingerprint {
        Some(fingerprint) => {
            let Some(state) = state else {
                return Err(ApiError::unauthorized());
            };
            Some(
                state
                    .registry()
                    .authenticate_certificate(&fingerprint)
                    .map_err(|err| {
                        tracing::warn!(
                            source = connection.source.as_str(),
                            peer = connection.peer.as_deref().unwrap_or(""),
                            %err,
                            "client certificate authentication failed"
                        );
                        ApiError::unauthorized()
                    })?,
            )
        }
        None => None,
    };

    let token_principal = match bearer_token(headers)? {
        Some(token) => {
            let Some(state) = state else {
                return Err(ApiError::unauthorized());
            };
            let registry = state.registry();
            Some(registry.authenticate_token(&token).map_err(|err| {
                tracing::warn!(
                    source = connection.source.as_str(),
                    peer = connection.peer.as_deref().unwrap_or(""),
                    %err,
                    "bearer authentication failed"
                );
                ApiError::unauthorized()
            })?)
        }
        None => None,
    };

    match (cert_principal, token_principal) {
        (Some(cert), Some(token)) => {
            if cert.name() != token.name() {
                return Err(ApiError::forbidden());
            }
            Ok(token)
        }
        (Some(cert), None) => Ok(cert),
        (None, Some(token)) => Ok(token),
        (None, None) => Err(ApiError::unauthorized()),
    }
}

fn has_enabled_admin(file: &AuthFile) -> bool {
    let admins: std::collections::HashSet<&str> = file
        .principals
        .iter()
        .filter(|(name, def)| {
            Principal::new((*name).clone(), &def.scopes)
                .map(|principal| principal.allows_admin_database())
                .unwrap_or(false)
        })
        .map(|(name, _)| name.as_str())
        .collect();
    file.tokens
        .iter()
        .any(|token| !token.disabled && admins.contains(token.principal.as_str()))
        || file
            .certificates
            .iter()
            .any(|cert| !cert.disabled && admins.contains(cert.principal.as_str()))
}

fn bearer_token(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    let Some(value) = headers.get(AUTHORIZATION) else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| ApiError::unauthorized())?;
    let (scheme, token) = value.split_once(' ').ok_or_else(ApiError::unauthorized)?;
    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() {
        return Err(ApiError::unauthorized());
    }
    Ok(Some(token.to_string()))
}

fn file_stamp(path: &Path) -> Result<FileStamp, String> {
    let meta = std::fs::metadata(path)
        .map_err(|e| format!("cannot stat auth file {}: {e}", path.display()))?;
    Ok(FileStamp {
        modified: meta.modified().ok(),
        len: meta.len(),
    })
}

fn check_permissions(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(path) {
            let mode = meta.permissions().mode() & 0o777;
            if mode & 0o007 != 0 {
                tracing::warn!(
                    path = %path.display(),
                    mode = format!("{mode:o}"),
                    "auth file is accessible by other users; recommended mode is 0640"
                );
            }
        }
    }
}
