//! Runtime listener information exposed to the CLI and GUI.

use std::sync::{OnceLock, RwLock};

use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct TcpListenerInfo {
    pub configured: String,
    pub bound: String,
    pub tls: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ServerInfo {
    pub version: String,
    pub tcp: Option<TcpListenerInfo>,
    pub unix: Option<String>,
    pub advertise_url: Option<String>,
}

static INFO: OnceLock<RwLock<ServerInfo>> = OnceLock::new();

fn slot() -> &'static RwLock<ServerInfo> {
    INFO.get_or_init(|| {
        RwLock::new(ServerInfo {
            version: env!("CARGO_PKG_VERSION").to_string(),
            ..ServerInfo::default()
        })
    })
}

pub fn set_tcp(configured: &str, bound: &str, tls: bool) {
    let mut info = slot().write().unwrap_or_else(|p| p.into_inner());
    info.tcp = Some(TcpListenerInfo {
        configured: configured.to_string(),
        bound: bound.to_string(),
        tls,
    });
}

pub fn set_unix(path: &std::path::Path) {
    let mut info = slot().write().unwrap_or_else(|p| p.into_inner());
    info.unix = Some(path.display().to_string());
}

pub fn set_advertise_url(value: Option<String>) {
    let mut info = slot().write().unwrap_or_else(|p| p.into_inner());
    info.advertise_url = value;
}

pub fn snapshot() -> ServerInfo {
    slot().read().unwrap_or_else(|p| p.into_inner()).clone()
}
