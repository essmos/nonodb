//! Publish this daemon in the per-user instance registry.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use nonodb_core::instance::{self as registry, InstanceRecord};

static CURRENT: OnceLock<Mutex<InstanceRecord>> = OnceLock::new();

fn absolute(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    }
}

fn slot() -> Option<&'static Mutex<InstanceRecord>> {
    CURRENT.get()
}

fn commit(record: &InstanceRecord) -> Result<(), String> {
    registry::write(record).map(|_| ())
}

pub fn start(data: &Path, socket: Option<&Path>, tcp: Option<&str>) -> Result<(), String> {
    let _ = registry::prune_stale();
    let record = InstanceRecord {
        pid: std::process::id(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        data: absolute(data).display().to_string(),
        socket: socket.map(|path| absolute(path).display().to_string()),
        tcp_configured: tcp.map(str::to_string),
        tcp_bound: None,
        tls: false,
        advertise_url: None,
        node_id: crate::replicator::node_id(),
        role: crate::replicator::role(),
        started_at_ms: registry::now_ms(),
        cwd: std::env::current_dir()
            .ok()
            .map(|path| path.display().to_string()),
    };
    commit(&record)?;
    let _ = CURRENT.set(Mutex::new(record));
    Ok(())
}

pub fn set_tcp(configured: &str, bound: &str, tls: bool) {
    let Some(slot) = slot() else { return };
    let mut record = slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    record.tcp_configured = Some(configured.to_string());
    record.tcp_bound = Some(bound.to_string());
    record.tls = tls;
    let _ = commit(&record);
}

pub fn set_advertise_url(url: Option<String>) {
    let Some(slot) = slot() else { return };
    let mut record = slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    record.advertise_url = url;
    let _ = commit(&record);
}

pub fn stop() {
    registry::remove(std::process::id());
}
