//! Per-user registry of running daemon instances.
//!
//! Each daemon writes a small JSON file describing its PID, listeners, data
//! directory, role, and node id. The CLI uses this to let an operator select
//! one local instance when several daemons are running. The files are
//! node-local and are never part of a database backup.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

const DIR_ENV: &str = "NONODB_INSTANCE_DIR";
const DEFAULT_DIR: &str = "nonodb/instances";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstanceRecord {
    pub pid: u32,
    pub version: String,
    pub data: String,
    pub socket: Option<String>,
    pub tcp_configured: Option<String>,
    pub tcp_bound: Option<String>,
    pub tls: bool,
    pub advertise_url: Option<String>,
    pub node_id: String,
    pub role: String,
    pub started_at_ms: u128,
    pub cwd: Option<String>,
}

#[derive(Clone, Debug)]
pub struct InstanceEntry {
    pub path: PathBuf,
    pub record: InstanceRecord,
    pub alive: bool,
}

/// Directory holding one JSON file per daemon. Override with
/// `NONODB_INSTANCE_DIR`.
pub fn dir() -> PathBuf {
    if let Some(value) = std::env::var_os(DIR_ENV) {
        if !value.is_empty() {
            return PathBuf::from(value);
        }
    }
    if let Some(value) = std::env::var_os("XDG_RUNTIME_DIR") {
        if !value.is_empty() {
            return PathBuf::from(value).join(DEFAULT_DIR);
        }
    }
    #[cfg(windows)]
    if let Some(value) = std::env::var_os("LOCALAPPDATA") {
        if !value.is_empty() {
            return PathBuf::from(value).join("nonodb").join("instances");
        }
    }
    if let Some(value) = std::env::var_os("HOME") {
        if !value.is_empty() {
            return PathBuf::from(value).join(".nonodb").join("instances");
        }
    }
    std::env::temp_dir().join("nonodb-instances")
}

fn ensure_dir() -> Result<PathBuf, String> {
    let path = dir();
    fs::create_dir_all(&path)
        .map_err(|error| format!("create instance dir {}: {error}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o700));
    }
    Ok(path)
}

fn record_path(pid: u32) -> Result<PathBuf, String> {
    Ok(ensure_dir()?.join(format!("{pid}.json")))
}

/// Atomically create or replace the registry file for this daemon.
pub fn write(record: &InstanceRecord) -> Result<PathBuf, String> {
    let path = record_path(record.pid)?;
    let temp = path.with_extension(format!("json.{}.tmp", std::process::id()));
    let body = serde_json::to_vec_pretty(record).map_err(|error| error.to_string())?;
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    {
        use std::io::Write;
        let mut file = options
            .open(&temp)
            .map_err(|error| format!("create {}: {error}", temp.display()))?;
        file.write_all(&body)
            .map_err(|error| format!("write {}: {error}", temp.display()))?;
        file.sync_all()
            .map_err(|error| format!("sync {}: {error}", temp.display()))?;
    }
    fs::rename(&temp, &path).map_err(|error| format!("rename {}: {error}", path.display()))?;
    Ok(path)
}

pub fn remove(pid: u32) {
    if let Ok(path) = record_path(pid) {
        let _ = fs::remove_file(path);
    }
}

/// Remove files whose process is no longer alive. Called at daemon startup and
/// by `nonodb-cli instances prune`.
pub fn prune_stale() -> Result<usize, String> {
    let path = ensure_dir()?;
    let mut removed = 0;
    let entries = match fs::read_dir(&path) {
        Ok(entries) => entries,
        Err(_) => return Ok(0),
    };
    for entry in entries.flatten() {
        let file = entry.path();
        if file.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let Some(record) = read_record(&file) else {
            let _ = fs::remove_file(&file);
            removed += 1;
            continue;
        };
        if !process_alive(record.pid) {
            let _ = fs::remove_file(&file);
            removed += 1;
        }
    }
    Ok(removed)
}

fn read_record(path: &Path) -> Option<InstanceRecord> {
    let bytes = fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// List all registry entries, including stale ones, with liveness attached.
pub fn list() -> Result<Vec<InstanceEntry>, String> {
    let path = ensure_dir()?;
    let mut result = Vec::new();
    let entries = match fs::read_dir(&path) {
        Ok(entries) => entries,
        Err(_) => return Ok(result),
    };
    for entry in entries.flatten() {
        let file = entry.path();
        if file.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        if let Some(record) = read_record(&file) {
            result.push(InstanceEntry {
                path: file,
                alive: process_alive(record.pid),
                record,
            });
        }
    }
    result.sort_by(|left, right| {
        left.record
            .started_at_ms
            .cmp(&right.record.started_at_ms)
            .then_with(|| left.record.pid.cmp(&right.record.pid))
    });
    Ok(result)
}

/// Resolve a PID or node id to one live registry entry. A socket/data path is
/// accepted as a convenience when the user copied it from `instances`.
pub fn resolve(selector: &str) -> Result<InstanceEntry, String> {
    let entries = list()?;
    let mut matches: Vec<InstanceEntry> = entries
        .into_iter()
        .filter(|entry| {
            entry.alive
                && (entry.record.pid.to_string() == selector
                    || entry.record.node_id == selector
                    || entry
                        .record
                        .socket
                        .as_deref()
                        .is_some_and(|socket| socket == selector)
                    || entry.record.data == selector)
        })
        .collect();
    matches.sort_by_key(|entry| std::cmp::Reverse(entry.record.started_at_ms));
    match matches.into_iter().next() {
        Some(entry) => Ok(entry),
        None => Err(format!(
            "no live instance matches `{selector}`; run `nonodb-cli instances`"
        )),
    }
}

pub fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    // Signal 0 only checks existence. EPERM means the process exists but is
    // owned by another user.
    let result = unsafe { kill(pid as i32, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(1)
}

#[cfg(not(unix))]
fn process_alive(_pid: u32) -> bool {
    // The registry is best-effort on non-Unix platforms; stale files are
    // ignored by selectors that fail their health check.
    true
}
