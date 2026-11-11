//! Append-only JSONL audit log.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

#[derive(Clone, Copy, Debug)]
pub struct AuthAudit {
    pub failures: u32,
    pub blocked_for_secs: Option<u64>,
}

pub struct AuditLog {
    path: PathBuf,
    file: Mutex<File>,
    fsync: bool,
    reads: bool,
}

impl AuditLog {
    pub fn from_env() -> Result<Option<Arc<Self>>, String> {
        let Some(path) = std::env::var_os("NONODB_AUDIT_LOG").map(PathBuf::from) else {
            return Ok(None);
        };
        if path.as_os_str().is_empty() {
            return Ok(None);
        }
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    format!("cannot create audit directory {}: {e}", parent.display())
                })?;
            }
        }
        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o640);
        }
        let file = options
            .open(&path)
            .map_err(|e| format!("cannot open audit log {}: {e}", path.display()))?;
        Ok(Some(Arc::new(Self {
            path,
            file: Mutex::new(file),
            fsync: env_bool("NONODB_AUDIT_FSYNC", false),
            reads: env_bool("NONODB_AUDIT_READS", false),
        })))
    }

    pub fn log_reads(&self) -> bool {
        self.reads
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &self,
        principal: &str,
        source: &str,
        peer: Option<&str>,
        op: &str,
        collections: &[&str],
        ok: bool,
        error: Option<&str>,
        duration_ms: f64,
        auth: Option<AuthAudit>,
    ) {
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let mut collections: Vec<&str> = collections.to_vec();
        collections.sort_unstable();
        collections.dedup();
        let mut line = json!({
            "ts_ms": ts_ms,
            "principal": principal,
            "source": source,
            "peer": peer,
            "op": op,
            "collections": collections,
            "result": if ok { "ok" } else { "error" },
            "error": error,
            "duration_ms": duration_ms,
        });
        if let Some(auth) = auth {
            if let Some(object) = line.as_object_mut() {
                object.insert("auth_failures".into(), Value::from(auth.failures));
                if let Some(secs) = auth.blocked_for_secs {
                    object.insert("blocked_for_secs".into(), Value::from(secs));
                }
            }
        }
        let mut encoded = serde_json::to_vec(&line).unwrap_or_default();
        encoded.push(b'\n');
        let mut file = self.file.lock().expect("audit lock");
        if let Err(err) = file.write_all(&encoded) {
            tracing::error!(path = %self.path.display(), %err, "audit write failed");
            return;
        }
        if let Err(err) = file.flush() {
            tracing::error!(path = %self.path.display(), %err, "audit flush failed");
            return;
        }
        if self.fsync {
            if let Err(err) = file.sync_all() {
                tracing::error!(path = %self.path.display(), %err, "audit fsync failed");
            }
        }
    }
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}
