//! Staged import of native NonoDB backups from local folders or S3/R2.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use nonodb_core::backup_compress;
use nonodb_core::backup_crypto;
use nonodb_core::verify::{self, BackupManifest};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::databases::{validate_name as validate_database_name, Databases};
use crate::s3_import::{S3Reader, S3Source};

const MAX_LOCAL_RESULTS: usize = 500;
const MAX_LOCAL_DEPTH: usize = 6;
const MAX_S3_MANIFESTS: usize = 200;

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ImportSource {
    Local {
        #[serde(default)]
        path: String,
    },
    S3 {
        endpoint: String,
        bucket: String,
        #[serde(default = "default_region")]
        region: String,
        #[serde(default)]
        prefix: String,
        #[serde(default = "default_access_env")]
        access_key_env: String,
        #[serde(default = "default_secret_env")]
        secret_key_env: String,
        #[serde(default)]
        session_token_env: Option<String>,
    },
}

fn default_region() -> String {
    "auto".into()
}
fn default_access_env() -> String {
    "AWS_ACCESS_KEY_ID".into()
}
fn default_secret_env() -> String {
    "AWS_SECRET_ACCESS_KEY".into()
}

impl ImportSource {
    fn label(&self) -> String {
        match self {
            Self::Local { path } => format!("local:{path}"),
            Self::S3 {
                endpoint,
                bucket,
                prefix,
                ..
            } => format!("s3:{endpoint}/{bucket}/{prefix}"),
        }
    }

    fn s3(&self) -> Option<S3Source> {
        match self {
            Self::S3 {
                endpoint,
                bucket,
                region,
                prefix,
                access_key_env,
                secret_key_env,
                session_token_env,
            } => Some(S3Source {
                endpoint: endpoint.clone(),
                bucket: bucket.clone(),
                region: region.clone(),
                prefix: prefix.clone(),
                access_key_env: access_key_env.clone(),
                secret_key_env: secret_key_env.clone(),
                session_token_env: session_token_env.clone(),
            }),
            Self::Local { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveredBackup {
    pub location: String,
    pub manifest: BackupManifest,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BrowseRequest {
    pub source: ImportSource,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunImportRequest {
    pub source: ImportSource,
    pub location: String,
    pub target_database: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ImportStatus {
    pub id: String,
    pub status: String,
    pub source: String,
    pub location: String,
    pub target_database: String,
    pub error: Option<String>,
    pub bytes: u64,
    pub started_at_ms: u64,
    pub finished_at_ms: Option<u64>,
}

#[derive(Clone)]
struct StagingGuard(PathBuf);

impl Drop for StagingGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub struct ImportManager {
    staging_root: PathBuf,
    roots: Vec<PathBuf>,
    databases: Arc<Databases>,
    jobs: Mutex<HashMap<String, ImportStatus>>,
    counter: AtomicU64,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl ImportManager {
    pub fn open(data_root: &Path, databases: Arc<Databases>) -> Result<Arc<Self>, String> {
        let staging_root = data_root.join(".import-staging");
        let mut roots = std::env::var_os("NONODB_IMPORT_ROOTS")
            .map(|value| std::env::split_paths(&value).collect::<Vec<_>>())
            .unwrap_or_default();
        if roots.is_empty() {
            roots.push(data_root.join("backups"));
        }
        if let Some(first) = roots.first() {
            let _ = std::fs::create_dir_all(first);
        }
        Ok(Arc::new(Self {
            staging_root,
            roots,
            databases,
            jobs: Mutex::new(HashMap::new()),
            counter: AtomicU64::new(0),
        }))
    }

    pub fn view(&self) -> Value {
        json!({
            "roots": self.roots.iter().map(|path| path.display().to_string()).collect::<Vec<_>>(),
            "default_root": self.roots.first().map(|path| path.display().to_string()),
            "jobs": self.statuses(),
        })
    }

    pub fn statuses(&self) -> Vec<ImportStatus> {
        let jobs = self.jobs.lock().unwrap_or_else(|p| p.into_inner());
        let mut out: Vec<ImportStatus> = jobs.values().cloned().collect();
        out.sort_by_key(|item| std::cmp::Reverse(item.started_at_ms));
        out
    }

    pub fn status(&self, id: &str) -> Option<ImportStatus> {
        self.jobs
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(id)
            .cloned()
    }

    pub async fn browse(&self, source: ImportSource) -> Result<Vec<DiscoveredBackup>, String> {
        match source {
            ImportSource::Local { path } => {
                let root = if path.trim().is_empty() {
                    self.roots
                        .first()
                        .cloned()
                        .ok_or("no local import root configured")?
                } else {
                    PathBuf::from(path)
                };
                let root = self.allowed_local(&root)?;
                let mut out = Vec::new();
                discover_local(&root, 0, &mut out)?;
                out.sort_by_key(|item| std::cmp::Reverse(item.manifest.created_at_ms));
                Ok(out)
            }
            ImportSource::S3 { .. } => {
                let s3 = source.s3().ok_or("missing S3 source")?;
                let reader = S3Reader::new(s3.clone())?;
                let keys = reader.list(&s3.prefix).await?;
                let mut out = Vec::new();
                for key in keys {
                    if out.len() >= MAX_S3_MANIFESTS {
                        break;
                    }
                    let Some(location) = manifest_location(&key) else {
                        continue;
                    };
                    let text = reader.get_text(&key).await?;
                    let Ok(manifest) = serde_json::from_str::<BackupManifest>(&text) else {
                        continue;
                    };
                    out.push(DiscoveredBackup {
                        location,
                        manifest,
                        info: None,
                    });
                }
                out.sort_by_key(|item| std::cmp::Reverse(item.manifest.created_at_ms));
                Ok(out)
            }
        }
    }

    pub fn start(self: &Arc<Self>, request: RunImportRequest) -> Result<ImportStatus, String> {
        validate_database_name(&request.target_database)
            .map_err(|error| format!("invalid target database: {error}"))?;
        if request.location.trim().is_empty() {
            return Err("backup location is required".into());
        }
        let id = format!(
            "import-{}-{}",
            now_ms(),
            self.counter.fetch_add(1, Ordering::Relaxed)
        );
        let status = ImportStatus {
            id: id.clone(),
            status: "queued".into(),
            source: request.source.label(),
            location: request.location.clone(),
            target_database: request.target_database.clone(),
            error: None,
            bytes: 0,
            started_at_ms: now_ms(),
            finished_at_ms: None,
        };
        self.jobs
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id.clone(), status.clone());
        let manager = self.clone();
        tokio::spawn(async move {
            manager.execute(id, request).await;
        });
        Ok(status)
    }

    async fn execute(self: Arc<Self>, id: String, request: RunImportRequest) {
        self.update(&id, |status| {
            status.status = "running".into();
        });
        let result = self.run_import(&id, &request).await;
        self.update(&id, |status| {
            status.finished_at_ms = Some(now_ms());
            match &result {
                Ok(bytes) => {
                    status.status = "ok".into();
                    status.bytes = *bytes;
                    status.error = None;
                }
                Err(error) => {
                    status.status = "error".into();
                    status.error = Some(error.clone());
                }
            }
        });
    }

    fn update(&self, id: &str, apply: impl FnOnce(&mut ImportStatus)) {
        let mut jobs = self.jobs.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(status) = jobs.get_mut(id) {
            apply(status);
        }
    }

    async fn run_import(&self, id: &str, request: &RunImportRequest) -> Result<u64, String> {
        let staging = self.staging_root.join(id);
        let _ = std::fs::remove_dir_all(&staging);
        std::fs::create_dir_all(&staging)
            .map_err(|e| format!("create import staging {}: {e}", staging.display()))?;
        let _guard = StagingGuard(staging.clone());

        let (manifest, payload, info) = match &request.source {
            ImportSource::Local { .. } => {
                let backup_dir = self.allowed_local(Path::new(&request.location))?;
                let manifest = verify::read_manifest(&backup_dir)
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| format!("missing manifest.json in {}", backup_dir.display()))?;
                let source = verify::manifest_data_path(&backup_dir, &manifest)
                    .map_err(|e| e.to_string())?;
                let payload = staging.join("payload.bin");
                std::fs::copy(&source, &payload)
                    .map_err(|e| format!("copy backup payload: {e}"))?;
                let info =
                    std::fs::read_to_string(backup_dir.join(nonodb_core::BACKUP_INFO_FILE)).ok();
                (manifest, payload, info)
            }
            ImportSource::S3 { .. } => {
                let s3 = request.source.s3().ok_or("missing S3 source")?;
                let reader = S3Reader::new(s3)?;
                let manifest_key = path_join(&request.location, "manifest.json");
                let text = reader.get_text(&manifest_key).await?;
                let manifest = serde_json::from_str::<BackupManifest>(&text)
                    .map_err(|e| format!("parse remote manifest: {e}"))?;
                let payload = staging.join("payload.bin");
                let data_key = path_join(&request.location, &manifest.data_file);
                reader.get_to_file(&data_key, &payload).await?;
                let info_key = path_join(&request.location, nonodb_core::BACKUP_INFO_FILE);
                let info = reader.get_text(&info_key).await.ok();
                (manifest, payload, info)
            }
        };

        let size = std::fs::metadata(&payload)
            .map_err(|e| e.to_string())?
            .len();
        if size != manifest.data_size {
            return Err(format!(
                "backup size mismatch: manifest {}, actual {size}",
                manifest.data_size
            ));
        }
        let digest = verify::sha256_file(&payload).map_err(|e| e.to_string())?;
        if digest != manifest.sha256 {
            return Err(format!(
                "backup SHA-256 mismatch: manifest {}, actual {digest}",
                manifest.sha256
            ));
        }

        let data = prepare_payload(&staging, &manifest, &payload)?;
        verify::verify_data_dir(&staging).map_err(|e| format!("verify imported database: {e}"))?;
        self.databases
            .import_database(&request.target_database, &data)
            .await
            .map_err(|e| e.message)?;
        tracing::info!(
            import = %id,
            source = %request.source.label(),
            target = %request.target_database,
            bytes = size,
            info_present = info.is_some(),
            "backup imported"
        );
        Ok(size)
    }

    fn allowed_local(&self, path: &Path) -> Result<PathBuf, String> {
        let canonical = path
            .canonicalize()
            .map_err(|e| format!("resolve {}: {e}", path.display()))?;
        for root in &self.roots {
            let Ok(root) = root.canonicalize() else {
                continue;
            };
            if canonical.starts_with(root) {
                return Ok(canonical);
            }
        }
        Err(format!(
            "path {} is outside NONODB_IMPORT_ROOTS (configured: {})",
            canonical.display(),
            self.roots
                .iter()
                .map(|root| root.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }
}

fn prepare_payload(
    staging: &Path,
    manifest: &BackupManifest,
    raw: &Path,
) -> Result<PathBuf, String> {
    let mut current = raw.to_path_buf();
    if manifest.encryption.is_some() {
        let key_path = std::env::var_os("NONODB_BACKUP_IDENTITY_FILE")
            .map(PathBuf::from)
            .ok_or("encrypted backup requires NONODB_BACKUP_IDENTITY_FILE")?;
        let identity = backup_crypto::read_key_file(&key_path).map_err(|e| e.to_string())?;
        let decrypted = staging.join("payload.decrypted");
        backup_crypto::decrypt_file(&current, &decrypted, &identity)
            .map_err(|e| format!("decrypt backup: {e}"))?;
        current = decrypted;
    }
    let data = staging.join("data.mdb");
    if manifest.compression.is_some() {
        backup_compress::gunzip_file(&current, &data).map_err(|e| format!("decompress: {e}"))?;
    } else {
        std::fs::rename(&current, &data)
            .or_else(|_| std::fs::copy(&current, &data).map(|_| ()))
            .map_err(|e| format!("prepare imported data: {e}"))?;
    }
    let meta = std::fs::metadata(&data).map_err(|e| e.to_string())?;
    if let Some(size) = manifest.plain_size {
        if meta.len() != size {
            return Err(format!(
                "restored size mismatch: manifest {size}, actual {}",
                meta.len()
            ));
        }
    }
    if let Some(expected) = &manifest.plain_sha256 {
        let digest = verify::sha256_file(&data).map_err(|e| e.to_string())?;
        if &digest != expected {
            return Err(format!(
                "restored SHA-256 mismatch: manifest {expected}, actual {digest}"
            ));
        }
    }
    Ok(data)
}

fn discover_local(dir: &Path, depth: usize, out: &mut Vec<DiscoveredBackup>) -> Result<(), String> {
    if depth > MAX_LOCAL_DEPTH || out.len() >= MAX_LOCAL_RESULTS {
        return Ok(());
    }
    let entries = std::fs::read_dir(dir).map_err(|e| format!("read {}: {e}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        let file_type = entry.file_type().map_err(|e| e.to_string())?;
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            discover_local(&entry.path(), depth + 1, out)?;
            continue;
        }
        if entry.file_name() != "manifest.json" {
            continue;
        }
        let path = entry.path();
        let Some(parent) = path.parent() else {
            continue;
        };
        let Ok(raw) = std::fs::read(&path) else {
            continue;
        };
        let Ok(manifest) = serde_json::from_slice::<BackupManifest>(&raw) else {
            continue;
        };
        let info = std::fs::read_to_string(parent.join(nonodb_core::BACKUP_INFO_FILE))
            .ok()
            .filter(|text| text.len() <= 64 * 1024);
        out.push(DiscoveredBackup {
            location: parent.display().to_string(),
            manifest,
            info,
        });
        if out.len() >= MAX_LOCAL_RESULTS {
            return Ok(());
        }
    }
    Ok(())
}

fn manifest_location(key: &str) -> Option<String> {
    if key == "manifest.json" {
        return Some(String::new());
    }
    key.strip_suffix("/manifest.json").map(str::to_string)
}

fn path_join(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}/{name}")
    }
}
