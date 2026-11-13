//! Scheduled full backups, optional age encryption, and local/S3 uploads.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nonodb_core::backup_compress;
use nonodb_core::backup_crypto;
use nonodb_core::backup_info;
use nonodb_core::heed::CompactionOption;
use nonodb_core::verify::{self, BackupCompression, BackupEncryption, BackupManifest};
use nonodb_core::{Store, MAP_SIZE, PAGE_SIZE};
use reqwest::Url;
use ring::hmac;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio_util::io::ReaderStream;

use crate::databases::{validate_name as validate_database_name, Databases};

pub const CONFIG_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupConfig {
    pub version: u32,
    #[serde(default)]
    pub jobs: Vec<BackupJob>,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            version: CONFIG_VERSION,
            jobs: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupJob {
    pub id: String,
    /// Legacy single-database field. New jobs set `databases`; both are
    /// accepted so existing backups.json files keep working.
    #[serde(default)]
    pub database: String,
    #[serde(default)]
    pub databases: Vec<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Backup interval in minutes.
    pub every_minutes: u64,
    pub destination: Destination,
    #[serde(default)]
    pub encryption: Option<Encryption>,
    #[serde(default = "default_keep")]
    pub keep_last: usize,
    #[serde(default)]
    pub verify: bool,
    /// Compression for native copies. Restic manages its own compression.
    #[serde(default)]
    pub compression: Compression,
    /// Collections removed from the backup copy. The live database is not
    /// modified.
    #[serde(default)]
    pub exclude_collections: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    #[default]
    None,
    Gzip,
}

impl BackupJob {
    /// Effective database list, preserving the legacy `database` field.
    pub fn database_list(&self) -> Vec<String> {
        let mut out = if self.databases.is_empty() {
            if self.database.is_empty() {
                Vec::new()
            } else {
                vec![self.database.clone()]
            }
        } else {
            self.databases.clone()
        };
        out.retain(|name| !name.is_empty());
        out.sort();
        out.dedup();
        out
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Destination {
    Local {
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
    /// Restic repository. Restic owns chunking, encryption, deduplication, S3,
    /// retention, and restore.
    Restic {
        repository: String,
        password_file: String,
        #[serde(default = "default_restic_binary")]
        binary: String,
    },
}

fn default_restic_binary() -> String {
    "restic".into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Encryption {
    Age { recipient_file: String },
}

fn default_true() -> bool {
    true
}

fn default_keep() -> usize {
    14
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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct JobState {
    pub last_run_unix: u64,
    pub next_run_unix: u64,
    pub last_status: String,
    pub last_error: Option<String>,
    pub last_size_bytes: u64,
    pub last_duration_ms: u64,
    pub last_destination: String,
    pub running: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct StateFile {
    version: u32,
    #[serde(default)]
    jobs: BTreeMap<String, JobState>,
}

struct Inner {
    config: BackupConfig,
    state: StateFile,
}
struct StagingGuard(PathBuf);

impl Drop for StagingGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub struct BackupManager {
    config_path: PathBuf,
    state_path: PathBuf,
    staging_root: PathBuf,
    key_dir: PathBuf,
    default_local_path: PathBuf,
    default_restic_repository: PathBuf,
    databases: Arc<Databases>,
    inner: Mutex<Inner>,
    execution: tokio::sync::Mutex<()>,
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn atomic_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    let tmp = dir.join(format!(
        ".{}.tmp.{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("backup"),
        std::process::id()
    ));
    let raw = serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?;
    std::fs::write(&tmp, raw).map_err(|e| format!("write {}: {e}", tmp.display()))?;
    let file = std::fs::File::open(&tmp).map_err(|e| e.to_string())?;
    let _ = file.sync_all();
    std::fs::rename(&tmp, path).map_err(|e| format!("replace {}: {e}", path.display()))?;
    Ok(())
}

fn load_config(path: &Path) -> Result<BackupConfig, String> {
    if !path.is_file() {
        return Ok(BackupConfig::default());
    }
    let raw = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let config: BackupConfig =
        serde_json::from_slice(&raw).map_err(|e| format!("parse {}: {e}", path.display()))?;
    if config.version != CONFIG_VERSION {
        return Err(format!(
            "unsupported backup config version {} (expected {})",
            config.version, CONFIG_VERSION
        ));
    }
    validate_config(&config)?;
    Ok(config)
}

fn load_state(path: &Path) -> Result<StateFile, String> {
    if !path.is_file() {
        return Ok(StateFile {
            version: 1,
            jobs: BTreeMap::new(),
        });
    }
    let raw = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    serde_json::from_slice(&raw).map_err(|e| format!("parse {}: {e}", path.display()))
}

fn validate_job_id(id: &str) -> Result<(), String> {
    if id.is_empty()
        || id.len() > 96
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
    {
        return Err(format!(
            "invalid backup job id `{id}` (letters, digits, `_`, `-` only)"
        ));
    }
    Ok(())
}

#[allow(clippy::collapsible_match)]
fn validate_config(config: &BackupConfig) -> Result<(), String> {
    let mut ids = std::collections::HashSet::new();
    for job in &config.jobs {
        validate_job_id(&job.id)?;
        if !ids.insert(job.id.clone()) {
            return Err(format!("duplicate backup job id `{}`", job.id));
        }
        let databases = job.database_list();
        if databases.is_empty() {
            return Err(format!(
                "backup job `{}` must select at least one database",
                job.id
            ));
        }
        for database in &databases {
            validate_database_name(database)
                .map_err(|error| format!("backup job `{}`: {error}", job.id))?;
        }
        if job.every_minutes == 0 {
            return Err(format!(
                "backup job `{}` must run at least every minute",
                job.id
            ));
        }
        match &job.destination {
            Destination::Local { path } if path.trim().is_empty() => {
                return Err(format!("backup job `{}` has an empty local path", job.id));
            }
            Destination::S3 {
                endpoint, bucket, ..
            } => {
                if endpoint.trim().is_empty() || bucket.trim().is_empty() {
                    return Err(format!(
                        "backup job `{}` needs an S3 endpoint and bucket",
                        job.id
                    ));
                }
            }
            _ => {}
        }
        if let Destination::Restic {
            repository,
            password_file,
            binary,
        } = &job.destination
        {
            if repository.trim().is_empty()
                || password_file.trim().is_empty()
                || binary.trim().is_empty()
            {
                return Err(format!(
                    "backup job `{}` needs a restic repository, password file, and binary",
                    job.id
                ));
            }
            if job.encryption.is_some() {
                return Err(format!(
                    "backup job `{}` should not use age encryption with restic",
                    job.id
                ));
            }
        }
        for name in &job.exclude_collections {
            if name.is_empty() || name.len() > 200 || name.contains('\0') {
                return Err(format!(
                    "backup job `{}` has an invalid excluded collection name",
                    job.id
                ));
            }
        }
        if let Some(Encryption::Age { recipient_file }) = &job.encryption {
            if recipient_file.trim().is_empty() {
                return Err(format!(
                    "backup job `{}` has an empty age recipient file",
                    job.id
                ));
            }
        }
    }
    Ok(())
}

impl BackupManager {
    pub fn open(data_root: &Path, databases: Arc<Databases>) -> Result<Arc<Self>, String> {
        let config_path = std::env::var_os("NONODB_BACKUP_CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(|| data_root.join("backups.json"));
        let state_path = config_path.with_file_name("backups-state.json");
        let staging_root = std::env::var_os("NONODB_BACKUP_STAGING")
            .map(PathBuf::from)
            .unwrap_or_else(|| data_root.join(".backup-staging"));
        let key_dir = std::env::var_os("NONODB_BACKUP_KEY_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| data_root.join("backup-keys"));
        let default_local_path = data_root.join("backups");
        let default_restic_repository = data_root.join("restic-repo");
        if let Err(error) = std::fs::create_dir_all(&key_dir) {
            return Err(format!(
                "create backup key dir {}: {error}",
                key_dir.display()
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&key_dir, std::fs::Permissions::from_mode(0o700));
        }
        let config = load_config(&config_path)?;
        let mut state = load_state(&state_path)?;
        for job_state in state.jobs.values_mut() {
            if job_state.running {
                job_state.running = false;
                job_state.last_status = "interrupted".into();
                job_state.last_error = Some("daemon restarted while the job was running".into());
            }
        }
        Ok(Arc::new(Self {
            config_path,
            state_path,
            staging_root,
            key_dir,
            default_local_path,
            default_restic_repository,
            databases,
            inner: Mutex::new(Inner { config, state }),
            execution: tokio::sync::Mutex::new(()),
        }))
    }

    pub fn start_scheduler(self: &Arc<Self>) {
        let interval = std::env::var("NONODB_BACKUP_SCHEDULER_INTERVAL")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(15);
        let manager = self.clone();
        tokio::spawn(async move {
            loop {
                let due = manager.due_jobs();
                for id in due {
                    manager.execute(&id).await;
                }
                tokio::time::sleep(Duration::from_secs(interval)).await;
            }
        });
    }

    fn due_jobs(&self) -> Vec<String> {
        let now = now_unix();
        let inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner
            .config
            .jobs
            .iter()
            .filter(|job| job.enabled)
            .filter_map(|job| {
                let state = inner.state.jobs.get(&job.id);
                if state.is_some_and(|state| state.running) {
                    return None;
                }
                let next = state.map(|state| state.next_run_unix).unwrap_or(0);
                (next == 0 || next <= now).then(|| job.id.clone())
            })
            .collect()
    }

    pub fn view(&self) -> Value {
        let inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let jobs: Vec<Value> = inner
            .config
            .jobs
            .iter()
            .map(|job| self.job_view(job, &inner))
            .collect();
        json!({
            "config_path": self.config_path.display().to_string(),
            "staging_path": self.staging_root.display().to_string(),
            "key_dir": self.key_dir.display().to_string(),
            "default_identity_file": self.key_dir.join("backup-identity.txt").display().to_string(),
            "default_recipient_file": self.key_dir.join("backup-recipient.txt").display().to_string(),
            "default_restic_password_file": self.key_dir.join("restic-password").display().to_string(),
            "default_local_path": self.default_local_path.display().to_string(),
            "default_restic_repository": self.default_restic_repository.display().to_string(),
            "jobs": jobs,
        })
    }

    fn job_view(&self, job: &BackupJob, inner: &Inner) -> Value {
        let state = inner.state.jobs.get(&job.id).cloned().unwrap_or_default();
        json!({
            "job": job,
            "state": state,
        })
    }

    fn save(&self, inner: &Inner) -> Result<(), String> {
        atomic_json(&self.config_path, &inner.config)?;
        atomic_json(&self.state_path, &inner.state)
    }

    pub fn upsert(&self, job: BackupJob) -> Result<Value, String> {
        validate_job_id(&job.id)?;
        if job.every_minutes == 0 {
            return Err("every_minutes must be greater than zero".into());
        }
        let config = BackupConfig {
            version: CONFIG_VERSION,
            jobs: vec![job.clone()],
        };
        validate_config(&config)?;
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(existing) = inner.config.jobs.iter_mut().find(|item| item.id == job.id) {
            *existing = job.clone();
        } else {
            inner.config.jobs.push(job.clone());
        }
        let state = inner.state.jobs.entry(job.id.clone()).or_default();
        if state.next_run_unix == 0 {
            state.next_run_unix = now_unix() + job.every_minutes * 60;
        }
        self.save(&inner)?;
        Ok(self.job_view(&job, &inner))
    }

    pub fn delete(&self, id: &str) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let before = inner.config.jobs.len();
        inner.config.jobs.retain(|job| job.id != id);
        if inner.config.jobs.len() == before {
            return Err(format!("unknown backup job `{id}`"));
        }
        inner.state.jobs.remove(id);
        self.save(&inner)
    }

    pub fn run_now(self: &Arc<Self>, id: &str) -> Result<Value, String> {
        let job = {
            let inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            let job = inner
                .config
                .jobs
                .iter()
                .find(|job| job.id == id)
                .cloned()
                .ok_or_else(|| format!("unknown backup job `{id}`"))?;
            if inner.state.jobs.get(id).is_some_and(|state| state.running) {
                return Err(format!("backup job `{id}` is already running"));
            }
            job
        };
        if !job.enabled {
            return Err(format!("backup job `{id}` is disabled"));
        }
        {
            let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            let state = inner.state.jobs.entry(id.to_string()).or_default();
            state.running = true;
            state.last_status = "queued".into();
            state.last_error = None;
            self.save(&inner)?;
        }
        let manager = self.clone();
        let task_id = id.to_string();
        tokio::spawn(async move {
            manager.execute(&task_id).await;
        });
        Ok(json!({ "id": id, "started": true }))
    }

    async fn execute(self: &Arc<Self>, id: &str) {
        let _guard = self.execution.lock().await;
        let job = {
            let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            let Some(job) = inner.config.jobs.iter().find(|job| job.id == id).cloned() else {
                return;
            };
            let state = inner.state.jobs.entry(id.to_string()).or_default();
            state.running = true;
            state.last_status = "running".into();
            state.last_error = None;
            let _ = self.save(&inner);
            job
        };

        let started = std::time::Instant::now();
        let result = self.run_job(&job).await;
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let state = inner.state.jobs.entry(id.to_string()).or_default();
        state.running = false;
        state.last_run_unix = now_unix();
        state.next_run_unix = now_unix().saturating_add(job.every_minutes * 60);
        state.last_duration_ms = started.elapsed().as_millis() as u64;
        match result {
            Ok((size, destination)) => {
                state.last_status = "ok".into();
                state.last_error = None;
                state.last_size_bytes = size;
                state.last_destination = destination;
                tracing::info!(job = %id, databases = %job.database_list().join(","), bytes = size, "backup completed");
            }
            Err(error) => {
                state.last_status = "error".into();
                state.last_error = Some(error.clone());
                tracing::error!(job = %id, database = %job.database, %error, "backup failed");
            }
        }
        let _ = self.save(&inner);
    }

    async fn run_job(&self, job: &BackupJob) -> Result<(u64, String), String> {
        let databases = job.database_list();
        let multi = databases.len() > 1;
        let stamp = now_ms();
        let mut total = 0u64;
        let mut first_destination = String::new();
        for database in &databases {
            let (size, destination) = self
                .run_database_job(job, database, stamp, multi)
                .await
                .map_err(|error| format!("database `{database}`: {error}"))?;
            total = total.saturating_add(size);
            if first_destination.is_empty() {
                first_destination = destination;
            }
        }
        if let Destination::Local { path } = &job.destination {
            self.prune_local(&PathBuf::from(path).join(&job.id), job.keep_last)?;
        }
        let destination = if multi {
            match &job.destination {
                Destination::Local { path } => {
                    format!("{} ({} databases)", path, databases.len())
                }
                Destination::S3 { bucket, prefix, .. } => format!(
                    "s3://{bucket}/{} ({} databases)",
                    prefix.trim_matches('/'),
                    databases.len()
                ),
                Destination::Restic { repository, .. } => {
                    format!("restic:{repository} ({} databases)", databases.len())
                }
            }
        } else {
            first_destination
        };
        Ok((total, destination))
    }

    async fn run_database_job(
        &self,
        job: &BackupJob,
        database: &str,
        stamp: u64,
        multi: bool,
    ) -> Result<(u64, String), String> {
        let runtime = self
            .databases
            .get(database)
            .await
            .map_err(|err| err.message)?;
        let store = runtime.store.clone();
        let staging = self
            .staging_root
            .join(format!("{}-{stamp}", job.id))
            .join(database);
        std::fs::create_dir_all(&staging)
            .map_err(|e| format!("create staging {}: {e}", staging.display()))?;
        let _guard = StagingGuard(staging.clone());
        let plain = staging.join("data.mdb");
        let restic = matches!(job.destination, Destination::Restic { .. });
        let filtering = !job.exclude_collections.is_empty();
        let copy_store = store.clone();
        let copy_path = plain.clone();
        tokio::task::spawn_blocking(move || -> Result<(), String> {
            let option = if restic || filtering {
                CompactionOption::Disabled
            } else {
                CompactionOption::Enabled
            };
            let file = copy_store
                .copy_to_file(&copy_path, option)
                .map_err(|e| e.to_string())?;
            file.sync_all().map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| format!("backup copy task: {e}"))??;

        if filtering {
            let staging_path = staging.clone();
            let excluded = job.exclude_collections.clone();
            tokio::task::spawn_blocking(move || filter_excluded(&staging_path, &excluded))
                .await
                .map_err(|e| format!("backup filter task: {e}"))??;
        }

        if let Destination::Restic {
            repository,
            password_file,
            binary,
        } = &job.destination
        {
            if job.verify {
                verify::verify_data_dir(&staging).map_err(|e| format!("verify: {e}"))?;
            }
            let size = std::fs::metadata(&plain).map_err(|e| e.to_string())?.len();
            let repo = repository.clone();
            let task_repo = repo.clone();
            let password_file = password_file.clone();
            let binary = binary.clone();
            let job_id = job.id.clone();
            let database = database.to_string();
            let keep_last = job.keep_last;
            let verify_repo = job.verify;
            let data_file = plain.clone();
            let mut info_manifest =
                BackupManifest::from_backup(&staging, &database).map_err(|e| e.to_string())?;
            info_manifest.job_id = job.id.clone();
            info_manifest.excluded_collections = job.exclude_collections.clone();
            backup_info::write(&staging, &info_manifest).map_err(|e| e.to_string())?;
            let info_file = staging.join(backup_info::INFO_FILE);
            tokio::task::spawn_blocking(move || {
                restic_backup(
                    &binary,
                    &task_repo,
                    &password_file,
                    &job_id,
                    &database,
                    keep_last,
                    verify_repo,
                    &data_file,
                    &info_file,
                )
            })
            .await
            .map_err(|e| format!("restic task: {e}"))??;
            return Ok((size, format!("restic:{repo}")));
        }

        let mut manifest =
            BackupManifest::from_backup(&staging, database).map_err(|e| e.to_string())?;
        manifest.job_id = job.id.clone();
        manifest.excluded_collections = job.exclude_collections.clone();
        verify::write_manifest(&staging, &manifest).map_err(|e| e.to_string())?;
        if job.verify {
            verify::verify_backup(&staging).map_err(|e| format!("verify: {e}"))?;
        }

        if job.compression == Compression::Gzip {
            let compressed = staging.join("data.mdb.gz");
            backup_compress::gzip_file(&plain, &compressed).map_err(|e| e.to_string())?;
            std::fs::remove_file(&plain).map_err(|e| e.to_string())?;
            manifest.plain_sha256 = Some(manifest.sha256.clone());
            manifest.plain_size = Some(manifest.data_size);
            manifest.data_file = "data.mdb.gz".into();
            manifest.data_size = std::fs::metadata(&compressed)
                .map_err(|e| e.to_string())?
                .len();
            manifest.sha256 = verify::sha256_file(&compressed).map_err(|e| e.to_string())?;
            manifest.compression = Some(BackupCompression {
                kind: "gzip".into(),
            });
            verify::write_manifest(&staging, &manifest).map_err(|e| e.to_string())?;
        }

        if let Some(Encryption::Age { recipient_file }) = &job.encryption {
            let recipient = backup_crypto::read_key_file(Path::new(recipient_file))
                .map_err(|e| e.to_string())?;
            let source = staging.join(&manifest.data_file);
            let encrypted_name = format!("{}.age", manifest.data_file);
            let encrypted = staging.join(&encrypted_name);
            backup_crypto::encrypt_file(&source, &encrypted, &recipient)
                .map_err(|e| e.to_string())?;
            let enc_size = std::fs::metadata(&encrypted)
                .map_err(|e| e.to_string())?
                .len();
            let enc_sha = verify::sha256_file(&encrypted).map_err(|e| e.to_string())?;
            std::fs::remove_file(&source).map_err(|e| e.to_string())?;
            if manifest.plain_sha256.is_none() {
                manifest.plain_sha256 = Some(manifest.sha256.clone());
                manifest.plain_size = Some(manifest.data_size);
            }
            manifest.data_file = encrypted_name;
            manifest.data_size = enc_size;
            manifest.sha256 = enc_sha;
            manifest.encryption = Some(BackupEncryption { kind: "age".into() });
            verify::write_manifest(&staging, &manifest).map_err(|e| e.to_string())?;
        }
        backup_info::write(&staging, &manifest).map_err(|e| e.to_string())?;

        let payload = staging.join(&manifest.data_file);
        let destination = match &job.destination {
            Destination::Local { path } => {
                let root = PathBuf::from(path);
                let out = root.join(&job.id).join(stamp.to_string());
                let out = if multi { out.join(database) } else { out };
                std::fs::create_dir_all(&out)
                    .map_err(|e| format!("create backup destination {}: {e}", out.display()))?;
                for name in [&manifest.data_file, "manifest.json", backup_info::INFO_FILE] {
                    std::fs::copy(staging.join(name), out.join(name))
                        .map_err(|e| format!("copy backup to {}: {e}", out.display()))?;
                }
                out.display().to_string()
            }
            Destination::Restic { .. } => unreachable!("restic handled before manifest"),
            Destination::S3 {
                endpoint,
                bucket,
                region,
                prefix,
                access_key_env,
                secret_key_env,
                session_token_env,
            } => {
                let key_prefix = format!("{}/{}/{}", prefix.trim_matches('/'), job.id, stamp)
                    .trim_start_matches('/')
                    .to_string();
                let key_prefix = if multi {
                    path_join(&key_prefix, database)
                } else {
                    key_prefix
                };
                s3_put(
                    endpoint,
                    bucket,
                    region,
                    &path_join(&key_prefix, &manifest.data_file),
                    &payload,
                    access_key_env,
                    secret_key_env,
                    session_token_env.as_deref(),
                )
                .await?;
                s3_put(
                    endpoint,
                    bucket,
                    region,
                    &path_join(&key_prefix, "manifest.json"),
                    &staging.join("manifest.json"),
                    access_key_env,
                    secret_key_env,
                    session_token_env.as_deref(),
                )
                .await?;
                s3_put(
                    endpoint,
                    bucket,
                    region,
                    &path_join(&key_prefix, backup_info::INFO_FILE),
                    &staging.join(backup_info::INFO_FILE),
                    access_key_env,
                    secret_key_env,
                    session_token_env.as_deref(),
                )
                .await?;
                format!("s3://{bucket}/{key_prefix}")
            }
        };
        let _ = std::fs::remove_dir_all(&staging);
        Ok((manifest.data_size, destination))
    }

    fn prune_local(&self, dir: &Path, keep: usize) -> Result<(), String> {
        if keep == 0 {
            return Ok(());
        }
        let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
            .map_err(|e| format!("read {}: {e}", dir.display()))?
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| path.is_dir())
            .collect();
        entries.sort();
        let remove = entries.len().saturating_sub(keep);
        for path in entries.into_iter().take(remove) {
            std::fs::remove_dir_all(&path)
                .map_err(|e| format!("remove old backup {}: {e}", path.display()))?;
        }
        Ok(())
    }
}

fn filter_excluded(staging: &Path, excluded: &[String]) -> Result<(), String> {
    let data = staging.join("data.mdb");
    let filtered = staging.join("filtered.mdb");
    let work = staging.join("filter-work");
    let len = std::fs::metadata(&data).map_err(|e| e.to_string())?.len();
    let map_size = len
        .saturating_mul(2)
        .max(MAP_SIZE as u64)
        .min(usize::MAX as u64) as usize;
    let map_size = map_size.max(PAGE_SIZE);

    std::fs::create_dir_all(&work).map_err(|e| e.to_string())?;
    std::fs::rename(&data, work.join("data.mdb")).map_err(|e| e.to_string())?;
    {
        // Open the copied environment under a separate path so a later deep
        // verification can open the staging root with read-only options.
        let store = Store::open_with_map_size(&work, map_size).map_err(|e| e.to_string())?;
        for name in excluded {
            store.drop_collection(name).map_err(|e| e.to_string())?;
        }
        let file = store
            .copy_to_file(&filtered, CompactionOption::Enabled)
            .map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        let env = store.env().clone();
        drop(store);
        env.prepare_for_closing().wait();
    }
    std::fs::remove_dir_all(&work).map_err(|e| e.to_string())?;
    std::fs::rename(&filtered, &data).map_err(|e| e.to_string())?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn restic_backup(
    binary: &str,
    repository: &str,
    password_file: &str,
    job_id: &str,
    database: &str,
    keep_last: usize,
    verify_repo: bool,
    data_file: &Path,
    info_file: &Path,
) -> Result<(), String> {
    if !Path::new(password_file).is_file() {
        return Err(format!("missing restic password file {password_file}"));
    }
    let base = || {
        let mut command = Command::new(binary);
        command
            .arg("-r")
            .arg(repository)
            .env("RESTIC_PASSWORD_FILE", password_file);
        command
    };

    let check = base()
        .args(["cat", "config"])
        .output()
        .map_err(|e| format!("run restic: {e}"))?;
    if !check.status.success() {
        let init = base()
            .arg("init")
            .output()
            .map_err(|e| format!("run restic init: {e}"))?;
        if !init.status.success() {
            return Err(format!(
                "restic init failed: {}",
                String::from_utf8_lossy(&init.stderr).trim()
            ));
        }
    }

    let safe_database: String = database
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    let tag = format!("nonodb-job-{job_id}-{safe_database}");
    let backup = base()
        .arg("backup")
        .arg(data_file)
        .arg(info_file)
        .arg("--tag")
        .arg("nonodb")
        .arg("--tag")
        .arg(&tag)
        .output()
        .map_err(|e| format!("run restic backup: {e}"))?;
    if !backup.status.success() {
        return Err(format!(
            "restic backup failed: {}",
            String::from_utf8_lossy(&backup.stderr).trim()
        ));
    }

    if keep_last > 0 {
        let forget = base()
            .arg("forget")
            .arg("--tag")
            .arg(&tag)
            .arg("--keep-last")
            .arg(keep_last.to_string())
            .arg("--prune")
            .output()
            .map_err(|e| format!("run restic forget: {e}"))?;
        if !forget.status.success() {
            return Err(format!(
                "restic forget failed: {}",
                String::from_utf8_lossy(&forget.stderr).trim()
            ));
        }
    }

    if verify_repo {
        let check = base()
            .arg("check")
            .output()
            .map_err(|e| format!("run restic check: {e}"))?;
        if !check.status.success() {
            return Err(format!(
                "restic check failed: {}",
                String::from_utf8_lossy(&check.stderr).trim()
            ));
        }
    }
    Ok(())
}

fn path_join(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}/{name}")
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn sha256_hex(data: &[u8]) -> String {
    hex(ring::digest::digest(&ring::digest::SHA256, data).as_ref())
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let key = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&key, data).as_ref().to_vec()
}

fn uri_encode(value: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        let keep = byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_' | b'.' | b'~')
            || (!encode_slash && byte == b'/');
        if keep {
            out.push(byte as char);
        } else {
            use std::fmt::Write as _;
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
async fn s3_put(
    endpoint: &str,
    bucket: &str,
    region: &str,
    key: &str,
    file: &Path,
    access_key_env: &str,
    secret_key_env: &str,
    session_token_env: Option<&str>,
) -> Result<(), String> {
    let access = std::env::var(access_key_env)
        .map_err(|_| format!("missing S3 credential env `{access_key_env}`"))?;
    let secret = std::env::var(secret_key_env)
        .map_err(|_| format!("missing S3 credential env `{secret_key_env}`"))?;
    let session = session_token_env.and_then(|name| std::env::var(name).ok());
    let endpoint = Url::parse(endpoint).map_err(|e| format!("invalid S3 endpoint: {e}"))?;
    let host = match (endpoint.host_str(), endpoint.port()) {
        (Some(host), Some(port)) => format!("{host}:{port}"),
        (Some(host), None) => host.to_string(),
        _ => return Err("S3 endpoint has no host".into()),
    };
    let base = endpoint.path().trim_end_matches('/');
    let canonical_uri = format!("{}/{}/{}", base, bucket, uri_encode(key, false));
    let url = format!("{}://{}{}", endpoint.scheme(), host, canonical_uri);

    let now = time::OffsetDateTime::now_utc();
    let amz_date = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    );
    let date = amz_date[..8].to_string();
    let payload_hash = "UNSIGNED-PAYLOAD";
    let mut headers = vec![
        ("host".to_string(), host.clone()),
        ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    if let Some(token) = &session {
        headers.push(("x-amz-security-token".to_string(), token.clone()));
    }
    headers.sort_by(|a, b| a.0.cmp(&b.0));
    let canonical_headers = headers
        .iter()
        .map(|(key, value)| format!("{key}:{value}\n"))
        .collect::<String>();
    let signed_headers = headers
        .iter()
        .map(|(key, _)| key.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_request = format!(
        "PUT\n{}\n\n{}\n{}\n{}",
        canonical_uri, canonical_headers, signed_headers, payload_hash
    );
    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        scope,
        sha256_hex(canonical_request.as_bytes())
    );
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );

    let meta = std::fs::metadata(file).map_err(|e| format!("stat {}: {e}", file.display()))?;
    if meta.len() > 5 * 1024 * 1024 * 1024 {
        return Err(
            "S3 upload is limited to 5 GiB per object; split the backup or use multipart support"
                .into(),
        );
    }
    let file = tokio::fs::File::open(file)
        .await
        .map_err(|e| format!("open {}: {e}", file.display()))?;
    let stream = ReaderStream::new(file);
    let client = reqwest::Client::new();
    let mut request = client
        .put(&url)
        .header("host", host)
        .header("x-amz-content-sha256", payload_hash)
        .header("x-amz-date", amz_date)
        .header("authorization", authorization)
        .header("content-length", meta.len())
        .header("content-type", "application/octet-stream")
        .body(reqwest::Body::wrap_stream(stream));
    if let Some(token) = session {
        request = request.header("x-amz-security-token", token);
    }
    let response = request
        .send()
        .await
        .map_err(|e| format!("S3 upload: {e}"))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(format!("S3 upload failed: HTTP {status}: {body}"));
    }
    Ok(())
}
