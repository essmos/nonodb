//! Multi-database runtime: one LMDB environment and writer per open database.
//!
//! Databases are discovered from the data root and opened lazily. A bounded
//! number of environments stay open; idle environments without active requests
//! are evicted when another database needs to be opened.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::{mpsc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nonodb_core::Store;
use tokio::sync::{Mutex as AsyncMutex, RwLock};

use crate::analytics::AnalyticsManager;
use crate::auto_index::AutoIndexer;
use crate::cache::QueryCache;
use crate::protocol::ApiError;
use crate::quota::Quota;
use crate::writer::{self, WriteJob};

pub const DEFAULT_DATABASE: &str = "default";
const RESERVED: &[&str] = &["v1", "healthz", "metrics", "gui", "db", "api"];

fn invalid(message: impl Into<String>) -> ApiError {
    ApiError {
        code: "invalid_query".into(),
        message: message.into(),
    }
}

fn max_open_databases() -> usize {
    std::env::var("NONODB_MAX_OPEN_DATABASES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(64)
}

pub struct DatabaseRuntime {
    pub store: Arc<Store>,
    pub write_tx: mpsc::Sender<WriteJob>,
    pub cache: Arc<QueryCache>,
    pub auto_index: Arc<AutoIndexer>,
    pub quota: Arc<Quota>,
    pub analytics: Arc<AnalyticsManager>,
    stop: Arc<AtomicBool>,
    writer: Mutex<Option<std::thread::JoinHandle<()>>>,
    maintenance: Mutex<Option<std::thread::JoinHandle<()>>>,
    webhooks: Mutex<Option<std::thread::JoinHandle<()>>>,
    snapshots: Mutex<Option<std::thread::JoinHandle<()>>>,
    wal: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl DatabaseRuntime {
    /// True while the dedicated writer thread is still running.
    pub fn writer_alive(&self) -> bool {
        self.writer
            .lock()
            .ok()
            .and_then(|handle| handle.as_ref().map(|handle| !handle.is_finished()))
            .unwrap_or(false)
    }
}

impl Drop for DatabaseRuntime {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.analytics.shutdown();
        if let Some(handle) = self.maintenance.lock().expect("maintenance lock").take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.webhooks.lock().expect("webhooks lock").take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.snapshots.lock().expect("snapshots lock").take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.wal.lock().expect("wal lock").take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.writer.lock().expect("writer lock").take() {
            let _ = handle.join();
        }
    }
}

pub struct Databases {
    root: PathBuf,
    known: RwLock<HashSet<String>>,
    runtimes: RwLock<HashMap<String, Arc<DatabaseRuntime>>>,
    open_lock: AsyncMutex<()>,
    max_open: usize,
}

impl Databases {
    pub fn open(root: impl AsRef<Path>) -> Result<Arc<Self>, String> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|e| format!("create data root: {e}"))?;
        cleanup_dropped(&root);
        migrate_legacy(&root)?;
        let me = Arc::new(Self {
            root,
            known: RwLock::new(HashSet::new()),
            runtimes: RwLock::new(HashMap::new()),
            open_lock: AsyncMutex::new(()),
            max_open: max_open_databases(),
        });

        let mut known = HashSet::new();
        known.insert(DEFAULT_DATABASE.to_string());
        for entry in fs::read_dir(&me.root).map_err(|e| format!("read data root: {e}"))? {
            let entry = entry.map_err(|e| e.to_string())?;
            if !entry.file_type().map_err(|e| e.to_string())?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if validate_name(&name).is_ok() && entry.path().join("data.mdb").is_file() {
                known.insert(name);
            }
        }
        let default = start_runtime(&me.root, DEFAULT_DATABASE)?;
        let mut runtimes = HashMap::new();
        runtimes.insert(DEFAULT_DATABASE.to_string(), Arc::new(default));
        *me.known.try_write().map_err(|e| e.to_string())? = known;
        *me.runtimes.try_write().map_err(|e| e.to_string())? = runtimes;
        Ok(me)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub async fn get(&self, name: &str) -> Result<Arc<DatabaseRuntime>, ApiError> {
        validate_name(name).map_err(invalid)?;
        if let Some(runtime) = self.runtimes.read().await.get(name).cloned() {
            return Ok(runtime);
        }

        let _open_guard = self.open_lock.lock().await;
        if let Some(runtime) = self.runtimes.read().await.get(name).cloned() {
            return Ok(runtime);
        }
        if !self.known.read().await.contains(name) {
            return Err(ApiError {
                code: "not_found".into(),
                message: format!("database `{name}` does not exist"),
            });
        }

        let evicted = {
            let mut guard = self.runtimes.write().await;
            let mut evicted = Vec::new();
            if guard.len() >= self.max_open {
                let candidates: Vec<String> = guard
                    .iter()
                    .filter(|(candidate, runtime)| {
                        candidate.as_str() != name && Arc::strong_count(runtime) == 1
                    })
                    .map(|(candidate, _)| candidate.clone())
                    .collect();
                for candidate in candidates {
                    if guard.len() < self.max_open {
                        break;
                    }
                    if let Some(runtime) = guard.remove(&candidate) {
                        evicted.push(runtime);
                    }
                }
            }
            if guard.len() >= self.max_open {
                return Err(ApiError {
                    code: "database_limit".into(),
                    message: format!(
                        "too many open databases (limit {}); set NONODB_MAX_OPEN_DATABASES or close idle clients",
                        self.max_open
                    ),
                });
            }
            evicted
        };
        drop(evicted);

        let root = self.root.clone();
        let name_owned = name.to_string();
        let name_for_open = name_owned.clone();
        let runtime = tokio::task::spawn_blocking(move || start_runtime(&root, &name_for_open))
            .await
            .map_err(|e| ApiError::internal(format!("database open join error: {e}")))?
            .map_err(ApiError::internal)?;
        let runtime = Arc::new(runtime);
        self.runtimes
            .write()
            .await
            .insert(name_owned, runtime.clone());
        Ok(runtime)
    }

    pub async fn default_runtime(&self) -> Result<Arc<DatabaseRuntime>, ApiError> {
        self.get(DEFAULT_DATABASE).await
    }

    pub async fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.known.read().await.iter().cloned().collect();
        names.sort();
        names
    }

    pub async fn create(&self, name: &str) -> Result<(), ApiError> {
        validate_name(name).map_err(invalid)?;
        if name == DEFAULT_DATABASE {
            self.default_runtime().await?;
            return Ok(());
        }
        let mut known = self.known.write().await;
        if known.contains(name) {
            return Err(invalid(format!("database `{name}` already exists")));
        }
        let path = self.root.join(name);
        fs::create_dir_all(&path)
            .map_err(|e| ApiError::internal(format!("create database directory: {e}")))?;
        // Initialize data.mdb so the database survives a restart even if it was
        // never queried after creation.
        Store::open(&path).map_err(|e| ApiError::internal(format!("create database: {e}")))?;
        known.insert(name.to_string());
        Ok(())
    }

    /// Installs a verified `data.mdb` as a new database without replacing or
    /// opening a live environment. The caller must verify the file first.
    pub async fn import_database(&self, name: &str, source: &Path) -> Result<(), ApiError> {
        validate_name(name).map_err(invalid)?;
        let _guard = self.open_lock.lock().await;
        if self.known.read().await.contains(name) {
            return Err(invalid(format!("database `{name}` already exists")));
        }
        let target = self.root.join(name);
        if target.exists() {
            return Err(invalid(format!("database `{name}` already exists")));
        }
        let staging = self
            .root
            .join(format!(".import-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&staging);
        fs::create_dir_all(&staging)
            .map_err(|e| ApiError::internal(format!("create import staging: {e}")))?;
        let staging_data = staging.join("data.mdb");
        fs::copy(source, &staging_data)
            .map_err(|e| ApiError::internal(format!("copy imported data: {e}")))?;
        let result = tokio::task::spawn_blocking({
            let staging = staging.clone();
            move || -> Result<(), String> {
                let store = Store::open(&staging).map_err(|e| e.to_string())?;
                for collection in store.collections().map_err(|e| e.to_string())? {
                    store
                        .collection(&collection)
                        .map_err(|e| e.to_string())?
                        .rebuild_missing_search_sidecars()
                        .map_err(|e| format!("rebuild imported search sidecars: {e}"))?;
                }
                let env = store.env().clone();
                drop(store);
                env.prepare_for_closing().wait();
                let _ = fs::remove_file(staging.join("lock.mdb"));
                Ok(())
            }
        })
        .await
        .map_err(|e| ApiError::internal(format!("import validation task: {e}")))?;
        if let Err(error) = result {
            let _ = fs::remove_dir_all(&staging);
            return Err(ApiError::internal(format!(
                "open imported database: {error}"
            )));
        }
        if let Err(error) = fs::rename(&staging, &target) {
            let _ = fs::remove_dir_all(&staging);
            return Err(ApiError::internal(format!(
                "install imported database: {error}"
            )));
        }
        self.known.write().await.insert(name.to_string());
        Ok(())
    }

    /// Replace a database with a verified native backup while the daemon is
    /// running. The previous directory is kept as a restore safety snapshot.
    /// Only non-default databases are supported because the default runtime is
    /// permanently owned by the instance state.
    pub async fn restore_database(
        &self,
        name: &str,
        source: &Path,
        source_label: &str,
        location: &str,
    ) -> Result<RestoreOutcome, ApiError> {
        validate_name(name).map_err(invalid)?;
        if name == DEFAULT_DATABASE {
            return Err(invalid(
                "restore-in-place does not support the default database; stop the daemon and use nonodb-cli restore",
            ));
        }
        let _open_guard = self.open_lock.lock().await;
        if !self.known.read().await.contains(name) {
            return Err(ApiError {
                code: "not_found".into(),
                message: format!("database `{name}` does not exist"),
            });
        }

        let runtime = self.runtimes.write().await.remove(name);
        if let Some(runtime) = runtime {
            let deadline = Instant::now() + Duration::from_secs(30);
            while Arc::strong_count(&runtime) > 1 {
                if Instant::now() >= deadline {
                    self.runtimes
                        .write()
                        .await
                        .insert(name.to_string(), runtime);
                    return Err(ApiError {
                        code: "database_busy".into(),
                        message: format!(
                            "database `{name}` still has active requests; close clients and retry"
                        ),
                    });
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            drop(runtime);
        }

        let path = self.root.join(name);
        let safety_root = self.root.join("restore-safety");
        fs::create_dir_all(&safety_root)
            .map_err(|error| ApiError::internal(format!("create restore safety root: {error}")))?;
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0);
        let safety = safety_root.join(format!("{name}-{stamp}-{}", std::process::id()));
        let had_live = path.exists();
        if had_live {
            fs::rename(&path, &safety).map_err(|error| {
                ApiError::internal(format!(
                    "move live database to safety snapshot {}: {error}",
                    safety.display()
                ))
            })?;
        }

        let source_owned = source.to_path_buf();
        let root = self.root.clone();
        let name_owned = name.to_string();
        let safety_for_install = safety.clone();
        let installed = tokio::task::spawn_blocking(move || {
            install_restored(
                &root,
                &name_owned,
                &source_owned,
                safety_for_install.as_path(),
            )
        })
        .await
        .map_err(|error| ApiError::internal(format!("restore install task: {error}")))?;

        match installed {
            Ok(runtime) => {
                self.runtimes
                    .write()
                    .await
                    .insert(name.to_string(), Arc::new(runtime));
                let info = serde_json::json!({
                    "version": 1,
                    "database": name,
                    "source": source_label,
                    "location": location,
                    "restored_at_ms": stamp,
                    "safety_snapshot": safety,
                });
                if let Err(error) = fs::write(
                    safety.join("restore-info.json"),
                    serde_json::to_vec_pretty(&info).unwrap_or_default(),
                ) {
                    tracing::warn!(
                        path = %safety.display(),
                        error = %error,
                        "failed to write restore safety metadata"
                    );
                }
                tracing::info!(
                    database = name,
                    source = source_label,
                    location,
                    safety = %safety.display(),
                    "database restored in place"
                );
                Ok(RestoreOutcome {
                    database: name.to_string(),
                    safety_snapshot: safety,
                    source: source_label.to_string(),
                })
            }
            Err(install_error) => {
                let path_for_rollback = path.clone();
                let safety_for_rollback = safety.clone();
                let rollback = tokio::task::spawn_blocking(move || -> Result<(), String> {
                    let _ = fs::remove_dir_all(&path_for_rollback);
                    if had_live {
                        fs::rename(&safety_for_rollback, &path_for_rollback)
                            .map_err(|error| format!("restore previous database: {error}"))?;
                    }
                    Ok(())
                })
                .await
                .map_err(|error| ApiError::internal(format!("restore rollback task: {error}")))?;
                if let Err(rollback_error) = rollback {
                    return Err(ApiError::internal(format!(
                        "restore failed ({install_error}) and rollback failed ({rollback_error}); recovery data is at {}",
                        safety.display()
                    )));
                }
                // Reopen the previous database so the instance remains usable.
                let root = self.root.clone();
                let name_owned = name.to_string();
                let reopened =
                    tokio::task::spawn_blocking(move || start_runtime(&root, &name_owned))
                        .await
                        .map_err(|error| {
                            ApiError::internal(format!("restore reopen task: {error}"))
                        })?;
                match reopened {
                    Ok(runtime) => {
                        self.runtimes
                            .write()
                            .await
                            .insert(name.to_string(), Arc::new(runtime));
                    }
                    Err(error) => {
                        return Err(ApiError::internal(format!(
                            "restore failed ({install_error}); rollback data was restored but reopening failed: {error}"
                        )));
                    }
                }
                Err(ApiError::internal(format!(
                    "restore failed: {install_error}; previous database was restored"
                )))
            }
        }
    }

    pub async fn drop_database(&self, name: &str) -> Result<bool, ApiError> {
        validate_name(name).map_err(invalid)?;
        if name == DEFAULT_DATABASE {
            return Err(invalid("cannot drop the default database"));
        }
        let removed = self.runtimes.write().await.remove(name);
        if let Some(runtime) = removed.as_ref() {
            if Arc::strong_count(runtime) > 1 {
                self.runtimes
                    .write()
                    .await
                    .insert(name.to_string(), runtime.clone());
                return Err(invalid(format!(
                    "database `{name}` is in use; close its clients and retry"
                )));
            }
        }
        let was_known = self.known.write().await.remove(name);
        let Some(runtime) = removed else {
            if !was_known {
                return Ok(false);
            }
            let path = self.root.join(name);
            if path.exists() {
                fs::remove_dir_all(&path)
                    .map_err(|e| ApiError::internal(format!("drop database remove: {e}")))?;
            }
            return Ok(true);
        };
        drop(runtime);
        let path = self.root.join(name);
        let trash = self
            .root
            .join(format!(".dropped-{name}-{}", std::process::id()));
        fs::rename(&path, &trash)
            .map_err(|e| ApiError::internal(format!("drop database rename: {e}")))?;
        // The live directory is already gone. A failed cleanup must not make the
        // drop look unsuccessful; the next startup will remove the trash.
        if let Err(error) = fs::remove_dir_all(&trash) {
            tracing::warn!(
                path = %trash.display(),
                error = %error,
                "deferred removed database cleanup"
            );
        }
        Ok(true)
    }
}

fn parse_pitr_target(raw: &str) -> Result<u64, String> {
    let raw = raw.trim();
    if let Ok(value) = raw.parse::<u64>() {
        return Ok(if value < 100_000_000_000 {
            value.saturating_mul(1000)
        } else {
            value
        });
    }
    let ms = nonodb_core::date::parse_rfc3339_ms(raw)
        .ok_or_else(|| format!("invalid NONODB_PITR_REPLAY_TO: {raw}"))?;
    u64::try_from(ms).map_err(|_| format!("NONODB_PITR_REPLAY_TO is before the Unix epoch: {raw}"))
}

fn start_runtime(root: &Path, name: &str) -> Result<DatabaseRuntime, String> {
    let path = root.join(name);
    fs::create_dir_all(&path).map_err(|e| format!("create database `{name}`: {e}"))?;
    let store = Arc::new(Store::open(&path).map_err(|e| format!("open database `{name}`: {e}"))?);
    if let Some(raw) = std::env::var_os("NONODB_PITR_REPLAY_TO") {
        let raw = raw.to_string_lossy();
        let target = parse_pitr_target(&raw)?;
        let wal_dir = crate::wal::dir(&path);
        let report = nonodb_core::pitr::replay_to(&store, &wal_dir, target)
            .map_err(|e| format!("PITR startup replay for `{name}`: {e}"))?;
        tracing::info!(
            database = name,
            target,
            applied = report.applied,
            last_seq = report.last_replayed_seq,
            "PITR startup replay completed"
        );
    }
    let quota =
        Arc::new(Quota::load(name, &path).map_err(|e| format!("load quota for `{name}`: {e}"))?);
    let (write_tx, write_rx) = mpsc::channel::<WriteJob>();
    let cache = Arc::new(QueryCache::from_env());
    let writer_store = store.clone();
    let writer_cache = cache.clone();
    let writer_quota = quota.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let writer_stop = stop.clone();
    let writer = std::thread::Builder::new()
        .name(format!("nonodb-writer-{name}"))
        .spawn(move || {
            writer::run(
                writer_store,
                write_rx,
                writer_cache,
                writer_quota,
                writer_stop,
            )
        })
        .map_err(|e| format!("start writer for `{name}`: {e}"))?;
    let analytics = AnalyticsManager::new(name, stop.clone(), write_tx.clone());
    let auto_index = AutoIndexer::start(store.clone(), write_tx.clone(), &path);
    let maintenance = crate::maintenance::start(
        store.clone(),
        write_tx.clone(),
        auto_index.clone(),
        stop.clone(),
    );
    let webhooks =
        crate::webhooks::start(store.clone(), write_tx.clone(), path.clone(), stop.clone());
    let wal = crate::wal::start(store.clone(), path.clone(), name.to_string(), stop.clone());
    let snapshot_dir = std::env::var_os("NONODB_SNAPSHOT_DIR")
        .map(|dir| PathBuf::from(dir).join(name))
        .or_else(|| crate::wal::enabled().then(|| path.join("snapshots")));
    let snapshots = snapshot_dir.and_then(|dir| {
        crate::spawn_snapshots(
            store.clone(),
            dir,
            crate::wal::dir(&path),
            name.to_string(),
            stop.clone(),
        )
    });
    Ok(DatabaseRuntime {
        store,
        write_tx,
        cache,
        auto_index,
        quota,
        analytics,
        stop,
        writer: Mutex::new(Some(writer)),
        maintenance: Mutex::new(Some(maintenance)),
        webhooks: Mutex::new(Some(webhooks)),
        snapshots: Mutex::new(snapshots),
        wal: Mutex::new(wal),
    })
}

#[derive(Debug, Clone)]
pub struct RestoreOutcome {
    pub database: String,
    pub safety_snapshot: PathBuf,
    pub source: String,
}

fn install_restored(
    root: &Path,
    name: &str,
    source: &Path,
    safety: &Path,
) -> Result<DatabaseRuntime, String> {
    let path = root.join(name);
    fs::create_dir_all(&path).map_err(|error| format!("create restored database: {error}"))?;
    fs::copy(source, path.join("data.mdb"))
        .map_err(|error| format!("install restored data.mdb: {error}"))?;
    // Operational settings that live beside data.mdb should survive a restore.
    let quota = safety.join("quota.json");
    if quota.is_file() {
        fs::copy(&quota, path.join("quota.json"))
            .map_err(|error| format!("preserve restore quota config: {error}"))?;
    }

    let rebuild_path = path.clone();
    let store =
        Store::open(&rebuild_path).map_err(|error| format!("open restored data: {error}"))?;
    for collection in store
        .collections()
        .map_err(|error| format!("list restored collections: {error}"))?
    {
        store
            .collection(&collection)
            .map_err(|error| format!("open restored collection `{collection}`: {error}"))?
            .rebuild_missing_search_sidecars()
            .map_err(|error| format!("rebuild restored search sidecars: {error}"))?;
    }
    let env = store.env().clone();
    drop(store);
    env.prepare_for_closing().wait();
    let _ = fs::remove_file(path.join("lock.mdb"));

    start_runtime(root, name)
}

fn cleanup_dropped(root: &Path) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with(".dropped-") {
            continue;
        }
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if let Err(error) = fs::remove_dir_all(&path) {
            tracing::warn!(
                path = %path.display(),
                error = %error,
                "failed to clean up removed database directory"
            );
        }
    }
}

fn migrate_legacy(root: &Path) -> Result<(), String> {
    let legacy = root.join("data.mdb");
    if !legacy.is_file() {
        return Ok(());
    }
    let default = root.join(DEFAULT_DATABASE);
    if default.join("data.mdb").exists() {
        return Err(format!(
            "both legacy {} and {}/data.mdb exist; refusing to guess the default database",
            legacy.display(),
            default.display()
        ));
    }
    fs::create_dir_all(&default).map_err(|e| format!("create default database: {e}"))?;
    fs::rename(&legacy, default.join("data.mdb"))
        .map_err(|e| format!("move legacy data.mdb: {e}"))?;
    let legacy_lock = root.join("lock.mdb");
    if legacy_lock.exists() {
        fs::rename(&legacy_lock, default.join("lock.mdb"))
            .map_err(|e| format!("move legacy lock.mdb: {e}"))?;
    }
    let legacy_auto_index = root.join("auto-index.json");
    if legacy_auto_index.exists() {
        fs::rename(&legacy_auto_index, default.join("auto-index.json"))
            .map_err(|e| format!("move legacy auto-index.json: {e}"))?;
    }
    Ok(())
}

pub fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 64 {
        return Err("database name must be 1..=64 characters".into());
    }
    if RESERVED.contains(&name) {
        return Err(format!("database name `{name}` is reserved"));
    }
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err("database name is empty".into());
    };
    if !first.is_ascii_alphanumeric() {
        return Err("database name must start with an ASCII letter or digit".into());
    }
    if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-') {
        return Err("database name may contain only letters, digits, _ and -".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root() -> tempfile::TempDir {
        tempfile::tempdir().expect("temp dir")
    }

    #[tokio::test]
    async fn open_removes_orphaned_dropped_directories() {
        let dir = temp_root();
        let trash = dir.path().join(".dropped-old-1234");
        fs::create_dir_all(&trash).unwrap();
        fs::write(trash.join("data.mdb"), b"stale").unwrap();
        fs::write(trash.join("lock.mdb"), b"stale").unwrap();

        let databases = Databases::open(dir.path()).unwrap();
        assert!(!trash.exists(), "startup cleanup left {}", trash.display());
        drop(databases);
    }

    #[tokio::test]
    async fn drop_removes_database_without_leaving_trash() {
        let dir = temp_root();
        let databases = Databases::open(dir.path()).unwrap();
        databases.create("scratch").await.unwrap();
        databases.get("scratch").await.unwrap();
        assert!(databases.drop_database("scratch").await.unwrap());

        assert!(!dir.path().join("scratch").exists());
        for entry in fs::read_dir(dir.path()).unwrap() {
            let name = entry.unwrap().file_name();
            assert!(
                !name.to_string_lossy().starts_with(".dropped-"),
                "left trash {}",
                name.to_string_lossy()
            );
        }
        drop(databases);
    }
}
