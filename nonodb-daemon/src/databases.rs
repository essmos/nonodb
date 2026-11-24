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

use nonodb_core::Store;
use tokio::sync::{Mutex as AsyncMutex, RwLock};

use crate::auto_index::AutoIndexer;
use crate::cache::QueryCache;
use crate::protocol::ApiError;
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
    stop: Arc<AtomicBool>,
    writer: Mutex<Option<std::thread::JoinHandle<()>>>,
    maintenance: Mutex<Option<std::thread::JoinHandle<()>>>,
    webhooks: Mutex<Option<std::thread::JoinHandle<()>>>,
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
        if let Some(handle) = self.maintenance.lock().expect("maintenance lock").take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.webhooks.lock().expect("webhooks lock").take() {
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

    /// Compacts one database while it has no active request holding its runtime.
    ///
    /// The runtime is removed from the pool, its writer/maintenance threads are
    /// joined (which closes the LMDB environment), the file is compacted, and a
    /// fresh runtime is started. If a request already cloned the runtime, the
    /// function returns `Ok(None)` and leaves it untouched.
    pub async fn compact_idle_database(
        &self,
        name: &str,
    ) -> Result<Option<nonodb_core::store::CompactReport>, String> {
        validate_name(name)?;
        let _open_guard = self.open_lock.lock().await;
        let Some(runtime) = self.runtimes.write().await.remove(name) else {
            return Ok(None);
        };
        if Arc::strong_count(&runtime) > 1 {
            self.runtimes
                .write()
                .await
                .insert(name.to_string(), runtime);
            return Ok(None);
        }
        let env = runtime.store.env().clone();
        drop(runtime);
        env.prepare_for_closing().wait();
        let path = self.root.join(name);
        let report = crate::maintenance::compact_if_requested(&path);
        let startup = tokio::task::spawn_blocking({
            let root = self.root.clone();
            let name = name.to_string();
            move || start_runtime(&root, &name)
        })
        .await
        .map_err(|e| format!("database reopen join error: {e}"))?;
        let runtime = startup?;
        self.runtimes
            .write()
            .await
            .insert(name.to_string(), Arc::new(runtime));
        report
    }

    /// Watches for offline-compaction markers and compacts idle databases.
    pub fn start_online_compactor(self: &Arc<Self>) {
        let interval = std::env::var("NONODB_TS_ONLINE_COMPACT_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(30);
        if interval == 0 {
            return;
        }
        let databases = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
                let names: Vec<String> = databases.known.read().await.iter().cloned().collect();
                for name in names {
                    let marker = databases
                        .root
                        .join(&name)
                        .join(crate::maintenance::COMPACT_REQUEST_FILE);
                    if !marker.is_file() {
                        continue;
                    }
                    match databases.compact_idle_database(&name).await {
                        Ok(Some(report)) => tracing::info!(
                            database = %name,
                            before = report.before_bytes,
                            after = report.after_bytes,
                            "compacted idle database online"
                        ),
                        Ok(None) => {}
                        Err(error) => tracing::warn!(
                            database = %name,
                            error = %error,
                            "online compaction failed; keeping request marker"
                        ),
                    }
                }
            }
        });
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

fn start_runtime(root: &Path, name: &str) -> Result<DatabaseRuntime, String> {
    let path = root.join(name);
    fs::create_dir_all(&path).map_err(|e| format!("create database `{name}`: {e}"))?;
    if let Err(error) = crate::maintenance::compact_if_requested(&path) {
        tracing::warn!(
            database = name,
            path = %path.display(),
            error = %error,
            "deferred time-series compaction; keeping the request marker"
        );
    }
    let store = Arc::new(Store::open(&path).map_err(|e| format!("open database `{name}`: {e}"))?);
    let (write_tx, write_rx) = mpsc::channel::<WriteJob>();
    let cache = Arc::new(QueryCache::from_env());
    let writer_store = store.clone();
    let writer_cache = cache.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let writer_stop = stop.clone();
    let writer = std::thread::Builder::new()
        .name(format!("nonodb-writer-{name}"))
        .spawn(move || writer::run(writer_store, write_rx, writer_cache, writer_stop))
        .map_err(|e| format!("start writer for `{name}`: {e}"))?;
    let auto_index = AutoIndexer::start(store.clone(), write_tx.clone(), &path);
    let maintenance = crate::maintenance::start(
        store.clone(),
        write_tx.clone(),
        auto_index.clone(),
        path.clone(),
        stop.clone(),
    );
    let webhooks =
        crate::webhooks::start(store.clone(), write_tx.clone(), path.clone(), stop.clone());
    if let Some(dir) = std::env::var_os("NONODB_SNAPSHOT_DIR") {
        crate::spawn_snapshots(
            store.clone(),
            PathBuf::from(dir).join(name),
            name.to_string(),
        );
    }
    Ok(DatabaseRuntime {
        store,
        write_tx,
        cache,
        auto_index,
        stop,
        writer: Mutex::new(Some(writer)),
        maintenance: Mutex::new(Some(maintenance)),
        webhooks: Mutex::new(Some(webhooks)),
    })
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
    async fn online_compaction_waits_for_idle_runtime_and_preserves_data() {
        use serde_json::json;

        let dir = temp_root();
        let databases = Databases::open(dir.path()).unwrap();
        let runtime = databases.default_runtime().await.unwrap();
        let coll = runtime.store.collection("docs").unwrap();
        coll.insert_one(json!({"_id": "a", "v": 1})).unwrap();
        let marker = dir
            .path()
            .join(DEFAULT_DATABASE)
            .join(crate::maintenance::COMPACT_REQUEST_FILE);
        fs::write(&marker, b"1").unwrap();

        // A live request clone prevents the runtime swap.
        assert!(databases
            .compact_idle_database(DEFAULT_DATABASE)
            .await
            .unwrap()
            .is_none());
        assert!(marker.exists());
        assert_eq!(coll.count(&json!({})).unwrap(), 1);
        drop(coll);
        drop(runtime);

        let report = databases
            .compact_idle_database(DEFAULT_DATABASE)
            .await
            .unwrap();
        assert!(report.is_some());
        assert!(!marker.exists());
        let reopened = databases.default_runtime().await.unwrap();
        assert_eq!(
            reopened
                .store
                .collection("docs")
                .unwrap()
                .count(&json!({}))
                .unwrap(),
            1
        );
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
