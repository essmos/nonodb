//! Node-local hidden metadata store.
//!
//! Replication role, source, vote, lease, and peer-token state live in a small
//! LMDB environment under `<NONODB_DATA>/.node`, not in loose JSON files. This
//! mirrors how MongoDB keeps replica-set state in a system collection and makes
//! updates atomic and crash-safe. The environment is node-local and is never
//! replicated or copied by database snapshots.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use nonodb_core::heed::types::Bytes;
use nonodb_core::heed::{Database, Env, EnvOpenOptions};
use serde::{de::DeserializeOwned, Serialize};

const STATE_DIR: &str = ".node";
const STATE_DB: &str = "meta";
const MAP_SIZE: usize = 4 * 1024 * 1024;
const MAX_DBS: u32 = 4;

static GLOBAL: OnceLock<Arc<NodeState>> = OnceLock::new();

struct NodeState {
    _env: Env,
    db: Database<Bytes, Bytes>,
    write_lock: Mutex<()>,
}

pub fn open(data_root: &Path) -> Result<(), String> {
    let path = data_root.join(STATE_DIR);
    std::fs::create_dir_all(&path)
        .map_err(|error| format!("create node state dir {}: {error}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700));
    }
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(MAP_SIZE)
            .max_dbs(MAX_DBS)
            .open(&path)
            .map_err(|error| format!("open node state {}: {error}", path.display()))?
    };
    let _ = env.clear_stale_readers();
    let mut wtxn = env.write_txn().map_err(|error| error.to_string())?;
    let db: Database<Bytes, Bytes> = env
        .create_database(&mut wtxn, Some(STATE_DB))
        .map_err(|error| error.to_string())?;
    wtxn.commit().map_err(|error| error.to_string())?;
    let state = Arc::new(NodeState {
        _env: env,
        db,
        write_lock: Mutex::new(()),
    });
    let _ = GLOBAL.set(state);
    Ok(())
}

fn global() -> Result<&'static Arc<NodeState>, String> {
    GLOBAL
        .get()
        .ok_or_else(|| "node state is not initialized".to_string())
}

pub fn get(key: &str) -> Result<Option<Vec<u8>>, String> {
    let state = global()?.clone();
    let rtxn = state._env.read_txn().map_err(|error| error.to_string())?;
    let value = state
        .db
        .get(&rtxn, key.as_bytes())
        .map_err(|error| error.to_string())?
        .map(|bytes| bytes.to_vec());
    rtxn.commit().map_err(|error| error.to_string())?;
    Ok(value)
}

pub fn put(key: &str, value: &[u8]) -> Result<(), String> {
    let state = global()?.clone();
    let _guard = state
        .write_lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut wtxn = state._env.write_txn().map_err(|error| error.to_string())?;
    state
        .db
        .put(&mut wtxn, key.as_bytes(), value)
        .map_err(|error| error.to_string())?;
    wtxn.commit().map_err(|error| error.to_string())
}

pub fn remove(key: &str) -> Result<(), String> {
    let state = global()?.clone();
    let _guard = state
        .write_lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut wtxn = state._env.write_txn().map_err(|error| error.to_string())?;
    state
        .db
        .delete(&mut wtxn, key.as_bytes())
        .map_err(|error| error.to_string())?;
    wtxn.commit().map_err(|error| error.to_string())
}

pub fn get_json<T: DeserializeOwned>(key: &str) -> Result<Option<T>, String> {
    match get(key)? {
        Some(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| format!("parse node state `{key}`: {error}")),
        None => Ok(None),
    }
}

pub fn put_json<T: Serialize + ?Sized>(key: &str, value: &T) -> Result<(), String> {
    let bytes =
        serde_json::to_vec(value).map_err(|error| format!("encode node state `{key}`: {error}"))?;
    put(key, &bytes)
}

/// Legacy JSON paths used by 0.23.0 and earlier. They are read once for
/// migration by the replication layer.
pub fn legacy_path(data_root: &Path, name: &str) -> PathBuf {
    data_root.join(name)
}
