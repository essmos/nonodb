//! Opening LMDB and managing collections.

use std::path::Path;

use heed::types::Bytes;
use heed::{Database, Env, EnvOpenOptions, RoTxn, RwTxn};

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::history::{self, HistoryConfig, HISTORY_DB};

/// Maximum database file size. This is mmap address space - **not**
/// a disk or RAM reservation. The file grows on demand up to this limit.
pub const MAP_SIZE: usize = 1024 * 1024 * 1024; // 1 GiB

/// Map size from `NONODB_MAP_SIZE` (e.g. `4GiB`, `2048MiB`, bytes).
/// `map_size` is only an address-space reservation - not disk or RAM.
fn configured_map_size() -> usize {
    std::env::var("NONODB_MAP_SIZE")
        .ok()
        .and_then(|raw| parse_size(&raw))
        .filter(|&n| n >= PAGE_SIZE)
        .unwrap_or(MAP_SIZE)
}

fn parse_size(raw: &str) -> Option<usize> {
    let s = raw.trim().to_ascii_lowercase();
    const UNITS: [(&str, u64); 6] = [
        ("gib", 1 << 30),
        ("gb", 1_000_000_000),
        ("mib", 1 << 20),
        ("mb", 1_000_000),
        ("kib", 1 << 10),
        ("kb", 1_000),
    ];
    for (suffix, mult) in UNITS {
        if let Some(num) = s.strip_suffix(suffix) {
            let value: f64 = num.trim().parse().ok()?;
            let bytes = value * mult as f64;
            return (bytes >= 1.0 && bytes <= usize::MAX as f64).then_some(bytes as usize);
        }
    }
    s.parse().ok()
}
/// Default LMDB page size (4 KiB).
pub const PAGE_SIZE: usize = 4096;
/// Maximum number of sub-databases (collections + meta + indexes).
pub const MAX_DBS: u32 = 256;
/// Metadata sub-database name.
pub const META_DB: &str = "meta";
/// Data format version (for future migrations).
pub const SCHEMA_VERSION: u32 = 1;
/// Soft document size limit (serialized JSON).
/// LMDB technically supports larger values (overflow pages), but a large
/// document means an expensive update (the whole value is rewritten).
pub const MAX_DOCUMENT_BYTES: usize = 1024 * 1024;

/// Database handle. One `Store` = one `Env` = one LMDB file.
pub struct Store {
    env: Env,
    history: Database<Bytes, Bytes>,
    history_config: HistoryConfig,
}

impl Store {
    /// Opens (or creates) the database in the `path` directory.
    ///
    /// History is disabled by default for library users. The daemon enables it
    /// with the `NONODB_HISTORY` environment setting.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_history(path, HistoryConfig::default())
    }

    /// Opens the database with explicit history settings.
    pub fn open_with_history(
        path: impl AsRef<Path>,
        history_config: HistoryConfig,
    ) -> Result<Self> {
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;

        // SAFETY: the file is not modified outside LMDB and is not opened
        // concurrently with another `map_size` (the daemon is the sole owner).
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(configured_map_size())
                .max_dbs(MAX_DBS)
                .open(path)?
        };

        // After `kill -9`, dead reader slots may remain in the lock table.
        let _ = env.clear_stale_readers();
        let history = Self::init_meta(&env)?;
        Ok(Self {
            env,
            history,
            history_config,
        })
    }

    /// Current history settings.
    pub fn history_config(&self) -> HistoryConfig {
        self.history_config
    }

    /// Deletes old revisions above the configured per-document limit.
    ///
    /// Call only from the writer thread. This does a bounded scan and is meant
    /// to run periodically, not as part of every write.
    pub fn prune_history(&self, limit: usize) -> Result<usize> {
        if !self.history_config.active() {
            return Ok(0);
        }
        let mut wtxn = self.write_txn()?;
        let meta = self
            .env
            .open_database::<Bytes, Bytes>(&wtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        let deleted = history::prune(
            &self.history,
            &meta,
            &mut wtxn,
            self.history_config.max_versions,
            limit,
        )?;
        wtxn.commit()?;
        Ok(deleted)
    }

    pub fn env(&self) -> &Env {
        &self.env
    }

    pub fn read_txn(&self) -> Result<RoTxn<'_>> {
        Ok(self.env.read_txn()?)
    }

    pub fn write_txn(&self) -> Result<RwTxn<'_>> {
        Ok(self.env.write_txn()?)
    }

    fn init_meta(env: &Env) -> Result<Database<Bytes, Bytes>> {
        let mut wtxn = env.write_txn()?;
        let meta: Database<Bytes, Bytes> = env.create_database(&mut wtxn, Some(META_DB))?;
        if meta.get(&wtxn, b"schema_version")?.is_none() {
            meta.put(
                &mut wtxn,
                b"schema_version",
                SCHEMA_VERSION.to_string().as_bytes(),
            )?;
        }
        let history: Database<Bytes, Bytes> = env.create_database(&mut wtxn, Some(HISTORY_DB))?;
        wtxn.commit()?;
        Ok(history)
    }

    fn validate_name(name: &str) -> Result<()> {
        if name.is_empty() || name.len() > 200 || name.contains('\0') {
            return Err(Error::InvalidCollectionName(name.to_string()));
        }
        Ok(())
    }

    /// Opens a collection; creates it if it does not exist.
    ///
    /// An existing collection is opened with a read-only transaction.
    pub fn collection(&self, name: &str) -> Result<Collection<'_>> {
        Self::validate_name(name)?;

        // Fast path: the collection already exists - read only.
        {
            let rtxn = self.read_txn()?;
            if let Some(db) = self.env.open_database::<Bytes, Bytes>(&rtxn, Some(name))? {
                let meta = self
                    .env
                    .open_database::<Bytes, Bytes>(&rtxn, Some(META_DB))?
                    .ok_or_else(|| Error::Query("meta database missing".into()))?;
                // NOTE: heed requires `commit()` on the read transaction in which
                // the database was opened; otherwise later `db` use fails with EINVAL.
                rtxn.commit()?;
                return Ok(self.wrap(name, db, meta));
            }
            rtxn.commit()?;
        }

        // Slow path: first use - create the collection.
        let mut wtxn = self.write_txn()?;
        let collection = self.collection_in(&mut wtxn, name)?;
        wtxn.commit()?;
        Ok(collection)
    }

    /// Opens/creates a collection within an **existing** write transaction.
    /// Used by the writer task (group commit) to avoid opening a nested
    /// transaction.
    pub fn collection_in<'e>(&'e self, wtxn: &mut RwTxn<'_>, name: &str) -> Result<Collection<'e>> {
        Self::validate_name(name)?;
        let db = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(name))?;
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        let key = format!("collection:{name}");
        if meta.get(&*wtxn, key.as_bytes())?.is_none() {
            meta.put(&mut *wtxn, key.as_bytes(), b"1")?;
        }
        Ok(self.wrap(name, db, meta))
    }

    /// Opens an **existing** collection in the given read transaction.
    /// Returns `None` if the collection does not exist (without creating it).
    pub fn collection_at<'e>(
        &'e self,
        rtxn: &RoTxn<'_>,
        name: &str,
    ) -> Result<Option<Collection<'e>>> {
        Self::validate_name(name)?;
        let Some(db) = self.env.open_database::<Bytes, Bytes>(rtxn, Some(name))? else {
            return Ok(None);
        };
        let meta = self
            .env
            .open_database::<Bytes, Bytes>(rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        Ok(Some(self.wrap(name, db, meta)))
    }

    fn wrap<'e>(
        &'e self,
        name: &str,
        db: Database<Bytes, Bytes>,
        meta: Database<Bytes, Bytes>,
    ) -> Collection<'e> {
        Collection::new(
            &self.env,
            db,
            meta,
            self.history,
            self.history_config,
            name.to_string(),
        )
    }

    /// List of registered collections.
    pub fn collections(&self) -> Result<Vec<String>> {
        let rtxn = self.read_txn()?;
        let out = self.collections_in(&rtxn)?;
        rtxn.commit()?;
        Ok(out)
    }

    /// List of collections in the given read transaction.
    pub fn collections_in(&self, rtxn: &RoTxn<'_>) -> Result<Vec<String>> {
        let meta: Database<Bytes, Bytes> = self
            .env
            .open_database(rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        let mut out = Vec::new();
        for item in meta.iter(rtxn)? {
            let (key, _) = item?;
            if let Ok(k) = std::str::from_utf8(key) {
                if let Some(name) = k.strip_prefix("collection:") {
                    out.push(name.to_string());
                }
            }
        }
        out.sort();
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::parse_size;

    #[test]
    fn parses_map_sizes() {
        assert_eq!(parse_size("4GiB"), Some(4 * (1 << 30)));
        assert_eq!(parse_size("2048MiB"), Some(2048 * (1 << 20)));
        assert_eq!(parse_size("512mb"), Some(512_000_000));
        assert_eq!(parse_size("1073741824"), Some(1 << 30));
        assert_eq!(parse_size("nonsense"), None);
    }
}
