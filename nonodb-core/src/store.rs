//! Opening LMDB and managing collections.

use std::path::Path;

use heed::types::Bytes;
use heed::{Database, Env, EnvOpenOptions, RoTxn, RwTxn};

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::history::{self, HistoryConfig, HISTORY_DB};
use crate::ttl::{self, TTL_DB};
use crate::vault::CollectionProfile;

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
    ttl: Database<Bytes, Bytes>,
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
        let (history, ttl) = Self::init_meta(&env)?;
        Ok(Self {
            env,
            history,
            ttl,
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
        let mut wtxn = self.write_txn()?;
        let deleted = self.prune_history_in(&mut wtxn, limit)?;
        wtxn.commit()?;
        Ok(deleted)
    }

    /// History pruning inside an existing writer transaction.
    pub fn prune_history_in(&self, wtxn: &mut RwTxn<'_>, limit: usize) -> Result<usize> {
        if !self.history_config.active() {
            return Ok(0);
        }
        let meta = self
            .env
            .open_database::<Bytes, Bytes>(wtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        history::prune(
            &self.history,
            &meta,
            wtxn,
            self.history_config.max_versions,
            limit,
        )
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

    fn init_meta(env: &Env) -> Result<(Database<Bytes, Bytes>, Database<Bytes, Bytes>)> {
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
        let ttl: Database<Bytes, Bytes> = env.create_database(&mut wtxn, Some(TTL_DB))?;
        wtxn.commit()?;
        Ok((history, ttl))
    }

    fn validate_name(name: &str) -> Result<()> {
        if name.is_empty() || name.len() > 200 || name.contains('\0') {
            return Err(Error::InvalidCollectionName(name.to_string()));
        }
        Ok(())
    }

    fn profile_key(name: &str) -> String {
        format!("profile:{name}")
    }

    fn ttl_key(name: &str) -> String {
        format!("ttl:{name}")
    }

    fn ttl_purge_key(name: &str) -> String {
        format!("ttl_purge:{name}")
    }

    fn ttl_from_ro(
        meta: Database<Bytes, Bytes>,
        rtxn: &RoTxn<'_>,
        name: &str,
    ) -> Result<Option<u64>> {
        Ok(meta
            .get(rtxn, Self::ttl_key(name).as_bytes())?
            .and_then(|b| std::str::from_utf8(b).ok())
            .and_then(|s| s.parse::<u64>().ok()))
    }

    fn ttl_from_rw(
        meta: Database<Bytes, Bytes>,
        wtxn: &RwTxn<'_>,
        name: &str,
    ) -> Result<Option<u64>> {
        Ok(meta
            .get(wtxn, Self::ttl_key(name).as_bytes())?
            .and_then(|b| std::str::from_utf8(b).ok())
            .and_then(|s| s.parse::<u64>().ok()))
    }

    fn ttl_purge_from_ro(
        meta: Database<Bytes, Bytes>,
        rtxn: &RoTxn<'_>,
        name: &str,
    ) -> Result<bool> {
        Ok(meta
            .get(rtxn, Self::ttl_purge_key(name).as_bytes())?
            .is_some_and(|b| b == b"1"))
    }

    fn ttl_purge_from_rw(
        meta: Database<Bytes, Bytes>,
        wtxn: &RwTxn<'_>,
        name: &str,
    ) -> Result<bool> {
        Ok(meta
            .get(wtxn, Self::ttl_purge_key(name).as_bytes())?
            .is_some_and(|b| b == b"1"))
    }

    fn profile_from_ro(
        meta: Database<Bytes, Bytes>,
        rtxn: &RoTxn<'_>,
        name: &str,
    ) -> Result<CollectionProfile> {
        let profile = meta
            .get(rtxn, Self::profile_key(name).as_bytes())?
            .and_then(|b| std::str::from_utf8(b).ok())
            .unwrap_or("standard");
        CollectionProfile::parse(profile)
    }

    fn profile_from_rw(
        meta: Database<Bytes, Bytes>,
        wtxn: &RwTxn<'_>,
        name: &str,
    ) -> Result<CollectionProfile> {
        let profile = meta
            .get(wtxn, Self::profile_key(name).as_bytes())?
            .and_then(|b| std::str::from_utf8(b).ok())
            .unwrap_or("standard");
        CollectionProfile::parse(profile)
    }

    /// Creates a collection with an explicit profile.
    ///
    /// Existing collections cannot be converted between `standard` and `vault`;
    /// create a new collection and reinsert the data instead.
    pub fn create_collection(
        &self,
        name: &str,
        profile: CollectionProfile,
    ) -> Result<Collection<'_>> {
        self.create_collection_with_ttl(name, profile, None)
    }

    /// Creates a collection with an optional default document TTL.
    pub fn create_collection_with_ttl(
        &self,
        name: &str,
        profile: CollectionProfile,
        ttl_seconds: Option<u64>,
    ) -> Result<Collection<'_>> {
        self.create_collection_with_ttl_and_purge(name, profile, ttl_seconds, None)
    }

    /// Creates a collection with TTL settings and optional history purging on expiry.
    pub fn create_collection_with_ttl_and_purge(
        &self,
        name: &str,
        profile: CollectionProfile,
        ttl_seconds: Option<u64>,
        purge_history: Option<bool>,
    ) -> Result<Collection<'_>> {
        let mut wtxn = self.write_txn()?;
        let collection = self.create_collection_with_ttl_and_purge_in(
            &mut wtxn,
            name,
            profile,
            ttl_seconds,
            purge_history,
        )?;
        wtxn.commit()?;
        Ok(collection)
    }

    pub fn create_collection_in<'e>(
        &'e self,
        wtxn: &mut RwTxn<'_>,
        name: &str,
        profile: CollectionProfile,
    ) -> Result<Collection<'e>> {
        self.create_collection_with_ttl_in(wtxn, name, profile, None)
    }

    pub fn create_collection_with_ttl_in<'e>(
        &'e self,
        wtxn: &mut RwTxn<'_>,
        name: &str,
        profile: CollectionProfile,
        ttl_seconds: Option<u64>,
    ) -> Result<Collection<'e>> {
        self.create_collection_with_ttl_and_purge_in(wtxn, name, profile, ttl_seconds, None)
    }

    pub fn create_collection_with_ttl_and_purge_in<'e>(
        &'e self,
        wtxn: &mut RwTxn<'_>,
        name: &str,
        profile: CollectionProfile,
        ttl_seconds: Option<u64>,
        purge_history: Option<bool>,
    ) -> Result<Collection<'e>> {
        Self::validate_name(name)?;
        let db = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(name))?;
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        let key = format!("collection:{name}");
        let exists = meta.get(&*wtxn, key.as_bytes())?.is_some();
        let current = Self::profile_from_rw(meta, wtxn, name)?;
        if exists && current != profile {
            return Err(Error::Vault {
                code: "vault_profile_immutable",
                message: format!(
                    "collection `{name}` already exists as `{}`",
                    current.as_str()
                ),
            });
        }
        if !exists {
            meta.put(&mut *wtxn, key.as_bytes(), b"1")?;
        }
        meta.put(
            &mut *wtxn,
            Self::profile_key(name).as_bytes(),
            profile.as_str().as_bytes(),
        )?;
        if let Some(seconds) = ttl_seconds {
            Self::validate_ttl(seconds)?;
            meta.put(
                &mut *wtxn,
                Self::ttl_key(name).as_bytes(),
                seconds.to_string().as_bytes(),
            )?;
        }
        if let Some(purge) = purge_history {
            if purge {
                meta.put(&mut *wtxn, Self::ttl_purge_key(name).as_bytes(), b"1")?;
            } else {
                meta.delete(&mut *wtxn, Self::ttl_purge_key(name).as_bytes())?;
            }
        }
        let ttl_default = Self::ttl_from_rw(meta, wtxn, name)?;
        let purge = Self::ttl_purge_from_rw(meta, wtxn, name)?;
        Ok(self.wrap(name, db, meta, profile, ttl_default, purge))
    }

    fn validate_ttl(seconds: u64) -> Result<()> {
        if seconds == 0 {
            return Err(Error::InvalidTtl(
                "collection `ttl_seconds` must be greater than zero".into(),
            ));
        }
        Ok(())
    }

    /// Sets or clears a collection's default document TTL.
    pub fn set_collection_ttl(&self, name: &str, ttl_seconds: Option<u64>) -> Result<()> {
        self.set_collection_ttl_with_purge(name, ttl_seconds, None)
    }

    /// Sets TTL options; `purge_history: None` preserves the current setting.
    pub fn set_collection_ttl_with_purge(
        &self,
        name: &str,
        ttl_seconds: Option<u64>,
        purge_history: Option<bool>,
    ) -> Result<()> {
        let mut wtxn = self.write_txn()?;
        self.set_collection_ttl_with_purge_in(&mut wtxn, name, ttl_seconds, purge_history)?;
        wtxn.commit()?;
        Ok(())
    }

    /// Sets or clears a collection TTL inside an existing writer transaction.
    pub fn set_collection_ttl_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        name: &str,
        ttl_seconds: Option<u64>,
    ) -> Result<()> {
        self.set_collection_ttl_with_purge_in(wtxn, name, ttl_seconds, None)
    }

    pub fn set_collection_ttl_with_purge_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        name: &str,
        ttl_seconds: Option<u64>,
        purge_history: Option<bool>,
    ) -> Result<()> {
        Self::validate_name(name)?;
        if self.collection_profile_in_rw(wtxn, name)?.is_none() {
            return Err(Error::Query(format!("collection `{name}` does not exist")));
        }
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        if let Some(seconds) = ttl_seconds {
            Self::validate_ttl(seconds)?;
            meta.put(
                &mut *wtxn,
                Self::ttl_key(name).as_bytes(),
                seconds.to_string().as_bytes(),
            )?;
        } else {
            meta.delete(&mut *wtxn, Self::ttl_key(name).as_bytes())?;
        }
        if let Some(purge) = purge_history {
            if purge {
                meta.put(&mut *wtxn, Self::ttl_purge_key(name).as_bytes(), b"1")?;
            } else {
                meta.delete(&mut *wtxn, Self::ttl_purge_key(name).as_bytes())?;
            }
        }
        Ok(())
    }

    /// Returns a collection's default TTL, if any.
    pub fn collection_ttl(&self, name: &str) -> Result<Option<u64>> {
        let rtxn = self.read_txn()?;
        let meta = self
            .env
            .open_database::<Bytes, Bytes>(&rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        let out = Self::ttl_from_ro(meta, &rtxn, name)?;
        rtxn.commit()?;
        Ok(out)
    }

    /// Whether TTL expiry purges history for this collection.
    pub fn collection_ttl_purge(&self, name: &str) -> Result<bool> {
        let rtxn = self.read_txn()?;
        let meta = self
            .env
            .open_database::<Bytes, Bytes>(&rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        let out = Self::ttl_purge_from_ro(meta, &rtxn, name)?;
        rtxn.commit()?;
        Ok(out)
    }

    /// Returns the profile of a collection, or `None` if it does not exist.
    pub fn collection_profile(&self, name: &str) -> Result<Option<CollectionProfile>> {
        let rtxn = self.read_txn()?;
        let out = self.collection_profile_in(&rtxn, name)?;
        rtxn.commit()?;
        Ok(out)
    }

    /// Profile lookup inside an existing write transaction.
    pub fn collection_profile_in_rw(
        &self,
        wtxn: &RwTxn<'_>,
        name: &str,
    ) -> Result<Option<CollectionProfile>> {
        Self::validate_name(name)?;
        let meta = self
            .env
            .open_database::<Bytes, Bytes>(wtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        if meta
            .get(wtxn, format!("collection:{name}").as_bytes())?
            .is_none()
        {
            return Ok(None);
        }
        Ok(Some(Self::profile_from_rw(meta, wtxn, name)?))
    }

    pub fn collection_profile_in(
        &self,
        rtxn: &RoTxn<'_>,
        name: &str,
    ) -> Result<Option<CollectionProfile>> {
        Self::validate_name(name)?;
        let meta = self
            .env
            .open_database::<Bytes, Bytes>(rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        if meta
            .get(rtxn, format!("collection:{name}").as_bytes())?
            .is_none()
        {
            return Ok(None);
        }
        Ok(Some(Self::profile_from_ro(meta, rtxn, name)?))
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
                let profile = Self::profile_from_ro(meta, &rtxn, name)?;
                let ttl_default = Self::ttl_from_ro(meta, &rtxn, name)?;
                let purge = Self::ttl_purge_from_ro(meta, &rtxn, name)?;
                rtxn.commit()?;
                return Ok(self.wrap(name, db, meta, profile, ttl_default, purge));
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
        let profile = Self::profile_from_rw(meta, wtxn, name)?;
        let ttl_default = Self::ttl_from_rw(meta, wtxn, name)?;
        let purge = Self::ttl_purge_from_rw(meta, wtxn, name)?;
        Ok(self.wrap(name, db, meta, profile, ttl_default, purge))
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
        let profile = Self::profile_from_ro(meta, rtxn, name)?;
        let ttl_default = Self::ttl_from_ro(meta, rtxn, name)?;
        let purge = Self::ttl_purge_from_ro(meta, rtxn, name)?;
        Ok(Some(self.wrap(name, db, meta, profile, ttl_default, purge)))
    }

    #[allow(clippy::too_many_arguments)]
    fn wrap<'e>(
        &'e self,
        name: &str,
        db: Database<Bytes, Bytes>,
        meta: Database<Bytes, Bytes>,
        profile: CollectionProfile,
        ttl_default: Option<u64>,
        purge_history: bool,
    ) -> Collection<'e> {
        Collection::new(
            &self.env,
            db,
            meta,
            self.history,
            self.ttl,
            self.history_config,
            name.to_string(),
            profile,
            ttl_default,
            purge_history,
        )
    }

    /// Reads due TTL entries with a short-lived read transaction.
    pub fn ttl_due(&self, now: u64, limit: usize) -> Result<Vec<crate::ttl::DueEntry>> {
        let rtxn = self.read_txn()?;
        let out = ttl::due_entries(&self.ttl, &rtxn, now, limit)?;
        rtxn.commit()?;
        Ok(out)
    }

    /// Removes stale TTL entries for a collection that no longer exists.
    pub fn ttl_remove_in(&self, wtxn: &mut RwTxn<'_>, coll: &str, id: &str) -> Result<()> {
        ttl::remove_entry(&self.ttl, wtxn, coll, id)
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
