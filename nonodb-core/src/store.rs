//! Opening LMDB and managing collections.

use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::{Condvar, Mutex};

use heed::types::Bytes;
use heed::{Database, Env, EnvFlags, EnvOpenOptions, RoTxn, RwTxn};

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

/// Parses sizes such as `4GiB`, `2048MiB`, `1GB`, or a raw byte count.
pub fn parse_size(raw: &str) -> Option<usize> {
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
/// Maximum number of LMDB sub-databases (collections + meta + index slots).
///
/// Dropped index/search resources return their reusable slot to the pool, so
/// normal create/drop cycles do not consume new slots. The higher ceiling also
/// leaves room for databases created by older releases before slot reuse.
pub const MAX_DBS: u32 = 1024;
/// Metadata sub-database name.
pub const META_DB: &str = "meta";
/// Data format version (for future migrations).
pub const SCHEMA_VERSION: u32 = 1;
/// Soft document size limit (serialized JSON).
/// LMDB technically supports larger values (overflow pages), but a large
/// document means an expensive update (the whole value is rewritten).
pub const MAX_DOCUMENT_BYTES: usize = 1024 * 1024;

/// Serializes transaction entry with an exclusive resize request.
///
/// A resize may only run while no LMDB transaction is active. New transactions
/// are blocked as soon as a resize is queued, so a stream of readers cannot
/// starve the resize.
#[derive(Default)]
pub(crate) struct TxnGate {
    inner: Mutex<TxnGateState>,
    cv: Condvar,
}

#[derive(Default)]
struct TxnGateState {
    active: usize,
    resize_queued: bool,
    resizing: bool,
}

pub(crate) struct TxnGuard<'a> {
    gate: &'a TxnGate,
}

impl Drop for TxnGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.gate.inner.lock().unwrap_or_else(|e| e.into_inner());
        state.active = state.active.saturating_sub(1);
        if state.active == 0 {
            self.gate.cv.notify_all();
        }
    }
}

struct ResizeGuard<'a> {
    gate: &'a TxnGate,
}

impl Drop for ResizeGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.gate.inner.lock().unwrap_or_else(|e| e.into_inner());
        state.resizing = false;
        self.gate.cv.notify_all();
    }
}

impl TxnGate {
    pub(crate) fn enter(&self) -> TxnGuard<'_> {
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        while state.resizing || state.resize_queued {
            state = self.cv.wait(state).unwrap_or_else(|e| e.into_inner());
        }
        state.active += 1;
        TxnGuard { gate: self }
    }

    fn enter_resize(&self) -> ResizeGuard<'_> {
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        state.resize_queued = true;
        while state.active > 0 || state.resizing {
            state = self.cv.wait(state).unwrap_or_else(|e| e.into_inner());
        }
        state.resize_queued = false;
        state.resizing = true;
        ResizeGuard { gate: self }
    }
}

/// A read transaction that also holds a transaction gate.
///
/// It dereferences to [`heed::RoTxn`], so existing collection methods accept it
/// unchanged. Keeping the guard alive guarantees that an online map resize
/// cannot run while the transaction is active.
pub struct ReadTxn<'a> {
    txn: RoTxn<'a>,
    _guard: TxnGuard<'a>,
}

impl<'a> ReadTxn<'a> {
    pub(crate) fn new(txn: RoTxn<'a>, guard: TxnGuard<'a>) -> Self {
        Self { txn, _guard: guard }
    }

    /// Commits (releases) the read transaction.
    pub fn commit(self) -> heed::Result<()> {
        self.txn.commit()
    }
}

impl<'a> Deref for ReadTxn<'a> {
    type Target = RoTxn<'a>;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

/// A write transaction that also holds a transaction gate.
pub struct WriteTxn<'a> {
    txn: RwTxn<'a>,
    _guard: TxnGuard<'a>,
}

impl<'a> WriteTxn<'a> {
    pub(crate) fn new(txn: RwTxn<'a>, guard: TxnGuard<'a>) -> Self {
        Self { txn, _guard: guard }
    }

    /// Commits the transaction and releases the gate.
    pub fn commit(self) -> heed::Result<()> {
        self.txn.commit()
    }

    /// Aborts the transaction and releases the gate.
    pub fn abort(self) {
        self.txn.abort()
    }
}

impl<'a> Deref for WriteTxn<'a> {
    type Target = RwTxn<'a>;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

impl DerefMut for WriteTxn<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.txn
    }
}

/// Database handle. One `Store` = one `Env` = one LMDB file.
pub struct Store {
    env: Env,
    history: Database<Bytes, Bytes>,
    ttl: Database<Bytes, Bytes>,
    history_config: HistoryConfig,
    txn_gate: TxnGate,
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
        Self::open_with_history_and_map_size(path, history_config, configured_map_size())
    }

    /// Opens the database with explicit history settings and map size.
    ///
    /// Mostly useful for tests, embedded tools, and a controlled offline resize.
    pub fn open_with_history_and_map_size(
        path: impl AsRef<Path>,
        history_config: HistoryConfig,
        map_size: usize,
    ) -> Result<Self> {
        if map_size < PAGE_SIZE {
            return Err(Error::Query(format!(
                "map size must be at least one page ({PAGE_SIZE} bytes)"
            )));
        }
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;

        // SAFETY: the file is not modified outside LMDB and is not opened
        // concurrently with another `map_size` (the daemon is the sole owner).
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(map_size)
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
            txn_gate: TxnGate::default(),
        })
    }

    /// Opens a standard database with an explicit map size.
    pub fn open_with_map_size(path: impl AsRef<Path>, map_size: usize) -> Result<Self> {
        Self::open_with_history_and_map_size(path, HistoryConfig::default(), map_size)
    }

    /// Opens an existing database read-only for verification and inspection.
    ///
    /// Missing history/TTL databases from older releases fall back to the meta
    /// handle; verification reads those databases directly when present.
    pub(crate) fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        // SAFETY: read-only mapping; the environment is not modified.
        let env = unsafe {
            EnvOpenOptions::new()
                .flags(EnvFlags::READ_ONLY)
                .max_dbs(MAX_DBS)
                .open(path)?
        };
        let rtxn = env.read_txn()?;
        let meta = env
            .open_database::<Bytes, Bytes>(&rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        let history = env
            .open_database::<Bytes, Bytes>(&rtxn, Some(HISTORY_DB))?
            .unwrap_or(meta);
        let ttl = env
            .open_database::<Bytes, Bytes>(&rtxn, Some(TTL_DB))?
            .unwrap_or(meta);
        rtxn.commit()?;
        Ok(Self {
            env,
            history,
            ttl,
            history_config: HistoryConfig::default(),
            txn_gate: TxnGate::default(),
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

    /// Raw LMDB environment.
    ///
    /// Transactions opened directly on this handle bypass the online-resize
    /// gate. Prefer [`Store::read_txn`] and [`Store::write_txn`], or pause
    /// external administrative work while resizing.
    pub fn env(&self) -> &Env {
        &self.env
    }

    pub fn read_txn(&self) -> Result<ReadTxn<'_>> {
        let guard = self.txn_gate.enter();
        Ok(ReadTxn::new(self.env.read_txn()?, guard))
    }

    pub fn write_txn(&self) -> Result<WriteTxn<'_>> {
        let guard = self.txn_gate.enter();
        Ok(WriteTxn::new(self.env.write_txn()?, guard))
    }

    /// Current LMDB map size in bytes.
    pub fn map_size(&self) -> usize {
        self.env.info().map_size
    }

    /// Approximate number of bytes occupied by the database high-water mark.
    pub fn map_used_bytes(&self) -> usize {
        let info = self.env.info();
        let pages = info.last_page_number + 1;
        pages.saturating_mul(PAGE_SIZE)
    }

    /// Grows the LMDB map without restarting the process.
    ///
    /// The call waits for all gated transactions to finish and prevents new
    /// ones from starting until LMDB has changed the mapping. It only grows the
    /// map; shrinking requires an offline restart. Returns the previous size.
    pub fn resize_map_size(&self, new_size: usize) -> Result<usize> {
        if new_size < PAGE_SIZE {
            return Err(Error::Query(format!(
                "map size must be at least one page ({PAGE_SIZE} bytes)"
            )));
        }
        if !new_size.is_multiple_of(PAGE_SIZE) {
            return Err(Error::Query(format!(
                "map size must be a multiple of the page size ({PAGE_SIZE} bytes)"
            )));
        }
        let _guard = self.txn_gate.enter_resize();
        let old_size = self.env.info().map_size;
        if new_size < old_size {
            return Err(Error::Query(format!(
                "online resize can only grow the map (current {old_size} bytes, requested {new_size})"
            )));
        }
        if new_size == old_size {
            return Ok(old_size);
        }
        if new_size < self.map_used_bytes() {
            return Err(Error::Query(format!(
                "map size {new_size} is smaller than the used database high-water mark"
            )));
        }
        // SAFETY: the resize guard excludes every transaction created through
        // `Store`/`Collection`; in-process snapshots use `copy_to_file`, which
        // also takes the gate. External processes must be paused by operators.
        unsafe { self.env.resize(new_size)? };
        Ok(old_size)
    }

    /// Makes a consistent LMDB copy while holding the resize gate.
    pub fn copy_to_file(
        &self,
        path: impl AsRef<Path>,
        option: heed::CompactionOption,
    ) -> Result<std::fs::File> {
        let _guard = self.txn_gate.enter();
        Ok(self.env.copy_to_file(path, option)?)
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
            &self.txn_gate,
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
