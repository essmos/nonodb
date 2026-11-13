//! Opening LMDB and managing collections.

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::{Condvar, Mutex};

use heed::types::Bytes;
use heed::{Database, Env, EnvFlags, EnvOpenOptions, RoTxn, RwTxn};

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::events::EventConfig;
use crate::history::{self, DEFAULT_HISTORY_MAX_VERSIONS, HISTORY_DB};
use crate::subscriptions::{self, Subscription};
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
const DEFAULT_SUBSCRIPTION_LEASE_SECONDS: u64 = 600;
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
    env: &'a Env,
    sync_on_commit: bool,
}

impl<'a> WriteTxn<'a> {
    pub(crate) fn new(
        txn: RwTxn<'a>,
        guard: TxnGuard<'a>,
        env: &'a Env,
        sync_on_commit: bool,
    ) -> Self {
        Self {
            txn,
            _guard: guard,
            env,
            sync_on_commit,
        }
    }

    /// Commits the transaction, optionally forcing an `fsync`.
    pub fn commit(self) -> heed::Result<()> {
        let Self {
            txn,
            _guard,
            env,
            sync_on_commit,
        } = self;
        txn.commit()?;
        if sync_on_commit {
            env.force_sync()?;
        }
        Ok(())
    }

    /// Commits without forcing `fsync`; the caller decides when to sync.
    pub fn commit_unsynced(self) -> heed::Result<()> {
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
    txn_gate: TxnGate,
}

impl Store {
    /// Opens (or creates) the database in the `path` directory.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_map_size(path, configured_map_size())
    }

    /// Opens the database with an explicit map size.
    ///
    /// Mostly useful for tests, embedded tools, and a controlled offline resize.
    pub fn open_with_map_size(path: impl AsRef<Path>, map_size: usize) -> Result<Self> {
        if map_size < PAGE_SIZE {
            return Err(Error::Query(format!(
                "map size must be at least one page ({PAGE_SIZE} bytes)"
            )));
        }
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;

        // SAFETY: the file is not modified outside LMDB and is not opened
        // concurrently with another `map_size` (the daemon is the sole owner).
        // LMDB sync flags are environment-wide, not transaction-wide. We open
        // with NO_META_SYNC and force a full sync for durable commits. This
        // allows per-request `relaxed` writes without changing the default
        // durability of ordinary writes. Unsafe mode is opt-in because NO_SYNC
        // can lose recent transactions on a crash.
        let flags = if std::env::var("NONODB_ALLOW_UNSAFE_WRITES")
            .ok()
            .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
        {
            EnvFlags::NO_SYNC
        } else {
            EnvFlags::NO_META_SYNC
        };
        let env = unsafe {
            EnvOpenOptions::new()
                .flags(flags)
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
            txn_gate: TxnGate::default(),
        })
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
            txn_gate: TxnGate::default(),
        })
    }

    /// Deletes old revisions above each collection's configured per-document limit.
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
        let meta = self
            .env
            .open_database::<Bytes, Bytes>(wtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        let mut limits = HashMap::new();
        for item in meta.iter(&*wtxn)? {
            let (key, value) = item?;
            let Some(name) = std::str::from_utf8(key)
                .ok()
                .and_then(|k| k.strip_prefix("history:"))
            else {
                continue;
            };
            if let Some(max) = std::str::from_utf8(value)
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
            {
                limits.insert(name.to_string(), max);
            }
        }
        history::prune(&self.history, &meta, wtxn, &limits, limit)
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
        Ok(WriteTxn::new(self.env.write_txn()?, guard, &self.env, true))
    }

    /// Writer-only transaction that leaves the final sync decision to the
    /// caller. Used by group commit to implement per-request durability.
    pub fn write_txn_unsynced(&self) -> Result<WriteTxn<'_>> {
        let guard = self.txn_gate.enter();
        Ok(WriteTxn::new(
            self.env.write_txn()?,
            guard,
            &self.env,
            false,
        ))
    }

    /// Flushes the LMDB environment to stable storage.
    pub fn force_sync(&self) -> Result<()> {
        self.env.force_sync()?;
        Ok(())
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
        env.force_sync()?;
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

    fn history_key(name: &str) -> String {
        format!("history:{name}")
    }

    fn events_key(name: &str) -> String {
        format!("events:{name}")
    }

    fn events_from_ro(
        meta: Database<Bytes, Bytes>,
        rtxn: &RoTxn<'_>,
        name: &str,
    ) -> Result<EventConfig> {
        Ok(meta
            .get(rtxn, Self::events_key(name).as_bytes())?
            .and_then(|raw| serde_json::from_slice(raw).ok())
            .unwrap_or_default())
    }

    fn events_from_rw(
        meta: Database<Bytes, Bytes>,
        wtxn: &RwTxn<'_>,
        name: &str,
    ) -> Result<EventConfig> {
        Ok(meta
            .get(wtxn, Self::events_key(name).as_bytes())?
            .and_then(|raw| serde_json::from_slice(raw).ok())
            .unwrap_or_default())
    }

    fn history_from_ro(
        meta: Database<Bytes, Bytes>,
        rtxn: &RoTxn<'_>,
        name: &str,
    ) -> Result<usize> {
        Ok(meta
            .get(rtxn, Self::history_key(name).as_bytes())?
            .and_then(|b| std::str::from_utf8(b).ok())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_HISTORY_MAX_VERSIONS))
    }

    fn history_from_rw(
        meta: Database<Bytes, Bytes>,
        wtxn: &RwTxn<'_>,
        name: &str,
    ) -> Result<usize> {
        Ok(meta
            .get(wtxn, Self::history_key(name).as_bytes())?
            .and_then(|b| std::str::from_utf8(b).ok())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_HISTORY_MAX_VERSIONS))
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
        let history = Self::history_from_rw(meta, wtxn, name)?;
        Ok(self.wrap(name, db, meta, profile, ttl_default, purge, history))
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

    /// Sets the per-collection history retention. `0` disables new history writes.
    pub fn set_collection_history(&self, name: &str, max_versions: usize) -> Result<()> {
        let mut wtxn = self.write_txn()?;
        self.set_collection_history_in(&mut wtxn, name, max_versions)?;
        wtxn.commit()?;
        Ok(())
    }

    /// Sets per-collection history retention inside an existing writer transaction.
    pub fn set_collection_history_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        name: &str,
        max_versions: usize,
    ) -> Result<()> {
        Self::validate_name(name)?;
        if self.collection_profile_in_rw(wtxn, name)?.is_none() {
            return Err(Error::Query(format!("collection `{name}` does not exist")));
        }
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        if max_versions == 0 {
            meta.delete(&mut *wtxn, Self::history_key(name).as_bytes())?;
        } else {
            meta.put(
                &mut *wtxn,
                Self::history_key(name).as_bytes(),
                max_versions.to_string().as_bytes(),
            )?;
        }
        Ok(())
    }

    /// Configures automatic change events for one collection.
    pub fn set_collection_events(&self, name: &str, config: &EventConfig) -> Result<()> {
        let mut wtxn = self.write_txn()?;
        self.set_collection_events_in(&mut wtxn, name, config)?;
        wtxn.commit()?;
        Ok(())
    }

    /// Configures automatic change events inside an existing writer transaction.
    pub fn set_collection_events_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        name: &str,
        config: &EventConfig,
    ) -> Result<()> {
        Self::validate_name(name)?;
        if self.collection_profile_in_rw(wtxn, name)?.is_none() {
            return Err(Error::Query(format!("collection `{name}` does not exist")));
        }
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        if config.enabled {
            let raw = serde_json::to_vec(config)?;
            meta.put(&mut *wtxn, Self::events_key(name).as_bytes(), &raw)?;
        } else {
            meta.delete(&mut *wtxn, Self::events_key(name).as_bytes())?;
        }
        Ok(())
    }

    fn subscriptions_from_ro(
        meta: Database<Bytes, Bytes>,
        rtxn: &RoTxn<'_>,
        coll: &str,
    ) -> Result<Vec<Subscription>> {
        Ok(meta
            .get(rtxn, subscriptions::meta_key(coll).as_bytes())?
            .and_then(|raw| serde_json::from_slice(raw).ok())
            .unwrap_or_default())
    }

    fn subscriptions_from_rw(
        meta: Database<Bytes, Bytes>,
        wtxn: &RwTxn<'_>,
        coll: &str,
    ) -> Result<Vec<Subscription>> {
        Ok(meta
            .get(wtxn, subscriptions::meta_key(coll).as_bytes())?
            .and_then(|raw| serde_json::from_slice(raw).ok())
            .unwrap_or_default())
    }

    /// Creates or replaces a subscription on a collection.
    pub fn create_subscription(
        &self,
        coll: &str,
        subscription: &Subscription,
    ) -> Result<Subscription> {
        let mut wtxn = self.write_txn()?;
        let out = self.create_subscription_in(&mut wtxn, coll, subscription)?;
        wtxn.commit()?;
        Ok(out)
    }

    /// Creates or replaces a subscription inside the writer transaction.
    pub fn create_subscription_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        coll: &str,
        subscription: &Subscription,
    ) -> Result<Subscription> {
        Self::validate_name(coll)?;
        if self.collection_profile_in_rw(wtxn, coll)?.is_none() {
            return Err(Error::Query(format!("collection `{coll}` does not exist")));
        }
        let mut subscription = subscription.clone();
        subscription.validate()?;
        if subscription.filter.is_null() {
            subscription.filter = serde_json::json!({});
        }
        if let Some(count) = &mut subscription.count {
            if count.filter.is_null() {
                count.filter = serde_json::json!({});
            }
        }
        if subscription.lease_seconds == 0 {
            subscription.lease_seconds = DEFAULT_SUBSCRIPTION_LEASE_SECONDS;
        }
        subscription.lease_seconds = subscription.lease_seconds.clamp(1, 2_592_000);
        subscription.expires_at =
            subscriptions::now_ms() + subscription.lease_seconds.saturating_mul(1000);
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        let mut list = Self::subscriptions_from_rw(meta, wtxn, coll)?;
        if let Some(existing) = list.iter_mut().find(|item| item.name == subscription.name) {
            *existing = subscription.clone();
        } else {
            list.push(subscription.clone());
        }
        let raw = serde_json::to_vec(&list)?;
        meta.put(&mut *wtxn, subscriptions::meta_key(coll).as_bytes(), &raw)?;
        Ok(subscription)
    }

    /// Deletes a subscription. Returns whether it existed.
    pub fn delete_subscription(&self, coll: &str, name: &str) -> Result<bool> {
        let mut wtxn = self.write_txn()?;
        let existed = self.delete_subscription_in(&mut wtxn, coll, name)?;
        wtxn.commit()?;
        Ok(existed)
    }

    pub fn delete_subscription_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        coll: &str,
        name: &str,
    ) -> Result<bool> {
        Self::validate_name(coll)?;
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        let mut list = Self::subscriptions_from_rw(meta, wtxn, coll)?;
        let before = list.len();
        list.retain(|item| item.name != name);
        if list.len() == before {
            return Ok(false);
        }
        if list.is_empty() {
            meta.delete(&mut *wtxn, subscriptions::meta_key(coll).as_bytes())?;
        } else {
            let raw = serde_json::to_vec(&list)?;
            meta.put(&mut *wtxn, subscriptions::meta_key(coll).as_bytes(), &raw)?;
        }
        Ok(true)
    }

    /// Whether a collection has any conditional subscriptions.
    pub fn has_subscriptions_in_rw(&self, wtxn: &RwTxn<'_>, coll: &str) -> Result<bool> {
        if coll == crate::events::EVENTS_COLLECTION {
            return Ok(false);
        }
        let meta = self
            .env
            .open_database::<Bytes, Bytes>(wtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        Ok(meta
            .get(wtxn, subscriptions::meta_key(coll).as_bytes())?
            .is_some())
    }

    /// Renews a subscription lease. Returns `None` if it does not exist.
    pub fn renew_subscription(
        &self,
        coll: &str,
        name: &str,
        lease_seconds: Option<u64>,
    ) -> Result<Option<Subscription>> {
        let mut wtxn = self.write_txn()?;
        let out = self.renew_subscription_in(&mut wtxn, coll, name, lease_seconds)?;
        wtxn.commit()?;
        Ok(out)
    }

    pub fn renew_subscription_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        coll: &str,
        name: &str,
        lease_seconds: Option<u64>,
    ) -> Result<Option<Subscription>> {
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        let mut list = Self::subscriptions_from_rw(meta, wtxn, coll)?;
        let now = subscriptions::now_ms();
        let Some(index) = list.iter().position(|item| item.name == name) else {
            return Ok(None);
        };
        if list[index].is_expired(now) {
            list.remove(index);
            if list.is_empty() {
                meta.delete(&mut *wtxn, subscriptions::meta_key(coll).as_bytes())?;
            } else {
                let raw = serde_json::to_vec(&list)?;
                meta.put(&mut *wtxn, subscriptions::meta_key(coll).as_bytes(), &raw)?;
            }
            return Ok(None);
        }
        let subscription = &mut list[index];
        if let Some(lease) = lease_seconds {
            subscription.lease_seconds = lease.clamp(1, 2_592_000);
        }
        if subscription.lease_seconds == 0 {
            subscription.lease_seconds = DEFAULT_SUBSCRIPTION_LEASE_SECONDS;
        }
        subscription.expires_at =
            subscriptions::now_ms() + subscription.lease_seconds.saturating_mul(1000);
        let out = subscription.clone();
        let raw = serde_json::to_vec(&list)?;
        meta.put(&mut *wtxn, subscriptions::meta_key(coll).as_bytes(), &raw)?;
        Ok(Some(out))
    }

    /// Deletes expired subscriptions from every collection.
    pub fn prune_expired_subscriptions_in(&self, wtxn: &mut RwTxn<'_>) -> Result<usize> {
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        let now = subscriptions::now_ms();
        let mut keys = Vec::new();
        for item in meta.iter(&*wtxn)? {
            let (key, _) = item?;
            if std::str::from_utf8(key)
                .ok()
                .is_some_and(|key| key.starts_with("subscriptions:"))
            {
                keys.push(key.to_vec());
            }
        }
        let mut removed = 0usize;
        for key in keys {
            let Some(raw) = meta.get(&*wtxn, &key)? else {
                continue;
            };
            let mut list: Vec<Subscription> = serde_json::from_slice(raw)?;
            let before = list.len();
            list.retain(|subscription| !subscription.is_expired(now));
            if list.len() == before {
                continue;
            }
            removed += before - list.len();
            if list.is_empty() {
                meta.delete(&mut *wtxn, &key)?;
            } else {
                let raw = serde_json::to_vec(&list)?;
                meta.put(&mut *wtxn, &key, &raw)?;
            }
        }
        Ok(removed)
    }

    /// Deletes all expired subscriptions.
    pub fn prune_expired_subscriptions(&self) -> Result<usize> {
        let mut wtxn = self.write_txn()?;
        let removed = self.prune_expired_subscriptions_in(&mut wtxn)?;
        wtxn.commit()?;
        Ok(removed)
    }

    /// Lists subscriptions for one collection.
    pub fn list_subscriptions(&self, coll: &str) -> Result<Vec<Subscription>> {
        let rtxn = self.read_txn()?;
        let meta = self
            .env
            .open_database::<Bytes, Bytes>(&rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        let out = Self::subscriptions_from_ro(meta, &rtxn, coll)?;
        rtxn.commit()?;
        Ok(out)
    }

    /// Returns the automatic change-event configuration for a collection.
    pub fn collection_events(&self, name: &str) -> Result<EventConfig> {
        let rtxn = self.read_txn()?;
        let meta = self
            .env
            .open_database::<Bytes, Bytes>(&rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        let out = Self::events_from_ro(meta, &rtxn, name)?;
        rtxn.commit()?;
        Ok(out)
    }

    /// Reads event settings inside the writer transaction.
    pub fn collection_events_in_rw(&self, wtxn: &RwTxn<'_>, name: &str) -> Result<EventConfig> {
        Self::validate_name(name)?;
        let meta = self
            .env
            .open_database::<Bytes, Bytes>(wtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        Self::events_from_rw(meta, wtxn, name)
    }

    /// Returns the per-collection history retention. `0` means disabled.
    pub fn collection_history(&self, name: &str) -> Result<usize> {
        let rtxn = self.read_txn()?;
        let meta = self
            .env
            .open_database::<Bytes, Bytes>(&rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        let out = Self::history_from_ro(meta, &rtxn, name)?;
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
                let history = Self::history_from_ro(meta, &rtxn, name)?;
                rtxn.commit()?;
                return Ok(self.wrap(name, db, meta, profile, ttl_default, purge, history));
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
        let history = Self::history_from_rw(meta, wtxn, name)?;
        Ok(self.wrap(name, db, meta, profile, ttl_default, purge, history))
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
        let history = Self::history_from_ro(meta, rtxn, name)?;
        Ok(Some(self.wrap(
            name,
            db,
            meta,
            profile,
            ttl_default,
            purge,
            history,
        )))
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
        history_max_versions: usize,
    ) -> Collection<'e> {
        Collection::new(
            &self.env,
            db,
            meta,
            self.history,
            self.ttl,
            history_max_versions,
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

    /// Stable process-local identifier for this LMDB environment.
    pub fn id(&self) -> usize {
        &self.env as *const _ as usize
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
