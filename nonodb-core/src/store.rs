//! Opening LMDB and managing collections.

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::{Condvar, Mutex};

use heed::types::Bytes;
use heed::{Database, Env, EnvFlags, EnvOpenOptions, RoTxn, RwTxn};
use serde::Serialize;
use serde_json::Value;

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::events::EventConfig;
use crate::graph;
use crate::history::{self, DEFAULT_HISTORY_MAX_VERSIONS, HISTORY_DB};
use crate::replication::{
    ReplicationChange, ReplicationOp, ReplicationTxn, REPLICATION_DB, REPLICATION_SEQ_KEY,
};
use crate::streams::{ResetTo, StartAt, StreamDef, StreamGroup, StreamLease};
use crate::subscriptions::{self, Subscription};
use crate::ttl::{self, TTL_DB};
use crate::vault::CollectionProfile;

/// How commits reach stable storage.
///
/// LMDB sync flags are environment-wide, so this has to be decided when the
/// environment is opened:
///
/// * [`SyncPolicy::PerCommit`] - default. The environment is opened with
///   `NO_META_SYNC`; a commit still syncs data pages, and `Durability::Full`
///   adds an explicit `force_sync`. `relaxed` therefore measures close to
///   `full`: only the meta page is skipped.
/// * [`SyncPolicy::Interval`] - `NONODB_SYNC_INTERVAL_MS=N`. The environment is
///   opened with `NO_SYNC` and a background syncer calls `force_sync` every `N`
///   milliseconds. `Durability::Full` stays durable because it still syncs per
///   commit; `relaxed` writes now cost a memory copy, at the price of losing up
///   to `N` milliseconds of `relaxed` writes on a crash.
/// * [`SyncPolicy::Unsafe`] - `NONODB_ALLOW_UNSAFE_WRITES=1` without an
///   interval. Nothing is synced unless a write asks for `Durability::Full`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    PerCommit,
    Interval,
    Unsafe,
}

/// Decides the environment sync policy from the two opt-in flags.
///
/// `unsafe_writes` wins: an operator who allowed crash loss explicitly should
/// not silently get interval syncing on top.
pub fn sync_policy(unsafe_writes: bool, sync_interval_ms: u64) -> SyncPolicy {
    if unsafe_writes {
        SyncPolicy::Unsafe
    } else if sync_interval_ms > 0 {
        SyncPolicy::Interval
    } else {
        SyncPolicy::PerCommit
    }
}

/// `NONODB_SYNC_INTERVAL_MS`, `0` meaning "no interval syncing".
pub fn sync_interval_ms() -> u64 {
    env_u64("NONODB_SYNC_INTERVAL_MS")
}

fn env_u64(name: &str) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}

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
/// Internal cluster control-plane collection. Hidden from normal collection
/// listings and backup exclusions; replicated like ordinary documents.
pub const CONTROL_DB: &str = "__nonodb_control";
const DEFAULT_SUBSCRIPTION_LEASE_SECONDS: u64 = 600;
/// Data format version (for future migrations).
pub const SCHEMA_VERSION: u32 = 1;
/// Soft document size limit (serialized JSON).
/// LMDB technically supports larger values (overflow pages), but a large
/// document means an expensive update (the whole value is rewritten).
pub const MAX_DOCUMENT_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Serialize)]
pub struct CollectionSpace {
    pub collection: String,
    pub records: u64,
    pub data_bytes: u64,
    pub index_bytes: u64,
    pub indexes: u64,
    pub history_bytes: u64,
    pub ttl_bytes: u64,
    pub total_bytes: u64,
    pub requests_last_minute: u64,
    pub avg_requests_per_second: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DatabaseSpace {
    pub map_size: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub page_size: u64,
    pub total_records: u64,
    pub collection_bytes: u64,
    pub collections: Vec<CollectionSpace>,
}

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

/// Result of an offline compaction pass.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct CompactReport {
    pub before_bytes: u64,
    pub after_bytes: u64,
}

/// Restores the database if a previous compaction crashed between its two swaps.
///
/// The normal compaction sequence is:
/// `data.mdb -> data.mdb.before-compact` then `data.mdb.compacting -> data.mdb`.
/// A crash can leave only the backup or the compacting copy in place.
fn recover_interrupted_compaction(data: &Path) -> Result<()> {
    let source = data.join("data.mdb");
    let backup = data.join("data.mdb.before-compact");
    let tmp = data.join("data.mdb.compacting");
    let mut changed = false;
    if !source.exists() {
        if backup.is_file() {
            std::fs::rename(&backup, &source)?;
            changed = true;
        } else if tmp.is_file() {
            std::fs::rename(&tmp, &source)?;
            changed = true;
        }
    }
    if source.exists() {
        if tmp.exists() {
            std::fs::remove_file(&tmp)?;
            changed = true;
        }
        if backup.exists() {
            std::fs::remove_file(&backup)?;
            changed = true;
        }
    }
    if changed {
        if let Ok(dir) = std::fs::File::open(data) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

/// Rewrites `data.mdb` through LMDB's compacting copy and swaps it in.
///
/// LMDB never shrinks the file when rows are deleted; this is the offline way
/// to return freed pages to the filesystem. The environment must be closed:
/// the directory is opened read-only, a compacted copy is written to
/// `data.mdb.compacting`, fsynced, and atomically swapped in. The previous file
/// is kept as `data.mdb.before-compact` until the swap succeeds.
pub fn compact_data_dir(data: &Path) -> Result<CompactReport> {
    let source = data.join("data.mdb");
    if !source.is_file() {
        return Err(Error::Query(format!("{} has no data.mdb", data.display())));
    }
    let before_bytes = std::fs::metadata(&source)?.len();
    let tmp = data.join("data.mdb.compacting");
    if tmp.exists() {
        std::fs::remove_file(&tmp)?;
    }
    let env = unsafe {
        EnvOpenOptions::new()
            .flags(EnvFlags::READ_ONLY)
            .open(data)
            .map_err(|e| Error::Query(format!("open {}: {e}", data.display())))?
    };
    let file = env.copy_to_file(&tmp, heed::CompactionOption::Enabled)?;
    file.sync_all()?;
    env.prepare_for_closing().wait();

    let backup = data.join("data.mdb.before-compact");
    if backup.exists() {
        std::fs::remove_file(&backup)?;
    }
    std::fs::rename(&source, &backup)?;
    if let Err(error) = std::fs::rename(&tmp, &source) {
        let _ = std::fs::rename(&backup, &source);
        return Err(Error::Query(format!(
            "compaction swap failed, original restored: {error}"
        )));
    }
    std::fs::remove_file(&backup)?;
    // Persist the directory entry so a crash cannot resurrect the old inode.
    if let Ok(dir) = std::fs::File::open(data) {
        let _ = dir.sync_all();
    }
    let after_bytes = std::fs::metadata(&source)?.len();
    Ok(CompactReport {
        before_bytes,
        after_bytes,
    })
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
        recover_interrupted_compaction(path)?;

        // SAFETY: the file is not modified outside LMDB and is not opened
        // concurrently with another `map_size` (the daemon is the sole owner).
        // LMDB sync flags are environment-wide, not transaction-wide, so the
        // open mode decides what a commit does and `force_sync` decides what
        // "durable" means:
        //
        // * `NO_META_SYNC` (default): the commit syncs data pages itself and
        //   only the meta page is flushed lazily. `relaxed` therefore costs
        //   almost the same as `full` - see `sync_policy` below.
        // * `NO_SYNC`: the commit touches nothing; only an explicit
        //   `force_sync` puts data on disk. This is what makes `relaxed`
        //   actually cheap, and it is why the daemon runs a syncer thread when
        //   `NONODB_SYNC_INTERVAL_MS` is set.
        let flags = match sync_policy(
            env_flag("NONODB_ALLOW_UNSAFE_WRITES"),
            env_u64("NONODB_SYNC_INTERVAL_MS"),
        ) {
            SyncPolicy::Unsafe => EnvFlags::NO_SYNC,
            SyncPolicy::Interval => EnvFlags::NO_SYNC,
            SyncPolicy::PerCommit => EnvFlags::NO_META_SYNC,
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
        let store = Self {
            env,
            history,
            ttl,
            txn_gate: TxnGate::default(),
        };
        store.ensure_timeseries_bucket_indexes()?;
        Ok(store)
    }

    fn ensure_timeseries_bucket_indexes(&self) -> Result<()> {
        for name in self.collections()? {
            if self.timeseries_settings(&name)?.is_none() {
                continue;
            }
            let mut wtxn = self.write_txn()?;
            crate::timeseries::ensure_bucket_index(&mut wtxn, &self.env, &name)?;
            wtxn.commit()?;
        }
        Ok(())
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
    /// Consumes the store and waits until LMDB releases the environment path.
    ///
    /// Useful for offline maintenance (compaction, file replacement) performed
    /// in the same process. The daemon normally relies on process exit.
    pub fn close(self) {
        let env = self.env.clone();
        drop(self);
        env.prepare_for_closing().wait();
    }

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
        let _replication: Database<Bytes, Bytes> =
            env.create_database(&mut wtxn, Some(REPLICATION_DB))?;
        wtxn.commit()?;
        env.force_sync()?;
        Ok((history, ttl))
    }

    fn validate_name(name: &str) -> Result<()> {
        if name.is_empty() || name.len() > 200 || name.contains('\0') {
            return Err(Error::InvalidCollectionName(name.to_string()));
        }
        if matches!(name, META_DB | HISTORY_DB | TTL_DB | REPLICATION_DB) {
            return Err(Error::InvalidCollectionName(format!(
                "{name} is reserved for internal metadata"
            )));
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

    fn ttl_field_key(name: &str) -> String {
        format!("ttl_field:{name}")
    }

    fn ttl_field_from_ro(
        meta: Database<Bytes, Bytes>,
        rtxn: &RoTxn<'_>,
        name: &str,
    ) -> Result<Option<String>> {
        Ok(meta
            .get(rtxn, Self::ttl_field_key(name).as_bytes())?
            .and_then(|b| std::str::from_utf8(b).ok())
            .map(ToString::to_string))
    }

    fn ttl_field_from_rw(
        meta: Database<Bytes, Bytes>,
        wtxn: &RwTxn<'_>,
        name: &str,
    ) -> Result<Option<String>> {
        Ok(meta
            .get(wtxn, Self::ttl_field_key(name).as_bytes())?
            .and_then(|b| std::str::from_utf8(b).ok())
            .map(ToString::to_string))
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
    /// Existing collections cannot be converted between `standard`, `vault`,
    /// and `timeseries`; create a new collection and reinsert the data instead.
    pub fn create_collection(
        &self,
        name: &str,
        profile: CollectionProfile,
    ) -> Result<Collection<'_>> {
        self.create_collection_with_ttl(name, profile, None)
    }

    /// Seals all pending time-series rows into columnar buckets.
    ///
    /// Open buckets keep rows as raw JSON so inserts stay O(1). Sealing is
    /// automatic when a bucket fills or the window advances; this method lets
    /// operators force it (for example before a compression report or a cold
    /// backup).
    pub fn seal_timeseries(&self, name: &str) -> Result<usize> {
        let mut wtxn = self.write_txn()?;
        let collection = self.collection_in(&mut wtxn, name)?;
        let Some(settings) = collection.timeseries_settings_in(&wtxn)? else {
            return Ok(0);
        };
        let sealed = crate::timeseries::seal_all(&mut wtxn, &self.env, name, &settings)?;
        wtxn.commit()?;
        Ok(sealed)
    }

    /// Re-encodes sealed buckets with at least `min_deleted` deleted rows,
    /// dropping their column values while keeping synthetic ids stable.
    pub fn compact_timeseries_buckets(
        &self,
        name: &str,
        min_deleted: usize,
    ) -> Result<crate::timeseries::BucketCompactReport> {
        let mut wtxn = self.write_txn()?;
        let collection = self.collection_in(&mut wtxn, name)?;
        let Some(settings) = collection.timeseries_settings_in(&wtxn)? else {
            return Err(Error::Query(format!(
                "collection `{name}` is not a time-series collection"
            )));
        };
        let report = crate::timeseries::compact_deleted_buckets(
            &mut wtxn,
            &self.env,
            name,
            &settings,
            min_deleted,
        )?;
        wtxn.commit()?;
        Ok(report)
    }

    /// Removes time-series rows older than `before_ms`.
    pub fn retain_timeseries(
        &self,
        name: &str,
        before_ms: i64,
    ) -> Result<crate::timeseries::RetainReport> {
        let mut wtxn = self.write_txn()?;
        let report = self.retain_timeseries_in(&mut wtxn, name, before_ms)?;
        wtxn.commit()?;
        Ok(report)
    }

    /// Retention variant for an existing writer transaction.
    pub fn retain_timeseries_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        name: &str,
        before_ms: i64,
    ) -> Result<crate::timeseries::RetainReport> {
        let collection = self.collection_in(wtxn, name)?;
        collection.retain_timeseries_txn(wtxn, before_ms)
    }

    /// Creates a time-series collection backed by columnar buckets.
    pub fn create_timeseries_collection(
        &self,
        name: &str,
        settings: &crate::timeseries::Settings,
    ) -> Result<Collection<'_>> {
        settings.validate()?;
        let mut wtxn = self.write_txn()?;
        let collection = self.create_collection_with_ttl_and_purge_in(
            &mut wtxn,
            name,
            CollectionProfile::TimeSeries,
            None,
            None,
        )?;
        collection.set_timeseries_settings_in(&mut wtxn, settings)?;
        crate::timeseries::init_bucket_index(&mut wtxn, &self.env, name)?;
        wtxn.commit()?;
        Ok(collection)
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
        let ttl_field = Self::ttl_field_from_rw(meta, wtxn, name)?;
        let purge = Self::ttl_purge_from_rw(meta, wtxn, name)?;
        let history = Self::history_from_rw(meta, wtxn, name)?;
        Ok(self.wrap(
            name,
            db,
            meta,
            profile,
            ttl_default,
            ttl_field,
            purge,
            history,
        ))
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
            meta.delete(&mut *wtxn, Self::ttl_field_key(name).as_bytes())?;
        } else {
            meta.delete(&mut *wtxn, Self::ttl_key(name).as_bytes())?;
            meta.delete(&mut *wtxn, Self::ttl_field_key(name).as_bytes())?;
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

    /// Sets a date-field TTL, replacing any insertion-based collection TTL.
    pub fn set_collection_ttl_after(
        &self,
        name: &str,
        field: &str,
        ttl_seconds: u64,
        purge_history: Option<bool>,
    ) -> Result<()> {
        let mut wtxn = self.write_txn()?;
        self.set_collection_ttl_after_in(&mut wtxn, name, field, ttl_seconds, purge_history)?;
        wtxn.commit()?;
        Ok(())
    }

    /// Date-field TTL setter for an existing writer transaction.
    pub fn set_collection_ttl_after_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        name: &str,
        field: &str,
        ttl_seconds: u64,
        purge_history: Option<bool>,
    ) -> Result<()> {
        Self::validate_name(name)?;
        if field.trim().is_empty() {
            return Err(Error::InvalidTtl("TTL field must not be empty".into()));
        }
        let Some(profile) = self.collection_profile_in_rw(wtxn, name)? else {
            return Err(Error::Query(format!("collection `{name}` does not exist")));
        };
        if profile.is_vault() {
            return Err(Error::Vault {
                code: "vault_ttl_field_unsupported",
                message: "date-field TTL is not supported for vault collections".into(),
            });
        }
        Self::validate_ttl(ttl_seconds)?;
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        meta.put(
            &mut *wtxn,
            Self::ttl_key(name).as_bytes(),
            ttl_seconds.to_string().as_bytes(),
        )?;
        meta.put(
            &mut *wtxn,
            Self::ttl_field_key(name).as_bytes(),
            field.as_bytes(),
        )?;
        if let Some(purge) = purge_history {
            if purge {
                meta.put(&mut *wtxn, Self::ttl_purge_key(name).as_bytes(), b"1")?;
            } else {
                meta.delete(&mut *wtxn, Self::ttl_purge_key(name).as_bytes())?;
            }
        }
        Ok(())
    }

    /// Returns `(field, seconds)` for a date-field TTL collection.
    pub fn collection_ttl_after(&self, name: &str) -> Result<Option<(String, u64)>> {
        let rtxn = self.read_txn()?;
        let meta = self
            .env
            .open_database::<Bytes, Bytes>(&rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        let field = Self::ttl_field_from_ro(meta, &rtxn, name)?;
        let seconds = Self::ttl_from_ro(meta, &rtxn, name)?;
        rtxn.commit()?;
        Ok(field.zip(seconds))
    }

    /// Backfills one page of date-field TTL metadata.
    #[allow(clippy::too_many_arguments)]
    pub fn backfill_collection_ttl_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        name: &str,
        field: &str,
        ttl_seconds: u64,
        after: Option<&Value>,
        limit: usize,
        dry_run: bool,
    ) -> Result<crate::ttl::BackfillPage> {
        Self::validate_ttl(ttl_seconds)?;
        let collection = self.collection_in(wtxn, name)?;
        collection.backfill_ttl_in(wtxn, field, ttl_seconds, after, limit, dry_run)
    }

    /// Returns a collection's default TTL, if any.
    /// Time-series settings for `name`, or `None` when it is not a
    /// time-series collection.
    pub fn timeseries_settings(&self, name: &str) -> Result<Option<crate::timeseries::Settings>> {
        let rtxn = self.read_txn()?;
        let collection = self.collection_at(&rtxn, name)?;
        let out = match collection {
            Some(collection) => collection.timeseries_settings_in(&rtxn)?,
            None => None,
        };
        rtxn.commit()?;
        Ok(out)
    }

    /// Storage statistics for a time-series collection.
    pub fn timeseries_stats(&self, name: &str) -> Result<Option<crate::timeseries::Stats>> {
        let Some(settings) = self.timeseries_settings(name)? else {
            return Ok(None);
        };
        let rtxn = self.read_txn()?;
        let out = crate::timeseries::stats(&rtxn, self.env(), name, &settings)?;
        rtxn.commit()?;
        Ok(Some(out))
    }

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

    // ---------------------------------------------------------------------
    // Streams: durable offsets and consumer groups.
    // ---------------------------------------------------------------------

    fn stream_key(name: &str) -> String {
        format!("{}{name}", crate::streams::STREAM_PREFIX)
    }

    fn stream_seq_key(name: &str) -> String {
        format!("{}{name}", crate::streams::STREAM_SEQ_PREFIX)
    }

    fn stream_coll_key(coll: &str) -> String {
        format!("stream:coll:{coll}")
    }

    fn stream_group_key(stream: &str, group: &str) -> String {
        format!("{}{stream}:{group}", crate::streams::GROUP_PREFIX)
    }

    /// Creates or replaces a stream definition and its offset index.
    pub fn set_stream(&self, def: &StreamDef) -> Result<()> {
        let mut wtxn = self.write_txn()?;
        self.set_stream_in(&mut wtxn, def)?;
        wtxn.commit()?;
        Ok(())
    }

    /// Creates or replaces a stream definition inside a writer transaction.
    pub fn set_stream_in(&self, wtxn: &mut RwTxn<'_>, def: &StreamDef) -> Result<()> {
        def.validate()?;
        let Some(profile) = self.collection_profile_in_rw(wtxn, &def.coll)? else {
            return Err(Error::Query(format!(
                "collection `{}` does not exist",
                def.coll
            )));
        };
        // Time-series buckets and vault collections do not store `_seq` on the
        // documents this feature reads, so a stream over them would silently
        // deliver nothing (or the wrong rows). Reject instead of lying.
        if profile != crate::vault::CollectionProfile::Standard {
            return Err(Error::Query(format!(
                "collection `{}` cannot back a stream: only standard collections are supported",
                def.coll
            )));
        }
        let collection = self.collection_in(wtxn, &def.coll)?;
        collection.create_index_fields_txn(
            wtxn,
            std::slice::from_ref(&def.seq_field),
            false,
            &[],
            false,
            false,
            None,
        )?;
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        let raw = serde_json::to_vec(def)?;
        meta.put(&mut *wtxn, Self::stream_key(&def.name).as_bytes(), &raw)?;
        // Reverse lookup used by the insert hot path: one key per collection.
        meta.put(
            &mut *wtxn,
            Self::stream_coll_key(&def.coll).as_bytes(),
            def.name.as_bytes(),
        )?;
        Ok(())
    }

    /// Removes a stream definition. Data and groups are left in place.
    pub fn drop_stream_in(&self, wtxn: &mut RwTxn<'_>, name: &str) -> Result<bool> {
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        let existing = meta
            .get(&*wtxn, Self::stream_key(name).as_bytes())?
            .and_then(|raw| serde_json::from_slice::<StreamDef>(raw).ok());
        let existed = meta.delete(&mut *wtxn, Self::stream_key(name).as_bytes())?;
        if let Some(def) = existing {
            let owner = meta
                .get(&*wtxn, Self::stream_coll_key(&def.coll).as_bytes())?
                .and_then(|raw| std::str::from_utf8(raw).ok());
            if owner == Some(name) {
                meta.delete(&mut *wtxn, Self::stream_coll_key(&def.coll).as_bytes())?;
            }
        }
        Ok(existed)
    }

    /// Returns one stream definition.
    pub fn stream(&self, name: &str) -> Result<Option<StreamDef>> {
        let rtxn = self.read_txn()?;
        let out = self.stream_in(&rtxn, name)?;
        rtxn.commit()?;
        Ok(out)
    }

    /// Stream lookup inside an existing read transaction.
    pub fn stream_in(&self, rtxn: &RoTxn<'_>, name: &str) -> Result<Option<StreamDef>> {
        let Some(meta) = self
            .env
            .open_database::<Bytes, Bytes>(rtxn, Some(META_DB))?
        else {
            return Ok(None);
        };
        Ok(meta
            .get(rtxn, Self::stream_key(name).as_bytes())?
            .and_then(|raw| serde_json::from_slice(raw).ok()))
    }

    /// Stream lookup inside a writer transaction.
    pub fn stream_in_rw(&self, wtxn: &RwTxn<'_>, name: &str) -> Result<Option<StreamDef>> {
        let Some(meta) = self
            .env
            .open_database::<Bytes, Bytes>(wtxn, Some(META_DB))?
        else {
            return Ok(None);
        };
        Ok(meta
            .get(wtxn, Self::stream_key(name).as_bytes())?
            .and_then(|raw| serde_json::from_slice(raw).ok()))
    }

    /// Returns the stream defined for a collection, if any.
    ///
    /// This runs on the insert path, so it uses the reverse `stream:coll:<coll>`
    /// key instead of scanning every definition.
    pub fn collection_stream_in_rw(
        &self,
        wtxn: &RwTxn<'_>,
        coll: &str,
    ) -> Result<Option<StreamDef>> {
        let Some(meta) = self
            .env
            .open_database::<Bytes, Bytes>(wtxn, Some(META_DB))?
        else {
            return Ok(None);
        };
        let Some(name) = meta
            .get(wtxn, Self::stream_coll_key(coll).as_bytes())?
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .map(str::to_string)
        else {
            return Ok(None);
        };
        self.stream_in_rw(wtxn, &name)
    }

    /// Lists every stream definition.
    pub fn streams(&self) -> Result<Vec<StreamDef>> {
        let rtxn = self.read_txn()?;
        let out = self.streams_in(&rtxn)?;
        rtxn.commit()?;
        Ok(out)
    }

    /// Lists stream definitions inside a read transaction.
    pub fn streams_in(&self, rtxn: &RoTxn<'_>) -> Result<Vec<StreamDef>> {
        let Some(meta) = self
            .env
            .open_database::<Bytes, Bytes>(rtxn, Some(META_DB))?
        else {
            return Ok(Vec::new());
        };
        let prefix = crate::streams::STREAM_PREFIX.as_bytes();
        let mut out = Vec::new();
        for item in meta.iter(rtxn)? {
            let (key, value) = item?;
            if !key.starts_with(prefix)
                || key.starts_with(crate::streams::STREAM_SEQ_PREFIX.as_bytes())
                || key.starts_with(b"stream:coll:")
            {
                continue;
            }
            if let Ok(def) = serde_json::from_slice::<StreamDef>(value) {
                out.push(def);
            }
        }
        out.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(out)
    }

    /// Lists stream definitions inside a writer transaction.
    pub fn streams_in_rw(&self, wtxn: &RwTxn<'_>) -> Result<Vec<StreamDef>> {
        let Some(meta) = self
            .env
            .open_database::<Bytes, Bytes>(wtxn, Some(META_DB))?
        else {
            return Ok(Vec::new());
        };
        let prefix = crate::streams::STREAM_PREFIX.as_bytes();
        let mut out = Vec::new();
        for item in meta.iter(wtxn)? {
            let (key, value) = item?;
            if !key.starts_with(prefix)
                || key.starts_with(crate::streams::STREAM_SEQ_PREFIX.as_bytes())
                || key.starts_with(b"stream:coll:")
            {
                continue;
            }
            if let Ok(def) = serde_json::from_slice::<StreamDef>(value) {
                out.push(def);
            }
        }
        Ok(out)
    }

    /// Highest offset allocated for a stream, or `0` when nothing was written.
    pub fn stream_head(&self, name: &str) -> Result<u64> {
        let rtxn = self.read_txn()?;
        let out = self.stream_head_in(&rtxn, name)?;
        rtxn.commit()?;
        Ok(out)
    }

    /// Highest allocated offset inside a read transaction.
    pub fn stream_head_in(&self, rtxn: &RoTxn<'_>, name: &str) -> Result<u64> {
        let Some(meta) = self
            .env
            .open_database::<Bytes, Bytes>(rtxn, Some(META_DB))?
        else {
            return Ok(0);
        };
        Ok(meta
            .get(rtxn, Self::stream_seq_key(name).as_bytes())?
            .and_then(|raw| {
                std::str::from_utf8(raw)
                    .ok()
                    .and_then(|text| text.parse().ok())
            })
            .unwrap_or(0))
    }

    /// Allocates the next offset for a stream inside a writer transaction.
    pub fn allocate_stream_seq_in(&self, wtxn: &mut RwTxn<'_>, name: &str) -> Result<u64> {
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        let key = Self::stream_seq_key(name);
        let next = meta
            .get(&*wtxn, key.as_bytes())?
            .and_then(|raw| {
                std::str::from_utf8(raw)
                    .ok()
                    .and_then(|text| text.parse::<u64>().ok())
            })
            .unwrap_or(0)
            + 1;
        meta.put(&mut *wtxn, key.as_bytes(), next.to_string().as_bytes())?;
        Ok(next)
    }

    /// Raises the stream counter to at least `seq`.
    ///
    /// Replicas apply the primary's `_seq` values verbatim, so the local counter
    /// must follow them; otherwise a promoted replica would reissue offsets that
    /// already exist.
    pub fn observe_stream_seq_in(&self, wtxn: &mut RwTxn<'_>, name: &str, seq: u64) -> Result<()> {
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        let key = Self::stream_seq_key(name);
        let current = meta
            .get(&*wtxn, key.as_bytes())?
            .and_then(|raw| {
                std::str::from_utf8(raw)
                    .ok()
                    .and_then(|text| text.parse::<u64>().ok())
            })
            .unwrap_or(0);
        if seq > current {
            meta.put(&mut *wtxn, key.as_bytes(), seq.to_string().as_bytes())?;
        }
        Ok(())
    }

    /// Returns one consumer group.
    pub fn stream_group(&self, stream: &str, group: &str) -> Result<Option<StreamGroup>> {
        let rtxn = self.read_txn()?;
        let out = self.stream_group_in(&rtxn, stream, group)?;
        rtxn.commit()?;
        Ok(out)
    }

    /// Consumer-group lookup inside a read transaction.
    pub fn stream_group_in(
        &self,
        rtxn: &RoTxn<'_>,
        stream: &str,
        group: &str,
    ) -> Result<Option<StreamGroup>> {
        let Some(meta) = self
            .env
            .open_database::<Bytes, Bytes>(rtxn, Some(META_DB))?
        else {
            return Ok(None);
        };
        Ok(meta
            .get(rtxn, Self::stream_group_key(stream, group).as_bytes())?
            .and_then(|raw| serde_json::from_slice(raw).ok()))
    }

    /// Consumer-group lookup inside a writer transaction.
    pub fn stream_group_in_rw(
        &self,
        wtxn: &RwTxn<'_>,
        stream: &str,
        group: &str,
    ) -> Result<Option<StreamGroup>> {
        let Some(meta) = self
            .env
            .open_database::<Bytes, Bytes>(wtxn, Some(META_DB))?
        else {
            return Ok(None);
        };
        Ok(meta
            .get(wtxn, Self::stream_group_key(stream, group).as_bytes())?
            .and_then(|raw| serde_json::from_slice(raw).ok()))
    }

    /// Lists groups for one stream, or every group when `stream` is `None`.
    pub fn stream_groups(&self, stream: Option<&str>) -> Result<Vec<StreamGroup>> {
        let rtxn = self.read_txn()?;
        let Some(meta) = self
            .env
            .open_database::<Bytes, Bytes>(&rtxn, Some(META_DB))?
        else {
            return Ok(Vec::new());
        };
        let prefix = crate::streams::GROUP_PREFIX.as_bytes();
        let mut out = Vec::new();
        for item in meta.iter(&rtxn)? {
            let (key, value) = item?;
            if !key.starts_with(prefix) {
                continue;
            }
            let Ok(group) = serde_json::from_slice::<StreamGroup>(value) else {
                continue;
            };
            if stream.is_some_and(|name| group.stream != name) {
                continue;
            }
            out.push(group);
        }
        out.sort_by(|left, right| {
            left.stream
                .cmp(&right.stream)
                .then_with(|| left.group.cmp(&right.group))
        });
        rtxn.commit()?;
        Ok(out)
    }

    /// Writes a consumer group inside a writer transaction.
    pub fn put_stream_group_in(&self, wtxn: &mut RwTxn<'_>, group: &StreamGroup) -> Result<()> {
        group.validate()?;
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        let raw = serde_json::to_vec(group)?;
        meta.put(
            &mut *wtxn,
            Self::stream_group_key(&group.stream, &group.group).as_bytes(),
            &raw,
        )?;
        Ok(())
    }

    /// Creates a consumer group at the requested position.
    pub fn create_stream_group_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        stream: &str,
        group: &str,
        start: StartAt,
        max_attempts: u32,
    ) -> Result<StreamGroup> {
        crate::streams::validate_name(group, "group")?;
        if self.stream_in_rw(wtxn, stream)?.is_none() {
            return Err(Error::Query(format!("stream `{stream}` does not exist")));
        }
        if self.stream_group_in_rw(wtxn, stream, group)?.is_some() {
            return Err(Error::DuplicateKey(group.to_string()));
        }
        let head = self.stream_head_in(wtxn, stream)?;
        let committed = match start {
            StartAt::Latest => head,
            StartAt::Earliest => 0,
            StartAt::Seq(seq) => {
                if seq > head {
                    return Err(Error::Query(format!(
                        "start offset {seq} is past the stream head {head}"
                    )));
                }
                seq
            }
        };
        let mut out = StreamGroup::new(stream, group, committed);
        out.max_attempts = max_attempts.max(1);
        out.touch();
        self.put_stream_group_in(wtxn, &out)?;
        Ok(out)
    }

    /// Moves a group and clears its in-flight batch.
    pub fn reset_stream_group_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        stream: &str,
        group: &str,
        to: ResetTo,
    ) -> Result<StreamGroup> {
        let mut current = self
            .stream_group_in_rw(wtxn, stream, group)?
            .ok_or_else(|| {
                Error::Query(format!("stream group `{stream}:{group}` does not exist"))
            })?;
        let head = self.stream_head_in(wtxn, stream)?;
        current.committed_seq = match to {
            // Resume from the oldest event retention left behind, so resetting a
            // truncated group does not immediately fail again.
            ResetTo::Earliest => self
                .stream_oldest_seq_in(wtxn, stream)?
                .map_or(0, |oldest| oldest.saturating_sub(1)),
            ResetTo::Latest => head,
            ResetTo::Seq(seq) => {
                if seq > head {
                    return Err(Error::Query(format!(
                        "reset offset {seq} is past the stream head {head}"
                    )));
                }
                seq
            }
        };
        current.truncated_below = None;
        current.next_seq = current.committed_seq;
        current.touch();
        self.clear_stream_leases_in(wtxn, stream, group)?;
        self.put_stream_group_in(wtxn, &current)?;
        Ok(current)
    }

    /// Deletes a group and its offset.
    pub fn delete_stream_group_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        stream: &str,
        group: &str,
    ) -> Result<bool> {
        // A recreated group must not inherit the previous group's batches: drop
        // its leases and dead letters together with the offset.
        self.clear_stream_leases_in(wtxn, stream, group)?;
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        Ok(meta.delete(&mut *wtxn, Self::stream_group_key(stream, group).as_bytes())?)
    }

    /// Deletes a group, its offset, its leases, and its dead letters.
    pub fn delete_stream_group(&self, stream: &str, group: &str) -> Result<bool> {
        let mut wtxn = self.write_txn()?;
        let out = self.delete_stream_group_in(&mut wtxn, stream, group)?;
        wtxn.commit()?;
        Ok(out)
    }

    /// Creates a consumer group in its own transaction.
    pub fn create_stream_group(
        &self,
        stream: &str,
        group: &str,
        start: StartAt,
    ) -> Result<StreamGroup> {
        self.create_stream_group_with_attempts(stream, group, start, 5)
    }

    /// Creates a consumer group with an explicit redelivery budget.
    pub fn create_stream_group_with_attempts(
        &self,
        stream: &str,
        group: &str,
        start: StartAt,
        max_attempts: u32,
    ) -> Result<StreamGroup> {
        let mut wtxn = self.write_txn()?;
        let out = self.create_stream_group_in(&mut wtxn, stream, group, start, max_attempts)?;
        wtxn.commit()?;
        Ok(out)
    }

    /// Moves a group offset in its own transaction.
    pub fn reset_stream_group(
        &self,
        stream: &str,
        group: &str,
        to: ResetTo,
    ) -> Result<StreamGroup> {
        let mut wtxn = self.write_txn()?;
        let out = self.reset_stream_group_in(&mut wtxn, stream, group, to)?;
        wtxn.commit()?;
        Ok(out)
    }

    /// Claims the next batch for a member, in its own transaction.
    pub fn claim_stream_lease(
        &self,
        stream: &str,
        group: &str,
        member: &str,
        limit: usize,
        lease_ms: u64,
    ) -> Result<Option<StreamLease>> {
        let mut wtxn = self.write_txn()?;
        match self.claim_stream_lease_in(&mut wtxn, stream, group, member, limit, lease_ms) {
            Ok(lease) => {
                wtxn.commit()?;
                Ok(lease)
            }
            Err(error) => {
                // Keep the `truncated_below` marker written on the error path.
                let _ = wtxn.commit();
                Err(error)
            }
        }
    }

    /// Acknowledges a batch in its own transaction.
    pub fn ack_stream_lease(
        &self,
        stream: &str,
        group: &str,
        lease_id: &str,
        seq: u64,
    ) -> Result<StreamLease> {
        let mut wtxn = self.write_txn()?;
        let out = self.ack_stream_lease_in(&mut wtxn, stream, group, lease_id, seq)?;
        wtxn.commit()?;
        Ok(out)
    }

    /// Releases a batch for redelivery in its own transaction.
    pub fn nack_stream_lease(
        &self,
        stream: &str,
        group: &str,
        lease_id: &str,
        retry_in_ms: u64,
    ) -> Result<StreamLease> {
        let mut wtxn = self.write_txn()?;
        let out = self.nack_stream_lease_in(&mut wtxn, stream, group, lease_id, retry_in_ms)?;
        wtxn.commit()?;
        Ok(out)
    }

    fn stream_lease_key(stream: &str, group: &str, from_seq: u64) -> String {
        format!(
            "{}{stream}:{group}:{from_seq:020}",
            crate::streams::LEASE_PREFIX
        )
    }

    fn stream_dead_key(stream: &str, group: &str, from_seq: u64) -> String {
        format!(
            "{}{stream}:{group}:{from_seq:020}",
            crate::streams::DEAD_PREFIX
        )
    }

    fn put_stream_lease_in(&self, wtxn: &mut RwTxn<'_>, lease: &StreamLease) -> Result<()> {
        lease.validate()?;
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        let raw = serde_json::to_vec(lease)?;
        meta.put(
            &mut *wtxn,
            Self::stream_lease_key(&lease.stream, &lease.group, lease.from_seq).as_bytes(),
            &raw,
        )?;
        Ok(())
    }

    fn put_stream_dead_in(&self, wtxn: &mut RwTxn<'_>, lease: &StreamLease) -> Result<()> {
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        let raw = serde_json::to_vec(lease)?;
        meta.put(
            &mut *wtxn,
            Self::stream_dead_key(&lease.stream, &lease.group, lease.from_seq).as_bytes(),
            &raw,
        )?;
        Ok(())
    }

    fn delete_stream_lease_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        stream: &str,
        group: &str,
        from_seq: u64,
    ) -> Result<()> {
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        meta.delete(
            &mut *wtxn,
            Self::stream_lease_key(stream, group, from_seq).as_bytes(),
        )?;
        Ok(())
    }

    /// Reads one lease record.
    pub fn stream_lease(
        &self,
        stream: &str,
        group: &str,
        from_seq: u64,
    ) -> Result<Option<StreamLease>> {
        let rtxn = self.read_txn()?;
        let Some(meta) = self
            .env
            .open_database::<Bytes, Bytes>(&rtxn, Some(META_DB))?
        else {
            return Ok(None);
        };
        let out = meta
            .get(
                &rtxn,
                Self::stream_lease_key(stream, group, from_seq).as_bytes(),
            )?
            .and_then(|raw| serde_json::from_slice(raw).ok());
        rtxn.commit()?;
        Ok(out)
    }

    /// Lists in-flight and pending batches of one group.
    /// Cheap read-only check used to avoid an empty write transaction when a
    /// consumer long-polls a caught-up group.
    ///
    /// Returns `true` when a claim would make progress: an expired lease is
    /// waiting for recovery, a released range is waiting for redelivery, or
    /// unpublished events exist above the committed offset.
    pub fn stream_claim_ready(&self, stream: &str, group: &str) -> Result<bool> {
        let rtxn = self.read_txn()?;
        let Some(group_row) = self.stream_group_in(&rtxn, stream, group)? else {
            // Let the writer raise the real "group does not exist" error instead
            // of silently reporting a caught-up group.
            return Ok(true);
        };
        let now = crate::streams::now_ms();
        let leases = self.stream_leases_in(&rtxn, stream, group)?;
        let ready = leases.iter().any(|lease| {
            (lease.status == crate::streams::LeaseStatus::Available && lease.lease_until <= now)
                || lease.is_expired(now)
        }) || {
            let leased_through = leases.iter().map(|lease| lease.to_seq).max().unwrap_or(0);
            let next_seq = group_row
                .committed_seq
                .max(leased_through)
                .saturating_add(1);
            let head = self.stream_head_in(&rtxn, stream)?;
            next_seq <= head
        };
        rtxn.commit()?;
        Ok(ready)
    }

    pub fn stream_leases(&self, stream: &str, group: &str) -> Result<Vec<StreamLease>> {
        let rtxn = self.read_txn()?;
        let out = self.stream_leases_in(&rtxn, stream, group)?;
        rtxn.commit()?;
        Ok(out)
    }

    fn stream_leases_in(
        &self,
        rtxn: &RoTxn<'_>,
        stream: &str,
        group: &str,
    ) -> Result<Vec<StreamLease>> {
        let Some(meta) = self
            .env
            .open_database::<Bytes, Bytes>(rtxn, Some(META_DB))?
        else {
            return Ok(Vec::new());
        };
        let prefix = format!("{}{stream}:{group}:", crate::streams::LEASE_PREFIX);
        let mut out = Vec::new();
        for item in meta.iter(rtxn)? {
            let (key, value) = item?;
            if !key.starts_with(prefix.as_bytes()) {
                continue;
            }
            if let Ok(lease) = serde_json::from_slice::<StreamLease>(value) {
                out.push(lease);
            }
        }
        out.sort_by_key(|lease| lease.from_seq);
        Ok(out)
    }

    fn stream_leases_in_rw(
        &self,
        wtxn: &RwTxn<'_>,
        stream: &str,
        group: &str,
    ) -> Result<Vec<StreamLease>> {
        let Some(meta) = self
            .env
            .open_database::<Bytes, Bytes>(wtxn, Some(META_DB))?
        else {
            return Ok(Vec::new());
        };
        let prefix = format!("{}{stream}:{group}:", crate::streams::LEASE_PREFIX);
        let mut out = Vec::new();
        for item in meta.iter(wtxn)? {
            let (key, value) = item?;
            if !key.starts_with(prefix.as_bytes()) {
                continue;
            }
            if let Ok(lease) = serde_json::from_slice::<StreamLease>(value) {
                out.push(lease);
            }
        }
        out.sort_by_key(|lease| lease.from_seq);
        Ok(out)
    }

    /// Lists dead-lettered batches, optionally for one group.
    pub fn stream_dead_letters(
        &self,
        stream: Option<&str>,
        group: Option<&str>,
    ) -> Result<Vec<StreamLease>> {
        let rtxn = self.read_txn()?;
        let Some(meta) = self
            .env
            .open_database::<Bytes, Bytes>(&rtxn, Some(META_DB))?
        else {
            return Ok(Vec::new());
        };
        let prefix = crate::streams::DEAD_PREFIX.as_bytes();
        let mut out = Vec::new();
        for item in meta.iter(&rtxn)? {
            let (key, value) = item?;
            if !key.starts_with(prefix) {
                continue;
            }
            let Ok(dead) = serde_json::from_slice::<StreamLease>(value) else {
                continue;
            };
            if stream.is_some_and(|name| dead.stream != name) {
                continue;
            }
            if group.is_some_and(|name| dead.group != name) {
                continue;
            }
            out.push(dead);
        }
        out.sort_by_key(|lease| (lease.stream.clone(), lease.group.clone(), lease.from_seq));
        rtxn.commit()?;
        Ok(out)
    }

    /// Drops the node-local leases of one group without touching its committed
    /// offset.
    ///
    /// Leases are ownership records for the node that handed them out, not
    /// cluster state; a node that takes a group over (promotion, restart with a
    /// copied data directory) must clear them so the ranges are claimed again
    /// instead of being skipped. Returns the number of leases removed.
    pub fn clear_stream_leases(&self, stream: &str, group: &str) -> Result<usize> {
        let mut wtxn = self.write_txn()?;
        let count = self.stream_leases_in_rw(&wtxn, stream, group)?.len();
        self.clear_stream_leases_in(&mut wtxn, stream, group)?;
        wtxn.commit()?;
        Ok(count)
    }

    fn clear_stream_leases_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        stream: &str,
        group: &str,
    ) -> Result<()> {
        for lease in self.stream_leases_in_rw(wtxn, stream, group)? {
            self.delete_stream_lease_in(wtxn, stream, group, lease.from_seq)?;
        }
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        let prefix = format!("{}{stream}:{group}:", crate::streams::DEAD_PREFIX);
        let keys: Vec<Vec<u8>> = meta
            .iter(&*wtxn)?
            .filter_map(|item| {
                let (key, _) = item.ok()?;
                key.starts_with(prefix.as_bytes()).then(|| key.to_vec())
            })
            .collect();
        for key in keys {
            meta.delete(&mut *wtxn, &key)?;
        }
        Ok(())
    }

    /// Advances `committed_seq` across every contiguous finished batch.
    fn advance_stream_committed_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        group: &mut StreamGroup,
    ) -> Result<()> {
        loop {
            let next = group.committed_seq.saturating_add(1);
            let ready = self
                .stream_leases_in_rw(wtxn, &group.stream, &group.group)?
                .into_iter()
                .find(|lease| {
                    lease.from_seq == next && lease.status == crate::streams::LeaseStatus::Done
                });
            let Some(lease) = ready else {
                break;
            };
            group.committed_seq = lease.to_seq;
            group.touch();
            self.delete_stream_lease_in(wtxn, &group.stream, &group.group, lease.from_seq)?;
        }
        self.put_stream_group_in(wtxn, group)?;
        Ok(())
    }

    fn finish_stream_lease_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        lease: &mut StreamLease,
        dead: bool,
        error: Option<String>,
    ) -> Result<()> {
        lease.status = crate::streams::LeaseStatus::Done;
        lease.lease_id = None;
        lease.member = None;
        lease.lease_until = 0;
        lease.error = error;
        lease.touch();
        // A dead-lettered range stays in the lease table marked `Done` so the
        // committed offset can advance across it; the dead record keeps it for
        // inspection and manual retry.
        if dead {
            self.put_stream_dead_in(wtxn, lease)?;
        }
        self.put_stream_lease_in(wtxn, lease)?;
        Ok(())
    }

    /// Runs the expiry janitor over every stream and group, in its own
    /// transactions.
    ///
    /// Returns `(groups_scanned, leases_recovered)`. Expired leases are requeued
    /// or dead-lettered even when no member is currently claiming, so a crashed
    /// worker surfaces in the dead-letter and stats views on the maintenance
    /// tick.
    pub fn recover_stream_leases(&self) -> Result<(usize, usize)> {
        let mut groups = 0usize;
        let mut recovered = 0usize;
        for def in self.streams()? {
            for group in self.stream_groups(Some(&def.name))? {
                let mut wtxn = self.write_txn()?;
                let (count, _dead) =
                    self.recover_stream_leases_in(&mut wtxn, &def.name, &group.group)?;
                wtxn.commit()?;
                groups += 1;
                recovered += count;
            }
        }
        Ok((groups, recovered))
    }

    /// Requeues or dead-letters every expired lease of one group.
    /// Returns `(recovered, dead_lettered)`; the dead leases are the ranges
    /// that crossed `max_attempts` and must be replicated as dead records.
    pub fn recover_stream_leases_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        stream: &str,
        group: &str,
    ) -> Result<(usize, Vec<StreamLease>)> {
        let now = crate::streams::now_ms();
        let mut recovered = 0;
        let mut dead_leases = Vec::new();
        let mut dead_range: Option<(u64, u64)> = None;
        for mut lease in self.stream_leases_in_rw(wtxn, stream, group)? {
            if !lease.is_expired(now) {
                continue;
            }
            recovered += 1;
            if lease.attempts >= lease.max_attempts {
                dead_range = Some((lease.from_seq, lease.to_seq));
                self.finish_stream_lease_in(
                    wtxn,
                    &mut lease,
                    true,
                    Some("lease expired".to_string()),
                )?;
                dead_leases.push(lease.clone());
            } else {
                lease.status = crate::streams::LeaseStatus::Available;
                lease.lease_id = None;
                lease.member = None;
                lease.lease_until = 0;
                lease.touch();
                self.put_stream_lease_in(wtxn, &lease)?;
            }
        }
        if dead_range.is_some() {
            if let Some(mut group_row) = self.stream_group_in_rw(wtxn, stream, group)? {
                self.advance_stream_committed_in(wtxn, &mut group_row)?;
            }
        }
        Ok((recovered, dead_leases))
    }

    /// Claims a contiguous batch for a member.
    ///
    /// Pending ranges are handed out before new offsets are allocated, so a
    /// redelivery keeps the original range and its attempt counter. Returns
    /// `None` when the group is caught up.
    pub fn claim_stream_lease_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        stream: &str,
        group: &str,
        member: &str,
        limit: usize,
        lease_ms: u64,
    ) -> Result<Option<StreamLease>> {
        let mut dead = Vec::new();
        self.claim_stream_lease_in_with_dead(
            wtxn, stream, group, member, limit, lease_ms, &mut dead,
        )
    }

    /// Same as [`Store::claim_stream_lease_in`], also returning any ranges that
    /// expired into dead letters while recovering, so the writer can replicate
    /// those dead records.
    #[allow(clippy::too_many_arguments)]
    pub fn claim_stream_lease_in_with_dead(
        &self,
        wtxn: &mut RwTxn<'_>,
        stream: &str,
        group: &str,
        member: &str,
        limit: usize,
        lease_ms: u64,
        dead: &mut Vec<StreamLease>,
    ) -> Result<Option<StreamLease>> {
        if self.stream_group_in_rw(wtxn, stream, group)?.is_none() {
            return Err(Error::Query(format!(
                "stream group `{stream}:{group}` does not exist"
            )));
        }
        // Recover first: it can advance the committed offset across freshly
        // dead-lettered ranges, and this claim must not write a stale group row
        // back over that advance.
        let (_, dead_now) = self.recover_stream_leases_in(wtxn, stream, group)?;
        dead.extend(dead_now);
        let mut group_row = self
            .stream_group_in_rw(wtxn, stream, group)?
            .ok_or_else(|| {
                Error::Query(format!("stream group `{stream}:{group}` does not exist"))
            })?;
        if let Some(below) = group_row.truncated_below {
            if group_row.committed_seq < below {
                return Err(Error::StreamTruncated(format!(
                    "group `{stream}:{group}` fell behind the retained window at offset {below}"
                )));
            }
        }
        // The never-delivered offset is derived from *durable* state (the
        // committed offset) plus the leases that exist on this node, never from
        // the cached `next_seq`. Leases are node-local, so after a failover a
        // promoted replica must resume from `committed_seq + 1` and redeliver
        // ranges that the previous primary had claimed but never committed.
        let leases = self.stream_leases_in_rw(wtxn, stream, group)?;
        let leased_through = leases.iter().map(|lease| lease.to_seq).max().unwrap_or(0);
        let next_seq = group_row
            .committed_seq
            .max(leased_through)
            .saturating_add(1);

        // The oldest surviving event decides whether the next offset to deliver
        // still exists; retention may have run without touching this group. An
        // empty window (everything pruned) still truncates a group that has
        // offsets left to read.
        let head = self.stream_head_in(wtxn, stream)?;
        if next_seq <= head {
            let oldest = self.stream_oldest_seq_in(wtxn, stream)?;
            let crossed = match oldest {
                Some(oldest) => oldest > next_seq,
                // No events survive, so nothing can be delivered below the head.
                None => true,
            };
            if crossed {
                let below = oldest.unwrap_or_else(|| head.saturating_add(1));
                group_row.truncated_below = Some(below);
                group_row.truncations = group_row.truncations.saturating_add(1);
                group_row.touch();
                self.put_stream_group_in(wtxn, &group_row)?;
                return Err(Error::StreamTruncated(format!(
                    "group `{stream}:{group}` fell behind the retained window at offset {below}"
                )));
            }
        }
        if group_row.next_seq != next_seq.saturating_sub(1) {
            group_row.next_seq = next_seq.saturating_sub(1);
        }
        let now = crate::streams::now_ms();
        let lease_until = now.saturating_add(lease_ms.max(1_000));

        let pending = leases
            .iter()
            .find(|lease| {
                lease.status == crate::streams::LeaseStatus::Available && lease.lease_until <= now
            })
            .cloned();
        if let Some(mut lease) = pending {
            group_row.redeliveries = group_row.redeliveries.saturating_add(1);
            group_row.touch();
            self.put_stream_group_in(wtxn, &group_row)?;
            lease.attempts += 1;
            lease.status = crate::streams::LeaseStatus::Leased;
            lease.lease_id = Some(crate::events::new_event_id());
            lease.member = Some(member.to_string());
            lease.lease_until = lease_until;
            lease.error = None;
            lease.touch();
            self.put_stream_lease_in(wtxn, &lease)?;
            return Ok(Some(lease));
        }

        let from = next_seq;
        if from > head {
            return Ok(None);
        }
        let to = head.min(from.saturating_add(limit.max(1) as u64 - 1));
        let mut lease = StreamLease::new(stream, group, from, to, group_row.max_attempts);
        lease.attempts = 1;
        lease.status = crate::streams::LeaseStatus::Leased;
        lease.lease_id = Some(crate::events::new_event_id());
        lease.member = Some(member.to_string());
        lease.lease_until = lease_until;
        group_row.next_seq = to;
        group_row.touch();
        self.put_stream_group_in(wtxn, &group_row)?;
        self.put_stream_lease_in(wtxn, &lease)?;
        Ok(Some(lease))
    }

    /// Acknowledges a batch and advances the committed offset when the prefix
    /// is contiguous.
    pub fn ack_stream_lease_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        stream: &str,
        group: &str,
        lease_id: &str,
        seq: u64,
    ) -> Result<StreamLease> {
        let mut group_row = self
            .stream_group_in_rw(wtxn, stream, group)?
            .ok_or_else(|| {
                Error::Query(format!("stream group `{stream}:{group}` does not exist"))
            })?;
        let mut lease = self
            .stream_leases_in_rw(wtxn, stream, group)?
            .into_iter()
            .find(|lease| lease.lease_id.as_deref() == Some(lease_id))
            .ok_or_else(|| Error::StaleLease(format!("no batch is leased under `{lease_id}`")))?;
        if lease.status != crate::streams::LeaseStatus::Leased {
            return Err(Error::StaleLease(format!(
                "batch {}..={} is not leased",
                lease.from_seq, lease.to_seq
            )));
        }
        // A batch is acknowledged as a whole: a partial ack inside the range
        // would mark the whole range done and silently drop the rest.
        if seq != lease.to_seq {
            return Err(Error::CommitOutOfOrder(format!(
                "offset {seq} is not the end of the leased range {}..={}",
                lease.from_seq, lease.to_seq
            )));
        }
        if seq < group_row.committed_seq {
            // A batch retried from the dead-letter queue can sit below the
            // committed offset because the group advanced while it was dead.
            // Acknowledge the replay and drop the lease; the offset is already
            // past it.
            if lease.retried {
                lease.status = crate::streams::LeaseStatus::Done;
                lease.lease_id = None;
                lease.member = None;
                lease.lease_until = 0;
                lease.touch();
                self.delete_stream_lease_in(wtxn, stream, group, lease.from_seq)?;
                return Ok(lease);
            }
            return Err(Error::CommitOutOfOrder(format!(
                "offset {seq} is below the committed offset {}",
                group_row.committed_seq
            )));
        }
        lease.status = crate::streams::LeaseStatus::Done;
        lease.lease_id = None;
        lease.member = None;
        lease.lease_until = 0;
        lease.touch();
        self.put_stream_lease_in(wtxn, &lease)?;
        self.advance_stream_committed_in(wtxn, &mut group_row)?;
        Ok(lease)
    }

    /// Releases a batch for redelivery, or dead-letters it once attempts are
    /// exhausted.
    pub fn nack_stream_lease_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        stream: &str,
        group: &str,
        lease_id: &str,
        retry_in_ms: u64,
    ) -> Result<StreamLease> {
        let mut group_row = self
            .stream_group_in_rw(wtxn, stream, group)?
            .ok_or_else(|| {
                Error::Query(format!("stream group `{stream}:{group}` does not exist"))
            })?;
        let mut lease = self
            .stream_leases_in_rw(wtxn, stream, group)?
            .into_iter()
            .find(|lease| lease.lease_id.as_deref() == Some(lease_id))
            .ok_or_else(|| Error::StaleLease(format!("no batch is leased under `{lease_id}`")))?;
        if lease.attempts >= lease.max_attempts {
            self.finish_stream_lease_in(
                wtxn,
                &mut lease,
                true,
                Some("attempts exhausted".to_string()),
            )?;
            self.advance_stream_committed_in(wtxn, &mut group_row)?;
            return Ok(lease);
        }
        lease.status = crate::streams::LeaseStatus::Available;
        lease.lease_id = None;
        lease.member = None;
        lease.lease_until = crate::streams::now_ms().saturating_add(retry_in_ms);
        lease.touch();
        self.put_stream_lease_in(wtxn, &lease)?;
        Ok(lease)
    }

    /// Moves a dead letter back into the redelivery queue, in its own transaction.
    pub fn retry_stream_dead(&self, stream: &str, group: &str, from_seq: u64) -> Result<bool> {
        let mut wtxn = self.write_txn()?;
        let out = self.retry_stream_dead_in(&mut wtxn, stream, group, from_seq)?;
        wtxn.commit()?;
        Ok(out)
    }

    /// Drops a dead letter, in its own transaction.
    pub fn drop_stream_dead(&self, stream: &str, group: &str, from_seq: u64) -> Result<bool> {
        let mut wtxn = self.write_txn()?;
        let out = self.drop_stream_dead_in(&mut wtxn, stream, group, from_seq)?;
        wtxn.commit()?;
        Ok(out)
    }

    /// Moves a dead letter back into the redelivery queue.
    pub fn retry_stream_dead_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        stream: &str,
        group: &str,
        from_seq: u64,
    ) -> Result<bool> {
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        let key = Self::stream_dead_key(stream, group, from_seq);
        let Some(raw) = meta.get(&*wtxn, key.as_bytes())? else {
            return Ok(false);
        };
        let mut lease: StreamLease = serde_json::from_slice(raw)?;
        meta.delete(&mut *wtxn, key.as_bytes())?;
        lease.status = crate::streams::LeaseStatus::Available;
        lease.attempts = 0;
        lease.lease_id = None;
        lease.member = None;
        lease.lease_until = 0;
        lease.error = None;
        lease.retried = true;
        lease.touch();
        self.put_stream_lease_in(wtxn, &lease)?;
        Ok(true)
    }

    /// Drops a dead letter without reprocessing it.
    pub fn drop_stream_dead_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        stream: &str,
        group: &str,
        from_seq: u64,
    ) -> Result<bool> {
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        Ok(meta.delete(
            &mut *wtxn,
            Self::stream_dead_key(stream, group, from_seq).as_bytes(),
        )?)
    }

    /// Oldest `_seq` still stored in a stream collection.
    fn stream_oldest_seq_in(&self, wtxn: &RwTxn<'_>, coll: &str) -> Result<Option<u64>> {
        let collection = self.collection(coll)?;
        let docs = collection.find_in(
            wtxn,
            &crate::collection::FindQuery {
                // Documents written before the stream was defined have no
                // `_seq` and must not be mistaken for "nothing is retained".
                filter: serde_json::json!({crate::streams::SEQ_FIELD: {"$gt": 0}}),
                projection: Some(serde_json::json!({"_seq": 1})),
                sort: vec![(crate::streams::SEQ_FIELD.to_string(), 1)],
                skip: 0,
                limit: Some(1),
                comment: None,
                max_time_ms: None,
                hint: None,
            },
        )?;
        Ok(docs
            .first()
            .and_then(|doc| doc.get(crate::streams::SEQ_FIELD))
            .and_then(Value::as_u64))
    }

    /// Newest surviving `_seq` for a collection, if any.
    fn stream_newest_seq_in(&self, wtxn: &RwTxn<'_>, coll: &str) -> Result<Option<u64>> {
        let collection = self.collection(coll)?;
        let docs = collection.find_in(
            wtxn,
            &crate::collection::FindQuery {
                filter: serde_json::json!({crate::streams::SEQ_FIELD: {"$gt": 0}}),
                projection: Some(serde_json::json!({"_seq": 1})),
                sort: vec![(crate::streams::SEQ_FIELD.to_string(), -1)],
                skip: 0,
                limit: Some(1),
                comment: None,
                max_time_ms: None,
                hint: None,
            },
        )?;
        Ok(docs
            .first()
            .and_then(|doc| doc.get(crate::streams::SEQ_FIELD))
            .and_then(Value::as_u64))
    }

    /// Highest `_seq` that is provably older than `max_age_ms`.
    ///
    /// Walks the `_seq` index forward from the oldest surviving event for at
    /// most one prune batch and stops at the first event that is *not* older
    /// than the cutoff. Out-of-order timestamps, a missing `created_at`, and a
    /// deleted head therefore block the boundary instead of widening it, so a
    /// retention pass can never delete an event that might still be fresh.
    /// Cost is bounded by the batch size, not by the stream length.
    fn stream_age_limit_in(
        &self,
        wtxn: &RwTxn<'_>,
        def: &StreamDef,
        batch: usize,
    ) -> Result<Option<u64>> {
        let Some(max_age_ms) = def.retention.max_age_ms else {
            return Ok(None);
        };
        let cutoff = crate::streams::now_ms() as i64 - max_age_ms as i64;
        let collection = self.collection(&def.coll)?;
        let docs = collection.find_in(
            wtxn,
            &crate::collection::FindQuery {
                filter: serde_json::json!({crate::streams::SEQ_FIELD: {"$gt": 0}}),
                projection: Some(serde_json::json!({
                    crate::streams::SEQ_FIELD: 1,
                    "created_at": 1,
                    "_id": 1,
                })),
                sort: vec![(crate::streams::SEQ_FIELD.to_string(), 1)],
                skip: 0,
                limit: Some(batch.max(1)),
                comment: None,
                max_time_ms: None,
                hint: None,
            },
        )?;
        let mut limit = None;
        for doc in docs {
            let Some(seq) = doc.get(crate::streams::SEQ_FIELD).and_then(Value::as_u64) else {
                break;
            };
            // Unknown age is not proof of age: stop rather than guess.
            let Some(created_at) = crate::events::document_created_at_ms(&doc) else {
                break;
            };
            if created_at >= cutoff {
                break;
            }
            limit = Some(seq);
        }
        Ok(limit)
    }

    /// Highest `_seq` retention may drop for a stream: the minimum of the age
    /// limit and the slowest consumer's position minus `min_seq_keep`.
    ///
    /// `max_bytes` is deliberately not part of the rule yet; `None` means
    /// nothing may be dropped.
    pub fn stream_retention_limit_in(
        &self,
        wtxn: &RwTxn<'_>,
        def: &StreamDef,
        batch: usize,
    ) -> Result<Option<u64>> {
        if def.retention.is_empty() {
            return Ok(None);
        }
        let mut limit = self.stream_age_limit_in(wtxn, def, batch)?;
        if let Some(max_events) = def.retention.max_events {
            // The newest *surviving* event, not the counter head: when the head
            // document was deleted by hand the counter would over-count and
            // retain one event fewer than requested.
            if let Some(newest) = self.stream_newest_seq_in(wtxn, &def.coll)? {
                let events_limit = newest.saturating_sub(max_events);
                limit = Some(match limit {
                    Some(other) => other.min(events_limit),
                    None => events_limit,
                });
            }
        }
        let groups = self.stream_groups(Some(&def.name))?;
        if !groups.is_empty() {
            let slowest = groups
                .iter()
                .map(|group| group.committed_seq)
                .min()
                .unwrap_or(0);
            let consumer_limit = slowest.saturating_sub(def.retention.min_seq_keep);
            limit = Some(match limit {
                Some(age) => age.min(consumer_limit),
                None => consumer_limit,
            });
        }
        Ok(limit)
    }

    /// Drops up to `batch` events below the retention limit.
    ///
    /// Groups that fell behind the retained window get `truncated_below` set, so
    /// their next claim fails with `stream_truncated` instead of silently
    /// skipping events.
    pub fn prune_stream_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        stream: &str,
        batch: usize,
    ) -> Result<Vec<(String, Value)>> {
        let Some(def) = self.stream_in_rw(wtxn, stream)? else {
            return Ok(Vec::new());
        };
        let Some(limit) = self.stream_retention_limit_in(wtxn, &def, batch)? else {
            return Ok(Vec::new());
        };
        let Some(oldest) = self.stream_oldest_seq_in(wtxn, &def.coll)? else {
            return Ok(Vec::new());
        };
        if oldest > limit {
            return Ok(Vec::new());
        }
        let collection = self.collection(&def.coll)?;
        let doomed = collection.find_in(
            wtxn,
            &crate::collection::FindQuery {
                filter: serde_json::json!({crate::streams::SEQ_FIELD: {"$lte": limit}}),
                projection: None,
                sort: vec![(crate::streams::SEQ_FIELD.to_string(), 1)],
                skip: 0,
                limit: Some(batch.max(1)),
                comment: None,
                max_time_ms: None,
                hint: None,
            },
        )?;
        let mut deleted = Vec::new();
        for doc in doomed {
            let Some(id) = doc.get("_id").cloned() else {
                continue;
            };
            if collection.delete_txn(wtxn, &serde_json::json!({"_id": id.clone()}), false)? > 0 {
                match id.as_str() {
                    Some(id) => deleted.push((id.to_string(), doc)),
                    None => continue,
                }
            }
        }
        if deleted.is_empty() {
            return Ok(deleted);
        }
        // When retention removed the entire window there is no surviving offset
        // to report; the group must resume after the current head instead.
        let remaining = self.stream_oldest_seq_in(wtxn, &def.coll)?;
        let head = self.stream_head_in(wtxn, &def.name)?;
        let below = remaining.unwrap_or(head.saturating_add(1));
        for mut group in self.stream_groups(Some(&def.name))? {
            // The group's next event (`next_seq + 1`) was dropped when it is at
            // or below the highest dropped offset.
            if group.next_seq < limit && group.truncated_below != Some(below) {
                group.truncated_below = Some(below);
                group.truncations = group.truncations.saturating_add(1);
                group.touch();
                self.put_stream_group_in(wtxn, &group)?;
            }
        }
        Ok(deleted)
    }

    /// Runs one retention pass over every stream, in its own transactions.
    ///
    /// Returns `(events_deleted, streams_touched)`.
    pub fn prune_streams(&self, batch: usize) -> Result<(usize, usize)> {
        let mut deleted = 0usize;
        let mut touched = 0usize;
        for def in self.streams()? {
            if def.retention.is_empty() {
                continue;
            }
            loop {
                let mut wtxn = self.write_txn()?;
                let dropped = self.prune_stream_in(&mut wtxn, &def.name, batch)?;
                let n = dropped.len();
                wtxn.commit()?;
                if n == 0 {
                    break;
                }
                deleted += n;
                touched += 1;
                if n < batch {
                    break;
                }
            }
        }
        Ok((deleted, touched))
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
                let ttl_field = Self::ttl_field_from_ro(meta, &rtxn, name)?;
                let purge = Self::ttl_purge_from_ro(meta, &rtxn, name)?;
                let history = Self::history_from_ro(meta, &rtxn, name)?;
                rtxn.commit()?;
                return Ok(self.wrap(
                    name,
                    db,
                    meta,
                    profile,
                    ttl_default,
                    ttl_field,
                    purge,
                    history,
                ));
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
        let ttl_field = Self::ttl_field_from_rw(meta, wtxn, name)?;
        let purge = Self::ttl_purge_from_rw(meta, wtxn, name)?;
        let history = Self::history_from_rw(meta, wtxn, name)?;
        Ok(self.wrap(
            name,
            db,
            meta,
            profile,
            ttl_default,
            ttl_field,
            purge,
            history,
        ))
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
        let ttl_field = Self::ttl_field_from_ro(meta, rtxn, name)?;
        let purge = Self::ttl_purge_from_ro(meta, rtxn, name)?;
        let history = Self::history_from_ro(meta, rtxn, name)?;
        Ok(Some(self.wrap(
            name,
            db,
            meta,
            profile,
            ttl_default,
            ttl_field,
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
        ttl_field: Option<String>,
        purge_history: bool,
        history_max_versions: usize,
    ) -> Collection<'e> {
        Collection::new(
            Some(self),
            &self.env,
            db,
            meta,
            self.history,
            self.ttl,
            history_max_versions,
            name.to_string(),
            profile,
            ttl_default,
            ttl_field,
            purge_history,
            &self.txn_gate,
        )
    }

    /// Earliest TTL expiry in the database, if any.
    pub fn next_ttl_expiry(&self) -> Result<Option<u64>> {
        let rtxn = self.read_txn()?;
        let out = ttl::earliest_expiry(&self.ttl, &rtxn)?;
        rtxn.commit()?;
        Ok(out)
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

    /// Drops a collection and its indexes, history, TTL, events, and
    /// subscription metadata. The collection sub-database is cleared; callers
    /// that need to reclaim file space must compact the environment afterwards.
    ///
    /// This is an offline/admin operation. The daemon should quiesce direct
    /// access to the collection while it runs.
    pub fn drop_collection(&self, name: &str) -> Result<bool> {
        let mut wtxn = self.write_txn()?;
        let dropped = self.drop_collection_in(&mut wtxn, name)?;
        wtxn.commit()?;
        Ok(dropped)
    }

    /// Drops a collection and all of its metadata in a caller-provided write
    /// transaction. The daemon writer uses this to keep collection DDL in the
    /// same single-writer transaction as replication capture.
    pub fn drop_collection_in(&self, wtxn: &mut RwTxn<'_>, name: &str) -> Result<bool> {
        Self::validate_name(name)?;
        let meta: Database<Bytes, Bytes> = self
            .env
            .open_database(&*wtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        if meta
            .get(&*wtxn, format!("collection:{name}").as_bytes())?
            .is_none()
        {
            return Ok(false);
        }
        let Some(collection) = self.collection_at(&*wtxn, name)? else {
            return Ok(false);
        };
        // Drop graph metadata/adjacency owned by this collection before its
        // documents and collection metadata disappear. This covers both the
        // node-side `graph:<node>` definition and the edge-side
        // `graph_edge:<edge>` reverse mapping, whichever collection is dropped.
        if collection.get_graph_in(&*wtxn)?.is_some() {
            collection.drop_graph_txn(wtxn)?;
        }
        if let Some(def) = collection.edge_graph_def_in(&*wtxn)? {
            if let Some(node) = self.collection_at(&*wtxn, &def.node_collection)? {
                node.drop_graph_txn(wtxn)?;
            } else {
                collection.clear_graph_adjacency_txn(wtxn, &def)?;
                collection.set_graph_rebuild_state_in(wtxn, None)?;
                if let Some(meta) = self
                    .env
                    .open_database::<Bytes, Bytes>(&*wtxn, Some(META_DB))?
                {
                    meta.delete(
                        &mut *wtxn,
                        graph::node_def_key(&def.node_collection).as_bytes(),
                    )?;
                    meta.delete(
                        &mut *wtxn,
                        graph::edge_def_key(collection.name()).as_bytes(),
                    )?;
                }
            }
        }

        let indexes = collection.index_defs_in(&*wtxn)?;
        let vectors = collection.list_vector_indexes_in(&*wtxn)?;
        let search = collection.search_index_defs_in(&*wtxn)?;

        for def in indexes {
            collection.drop_index_txn(wtxn, &def.name())?;
        }
        for def in vectors {
            collection.drop_vector_index_txn(wtxn, &def.field)?;
        }
        for def in search {
            collection.drop_search_index_txn(wtxn, def.kind, &def.name)?;
        }

        if collection.profile().is_timeseries() {
            crate::timeseries::clear(wtxn, &self.env, name)?;
        }

        if let Some(db) = self.env.open_database::<Bytes, Bytes>(&*wtxn, Some(name))? {
            db.clear(&mut *wtxn)?;
        }
        let exact = [
            format!("collection:{name}"),
            format!("profile:{name}"),
            format!("history:{name}"),
            format!("events:{name}"),
            format!("ttl:{name}"),
            format!("ttl_purge:{name}"),
            format!("{}{name}", crate::timeseries::SETTINGS_META_PREFIX),
            subscriptions::meta_key(name),
            graph::node_def_key(name),
            graph::edge_def_key(name),
            graph::rebuild_key(name),
        ];
        let prefixes = [
            format!("index:{name}:"),
            format!("vecdef:{name}:"),
            format!("search:{name}:"),
            format!("text:{name}:"),
            format!("fuzzy:{name}:"),
            format!("fttotal:{name}:"),
        ];
        let mut meta_keys = Vec::new();
        for item in meta.iter(&*wtxn)? {
            let (key, _) = item?;
            if exact.iter().any(|item| item.as_bytes() == key)
                || prefixes
                    .iter()
                    .any(|prefix| key.starts_with(prefix.as_bytes()))
            {
                meta_keys.push(key.to_vec());
            }
        }
        for key in meta_keys {
            meta.delete(&mut *wtxn, key.as_slice())?;
        }

        let mut history_keys = Vec::new();
        for item in self.history.iter(&*wtxn)? {
            let (key, _) = item?;
            if history::parse_key(key).is_some_and(|(coll, _, _)| coll == name) {
                history_keys.push(key.to_vec());
            }
        }
        for key in history_keys {
            self.history.delete(&mut *wtxn, key.as_slice())?;
        }

        let mut ttl_keys = Vec::new();
        for item in self.ttl.iter(&*wtxn)? {
            let (key, _) = item?;
            let matches = ttl::parse_expiry_key(key).is_some_and(|(_, coll, _)| coll == name)
                || ttl::parse_id_key(key).is_some_and(|(coll, _)| coll == name);
            if matches {
                ttl_keys.push(key.to_vec());
            }
        }
        for key in ttl_keys {
            self.ttl.delete(&mut *wtxn, key.as_slice())?;
        }

        Ok(true)
    }

    /// Clears all user collections and replication state from a replica.
    ///
    /// This is an in-place hard resync: the LMDB environment and writer stay
    /// open, so a replica can rejoin a new primary without a process restart.
    pub fn reset_replica_state(&self) -> Result<()> {
        let mut names = self.collections()?;
        let rtxn = self.read_txn()?;
        let has_control = self.collection_at(&rtxn, CONTROL_DB)?.is_some();
        rtxn.commit()?;
        if has_control {
            names.push(CONTROL_DB.to_string());
        }
        for name in names {
            let _ = self.drop_collection(&name)?;
        }
        let mut wtxn = self.write_txn()?;
        let meta: Database<Bytes, Bytes> = self.env.create_database(&mut wtxn, Some(META_DB))?;
        let replication: Database<Bytes, Bytes> =
            self.env.create_database(&mut wtxn, Some(REPLICATION_DB))?;
        replication.clear(&mut wtxn)?;
        meta.delete(&mut wtxn, REPLICATION_SEQ_KEY)?;
        let keys: Vec<Vec<u8>> = meta
            .iter(&wtxn)?
            .filter_map(|item| {
                let (key, _) = item.ok()?;
                key.starts_with(b"replication:").then(|| key.to_vec())
            })
            .collect();
        for key in keys {
            meta.delete(&mut wtxn, key.as_slice())?;
        }
        wtxn.commit()?;
        Ok(())
    }

    /// Returns the highest source sequence applied for `origin`.
    pub fn replication_applied(&self, origin: &str) -> Result<u64> {
        let rtxn = self.read_txn()?;
        let meta: Database<Bytes, Bytes> = self
            .env
            .open_database(&rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        let key = format!("replication:applied:{origin}");
        let applied = meta
            .get(&rtxn, key.as_bytes())?
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(0);
        rtxn.commit()?;
        Ok(applied)
    }

    /// Returns the highest source role epoch applied for `origin`.
    pub fn replication_applied_epoch(&self, origin: &str) -> Result<u64> {
        let rtxn = self.read_txn()?;
        let meta: Database<Bytes, Bytes> = self
            .env
            .open_database(&rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        let key = format!("replication:epoch:{origin}");
        let epoch = meta
            .get(&rtxn, key.as_bytes())?
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(0);
        rtxn.commit()?;
        Ok(epoch)
    }

    /// Global highest source sequence applied on this replica, across origins.
    pub fn replication_applied_global(&self) -> Result<u64> {
        let rtxn = self.read_txn()?;
        let meta: Database<Bytes, Bytes> = self
            .env
            .open_database(&rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        if let Some(raw) = meta.get(&rtxn, b"replication:applied_seq")? {
            let seq = std::str::from_utf8(raw)
                .ok()
                .and_then(|raw| raw.parse::<u64>().ok())
                .unwrap_or(0);
            rtxn.commit()?;
            return Ok(seq);
        }
        let mut max = 0u64;
        for item in meta.iter(&rtxn)? {
            let (key, value) = item?;
            let Some(origin) = key.strip_prefix(b"replication:applied:") else {
                continue;
            };
            if origin.is_empty() {
                continue;
            }
            max = max.max(
                std::str::from_utf8(value)
                    .ok()
                    .and_then(|raw| raw.parse::<u64>().ok())
                    .unwrap_or(0),
            );
        }
        rtxn.commit()?;
        Ok(max)
    }

    /// Returns all `(origin, applied sequence)` pairs recorded on this node.
    pub fn replication_applied_origins(&self) -> Result<Vec<(String, u64)>> {
        const PREFIX: &[u8] = b"replication:applied:";
        let rtxn = self.read_txn()?;
        let meta: Database<Bytes, Bytes> = self
            .env
            .open_database(&rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        let mut out = Vec::new();
        for item in meta.iter(&rtxn)? {
            let (key, value) = item?;
            let Some(origin) = key.strip_prefix(PREFIX) else {
                continue;
            };
            let Ok(origin) = std::str::from_utf8(origin) else {
                continue;
            };
            let seq = std::str::from_utf8(value)
                .ok()
                .and_then(|raw| raw.parse::<u64>().ok())
                .unwrap_or(0);
            out.push((origin.to_string(), seq));
        }
        rtxn.commit()?;
        Ok(out)
    }

    /// Last source head observed for `origin` by the pull loop.
    pub fn replication_source_head(&self, origin: &str) -> Result<u64> {
        let rtxn = self.read_txn()?;
        let meta: Database<Bytes, Bytes> = self
            .env
            .open_database(&rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        let key = format!("replication:source_head:{origin}");
        let head = meta
            .get(&rtxn, key.as_bytes())?
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(0);
        rtxn.commit()?;
        Ok(head)
    }

    /// Records the latest source head observed by the pull loop.
    pub fn set_replication_source_head(&self, origin: &str, head: u64) -> Result<()> {
        if head == self.replication_source_head(origin)? {
            return Ok(());
        }
        let mut wtxn = self.write_txn()?;
        let meta: Database<Bytes, Bytes> = self.env.create_database(&mut wtxn, Some(META_DB))?;
        let key = format!("replication:source_head:{origin}");
        meta.put(&mut wtxn, key.as_bytes(), head.to_string().as_bytes())?;
        wtxn.commit()?;
        Ok(())
    }

    /// Records a replica's acknowledged source sequence for `origin`.
    pub fn record_replica_ack(&self, origin: &str, node: &str, seq: u64) -> Result<()> {
        if origin.trim().is_empty() || node.trim().is_empty() {
            return Err(Error::Query("replication ack origin/node is empty".into()));
        }
        let key = format!("replication:ack:{origin}:{node}");
        let at_key = format!("replication:ack_at:{origin}:{node}");
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        let mut wtxn = self.write_txn()?;
        let meta: Database<Bytes, Bytes> = self.env.create_database(&mut wtxn, Some(META_DB))?;
        let current = meta
            .get(&wtxn, key.as_bytes())?
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(0);
        if seq > current {
            meta.put(&mut wtxn, key.as_bytes(), seq.to_string().as_bytes())?;
        }
        meta.put(&mut wtxn, at_key.as_bytes(), now_ms.to_string().as_bytes())?;
        wtxn.commit()?;
        Ok(())
    }

    /// Removes one replica acknowledgement and its timestamp.
    pub fn remove_replica_ack(&self, origin: &str, node: &str) -> Result<bool> {
        if origin.trim().is_empty() || node.trim().is_empty() {
            return Err(Error::Query("replication ack origin/node is empty".into()));
        }
        let key = format!("replication:ack:{origin}:{node}");
        let at_key = format!("replication:ack_at:{origin}:{node}");
        let mut wtxn = self.write_txn()?;
        let meta: Database<Bytes, Bytes> = self.env.create_database(&mut wtxn, Some(META_DB))?;
        let existed = meta.get(&wtxn, key.as_bytes())?.is_some();
        meta.delete(&mut wtxn, key.as_bytes())?;
        meta.delete(&mut wtxn, at_key.as_bytes())?;
        wtxn.commit()?;
        Ok(existed)
    }

    /// Returns `(node, acked sequence)` for all replicas of `origin`.
    pub fn replica_acks(&self, origin: &str) -> Result<Vec<(String, u64)>> {
        let prefix = format!("replication:ack:{origin}:");
        let rtxn = self.read_txn()?;
        let meta: Database<Bytes, Bytes> = self
            .env
            .open_database(&rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        let mut out = Vec::new();
        for item in meta.iter(&rtxn)? {
            let (key, value) = item?;
            let Some(node) = key.strip_prefix(prefix.as_bytes()) else {
                continue;
            };
            let Ok(node) = std::str::from_utf8(node) else {
                continue;
            };
            let seq = std::str::from_utf8(value)
                .ok()
                .and_then(|raw| raw.parse::<u64>().ok())
                .unwrap_or(0);
            out.push((node.to_string(), seq));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        rtxn.commit()?;
        Ok(out)
    }

    /// Returns `(node, acked sequence, ack timestamp ms)` for all replicas.
    pub fn replica_ack_details(&self, origin: &str) -> Result<Vec<(String, u64, u64)>> {
        let prefix = format!("replication:ack:{origin}:");
        let at_prefix = format!("replication:ack_at:{origin}:");
        let rtxn = self.read_txn()?;
        let meta: Database<Bytes, Bytes> = self
            .env
            .open_database(&rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        let mut times = std::collections::HashMap::new();
        for item in meta.iter(&rtxn)? {
            let (key, value) = item?;
            let Some(node) = key.strip_prefix(at_prefix.as_bytes()) else {
                continue;
            };
            let Ok(node) = std::str::from_utf8(node) else {
                continue;
            };
            times.insert(
                node.to_string(),
                std::str::from_utf8(value)
                    .ok()
                    .and_then(|raw| raw.parse::<u64>().ok())
                    .unwrap_or(0),
            );
        }
        let mut out = Vec::new();
        for item in meta.iter(&rtxn)? {
            let (key, value) = item?;
            let Some(node) = key.strip_prefix(prefix.as_bytes()) else {
                continue;
            };
            let Ok(node) = std::str::from_utf8(node) else {
                continue;
            };
            let seq = std::str::from_utf8(value)
                .ok()
                .and_then(|raw| raw.parse::<u64>().ok())
                .unwrap_or(0);
            out.push((node.to_string(), seq, times.get(node).copied().unwrap_or(0)));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        rtxn.commit()?;
        Ok(out)
    }

    /// Sets the applied sequence and observed source head for a bootstrap.
    pub fn set_replication_state(
        &self,
        origin: &str,
        applied: u64,
        source_head: u64,
        epoch: u64,
    ) -> Result<()> {
        if origin.trim().is_empty() {
            return Err(Error::Query("replication origin is empty".into()));
        }
        let mut wtxn = self.write_txn()?;
        let meta: Database<Bytes, Bytes> = self.env.create_database(&mut wtxn, Some(META_DB))?;
        let applied_key = format!("replication:applied:{origin}");
        let head_key = format!("replication:source_head:{origin}");
        let epoch_key = format!("replication:epoch:{origin}");
        meta.put(
            &mut wtxn,
            b"replication:applied_seq",
            applied.to_string().as_bytes(),
        )?;
        meta.put(
            &mut wtxn,
            applied_key.as_bytes(),
            applied.to_string().as_bytes(),
        )?;
        meta.put(
            &mut wtxn,
            head_key.as_bytes(),
            source_head.to_string().as_bytes(),
        )?;
        meta.put(
            &mut wtxn,
            epoch_key.as_bytes(),
            epoch.to_string().as_bytes(),
        )?;
        wtxn.commit()?;
        Ok(())
    }

    /// Applies one source replication transaction atomically and idempotently.
    ///
    /// The replica requires contiguous sequences for each origin. Re-applying
    /// an already applied sequence is a no-op.
    pub fn apply_replication_txn(&self, txn: &ReplicationTxn) -> Result<bool> {
        if txn.origin.trim().is_empty() {
            return Err(Error::Query("replication origin is empty".into()));
        }
        if txn.seq == 0 {
            return Err(Error::Query("replication sequence must be positive".into()));
        }
        if txn.changes.is_empty() && txn.ops.is_empty() {
            return Err(Error::Query(
                "replication transaction must contain at least one change".into(),
            ));
        }
        let mut wtxn = self.write_txn()?;
        let meta: Database<Bytes, Bytes> = self.env.create_database(&mut wtxn, Some(META_DB))?;
        let key = format!("replication:applied:{}", txn.origin);
        let applied = self.replication_applied_global().unwrap_or(0);
        let epoch_key = format!("replication:epoch:{}", txn.origin);
        let applied_epoch = meta
            .get(&wtxn, epoch_key.as_bytes())?
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(0);
        if txn.seq <= applied {
            wtxn.commit()?;
            return Ok(false);
        }
        if txn.epoch < applied_epoch {
            return Err(Error::Query(format!(
                "stale replication epoch for {}: applied {}, got {}",
                txn.origin, applied_epoch, txn.epoch
            )));
        }
        if txn.seq != applied + 1 {
            return Err(Error::Query(format!(
                "replication gap for {}: expected {}, got {}",
                txn.origin,
                applied + 1,
                txn.seq
            )));
        }
        let local_head = meta
            .get(&wtxn, REPLICATION_SEQ_KEY)?
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(0);
        if local_head + 1 != txn.seq {
            return Err(Error::Query(format!(
                "replica log sequence mismatch: local head {}, source seq {}",
                local_head, txn.seq
            )));
        }
        self.apply_replication_ops_in(&mut wtxn, &txn.changes, &txn.ops)?;
        self.append_replication_txn_ops_with_epoch_in(
            &mut wtxn,
            &txn.origin,
            txn.epoch,
            txn.changes.clone(),
            txn.ops.clone(),
        )?;
        meta.put(
            &mut wtxn,
            b"replication:applied_seq",
            txn.seq.to_string().as_bytes(),
        )?;
        meta.put(&mut wtxn, key.as_bytes(), txn.seq.to_string().as_bytes())?;
        meta.put(
            &mut wtxn,
            epoch_key.as_bytes(),
            txn.epoch.to_string().as_bytes(),
        )?;
        wtxn.commit()?;
        Ok(true)
    }

    fn apply_replication_ops_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        changes: &[ReplicationChange],
        ops: &[ReplicationOp],
    ) -> Result<()> {
        if ops.is_empty() {
            for change in changes {
                let collection = self.collection_in(wtxn, &change.collection)?;
                collection.apply_replication_change_txn(
                    wtxn,
                    &change.op,
                    &change.id,
                    change.after.as_ref(),
                    change.ttl_at,
                )?;
            }
        } else {
            for op in ops {
                self.apply_replication_op_in(wtxn, changes, op)?;
            }
        }
        Ok(())
    }

    /// Applies a transaction from this database's own PITR WAL. Unlike
    /// [`Self::apply_replication_txn`], this checks the local log head instead
    /// of the per-origin replica apply cursor, and preserves the original
    /// transaction timestamp in the restored log.
    pub fn apply_pitr_txn(&self, txn: &ReplicationTxn) -> Result<bool> {
        if txn.origin.trim().is_empty() {
            return Err(Error::Query("replication origin is empty".into()));
        }
        if txn.seq == 0 {
            return Err(Error::Query("replication sequence must be positive".into()));
        }
        if txn.changes.is_empty() && txn.ops.is_empty() {
            return Err(Error::Query(
                "replication transaction must contain at least one change".into(),
            ));
        }
        let mut wtxn = self.write_txn()?;
        let meta: Database<Bytes, Bytes> = self.env.create_database(&mut wtxn, Some(META_DB))?;
        let local_head = meta
            .get(&wtxn, REPLICATION_SEQ_KEY)?
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(0);
        if txn.seq <= local_head {
            wtxn.commit()?;
            return Ok(false);
        }
        if txn.seq != local_head.saturating_add(1) {
            return Err(Error::Query(format!(
                "PITR replay gap: local head {local_head}, got sequence {}",
                txn.seq
            )));
        }
        self.apply_replication_ops_in(&mut wtxn, &txn.changes, &txn.ops)?;
        let encoded = serde_json::to_vec(txn)?;
        let db: Database<Bytes, Bytes> =
            self.env.create_database(&mut wtxn, Some(REPLICATION_DB))?;
        db.put(&mut wtxn, &txn.seq.to_be_bytes(), &encoded)?;
        meta.put(
            &mut wtxn,
            REPLICATION_SEQ_KEY,
            txn.seq.to_string().as_bytes(),
        )?;
        wtxn.commit()?;
        Ok(true)
    }

    fn apply_replication_op_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        changes: &[ReplicationChange],
        op: &ReplicationOp,
    ) -> Result<()> {
        match op {
            ReplicationOp::Document { change } => {
                let change = changes.get(*change).ok_or_else(|| {
                    Error::Query("replication document index out of range".into())
                })?;
                let collection = self.collection_in(wtxn, &change.collection)?;
                if change.vault {
                    collection.apply_vault_replication_txn(
                        wtxn,
                        &change.op,
                        &change.id,
                        change.after.as_ref(),
                        change.ttl_at,
                    )
                } else {
                    collection.apply_replication_change_txn(
                        wtxn,
                        &change.op,
                        &change.id,
                        change.after.as_ref(),
                        change.ttl_at,
                    )
                }
            }
            ReplicationOp::CollectionCreate {
                name,
                profile,
                ttl_seconds,
                ttl_field,
                ttl_purge_history,
                timeseries,
            } => {
                let profile = CollectionProfile::parse(profile)?;
                let collection = self.create_collection_with_ttl_and_purge_in(
                    wtxn,
                    name,
                    profile,
                    *ttl_seconds,
                    *ttl_purge_history,
                )?;
                if let Some(raw) = timeseries {
                    let settings: crate::timeseries::Settings = serde_json::from_str(raw)?;
                    collection.set_timeseries_settings_in(wtxn, &settings)?;
                }
                if let (Some(field), Some(seconds)) = (ttl_field.as_deref(), ttl_seconds) {
                    self.set_collection_ttl_after_in(
                        wtxn,
                        name,
                        field,
                        *seconds,
                        *ttl_purge_history,
                    )?;
                }
                Ok(())
            }
            ReplicationOp::CollectionDrop { name } => {
                self.drop_collection_in(wtxn, name).map(|_| ())
            }
            ReplicationOp::TimeSeriesRetain { coll, before_ms } => self
                .retain_timeseries_in(wtxn, coll, *before_ms)
                .map(|_| ()),
            ReplicationOp::TimeSeriesSettings { coll, settings } => {
                let collection = self.collection_in(&mut *wtxn, coll)?;
                collection.set_timeseries_settings_in(wtxn, settings)?;
                Ok(())
            }
            ReplicationOp::CollectionTtl {
                coll,
                ttl_seconds,
                ttl_field,
                purge_history,
            } => match (ttl_field.as_deref(), ttl_seconds) {
                (Some(field), Some(seconds)) => {
                    self.set_collection_ttl_after_in(wtxn, coll, field, *seconds, *purge_history)
                }
                _ => {
                    self.set_collection_ttl_with_purge_in(wtxn, coll, *ttl_seconds, *purge_history)
                }
            },
            ReplicationOp::TtlSet { coll, entries } => {
                let collection = self.collection_in(wtxn, coll)?;
                for entry in entries {
                    collection.set_ttl(wtxn, &entry.id, entry.ttl_at)?;
                }
                Ok(())
            }
            ReplicationOp::CollectionHistory { coll, max_versions } => {
                self.set_collection_history_in(wtxn, coll, *max_versions)
            }
            ReplicationOp::CollectionEvents { coll, config } => {
                self.set_collection_events_in(wtxn, coll, config)
            }
            ReplicationOp::GraphSet { coll, def } => {
                let collection = self.collection_in(wtxn, coll)?;
                collection.set_graph_txn(wtxn, def)
            }
            ReplicationOp::GraphDrop { coll } => {
                let collection = self.collection_in(wtxn, coll)?;
                collection.drop_graph_txn(wtxn).map(|_| ())
            }
            ReplicationOp::IndexCreate {
                coll,
                fields,
                unique,
                sparse,
                partial_filter_expression,
                include,
                geo,
            } => {
                let collection = self.collection_in(wtxn, coll)?;
                collection.create_index_fields_txn(
                    wtxn,
                    fields,
                    *unique,
                    include,
                    *geo,
                    *sparse,
                    partial_filter_expression.clone(),
                )
            }
            ReplicationOp::IndexDrop { coll, field } => {
                let collection = self.collection_in(wtxn, coll)?;
                collection.drop_index_txn(wtxn, field).map(|_| ())
            }
            ReplicationOp::SearchIndexCreate { coll, index } => {
                let collection = self.collection_in(wtxn, coll)?;
                collection.create_search_index_txn(wtxn, index)
            }
            ReplicationOp::SearchIndexDrop {
                coll,
                index_kind,
                name,
            } => {
                let collection = self.collection_in(wtxn, coll)?;
                collection
                    .drop_search_index_txn(wtxn, *index_kind, name)
                    .map(|_| ())
            }
            ReplicationOp::TextIndexCreate { coll, field } => {
                let collection = self.collection_in(wtxn, coll)?;
                collection.create_text_index_txn(wtxn, field)
            }
            ReplicationOp::TextIndexDrop { coll, field } => {
                let collection = self.collection_in(wtxn, coll)?;
                collection.drop_text_index_txn(wtxn, field).map(|_| ())
            }
            ReplicationOp::FuzzyIndexCreate { coll, field } => {
                let collection = self.collection_in(wtxn, coll)?;
                collection.create_fuzzy_index_txn(wtxn, field)
            }
            ReplicationOp::FuzzyIndexDrop { coll, field } => {
                let collection = self.collection_in(wtxn, coll)?;
                collection.drop_fuzzy_index_txn(wtxn, field).map(|_| ())
            }
            ReplicationOp::VectorIndexCreate {
                coll,
                field,
                dims,
                metric,
            } => {
                let collection = self.collection_in(wtxn, coll)?;
                collection.create_vector_index_txn(wtxn, field, *dims, *metric)
            }
            ReplicationOp::VectorIndexDrop { coll, field } => {
                let collection = self.collection_in(wtxn, coll)?;
                collection.drop_vector_index_txn(wtxn, field).map(|_| ())
            }
            ReplicationOp::SubscriptionUpsert { coll, subscription } => {
                self.create_subscription_in(wtxn, coll, subscription)?;
                Ok(())
            }
            ReplicationOp::SubscriptionDelete { coll, name } => {
                self.delete_subscription_in(wtxn, coll, name)?;
                Ok(())
            }
            ReplicationOp::StreamSet { def } => self.set_stream_in(wtxn, def),
            ReplicationOp::StreamDrop { name } => {
                self.drop_stream_in(wtxn, name)?;
                Ok(())
            }
            ReplicationOp::StreamGroupSet { group } => self.put_stream_group_in(wtxn, group),
            ReplicationOp::StreamLeaseDead { lease } => self.put_stream_dead_in(wtxn, lease),
            ReplicationOp::StreamDeadDelete {
                stream,
                group,
                from_seq,
            } => {
                self.drop_stream_dead_in(wtxn, stream, group, *from_seq)?;
                Ok(())
            }
            ReplicationOp::StreamGroupClear { stream, group } => {
                self.clear_stream_leases_in(wtxn, stream, group)
            }
            ReplicationOp::StreamGroupDelete { stream, group } => {
                self.delete_stream_group_in(wtxn, stream, group)?;
                Ok(())
            }
            ReplicationOp::EventAppend { doc } => {
                let id = doc
                    .get("_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::Query("replication event has no string _id".into()))?;
                let collection = self.collection_in(wtxn, crate::events::EVENTS_COLLECTION)?;
                collection.apply_replication_change_txn(wtxn, "insert", id, Some(doc), None)
            }
            ReplicationOp::EventDelete { id } => {
                let collection = self.collection_in(wtxn, crate::events::EVENTS_COLLECTION)?;
                collection.apply_replication_change_txn(wtxn, "delete", id, None, None)
            }
        }
    }

    /// Appends one committed transaction to the durable replication log.
    ///
    /// The caller must use the same LMDB write transaction as the data writes,
    /// so data and replication log commit atomically.
    pub fn append_replication_txn_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        origin: &str,
        changes: Vec<ReplicationChange>,
    ) -> Result<ReplicationTxn> {
        self.append_replication_txn_with_epoch_in(wtxn, origin, 0, changes)
    }

    /// Appends a transaction with the writer's active role epoch.
    pub fn append_replication_txn_with_epoch_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        origin: &str,
        epoch: u64,
        changes: Vec<ReplicationChange>,
    ) -> Result<ReplicationTxn> {
        self.append_replication_txn_ops_with_epoch_in(wtxn, origin, epoch, changes, Vec::new())
    }

    /// Appends a transaction with ordered logical operations. Document payloads
    /// stay in `changes`; `ops` carries metadata, index, subscription, and
    /// outbox operations at their exact position.
    pub fn append_replication_txn_ops_with_epoch_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        origin: &str,
        epoch: u64,
        changes: Vec<ReplicationChange>,
        ops: Vec<ReplicationOp>,
    ) -> Result<ReplicationTxn> {
        if changes.is_empty() && ops.is_empty() {
            return Err(Error::Query(
                "replication transaction must contain at least one change".into(),
            ));
        }
        let meta: Database<Bytes, Bytes> = self.env.create_database(&mut *wtxn, Some(META_DB))?;
        let previous = meta
            .get(&*wtxn, REPLICATION_SEQ_KEY)?
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(0);
        let seq = previous
            .checked_add(1)
            .ok_or_else(|| Error::Query("replication sequence overflow".into()))?;
        let txn = ReplicationTxn::new_with_ops(seq, origin, epoch, changes, ops);
        let encoded = serde_json::to_vec(&txn)?;
        let db: Database<Bytes, Bytes> =
            self.env.create_database(&mut *wtxn, Some(REPLICATION_DB))?;
        db.put(&mut *wtxn, &seq.to_be_bytes(), &encoded)?;
        meta.put(&mut *wtxn, REPLICATION_SEQ_KEY, seq.to_string().as_bytes())?;
        Ok(txn)
    }

    /// Returns the highest allocated replication sequence, or zero.
    pub fn replication_head(&self) -> Result<u64> {
        let rtxn = self.read_txn()?;
        let meta: Database<Bytes, Bytes> = self
            .env
            .open_database(&rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        let head = meta
            .get(&rtxn, REPLICATION_SEQ_KEY)?
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(0);
        rtxn.commit()?;
        Ok(head)
    }

    /// Reads replication transactions strictly after `after`, in sequence order.
    pub fn replication_after(&self, after: u64, limit: usize) -> Result<Vec<ReplicationTxn>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rtxn = self.read_txn()?;
        let db: Option<Database<Bytes, Bytes>> =
            self.env.open_database(&rtxn, Some(REPLICATION_DB))?;
        let Some(db) = db else {
            rtxn.commit()?;
            return Ok(Vec::new());
        };
        let start = after.saturating_add(1).to_be_bytes();
        let mut out = Vec::new();
        for item in db.range(
            &rtxn,
            &(
                std::ops::Bound::Included(start.as_slice()),
                std::ops::Bound::Unbounded,
            ),
        )? {
            let (_key, value) = item?;
            out.push(serde_json::from_slice(value)?);
            if out.len() >= limit {
                break;
            }
        }
        rtxn.commit()?;
        Ok(out)
    }

    /// Deletes replication records at or below `seq`. Intended for retention
    /// after every replica has acknowledged the sequence.
    pub fn prune_replication_through(&self, seq: u64) -> Result<usize> {
        let mut wtxn = self.write_txn()?;
        let db: Option<Database<Bytes, Bytes>> =
            self.env.open_database(&wtxn, Some(REPLICATION_DB))?;
        let Some(db) = db else {
            wtxn.commit()?;
            return Ok(0);
        };
        let keys: Vec<Vec<u8>> = db
            .iter(&wtxn)?
            .filter_map(|item| {
                let (key, _) = item.ok()?;
                let value = u64::from_be_bytes(key.try_into().ok()?);
                (value <= seq).then(|| key.to_vec())
            })
            .collect();
        let deleted = keys.len();
        for key in keys {
            db.delete(&mut wtxn, key.as_slice())?;
        }
        wtxn.commit()?;
        Ok(deleted)
    }

    /// List of registered collections.
    pub fn collections(&self) -> Result<Vec<String>> {
        let rtxn = self.read_txn()?;
        let out = self.collections_in(&rtxn)?;
        rtxn.commit()?;
        Ok(out)
    }

    /// Per-collection record counts and storage estimates for the current
    /// database. Data/index bytes use LMDB B-tree page allocation; history and
    /// TTL bytes are measured from shared key/value entries. The file
    /// high-water mark can be larger because LMDB does not shrink the file and
    /// freed pages may remain allocated.
    pub fn storage_stats(&self) -> Result<DatabaseSpace> {
        let rtxn = self.read_txn()?;
        let names = self.collections_in(&rtxn)?;
        let history_db: Option<Database<Bytes, Bytes>> =
            self.env.open_database(&rtxn, Some(HISTORY_DB))?;
        let ttl_db: Option<Database<Bytes, Bytes>> = self.env.open_database(&rtxn, Some(TTL_DB))?;
        let mut history = HashMap::<String, u64>::new();
        let mut ttl = HashMap::<String, u64>::new();

        if let Some(db) = history_db {
            for item in db.iter(&rtxn)? {
                let (key, value) = item?;
                if let Some((collection, _, _)) = history::parse_key(key) {
                    let bytes = (key.len() + value.len()) as u64;
                    *history.entry(collection.to_string()).or_default() += bytes;
                }
            }
        }
        if let Some(db) = ttl_db {
            for item in db.iter(&rtxn)? {
                let (key, value) = item?;
                let collection = ttl::parse_expiry_key(key)
                    .map(|(_, collection, _)| collection)
                    .or_else(|| ttl::parse_id_key(key).map(|(collection, _)| collection));
                if let Some(collection) = collection {
                    let bytes = (key.len() + value.len()) as u64;
                    *ttl.entry(collection.to_string()).or_default() += bytes;
                }
            }
        }

        let mut collections = Vec::with_capacity(names.len());
        let mut total_records = 0u64;
        let mut collection_bytes = 0u64;
        for name in names {
            let Some(collection) = self.collection_at(&rtxn, &name)? else {
                continue;
            };
            let stats = collection.storage_stats(&rtxn)?;
            let history_bytes = history.get(&name).copied().unwrap_or(0);
            let ttl_bytes = ttl.get(&name).copied().unwrap_or(0);
            let total = stats
                .data_bytes
                .saturating_add(stats.index_bytes)
                .saturating_add(history_bytes)
                .saturating_add(ttl_bytes);
            total_records = total_records.saturating_add(stats.records);
            collection_bytes = collection_bytes.saturating_add(total);
            collections.push(CollectionSpace {
                collection: name,
                records: stats.records,
                data_bytes: stats.data_bytes,
                index_bytes: stats.index_bytes,
                indexes: stats.indexes,
                history_bytes,
                ttl_bytes,
                total_bytes: total,
                requests_last_minute: 0,
                avg_requests_per_second: 0.0,
            });
        }
        let map_size = self.map_size() as u64;
        let used_bytes = self.map_used_bytes() as u64;
        rtxn.commit()?;
        Ok(DatabaseSpace {
            map_size,
            used_bytes,
            free_bytes: map_size.saturating_sub(used_bytes),
            page_size: PAGE_SIZE as u64,
            total_records,
            collection_bytes,
            collections,
        })
    }

    /// List of collections in the given read transaction.
    /// Lists immutable search sidecar files referenced by every collection in
    /// this environment. Paths are relative to the database directory.
    pub fn search_sidecar_files(&self) -> Result<Vec<String>> {
        let rtxn = self.read_txn()?;
        let mut files = Vec::new();
        for name in self.collections_in(&rtxn)? {
            if let Some(collection) = self.collection_at(&rtxn, &name)? {
                files.extend(collection.search_sidecar_files_in(&rtxn)?);
            }
        }
        rtxn.commit()?;
        files.sort_unstable();
        files.dedup();
        Ok(files)
    }

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
                    if name != CONTROL_DB {
                        out.push(name.to_string());
                    }
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
    fn drop_collection_removes_data_indexes_and_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let store = super::Store::open(dir.path()).unwrap();
        let users = store
            .create_collection("users", crate::vault::CollectionProfile::Standard)
            .unwrap();
        users
            .insert_one(serde_json::json!({"name": "Ala"}))
            .unwrap();
        users.create_index("name", true).unwrap();
        store.set_collection_history("users", 5).unwrap();
        let orders = store
            .create_collection("orders", crate::vault::CollectionProfile::Standard)
            .unwrap();
        orders.insert_one(serde_json::json!({"n": 1})).unwrap();

        assert!(store.drop_collection("users").unwrap());
        assert_eq!(store.collections().unwrap(), vec!["orders".to_string()]);
        assert!(!store.drop_collection("users").unwrap());
        let env = store.env().clone();
        drop(users);
        drop(orders);
        drop(store);
        env.prepare_for_closing().wait();

        let report = crate::verify_data_dir(dir.path()).unwrap();
        assert!(report.collections.contains_key("orders"));
        assert!(!report.collections.contains_key("users"));
        assert_eq!(report.history_entries, 0);
    }

    #[test]
    fn drop_collection_cleans_graph_metadata_and_adjacency() {
        let dir = tempfile::tempdir().unwrap();
        let store = super::Store::open(dir.path()).unwrap();
        let nodes = store
            .create_collection("nodes", crate::vault::CollectionProfile::Standard)
            .unwrap();
        let _edges = store
            .create_collection("edges", crate::vault::CollectionProfile::Standard)
            .unwrap();
        let def: crate::graph::GraphDef = serde_json::from_value(serde_json::json!({
            "node_collection": "nodes",
            "edge_collection": "edges"
        }))
        .unwrap();
        nodes.set_graph(def.clone()).unwrap();
        assert!(nodes.get_graph().unwrap().is_some());

        assert!(store.drop_collection("nodes").unwrap());
        let recreated = store
            .create_collection("nodes", crate::vault::CollectionProfile::Standard)
            .unwrap();
        assert!(recreated.get_graph().unwrap().is_none());

        // Dropping the node side must also free the edge collection's reverse
        // mapping so another graph can reuse it.
        let other = store
            .create_collection("other", crate::vault::CollectionProfile::Standard)
            .unwrap();
        let mut reused = def.clone();
        reused.node_collection = "other".into();
        other.set_graph(reused).unwrap();

        assert!(store.drop_collection("edges").unwrap());
        let _edges = store
            .create_collection("edges", crate::vault::CollectionProfile::Standard)
            .unwrap();
        assert!(other.get_graph().unwrap().is_none());
        let third = store
            .create_collection("third", crate::vault::CollectionProfile::Standard)
            .unwrap();
        let mut third_def = def;
        third_def.node_collection = "third".into();
        third.set_graph(third_def).unwrap();
    }

    #[test]
    fn parses_map_sizes() {
        assert_eq!(parse_size("4GiB"), Some(4 * (1 << 30)));
        assert_eq!(parse_size("2048MiB"), Some(2048 * (1 << 20)));
        assert_eq!(parse_size("512mb"), Some(512_000_000));
        assert_eq!(parse_size("1073741824"), Some(1 << 30));
        assert_eq!(parse_size("nonsense"), None);
    }

    #[test]
    fn store_open_recovers_interrupted_compaction_swap() {
        let dir = tempfile::tempdir().unwrap();
        let store = super::Store::open(dir.path()).unwrap();
        let c = store.collection("docs").unwrap();
        c.insert_one(serde_json::json!({"_id": "a", "value": 1}))
            .unwrap();
        store.close();

        let source = dir.path().join("data.mdb");
        let backup = dir.path().join("data.mdb.before-compact");
        std::fs::rename(&source, &backup).unwrap();
        assert!(!source.exists());

        let store = super::Store::open(dir.path()).unwrap();
        assert_eq!(
            store
                .collection("docs")
                .unwrap()
                .count(&serde_json::json!({}))
                .unwrap(),
            1
        );
        assert!(source.exists());
        assert!(!backup.exists());
    }

    #[test]
    fn compact_data_dir_reclaims_deleted_timeseries_rows() {
        let dir = tempfile::tempdir().unwrap();
        let store = super::Store::open(dir.path()).unwrap();
        let settings = crate::timeseries::Settings {
            time_field: "ts".into(),
            meta_field: Some("device".into()),
            ..Default::default()
        };
        let c = store
            .create_timeseries_collection("metrics", &settings)
            .unwrap();
        for idx in 0..2000i64 {
            c.insert_one(serde_json::json!({
                "ts": idx * 1000,
                "device": {"id": format!("dev-{}", idx % 10)},
                "value": idx,
                "payload": "x".repeat(200),
            }))
            .unwrap();
        }
        store.seal_timeseries("metrics").unwrap();
        assert_eq!(
            c.delete(&serde_json::json!({"value": {"$lt": 1900}}), true)
                .unwrap(),
            1900
        );
        store.close();

        let before = std::fs::metadata(dir.path().join("data.mdb"))
            .unwrap()
            .len();
        let report = super::compact_data_dir(dir.path()).unwrap();
        assert!(report.after_bytes < before);
        assert!(!dir.path().join("data.mdb.compacting").exists());
        assert!(!dir.path().join("data.mdb.before-compact").exists());

        let store = super::Store::open(dir.path()).unwrap();
        let c = store.collection("metrics").unwrap();
        assert_eq!(c.count(&serde_json::json!({})).unwrap(), 100);
        let docs = c
            .find(&crate::collection::FindQuery {
                filter: serde_json::json!({}),
                projection: None,
                sort: Vec::new(),
                skip: 0,
                limit: Some(1),
                comment: None,
                max_time_ms: None,
                hint: None,
            })
            .unwrap();
        let id = docs[0]["_id"].as_str().unwrap();
        assert!(c.find_by_key(id).unwrap().is_some());
    }
}

#[cfg(test)]
mod sync_policy_tests {
    use super::*;

    #[test]
    fn default_policy_syncs_every_commit() {
        assert_eq!(sync_policy(false, 0), SyncPolicy::PerCommit);
    }

    #[test]
    fn interval_policy_skips_per_commit_sync() {
        assert_eq!(sync_policy(false, 100), SyncPolicy::Interval);
    }

    #[test]
    fn unsafe_writes_win_over_the_interval() {
        // An operator who asked for crash loss explicitly must not silently get
        // interval syncing on top of it.
        assert_eq!(sync_policy(true, 100), SyncPolicy::Unsafe);
        assert_eq!(sync_policy(true, 0), SyncPolicy::Unsafe);
    }
}
