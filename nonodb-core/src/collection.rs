//! Document collection operations.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use heed::types::Bytes;
use heed::{Database, Env, RoTxn, RwTxn};
use serde_json::{Map, Value};

use crate::aggregate;
use crate::bson_types;
use crate::date;
use crate::error::{Error, Result};
use crate::history::{self, HistoryConfig, HistoryEntry};
use crate::index::{self, IndexDef};
use crate::numeric;
use crate::query::{
    apply_upsert_with_options, apply_with_options, matches, path, seed_from_filter,
};
use crate::search;
use crate::store::MAX_DOCUMENT_BYTES;
use crate::ttl::{self, TtlUpdate};
use crate::vault::{self, CollectionProfile, VaultKey};
use crate::verify::{CollectionVerification, IndexVerification};

/// Meta prefix for reusable physical sub-database slots.
const DB_SLOT_META_PREFIX: &str = "\u{1}dbslot:";
/// Reserve some LMDB sub-database slots for collections and fixed databases.
const MAX_DB_SLOTS: u32 = crate::store::MAX_DBS - 64;

/// Minimum document count for a sorted full scan to use multiple threads.
#[cfg(not(test))]
const PARALLEL_SCAN_MIN_ENTRIES: usize = 50_000;
#[cfg(test)]
const PARALLEL_SCAN_MIN_ENTRIES: usize = 200;

/// Maximum `skip + limit` for the parallel path. With a deep `skip`
/// each thread would have to keep its own large TopK; it is better to stay on
/// one thread and keep memory use bounded.
const PARALLEL_SCAN_MAX_WANT: usize = 50_000;

/// Minimum `skip + limit` for a parallel scan without sorting to be worthwhile.
/// For `find_one` and very small limits, early exit is usually faster.
const PARALLEL_UNSORTED_MIN_WANT: usize = 16;

/// Size of the raw-byte chunk passed to worker threads.
const PARALLEL_SCAN_CHUNK: usize = 8192;

/// Scan chunk: one byte buffer plus document offsets (fewer allocations).
struct ScanChunk {
    idx: usize,
    data: Vec<u8>,
    docs: Vec<(usize, u32, u32)>,
}

/// Cheap full-scan prefilter: for simple string/bool equalities, checks
/// whether the key and value appear in the raw JSON at all. False positives are
/// acceptable (the matcher runs afterwards), but false negatives are not
/// allowed. Numbers and `null` are skipped because their semantics/formatting
/// differences require the full parser.
#[derive(Debug, Clone, Default)]
struct RawPrefilter {
    needles: Vec<Vec<u8>>,
}

impl RawPrefilter {
    fn matches(&self, bytes: &[u8]) -> bool {
        self.needles
            .iter()
            .all(|needle| memchr::memmem::find(bytes, needle).is_some())
    }
}

/// Builds a prefilter for safe equality conditions. Conditions we cannot
/// check at the byte level are ignored (the filter is an AND, so this is safe).
fn raw_prefilter(filter: &Value) -> Option<RawPrefilter> {
    let obj = filter.as_object()?;
    let mut needles = Vec::new();
    for (key, cond) in obj {
        if key.starts_with('$') {
            // `$or` / `$not` / `$nor` cannot be safely ignored.
            return None;
        }
        if key.contains('.') || key == "_id" {
            continue;
        }
        let Some(value) = raw_equality_value(cond) else {
            continue;
        };
        let key_bytes = serde_json::to_vec(&Value::String(key.clone())).ok()?;
        let value_bytes = serde_json::to_vec(value).ok()?;
        if !needles.contains(&key_bytes) {
            needles.push(key_bytes);
        }
        if !needles.contains(&value_bytes) {
            needles.push(value_bytes);
        }
    }
    if needles.is_empty() {
        None
    } else {
        Some(RawPrefilter { needles })
    }
}

/// Returns the equality value if it can be safely found in the raw
/// JSON. Only strings and booleans are supported; the rest goes to the parser.
fn raw_equality_value(cond: &Value) -> Option<&Value> {
    match cond {
        Value::String(_) | Value::Bool(_) => Some(cond),
        Value::Object(ops) if ops.len() == 1 => match ops.get("$eq") {
            Some(Value::String(_)) | Some(Value::Bool(_)) => ops.get("$eq"),
            _ => None,
        },
        _ => None,
    }
}

/// Parses a document only when the prefilter did not reject it.
fn parse_prefiltered(bytes: &[u8], raw: Option<&RawPrefilter>) -> Result<Option<Value>> {
    if raw.is_some_and(|r| !r.matches(bytes)) {
        return Ok(None);
    }
    parse_json(bytes).map(Some)
}

/// Fast JSON parsing (SIMD) into `serde_json::Value`.
fn parse_json(bytes: &[u8]) -> Result<Value> {
    sonic_rs::from_slice::<Value>(bytes).map_err(|e| Error::JsonFast(e.to_string()))
}

type TextBackfillEntry = (String, Vec<(String, u32)>, u32);

/// `update` operation result (with optional upsert).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateOutcome {
    pub matched: usize,
    pub modified: usize,
    pub upserted_id: Option<String>,
}

/// Options for an atomic find-and-modify operation.
#[derive(Debug, Clone)]
pub struct FindAndModifyOptions<'a> {
    pub filter: &'a Value,
    /// Update document, or `None` when `remove` is set.
    pub update: Option<&'a Value>,
    pub sort: &'a [(String, i32)],
    /// Return the document after the operation instead of before it.
    pub new: bool,
    pub upsert: bool,
    pub remove: bool,
    pub array_filters: &'a [Value],
    pub projection: Option<&'a Value>,
}

/// Result of an atomic find-and-modify operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindAndModifyOutcome {
    /// Pre-image when `new = false`, post-image when `new = true`.
    pub value: Option<Value>,
    pub matched: usize,
    pub modified: usize,
    pub upserted_id: Option<String>,
}
type IndexIter<'a> = Box<dyn Iterator<Item = heed::Result<(&'a [u8], &'a [u8])>> + 'a>;
type IdBounds = (Bound<Vec<u8>>, Bound<Vec<u8>>);

/// Backfill chunk: a buffer of `_id` values plus documents.
struct BackfillChunk {
    data: Vec<u8>,
    docs: Vec<(u32, u32, u32, u32)>, // id_start, id_len, doc_start, doc_len
}

/// Selected index and key range for a filter.
struct IndexPlan {
    db: Database<Bytes, Bytes>,
    bounds: index::RangeBytes,
    def: IndexDef,
}

/// `find` query parameters.
#[derive(Debug, Clone, Default)]
pub struct FindQuery {
    /// MongoDB-style filter; `Null`/`{}` = match everything.
    pub filter: Value,
    /// Field projection (currently flat, top-level).
    pub projection: Option<Value>,
    /// Sort keys: `(path, 1 | -1)`.
    pub sort: Vec<(String, i32)>,
    pub skip: usize,
    pub limit: Option<usize>,
}

pub struct Collection<'e> {
    db: Database<Bytes, Bytes>,
    meta: Database<Bytes, Bytes>,
    history: Database<Bytes, Bytes>,
    ttl_db: Database<Bytes, Bytes>,
    history_config: HistoryConfig,
    name: String,
    profile: CollectionProfile,
    ttl_default_seconds: Option<u64>,
    ttl_purge_history: bool,
    env: &'e Env,
    txn_gate: &'e crate::store::TxnGate,
}

impl<'e> Collection<'e> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        env: &'e Env,
        db: Database<Bytes, Bytes>,
        meta: Database<Bytes, Bytes>,
        history: Database<Bytes, Bytes>,
        ttl_db: Database<Bytes, Bytes>,
        history_config: HistoryConfig,
        name: String,
        profile: CollectionProfile,
        ttl_default_seconds: Option<u64>,
        ttl_purge_history: bool,
        txn_gate: &'e crate::store::TxnGate,
    ) -> Self {
        Self {
            db,
            meta,
            history,
            ttl_db,
            history_config,
            name,
            profile,
            ttl_default_seconds,
            ttl_purge_history,
            env,
            txn_gate,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Collection profile (`standard` or `vault`).
    pub fn profile(&self) -> CollectionProfile {
        self.profile
    }

    fn ensure_vault(&self) -> Result<()> {
        if self.profile.is_vault() {
            Ok(())
        } else {
            Err(Error::Vault {
                code: "not_a_vault_collection",
                message: format!("collection `{}` is not a vault collection", self.name),
            })
        }
    }

    fn ensure_standard(&self) -> Result<()> {
        if self.profile.is_vault() {
            Err(vault::standard_write_forbidden())
        } else {
            Ok(())
        }
    }

    fn set_ttl(&self, wtxn: &mut RwTxn<'_>, id: &str, at: Option<u64>) -> Result<()> {
        ttl::set_entry(&self.ttl_db, wtxn, &self.name, id, at)
    }

    fn ttl_at(&self, txn: &RoTxn<'_>, id: &str) -> Result<Option<u64>> {
        ttl::get_entry(&self.ttl_db, txn, &self.name, id)
    }

    /// Collection default TTL in seconds, if configured.
    pub fn ttl_default_seconds(&self) -> Option<u64> {
        self.ttl_default_seconds
    }

    /// Whether TTL expiry purges stored history instead of appending a tombstone.
    pub fn ttl_purge_history(&self) -> bool {
        self.ttl_purge_history
    }

    // ----------------------------- indexes -----------------------------

    fn db_slot_map_key(resource: &str) -> String {
        format!("{DB_SLOT_META_PREFIX}map:{resource}")
    }

    fn db_slot_owner_key(slot: u32) -> String {
        format!("{DB_SLOT_META_PREFIX}owner:{slot}")
    }

    fn db_slot_name(slot: u32) -> String {
        format!("{DB_SLOT_META_PREFIX}db:{slot}")
    }

    fn db_slot_for(&self, txn: &RoTxn<'_>, resource: &str) -> Result<Option<u32>> {
        let key = Self::db_slot_map_key(resource);
        let Some(bytes) = self.meta.get(txn, key.as_bytes())? else {
            return Ok(None);
        };
        let bytes: [u8; 4] = bytes
            .try_into()
            .map_err(|_| Error::Query("invalid sub-database slot mapping".into()))?;
        Ok(Some(u32::from_le_bytes(bytes)))
    }

    /// Opens a resource DB by its reusable slot, falling back to the legacy
    /// logical name for databases created before slot reuse was introduced.
    fn resource_db(
        &self,
        txn: &RoTxn<'_>,
        resource: &str,
        legacy_name: &str,
    ) -> Result<Option<Database<Bytes, Bytes>>> {
        match self.db_slot_for(txn, resource)? {
            Some(slot) => Ok(self
                .env
                .open_database(txn, Some(&Self::db_slot_name(slot)))?),
            None => Ok(self.env.open_database(txn, Some(legacy_name))?),
        }
    }

    fn resource_db_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        resource: &str,
        legacy_name: &str,
    ) -> Result<Database<Bytes, Bytes>> {
        if let Some(slot) = self.db_slot_for(&*wtxn, resource)? {
            return Ok(self
                .env
                .create_database(&mut *wtxn, Some(&Self::db_slot_name(slot)))?);
        }
        // Keep using an existing legacy DB until it is dropped. Its LMDB slot
        // cannot be deregistered by heed, but all newly created resources use
        // the reusable pool below.
        if self
            .env
            .open_database::<Bytes, Bytes>(&*wtxn, Some(legacy_name))?
            .is_some()
        {
            return Ok(self.env.create_database(&mut *wtxn, Some(legacy_name))?);
        }
        let slot = self.allocate_db_slot(wtxn, resource)?;
        Ok(self
            .env
            .create_database(&mut *wtxn, Some(&Self::db_slot_name(slot)))?)
    }

    fn allocate_db_slot(&self, wtxn: &mut RwTxn<'_>, resource: &str) -> Result<u32> {
        for slot in 1..=MAX_DB_SLOTS {
            let owner_key = Self::db_slot_owner_key(slot);
            if self.meta.get(&*wtxn, owner_key.as_bytes())?.is_some() {
                continue;
            }
            let map_key = Self::db_slot_map_key(resource);
            self.meta
                .put(&mut *wtxn, owner_key.as_bytes(), resource.as_bytes())?;
            self.meta
                .put(&mut *wtxn, map_key.as_bytes(), &slot.to_le_bytes())?;
            return Ok(slot);
        }
        Err(Error::Query(
            "LMDB sub-database slot limit reached; drop unused indexes".into(),
        ))
    }

    /// Clears a resource DB and returns its slot to the reusable pool.
    fn drop_resource_db(
        &self,
        wtxn: &mut RwTxn<'_>,
        resource: &str,
        legacy_name: &str,
    ) -> Result<()> {
        if let Some(slot) = self.db_slot_for(&*wtxn, resource)? {
            if let Some(db) = self
                .env
                .open_database::<Bytes, Bytes>(&*wtxn, Some(&Self::db_slot_name(slot)))?
            {
                db.clear(&mut *wtxn)?;
            }
            self.meta
                .delete(&mut *wtxn, Self::db_slot_map_key(resource).as_bytes())?;
            self.meta
                .delete(&mut *wtxn, Self::db_slot_owner_key(slot).as_bytes())?;
            return Ok(());
        }
        if let Some(db) = self
            .env
            .open_database::<Bytes, Bytes>(&*wtxn, Some(legacy_name))?
        {
            db.clear(&mut *wtxn)?;
        }
        Ok(())
    }

    /// Index definitions for this collection (read in the given transaction).
    fn index_defs(&self, txn: &RoTxn<'_>) -> Result<Vec<IndexDef>> {
        let prefix = format!("index:{}:", self.name);
        let mut defs = Vec::new();
        for item in self.meta.prefix_iter(txn, prefix.as_bytes())? {
            let (_key, bytes) = item?;
            defs.push(serde_json::from_slice(bytes)?);
        }
        Ok(defs)
    }

    fn index_db(&self, txn: &RoTxn<'_>, def: &IndexDef) -> Result<Option<Database<Bytes, Bytes>>> {
        let name = index::db_name(&self.name, &def.name());
        self.resource_db(txn, &name, &name)
    }

    fn index_db_in(&self, wtxn: &mut RwTxn<'_>, def: &IndexDef) -> Result<Database<Bytes, Bytes>> {
        let name = index::db_name(&self.name, &def.name());
        self.resource_db_in(wtxn, &name, &name)
    }

    /// Creates a secondary index (single field) and backfills it with existing
    /// documents.
    pub fn create_index(&self, field: &str, unique: bool) -> Result<()> {
        self.create_index_fields(&[field.to_string()], unique, &[])
    }

    pub fn create_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str, unique: bool) -> Result<()> {
        self.create_index_fields_txn(wtxn, &[field.to_string()], unique, &[])
    }

    /// Creates a compound index. Field order matters: a field prefix can
    /// serve a filter, and the full index can also serve sorting.
    pub fn create_index_fields(
        &self,
        fields: &[String],
        unique: bool,
        include: &[String],
    ) -> Result<()> {
        let mut wtxn = self.write_txn()?;
        self.create_index_fields_txn(&mut wtxn, fields, unique, include)?;
        wtxn.commit()?;
        Ok(())
    }

    pub fn create_index_fields_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        fields: &[String],
        unique: bool,
        include: &[String],
    ) -> Result<()> {
        self.ensure_standard()?;
        if fields.is_empty() {
            return Err(Error::Query("index requires at least one field".into()));
        }
        if fields.iter().any(|f| f.is_empty() || f.contains('+')) {
            return Err(Error::Query(
                "index fields must be non-empty and must not contain `+`".into(),
            ));
        }
        let mut def = IndexDef::compound(fields, unique, include);
        let key = index::def_key(&self.name, &def.name());
        self.meta.put(
            &mut *wtxn,
            key.as_bytes(),
            serde_json::to_vec(&def)?.as_slice(),
        )?;
        let idx_db = self.index_db_in(wtxn, &def)?;

        // Backfill: parallel parsing/encoding, written in one transaction.
        let total_docs = self.db.stat(wtxn)?.entries as u64;
        let def_ref = &def;
        let entries: Vec<(index::IndexKeys, Vec<u8>, String)> =
            self.parallel_backfill(wtxn, "index", &def.name(), |id, doc| {
                let keys = index::encode_index_keys(def_ref, &doc)?;
                if keys.keys.is_empty() {
                    return Ok(Vec::new());
                }
                let value = index::encode_index_value(def_ref, &doc, &id);
                Ok(vec![(keys, value, id)])
            })?;

        for (keys, _, _) in &entries {
            for field in &keys.multikey_fields {
                if !def.multikey_fields.contains(field) {
                    def.multikey_fields.push(field.clone());
                }
            }
        }
        def.multikey_all_docs = entries.len() as u64 == total_docs;
        self.meta.put(
            &mut *wtxn,
            key.as_bytes(),
            serde_json::to_vec(&def)?.as_slice(),
        )?;

        let mut seen: HashMap<Vec<u8>, String> = HashMap::new();

        for (keys, value, id) in entries {
            for vb in keys.keys {
                if unique {
                    match seen.get(&vb) {
                        Some(existing) if existing != &id => {
                            return Err(Error::DuplicateKey(id));
                        }
                        Some(_) => continue,
                        None => {
                            seen.insert(vb.clone(), id.clone());
                        }
                    }
                }
                let mut key = vb;
                key.extend_from_slice(id.as_bytes());
                idx_db.put(&mut *wtxn, key.as_slice(), value.as_slice())?;
            }
        }
        Ok(())
    }

    /// Drops an index (clears its data and definition).
    pub fn drop_index(&self, field: &str) -> Result<bool> {
        let mut wtxn = self.write_txn()?;
        let existed = self.drop_index_txn(&mut wtxn, field)?;
        wtxn.commit()?;
        Ok(existed)
    }

    pub fn drop_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<bool> {
        let key = index::def_key(&self.name, field);
        let existed = self.meta.delete(&mut *wtxn, key.as_bytes())?;
        if existed {
            let name = index::db_name(&self.name, field);
            self.drop_resource_db(wtxn, &name, &name)?;
        }
        Ok(existed)
    }

    /// Index list `(name, unique)`; a compound index is named `a+b`.
    pub fn list_indexes(&self) -> Result<Vec<(String, bool)>> {
        let rtxn = self.read_txn()?;
        let out = self.list_indexes_in(&rtxn)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn list_indexes_in(&self, rtxn: &RoTxn<'_>) -> Result<Vec<(String, bool)>> {
        Ok(self
            .index_defs(rtxn)?
            .into_iter()
            .map(|d| (d.name(), d.unique))
            .collect())
    }

    /// Full index definitions in the given transaction.
    pub fn index_defs_in(&self, rtxn: &RoTxn<'_>) -> Result<Vec<IndexDef>> {
        self.index_defs(rtxn)
    }

    /// Full index definitions (for the API/CLI).
    pub fn list_index_defs(&self) -> Result<Vec<IndexDef>> {
        let rtxn = self.read_txn()?;
        let out = self.index_defs(&rtxn)?;
        rtxn.commit()?;
        Ok(out)
    }

    /// Checks the new value for uniqueness across all unique indexes.
    fn validate_unique(&self, txn: &RoTxn<'_>, doc: &Value, id: &str) -> Result<()> {
        for def in self.index_defs(txn)? {
            if !def.unique {
                continue;
            }
            let keys = index::encode_index_keys(&def, doc)?;
            if keys.keys.is_empty() {
                continue;
            }
            let Some(db) = self.index_db(txn, &def)? else {
                continue;
            };
            for vb in keys.keys {
                for item in db.prefix_iter(txn, vb.as_slice())? {
                    let (_key, existing) = item?;
                    if index::decode_index_value(existing)
                        .is_some_and(|(existing_id, _)| existing_id != id)
                    {
                        return Err(Error::DuplicateKey(id.to_string()));
                    }
                }
            }
        }
        Ok(())
    }

    /// Updates index entries for an `old -> new` change (insert: `old=None`,
    /// delete: `new=None`).
    fn apply_index_entries(
        &self,
        wtxn: &mut RwTxn<'_>,
        old: Option<&Value>,
        new: Option<&Value>,
        id: &str,
    ) -> Result<()> {
        let defs = self.index_defs(&*wtxn)?;
        for mut def in defs {
            let mut old_keys = old
                .map(|doc| index::encode_index_keys(&def, doc))
                .transpose()?
                .unwrap_or(index::IndexKeys {
                    keys: Vec::new(),
                    multikey_fields: Vec::new(),
                });
            let new_keys = new
                .map(|doc| index::encode_index_keys(&def, doc))
                .transpose()?
                .unwrap_or(index::IndexKeys {
                    keys: Vec::new(),
                    multikey_fields: Vec::new(),
                });
            // With `include`, copied fields must also be compared because they can
            // change without changing the index key.
            let old_value = old.map(|d| index::encode_index_value(&def, d, id));
            let new_value = new.map(|d| index::encode_index_value(&def, d, id));
            let mut def_changed = false;
            for field in &new_keys.multikey_fields {
                if !def.multikey_fields.contains(field) {
                    def.multikey_fields.push(field.clone());
                    def_changed = true;
                }
            }
            if new.is_some() && def.multikey_all_docs && new_keys.keys.is_empty() {
                def.multikey_all_docs = false;
                def_changed = true;
            }
            if def_changed {
                let def_key = index::def_key(&self.name, &def.name());
                self.meta.put(
                    &mut *wtxn,
                    def_key.as_bytes(),
                    serde_json::to_vec(&def)?.as_slice(),
                )?;
            }
            old_keys.keys.sort();
            old_keys.keys.dedup();
            if old_keys.keys == new_keys.keys && old_value == new_value {
                continue;
            }
            let idx_db = self.index_db_in(wtxn, &def)?;
            for vb in old_keys.keys {
                let mut key = vb;
                key.extend_from_slice(id.as_bytes());
                idx_db.delete(&mut *wtxn, key.as_slice())?;
            }
            for vb in new_keys.keys {
                let mut key = vb;
                key.extend_from_slice(id.as_bytes());
                let value = new_value.as_deref().expect("nowy dokument");
                idx_db.put(&mut *wtxn, key.as_slice(), value)?;
            }
        }
        Ok(())
    }

    /// Selects the index sub-database and key range for a filter.
    ///
    /// A compound index `[a, b, c]` is used when the filter has conditions for
    /// all fields, with every field except the last being equality conditions
    /// (`a: 1` or `a: {$eq: 1}`). The last field may have a range condition.
    fn index_plan(&self, rtxn: &RoTxn<'_>, filter: &Value) -> Result<Option<IndexPlan>> {
        let Some(obj) = filter.as_object() else {
            return Ok(None);
        };
        if obj.is_empty() || obj.keys().any(|k| k.starts_with('$')) {
            return Ok(None);
        }

        // Try indexes with the longest field prefix first (most selective).
        let mut defs = self.index_defs(rtxn)?;
        defs.sort_by_key(|d| std::cmp::Reverse(d.effective_fields().len()));

        for def in defs {
            // Older indexes skipped arrays and some Extended JSON values during
            // backfill. Only current-format indexes are safe for planning.
            if !def.current_format() {
                continue;
            }
            let Some(bounds) = bounds_for_index(&def, obj) else {
                continue;
            };
            let Some(db) = self.index_db(rtxn, &def)? else {
                continue;
            };
            return Ok(Some(IndexPlan { db, bounds, def }));
        }
        Ok(None)
    }

    /// Selects an index that can provide a sort-only query order.
    ///
    /// Missing fields are absent from secondary indexes, while a sort must place
    /// them last. The index is therefore usable only when it has one entry for
    /// every document; otherwise the query falls back to a collection scan.
    fn sort_index_plan(
        &self,
        rtxn: &RoTxn<'_>,
        sort: &[(String, i32)],
    ) -> Result<Option<IndexPlan>> {
        if sort.is_empty() {
            return Ok(None);
        }
        let document_count = self.db.stat(rtxn)?.entries;
        let mut defs = self.index_defs(rtxn)?;
        defs.sort_by_key(|d| std::cmp::Reverse(d.effective_fields().len()));
        for def in defs {
            if !def.current_format()
                || def.is_multikey()
                || index_matches_sort(&def, sort).is_none()
            {
                continue;
            }
            let Some(db) = self.index_db(rtxn, &def)? else {
                continue;
            };
            if db.stat(rtxn)?.entries != document_count {
                continue;
            }
            return Ok(Some(IndexPlan {
                db,
                bounds: (Bound::Unbounded, Bound::Unbounded),
                def,
            }));
        }
        Ok(None)
    }

    /// Selects a multikey index for a sort-only query.
    ///
    /// The array field must be the last index field. Scanning the index in the
    /// requested direction and taking the first entry per document then yields
    /// the minimum (ascending) or maximum (descending) array element, matching
    /// MongoDB's multikey sort rule. `multikey_all_docs` guarantees that no
    /// document is missing from the index.
    fn multikey_sort_plan(
        &self,
        rtxn: &RoTxn<'_>,
        sort: &[(String, i32)],
    ) -> Result<Option<IndexPlan>> {
        if sort.is_empty() {
            return Ok(None);
        }
        let mut defs = self.index_defs(rtxn)?;
        defs.sort_by_key(|d| std::cmp::Reverse(d.effective_fields().len()));
        for def in defs {
            if !def.current_format()
                || !def.is_multikey()
                || !def.multikey_all_docs
                || index_matches_sort(&def, sort).is_none()
            {
                continue;
            }
            let fields = def.effective_fields();
            let Some(last) = fields.last() else {
                continue;
            };
            if !def.multikey_fields.contains(last) {
                continue;
            }
            let Some(db) = self.index_db(rtxn, &def)? else {
                continue;
            };
            return Ok(Some(IndexPlan {
                db,
                bounds: (Bound::Unbounded, Bound::Unbounded),
                def,
            }));
        }
        Ok(None)
    }

    /// Sorted match-all find using primary-key or secondary-index order.
    ///
    /// Returns `None` when no safe ordered path is available; the caller then
    /// falls back to the normal collection scan.
    fn find_sorted_match_all(
        &self,
        rtxn: &RoTxn<'_>,
        query: &FindQuery,
    ) -> Result<Option<Vec<Value>>> {
        let limit = query.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Some(Vec::new()));
        }
        if let Some(reverse) = id_sort_direction(&query.sort) {
            let iter: IndexIter<'_> = if reverse {
                Box::new(self.db.rev_iter(rtxn)?)
            } else {
                Box::new(self.db.iter(rtxn)?)
            };
            let mut out = Vec::new();
            for item in iter.skip(query.skip) {
                let (_key, bytes) = item?;
                out.push(parse_json(bytes)?);
                if out.len() >= limit {
                    break;
                }
            }
            return Ok(Some(
                out.into_iter()
                    .map(|doc| match &query.projection {
                        Some(projection) => project(&doc, projection),
                        None => doc,
                    })
                    .collect(),
            ));
        }

        let plan = match self.sort_index_plan(rtxn, &query.sort)? {
            Some(plan) => plan,
            None => match self.multikey_sort_plan(rtxn, &query.sort)? {
                Some(plan) => plan,
                None => return Ok(None),
            },
        };
        let reverse = index_matches_sort(&plan.def, &query.sort).expect("matched sort");
        let multikey = plan.def.is_multikey();
        let range = (Bound::Unbounded, Bound::Unbounded);
        let iter: IndexIter<'_> = if reverse {
            Box::new(plan.db.rev_range(rtxn, &range)?)
        } else {
            Box::new(plan.db.range(rtxn, &range)?)
        };
        let mut skipped = 0usize;
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        for item in iter {
            let (_key, value) = item?;
            let Some((id, inc)) = index::decode_index_value(value) else {
                continue;
            };
            if multikey && !seen.insert(id.clone()) {
                continue;
            }
            let doc = if !multikey {
                if let Some(doc) = covering_doc(&id, inc.as_ref(), &query.filter, &query.projection)
                {
                    doc
                } else {
                    let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                        continue;
                    };
                    parse_json(bytes)?
                }
            } else {
                let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                    continue;
                };
                parse_json(bytes)?
            };
            if skipped < query.skip {
                skipped += 1;
                continue;
            }
            out.push(doc);
            if out.len() >= limit {
                break;
            }
        }
        Ok(Some(
            out.into_iter()
                .map(|doc| match &query.projection {
                    Some(projection) => project(&doc, projection),
                    None => doc,
                })
                .collect(),
        ))
    }

    /// Returns one index plan per top-level `$or` branch, but only when every
    /// branch is indexable. A union is not semantics-preserving if any branch
    /// would require a collection scan.
    fn or_index_plans(&self, rtxn: &RoTxn<'_>, filter: &Value) -> Result<Option<Vec<IndexPlan>>> {
        let Some(obj) = filter.as_object() else {
            return Ok(None);
        };
        if obj.len() != 1 {
            return Ok(None);
        }
        let Some(branches) = obj.get("$or").and_then(Value::as_array) else {
            return Ok(None);
        };
        if branches.is_empty() {
            return Ok(None);
        }
        let mut plans = Vec::with_capacity(branches.len());
        for branch in branches {
            let Some(plan) = self.index_plan(rtxn, branch)? else {
                return Ok(None);
            };
            plans.push(plan);
        }
        Ok(Some(plans))
    }

    /// Candidate ids for a single-field `$all` filter. Every value must be
    /// present in the array, so the result is the intersection of the per-value
    /// index ranges. Returns `None` when no suitable multikey index exists.
    fn all_index_candidates(
        &self,
        rtxn: &RoTxn<'_>,
        filter: &Value,
    ) -> Result<Option<Vec<String>>> {
        let Some(obj) = filter.as_object() else {
            return Ok(None);
        };
        if obj.len() != 1 {
            return Ok(None);
        }
        let Some((field, cond)) = obj.iter().next() else {
            return Ok(None);
        };
        let Some(values) = cond
            .as_object()
            .filter(|ops| ops.len() == 1)
            .and_then(|ops| ops.get("$all"))
            .and_then(Value::as_array)
        else {
            return Ok(None);
        };
        if values.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let mut defs = self.index_defs(rtxn)?;
        defs.sort_by_key(|d| std::cmp::Reverse(d.effective_fields().len()));
        let Some(def) = defs.into_iter().find(|def| {
            def.current_format() && def.effective_fields().as_slice() == [field.as_str()]
        }) else {
            return Ok(None);
        };
        let Some(db) = self.index_db(rtxn, &def)? else {
            return Ok(None);
        };
        let mut result: Option<HashSet<String>> = None;
        for value in values {
            let Some((lower, upper)) = index::bounds("$eq", value) else {
                return Ok(None);
            };
            let range = (as_bound(&lower), as_bound(&upper));
            let mut ids = HashSet::new();
            for item in db.range(rtxn, &range)? {
                let (_key, value) = item?;
                if let Some((id, _)) = index::decode_index_value(value) {
                    ids.insert(id);
                }
            }
            result = Some(match result {
                Some(current) => current.intersection(&ids).cloned().collect(),
                None => ids,
            });
            if result.as_ref().is_some_and(HashSet::is_empty) {
                break;
            }
        }
        Ok(Some(result.unwrap_or_default().into_iter().collect()))
    }

    fn index_candidates(
        &self,
        rtxn: &RoTxn<'_>,
        filter: &Value,
        max: Option<usize>,
    ) -> Result<Option<Vec<String>>> {
        let Some(plan) = self.index_plan(rtxn, filter)? else {
            return Ok(None);
        };
        let (lower, upper) = &plan.bounds;
        let range = (as_bound(lower), as_bound(upper));

        let mut ids = Vec::new();
        let mut seen = HashSet::new();
        for item in plan.db.range(rtxn, &range)? {
            let (_key, value) = item?;
            if let Some((id, _)) = index::decode_index_value(value) {
                if seen.insert(id.clone()) {
                    ids.push(id);
                }
            }
            if max.is_some_and(|m| ids.len() >= m) {
                break;
            }
        }
        Ok(Some(ids))
    }

    /// Counts index entries in a range without allocating an `_id` list.
    fn index_count_plan(&self, rtxn: &RoTxn<'_>, plan: &IndexPlan) -> Result<usize> {
        let (lower, upper) = &plan.bounds;
        let range = (as_bound(lower), as_bound(upper));
        let mut n = 0usize;
        for item in plan.db.range(rtxn, &range)? {
            item?;
            n += 1;
        }
        Ok(n)
    }

    /// Counts distinct documents in a multikey index range.
    fn index_count_unique_plan(&self, rtxn: &RoTxn<'_>, plan: &IndexPlan) -> Result<usize> {
        let (lower, upper) = &plan.bounds;
        let range = (as_bound(lower), as_bound(upper));
        let mut ids = HashSet::new();
        for item in plan.db.range(rtxn, &range)? {
            let (_key, value) = item?;
            if let Some((id, _)) = index::decode_index_value(value) {
                ids.insert(id);
            }
        }
        Ok(ids.len())
    }

    // --------------------------- full-text ----------------------------

    fn text_defs(&self, txn: &RoTxn<'_>) -> Result<Vec<String>> {
        self.field_defs(txn, "text")
    }

    fn fuzzy_defs(&self, txn: &RoTxn<'_>) -> Result<Vec<String>> {
        self.field_defs(txn, "fuzzy")
    }

    fn field_defs(&self, txn: &RoTxn<'_>, kind: &str) -> Result<Vec<String>> {
        let prefix = format!("{kind}:{}:", self.name);
        let mut out = Vec::new();
        for item in self.meta.prefix_iter(txn, prefix.as_bytes())? {
            let (key, _) = item?;
            let s = std::str::from_utf8(key).map_err(|e| Error::Query(e.to_string()))?;
            if let Some(field) = s.strip_prefix(&prefix) {
                out.push(field.to_string());
            }
        }
        Ok(out)
    }

    fn text_postings(
        &self,
        txn: &RoTxn<'_>,
        field: &str,
    ) -> Result<Option<Database<Bytes, Bytes>>> {
        let name = search::text_db_name(&self.name, field);
        self.resource_db(txn, &name, &name)
    }

    fn text_postings_in(
        &self,
        wtxn: &mut RwTxn<'_>,
        field: &str,
    ) -> Result<Database<Bytes, Bytes>> {
        let name = search::text_db_name(&self.name, field);
        self.resource_db_in(wtxn, &name, &name)
    }

    fn text_len_db(&self, txn: &RoTxn<'_>, field: &str) -> Result<Option<Database<Bytes, Bytes>>> {
        let name = search::text_len_db_name(&self.name, field);
        self.resource_db(txn, &name, &name)
    }

    fn text_len_db_in(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<Database<Bytes, Bytes>> {
        let name = search::text_len_db_name(&self.name, field);
        self.resource_db_in(wtxn, &name, &name)
    }

    fn fuzzy_db(&self, txn: &RoTxn<'_>, field: &str) -> Result<Option<Database<Bytes, Bytes>>> {
        let name = search::fuzzy_db_name(&self.name, field);
        self.resource_db(txn, &name, &name)
    }

    fn fuzzy_db_in(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<Database<Bytes, Bytes>> {
        let name = search::fuzzy_db_name(&self.name, field);
        self.resource_db_in(wtxn, &name, &name)
    }

    /// Creates a full-text (BM25) index and backfills it with existing documents.
    pub fn create_text_index(&self, field: &str) -> Result<()> {
        let mut wtxn = self.write_txn()?;
        self.create_text_index_txn(&mut wtxn, field)?;
        wtxn.commit()?;
        Ok(())
    }

    pub fn create_text_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<()> {
        self.ensure_standard()?;
        let def_key = search::text_def_key(&self.name, field);
        if self.meta.get(&*wtxn, def_key.as_bytes())?.is_some() {
            return Ok(());
        }

        // Backfill: parallel tokenization, written in one transaction.
        let entries: Vec<TextBackfillEntry> =
            self.parallel_backfill(wtxn, "text", field, |id, doc| {
                let Some(text) = index::field_value(&doc, field).and_then(search::field_text)
                else {
                    return Ok(Vec::new());
                };
                let tokens = search::tokenize(&text);
                let dl = tokens.len() as u32;
                let tf = search::term_freqs(&tokens).into_iter().collect();
                Ok(vec![(id, tf, dl)])
            })?;

        let postings = self.text_postings_in(wtxn, field)?;
        let lens = self.text_len_db_in(wtxn, field)?;
        postings.clear(&mut *wtxn)?;
        lens.clear(&mut *wtxn)?;

        let mut total = 0u64;
        for (id, tf, dl) in entries {
            lens.put(&mut *wtxn, id.as_bytes(), &dl.to_le_bytes())?;
            total += u64::from(dl);
            for (term, tf) in tf {
                postings.put(
                    &mut *wtxn,
                    search::postings_key(&term, &id).as_slice(),
                    &tf.to_le_bytes(),
                )?;
            }
        }

        self.meta.put(&mut *wtxn, def_key.as_bytes(), b"1")?;
        self.meta.put(
            &mut *wtxn,
            search::text_total_key(&self.name, field).as_bytes(),
            &total.to_le_bytes(),
        )?;
        Ok(())
    }

    /// Drops a full-text index.
    pub fn drop_text_index(&self, field: &str) -> Result<bool> {
        let mut wtxn = self.write_txn()?;
        let existed = self.drop_text_index_txn(&mut wtxn, field)?;
        wtxn.commit()?;
        Ok(existed)
    }

    pub fn drop_text_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<bool> {
        let def_key = search::text_def_key(&self.name, field);
        let existed = self.meta.delete(&mut *wtxn, def_key.as_bytes())?;
        if existed {
            let postings = search::text_db_name(&self.name, field);
            self.drop_resource_db(wtxn, &postings, &postings)?;
            let lens = search::text_len_db_name(&self.name, field);
            self.drop_resource_db(wtxn, &lens, &lens)?;
            self.meta.delete(
                &mut *wtxn,
                search::text_total_key(&self.name, field).as_bytes(),
            )?;
        }
        Ok(existed)
    }

    pub fn list_text_indexes(&self) -> Result<Vec<String>> {
        let rtxn = self.read_txn()?;
        let out = self.list_text_indexes_in(&rtxn)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn list_text_indexes_in(&self, rtxn: &RoTxn<'_>) -> Result<Vec<String>> {
        self.text_defs(rtxn)
    }

    /// Creates a trigram (fuzzy) index and backfills it with existing documents.
    pub fn create_fuzzy_index(&self, field: &str) -> Result<()> {
        let mut wtxn = self.write_txn()?;
        self.create_fuzzy_index_txn(&mut wtxn, field)?;
        wtxn.commit()?;
        Ok(())
    }

    pub fn create_fuzzy_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<()> {
        self.ensure_standard()?;
        let def_key = search::fuzzy_def_key(&self.name, field);
        if self.meta.get(&*wtxn, def_key.as_bytes())?.is_some() {
            return Ok(());
        }

        let entries: Vec<(String, Vec<String>)> =
            self.parallel_backfill(wtxn, "fuzzy", field, |id, doc| {
                let Some(text) = index::field_value(&doc, field).and_then(search::field_text)
                else {
                    return Ok(Vec::new());
                };
                Ok(vec![(id, search::trigrams(&text))])
            })?;

        let db = self.fuzzy_db_in(wtxn, field)?;
        db.clear(&mut *wtxn)?;
        for (id, grams) in entries {
            for gram in grams {
                db.put(&mut *wtxn, search::postings_key(&gram, &id).as_slice(), &[])?;
            }
        }

        self.meta.put(&mut *wtxn, def_key.as_bytes(), b"1")?;
        Ok(())
    }

    pub fn drop_fuzzy_index(&self, field: &str) -> Result<bool> {
        let mut wtxn = self.write_txn()?;
        let existed = self.drop_fuzzy_index_txn(&mut wtxn, field)?;
        wtxn.commit()?;
        Ok(existed)
    }

    pub fn drop_fuzzy_index_txn(&self, wtxn: &mut RwTxn<'_>, field: &str) -> Result<bool> {
        let def_key = search::fuzzy_def_key(&self.name, field);
        let existed = self.meta.delete(&mut *wtxn, def_key.as_bytes())?;
        if existed {
            let name = search::fuzzy_db_name(&self.name, field);
            self.drop_resource_db(wtxn, &name, &name)?;
        }
        Ok(existed)
    }

    pub fn list_fuzzy_indexes(&self) -> Result<Vec<String>> {
        let rtxn = self.read_txn()?;
        let out = self.list_fuzzy_indexes_in(&rtxn)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn list_fuzzy_indexes_in(&self, rtxn: &RoTxn<'_>) -> Result<Vec<String>> {
        self.fuzzy_defs(rtxn)
    }

    /// Maintains full-text indexes on an `old -> new` change.
    fn apply_text_entries(
        &self,
        wtxn: &mut RwTxn<'_>,
        old: Option<&Value>,
        new: Option<&Value>,
        id: &str,
    ) -> Result<()> {
        for field in self.text_defs(&*wtxn)? {
            let old_text = old
                .and_then(|d| index::field_value(d, &field))
                .and_then(search::field_text);
            let new_text = new
                .and_then(|d| index::field_value(d, &field))
                .and_then(search::field_text);
            if old_text == new_text {
                continue;
            }
            let postings = self.text_postings_in(wtxn, &field)?;
            let lens = self.text_len_db_in(wtxn, &field)?;
            let total_key = search::text_total_key(&self.name, &field);
            let mut total = self
                .meta
                .get(&*wtxn, total_key.as_bytes())?
                .and_then(search::read_u64)
                .unwrap_or(0);

            let old_tokens = old_text.as_ref().map(|t| search::tokenize(t));
            let new_tokens = new_text.as_ref().map(|t| search::tokenize(t));
            let old_len = old_tokens.as_ref().map(|t| t.len() as u64).unwrap_or(0);
            let new_len = new_tokens.as_ref().map(|t| t.len() as u64).unwrap_or(0);

            if let Some(tokens) = &old_tokens {
                for term in search::term_freqs(tokens).into_keys() {
                    postings.delete(&mut *wtxn, search::postings_key(&term, id).as_slice())?;
                }
            }
            if let Some(tokens) = &new_tokens {
                for (term, tf) in search::term_freqs(tokens) {
                    postings.put(
                        &mut *wtxn,
                        search::postings_key(&term, id).as_slice(),
                        &tf.to_le_bytes(),
                    )?;
                }
                lens.put(&mut *wtxn, id.as_bytes(), &(new_len as u32).to_le_bytes())?;
            } else {
                lens.delete(&mut *wtxn, id.as_bytes())?;
            }

            total = total.saturating_sub(old_len) + new_len;
            self.meta
                .put(&mut *wtxn, total_key.as_bytes(), &total.to_le_bytes())?;
        }
        Ok(())
    }

    /// Maintains trigram indexes on an `old -> new` change.
    fn apply_fuzzy_entries(
        &self,
        wtxn: &mut RwTxn<'_>,
        old: Option<&Value>,
        new: Option<&Value>,
        id: &str,
    ) -> Result<()> {
        for field in self.fuzzy_defs(&*wtxn)? {
            let old_text = old
                .and_then(|d| index::field_value(d, &field))
                .and_then(search::field_text)
                .map(|t| search::normalize(&t));
            let new_text = new
                .and_then(|d| index::field_value(d, &field))
                .and_then(search::field_text)
                .map(|t| search::normalize(&t));
            if old_text == new_text {
                continue;
            }
            let db = self.fuzzy_db_in(wtxn, &field)?;
            if let Some(text) = &old_text {
                for gram in search::trigrams(text) {
                    db.delete(&mut *wtxn, search::postings_key(&gram, id).as_slice())?;
                }
            }
            if let Some(text) = &new_text {
                for gram in search::trigrams(text) {
                    db.put(&mut *wtxn, search::postings_key(&gram, id).as_slice(), &[])?;
                }
            }
        }
        Ok(())
    }

    /// Full-text search (BM25). Results include an extra `_score` field.
    pub fn search(
        &self,
        field: &str,
        query: &str,
        filter: &Value,
        skip: usize,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        let rtxn = self.read_txn()?;
        let out = self.search_in(&rtxn, field, query, filter, skip, limit)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn search_in(
        &self,
        rtxn: &RoTxn<'_>,
        field: &str,
        query: &str,
        filter: &Value,
        skip: usize,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        crate::query::validate::validate_filter(filter)?;
        if self
            .meta
            .get(rtxn, search::text_def_key(&self.name, field).as_bytes())?
            .is_none()
        {
            return Err(Error::Query(format!("no text index on field `{field}`")));
        }
        let Some(postings) = self.text_postings(rtxn, field)? else {
            return Err(Error::Query(format!("no text index on field `{field}`")));
        };
        let Some(lens) = self.text_len_db(rtxn, field)? else {
            return Err(Error::Query(format!("no text index on field `{field}`")));
        };

        let n_docs = lens.stat(rtxn)?.entries as f64;
        if n_docs == 0.0 {
            return Ok(Vec::new());
        }
        let total = self
            .meta
            .get(rtxn, search::text_total_key(&self.name, field).as_bytes())?
            .and_then(search::read_u64)
            .unwrap_or(0) as f64;
        let avgdl = total / n_docs;

        let phrases = search::parse_phrases(query);
        let mut terms = search::tokenize(query);
        terms.sort();
        terms.dedup();
        if terms.is_empty() {
            return Ok(Vec::new());
        }

        let limit = limit.unwrap_or(10).min(1000);
        let mut scores: HashMap<String, f64> = HashMap::new();
        for term in &terms {
            let prefix = search::posting_prefix(term);
            let mut hits: Vec<(String, f64)> = Vec::new();
            for item in postings.prefix_iter(rtxn, prefix.as_slice())? {
                let (key, tf) = item?;
                if let Some(id) = search::parse_posting(key) {
                    hits.push((id.to_string(), search::read_u32(tf).unwrap_or(1) as f64));
                }
            }
            let idf = search::idf(n_docs, hits.len() as f64);
            for (id, tf) in hits {
                let dl = lens
                    .get(rtxn, id.as_bytes())?
                    .and_then(search::read_u32)
                    .unwrap_or(0) as f64;
                *scores.entry(id).or_insert(0.0) += search::bm25_term(tf, dl, avgdl, idf);
            }
        }

        let mut ranked: Vec<(String, f64)> = scores.into_iter().collect();
        ranked.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        let mut out = Vec::new();
        let mut skipped = 0usize;
        for (id, score) in ranked {
            if out.len() >= limit {
                break;
            }
            let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                continue;
            };
            let mut doc: Value = parse_json(bytes)?;
            if !matches(&doc, filter) {
                continue;
            }
            if !phrases.is_empty() {
                let text = index::field_value(&doc, field)
                    .and_then(search::field_text)
                    .unwrap_or_default();
                let doc_tokens = search::tokenize(&text);
                if !phrases
                    .iter()
                    .all(|p| search::contains_phrase(&doc_tokens, p))
                {
                    continue;
                }
            }
            if skipped < skip {
                skipped += 1;
                continue;
            }
            if let Some(obj) = doc.as_object_mut() {
                obj.insert("_score".to_string(), serde_json::json!(score));
            }
            out.push(doc);
        }
        Ok(out)
    }

    /// Approximate search (trigrams + Levenshtein). Results include `_distance`.
    pub fn fuzzy_search(
        &self,
        field: &str,
        query: &str,
        max_distance: usize,
        filter: &Value,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        let rtxn = self.read_txn()?;
        let out = self.fuzzy_search_in(&rtxn, field, query, max_distance, filter, limit)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn fuzzy_search_in(
        &self,
        rtxn: &RoTxn<'_>,
        field: &str,
        query: &str,
        max_distance: usize,
        filter: &Value,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        crate::query::validate::validate_filter(filter)?;
        if self
            .meta
            .get(rtxn, search::fuzzy_def_key(&self.name, field).as_bytes())?
            .is_none()
        {
            return Err(Error::Query(format!("no fuzzy index on field `{field}`")));
        }
        let Some(db) = self.fuzzy_db(rtxn, field)? else {
            return Err(Error::Query(format!("no fuzzy index on field `{field}`")));
        };
        let query = search::normalize(query);
        if query.is_empty() {
            return Ok(Vec::new());
        }

        // Candidates: documents sharing at least one trigram.
        let mut counts: HashMap<String, u32> = HashMap::new();
        for gram in search::trigrams(&query) {
            let prefix = search::posting_prefix(&gram);
            for item in db.prefix_iter(rtxn, prefix.as_slice())? {
                let (key, _) = item?;
                if let Some(id) = search::parse_posting(key) {
                    *counts.entry(id.to_string()).or_insert(0) += 1;
                }
            }
        }
        let mut candidates: Vec<(String, u32)> = counts.into_iter().collect();
        candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        candidates.truncate(5000);

        let mut scored: Vec<(usize, String, Value)> = Vec::new();
        for (id, _) in candidates {
            let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                continue;
            };
            let doc: Value = parse_json(bytes)?;
            let Some(text) = index::field_value(&doc, field).and_then(search::field_text) else {
                continue;
            };
            let Some(distance) = search::fuzzy_distance(&text, &query, max_distance) else {
                continue;
            };
            if !matches(&doc, filter) {
                continue;
            }
            scored.push((distance, id, doc));
        }

        scored.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        let limit = limit.unwrap_or(10).min(1000);
        Ok(scored
            .into_iter()
            .take(limit)
            .map(|(distance, _, mut doc)| {
                if let Some(obj) = doc.as_object_mut() {
                    obj.insert("_distance".to_string(), serde_json::json!(distance));
                }
                doc
            })
            .collect())
    }

    // ----------------------------- write -----------------------------

    /// Inserts a document in its own transaction (handy in tests / without the daemon).
    pub fn insert_one(&self, doc: Value) -> Result<String> {
        let mut wtxn = self.write_txn()?;
        let id = self.insert_one_txn(&mut wtxn, doc)?;
        wtxn.commit()?;
        Ok(id)
    }

    /// Inserts a document in the given transaction. Assigns `_id` (UUID v4) if missing.
    pub fn insert_one_txn(&self, wtxn: &mut RwTxn<'_>, mut doc: Value) -> Result<String> {
        self.ensure_standard()?;
        let ttl_at = ttl::take_insert_ttl(&mut doc, self.ttl_default_seconds)?;
        date::normalize_document(&mut doc)?;
        numeric::normalize_document(&mut doc)?;
        bson_types::normalize_document(&mut doc)?;
        let obj = doc
            .as_object_mut()
            .ok_or_else(|| Error::Query("document must be an object".into()))?;

        let id = match obj.get("_id") {
            Some(Value::String(s)) => s.clone(),
            Some(_) => return Err(Error::InvalidId),
            None => {
                let id = uuid::Uuid::new_v4().to_string();
                obj.insert("_id".to_string(), Value::String(id.clone()));
                id
            }
        };

        if self.db.get(&*wtxn, id.as_bytes())?.is_some() {
            return Err(Error::DuplicateKey(id));
        }
        self.validate_unique(&*wtxn, &doc, &id)?;

        let bytes = serde_json::to_vec(&doc)?;
        if bytes.len() > MAX_DOCUMENT_BYTES {
            return Err(Error::DocumentTooLarge(bytes.len(), MAX_DOCUMENT_BYTES));
        }
        self.db.put(&mut *wtxn, id.as_bytes(), &bytes)?;
        self.apply_index_entries(wtxn, None, Some(&doc), &id)?;
        self.apply_text_entries(wtxn, None, Some(&doc), &id)?;
        self.apply_fuzzy_entries(wtxn, None, Some(&doc), &id)?;
        self.set_ttl(wtxn, &id, ttl_at)?;
        self.record_history(wtxn, &id, "insert", Some(doc))?;
        Ok(id)
    }

    /// Update with optional upsert; returns the full operation result.
    pub fn update_opts_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        filter: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
    ) -> Result<UpdateOutcome> {
        self.update_opts_txn_filtered(wtxn, filter, update, multi, upsert, &[])
    }

    pub fn update_opts_txn_filtered(
        &self,
        wtxn: &mut RwTxn<'_>,
        filter: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
        array_filters: &[Value],
    ) -> Result<UpdateOutcome> {
        self.ensure_standard()?;
        let keys = self.matching_keys(&*wtxn, filter, multi)?;
        if keys.is_empty() {
            if !upsert {
                return Ok(UpdateOutcome {
                    matched: 0,
                    modified: 0,
                    upserted_id: None,
                });
            }
            let mut doc = seed_from_filter(filter);
            let filter_id = doc.get("_id").cloned();
            apply_upsert_with_options(&mut doc, update, filter, array_filters)?;
            // `_id` from the filter is immutable; do not let an update lose it.
            if let Some(id) = filter_id {
                doc["_id"] = id;
            }
            let id = self.insert_one_txn(wtxn, doc)?;
            return Ok(UpdateOutcome {
                matched: 0,
                modified: 0,
                upserted_id: Some(id),
            });
        }

        let matched = keys.len();
        let mut modified = 0usize;
        for key in keys {
            let raw = self
                .db
                .get(&*wtxn, key.as_slice())?
                .ok_or_else(|| Error::Query("document disappeared".into()))?
                .to_vec();
            let old: Value = parse_json(&raw)?;
            self.write_update_one(wtxn, old, filter, update, array_filters)?;
            modified += 1;
        }
        Ok(UpdateOutcome {
            matched,
            modified,
            upserted_id: None,
        })
    }

    fn write_update_one(
        &self,
        wtxn: &mut RwTxn<'_>,
        old: Value,
        filter: &Value,
        update: &Value,
        array_filters: &[Value],
    ) -> Result<Value> {
        let id = old
            .get("_id")
            .and_then(Value::as_str)
            .ok_or(Error::InvalidId)?
            .to_string();
        let old_ttl = self.ttl_at(&*wtxn, &id)?;
        let mut new = old.clone();
        apply_with_options(&mut new, update, filter, array_filters)?;
        let ttl_at = match ttl::take_update_ttl(&mut new)? {
            TtlUpdate::Preserve => old_ttl,
            TtlUpdate::Clear => None,
            TtlUpdate::Set(at) => Some(at),
        };
        date::normalize_document(&mut new)?;
        numeric::normalize_document(&mut new)?;
        bson_types::normalize_document(&mut new)?;

        self.validate_unique(&*wtxn, &new, &id)?;
        let bytes = serde_json::to_vec(&new)?;
        if bytes.len() > MAX_DOCUMENT_BYTES {
            return Err(Error::DocumentTooLarge(bytes.len(), MAX_DOCUMENT_BYTES));
        }
        self.db.put(&mut *wtxn, id.as_bytes(), &bytes)?;
        self.apply_index_entries(wtxn, Some(&old), Some(&new), &id)?;
        self.apply_text_entries(wtxn, Some(&old), Some(&new), &id)?;
        self.apply_fuzzy_entries(wtxn, Some(&old), Some(&new), &id)?;
        self.set_ttl(wtxn, &id, ttl_at)?;
        self.record_history(wtxn, &id, "update", Some(new.clone()))?;
        Ok(new)
    }

    /// Legacy signature: returns only the number of modified documents.
    pub fn update_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        filter: &Value,
        update: &Value,
        multi: bool,
    ) -> Result<usize> {
        Ok(self
            .update_opts_txn(wtxn, filter, update, multi, false)?
            .modified)
    }

    pub fn update_opts(
        &self,
        filter: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
    ) -> Result<UpdateOutcome> {
        self.update_opts_filtered(filter, update, multi, upsert, &[])
    }

    pub fn update_opts_filtered(
        &self,
        filter: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
        array_filters: &[Value],
    ) -> Result<UpdateOutcome> {
        let mut wtxn = self.write_txn()?;
        let out =
            self.update_opts_txn_filtered(&mut wtxn, filter, update, multi, upsert, array_filters)?;
        wtxn.commit()?;
        Ok(out)
    }

    pub fn update(&self, filter: &Value, update: &Value, multi: bool) -> Result<usize> {
        Ok(self.update_opts(filter, update, multi, false)?.modified)
    }

    /// Deletes matching documents. Returns the number deleted.
    /// Atomically selects one document, applies an update or removes it, and
    /// returns the pre-image or post-image in the same write transaction.
    pub fn find_and_modify_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        opts: &FindAndModifyOptions<'_>,
    ) -> Result<FindAndModifyOutcome> {
        self.ensure_standard()?;
        validate_find_and_modify(opts)?;
        let query = FindQuery {
            filter: opts.filter.clone(),
            projection: None,
            sort: opts.sort.to_vec(),
            skip: 0,
            limit: Some(1),
        };
        let selected = self.find_in(&*wtxn, &query)?.into_iter().next();

        if opts.remove {
            let Some(old) = selected else {
                return Ok(empty_find_and_modify());
            };
            let id = old
                .get("_id")
                .and_then(Value::as_str)
                .ok_or(Error::InvalidId)?
                .to_string();
            let removed = self.delete_txn(wtxn, &serde_json::json!({"_id": id}), false)?;
            return Ok(FindAndModifyOutcome {
                value: Some(apply_projection(old, opts.projection)),
                matched: removed,
                modified: removed,
                upserted_id: None,
            });
        }

        let update = opts
            .update
            .ok_or_else(|| Error::Query("find_and_modify requires `update` or `remove`".into()))?;
        if let Some(old) = selected {
            let new =
                self.write_update_one(wtxn, old.clone(), opts.filter, update, opts.array_filters)?;
            return Ok(FindAndModifyOutcome {
                value: Some(apply_projection(
                    if opts.new { new } else { old },
                    opts.projection,
                )),
                matched: 1,
                modified: 1,
                upserted_id: None,
            });
        }

        if !opts.upsert {
            return Ok(empty_find_and_modify());
        }
        let mut doc = seed_from_filter(opts.filter);
        let filter_id = doc.get("_id").cloned();
        apply_upsert_with_options(&mut doc, update, opts.filter, opts.array_filters)?;
        if let Some(id) = filter_id {
            doc["_id"] = id;
        }
        let id = self.insert_one_txn(wtxn, doc)?;
        let value = if opts.new {
            let raw = self
                .db
                .get(&*wtxn, id.as_bytes())?
                .ok_or_else(|| Error::Query("upserted document disappeared".into()))?;
            Some(apply_projection(parse_json(raw)?, opts.projection))
        } else {
            None
        };
        Ok(FindAndModifyOutcome {
            value,
            matched: 0,
            modified: 0,
            upserted_id: Some(id),
        })
    }

    pub fn find_and_modify(&self, opts: &FindAndModifyOptions<'_>) -> Result<FindAndModifyOutcome> {
        let mut wtxn = self.write_txn()?;
        let out = self.find_and_modify_txn(&mut wtxn, opts)?;
        wtxn.commit()?;
        Ok(out)
    }

    /// Vault variant. Vault filters must identify exactly one document.
    #[allow(clippy::too_many_arguments)]
    pub fn find_and_modify_vault_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        opts: &FindAndModifyOptions<'_>,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<FindAndModifyOutcome> {
        self.ensure_vault()?;
        validate_find_and_modify(opts)?;
        vault::validate_projection(opts.projection)?;
        vault::validate_sort(opts.sort)?;
        let query = FindQuery {
            filter: opts.filter.clone(),
            projection: None,
            sort: opts.sort.to_vec(),
            skip: 0,
            limit: Some(1),
        };
        let selected = self
            .find_vault_in(&*wtxn, &query, key, tenant, kid)?
            .into_iter()
            .next();
        if opts.remove {
            let Some(old) = selected else {
                return Ok(empty_find_and_modify());
            };
            let id = old
                .get("_id")
                .and_then(Value::as_str)
                .ok_or(Error::InvalidId)?
                .to_string();
            let filter = serde_json::json!({"_id": id});
            let removed = self.delete_vault_txn(wtxn, &filter, false, key, tenant, kid)?;
            return Ok(FindAndModifyOutcome {
                value: Some(old),
                matched: removed,
                modified: removed,
                upserted_id: None,
            });
        }
        let update = opts
            .update
            .ok_or_else(|| Error::Query("find_and_modify requires `update` or `remove`".into()))?;
        if let Some(old) = selected {
            let new = self.write_vault_update_one(
                wtxn,
                old.clone(),
                opts.filter,
                update,
                opts.array_filters,
                key,
                tenant,
                kid,
            )?;
            return Ok(FindAndModifyOutcome {
                value: Some(if opts.new { new } else { old }),
                matched: 1,
                modified: 1,
                upserted_id: None,
            });
        }
        if !opts.upsert {
            return Ok(empty_find_and_modify());
        }
        let id = vault::exact_id_filter(opts.filter, tenant)?;
        let mut doc = seed_from_filter(opts.filter);
        doc["_id"] = Value::String(id.clone());
        apply_upsert_with_options(&mut doc, update, opts.filter, opts.array_filters)?;
        let id = self.insert_vault_txn(wtxn, doc, key, tenant, kid)?;
        let value = if opts.new {
            self.find_one_vault_in(&*wtxn, &serde_json::json!({"_id": id}), key, tenant, kid)?
        } else {
            None
        };
        Ok(FindAndModifyOutcome {
            value,
            matched: 0,
            modified: 0,
            upserted_id: Some(id),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn find_and_modify_vault(
        &self,
        opts: &FindAndModifyOptions<'_>,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<FindAndModifyOutcome> {
        let mut wtxn = self.write_txn()?;
        let out = self.find_and_modify_vault_txn(&mut wtxn, opts, key, tenant, kid)?;
        wtxn.commit()?;
        Ok(out)
    }

    pub fn delete_txn(&self, wtxn: &mut RwTxn<'_>, filter: &Value, multi: bool) -> Result<usize> {
        self.ensure_standard()?;
        let keys = self.matching_keys(&*wtxn, filter, multi)?;
        for key in &keys {
            let id = std::str::from_utf8(key)
                .map_err(|e| Error::Query(e.to_string()))?
                .to_string();
            let raw = self
                .db
                .get(&*wtxn, key.as_slice())?
                .ok_or_else(|| Error::Query("document disappeared".into()))?
                .to_vec();
            let old: Value = parse_json(&raw)?;
            self.db.delete(&mut *wtxn, key.as_slice())?;
            self.apply_index_entries(wtxn, Some(&old), None, &id)?;
            self.apply_text_entries(wtxn, Some(&old), None, &id)?;
            self.apply_fuzzy_entries(wtxn, Some(&old), None, &id)?;
            self.set_ttl(wtxn, &id, None)?;
            self.record_history(wtxn, &id, "delete", None)?;
        }
        Ok(keys.len())
    }

    pub fn delete(&self, filter: &Value, multi: bool) -> Result<usize> {
        let mut wtxn = self.write_txn()?;
        let n = self.delete_txn(&mut wtxn, filter, multi)?;
        wtxn.commit()?;
        Ok(n)
    }

    // ----------------------------- vault -----------------------------

    fn vault_stored(id: &str, tenant: &str, envelope: Value) -> Result<Value> {
        let enc = envelope.get("_enc").cloned().ok_or_else(|| Error::Vault {
            code: "encryption_failed",
            message: "encryption did not produce an envelope".into(),
        })?;
        Ok(serde_json::json!({
            "_id": id,
            "tenant_id": tenant,
            "_enc": enc,
        }))
    }

    fn decrypt_stored(
        &self,
        stored: &Value,
        key: &VaultKey,
        tenant: &str,
        id: &str,
        kid: &str,
    ) -> Result<Value> {
        if vault::stored_tenant(stored) != Some(tenant) {
            return Err(Error::Vault {
                code: "tenant_mismatch",
                message: "document belongs to a different tenant".into(),
            });
        }
        let doc = vault::decrypt_document(stored, key, &self.name, tenant, id, kid)?;
        let got_id = doc.get("_id").and_then(Value::as_str);
        let got_tenant = doc.get(vault::TENANT_FIELD).and_then(Value::as_str);
        if got_id != Some(id) || got_tenant != Some(tenant) {
            return Err(Error::Vault {
                code: "decryption_failed",
                message: "decrypted document identity does not match its envelope".into(),
            });
        }
        Ok(doc)
    }

    fn check_vault_size(stored: &Value) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec(stored)?;
        if bytes.len() > MAX_DOCUMENT_BYTES {
            return Err(Error::DocumentTooLarge(bytes.len(), MAX_DOCUMENT_BYTES));
        }
        Ok(bytes)
    }

    /// Inserts a document into a vault collection in the given transaction.
    pub fn insert_vault_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        mut doc: Value,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<String> {
        self.ensure_vault()?;
        vault::validate_context(tenant, kid)?;
        let obj = doc
            .as_object_mut()
            .ok_or_else(|| Error::Query("document must be an object".into()))?;

        let id = match obj.get("_id") {
            Some(Value::String(s)) => s.clone(),
            Some(_) => return Err(Error::InvalidId),
            None => {
                let id = uuid::Uuid::new_v4().to_string();
                obj.insert("_id".to_string(), Value::String(id.clone()));
                id
            }
        };
        match obj.get(vault::TENANT_FIELD) {
            Some(Value::String(got)) if got == tenant => {}
            Some(Value::String(_)) => {
                return Err(Error::Vault {
                    code: "tenant_mismatch",
                    message: "document `tenant_id` does not match X-NonoDB-Tenant".into(),
                })
            }
            Some(_) => {
                return Err(Error::Vault {
                    code: "invalid_vault_document",
                    message: "`tenant_id` must be a string".into(),
                })
            }
            None => {
                obj.insert(
                    vault::TENANT_FIELD.to_string(),
                    Value::String(tenant.to_string()),
                );
            }
        }

        if self.db.get(&*wtxn, id.as_bytes())?.is_some() {
            return Err(Error::DuplicateKey(id));
        }
        let ttl_at = ttl::take_insert_ttl(&mut doc, self.ttl_default_seconds)?;
        date::normalize_document(&mut doc)?;
        numeric::normalize_document(&mut doc)?;
        bson_types::normalize_document(&mut doc)?;
        let envelope = vault::encrypt_document(&doc, key, &self.name, tenant, &id, kid)?;
        let stored = Self::vault_stored(&id, tenant, envelope)?;
        let bytes = Self::check_vault_size(&stored)?;
        self.db.put(&mut *wtxn, id.as_bytes(), &bytes)?;
        self.set_ttl(wtxn, &id, ttl_at)?;
        self.record_history(wtxn, &id, "insert", Some(stored))?;
        Ok(id)
    }

    /// Inserts many documents into a vault collection. Not all-or-nothing.
    pub fn insert_many_vault_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        docs: &[Value],
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<Vec<String>> {
        let mut ids = Vec::with_capacity(docs.len());
        for doc in docs {
            ids.push(self.insert_vault_txn(wtxn, doc.clone(), key, tenant, kid)?);
        }
        Ok(ids)
    }

    /// Updates exactly one vault document by `_id`. `multi` is not supported.
    #[allow(clippy::too_many_arguments)]
    pub fn update_vault_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        filter: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<UpdateOutcome> {
        self.update_vault_txn_filtered(wtxn, filter, update, multi, upsert, key, tenant, kid, &[])
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_vault_txn_filtered(
        &self,
        wtxn: &mut RwTxn<'_>,
        filter: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
        array_filters: &[Value],
    ) -> Result<UpdateOutcome> {
        self.ensure_vault()?;
        if multi {
            return Err(Error::Vault {
                code: "invalid_vault_filter",
                message: "multi update is not supported for vault collections".into(),
            });
        }
        let id = vault::exact_id_filter(filter, tenant)?;
        let Some(raw) = self.db.get(&*wtxn, id.as_bytes())? else {
            if !upsert {
                return Ok(UpdateOutcome {
                    matched: 0,
                    modified: 0,
                    upserted_id: None,
                });
            }
            let mut doc = seed_from_filter(filter);
            doc["_id"] = Value::String(id.clone());
            apply_upsert_with_options(&mut doc, update, filter, array_filters)?;
            let id = self.insert_vault_txn(wtxn, doc, key, tenant, kid)?;
            return Ok(UpdateOutcome {
                matched: 0,
                modified: 0,
                upserted_id: Some(id),
            });
        };
        let old_stored: Value = parse_json(raw)?;
        let old = self.decrypt_stored(&old_stored, key, tenant, &id, kid)?;
        self.write_vault_update_one(wtxn, old, filter, update, array_filters, key, tenant, kid)?;
        Ok(UpdateOutcome {
            matched: 1,
            modified: 1,
            upserted_id: None,
        })
    }

    /// Deletes exactly one vault document by `_id`.
    #[allow(clippy::too_many_arguments)]
    fn write_vault_update_one(
        &self,
        wtxn: &mut RwTxn<'_>,
        old: Value,
        filter: &Value,
        update: &Value,
        array_filters: &[Value],
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<Value> {
        let id = old
            .get("_id")
            .and_then(Value::as_str)
            .ok_or(Error::InvalidId)?
            .to_string();
        let old_ttl = self.ttl_at(&*wtxn, &id)?;
        let mut new = old.clone();
        apply_with_options(&mut new, update, filter, array_filters)?;
        let ttl_at = match ttl::take_update_ttl(&mut new)? {
            TtlUpdate::Preserve => old_ttl,
            TtlUpdate::Clear => None,
            TtlUpdate::Set(at) => Some(at),
        };
        date::normalize_document(&mut new)?;
        numeric::normalize_document(&mut new)?;
        bson_types::normalize_document(&mut new)?;
        if new.get("_id").and_then(Value::as_str) != Some(id.as_str()) {
            return Err(Error::InvalidId);
        }
        if new.get(vault::TENANT_FIELD).and_then(Value::as_str) != Some(tenant) {
            return Err(Error::Vault {
                code: "tenant_mismatch",
                message: "update cannot change `tenant_id`".into(),
            });
        }
        let envelope = vault::encrypt_document(&new, key, &self.name, tenant, &id, kid)?;
        let stored = Self::vault_stored(&id, tenant, envelope)?;
        let bytes = Self::check_vault_size(&stored)?;
        self.db.put(&mut *wtxn, id.as_bytes(), &bytes)?;
        self.set_ttl(wtxn, &id, ttl_at)?;
        self.record_history(wtxn, &id, "update", Some(stored))?;
        Ok(new)
    }

    pub fn delete_vault_txn(
        &self,
        wtxn: &mut RwTxn<'_>,
        filter: &Value,
        multi: bool,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<usize> {
        self.ensure_vault()?;
        if multi {
            return Err(Error::Vault {
                code: "invalid_vault_filter",
                message: "multi delete is not supported for vault collections".into(),
            });
        }
        let id = vault::exact_id_filter(filter, tenant)?;
        let Some(raw) = self.db.get(&*wtxn, id.as_bytes())? else {
            return Ok(0);
        };
        let stored: Value = parse_json(raw)?;
        // Decrypt before deleting: this enforces the tenant/key and catches corruption.
        self.decrypt_stored(&stored, key, tenant, &id, kid)?;
        self.db.delete(&mut *wtxn, id.as_bytes())?;
        self.set_ttl(wtxn, &id, None)?;
        self.record_history(wtxn, &id, "delete", None)?;
        Ok(1)
    }

    /// Deletes an expired document without decrypting vault payloads.
    ///
    /// The expected expiry must still match the TTL entry; this protects a
    /// concurrent update that refreshed the TTL after the sweeper read it.
    pub fn expire_txn(&self, wtxn: &mut RwTxn<'_>, id: &str, expected_ttl_at: u64) -> Result<bool> {
        let Some(current) = self.ttl_at(&*wtxn, id)? else {
            return Ok(false);
        };
        if current != expected_ttl_at {
            return Ok(false);
        }
        let Some(raw) = self.db.get(&*wtxn, id.as_bytes())? else {
            self.set_ttl(wtxn, id, None)?;
            return Ok(false);
        };
        let old: Value = parse_json(raw)?;
        self.db.delete(&mut *wtxn, id.as_bytes())?;
        if !self.profile.is_vault() {
            self.apply_index_entries(wtxn, Some(&old), None, id)?;
            self.apply_text_entries(wtxn, Some(&old), None, id)?;
            self.apply_fuzzy_entries(wtxn, Some(&old), None, id)?;
        }
        self.set_ttl(wtxn, id, None)?;
        if self.ttl_purge_history {
            history::purge(&self.history, wtxn, &self.name, id)?;
        } else {
            self.record_history(wtxn, id, "delete", None)?;
        }
        Ok(true)
    }

    /// Reads vault documents. Only `_id`/`tenant_id` filters and `_id` sorting are allowed.
    pub fn find_vault_in(
        &self,
        rtxn: &RoTxn<'_>,
        query: &FindQuery,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<Vec<Value>> {
        self.ensure_vault()?;
        vault::validate_projection(query.projection.as_ref())?;
        vault::validate_sort(&query.sort)?;
        let filter = vault::read_filter(&query.filter, tenant)?;
        let stored_query = FindQuery {
            filter,
            projection: None,
            sort: query.sort.clone(),
            skip: query.skip,
            limit: query.limit,
        };
        let stored = self.find_in(rtxn, &stored_query)?;
        let mut out = Vec::with_capacity(stored.len());
        for item in stored {
            let id = item
                .get("_id")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Vault {
                    code: "decryption_failed",
                    message: "stored vault document has no `_id`".into(),
                })?;
            out.push(self.decrypt_stored(&item, key, tenant, id, kid)?);
        }
        Ok(out)
    }

    pub fn find_one_vault_in(
        &self,
        rtxn: &RoTxn<'_>,
        filter: &Value,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<Option<Value>> {
        let query = FindQuery {
            filter: filter.clone(),
            projection: None,
            sort: Vec::new(),
            skip: 0,
            limit: Some(1),
        };
        Ok(self.find_vault_in(rtxn, &query, key, tenant, kid)?.pop())
    }

    /// Counts vault documents without decrypting them.
    pub fn count_vault_in(&self, rtxn: &RoTxn<'_>, filter: &Value, tenant: &str) -> Result<usize> {
        self.ensure_vault()?;
        let filter = vault::read_filter(filter, tenant)?;
        self.count_in(rtxn, &filter)
    }

    /// History for a vault document, with snapshots decrypted on read.
    #[allow(clippy::too_many_arguments)]
    pub fn history_vault_in(
        &self,
        rtxn: &RoTxn<'_>,
        id: &str,
        limit: Option<usize>,
        include_docs: bool,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<Vec<HistoryEntry>> {
        self.ensure_vault()?;
        let mut entries = self.history_in(rtxn, id, limit, include_docs)?;
        if include_docs {
            for entry in &mut entries {
                if let Some(stored) = entry.doc.take() {
                    entry.doc = Some(self.decrypt_stored(&stored, key, tenant, id, kid)?);
                }
            }
        }
        Ok(entries)
    }

    pub fn get_version_vault_in(
        &self,
        rtxn: &RoTxn<'_>,
        id: &str,
        rev: u64,
        key: &VaultKey,
        tenant: &str,
        kid: &str,
    ) -> Result<Option<HistoryEntry>> {
        self.ensure_vault()?;
        let mut entry = self.get_version_in(rtxn, id, rev)?;
        if let Some(entry) = &mut entry {
            if let Some(stored) = entry.doc.take() {
                entry.doc = Some(self.decrypt_stored(&stored, key, tenant, id, kid)?);
            }
        }
        Ok(entry)
    }

    fn record_history(
        &self,
        wtxn: &mut RwTxn<'_>,
        id: &str,
        op: &str,
        doc: Option<Value>,
    ) -> Result<()> {
        if !self.history_config.active() {
            return Ok(());
        }
        history::append(&self.history, &self.meta, wtxn, &self.name, id, op, doc)?;
        Ok(())
    }

    /// Reads document revisions, newest first.
    pub fn history(
        &self,
        id: &str,
        limit: Option<usize>,
        include_docs: bool,
    ) -> Result<Vec<HistoryEntry>> {
        let rtxn = self.read_txn()?;
        let out = self.history_in(&rtxn, id, limit, include_docs)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn history_in(
        &self,
        rtxn: &RoTxn<'_>,
        id: &str,
        limit: Option<usize>,
        include_docs: bool,
    ) -> Result<Vec<HistoryEntry>> {
        history::list(&self.history, rtxn, &self.name, id, limit, include_docs)
    }

    /// Reads one exact global revision.
    pub fn get_version(&self, id: &str, rev: u64) -> Result<Option<HistoryEntry>> {
        let rtxn = self.read_txn()?;
        let out = self.get_version_in(&rtxn, id, rev)?;
        rtxn.commit()?;
        Ok(out)
    }

    pub fn get_version_in(
        &self,
        rtxn: &RoTxn<'_>,
        id: &str,
        rev: u64,
    ) -> Result<Option<HistoryEntry>> {
        history::get(&self.history, rtxn, &self.name, id, rev)
    }

    /// Collects keys (`_id`) matching the filter.
    ///
    /// Uses the same fast paths as `find`: exact `_id`,
    /// secondary index, and only then a full scan.
    fn matching_keys(&self, txn: &RoTxn<'_>, filter: &Value, multi: bool) -> Result<Vec<Vec<u8>>> {
        crate::query::validate::validate_filter(filter)?;
        let mut keys = Vec::new();
        let raw = raw_prefilter(filter);

        if let Some(id) = direct_id(filter) {
            if self.db.get(txn, id.as_bytes())?.is_some() {
                keys.push(id.as_bytes().to_vec());
            }
            return Ok(keys);
        }

        if let Some(ids) = self.index_candidates(txn, filter, None)? {
            for id in ids {
                let Some(bytes) = self.db.get(txn, id.as_bytes())? else {
                    continue;
                };
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, filter) {
                    keys.push(id.into_bytes());
                    if !multi {
                        break;
                    }
                }
            }
            return Ok(keys);
        }

        if let Some((lower, upper)) = id_bounds(filter) {
            let range = (as_bound(&lower), as_bound(&upper));
            for item in self.db.range(txn, &range)? {
                let (key, bytes) = item?;
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, filter) {
                    keys.push(key.to_vec());
                    if !multi {
                        break;
                    }
                }
            }
            return Ok(keys);
        }

        for item in self.db.iter(txn)? {
            let (key, bytes) = item?;
            let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                continue;
            };
            if matches(&doc, filter) {
                keys.push(key.to_vec());
                if !multi {
                    break;
                }
            }
        }
        Ok(keys)
    }

    // ----------------------------- read ------------------------------

    /// Runs a read-only aggregation pipeline over this collection.
    ///
    /// A safe prefix (`$match`, `$sort`, `$skip`, `$limit`, simple `$project`)
    /// is pushed into the normal find path so indexes, `_id` ranges, covering
    /// projections, and query-level TopK can be used. A lone `$count` becomes a
    /// `count` call instead of materializing documents. Intermediate results
    /// are capped by [`aggregate::MAX_AGGREGATE_DOCS`].
    pub fn aggregate(&self, pipeline: &[Value]) -> Result<Vec<Value>> {
        let rtxn = self.read_txn()?;
        let out = self.aggregate_in(&rtxn, pipeline);
        rtxn.commit()?;
        out
    }

    pub fn aggregate_in(&self, rtxn: &RoTxn<'_>, pipeline: &[Value]) -> Result<Vec<Value>> {
        for stage in pipeline {
            if let Some(filter) = stage.as_object().and_then(|o| o.get("$match")) {
                crate::query::validate::validate_filter(filter)?;
            }
        }
        let plan = aggregate::pushdown(pipeline)?;

        if let Some(field) = plan.count {
            let count = self.count_in(rtxn, &plan.filter)?;
            if count == 0 {
                return Ok(Vec::new());
            }
            return Ok(vec![Value::Object(Map::from_iter([(
                field,
                Value::from(count as u64),
            )]))]);
        }

        if let Some(field) = aggregate::terminal_count_skippable(&pipeline[plan.next..])? {
            let count = self.count_in(rtxn, &plan.filter)?;
            let after_skip = count.saturating_sub(plan.skip);
            let count = plan.limit.map_or(after_skip, |limit| after_skip.min(limit));
            if count == 0 {
                return Ok(Vec::new());
            }
            return Ok(vec![Value::Object(Map::from_iter([(
                field,
                Value::from(count as u64),
            )]))]);
        }

        let max_docs = aggregate::max_docs();
        let remaining = &pipeline[plan.next..];

        // A pipeline whose remaining prefix is just `$group` does not need an
        // intermediate vector of every input document. Feed documents directly
        // into the group accumulators in `_id` order.
        if plan.sort.is_empty()
            && plan.skip == 0
            && plan.limit.is_none()
            && plan.projection.is_none()
        {
            if let Some(group_arg) = remaining
                .first()
                .and_then(Value::as_object)
                .filter(|obj| obj.len() == 1)
                .and_then(|obj| obj.get("$group"))
            {
                let mut builder = aggregate::GroupBuilder::new(group_arg)?;
                for item in self.db.iter(rtxn)? {
                    let (_key, raw) = item?;
                    let doc = parse_json(raw)?;
                    if matches(&doc, &plan.filter) {
                        builder.update(doc)?;
                    }
                }
                let grouped = builder.finish()?;
                if grouped.len() > max_docs {
                    return Err(Error::Query(format!(
                        "aggregation intermediate result exceeds {max_docs} documents"
                    )));
                }
                return aggregate::run(grouped, &remaining[1..], max_docs);
            }
        }

        let limit = plan
            .limit
            .map(|n| n.min(max_docs.saturating_add(1)))
            .unwrap_or_else(|| max_docs.saturating_add(1));
        let query = FindQuery {
            filter: plan.filter,
            projection: plan.projection,
            sort: plan.sort,
            skip: plan.skip,
            limit: Some(limit),
        };
        let docs = self.find_in(rtxn, &query)?;
        if docs.len() > max_docs {
            return Err(Error::Query(format!(
                "aggregation input exceeds {max_docs} documents"
            )));
        }
        aggregate::run(docs, &pipeline[plan.next..], max_docs)
    }

    pub fn find(&self, query: &FindQuery) -> Result<Vec<Value>> {
        crate::query::validate::validate_filter(&query.filter)?;
        let rtxn = self.read_txn()?;
        let limit = query.limit.unwrap_or(usize::MAX);
        let want = query.skip.saturating_add(limit);

        // Sorting the whole set is CPU-bound. When the filter has no
        // index, JSON parsing and TopK can be spread across all cores.
        // The snapshot stays consistent: the main thread reads one `RoTxn` and sends
        // only raw bytes to workers (the LMDB cursor is not shared).
        let sorted = !query.sort.is_empty();
        let parallel_base = limit > 0
            && want <= PARALLEL_SCAN_MAX_WANT
            && self.db.stat(&rtxn)?.entries >= PARALLEL_SCAN_MIN_ENTRIES
            && direct_id(&query.filter).is_none()
            && id_bounds(&query.filter).is_none()
            && id_sort_direction(&query.sort).is_none()
            && self.index_plan(&rtxn, &query.filter)?.is_none()
            && self.or_index_plans(&rtxn, &query.filter)?.is_none()
            && self.all_index_candidates(&rtxn, &query.filter)?.is_none()
            && !(is_match_all(&query.filter)
                && (self.sort_index_plan(&rtxn, &query.sort)?.is_some()
                    || self.multikey_sort_plan(&rtxn, &query.sort)?.is_some()));
        let parallel = parallel_base
            && (sorted || (!is_match_all(&query.filter) && want >= PARALLEL_UNSORTED_MIN_WANT));

        let out = if parallel {
            let docs = if sorted {
                self.parallel_sorted_scan(&rtxn, query, want)?
            } else {
                self.parallel_unsorted_scan(&rtxn, query, want)?
            };
            docs.into_iter()
                .skip(query.skip)
                .take(limit)
                .map(|doc| match &query.projection {
                    Some(projection) => project(&doc, projection),
                    None => doc,
                })
                .collect()
        } else {
            self.find_in(&rtxn, query)?
        };
        rtxn.commit()?;
        Ok(out)
    }

    /// Parallel index backfill: the main thread reads one `RwTxn`,
    /// while workers parse JSON and prepare entries. Writing to LMDB
    /// stays on the main thread (one transaction).
    fn parallel_backfill<T, F>(
        &self,
        wtxn: &RwTxn<'_>,
        kind: &str,
        target: &str,
        f: F,
    ) -> Result<Vec<T>>
    where
        T: Send,
        F: Fn(String, Value) -> Result<Vec<T>> + Sync,
    {
        let total = self.db.stat(wtxn)?.entries as u64;
        let progress = crate::progress::begin(&self.name, target, kind, total);
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(16);
        if workers <= 1 {
            return self.backfill_serial(wtxn, f, &progress);
        }

        let (job_tx, job_rx) = std::sync::mpsc::sync_channel::<BackfillChunk>(workers * 2);
        let job_rx = Arc::new(Mutex::new(job_rx));
        let (res_tx, res_rx) = std::sync::mpsc::channel::<Result<Vec<T>>>();
        let func = &f;

        std::thread::scope(|scope| -> Result<Vec<T>> {
            let progress_ref = &progress;
            for _ in 0..workers {
                let rx = Arc::clone(&job_rx);
                let tx = res_tx.clone();
                scope.spawn(move || loop {
                    let job = {
                        let guard = rx.lock().expect("kolejka backfillu");
                        guard.recv()
                    };
                    match job {
                        Ok(chunk) => {
                            let processed = chunk.docs.len() as u64;
                            let mut out = Vec::new();
                            for (id_start, id_len, doc_start, doc_len) in chunk.docs {
                                let id = std::str::from_utf8(
                                    &chunk.data[id_start as usize..][..id_len as usize],
                                )
                                .map_err(|e| Error::Query(e.to_string()))
                                .and_then(|id| {
                                    let doc = serde_json::from_slice::<Value>(
                                        &chunk.data[doc_start as usize..][..doc_len as usize],
                                    )?;
                                    func(id.to_string(), doc)
                                });
                                match id {
                                    Ok(entries) => out.extend(entries),
                                    Err(e) => {
                                        let _ = tx.send(Err(e));
                                        return;
                                    }
                                }
                            }
                            let _ = tx.send(Ok(out));
                            progress_ref.advance(processed);
                        }
                        Err(_) => break,
                    }
                });
            }
            drop(res_tx);

            let mut chunk = BackfillChunk {
                data: Vec::new(),
                docs: Vec::with_capacity(PARALLEL_SCAN_CHUNK),
            };
            for item in self.db.iter(&*wtxn)? {
                let (id_bytes, doc_bytes) = item?;
                let id_start = chunk.data.len() as u32;
                chunk.data.extend_from_slice(id_bytes);
                let id_len = id_bytes.len() as u32;
                let doc_start = chunk.data.len() as u32;
                chunk.data.extend_from_slice(doc_bytes);
                let doc_len = doc_bytes.len() as u32;
                chunk.docs.push((id_start, id_len, doc_start, doc_len));
                if chunk.docs.len() >= PARALLEL_SCAN_CHUNK {
                    let full = std::mem::replace(
                        &mut chunk,
                        BackfillChunk {
                            data: Vec::new(),
                            docs: Vec::with_capacity(PARALLEL_SCAN_CHUNK),
                        },
                    );
                    let _ = job_tx.send(full);
                }
            }
            if !chunk.docs.is_empty() {
                let _ = job_tx.send(chunk);
            }
            drop(job_tx);

            let mut all = Vec::new();
            for res in res_rx {
                all.extend(res?);
            }
            Ok(all)
        })
    }

    fn backfill_serial<T, F>(
        &self,
        wtxn: &RwTxn<'_>,
        f: F,
        progress: &crate::progress::BackfillGuard,
    ) -> Result<Vec<T>>
    where
        F: Fn(String, Value) -> Result<Vec<T>>,
    {
        let mut out = Vec::new();
        for item in self.db.iter(wtxn)? {
            let (id_bytes, doc_bytes) = item?;
            let id = std::str::from_utf8(id_bytes)
                .map_err(|e| Error::Query(e.to_string()))?
                .to_string();
            let doc: Value = parse_json(doc_bytes)?;
            out.extend(f(id, doc)?);
            progress.advance(1);
        }
        Ok(out)
    }

    /// Parallel full scan with sorting.
    ///
    /// The main thread iterates one `RoTxn` (consistent snapshot) and pushes chunks
    /// of raw bytes into a queue. Workers parse JSON, filter, and compute
    /// local TopK; finally the result is merged preserving `seq` order.
    fn parallel_sorted_scan(
        &self,
        rtxn: &RoTxn<'_>,
        query: &FindQuery,
        want: usize,
    ) -> Result<Vec<Value>> {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(16);
        if workers <= 1 || want == 0 {
            return self.scan_sorted_serial(rtxn, query, want);
        }

        let (job_tx, job_rx) = std::sync::mpsc::sync_channel::<ScanChunk>(workers * 2);
        let job_rx = Arc::new(Mutex::new(job_rx));
        let (res_tx, res_rx) = std::sync::mpsc::channel::<Result<Vec<(usize, Value)>>>();
        let sort = query.sort.as_slice();
        let filter = &query.filter;
        let raw = raw_prefilter(filter);
        let raw = raw.as_ref();

        std::thread::scope(|scope| -> Result<Vec<Value>> {
            for _ in 0..workers {
                let rx = Arc::clone(&job_rx);
                let tx = res_tx.clone();
                scope.spawn(move || {
                    let mut top = TopK::new(sort, want);
                    let mut failed = false;
                    loop {
                        let job = {
                            let guard = rx.lock().expect("kolejka zadań");
                            guard.recv()
                        };
                        match job {
                            Ok(chunk) => {
                                for (seq, start, len) in chunk.docs {
                                    let bytes = &chunk.data[start as usize..][..len as usize];
                                    match parse_prefiltered(bytes, raw) {
                                        Ok(Some(doc)) => {
                                            if matches(&doc, filter) {
                                                top.push_seq(doc, seq);
                                            }
                                        }
                                        Ok(None) => {}
                                        Err(e) => {
                                            if !failed {
                                                failed = true;
                                                let _ = tx.send(Err(e));
                                            }
                                        }
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    if !failed {
                        let _ = tx.send(Ok(top.into_sorted_seq()));
                    }
                });
            }
            drop(res_tx);

            let mut chunk = ScanChunk {
                idx: 0,
                data: Vec::new(),
                docs: Vec::with_capacity(PARALLEL_SCAN_CHUNK),
            };
            for (seq, item) in self.db.iter(rtxn)?.enumerate() {
                let (_key, bytes) = item?;
                let start = chunk.data.len() as u32;
                chunk.data.extend_from_slice(bytes);
                chunk.docs.push((seq, start, bytes.len() as u32));
                if chunk.docs.len() >= PARALLEL_SCAN_CHUNK {
                    let full = std::mem::replace(
                        &mut chunk,
                        ScanChunk {
                            idx: 0,
                            data: Vec::new(),
                            docs: Vec::with_capacity(PARALLEL_SCAN_CHUNK),
                        },
                    );
                    let _ = job_tx.send(full);
                }
            }
            if !chunk.docs.is_empty() {
                let _ = job_tx.send(chunk);
            }
            drop(job_tx);

            let mut merged = TopK::new(sort, want);
            for res in res_rx {
                for (seq, doc) in res? {
                    merged.push_seq(doc, seq);
                }
            }
            Ok(merged.into_sorted())
        })
    }

    /// Parallel full scan without sorting.
    ///
    /// Chunks are sent in waves: one to each worker. The coordinator
    /// receives the whole wave and assembles results in `_id` order. Once it has
    /// `want` matches, it stops sending more chunks. This lets broad
    /// filters keep early exit while selective scans run in parallel.
    fn parallel_unsorted_scan(
        &self,
        rtxn: &RoTxn<'_>,
        query: &FindQuery,
        want: usize,
    ) -> Result<Vec<Value>> {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(16);
        if workers <= 1 || want == 0 {
            return self.find_in(rtxn, query);
        }

        let mut job_txs = Vec::with_capacity(workers);
        let mut job_rxs = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (tx, rx) = std::sync::mpsc::sync_channel::<ScanChunk>(1);
            job_txs.push(tx);
            job_rxs.push(rx);
        }
        let (res_tx, res_rx) = std::sync::mpsc::channel::<Result<(usize, Vec<(usize, Value)>)>>();
        let filter = &query.filter;
        let raw = raw_prefilter(filter);
        let raw = raw.as_ref();
        // Create the iterator before starting workers: an LMDB error must not leave
        // workers waiting on a channel that is never closed.
        let mut iter = self.db.iter(rtxn)?;

        std::thread::scope(|scope| -> Result<Vec<Value>> {
            for rx in job_rxs {
                let tx = res_tx.clone();
                scope.spawn(move || {
                    let mut failed = false;
                    loop {
                        let job = rx.recv();
                        let Ok(chunk) = job else {
                            break;
                        };
                        let mut out = Vec::new();
                        if !failed {
                            for (seq, start, len) in chunk.docs {
                                let bytes = &chunk.data[start as usize..][..len as usize];
                                match parse_prefiltered(bytes, raw) {
                                    Ok(Some(doc)) => {
                                        if matches(&doc, filter) {
                                            out.push((seq, doc));
                                            if out.len() >= want {
                                                break;
                                            }
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(e) => {
                                        failed = true;
                                        let _ = tx.send(Err(e));
                                        break;
                                    }
                                }
                            }
                        }
                        if !failed {
                            let _ = tx.send(Ok((chunk.idx, out)));
                        }
                    }
                });
            }
            drop(res_tx);

            let mut merged = TopK::new(&[], want);
            let mut seq = 0usize;
            let mut next_chunk = 0usize;
            let mut expected = 0usize;
            let mut exhausted = false;
            let mut error: Option<Error> = None;

            // The first wave is small so a broad filter stops quickly.
            // Only when it is not enough do we enlarge the wave and amortize
            // synchronization on a selective full scan.
            let mut rounds = 1usize;
            'scan: loop {
                let mut sent = 0usize;
                'fill: for _ in 0..rounds {
                    for tx in &job_txs {
                        if exhausted {
                            break 'fill;
                        }
                        let mut chunk = ScanChunk {
                            idx: next_chunk,
                            data: Vec::new(),
                            docs: Vec::with_capacity(PARALLEL_SCAN_CHUNK),
                        };
                        while chunk.docs.len() < PARALLEL_SCAN_CHUNK {
                            match iter.next() {
                                Some(item) => match item {
                                    Ok((_key, bytes)) => {
                                        let start = chunk.data.len() as u32;
                                        chunk.data.extend_from_slice(bytes);
                                        chunk.docs.push((seq, start, bytes.len() as u32));
                                        seq += 1;
                                    }
                                    Err(e) => {
                                        error = Some(e.into());
                                        break 'scan;
                                    }
                                },
                                None => {
                                    exhausted = true;
                                    break;
                                }
                            }
                        }
                        if chunk.docs.is_empty() {
                            break 'fill;
                        }
                        next_chunk += 1;
                        if tx.send(chunk).is_err() {
                            error = Some(Error::Query("worker skanujący zakończył pracę".into()));
                            break 'scan;
                        }
                        sent += 1;
                    }
                }
                if sent == 0 {
                    break;
                }

                let mut slots: Vec<Option<Vec<(usize, Value)>>> = vec![None; sent];
                for _ in 0..sent {
                    let msg = match res_rx.recv() {
                        Ok(msg) => msg,
                        Err(_) => {
                            error = Some(Error::Query("brak wyniku skanu równoległego".into()));
                            break 'scan;
                        }
                    };
                    let (idx, docs) = match msg {
                        Ok(v) => v,
                        Err(e) => {
                            error = Some(e);
                            break 'scan;
                        }
                    };
                    let pos = idx.checked_sub(expected).unwrap_or(usize::MAX);
                    if pos >= sent {
                        error = Some(Error::Query("nieoczekiwana porcja skanu".into()));
                        break 'scan;
                    }
                    slots[pos] = Some(docs);
                }
                for docs in slots.into_iter().flatten() {
                    for (seq, doc) in docs {
                        merged.push_seq(doc, seq);
                    }
                    expected += 1;
                    if merged.heap.len() >= want {
                        break 'scan;
                    }
                }
                rounds = if want <= 1024 {
                    4
                } else if want <= 4096 {
                    2
                } else {
                    1
                };
            }
            drop(job_txs);
            match error {
                Some(e) => Err(e),
                None => Ok(merged.into_sorted()),
            }
        })
    }

    /// Single-threaded version used as a fallback (e.g. one core).
    fn scan_sorted_serial(
        &self,
        rtxn: &RoTxn<'_>,
        query: &FindQuery,
        want: usize,
    ) -> Result<Vec<Value>> {
        let mut top = TopK::new(&query.sort, want);
        let raw = raw_prefilter(&query.filter);
        for (seq, item) in self.db.iter(rtxn)?.enumerate() {
            let (_key, bytes) = item?;
            let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                continue;
            };
            if matches(&doc, &query.filter) {
                top.push_seq(doc, seq);
            }
        }
        Ok(top.into_sorted())
    }

    pub fn find_in(&self, rtxn: &RoTxn<'_>, query: &FindQuery) -> Result<Vec<Value>> {
        crate::query::validate::validate_filter(&query.filter)?;
        let limit = query.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let sorted = !query.sort.is_empty();
        // Without sorting, `skip + limit` matches are enough (early exit).
        // With sorting, TopK keeps only the `want` best documents.
        let want = query.skip.saturating_add(limit);
        let mut acc = Acc::new(sorted, &query.sort, want);
        let mut post_skip = query.skip;
        let raw = raw_prefilter(&query.filter);

        // 0) Ordered match-all queries can walk primary-key or index order
        //    directly, stopping after `skip + limit` entries.
        if is_match_all(&query.filter) && sorted {
            if let Some(out) = self.find_sorted_match_all(rtxn, query)? {
                return Ok(out);
            }
        }

        // 0b) Top-level `$or`: when every branch has an index, merge the
        //     candidate ids and apply the full filter as a residual.
        if let Some(plans) = self.or_index_plans(rtxn, &query.filter)? {
            let mut seen = HashSet::new();
            'plans: for plan in &plans {
                let range = (as_bound(&plan.bounds.0), as_bound(&plan.bounds.1));
                for item in plan.db.range(rtxn, &range)? {
                    let (_key, value) = item?;
                    let Some((id, _)) = index::decode_index_value(value) else {
                        continue;
                    };
                    if !seen.insert(id.clone()) {
                        continue;
                    }
                    let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                        continue;
                    };
                    let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                        continue;
                    };
                    if !matches(&doc, &query.filter) {
                        continue;
                    }
                    acc.push(doc);
                    if !sorted && acc.len() >= want {
                        break 'plans;
                    }
                }
            }
            let docs = acc.finish();
            return Ok(docs
                .into_iter()
                .skip(post_skip)
                .take(limit)
                .map(|doc| match &query.projection {
                    Some(projection) => project(&doc, projection),
                    None => doc,
                })
                .collect());
        }

        // 1) Exact `_id` -> direct get.
        if let Some(id) = direct_id(&query.filter) {
            if let Some(bytes) = self.db.get(rtxn, id.as_bytes())? {
                acc.push(parse_json(bytes)?);
            }
        }
        // 1b) Multikey `$all`: intersect the index ranges for every required
        //     value, then apply the full filter as a residual.
        else if let Some(ids) = self.all_index_candidates(rtxn, &query.filter)? {
            for id in ids {
                if !sorted && acc.len() >= want {
                    break;
                }
                let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                    continue;
                };
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, &query.filter) {
                    acc.push(doc);
                }
            }
        }
        // 2) Secondary index. First the fast "index-only" path: if
        //    the sort exactly matches the index order, we walk the index
        //    and fetch only `skip + limit` documents instead of the whole range.
        else if let Some(plan) = self.index_plan(rtxn, &query.filter)? {
            let sort_match = if sorted && !plan.def.is_multikey() {
                index_matches_sort(&plan.def, &query.sort)
            } else {
                None
            };
            if let Some(reverse) = sort_match {
                let (lower, upper) = &plan.bounds;
                let range = (as_bound(lower), as_bound(upper));
                let mut skipped = 0usize;
                let mut out = Vec::new();
                let iter: IndexIter<'_> = if reverse {
                    Box::new(plan.db.rev_range(rtxn, &range)?)
                } else {
                    Box::new(plan.db.range(rtxn, &range)?)
                };
                for item in iter {
                    let (_key, value) = item?;
                    let Some((id, inc)) = index::decode_index_value(value) else {
                        continue;
                    };
                    let doc = if let Some(doc) =
                        covering_doc(&id, inc.as_ref(), &query.filter, &query.projection)
                    {
                        doc
                    } else {
                        let Some(bytes) = self.db.get(rtxn, id.as_bytes())? else {
                            continue;
                        };
                        let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                            continue;
                        };
                        doc
                    };
                    if !matches(&doc, &query.filter) {
                        continue;
                    }
                    if skipped < query.skip {
                        skipped += 1;
                        continue;
                    }
                    out.push(doc);
                    if out.len() >= limit {
                        break;
                    }
                }
                return Ok(out
                    .into_iter()
                    .map(|doc| match &query.projection {
                        Some(projection) => project(&doc, projection),
                        None => doc,
                    })
                    .collect());
            }

            let ids = self.index_candidates(rtxn, &query.filter, (!sorted).then_some(want))?;
            for id in ids.into_iter().flatten() {
                if !sorted && acc.len() >= want {
                    break;
                }
                if let Some(bytes) = self.db.get(rtxn, id.as_bytes())? {
                    let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                        continue;
                    };
                    // Residual filter: the index may return extra entries.
                    if matches(&doc, &query.filter) {
                        acc.push(doc);
                    }
                }
            }
        }
        // 3) `_id` range over the primary key. This is not a secondary index, but
        //    it narrows the full scan to a contiguous B+tree interval.
        else if let Some((lower, upper)) = id_bounds(&query.filter) {
            let range = (as_bound(&lower), as_bound(&upper));
            let reverse = id_sort_direction(&query.sort).unwrap_or(false);
            let iter: IndexIter<'_> = if reverse {
                Box::new(self.db.rev_range(rtxn, &range)?)
            } else {
                Box::new(self.db.range(rtxn, &range)?)
            };

            if !sorted || id_sort_direction(&query.sort).is_some() {
                let mut skipped = 0usize;
                let mut out = Vec::new();
                for item in iter {
                    let (_key, bytes) = item?;
                    let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                        continue;
                    };
                    if !matches(&doc, &query.filter) {
                        continue;
                    }
                    if skipped < query.skip {
                        skipped += 1;
                        continue;
                    }
                    out.push(doc);
                    if out.len() >= limit {
                        break;
                    }
                }
                return Ok(out
                    .into_iter()
                    .map(|doc| match &query.projection {
                        Some(projection) => project(&doc, projection),
                        None => doc,
                    })
                    .collect());
            }

            let mut top = TopK::new(&query.sort, want);
            for item in iter {
                let (_key, bytes) = item?;
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, &query.filter) {
                    top.push(doc);
                }
            }
            return Ok(top
                .into_sorted()
                .into_iter()
                .skip(query.skip)
                .take(limit)
                .map(|doc| match &query.projection {
                    Some(projection) => project(&doc, projection),
                    None => doc,
                })
                .collect());
        }
        // 4) Full scan without sorting: skip immediately, without collecting
        //     skipped documents or sorting the whole batch at the end.
        else if !sorted {
            post_skip = 0;
            if is_match_all(&query.filter) {
                // Natural order is `_id`; skipped documents do not need parsing.
                for item in self.db.iter(rtxn)?.skip(query.skip).take(limit) {
                    let (_key, bytes) = item?;
                    acc.push(parse_json(bytes)?);
                }
            } else {
                let mut skipped = 0usize;
                for item in self.db.iter(rtxn)? {
                    let (_key, bytes) = item?;
                    let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                        continue;
                    };
                    if matches(&doc, &query.filter) {
                        if skipped < query.skip {
                            skipped += 1;
                            continue;
                        }
                        acc.push(doc);
                        if acc.len() >= limit {
                            break;
                        }
                    }
                }
            }
        }
        // 3b) Full scan with sorting: collect matches through TopK.
        else {
            for item in self.db.iter(rtxn)? {
                let (_key, bytes) = item?;
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, &query.filter) {
                    acc.push(doc);
                }
            }
        }

        let docs = acc.finish();
        Ok(docs
            .into_iter()
            .skip(post_skip)
            .take(limit)
            .map(|doc| match &query.projection {
                Some(projection) => project(&doc, projection),
                None => doc,
            })
            .collect())
    }

    pub fn find_one(&self, filter: &Value) -> Result<Option<Value>> {
        let query = FindQuery {
            filter: filter.clone(),
            sort: Vec::new(),
            skip: 0,
            limit: Some(1),
            projection: None,
        };
        Ok(self.find(&query)?.into_iter().next())
    }

    pub fn find_one_in(
        &self,
        rtxn: &RoTxn<'_>,
        filter: &Value,
        projection: Option<&Value>,
    ) -> Result<Option<Value>> {
        let query = FindQuery {
            filter: filter.clone(),
            sort: Vec::new(),
            skip: 0,
            limit: Some(1),
            projection: projection.cloned(),
        };
        Ok(self.find_in(rtxn, &query)?.into_iter().next())
    }

    /// Explains the access path and actual counts for a `find` query.
    ///
    /// This executes the query so `actual_matched` and `actual_returned` are
    /// exact. Use small limits when explaining broad queries.
    pub fn explain_in(&self, rtxn: &RoTxn<'_>, query: &FindQuery) -> Result<Value> {
        crate::query::validate::validate_filter(&query.filter)?;
        let mut out = self.explain_plan(rtxn, query)?;
        let matched = self.count_in(rtxn, &query.filter)?;
        let returned = self.find_in(rtxn, query)?.len();
        let obj = out
            .as_object_mut()
            .ok_or_else(|| Error::Query("internal explain error".into()))?;
        obj.insert("filter".into(), query.filter.clone());
        obj.insert(
            "sort".into(),
            Value::Array(
                query
                    .sort
                    .iter()
                    .map(|(field, dir)| serde_json::json!([field, dir]))
                    .collect(),
            ),
        );
        obj.insert("skip".into(), Value::from(query.skip));
        obj.insert("limit".into(), query.limit.map_or(Value::Null, Value::from));
        obj.insert(
            "estimated_documents".into(),
            Value::from(obj.get("candidates").and_then(Value::as_u64).unwrap_or(0)),
        );
        obj.insert("actual_matched".into(), Value::from(matched as u64));
        obj.insert("actual_returned".into(), Value::from(returned as u64));
        Ok(out)
    }

    pub fn explain(&self, query: &FindQuery) -> Result<Value> {
        let rtxn = self.read_txn()?;
        let out = self.explain_in(&rtxn, query)?;
        rtxn.commit()?;
        Ok(out)
    }

    fn explain_plan(&self, rtxn: &RoTxn<'_>, query: &FindQuery) -> Result<Value> {
        let sort_covered = |plan: Option<&IndexDef>| -> bool {
            if query.sort.is_empty() {
                return true;
            }
            if id_sort_direction(&query.sort).is_some() {
                return true;
            }
            plan.and_then(|def| index_matches_sort(def, &query.sort))
                .is_some()
        };
        let mut out = serde_json::Map::new();
        out.insert("collection".into(), Value::String(self.name.clone()));

        if is_match_all(&query.filter) {
            let candidates = self.db.stat(rtxn)?.entries as u64;
            if id_sort_direction(&query.sort).is_some() {
                out.insert("access_path".into(), Value::String("id_range".into()));
                out.insert("index_used".into(), Value::Bool(true));
                out.insert(
                    "index".into(),
                    serde_json::json!({"fields": ["_id"], "primary": true}),
                );
                out.insert("bounds".into(), Value::Null);
                out.insert("candidates".into(), Value::from(candidates));
                out.insert("sort_covered".into(), Value::Bool(true));
                return Ok(Value::Object(out));
            }
            let sort_plan = match self.sort_index_plan(rtxn, &query.sort)? {
                Some(plan) => Some(plan),
                None => self.multikey_sort_plan(rtxn, &query.sort)?,
            };
            if let Some(plan) = sort_plan {
                out.insert("access_path".into(), Value::String("index_scan".into()));
                out.insert("index_used".into(), Value::Bool(true));
                out.insert(
                    "index".into(),
                    serde_json::json!({
                        "fields": plan.def.effective_fields(),
                        "unique": plan.def.unique,
                        "include": plan.def.include,
                        "multikey": plan.def.multikey_fields,
                    }),
                );
                out.insert("bounds".into(), Value::Null);
                out.insert("candidates".into(), Value::from(candidates));
                out.insert("sort_covered".into(), Value::Bool(true));
                return Ok(Value::Object(out));
            }
            out.insert(
                "access_path".into(),
                Value::String("collection_scan".into()),
            );
            out.insert("index_used".into(), Value::Bool(false));
            out.insert("index".into(), Value::Null);
            out.insert("bounds".into(), Value::Null);
            out.insert("candidates".into(), Value::from(candidates));
            out.insert("sort_covered".into(), Value::Bool(sort_covered(None)));
            return Ok(Value::Object(out));
        }

        if let Some(id) = direct_id(&query.filter) {
            let exists = self.db.get(rtxn, id.as_bytes())?.is_some();
            out.insert("access_path".into(), Value::String("id_lookup".into()));
            out.insert("index_used".into(), Value::Bool(true));
            out.insert(
                "index".into(),
                serde_json::json!({"fields": ["_id"], "primary": true}),
            );
            out.insert("bounds".into(), Value::Null);
            out.insert("candidates".into(), Value::from(u64::from(exists)));
            out.insert("sort_covered".into(), Value::Bool(sort_covered(None)));
            return Ok(Value::Object(out));
        }

        if let Some(ids) = self.all_index_candidates(rtxn, &query.filter)? {
            out.insert("access_path".into(), Value::String("index_scan".into()));
            out.insert("index_used".into(), Value::Bool(true));
            out.insert(
                "index".into(),
                serde_json::json!({
                    "fields": [query.filter.as_object().and_then(|o| o.keys().next())],
                    "multikey": true,
                }),
            );
            out.insert("bounds".into(), Value::Null);
            out.insert("candidates".into(), Value::from(ids.len() as u64));
            out.insert("sort_covered".into(), Value::Bool(sort_covered(None)));
            return Ok(Value::Object(out));
        }

        if let Some(plans) = self.or_index_plans(rtxn, &query.filter)? {
            let mut candidates = 0usize;
            let mut indexes = Vec::with_capacity(plans.len());
            for plan in &plans {
                candidates = candidates.saturating_add(self.index_count_plan(rtxn, plan)?);
                indexes.push(serde_json::json!({
                    "fields": plan.def.effective_fields(),
                    "unique": plan.def.unique,
                    "include": plan.def.include,
                }));
            }
            out.insert("access_path".into(), Value::String("index_union".into()));
            out.insert("index_used".into(), Value::Bool(true));
            out.insert("index".into(), Value::Array(indexes));
            out.insert("bounds".into(), Value::Null);
            out.insert("candidates".into(), Value::from(candidates as u64));
            out.insert("sort_covered".into(), Value::Bool(sort_covered(None)));
            return Ok(Value::Object(out));
        }

        if let Some(plan) = self.index_plan(rtxn, &query.filter)? {
            let candidates = if plan.def.is_multikey() {
                self.index_count_unique_plan(rtxn, &plan)?
            } else {
                self.index_count_plan(rtxn, &plan)?
            };
            let fields = plan.def.effective_fields();
            out.insert("access_path".into(), Value::String("index_scan".into()));
            out.insert("index_used".into(), Value::Bool(true));
            out.insert(
                "index".into(),
                serde_json::json!({
                    "fields": fields,
                    "unique": plan.def.unique,
                    "include": plan.def.include,
                    "multikey": plan.def.multikey_fields,
                }),
            );
            out.insert(
                "bounds".into(),
                serde_json::json!({
                    "lower": explain_bound(&plan.bounds.0, false),
                    "upper": explain_bound(&plan.bounds.1, false),
                }),
            );
            out.insert("candidates".into(), Value::from(candidates as u64));
            out.insert(
                "sort_covered".into(),
                Value::Bool(sort_covered(Some(&plan.def))),
            );
            return Ok(Value::Object(out));
        }

        if let Some((lower, upper)) = id_bounds(&query.filter) {
            let range = (as_bound(&lower), as_bound(&upper));
            let candidates = self.db.range(rtxn, &range)?.count();
            out.insert("access_path".into(), Value::String("id_range".into()));
            out.insert("index_used".into(), Value::Bool(true));
            out.insert(
                "index".into(),
                serde_json::json!({"fields": ["_id"], "primary": true}),
            );
            out.insert(
                "bounds".into(),
                serde_json::json!({
                    "lower": explain_bound(&lower, true),
                    "upper": explain_bound(&upper, true),
                }),
            );
            out.insert("candidates".into(), Value::from(candidates as u64));
            out.insert("sort_covered".into(), Value::Bool(sort_covered(None)));
            return Ok(Value::Object(out));
        }

        out.insert(
            "access_path".into(),
            Value::String("collection_scan".into()),
        );
        out.insert("index_used".into(), Value::Bool(false));
        out.insert("index".into(), Value::Null);
        out.insert("bounds".into(), Value::Null);
        out.insert(
            "candidates".into(),
            Value::from(self.db.stat(rtxn)?.entries as u64),
        );
        out.insert("sort_covered".into(), Value::Bool(sort_covered(None)));
        Ok(Value::Object(out))
    }

    pub fn count(&self, filter: &Value) -> Result<usize> {
        let rtxn = self.read_txn()?;
        let n = self.count_in(&rtxn, filter)?;
        rtxn.commit()?;
        Ok(n)
    }

    pub fn count_in(&self, rtxn: &RoTxn<'_>, filter: &Value) -> Result<usize> {
        crate::query::validate::validate_filter(filter)?;
        // `count({})` / `count(null)` -> O(1) from B+tree metadata.
        if is_match_all(filter) {
            return Ok(self.db.stat(rtxn)?.entries);
        }
        // Exact `_id` -> direct `get`.
        if let Some(id) = direct_id(filter) {
            return Ok(usize::from(self.db.get(rtxn, id.as_bytes())?.is_some()));
        }
        // `_id` range over the primary key. When the filter contains only conditions on
        // `_id`, counting range entries without parsing JSON is enough.
        if let Some((lower, upper)) = id_bounds(filter) {
            let range = (as_bound(&lower), as_bound(&upper));
            if filter.as_object().is_some_and(|o| o.len() == 1) {
                let mut n = 0usize;
                for item in self.db.range(rtxn, &range)? {
                    item?;
                    n += 1;
                }
                return Ok(n);
            }
            let mut n = 0usize;
            let raw = raw_prefilter(filter);
            for item in self.db.range(rtxn, &range)? {
                let (_key, bytes) = item?;
                let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                    continue;
                };
                if matches(&doc, filter) {
                    n += 1;
                }
            }
            return Ok(n);
        }
        // Multikey `$all`: the intersection already contains distinct ids.
        if let Some(ids) = self.all_index_candidates(rtxn, filter)? {
            return Ok(ids.len());
        }
        // Filter fully covered by an index: count keys without parsing JSON.
        if let Some(plan) = self.index_plan(rtxn, filter)? {
            if index_filter_exact(&plan.def, filter) {
                return if plan.def.is_multikey() {
                    self.index_count_unique_plan(rtxn, &plan)
                } else {
                    self.index_count_plan(rtxn, &plan)
                };
            }
        }
        let mut n = 0;
        let raw = raw_prefilter(filter);
        for item in self.db.iter(rtxn)? {
            let (_key, bytes) = item?;
            let Some(doc) = parse_prefiltered(bytes, raw.as_ref())? else {
                continue;
            };
            if matches(&doc, filter) {
                n += 1;
            }
        }
        Ok(n)
    }

    /// Deep-checks documents and all index structures in a read transaction.
    pub fn verify_in(&self, rtxn: &RoTxn<'_>) -> Result<CollectionVerification> {
        let defs = self.index_defs(rtxn)?;
        let text_fields = self.text_defs(rtxn)?;
        let fuzzy_fields = self.fuzzy_defs(rtxn)?;

        let mut documents = 0u64;
        let mut bytes = 0u64;
        let mut expected_index = vec![0u64; defs.len()];
        let mut expected_postings = vec![0u64; text_fields.len()];
        let mut expected_lengths = vec![0u64; text_fields.len()];
        let mut expected_tokens = vec![0u64; text_fields.len()];
        let mut expected_trigrams = vec![0u64; fuzzy_fields.len()];

        for item in self.db.iter(rtxn)? {
            let (key, raw) = item?;
            let id = std::str::from_utf8(key).map_err(|e| {
                Error::Query(format!("invalid document key in `{}`: {e}", self.name))
            })?;
            let doc: Value = serde_json::from_slice(raw).map_err(|e| {
                Error::Query(format!("invalid JSON for `{}`/`{id}`: {e}", self.name))
            })?;
            let Some(obj) = doc.as_object() else {
                return Err(Error::Query(format!(
                    "document `{}`/`{id}` is not a JSON object",
                    self.name
                )));
            };
            if obj.get("_id").and_then(Value::as_str) != Some(id) {
                return Err(Error::Query(format!(
                    "document `{}`/`{id}` has a missing or mismatched _id",
                    self.name
                )));
            }
            documents += 1;
            bytes += raw.len() as u64;

            for (i, def) in defs.iter().enumerate() {
                if !def.current_format() {
                    continue;
                }
                let keys = index::encode_index_keys(def, &doc)?;
                if keys.keys.is_empty() {
                    continue;
                }
                let db = self.index_db(rtxn, def)?.ok_or_else(|| {
                    Error::Query(format!(
                        "index `{}` on `{}` is defined but its database is missing",
                        def.name(),
                        self.name
                    ))
                })?;
                let expected = index::encode_index_value(def, &doc, id);
                for value_bytes in keys.keys {
                    let mut index_key = value_bytes;
                    index_key.extend_from_slice(key);
                    let actual = db.get(rtxn, &index_key)?.ok_or_else(|| {
                        Error::Query(format!(
                            "index `{}` on `{}` is missing entry for `{id}`",
                            def.name(),
                            self.name
                        ))
                    })?;
                    if actual != expected.as_slice() {
                        return Err(Error::Query(format!(
                            "index `{}` on `{}` has a stale entry for `{id}`",
                            def.name(),
                            self.name
                        )));
                    }
                    expected_index[i] += 1;
                }
            }

            for (i, field) in text_fields.iter().enumerate() {
                let Some(text) = index::field_value(&doc, field).and_then(search::field_text)
                else {
                    continue;
                };
                let tokens = search::tokenize(&text);
                let postings = self.text_postings(rtxn, field)?.ok_or_else(|| {
                    Error::Query(format!(
                        "text index `{field}` on `{}` is defined but its postings are missing",
                        self.name
                    ))
                })?;
                let lengths = self.text_len_db(rtxn, field)?.ok_or_else(|| {
                    Error::Query(format!(
                        "text index `{field}` on `{}` is defined but its lengths are missing",
                        self.name
                    ))
                })?;
                for (term, tf) in search::term_freqs(&tokens) {
                    let key = search::postings_key(&term, id);
                    let actual = postings.get(rtxn, &key)?.ok_or_else(|| {
                        Error::Query(format!(
                            "text index `{field}` on `{}` is missing term `{term}` for `{id}`",
                            self.name
                        ))
                    })?;
                    if actual != tf.to_le_bytes().as_slice() {
                        return Err(Error::Query(format!(
                            "text index `{field}` on `{}` has a stale term `{term}` for `{id}`",
                            self.name
                        )));
                    }
                    expected_postings[i] += 1;
                }
                let actual = lengths.get(rtxn, key)?.ok_or_else(|| {
                    Error::Query(format!(
                        "text index `{field}` on `{}` is missing length for `{id}`",
                        self.name
                    ))
                })?;
                let expected_len = tokens.len() as u32;
                if actual != expected_len.to_le_bytes().as_slice() {
                    return Err(Error::Query(format!(
                        "text index `{field}` on `{}` has a stale length for `{id}`",
                        self.name
                    )));
                }
                expected_lengths[i] += 1;
                expected_tokens[i] += expected_len as u64;
            }

            for (i, field) in fuzzy_fields.iter().enumerate() {
                let Some(text) = index::field_value(&doc, field).and_then(search::field_text)
                else {
                    continue;
                };
                let db = self.fuzzy_db(rtxn, field)?.ok_or_else(|| {
                    Error::Query(format!(
                        "fuzzy index `{field}` on `{}` is defined but its database is missing",
                        self.name
                    ))
                })?;
                for gram in search::trigrams(&text) {
                    let key = search::postings_key(&gram, id);
                    if db.get(rtxn, &key)?.is_none() {
                        return Err(Error::Query(format!(
                            "fuzzy index `{field}` on `{}` is missing trigram `{gram}` for `{id}`",
                            self.name
                        )));
                    }
                    expected_trigrams[i] += 1;
                }
            }
        }

        let mut indexes = Vec::with_capacity(defs.len());
        for (i, def) in defs.iter().enumerate() {
            let db = self.index_db(rtxn, def)?.ok_or_else(|| {
                Error::Query(format!(
                    "index `{}` on `{}` is defined but its database is missing",
                    def.name(),
                    self.name
                ))
            })?;
            let entries = db.stat(rtxn)?.entries as u64;
            let checked = def.current_format();
            if checked && entries != expected_index[i] {
                return Err(Error::Query(format!(
                    "index `{}` on `{}` has {entries} entries, expected {}",
                    def.name(),
                    self.name,
                    expected_index[i]
                )));
            }
            indexes.push(IndexVerification {
                name: def.name(),
                fields: def.effective_fields(),
                unique: def.unique,
                include: def.include.clone(),
                format: def.format,
                entries,
                checked,
                multikey_fields: def.multikey_fields.clone(),
                multikey_all_docs: def.multikey_all_docs,
            });
        }

        for (i, field) in text_fields.iter().enumerate() {
            let postings = self.text_postings(rtxn, field)?.ok_or_else(|| {
                Error::Query(format!(
                    "text index `{field}` on `{}` is defined but its postings are missing",
                    self.name
                ))
            })?;
            let lengths = self.text_len_db(rtxn, field)?.ok_or_else(|| {
                Error::Query(format!(
                    "text index `{field}` on `{}` is defined but its lengths are missing",
                    self.name
                ))
            })?;
            let postings_entries = postings.stat(rtxn)?.entries as u64;
            if postings_entries != expected_postings[i] {
                return Err(Error::Query(format!(
                    "text index `{field}` on `{}` has {postings_entries} postings, expected {}",
                    self.name, expected_postings[i]
                )));
            }
            let length_entries = lengths.stat(rtxn)?.entries as u64;
            if length_entries != expected_lengths[i] {
                return Err(Error::Query(format!(
                    "text index `{field}` on `{}` has {length_entries} lengths, expected {}",
                    self.name, expected_lengths[i]
                )));
            }
            let total_key = search::text_total_key(&self.name, field);
            let total = match self.meta.get(rtxn, total_key.as_bytes())? {
                Some(raw) => search::read_u64(raw).ok_or_else(|| {
                    Error::Query(format!(
                        "text index `{field}` on `{}` has an invalid total length",
                        self.name
                    ))
                })?,
                None => 0,
            };
            if total != expected_tokens[i] {
                return Err(Error::Query(format!(
                    "text index `{field}` on `{}` has total length {total}, expected {}",
                    self.name, expected_tokens[i]
                )));
            }
        }

        for (i, field) in fuzzy_fields.iter().enumerate() {
            let db = self.fuzzy_db(rtxn, field)?.ok_or_else(|| {
                Error::Query(format!(
                    "fuzzy index `{field}` on `{}` is defined but its database is missing",
                    self.name
                ))
            })?;
            let entries = db.stat(rtxn)?.entries as u64;
            if entries != expected_trigrams[i] {
                return Err(Error::Query(format!(
                    "fuzzy index `{field}` on `{}` has {entries} trigrams, expected {}",
                    self.name, expected_trigrams[i]
                )));
            }
        }

        Ok(CollectionVerification {
            documents,
            bytes,
            indexes,
            text_indexes: text_fields,
            fuzzy_indexes: fuzzy_fields,
        })
    }

    fn read_txn(&self) -> Result<crate::store::ReadTxn<'_>> {
        let guard = self.txn_gate.enter();
        Ok(crate::store::ReadTxn::new(self.env.read_txn()?, guard))
    }

    fn write_txn(&self) -> Result<crate::store::WriteTxn<'_>> {
        let guard = self.txn_gate.enter();
        Ok(crate::store::WriteTxn::new(self.env.write_txn()?, guard))
    }
}

// ----------------------------- helpers -------------------------------

/// Equality value from a condition (`5` or `{"$eq": 5}`).
fn eq_value(cond: &Value) -> Option<&Value> {
    if date::is_date(cond) || numeric::is_numeric(cond) || bson_types::is_bson_value(cond) {
        return Some(cond);
    }
    if let Some(obj) = cond.as_object() {
        if obj.len() == 1 {
            if let Some(value) = obj.get("$eq") {
                return Some(value);
            }
            if let Some(values) = obj.get("$all").and_then(Value::as_array) {
                if values.len() == 1 {
                    return values.first();
                }
            }
        }
        return None;
    }
    Some(cond)
}

/// Builds an index range for the filter when all definition fields have
/// conditions and the prefix (everything except the last field) is equality-based.
fn nested_map_value<'a>(map: &'a Map<String, Value>, path: &str) -> Option<&'a Value> {
    let mut parts = path.split('.');
    let mut current = map.get(parts.next()?)?;
    for part in parts {
        current = current.as_object()?.get(part)?;
    }
    Some(current)
}

/// Returns the filter condition relevant to an index field. For a dotted field
/// such as `items.price`, a parent `$elemMatch` is translated into a condition
/// on that leaf path; the full filter is still applied as a residual.
fn index_condition<'a>(filter: &'a Map<String, Value>, field: &str) -> Option<&'a Value> {
    if let Some(cond) = filter.get(field) {
        if let Some(elem) = cond.as_object().and_then(|o| o.get("$elemMatch")) {
            if elem
                .as_object()
                .is_some_and(|o| o.keys().all(|k| k.starts_with('$')))
            {
                return Some(elem);
            }
        }
        return Some(cond);
    }
    let (parent, rest) = field.split_once('.')?;
    let elem = filter
        .get(parent)?
        .as_object()?
        .get("$elemMatch")?
        .as_object()?;
    nested_map_value(elem, rest)
}

fn bounds_for_index(def: &IndexDef, filter: &Map<String, Value>) -> Option<index::RangeBytes> {
    let fields = def.effective_fields();
    if fields.is_empty() {
        return None;
    }
    let mut prefix = Vec::new();
    for (i, field) in fields.iter().enumerate() {
        let cond = index_condition(filter, field)?;
        // `null` also matches a missing field, and missing fields are not in the index.
        if cond.is_null()
            || cond
                .as_object()
                .and_then(|o| o.get("$eq"))
                .is_some_and(Value::is_null)
        {
            return None;
        }
        if i + 1 == fields.len() {
            let bounds = index::bounds_from_cond(cond)?;
            return Some(if prefix.is_empty() {
                bounds
            } else {
                index::prefix_bounds(&prefix, bounds)
            });
        }
        let v = eq_value(cond)?;
        prefix.extend(index::encode_value(v)?);
    }
    Some(index::exact_prefix(&prefix))
}

fn explain_bound(bound: &Bound<Vec<u8>>, utf8: bool) -> Value {
    match bound {
        Bound::Unbounded => Value::Null,
        Bound::Included(bytes) => serde_json::json!({
            "value": explain_bytes(bytes, utf8),
            "inclusive": true,
        }),
        Bound::Excluded(bytes) => serde_json::json!({
            "value": explain_bytes(bytes, utf8),
            "inclusive": false,
        }),
    }
}

fn explain_bytes(bytes: &[u8], utf8: bool) -> String {
    if utf8 {
        return String::from_utf8_lossy(bytes).into_owned();
    }
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn is_match_all(filter: &Value) -> bool {
    filter.is_null() || filter.as_object().is_some_and(|o| o.is_empty())
}

/// Whether `count` can trust the index without reading documents.
///
/// We exclude `null` (the matcher treats a missing field as `null`, and missing fields
/// are not in the index) and operators the index does not support.
fn count_cond_safe(cond: &Value) -> bool {
    if date::is_date(cond) || numeric::is_numeric(cond) || bson_types::is_bson_value(cond) {
        return true;
    }
    match cond {
        Value::Null => false,
        Value::Object(ops) => {
            if ops.is_empty() || ops.values().any(Value::is_null) {
                return false;
            }
            // `$eq` only on its own; range operators may be combined (`$gte` + `$lt`).
            if ops.contains_key("$eq") && ops.len() != 1 {
                return false;
            }
            ops.keys()
                .all(|op| matches!(op.as_str(), "$eq" | "$gt" | "$gte" | "$lt" | "$lte"))
        }
        _ => true,
    }
}

/// The filter covers exactly all index fields (no extra conditions),
/// so the number of entries in the range equals the number of matching documents.
fn index_filter_exact(def: &IndexDef, filter: &Value) -> bool {
    let Some(obj) = filter.as_object() else {
        return false;
    };
    let fields = def.effective_fields();
    obj.len() == fields.len()
        && fields
            .iter()
            .all(|f| obj.get(f).is_some_and(count_cond_safe))
}

fn as_bound(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn value_at<'a>(doc: &'a Value, path_str: &str) -> Option<&'a Value> {
    path::get(doc, path_str)
}

/// Recognizes a filter equivalent to an `_id` lookup.
fn direct_id(filter: &Value) -> Option<&str> {
    let obj = filter.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    match obj.get("_id")? {
        Value::String(id) => Some(id),
        Value::Object(op) if op.len() == 1 => op.get("$eq")?.as_str(),
        _ => None,
    }
}

/// Builds a primary-key `_id` range from `$gt/$gte/$lt/$lte` conditions.
/// An exact `_id` (string or `{"$eq": ...}`) is also a range, which allows
/// combining it with additional conditions without a full scan.
fn id_bounds(filter: &Value) -> Option<IdBounds> {
    let cond = filter.as_object()?.get("_id")?;
    if let Some(id) = cond.as_str() {
        let key = id.as_bytes().to_vec();
        return Some((Bound::Included(key.clone()), Bound::Included(key)));
    }
    let ops = cond.as_object()?;
    if ops.is_empty() {
        return None;
    }
    if let Some(eq) = ops.get("$eq") {
        if ops.len() != 1 {
            return None;
        }
        let key = eq.as_str()?.as_bytes().to_vec();
        return Some((Bound::Included(key.clone()), Bound::Included(key)));
    }

    let mut lower = Bound::Unbounded;
    let mut upper = Bound::Unbounded;
    let mut saw = false;
    for (op, value) in ops {
        let key = value.as_str()?.as_bytes().to_vec();
        match op.as_str() {
            "$gt" => lower = tighter_lower(lower, Bound::Excluded(key)),
            "$gte" => lower = tighter_lower(lower, Bound::Included(key)),
            "$lt" => upper = tighter_upper(upper, Bound::Excluded(key)),
            "$lte" => upper = tighter_upper(upper, Bound::Included(key)),
            _ => return None,
        }
        saw = true;
    }
    saw.then_some((lower, upper))
}

fn tighter_lower(a: Bound<Vec<u8>>, b: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (a, b) {
        (Bound::Unbounded, b) => b,
        (a, Bound::Unbounded) => a,
        (Bound::Included(a), Bound::Included(b)) => Bound::Included(a.max(b)),
        (Bound::Excluded(a), Bound::Excluded(b)) => Bound::Excluded(a.max(b)),
        (Bound::Included(a), Bound::Excluded(b)) => {
            if a > b {
                Bound::Included(a)
            } else {
                Bound::Excluded(b)
            }
        }
        (Bound::Excluded(a), Bound::Included(b)) => {
            if b > a {
                Bound::Included(b)
            } else {
                Bound::Excluded(a)
            }
        }
    }
}

fn tighter_upper(a: Bound<Vec<u8>>, b: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (a, b) {
        (Bound::Unbounded, b) => b,
        (a, Bound::Unbounded) => a,
        (Bound::Included(a), Bound::Included(b)) => Bound::Included(a.min(b)),
        (Bound::Excluded(a), Bound::Excluded(b)) => Bound::Excluded(a.min(b)),
        (Bound::Included(a), Bound::Excluded(b)) => {
            if a < b {
                Bound::Included(a)
            } else {
                Bound::Excluded(b)
            }
        }
        (Bound::Excluded(a), Bound::Included(b)) => {
            if b < a {
                Bound::Included(b)
            } else {
                Bound::Excluded(a)
            }
        }
    }
}

/// Is the sort a single `_id`? Returns `Some(reverse)` for `-1`.
fn id_sort_direction(sort: &[(String, i32)]) -> Option<bool> {
    if sort.len() == 1 && sort[0].0 == "_id" {
        Some(sort[0].1 < 0)
    } else {
        None
    }
}

fn best_array_value(items: &[Value], descending: bool) -> Option<&Value> {
    let mut best: Option<&Value> = None;
    for item in items {
        let candidate = match item {
            Value::Array(inner) => best_array_value(inner, descending),
            _ => Some(item),
        };
        let Some(candidate) = candidate else {
            continue;
        };
        let replace = match best {
            None => true,
            Some(current) => {
                let ord = compare_scalar_values(candidate, current);
                if descending {
                    ord == Ordering::Greater
                } else {
                    ord == Ordering::Less
                }
            }
        };
        if replace {
            best = Some(candidate);
        }
    }
    best
}

fn sort_value(value: Option<&Value>, descending: bool) -> Option<&Value> {
    match value {
        Some(Value::Array(items)) => best_array_value(items, descending),
        other => other,
    }
}

fn compare_scalar_values(a: &Value, b: &Value) -> Ordering {
    if let Some(ord) = numeric::cmp_values(a, b) {
        return ord;
    }
    if let Some(ord) = bson_types::cmp_values(a, b) {
        return ord;
    }
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        _ => date::cmp(a, b).unwrap_or(Ordering::Equal),
    }
}

fn compare_values(a: Option<&Value>, b: Option<&Value>, descending: bool) -> Ordering {
    let a = sort_value(a, descending);
    let b = sort_value(b, descending);
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater, // missing field sorts last
        (Some(_), None) => Ordering::Less,
        (Some(a), Some(b)) => compare_scalar_values(a, b),
    }
}

/// Builds a document from covering-index fields when the filter and projection
/// fit within `inc`. Otherwise returns `None` (the document must be fetched).
fn covering_doc(
    id: &str,
    inc: Option<&Map<String, Value>>,
    filter: &Value,
    projection: &Option<Value>,
) -> Option<Value> {
    let inc = inc?;
    let proj = projection.as_ref()?.as_object()?;
    // Only inclusive projection: an exclusive projection needs the whole document.
    if !proj.iter().any(|(k, v)| k.as_str() != "_id" && truthy(v)) {
        return None;
    }
    for key in proj.keys().filter(|k| k.as_str() != "_id") {
        if key.contains('.') || key.starts_with('$') || !inc.contains_key(key) {
            return None;
        }
    }
    for key in filter.as_object()?.keys() {
        if key != "_id" && (key.contains('.') || !inc.contains_key(key)) {
            return None;
        }
    }
    let mut obj = inc.clone();
    obj.insert("_id".to_string(), Value::String(id.to_string()));
    Some(Value::Object(obj))
}

/// Does index order match the sort? Returns `Some(reverse)` when
/// the sort uses exactly the same fields (in the same order) and
/// all directions are ascending (`false`) or all descending (`true`).
fn index_matches_sort(def: &IndexDef, sort: &[(String, i32)]) -> Option<bool> {
    let fields = def.effective_fields();
    if fields.len() != sort.len() || sort.is_empty() {
        return None;
    }
    if fields.iter().zip(sort).any(|(f, (s, _))| f != s) {
        return None;
    }
    let positive = sort[0].1 >= 0;
    if sort.iter().any(|(_, d)| (*d >= 0) != positive) {
        return None;
    }
    Some(!positive)
}

pub(crate) fn compare_docs(a: &Value, b: &Value, sort: &[(String, i32)]) -> Ordering {
    for (field, dir) in sort {
        let ord = compare_values(value_at(a, field), value_at(b, field), *dir < 0);
        if ord != Ordering::Equal {
            return if *dir >= 0 { ord } else { ord.reverse() };
        }
    }
    Ordering::Equal
}

/// `find` result accumulator: a plain list without sorting, TopK with sorting.
enum Acc<'a> {
    All(Vec<Value>),
    Top(TopK<'a>),
}

impl<'a> Acc<'a> {
    fn new(sorted: bool, sort: &'a [(String, i32)], want: usize) -> Self {
        if sorted {
            Acc::Top(TopK::new(sort, want))
        } else {
            Acc::All(Vec::new())
        }
    }

    fn push(&mut self, doc: Value) {
        match self {
            Acc::All(docs) => docs.push(doc),
            Acc::Top(top) => top.push(doc),
        }
    }

    fn len(&self) -> usize {
        match self {
            Acc::All(docs) => docs.len(),
            Acc::Top(top) => top.heap.len(),
        }
    }

    fn finish(self) -> Vec<Value> {
        match self {
            Acc::All(mut docs) => {
                // Deterministic order of the returned set.
                docs.sort_by(|a, b| {
                    let ka = a.get("_id").and_then(Value::as_str).unwrap_or("");
                    let kb = b.get("_id").and_then(Value::as_str).unwrap_or("");
                    ka.cmp(kb)
                });
                docs
            }
            Acc::Top(top) => top.into_sorted(),
        }
    }
}

/// Document in the TopK heap; `seq` preserves stability for equal keys.
struct TopDoc<'a> {
    doc: Value,
    seq: usize,
    sort: &'a [(String, i32)],
}

impl PartialEq for TopDoc<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for TopDoc<'_> {}

impl PartialOrd for TopDoc<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopDoc<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Larger = worse, so the worst candidate is at the top of the heap.
        match compare_docs(&self.doc, &other.doc, self.sort) {
            Ordering::Equal => self.seq.cmp(&other.seq),
            ord => ord,
        }
    }
}

/// Bounded selection of the `want` best documents without sorting everything.
struct TopK<'a> {
    sort: &'a [(String, i32)],
    want: usize,
    seq: usize,
    heap: BinaryHeap<TopDoc<'a>>,
}

impl<'a> TopK<'a> {
    fn new(sort: &'a [(String, i32)], want: usize) -> Self {
        Self {
            sort,
            want,
            seq: 0,
            heap: BinaryHeap::new(),
        }
    }

    fn push(&mut self, doc: Value) {
        self.seq += 1;
        self.push_seq(doc, self.seq);
    }

    /// Inserts a document with a global sequence number (sort stability).
    fn push_seq(&mut self, doc: Value, seq: usize) {
        if self.want == 0 {
            return;
        }
        let entry = TopDoc {
            doc,
            seq,
            sort: self.sort,
        };
        if self.heap.len() < self.want {
            self.heap.push(entry);
        } else if self
            .heap
            .peek()
            .is_some_and(|worst| entry.cmp(worst) == Ordering::Less)
        {
            self.heap.pop();
            self.heap.push(entry);
        }
    }

    fn into_sorted(self) -> Vec<Value> {
        self.into_sorted_seq()
            .into_iter()
            .map(|(_, doc)| doc)
            .collect()
    }

    fn into_sorted_seq(self) -> Vec<(usize, Value)> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|e| (e.seq, e.doc))
            .collect()
    }
}

fn truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Number(_) => numeric::parse_value(v)
            .is_some_and(|n| !matches!(n.value(), numeric::Numeric::Finite { len: 1, digits, .. } if digits[0] == 0)),
        Value::Null => false,
        _ => {
            if let Some(n) = numeric::parse_value(v) {
                !matches!(n.value(), numeric::Numeric::Finite { len: 1, digits, .. } if digits[0] == 0)
            } else {
                true
            }
        }
    }
}

/// Top-level projection: include or exclude mode (like MongoDB).
fn validate_find_and_modify(opts: &FindAndModifyOptions<'_>) -> Result<()> {
    if opts.remove && opts.update.is_some() {
        return Err(Error::Query(
            "find_and_modify cannot combine `remove` and `update`".into(),
        ));
    }
    if !opts.remove && opts.update.is_none() {
        return Err(Error::Query(
            "find_and_modify requires `update` or `remove`".into(),
        ));
    }
    if opts.remove && opts.upsert {
        return Err(Error::Query(
            "find_and_modify cannot combine `remove` and `upsert`".into(),
        ));
    }
    if opts.remove && opts.new {
        return Err(Error::Query(
            "find_and_modify cannot return a new document when removing".into(),
        ));
    }
    Ok(())
}

fn empty_find_and_modify() -> FindAndModifyOutcome {
    FindAndModifyOutcome {
        value: None,
        matched: 0,
        modified: 0,
        upserted_id: None,
    }
}

fn apply_projection(doc: Value, projection: Option<&Value>) -> Value {
    match projection {
        Some(p) => project(&doc, p),
        None => doc,
    }
}

/// Applies a normal `find` projection. Used by streaming cursor pages, which
/// need the raw `_id` for the next page before hiding it from the caller.
pub fn project_document(doc: &Value, projection: Option<&Value>) -> Value {
    match projection {
        Some(projection) => project(doc, projection),
        None => doc.clone(),
    }
}

fn project(doc: &Value, projection: &Value) -> Value {
    let Some(p) = projection.as_object() else {
        return doc.clone();
    };
    if p.is_empty() {
        return doc.clone();
    }

    let include_mode = p.iter().any(|(k, v)| k.as_str() != "_id" && truthy(v));

    if include_mode {
        let mut out = Map::new();
        // `_id` is included by default unless the projection explicitly sets `_id: 0`.
        let include_id = p.get("_id").map(truthy).unwrap_or(true);
        if include_id {
            if let Some(id) = doc.get("_id") {
                out.insert("_id".to_string(), id.clone());
            }
        }
        for (k, v) in p {
            if k == "_id" || !truthy(v) {
                continue;
            }
            if let Some(val) = doc.get(k) {
                out.insert(k.clone(), val.clone());
            }
        }
        Value::Object(out)
    } else {
        let mut out = doc.as_object().cloned().unwrap_or_default();
        for (k, v) in p {
            if !truthy(v) {
                out.remove(k);
            }
        }
        Value::Object(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;
    use serde_json::json;

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        (dir, store)
    }

    fn history_store(max_versions: usize) -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open_with_history(
            dir.path(),
            HistoryConfig {
                enabled: true,
                max_versions,
            },
        )
        .unwrap();
        (dir, store)
    }

    #[test]
    fn history_records_versions_and_get_version() {
        let (_dir, store) = history_store(50);
        let c = store.collection("users").unwrap();

        let id = c.insert_one(json!({"name": "Ala", "n": 1})).unwrap();
        let first = c.history(&id, None, true).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].op, "insert");
        assert_eq!(first[0].doc.as_ref().unwrap()["n"], 1);
        let first_rev = first[0].rev;

        assert_eq!(
            c.update(&json!({"_id": id}), &json!({"$set": {"n": 2}}), false)
                .unwrap(),
            1
        );
        let versions = c.history(&id, None, true).unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].op, "update");
        assert_eq!(versions[0].doc.as_ref().unwrap()["n"], 2);
        assert!(versions[0].rev > first_rev);

        let old = c.get_version(&id, first_rev).unwrap().unwrap();
        assert_eq!(old.doc.as_ref().unwrap()["n"], 1);

        let metadata = c.history(&id, None, false).unwrap();
        assert_eq!(metadata.len(), 2);
        assert!(metadata.iter().all(|v| v.doc.is_none()));

        assert_eq!(c.delete(&json!({"_id": id}), false).unwrap(), 1);
        let deleted = c.history(&id, None, false).unwrap();
        assert_eq!(deleted[0].op, "delete");
        assert!(deleted[0].deleted);
        assert!(deleted[0].doc.is_none());
        assert_eq!(
            c.get_version(&id, versions[0].rev).unwrap().unwrap().op,
            "update"
        );
    }

    #[test]
    fn history_is_disabled_by_default() {
        let (_dir, store) = store();
        let c = store.collection("users").unwrap();
        let id = c.insert_one(json!({"name": "Ala"})).unwrap();
        assert!(c.history(&id, None, true).unwrap().is_empty());
    }

    #[test]
    fn history_prunes_old_versions() {
        let (_dir, store) = history_store(2);
        let c = store.collection("users").unwrap();
        let id = c.insert_one(json!({"n": 0})).unwrap();
        for n in 1..=4 {
            assert_eq!(
                c.update(&json!({"_id": id}), &json!({"$set": {"n": n}}), false)
                    .unwrap(),
                1
            );
        }
        assert_eq!(c.history(&id, None, false).unwrap().len(), 5);

        assert_eq!(store.prune_history(100).unwrap(), 3);
        let kept = c.history(&id, None, true).unwrap();
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].doc.as_ref().unwrap()["n"], 4);
        assert_eq!(kept[1].doc.as_ref().unwrap()["n"], 3);
    }

    #[test]
    fn history_prunes_all_documents_across_passes() {
        let (_dir, store) = history_store(2);
        let c = store.collection("users").unwrap();
        let ids: Vec<String> = (0..3)
            .map(|n| c.insert_one(json!({"n": n, "v": 0})).unwrap())
            .collect();
        for id in &ids {
            for n in 1..=2 {
                c.update(&json!({"_id": id}), &json!({"$set": {"v": n}}), false)
                    .unwrap();
            }
        }

        // The deletion limit forces several bounded maintenance passes.
        while store.prune_history(1).unwrap() > 0 {}
        for id in &ids {
            let kept = c.history(id, None, true).unwrap();
            assert_eq!(kept.len(), 2);
            assert_eq!(kept[0].doc.as_ref().unwrap()["v"], 2);
            assert_eq!(kept[1].doc.as_ref().unwrap()["v"], 1);
        }
    }

    #[test]
    fn insert_find_update_delete() {
        let (_dir, store) = store();
        let c = store.collection("users").unwrap();

        let id = c.insert_one(json!({"name": "Ala", "age": 30})).unwrap();
        c.insert_one(json!({"name": "Bob", "age": 17})).unwrap();
        assert!(c.insert_one(json!({"_id": id, "name": "dup"})).is_err());

        let q = FindQuery {
            filter: json!({"age": {"$gte": 18}}),
            ..Default::default()
        };
        let found = c.find(&q).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0]["name"], "Ala");

        let n = c
            .update(&json!({"name": "Ala"}), &json!({"$inc": {"age": 1}}), false)
            .unwrap();
        assert_eq!(n, 1);
        let ala = c.find_one(&json!({"name": "Ala"})).unwrap().unwrap();
        assert_eq!(ala["age"], 31);

        let d = c.delete(&json!({"age": {"$lt": 18}}), true).unwrap();
        assert_eq!(d, 1);
        assert_eq!(c.count(&json!({})).unwrap(), 1);
    }

    #[test]
    fn raw_prefilter_avoids_false_negatives() {
        let filter = json!({"name": "a\"b\n", "active": true});
        let raw = raw_prefilter(&filter).unwrap();
        let matching = json!({"_id": "1", "name": "a\"b\n", "active": true, "junk": "x"});
        let bytes = serde_json::to_vec(&matching).unwrap();
        assert!(raw.matches(&bytes));

        let wrong_bool = json!({"_id": "1", "name": "a\"b\n", "active": false});
        assert!(!raw.matches(&serde_json::to_vec(&wrong_bool).unwrap()));

        let wrong_string = json!({"_id": "1", "name": "other", "active": true});
        assert!(!raw.matches(&serde_json::to_vec(&wrong_string).unwrap()));

        // `$or` cannot be used as a prefilter.
        assert!(raw_prefilter(&json!({"$or": [{"name": "a"}, {"name": "b"}]})).is_none());
        // A number on its own requires the parser (e.g. 1 vs 1.0).
        assert!(raw_prefilter(&json!({"n": 1})).is_none());
    }

    #[test]
    fn raw_prefilter_preserves_string_and_number_semantics() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        c.insert_one(json!({"_id": "a", "name": "a\"b\n", "n": 1, "tags": ["x"]}))
            .unwrap();
        c.insert_one(json!({"_id": "b", "name": "other", "n": 2, "tags": ["y"]}))
            .unwrap();

        assert_eq!(
            c.find_one(&json!({"name": "a\"b\n"})).unwrap().unwrap()["_id"],
            "a"
        );
        assert_eq!(
            c.find_one(&json!({"name": {"$eq": "other"}}))
                .unwrap()
                .unwrap()["_id"],
            "b"
        );
        assert_eq!(
            c.find_one(&json!({"tags": "x"})).unwrap().unwrap()["_id"],
            "a"
        );
        // The prefilter uses only the string; the number is still compared by the matcher.
        assert_eq!(
            c.find_one(&json!({"name": "a\"b\n", "n": 1.0}))
                .unwrap()
                .unwrap()["_id"],
            "a"
        );
        assert!(c
            .find_one(&json!({"name": "a\"b\n", "n": 2}))
            .unwrap()
            .is_none());
    }

    #[test]
    fn parallel_sorted_scan_matches_serial() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..1000 {
            c.insert_one(json!({
                "_id": format!("id{i:04}"),
                "g": i % 7,
                "n": i,
                "junk": "x",
            }))
            .unwrap();
        }
        let q = FindQuery {
            filter: json!({"n": {"$gte": 100}}),
            projection: Some(json!({"g": 1, "n": 1, "_id": 0})),
            sort: vec![("g".to_string(), 1), ("n".to_string(), -1)],
            skip: 7,
            limit: Some(31),
        };
        // `find` takes the parallel path (1000 entries > test threshold).
        let parallel = c.find(&q).unwrap();
        // `find_in` is the same scan but single-threaded.
        let rtxn = store.read_txn().unwrap();
        let serial = c.find_in(&rtxn, &q).unwrap();
        rtxn.commit().unwrap();
        assert_eq!(parallel, serial);
        assert_eq!(parallel.len(), 31);
        for pair in parallel.windows(2) {
            let a = (
                pair[0]["g"].as_i64().unwrap(),
                -pair[0]["n"].as_i64().unwrap(),
            );
            let b = (
                pair[1]["g"].as_i64().unwrap(),
                -pair[1]["n"].as_i64().unwrap(),
            );
            assert!(a <= b, "niestabilna kolejność: {a:?} > {b:?}");
        }
    }

    #[test]
    fn parallel_unsorted_scan_matches_serial() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..1000 {
            c.insert_one(json!({
                "_id": format!("id{i:04}"),
                "g": i % 7,
                "n": i,
                "junk": "x",
            }))
            .unwrap();
        }
        let q = FindQuery {
            filter: json!({"n": {"$gte": 100}}),
            projection: Some(json!({"g": 1, "n": 1, "_id": 0})),
            skip: 7,
            limit: Some(31),
            ..Default::default()
        };
        // `find` takes the parallel scan without sorting.
        let parallel = c.find(&q).unwrap();
        let rtxn = store.read_txn().unwrap();
        let serial = c.find_in(&rtxn, &q).unwrap();
        rtxn.commit().unwrap();
        assert_eq!(parallel, serial);
        assert_eq!(parallel.len(), 31);
    }

    #[test]
    fn id_range_uses_primary_key_order() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..20 {
            c.insert_one(json!({"_id": format!("id{i:02}"), "n": i}))
                .unwrap();
        }

        // `_id` range plus an extra condition; natural `_id` order.
        let q = FindQuery {
            filter: json!({"_id": {"$gte": "id05", "$lt": "id09"}, "n": {"$gte": 6}}),
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        let ids: Vec<_> = got.iter().map(|d| d["_id"].as_str().unwrap()).collect();
        assert_eq!(ids, ["id06", "id07", "id08"]);

        // Descending `_id` sorting still uses the range.
        let q = FindQuery {
            filter: json!({"_id": {"$gt": "id05", "$lte": "id09"}}),
            sort: vec![("_id".to_string(), -1)],
            limit: Some(2),
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        let ids: Vec<_> = got.iter().map(|d| d["_id"].as_str().unwrap()).collect();
        assert_eq!(ids, ["id09", "id08"]);

        // An exact `_id` with an extra condition does not require a full scan.
        assert!(c
            .find_one(&json!({"_id": "id07", "n": 7}))
            .unwrap()
            .is_some());
        assert!(c
            .find_one(&json!({"_id": "id07", "n": 8}))
            .unwrap()
            .is_none());

        assert_eq!(
            c.count(&json!({"_id": {"$gte": "id05", "$lt": "id09"}}))
                .unwrap(),
            4
        );
        assert_eq!(
            c.count(&json!({"_id": {"$gte": "id05", "$lt": "id09"}, "n": {"$gte": 7}}))
                .unwrap(),
            2
        );

        // An empty or reversed range must not fall back to a full scan.
        assert!(c
            .find(&FindQuery {
                filter: json!({"_id": {"$gt": "id09", "$lt": "id05"}}),
                limit: Some(10),
                ..Default::default()
            })
            .unwrap()
            .is_empty());
        assert_eq!(
            c.count(&json!({"_id": {"$gt": "id09", "$lt": "id05"}}))
                .unwrap(),
            0
        );
    }

    #[test]
    fn id_range_update_delete() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..20 {
            c.insert_one(json!({"_id": format!("id{i:02}"), "n": i}))
                .unwrap();
        }
        let n = c
            .update(
                &json!({"_id": {"$gte": "id05", "$lt": "id09"}}),
                &json!({"$set": {"flag": true}}),
                true,
            )
            .unwrap();
        assert_eq!(n, 4);
        assert_eq!(c.count(&json!({"flag": true})).unwrap(), 4);

        let n = c
            .delete(&json!({"_id": {"$gte": "id06", "$lte": "id08"}}), true)
            .unwrap();
        assert_eq!(n, 3);
        assert_eq!(c.count(&json!({})).unwrap(), 17);
    }

    #[test]
    fn sort_projection_pagination() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..5 {
            c.insert_one(json!({"i": i, "even": i % 2 == 0, "junk": "x"}))
                .unwrap();
        }
        let q = FindQuery {
            filter: json!({}),
            projection: Some(json!({"i": 1, "_id": 0})),
            sort: vec![("i".to_string(), -1)],
            skip: 1,
            limit: Some(2),
        };
        let got = c.find(&q).unwrap();
        assert_eq!(got, vec![json!({"i": 3}), json!({"i": 2})]);
    }

    #[test]
    fn secondary_index_equality_and_range() {
        let (_dir, store) = store();
        let c = store.collection("users").unwrap();
        for i in 0..10 {
            c.insert_one(json!({"name": format!("u{i}"), "age": i}))
                .unwrap();
        }
        c.create_index("age", false).unwrap();
        assert_eq!(c.list_indexes().unwrap(), vec![("age".to_string(), false)]);

        let eq = c.find_one(&json!({"age": 5})).unwrap().unwrap();
        assert_eq!(eq["name"], "u5");

        let q = FindQuery {
            filter: json!({"age": {"$gte": 7}}),
            sort: vec![("age".to_string(), 1)],
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0]["age"], 7);
        assert_eq!(got[2]["age"], 9);
    }

    #[test]
    fn sort_only_index_or_union_and_type_plans() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..10 {
            c.insert_one(json!({
                "_id": format!("d{i}"),
                "n": i,
                "name": format!("u{i}"),
            }))
            .unwrap();
        }
        c.create_index("n", false).unwrap();
        c.create_index("name", false).unwrap();

        let q = FindQuery {
            filter: json!({}),
            sort: vec![("n".into(), -1)],
            limit: Some(3),
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        let ns: Vec<_> = got.iter().map(|d| d["n"].as_i64()).collect();
        assert_eq!(ns, vec![Some(9), Some(8), Some(7)]);
        let plan = c.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_scan");
        assert_eq!(plan["sort_covered"], true);

        // A document missing the sort field is not in the index, so the planner
        // must fall back to a scan and still place it last.
        c.insert_one(json!({"_id": "missing"})).unwrap();
        let q = FindQuery {
            filter: json!({}),
            sort: vec![("n".into(), 1)],
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        assert_eq!(got.len(), 11);
        assert_eq!(got[10]["_id"], "missing");
        let plan = c.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "collection_scan");

        let q = FindQuery {
            filter: json!({"$or": [{"n": 3}, {"name": "u7"}]}),
            limit: Some(10),
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        assert_eq!(got.len(), 2);
        let plan = c.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_union");

        let q = FindQuery {
            filter: json!({"n": {"$type": "int"}}),
            limit: Some(3),
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 3);
        let plan = c.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_scan");
    }

    #[test]
    fn multikey_indexes_filter_unique_sort_and_verify() {
        let (_dir, store) = store();
        let c = store.collection("mk").unwrap();
        for (id, tags) in [
            ("a", json!(["b", "c"])),
            ("b", json!(["a", "z"])),
            ("c", json!(["b", "b"])),
            ("d", json!(["m"])),
        ] {
            c.insert_one(json!({"_id": id, "tags": tags})).unwrap();
        }
        c.create_index("tags", false).unwrap();
        let def = c.list_index_defs().unwrap().into_iter().next().unwrap();
        assert_eq!(def.multikey_fields, vec!["tags".to_string()]);
        assert!(def.multikey_all_docs);

        let q = FindQuery {
            filter: json!({"tags": "b"}),
            ..Default::default()
        };
        let mut ids: Vec<_> = c
            .find(&q)
            .unwrap()
            .into_iter()
            .map(|d| d["_id"].as_str().unwrap().to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["a", "c"]);
        assert_eq!(c.count(&json!({"tags": "b"})).unwrap(), 2);
        let plan = c.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_scan");
        assert_eq!(plan["index"]["multikey"], json!(["tags"]));

        let q = FindQuery {
            filter: json!({"tags": {"$all": ["b", "c"]}}),
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 1);
        assert_eq!(c.count(&json!({"tags": {"$all": ["b", "c"]}})).unwrap(), 1);
        let plan = c.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_scan");

        let q = FindQuery {
            filter: json!({}),
            sort: vec![("tags".into(), 1)],
            ..Default::default()
        };
        let ids: Vec<_> = c
            .find(&q)
            .unwrap()
            .into_iter()
            .map(|d| d["_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, vec!["b", "a", "c", "d"]);
        let plan = c.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_scan");
        assert_eq!(plan["sort_covered"], true);

        let q = FindQuery {
            filter: json!({}),
            sort: vec![("tags".into(), -1)],
            skip: 1,
            limit: Some(2),
            ..Default::default()
        };
        let ids: Vec<_> = c
            .find(&q)
            .unwrap()
            .into_iter()
            .map(|d| d["_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, vec!["d", "a"]);

        // A document without the indexed field cannot use index sort order.
        c.insert_one(json!({"_id": "e"})).unwrap();
        let q = FindQuery {
            filter: json!({}),
            sort: vec![("tags".into(), 1)],
            ..Default::default()
        };
        let ids: Vec<_> = c
            .find(&q)
            .unwrap()
            .into_iter()
            .map(|d| d["_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, vec!["b", "a", "c", "d", "e"]);
        {
            let rtxn = c.env.read_txn().unwrap();
            c.verify_in(&rtxn).unwrap();
        }

        // Updating an array replaces all entries for the document.
        c.update(
            &json!({"_id": "a"}),
            &json!({"$set": {"tags": ["q"]}}),
            false,
        )
        .unwrap();
        assert_eq!(c.count(&json!({"tags": "c"})).unwrap(), 0);
        assert_eq!(c.count(&json!({"tags": "q"})).unwrap(), 1);
        {
            let rtxn = c.env.read_txn().unwrap();
            c.verify_in(&rtxn).unwrap();
        }

        // Unique multikey indexes allow duplicate elements in one document,
        // but reject the same value in a different document.
        let u = store.collection("mkuniq").unwrap();
        u.insert_one(json!({"_id": "u1", "tags": ["x", "x"]}))
            .unwrap();
        u.insert_one(json!({"_id": "u2", "tags": ["y"]})).unwrap();
        u.create_index("tags", true).unwrap();
        assert!(u
            .update(
                &json!({"_id": "u2"}),
                &json!({"$set": {"tags": ["x"]}}),
                false
            )
            .is_err());
        assert!(u
            .insert_one(json!({"_id": "u3", "tags": ["y", "y"]}))
            .is_err());
        u.insert_one(json!({"_id": "u3", "tags": ["z", "z"]}))
            .unwrap();
        u.update(
            &json!({"_id": "u2"}),
            &json!({"$set": {"tags": ["y", "w"]}}),
            false,
        )
        .unwrap();
        {
            let rtxn = u.env.read_txn().unwrap();
            u.verify_in(&rtxn).unwrap();
        }

        // An index created before any writes becomes multikey when arrays are
        // inserted and remains usable for sort order.
        let early = store.collection("mkearly").unwrap();
        early.create_index("tags", false).unwrap();
        early
            .insert_one(json!({"_id": "x", "tags": ["b", "d"]}))
            .unwrap();
        early
            .insert_one(json!({"_id": "y", "tags": ["a", "c"]}))
            .unwrap();
        let q = FindQuery {
            filter: json!({}),
            sort: vec![("tags".into(), 1)],
            ..Default::default()
        };
        let ids: Vec<_> = early
            .find(&q)
            .unwrap()
            .into_iter()
            .map(|d| d["_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, vec!["y", "x"]);
        let plan = early.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_scan");

        let d = store.collection("mkdup").unwrap();
        d.insert_one(json!({"_id": "a", "tags": ["x"]})).unwrap();
        d.insert_one(json!({"_id": "b", "tags": ["x", "y"]}))
            .unwrap();
        assert!(d.create_index("tags", true).is_err());
    }

    #[test]
    fn multikey_elem_match_uses_nested_index() {
        let (_dir, store) = store();
        let c = store.collection("mk_elem").unwrap();
        c.insert_one(json!({"_id": "e1", "items": [{"price": 5}]}))
            .unwrap();
        c.insert_one(json!({"_id": "e2", "items": [{"price": 15}]}))
            .unwrap();
        c.insert_one(json!({"_id": "e3", "items": [{"price": 20}, {"price": 1}]}))
            .unwrap();
        c.create_index("items.price", false).unwrap();

        let q = FindQuery {
            filter: json!({"items": {"$elemMatch": {"price": {"$gte": 10, "$lt": 18}}}}),
            ..Default::default()
        };
        let ids: Vec<_> = c
            .find(&q)
            .unwrap()
            .into_iter()
            .map(|d| d["_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, vec!["e2"]);
        let plan = c.explain(&q).unwrap();
        assert_eq!(plan["access_path"], "index_scan");
        assert_eq!(plan["index"]["multikey"], json!(["items.price"]));

        let q = FindQuery {
            filter: json!({"items": {"$elemMatch": {"price": 1}}}),
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 1);
        assert_eq!(
            c.count(&json!({"items": {"$elemMatch": {"price": 1}}}))
                .unwrap(),
            1
        );
        {
            let rtxn = c.env.read_txn().unwrap();
            c.verify_in(&rtxn).unwrap();
        }
    }

    #[test]
    fn compound_index_filters_sorts_and_maintains() {
        let (_dir, store) = store();
        let c = store.collection("events").unwrap();
        for g in 0..3 {
            for age in 0..10 {
                c.insert_one(json!({
                    "_id": format!("g{g}a{age}"),
                    "g": g,
                    "age": age,
                    "name": format!("{g}-{age}"),
                }))
                .unwrap();
            }
        }
        c.create_index_fields(&["g".to_string(), "age".to_string()], false, &[])
            .unwrap();
        assert!(c
            .list_indexes()
            .unwrap()
            .contains(&("g+age".to_string(), false)));

        // Range on the last field + sorting along the index.
        let q = FindQuery {
            filter: json!({"g": 1, "age": {"$gte": 4, "$lt": 8}}),
            sort: vec![("age".to_string(), 1)],
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        assert_eq!(got.len(), 4);
        assert_eq!(got[0]["age"], 4);
        assert_eq!(got[3]["age"], 7);

        // Equality on all fields + count through the index.
        let q = FindQuery {
            filter: json!({"g": 2, "age": 3}),
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 1);
        assert_eq!(
            c.count(&json!({"g": 1, "age": {"$gte": 4, "$lt": 8}}))
                .unwrap(),
            4
        );
        // An extra condition cannot use the index alone for count.
        assert_eq!(
            c.count(&json!({"g": 1, "age": {"$gte": 4, "$lt": 8}, "name": "1-4"}))
                .unwrap(),
            1
        );

        // Update moves the entry in the index.
        c.update(
            &json!({"_id": "g1a5"}),
            &json!({"$set": {"age": 99}}),
            false,
        )
        .unwrap();
        assert_eq!(
            c.count(&json!({"g": 1, "age": {"$gte": 4, "$lt": 8}}))
                .unwrap(),
            3
        );
        assert!(c.find_one(&json!({"g": 1, "age": 99})).unwrap().is_some());

        // Delete clears the entry.
        c.delete(&json!({"_id": "g2a3"}), false).unwrap();
        assert!(c.find_one(&json!({"g": 2, "age": 3})).unwrap().is_none());
    }

    #[test]
    fn covering_index_serves_projection_without_document_fetch() {
        let (_dir, store) = store();
        let c = store.collection("cover").unwrap();
        c.create_index_fields(&["g".to_string()], false, &["v".to_string()])
            .unwrap();
        c.insert_one(json!({"_id": "a", "g": 1, "v": 42, "other": "x"}))
            .unwrap();
        c.update(&json!({"_id": "a"}), &json!({"$set": {"v": 43}}), false)
            .unwrap();
        // An exclusive projection cannot be served from the index alone.
        let excl = FindQuery {
            filter: json!({"g": 1}),
            projection: Some(json!({"other": 0})),
            sort: vec![("g".to_string(), 1)],
            ..Default::default()
        };
        let got = c.find(&excl).unwrap();
        assert_eq!(got[0]["v"], 43);
        assert!(got[0].get("other").is_none());
        // Delete the document but leave the index entry: coverage must not
        // reach the collection, so the result proves the index value was used.
        let mut wtxn = c.write_txn().unwrap();
        c.db.delete(&mut wtxn, b"a").unwrap();
        wtxn.commit().unwrap();

        let q = FindQuery {
            filter: json!({"g": 1}),
            projection: Some(json!({"g": 1, "v": 1, "_id": 0})),
            sort: vec![("g".to_string(), 1)],
            ..Default::default()
        };
        let got = c.find(&q).unwrap();
        assert_eq!(got, vec![json!({"g": 1, "v": 43})]);
    }

    #[test]
    fn update_upsert_and_array_operators() {
        let (_dir, store) = store();
        let c = store.collection("cfg").unwrap();
        let out = c
            .update_opts(
                &json!({"tenant": "a", "key": "mail"}),
                &json!({
                    "$set": {"host": "smtp"},
                    "$setOnInsert": {"created": true},
                    "$addToSet": {"tags": {"$each": ["x", "x", "y"]}},
                    "$push": {"ports": {"$each": [25, 587]}},
                }),
                false,
                true,
            )
            .unwrap();
        assert_eq!(out.matched, 0);
        assert_eq!(out.modified, 0);
        let id = out.upserted_id.expect("upserted id");
        let doc = c.find_one(&json!({"_id": id})).unwrap().unwrap();
        assert_eq!(doc["tenant"], "a");
        assert_eq!(doc["key"], "mail");
        assert_eq!(doc["host"], "smtp");
        assert_eq!(doc["created"], true);
        assert_eq!(doc["tags"], json!(["x", "y"]));
        assert_eq!(doc["ports"], json!([25, 587]));

        // The second upsert matches an existing document: no new `_id`,
        // `$setOnInsert` does not apply.
        let out = c
            .update_opts(
                &json!({"tenant": "a", "key": "mail"}),
                &json!({
                    "$set": {"host": "smtp2"},
                    "$setOnInsert": {"created": false},
                    "$addToSet": {"tags": "z"},
                }),
                false,
                true,
            )
            .unwrap();
        assert_eq!(out.matched, 1);
        assert_eq!(out.modified, 1);
        assert!(out.upserted_id.is_none());
        let doc = c.find_one(&json!({"_id": id})).unwrap().unwrap();
        assert_eq!(doc["host"], "smtp2");
        assert_eq!(doc["created"], true);
        assert_eq!(doc["tags"], json!(["x", "y", "z"]));

        // Replacement upsert adds equalities from the filter.
        let out = c
            .update_opts(
                &json!({"tenant": "b", "key": "db"}),
                &json!({"value": 1}),
                false,
                true,
            )
            .unwrap();
        let doc = c
            .find_one(&json!({"_id": out.upserted_id.unwrap()}))
            .unwrap()
            .unwrap();
        assert_eq!(doc["tenant"], "b");
        assert_eq!(doc["key"], "db");
        assert_eq!(doc["value"], 1);
    }

    #[test]
    fn upsert_without_match_and_no_upsert_is_noop() {
        let (_dir, store) = store();
        let c = store.collection("cfg2").unwrap();
        let out = c
            .update_opts(&json!({"a": 1}), &json!({"$set": {"b": 2}}), false, false)
            .unwrap();
        assert_eq!(
            out,
            UpdateOutcome {
                matched: 0,
                modified: 0,
                upserted_id: None
            }
        );
        assert_eq!(c.count(&json!({})).unwrap(), 0);
    }

    #[test]
    fn compound_unique_index_checks_full_key() {
        let (_dir, store) = store();
        let c = store.collection("u").unwrap();
        c.create_index_fields(&["a".to_string(), "b".to_string()], true, &[])
            .unwrap();
        c.insert_one(json!({"a": 1, "b": 1})).unwrap();
        c.insert_one(json!({"a": 1, "b": 2})).unwrap();
        let err = c.insert_one(json!({"a": 1, "b": 1})).unwrap_err();
        assert!(matches!(err, Error::DuplicateKey(_)));
    }

    #[test]
    fn index_maintained_on_update_and_delete() {
        let (_dir, store) = store();
        let c = store.collection("users").unwrap();
        let id = c.insert_one(json!({"name": "Ala", "age": 30})).unwrap();
        c.create_index("age", true).unwrap();

        // Changing an indexed field updates the entry.
        c.update(&json!({"_id": id}), &json!({"$set": {"age": 31}}), false)
            .unwrap();
        assert!(c.find_one(&json!({"age": 30})).unwrap().is_none());
        assert_eq!(
            c.find_one(&json!({"age": 31})).unwrap().unwrap()["name"],
            "Ala"
        );

        // Uniqueness.
        c.insert_one(json!({"name": "Bob", "age": 40})).unwrap();
        assert!(c.insert_one(json!({"name": "Cezary", "age": 40})).is_err());

        // Delete removes the entry from the index.
        c.delete(&json!({"age": 40}), true).unwrap();
        assert!(c.find_one(&json!({"age": 40})).unwrap().is_none());
    }

    #[test]
    fn rejects_oversized_document() {
        let (_dir, store) = store();
        let c = store.collection("big").unwrap();
        let big = "x".repeat(crate::MAX_DOCUMENT_BYTES + 10);
        let err = c.insert_one(json!({ "payload": big })).unwrap_err();
        assert!(matches!(err, Error::DocumentTooLarge(_, _)));
    }

    #[test]
    fn unique_index_backfill_detects_duplicates() {
        let (_dir, store) = store();
        let c = store.collection("users").unwrap();
        c.insert_one(json!({"name": "Ala"})).unwrap();
        c.insert_one(json!({"name": "Ala"})).unwrap();
        assert!(c.create_index("name", true).is_err());
    }

    #[test]
    fn text_search_ranks_and_stays_in_sync() {
        let (_dir, store) = store();
        let c = store.collection("docs").unwrap();
        c.insert_one(json!({"title": "rust database engine", "tag": "db"}))
            .unwrap();
        c.insert_one(json!({"title": "rust rust rust async runtime", "tag": "db"}))
            .unwrap();
        c.insert_one(json!({"title": "python web framework", "tag": "web"}))
            .unwrap();
        c.create_text_index("title").unwrap();
        assert_eq!(c.list_text_indexes().unwrap(), vec!["title".to_string()]);

        let hits = c.search("title", "rust", &json!({}), 0, Some(10)).unwrap();
        assert_eq!(hits.len(), 2);
        // A document with higher `rust` frequency has a higher score.
        assert!(hits[0]["title"].as_str().unwrap().contains("rust rust"));
        assert!(hits[0]["_score"].as_f64().unwrap() > hits[1]["_score"].as_f64().unwrap());

        // The residual filter works.
        let only_db = c
            .search("title", "rust", &json!({"tag": "db"}), 0, Some(10))
            .unwrap();
        assert_eq!(only_db.len(), 2);

        // Update and delete keep the index in sync.
        c.update(
            &json!({"title": "python web framework"}),
            &json!({"$set": {"title": "rust web framework"}}),
            false,
        )
        .unwrap();
        let hits = c.search("title", "rust", &json!({}), 0, Some(10)).unwrap();
        assert_eq!(hits.len(), 3);

        c.delete(&json!({"title": "rust database engine"}), false)
            .unwrap();
        let hits = c
            .search("title", "database", &json!({}), 0, Some(10))
            .unwrap();
        assert!(hits.is_empty());

        c.drop_text_index("title").unwrap();
        assert!(c.search("title", "rust", &json!({}), 0, None).is_err());
    }

    #[test]
    fn fuzzy_search_finds_typos_and_orders_by_distance() {
        let (_dir, store) = store();
        let c = store.collection("people").unwrap();
        c.insert_one(json!({"name": "Aleksandra"})).unwrap();
        c.insert_one(json!({"name": "Aleksander"})).unwrap();
        c.insert_one(json!({"name": "Bartosz"})).unwrap();
        c.create_fuzzy_index("name").unwrap();

        let hits = c
            .fuzzy_search("name", "aleksander", 2, &json!({}), Some(10))
            .unwrap();
        assert!(hits.len() >= 2);
        assert_eq!(hits[0]["name"], "Aleksander");
        assert_eq!(hits[0]["_distance"], 0);

        let hits = c
            .fuzzy_search("name", "aleksndra", 2, &json!({}), Some(10))
            .unwrap();
        assert!(hits.iter().any(|d| d["name"] == "Aleksandra"));

        let hits = c
            .fuzzy_search("name", "bartek", 1, &json!({}), Some(10))
            .unwrap();
        assert!(hits.is_empty());

        c.update(
            &json!({"name": "Bartosz"}),
            &json!({"$set": {"name": "Bartlomiej"}}),
            false,
        )
        .unwrap();
        let hits = c
            .fuzzy_search("name", "bartlomiej", 2, &json!({}), Some(10))
            .unwrap();
        assert_eq!(hits.len(), 1);

        c.delete(&json!({"name": "Aleksander"}), false).unwrap();
        let hits = c
            .fuzzy_search("name", "aleksander", 1, &json!({}), Some(10))
            .unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn count_fast_paths_are_correct() {
        let (_dir, store) = store();
        let c = store.collection("nums").unwrap();
        c.insert_one(json!({"v": 1})).unwrap();
        c.insert_one(json!({"v": 2})).unwrap();
        c.insert_one(json!({})).unwrap(); // missing field
        c.insert_one(json!({"v": null})).unwrap();
        c.create_index("v", false).unwrap();

        assert_eq!(c.count(&json!({})).unwrap(), 4);
        assert_eq!(c.count(&Value::Null).unwrap(), 4);
        assert_eq!(c.count(&json!({"v": 1})).unwrap(), 1);
        assert_eq!(c.count(&json!({"v": {"$gte": 2}})).unwrap(), 1);
        // `null` also matches a missing field -> the index must not be used.
        assert_eq!(c.count(&json!({"v": null})).unwrap(), 2);
        assert_eq!(c.count(&json!({"v": {"$eq": null}})).unwrap(), 2);

        let id = c.find_one(&json!({"v": 1})).unwrap().unwrap()["_id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(c.count(&json!({"_id": id})).unwrap(), 1);
        assert_eq!(c.count(&json!({"_id": "missing"})).unwrap(), 0);
    }

    #[test]
    fn find_limit_early_exit_and_sorted_queries() {
        let (_dir, store) = store();
        let c = store.collection("items").unwrap();
        for i in 0..50 {
            c.insert_one(json!({"n": i, "grp": i % 5})).unwrap();
        }
        c.create_index("grp", false).unwrap();

        let q = FindQuery {
            filter: json!({"grp": 1}),
            limit: Some(3),
            skip: 2,
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 3);

        let q = FindQuery {
            filter: json!({}),
            sort: vec![("n".into(), 1)],
            limit: Some(3),
            ..Default::default()
        };
        let ns: Vec<_> = c
            .find(&q)
            .unwrap()
            .iter()
            .map(|d| d["n"].as_i64())
            .collect();
        assert_eq!(ns, vec![Some(0), Some(1), Some(2)]);

        let q = FindQuery {
            filter: json!({}),
            sort: vec![("n".into(), -1)],
            skip: 10,
            limit: Some(3),
            ..Default::default()
        };
        let ns: Vec<_> = c
            .find(&q)
            .unwrap()
            .iter()
            .map(|d| d["n"].as_i64())
            .collect();
        assert_eq!(ns, vec![Some(39), Some(38), Some(37)]);

        // `find_one` on an indexed field returns exactly one document.
        let one = c.find_one(&json!({"grp": 1})).unwrap().unwrap();
        assert_eq!(one["grp"], 1);
    }

    #[test]
    fn skip_and_sorted_topk_match_full_sort() {
        let (_dir, store) = store();
        let c = store.collection("skip").unwrap();
        for i in 0..100 {
            c.insert_one(json!({"n": i, "grp": i % 3})).unwrap();
        }

        // `skip` without a filter: natural `_id` order, without collecting skipped documents.
        let q = FindQuery {
            filter: json!({}),
            skip: 90,
            limit: Some(5),
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 5);

        // `skip` with a non-match-all filter: only matching documents are skipped.
        let q = FindQuery {
            filter: json!({"grp": 0}),
            skip: 20,
            limit: Some(5),
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 5);

        // TopK with sorting, skip, and many equal keys.
        let all = c
            .find(&FindQuery {
                filter: json!({}),
                ..Default::default()
            })
            .unwrap();
        let mut expected_docs = all.clone();
        expected_docs.sort_by_key(|d| d["grp"].as_i64().unwrap());
        let expected: Vec<_> = expected_docs
            .iter()
            .skip(50)
            .take(7)
            .map(|d| d["_id"].clone())
            .collect();

        let q = FindQuery {
            filter: json!({}),
            sort: vec![("grp".into(), 1)],
            skip: 50,
            limit: Some(7),
            ..Default::default()
        };
        let got: Vec<_> = c
            .find(&q)
            .unwrap()
            .iter()
            .map(|d| d["_id"].clone())
            .collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn null_filter_with_index_falls_back_to_scan() {
        let (_dir, store) = store();
        let c = store.collection("nulls").unwrap();
        c.insert_one(json!({"a": null, "tag": "present"})).unwrap();
        c.insert_one(json!({"b": 1, "tag": "missing"})).unwrap();
        c.create_index("a", false).unwrap();

        // A missing field also matches `null`; the index does not contain missing fields.
        assert_eq!(c.count(&json!({"a": null})).unwrap(), 2);
        let q = FindQuery {
            filter: json!({"a": null}),
            ..Default::default()
        };
        assert_eq!(c.find(&q).unwrap().len(), 2);
        assert_eq!(c.count(&json!({"a": {"$eq": null}})).unwrap(), 2);
    }

    #[test]
    fn range_index_respects_types() {
        let (_dir, store) = store();
        let c = store.collection("typed").unwrap();
        for v in [
            json!(1),
            json!(3),
            json!(10),
            json!("a"),
            json!("z"),
            json!(true),
        ] {
            c.insert_one(json!({"n": v})).unwrap();
        }
        c.create_index("n", false).unwrap();

        // The matcher does not compare different types: only numbers > 2.
        assert_eq!(c.count(&json!({"n": {"$gt": 2}})).unwrap(), 2);
        assert_eq!(c.count(&json!({"n": {"$lt": 2}})).unwrap(), 1);
        let mut ns: Vec<_> = c
            .find(&FindQuery {
                filter: json!({"n": {"$gt": 2}}),
                limit: Some(10),
                ..Default::default()
            })
            .unwrap()
            .iter()
            .map(|d| d["n"].as_i64().unwrap())
            .collect();
        ns.sort();
        assert_eq!(ns, vec![3, 10]);

        // Only strings < "m".
        assert_eq!(c.count(&json!({"n": {"$lt": "m"}})).unwrap(), 1);
    }

    #[test]
    fn combined_range_index_and_count() {
        let (_dir, store) = store();
        let c = store.collection("ranges").unwrap();
        for i in 0..100 {
            c.insert_one(json!({"n": i, "grp": i % 10})).unwrap();
        }
        c.create_index("n", false).unwrap();

        let q = FindQuery {
            filter: json!({"n": {"$gte": 30, "$lt": 35}}),
            limit: Some(100),
            ..Default::default()
        };
        let mut ns: Vec<_> = c
            .find(&q)
            .unwrap()
            .iter()
            .map(|d| d["n"].as_i64().unwrap())
            .collect();
        ns.sort();
        assert_eq!(ns, vec![30, 31, 32, 33, 34]);
        assert_eq!(c.count(&json!({"n": {"$gte": 30, "$lt": 35}})).unwrap(), 5);
        assert_eq!(c.count(&json!({"n": {"$gt": 30, "$lte": 35}})).unwrap(), 5);

        // Range + sorting + skip/limit.
        let q = FindQuery {
            filter: json!({"n": {"$gte": 30, "$lt": 35}}),
            sort: vec![("n".into(), -1)],
            skip: 1,
            limit: Some(2),
            ..Default::default()
        };
        let ns: Vec<_> = c
            .find(&q)
            .unwrap()
            .iter()
            .map(|d| d["n"].as_i64().unwrap())
            .collect();
        assert_eq!(ns, vec![33, 32]);
    }

    #[test]
    fn update_delete_use_index_and_residual_filter() {
        let (_dir, store) = store();
        let c = store.collection("groups").unwrap();
        c.insert_one(json!({"g": "a", "n": 1})).unwrap();
        c.insert_one(json!({"g": "a", "n": 2})).unwrap();
        c.insert_one(json!({"g": "b", "n": 3})).unwrap();
        c.create_index("g", false).unwrap();

        let updated = c
            .update(&json!({"g": "a"}), &json!({"$set": {"n": 0}}), true)
            .unwrap();
        assert_eq!(updated, 2);
        assert_eq!(c.count(&json!({"g": "a", "n": 0})).unwrap(), 2);

        let deleted = c.delete(&json!({"g": "b"}), false).unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(c.count(&json!({})).unwrap(), 2);

        // Exact `_id` path in update/delete.
        let one = c.find_one(&json!({"g": "a"})).unwrap().unwrap();
        let id = one["_id"].as_str().unwrap().to_string();
        assert_eq!(
            c.update(&json!({"_id": id}), &json!({"$inc": {"n": 5}}), false)
                .unwrap(),
            1
        );
        assert_eq!(c.delete(&json!({"_id": id}), false).unwrap(), 1);
        assert_eq!(c.count(&json!({})).unwrap(), 1);
    }

    #[test]
    fn aggregate_uses_leading_match_and_common_stages() {
        let (_dir, store) = store();
        let c = store.collection("orders").unwrap();
        c.insert_one(json!({"tenant": "a", "amount": 10, "tags": ["x"]}))
            .unwrap();
        c.insert_one(json!({"tenant": "a", "amount": 20, "tags": ["x", "y"]}))
            .unwrap();
        c.insert_one(json!({"tenant": "b", "amount": 5, "tags": []}))
            .unwrap();

        let out = c
            .aggregate(&[
                json!({"$match": {"amount": {"$gte": 10}}}),
                json!({"$group": {
                    "_id": "$tenant",
                    "total": {"$sum": "$amount"},
                    "n": {"$sum": 1}
                }}),
                json!({"$sort": {"total": -1}}),
                json!({"$project": {"_id": 0, "tenant": "$_id", "total": 1, "n": 1}}),
            ])
            .unwrap();
        assert_eq!(out, vec![json!({"tenant": "a", "total": 30, "n": 2})]);

        let out = c
            .aggregate(&[json!({"$unwind": "$tags"}), json!({"$count": "rows"})])
            .unwrap();
        assert_eq!(out, vec![json!({"rows": 3})]);
    }

    #[test]
    fn aggregate_pushdown_preserves_stage_order() {
        let (_dir, store) = store();
        let c = store.collection("pushdown").unwrap();
        c.insert_one(json!({"_id": "a", "amount": 30, "keep": true}))
            .unwrap();
        c.insert_one(json!({"_id": "b", "amount": 10, "keep": false}))
            .unwrap();
        c.insert_one(json!({"_id": "c", "amount": 20, "keep": true}))
            .unwrap();

        // `$limit` before `$sort`: sort only the first two documents.
        let out = c
            .aggregate(&[json!({"$limit": 2}), json!({"$sort": {"amount": -1}})])
            .unwrap();
        let amounts: Vec<_> = out.iter().map(|d| d["amount"].as_i64().unwrap()).collect();
        assert_eq!(amounts, vec![30, 10]);

        // `$sort`, `$skip`, `$limit` and a simple `$project` can move together.
        let out = c
            .aggregate(&[
                json!({"$sort": {"amount": -1}}),
                json!({"$skip": 1}),
                json!({"$limit": 1}),
                json!({"$project": {"_id": 0, "amount": 1}}),
            ])
            .unwrap();
        assert_eq!(out, vec![json!({"amount": 20})]);

        // A lone `$count` after `$match` uses the count fast path.
        let out = c
            .aggregate(&[json!({"$match": {"keep": true}}), json!({"$count": "rows"})])
            .unwrap();
        assert_eq!(out, vec![json!({"rows": 2})]);

        // `{_id: 1}` must not be pushed into `find`, which treats it as an
        // exclusion projection. Aggregation returns only `_id`.
        let out = c.aggregate(&[json!({"$project": {"_id": 1}})]).unwrap();
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|d| d.as_object().unwrap().len() == 1));

        // Consecutive leading `$match` stages are combined.
        let out = c
            .aggregate(&[
                json!({"$match": {"keep": true}}),
                json!({"$match": {"amount": {"$gt": 15}}}),
                json!({"$count": "rows"}),
            ])
            .unwrap();
        assert_eq!(out, vec![json!({"rows": 2})]);

        // A cardinality-preserving prefix before `$count` can be skipped.
        let out = c
            .aggregate(&[
                json!({"$sort": {"amount": -1}}),
                json!({"$project": {"_id": 0, "amount": 1}}),
                json!({"$count": "rows"}),
            ])
            .unwrap();
        assert_eq!(out, vec![json!({"rows": 3})]);

        // Computed stages are still evaluated before counting.
        let out = c
            .aggregate(&[
                json!({"$addFields": {"double": {"$multiply": ["$amount", 2]}}}),
                json!({"$count": "rows"}),
            ])
            .unwrap();
        assert_eq!(out, vec![json!({"rows": 3})]);
        assert!(c
            .aggregate(&[
                json!({"$project": {"bad": {"$divide": [1, 0]}}}),
                json!({"$count": "rows"}),
            ])
            .is_err());
    }
}

#[cfg(test)]
mod parse_json_tests {
    use super::*;

    #[test]
    fn sonic_matches_serde_for_edge_values() {
        let samples = [
            r#"{"a":1,"b":-2.5,"c":true,"d":null,"e":"x\\ny","u":"zażółć"}"#,
            r#"{"n":18446744073709551615,"i":-9223372036854775808}"#,
            r#"[1,2,3,{"x":"\\u00e9"}]"#,
            r#"0.1"#,
            r#""tekst""#,
        ];
        for raw in samples {
            let fast = parse_json(raw.as_bytes()).unwrap();
            let slow: Value = serde_json::from_slice(raw.as_bytes()).unwrap();
            assert_eq!(fast, slow, "raw={raw}");
        }
    }
}
