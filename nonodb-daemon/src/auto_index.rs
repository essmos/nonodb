//! Automatic secondary indexes for frequently repeated query shapes.
//!
//! The mechanism is deliberately conservative and **disabled by default**. It collects
//! only information about fields and operators (not concrete values),
//! and creates an index only after the hit threshold is exceeded. Automatically
//! created indexes are dropped after a period without demand.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nonodb_core::bson_types;
use nonodb_core::date;
use nonodb_core::numeric;
use nonodb_core::Store;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::oneshot;

use crate::metrics;
use crate::protocol::{Request, WriteReq};
use crate::writer::WriteJob;

const STATE_VERSION: u32 = 1;

#[derive(Clone, Debug)]
struct Config {
    enabled: bool,
    min_hits: u64,
    window: Duration,
    min_docs: usize,
    max_indexes: usize,
    max_fields: usize,
    ttl: Duration,
    min_age: Duration,
    interval: Duration,
}

impl Config {
    fn from_env() -> Self {
        Self {
            enabled: env_bool("NONODB_AUTO_INDEX", false),
            min_hits: env_u64("NONODB_AUTO_INDEX_MIN_HITS", 50).max(1),
            window: Duration::from_secs(env_u64("NONODB_AUTO_INDEX_WINDOW", 300).max(1)),
            min_docs: env_usize("NONODB_AUTO_INDEX_MIN_DOCS", 10_000),
            max_indexes: env_usize("NONODB_AUTO_INDEX_MAX", 8).max(1),
            max_fields: env_usize("NONODB_AUTO_INDEX_MAX_FIELDS", 3).clamp(1, 8),
            ttl: Duration::from_secs(env_u64("NONODB_AUTO_INDEX_TTL", 7 * 24 * 3600).max(1)),
            min_age: Duration::from_secs(env_u64("NONODB_AUTO_INDEX_MIN_AGE", 3600)),
            interval: Duration::from_secs(env_u64("NONODB_AUTO_INDEX_INTERVAL", 5).max(1)),
        }
    }
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[derive(Debug)]
struct CandidateStats {
    hits: u64,
    window_start: Instant,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AutoEntry {
    coll: String,
    fields: Vec<String>,
    created_at: u64,
    last_demand: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct StateFile {
    #[serde(default)]
    version: u32,
    #[serde(default)]
    indexes: Vec<AutoEntry>,
}

#[derive(Debug, Default)]
struct Inner {
    stats: HashMap<String, CandidateStats>,
    auto: HashMap<String, AutoEntry>,
}

/// Shared statistics registry and automatic-index manager.
pub struct AutoIndexer {
    config: Config,
    store: Arc<Store>,
    write_tx: mpsc::Sender<WriteJob>,
    state_path: PathBuf,
    inner: Mutex<Inner>,
    dirty: AtomicBool,
    last_tick: Mutex<Option<Instant>>,
}

impl AutoIndexer {
    /// Creates the registry. The daemon maintenance worker drives `tick_due`.
    /// When `NONODB_AUTO_INDEX=0`, all methods are no-ops.
    pub fn start(
        store: Arc<Store>,
        write_tx: mpsc::Sender<WriteJob>,
        data_dir: impl AsRef<Path>,
    ) -> Arc<Self> {
        let config = Config::from_env();
        let state_path = data_dir.as_ref().join("auto-index.json");
        let inner = Inner::default();
        let me = Arc::new(Self {
            config,
            store,
            write_tx,
            state_path,
            inner: Mutex::new(inner),
            dirty: AtomicBool::new(false),
            last_tick: Mutex::new(None),
        });

        if me.config.enabled {
            me.load_state();
        }
        me
    }

    /// Records a query. `db_work` means the request was not served
    /// by the cache and will actually perform a scan/index lookup.
    pub fn record(&self, req: &Request, db_work: bool) {
        if !self.config.enabled {
            return;
        }
        let Some((coll, fields)) = candidate_for(req, self.config.max_fields) else {
            return;
        };
        let key = candidate_key(&coll, &fields);
        let now = now_secs();
        let mut inner = self.inner.lock().unwrap();

        if db_work {
            let entry = inner
                .stats
                .entry(key.clone())
                .or_insert_with(|| CandidateStats {
                    hits: 0,
                    window_start: Instant::now(),
                });
            if entry.window_start.elapsed() >= self.config.window {
                entry.hits = 0;
                entry.window_start = Instant::now();
            }
            entry.hits += 1;
        }

        // A cache hit still means demand for this query shape.
        // This prevents TTL from dropping an index merely because the cache
        // temporarily served all reads.
        if let Some(auto) = inner.auto.get_mut(&key) {
            auto.last_demand = now;
            self.dirty.store(true, Ordering::Relaxed);
        }
    }

    /// Runs one maintenance tick if the configured interval has elapsed.
    pub fn tick_due(&self) {
        if !self.config.enabled {
            return;
        }
        {
            let mut last = self.last_tick.lock().unwrap();
            if let Some(at) = *last {
                if at.elapsed() < self.config.interval {
                    return;
                }
            }
            *last = Some(Instant::now());
        }
        if let Err(e) = self.tick() {
            tracing::warn!("auto-index tick failed: {e}");
        }
    }

    fn tick(&self) -> Result<(), String> {
        if !self.config.enabled {
            return Ok(());
        }
        // Do not build an index when the writer has a backlog. The build itself
        // happens in one transaction, so it only makes sense under a quiet
        // write load.
        let busy = metrics::QUEUE_LEN.load(Ordering::Relaxed) > 32;

        self.expire_unused()?;

        if !busy {
            self.create_candidates()?;
        }

        if self.dirty.swap(false, Ordering::Relaxed) {
            self.save_state()?;
        }
        let (active, candidates) = {
            let inner = self.inner.lock().unwrap();
            let candidates = inner
                .stats
                .values()
                .filter(|s| s.hits >= self.config.min_hits)
                .count();
            (inner.auto.len() as u64, candidates as u64)
        };
        metrics::AUTO_INDEX_ACTIVE.store(active, Ordering::Relaxed);
        metrics::AUTO_INDEX_CANDIDATES.store(candidates, Ordering::Relaxed);
        Ok(())
    }

    fn create_candidates(&self) -> Result<(), String> {
        let now = now_secs();
        let mut candidates: Vec<(String, String, Vec<String>, u64)> = {
            let inner = self.inner.lock().unwrap();
            inner
                .stats
                .iter()
                .filter(|(key, s)| s.hits >= self.config.min_hits && !inner.auto.contains_key(*key))
                .filter_map(|(key, s)| {
                    let (coll, fields) = split_candidate_key(key)?;
                    Some((key.clone(), coll, fields, s.hits))
                })
                .collect()
        };
        candidates.sort_by_key(|(_, _, _, hits)| std::cmp::Reverse(*hits));

        for (key, coll, fields, _) in candidates {
            let active = self.inner.lock().unwrap().auto.len();
            if active >= self.config.max_indexes {
                break;
            }
            // A manual index with the same shape is already optimal - do not
            // create a duplicate and stop counting this candidate.
            if self.index_exists(&coll, &fields)? {
                self.forget(&key);
                continue;
            }
            if !self.collection_ready(&coll, &fields)? {
                continue;
            }

            let started = Instant::now();
            match self.send_write(WriteReq::CreateIndex {
                coll: coll.clone(),
                fields: fields.clone(),
                unique: false,
                include: Vec::new(),
            }) {
                Ok(_) => {
                    metrics::inc(&metrics::AUTO_INDEX_CREATED);
                    metrics::add(
                        &metrics::AUTO_INDEX_BUILD_NS,
                        started.elapsed().as_nanos() as u64,
                    );
                    let entry = AutoEntry {
                        coll: coll.clone(),
                        fields: fields.clone(),
                        created_at: now,
                        last_demand: now,
                    };
                    let mut inner = self.inner.lock().unwrap();
                    inner.auto.insert(key.clone(), entry);
                    inner.stats.remove(&key);
                    drop(inner);
                    self.dirty.store(true, Ordering::Relaxed);
                    tracing::info!(
                        collection = %coll,
                        fields = ?fields,
                        "created automatic index"
                    );
                }
                Err(e) => {
                    metrics::inc(&metrics::AUTO_INDEX_BUILD_ERRORS);
                    tracing::warn!(
                        collection = %coll,
                        fields = ?fields,
                        "automatic index creation failed: {e}"
                    );
                    // Do not retry the same candidate on every tick.
                    if let Some(entry) = self.inner.lock().unwrap().stats.get_mut(&key) {
                        entry.hits = 0;
                        entry.window_start = Instant::now();
                    }
                }
            }
        }
        Ok(())
    }

    fn expire_unused(&self) -> Result<(), String> {
        let now = now_secs();
        let ttl = self.config.ttl.as_secs();
        let min_age = self.config.min_age.as_secs();
        let tracked: Vec<(String, AutoEntry)> = {
            let inner = self.inner.lock().unwrap();
            inner
                .auto
                .iter()
                .map(|(key, entry)| (key.clone(), entry.clone()))
                .collect()
        };

        for (key, entry) in tracked {
            // The index may have been dropped manually or disappeared after a restart.
            // In that case only clean up the registry; do not try to drop it again.
            if !self.index_exists(&entry.coll, &entry.fields)? {
                self.forget(&key);
                tracing::info!(
                    collection = %entry.coll,
                    fields = ?entry.fields,
                    "forgot automatic index that no longer exists"
                );
                continue;
            }

            if now.saturating_sub(entry.last_demand) < ttl
                || now.saturating_sub(entry.created_at) < min_age
            {
                continue;
            }

            let field = entry.fields.join("+");
            match self.send_write(WriteReq::DropIndex {
                coll: entry.coll.clone(),
                field,
            }) {
                Ok(_) => {
                    metrics::inc(&metrics::AUTO_INDEX_DROPPED);
                    self.forget(&key);
                    tracing::info!(
                        collection = %entry.coll,
                        fields = ?entry.fields,
                        "dropped unused automatic index"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        collection = %entry.coll,
                        fields = ?entry.fields,
                        "automatic index drop failed: {e}"
                    );
                }
            }
        }
        Ok(())
    }

    fn forget(&self, key: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.auto.remove(key);
        inner.stats.remove(key);
        drop(inner);
        self.dirty.store(true, Ordering::Relaxed);
    }

    fn index_exists(&self, coll: &str, fields: &[String]) -> Result<bool, String> {
        let rtxn = self.store.read_txn().map_err(|e| e.to_string())?;
        let Some(collection) = self
            .store
            .collection_at(&rtxn, coll)
            .map_err(|e| e.to_string())?
        else {
            rtxn.commit().ok();
            return Ok(false);
        };
        let name = fields.join("+");
        let exists = collection
            .index_defs_in(&rtxn)
            .map_err(|e| e.to_string())?
            .iter()
            .any(|d| d.name() == name);
        rtxn.commit().ok();
        Ok(exists)
    }

    /// Checks whether the collection exists, has enough documents, and does not
    /// already have an index with exactly these fields. It also removes registry
    /// entries for indexes that no longer exist in LMDB (e.g. after a manual drop).
    fn collection_ready(&self, coll: &str, fields: &[String]) -> Result<bool, String> {
        let rtxn = self.store.read_txn().map_err(|e| e.to_string())?;
        let Some(collection) = self
            .store
            .collection_at(&rtxn, coll)
            .map_err(|e| e.to_string())?
        else {
            rtxn.commit().ok();
            return Ok(false);
        };
        if collection.profile().is_vault() {
            rtxn.commit().ok();
            return Ok(false);
        }
        let defs = collection.index_defs_in(&rtxn).map_err(|e| e.to_string())?;
        let name = fields.join("+");
        if defs.iter().any(|d| d.name() == name) {
            rtxn.commit().ok();
            return Ok(false);
        }
        let count = collection
            .count_in(&rtxn, &Value::Null)
            .map_err(|e| e.to_string())?;
        rtxn.commit().ok();
        Ok(count >= self.config.min_docs)
    }

    fn send_write(&self, req: WriteReq) -> Result<Value, String> {
        let (tx, rx) = oneshot::channel();
        metrics::inc(&metrics::QUEUE_LEN);
        metrics::note_queue();
        if let Err(e) = self.write_tx.send(WriteJob {
            req,
            resp: tx,
            isolated: true,
        }) {
            metrics::sub(&metrics::QUEUE_LEN, 1);
            return Err(format!("writer channel closed: {e}"));
        }
        match rx.blocking_recv() {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(e)) => Err(e.message),
            Err(e) => Err(format!("writer response lost: {e}")),
        }
    }

    fn load_state(&self) {
        let Ok(bytes) = fs::read(&self.state_path) else {
            return;
        };
        let Ok(state) = serde_json::from_slice::<StateFile>(&bytes) else {
            tracing::warn!(path = %self.state_path.display(), "invalid auto-index state");
            return;
        };
        if state.version != STATE_VERSION {
            tracing::warn!(
                path = %self.state_path.display(),
                version = state.version,
                "unsupported auto-index state version"
            );
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        for entry in state.indexes {
            inner
                .auto
                .insert(candidate_key(&entry.coll, &entry.fields), entry);
        }
        tracing::info!(
            path = %self.state_path.display(),
            indexes = inner.auto.len(),
            "loaded automatic index state"
        );
    }

    fn save_state(&self) -> Result<(), String> {
        let indexes: Vec<AutoEntry> = {
            let inner = self.inner.lock().unwrap();
            let mut out: Vec<_> = inner.auto.values().cloned().collect();
            out.sort_by(|a, b| a.coll.cmp(&b.coll).then_with(|| a.fields.cmp(&b.fields)));
            out
        };
        let state = StateFile {
            version: STATE_VERSION,
            indexes,
        };
        let data = serde_json::to_vec_pretty(&state).map_err(|e| e.to_string())?;
        let tmp = self
            .state_path
            .with_extension(format!("tmp.{}", std::process::id()));
        fs::write(&tmp, data).map_err(|e| e.to_string())?;
        // `rename` does not overwrite an existing file on Windows.
        if let Err(first) = fs::rename(&tmp, &self.state_path) {
            let _ = fs::remove_file(&self.state_path);
            fs::rename(&tmp, &self.state_path).map_err(|e| format!("{first}; retry: {e}"))?;
        }
        Ok(())
    }
}

fn candidate_key(coll: &str, fields: &[String]) -> String {
    format!("{coll}\u{1}{}", fields.join("+"))
}

fn split_candidate_key(key: &str) -> Option<(String, Vec<String>)> {
    let (coll, fields) = key.split_once('\u{1}')?;
    let fields: Vec<String> = fields.split('+').map(str::to_string).collect();
    if coll.is_empty() || fields.is_empty() || fields.iter().any(String::is_empty) {
        return None;
    }
    Some((coll.to_string(), fields))
}

/// Extracts the index shape from a filter: equality fields plus one range field.
/// Returns `None` if the query is not suitable for a safe automatic index.
fn candidate_for(req: &Request, max_fields: usize) -> Option<(String, Vec<String>)> {
    let (coll, filter) = match req {
        Request::Find { coll, filter, .. }
        | Request::FindOne { coll, filter, .. }
        | Request::Count { coll, filter } => (coll, filter),
        _ => return None,
    };
    let obj = filter.as_object()?;
    if obj.is_empty() || obj.keys().any(|k| k.starts_with('$')) {
        return None;
    }
    // `_id` has its own, better path over the primary key.
    if obj.contains_key("_id") {
        return None;
    }

    let mut eq = Vec::new();
    let mut ranges = Vec::new();
    for (field, cond) in obj {
        if field.is_empty() || field.contains('+') || field.contains('\0') {
            return None;
        }
        match condition_kind(cond)? {
            Some(CondKind::Eq) => eq.push(field.clone()),
            Some(CondKind::Range) => ranges.push(field.clone()),
            None => {}
        }
    }

    eq.sort();
    ranges.sort();
    eq.dedup();
    let mut fields = eq;
    fields.truncate(max_fields);
    if fields.len() < max_fields {
        if let Some(range) = ranges.into_iter().next() {
            if !fields.contains(&range) {
                fields.push(range);
            }
        }
    }
    if fields.is_empty() {
        return None;
    }
    Some((coll.clone(), fields))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CondKind {
    Eq,
    Range,
}

/// `None` means a condition we do not want to index (e.g. bool/null or
/// an operator outside equality/range). `Some(None)` is a condition that is skipped but
/// safe for the rest of the query.
fn condition_kind(value: &Value) -> Option<Option<CondKind>> {
    if date::is_date(value) || numeric::is_numeric(value) || bson_types::is_bson_value(value) {
        return Some(Some(CondKind::Eq));
    }
    match value {
        Value::String(_) | Value::Number(_) => Some(Some(CondKind::Eq)),
        Value::Bool(_) | Value::Null => Some(None),
        Value::Object(ops) => {
            if ops.is_empty() {
                return None;
            }
            let mut eq = false;
            let mut range = false;
            for (op, value) in ops {
                match op.as_str() {
                    "$eq" => {
                        if !matches!(value, Value::String(_) | Value::Number(_))
                            && !date::is_date(value)
                            && !numeric::is_numeric(value)
                            && !bson_types::is_bson_value(value)
                        {
                            return Some(None);
                        }
                        eq = true;
                    }
                    "$gt" | "$gte" | "$lt" | "$lte" => {
                        if !matches!(value, Value::String(_) | Value::Number(_))
                            && !date::is_date(value)
                            && !numeric::is_numeric(value)
                            && !bson_types::is_bson_value(value)
                        {
                            return Some(None);
                        }
                        range = true;
                    }
                    _ => return None,
                }
            }
            if eq {
                Some(Some(CondKind::Eq))
            } else if range {
                Some(Some(CondKind::Range))
            } else {
                Some(None)
            }
        }
        Value::Array(_) => Some(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn find(filter: Value) -> Request {
        Request::Find {
            coll: "users".into(),
            filter,
            projection: None,
            sort: Vec::new(),
            skip: 0,
            limit: Some(100),
        }
    }

    #[test]
    fn extracts_single_field_equality() {
        let (coll, fields) = candidate_for(&find(json!({"status": "active"})), 3).unwrap();
        assert_eq!(coll, "users");
        assert_eq!(fields, vec!["status"]);
    }

    #[test]
    fn extracts_compound_equality_and_range() {
        let (_, fields) = candidate_for(
            &find(json!({
                "tenant": "acme",
                "kind": "event",
                "created_at": {"$gte": 10, "$lt": 20}
            })),
            3,
        )
        .unwrap();
        assert_eq!(fields, vec!["kind", "tenant", "created_at"]);
    }

    #[test]
    fn extracts_date_equality_and_range() {
        let (_, fields) = candidate_for(
            &find(json!({
                "tenant": "acme",
                "created_at": {"$gte": {"$date": "2026-01-01T00:00:00Z"}}
            })),
            3,
        )
        .unwrap();
        assert_eq!(fields, vec!["tenant", "created_at"]);

        let (_, fields) = candidate_for(
            &find(json!({"created_at": {"$date": "2026-01-01T00:00:00Z"}})),
            3,
        )
        .unwrap();
        assert_eq!(fields, vec!["created_at"]);
    }

    #[test]
    fn extracts_decimal_equality_and_range() {
        let (_, fields) = candidate_for(
            &find(json!({
                "price": {"$gte": {"$numberDecimal": "10.00"}}
            })),
            3,
        )
        .unwrap();
        assert_eq!(fields, vec!["price"]);
        let (_, fields) =
            candidate_for(&find(json!({"price": {"$numberDecimal": "10.00"}})), 3).unwrap();
        assert_eq!(fields, vec!["price"]);
    }

    #[test]
    fn extracts_binary_and_object_id_equality() {
        let (_, fields) = candidate_for(
            &find(json!({"blob": {"$binary": {"base64": "eA==", "subType": "00"}}})),
            3,
        )
        .unwrap();
        assert_eq!(fields, vec!["blob"]);
        let (_, fields) = candidate_for(
            &find(json!({"ref": {"$oid": "000000000000000000000001"}})),
            3,
        )
        .unwrap();
        assert_eq!(fields, vec!["ref"]);
    }

    #[test]
    fn skips_unsupported_and_primary_key_filters() {
        assert!(candidate_for(&find(json!({"$or": [{"a": 1}]})), 3).is_none());
        assert!(candidate_for(&find(json!({"_id": "x", "a": 1})), 3).is_none());
        assert!(candidate_for(&find(json!({"a": {"$ne": 1}})), 3).is_none());
        assert!(candidate_for(&find(json!({"a": true})), 3).is_none());
        assert!(candidate_for(&find(json!({"a": null})), 3).is_none());
        assert!(candidate_for(&find(json!({"a": [1, 2]})), 3).is_none());
    }

    #[test]
    fn skips_field_names_with_index_separator() {
        assert!(candidate_for(&find(json!({"a+b": 1})), 3).is_none());
    }

    #[test]
    fn candidate_key_roundtrips() {
        let key = candidate_key("users", &["a".into(), "b".into()]);
        let (coll, fields) = split_candidate_key(&key).unwrap();
        assert_eq!(coll, "users");
        assert_eq!(fields, vec!["a", "b"]);
    }
}
