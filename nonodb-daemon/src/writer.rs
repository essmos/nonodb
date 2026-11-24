//! Jeden pisarz + group commit.
//!
//! All write operations go through an `mpsc` channel to one thread. The thread
//! collects a batch of requests (up to `MAX_BATCH` in a `BATCH_WINDOW`), executes
//! them in **one** LMDB transaction and performs **one** `commit` - that is, one
//! `fsync` per batch. The client response is sent only after the commit.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use nonodb_core::replication::{ReplicationChange, ReplicationOp};
use nonodb_core::{
    events, id, vault, Collection, CollectionProfile, FindAndModifyOptions, FindQuery, Store,
};
use serde_json::{json, Value};
use tokio::sync::oneshot;

use crate::cache::QueryCache;
use crate::live;
use crate::metrics;
use crate::protocol::{ApiError, Durability, WriteReq};

/// Maximum number of operations in one transaction.
const MAX_BATCH: usize = 512;

#[derive(Default)]
struct ReplicationContext {
    origin: Option<String>,
    changes: Vec<ReplicationChange>,
    ops: Vec<ReplicationOp>,
}

impl ReplicationContext {
    fn capture(
        &mut self,
        collection: &str,
        op: &str,
        id: &str,
        before: Option<&Value>,
        after: Option<&Value>,
    ) {
        if self.origin.is_none() {
            return;
        }
        let change = self.changes.len();
        self.changes.push(ReplicationChange {
            collection: collection.to_string(),
            op: op.to_string(),
            id: id.to_string(),
            before: before.cloned(),
            after: after.cloned(),
            vault: false,
            ttl_at: None,
        });
        self.ops.push(ReplicationOp::Document { change });
    }

    fn capture_vault(
        &mut self,
        collection: &str,
        op: &str,
        id: &str,
        before: Option<&Value>,
        after: Option<&Value>,
    ) {
        if self.origin.is_none() {
            return;
        }
        let change = self.changes.len();
        self.changes.push(ReplicationChange {
            collection: collection.to_string(),
            op: op.to_string(),
            id: id.to_string(),
            before: before.cloned(),
            after: after.cloned(),
            vault: true,
            ttl_at: None,
        });
        self.ops.push(ReplicationOp::Document { change });
    }

    fn capture_op(&mut self, op: ReplicationOp) {
        if self.origin.is_some() {
            self.ops.push(op);
        }
    }

    fn capture_event(&mut self, event: &Value) {
        if self.origin.is_some() {
            self.ops
                .push(ReplicationOp::EventAppend { doc: event.clone() });
        }
    }

    fn capture_sql_changes(&mut self, start: usize) {
        for change in start..self.changes.len() {
            self.ops.push(ReplicationOp::Document { change });
        }
    }
}

pub(crate) fn replication_origin() -> Option<String> {
    static ORIGIN: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    ORIGIN
        .get_or_init(|| {
            let enabled = std::env::var("NONODB_REPLICATION_ENABLED")
                .ok()
                .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"));
            if !enabled {
                return None;
            }
            Some(
                std::env::var("NONODB_REPLICATION_ORIGIN")
                    .or_else(|_| std::env::var("NONODB_NODE_ID"))
                    .unwrap_or_else(|_| "local".to_string()),
            )
        })
        .clone()
}

fn replication_enabled() -> bool {
    replication_origin().is_some()
}

thread_local! {
    /// SQL writes reach the daemon through the core observer rather than the
    /// writer's direct emit_event path. This buffer merges them into the same
    /// replication transaction as the surrounding writer batch.
    static SQL_REPLICATION_CHANGES: RefCell<Vec<ReplicationChange>> =
        const { RefCell::new(Vec::new()) };
    static OUTBOX_REPLICATION_EVENTS: RefCell<Vec<Value>> = const { RefCell::new(Vec::new()) };
}

pub(crate) fn capture_sql_replication(
    collection: &str,
    op: &str,
    id: &str,
    before: Option<&Value>,
    after: Option<&Value>,
) {
    if !replication_enabled() {
        return;
    }
    SQL_REPLICATION_CHANGES.with(|slot| {
        slot.borrow_mut().push(ReplicationChange {
            collection: collection.to_string(),
            op: op.to_string(),
            id: id.to_string(),
            before: before.cloned(),
            after: after.cloned(),
            vault: false,
            ttl_at: None,
        });
    });
}

fn take_sql_replication_changes() -> Vec<ReplicationChange> {
    SQL_REPLICATION_CHANGES.with(|slot| std::mem::take(&mut *slot.borrow_mut()))
}

pub(crate) fn capture_outbox_event(event: &Value) {
    OUTBOX_REPLICATION_EVENTS.with(|slot| slot.borrow_mut().push(event.clone()));
}

fn take_outbox_events() -> Vec<Value> {
    OUTBOX_REPLICATION_EVENTS.with(|slot| std::mem::take(&mut *slot.borrow_mut()))
}
fn env_micros(name: &str, default_us: u64) -> Duration {
    Duration::from_micros(
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(default_us),
    )
}

fn window_config() -> (Duration, Duration, Duration, Duration) {
    let base = env_micros("NONODB_WRITER_WINDOW_US", 1000);
    let max = env_micros("NONODB_WRITER_WINDOW_MAX_US", 5000).max(base);
    let idle = env_micros("NONODB_WRITER_WINDOW_IDLE_US", 100).min(base);
    let pressure = env_micros("NONODB_WRITER_WINDOW_PRESSURE_US", 3000)
        .max(base)
        .min(max);
    (idle, base, pressure, max)
}
pub struct WriteJob {
    pub req: WriteReq,
    pub resp: oneshot::Sender<Result<Value, ApiError>>,
    /// This operation alone in the transaction (e.g. a long auto-index backfill).
    pub isolated: bool,
    /// Requested durability. A group commit uses the strictest mode in the batch.
    pub durability: Durability,
}

/// Writer thread loop. Ends when the channel is closed.
pub fn run(
    store: Arc<Store>,
    rx: Receiver<WriteJob>,
    cache: Arc<QueryCache>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    let (idle_window, base_window, pressure_window, max_window) = window_config();
    loop {
        let first = match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(job) => job,
            Err(RecvTimeoutError::Timeout) => {
                if stop.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                continue;
            }
            Err(RecvTimeoutError::Disconnected) => break,
        };
        let mut jobs = vec![first];
        // Adaptive window: use the fast path when no other writes are queued,
        // grow with queue pressure, and use the base window for a small
        // backlog. QUEUE_LEN still includes the job we just received; only the
        // remainder is useful for choosing the window.
        let pending = metrics::QUEUE_LEN
            .load(std::sync::atomic::Ordering::Relaxed)
            .saturating_sub(1);
        let (window, window_counter) = if pending >= 256 {
            (max_window, &metrics::WRITER_MAX_WINDOWS)
        } else if pending >= 64 {
            (pressure_window, &metrics::WRITER_PRESSURE_WINDOWS)
        } else if pending == 0 {
            (idle_window, &metrics::WRITER_IDLE_WINDOWS)
        } else {
            (base_window, &metrics::WRITER_BASE_WINDOWS)
        };
        metrics::inc(window_counter);
        metrics::add(&metrics::WRITER_WINDOW_NS_TOTAL, window.as_nanos() as u64);
        metrics::set(&metrics::WRITER_WINDOW_US, window.as_micros() as u64);
        let deadline = Instant::now() + window;

        while jobs.len() < MAX_BATCH {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            match rx.recv_timeout(deadline - now) {
                Ok(job) => jobs.push(job),
                Err(RecvTimeoutError::Timeout) | Err(RecvTimeoutError::Disconnected) => break,
            }
        }

        metrics::set(&metrics::WRITER_LAST_BATCH_SIZE, jobs.len() as u64);
        metrics::inc(&metrics::WRITER_BUSY);
        run_batch(&store, &cache, jobs);
        metrics::sub(&metrics::WRITER_BUSY, 1);
    }
}

fn capture_replicated_ttls(
    store: &Store,
    wtxn: &mut nonodb_core::heed::RwTxn<'_>,
    changes: &mut [ReplicationChange],
) {
    for change in changes {
        change.ttl_at = None;
        if change.op == "delete" {
            continue;
        }
        let Ok(collection) = store.collection_in(wtxn, &change.collection) else {
            continue;
        };
        change.ttl_at = collection.ttl_expiry_in(&*wtxn, &change.id).ok().flatten();
    }
}

fn run_batch(store: &Store, cache: &QueryCache, jobs: Vec<WriteJob>) {
    metrics::sub(&metrics::QUEUE_LEN, jobs.len() as u64);
    if !crate::replicator::writable() {
        let err = ApiError {
            code: "read_only".to_string(),
            message: "replica is read-only; promote this node before accepting writes".to_string(),
        };
        for job in jobs {
            let _ = job.resp.send(Err(err.clone()));
        }
        return;
    }
    // An atomic `write_batch` cannot share a transaction with other
    // requests: one failed operation must roll back the whole batch, not
    // the other writes in the group. That is why such a group is run job by job.
    if jobs.iter().any(|j| j.req.is_atomic() || j.isolated) {
        for job in jobs {
            run_single(store, cache, job);
        }
        return;
    }
    metrics::inc(&metrics::WRITE_BATCHES);
    let sync = jobs.iter().any(|job| job.durability == Durability::Full);
    let mut wtxn = match store.write_txn_unsynced() {
        Ok(wtxn) => wtxn,
        Err(e) => {
            let err = ApiError::from(e);
            for job in jobs {
                let _ = job.resp.send(Err(err.clone()));
            }
            return;
        }
    };

    let mut replication = ReplicationContext {
        origin: replication_origin(),
        changes: Vec::new(),
        ops: Vec::new(),
    };
    let _ = take_sql_replication_changes();
    let mut results = Vec::with_capacity(jobs.len());
    for job in &jobs {
        results.push(execute(store, &mut wtxn, &job.req, &mut replication));
    }
    let sql_start = replication.changes.len();
    replication.changes.extend(take_sql_replication_changes());
    replication.capture_sql_changes(sql_start);
    for event in take_outbox_events() {
        replication.capture_event(&event);
    }
    if let Some(origin) = &replication.origin {
        if !replication.changes.is_empty() || !replication.ops.is_empty() {
            capture_replicated_ttls(store, &mut wtxn, &mut replication.changes);
            if let Err(error) = store.append_replication_txn_ops_with_epoch_in(
                &mut wtxn,
                origin,
                crate::replicator::epoch(),
                replication.changes,
                replication.ops,
            ) {
                drop(wtxn);
                live::discard_pending();
                let err = ApiError::from(error);
                for job in jobs {
                    let _ = job.resp.send(Err(err.clone()));
                }
                return;
            }
        }
    }

    let started = Instant::now();
    match wtxn.commit_unsynced() {
        Ok(()) => {
            if sync {
                if let Err(e) = store.force_sync() {
                    // The transaction is committed and visible; only durability
                    // failed. Invalidate reads and dispatch live events anyway.
                    cache.invalidate();
                    live::dispatch_pending();
                    let err = ApiError::from(e);
                    for job in jobs {
                        let _ = job.resp.send(Err(err.clone()));
                    }
                    return;
                }
            }
            metrics::add(&metrics::COMMIT_NS, started.elapsed().as_nanos() as u64);
            cache.invalidate();
            live::dispatch_pending();
            for (job, res) in jobs.into_iter().zip(results) {
                let _ = job.resp.send(res);
            }
        }
        Err(e) => {
            metrics::add(&metrics::COMMIT_NS, started.elapsed().as_nanos() as u64);
            live::discard_pending();
            let err = ApiError::from(nonodb_core::Error::from(e));
            for job in jobs {
                let _ = job.resp.send(Err(err.clone()));
            }
        }
    }
}

fn run_single(store: &Store, cache: &QueryCache, job: WriteJob) {
    metrics::inc(&metrics::WRITE_BATCHES);
    let sync = job.durability == Durability::Full;
    let mut wtxn = match store.write_txn_unsynced() {
        Ok(wtxn) => wtxn,
        Err(e) => {
            let _ = job.resp.send(Err(ApiError::from(e)));
            return;
        }
    };
    let mut replication = ReplicationContext {
        origin: replication_origin(),
        changes: Vec::new(),
        ops: Vec::new(),
    };
    let _ = take_sql_replication_changes();
    let started = Instant::now();
    match execute(store, &mut wtxn, &job.req, &mut replication) {
        Ok(value) => {
            let sql_start = replication.changes.len();
            replication.changes.extend(take_sql_replication_changes());
            replication.capture_sql_changes(sql_start);
            for event in take_outbox_events() {
                replication.capture_event(&event);
            }
            if let Some(origin) = &replication.origin {
                if !replication.changes.is_empty() || !replication.ops.is_empty() {
                    capture_replicated_ttls(store, &mut wtxn, &mut replication.changes);
                    if let Err(error) = store.append_replication_txn_ops_with_epoch_in(
                        &mut wtxn,
                        origin,
                        crate::replicator::epoch(),
                        replication.changes,
                        replication.ops,
                    ) {
                        drop(wtxn);
                        live::discard_pending();
                        let _ = job.resp.send(Err(ApiError::from(error)));
                        return;
                    }
                }
            }
            match wtxn.commit_unsynced() {
                Ok(()) => {
                    if sync {
                        if let Err(e) = store.force_sync() {
                            cache.invalidate();
                            live::dispatch_pending();
                            let _ = job.resp.send(Err(ApiError::from(e)));
                            return;
                        }
                    }
                    metrics::add(&metrics::COMMIT_NS, started.elapsed().as_nanos() as u64);
                    cache.invalidate();
                    live::dispatch_pending();
                    let _ = job.resp.send(Ok(value));
                }
                Err(e) => {
                    metrics::add(&metrics::COMMIT_NS, started.elapsed().as_nanos() as u64);
                    live::discard_pending();
                    let _ = job
                        .resp
                        .send(Err(ApiError::from(nonodb_core::Error::from(e))));
                }
            }
        }
        Err(e) => {
            // Drop `wtxn` = abort; for `WriteReq::Batch` this guarantees
            // all-or-nothing.
            let _ = take_sql_replication_changes();
            drop(wtxn);
            live::discard_pending();
            let _ = job.resp.send(Err(e));
        }
    }
}

fn write_collection<'a>(
    store: &'a Store,
    wtxn: &mut nonodb_core::heed::RwTxn<'_>,
    coll: &str,
    vault: Option<&crate::protocol::VaultAuth>,
) -> Result<Collection<'a>, ApiError> {
    if vault.is_some() {
        match store.collection_profile_in_rw(wtxn, coll)? {
            Some(nonodb_core::CollectionProfile::Vault) => {}
            Some(_) => return Err(vault::key_unexpected().into()),
            None => return Err(vault::not_a_vault_collection().into()),
        }
    }
    let c = store.collection_in(wtxn, coll)?;
    if vault.is_none() && c.profile().is_vault() {
        return Err(vault::standard_write_forbidden().into());
    }
    Ok(c)
}

fn standard_collection<'a>(
    store: &'a Store,
    wtxn: &mut nonodb_core::heed::RwTxn<'_>,
    coll: &str,
) -> Result<Collection<'a>, ApiError> {
    write_collection(store, wtxn, coll, None)
}

fn event_config(
    store: &Store,
    wtxn: &nonodb_core::heed::RwTxn<'_>,
    coll: &str,
) -> Result<Option<events::EventConfig>, ApiError> {
    if matches!(
        store.collection_profile_in_rw(wtxn, coll)?,
        Some(CollectionProfile::Vault)
    ) {
        return Ok(None);
    }
    if coll == events::EVENTS_COLLECTION {
        return if replication_enabled() {
            Ok(Some(events::EventConfig::default()))
        } else {
            Ok(None)
        };
    }
    let config = store.collection_events_in_rw(wtxn, coll)?;
    if config.active()
        || store.has_subscriptions_in_rw(wtxn, coll)?
        || live::registry().has_coll(store.id(), coll)
        || replication_enabled()
    {
        Ok(Some(config))
    } else {
        Ok(None)
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_event(
    store: &Store,
    wtxn: &mut nonodb_core::heed::RwTxn<'_>,
    coll: &str,
    op: &str,
    id: &str,
    before: Option<&Value>,
    after: Option<&Value>,
    config: &events::EventConfig,
    replication: &mut ReplicationContext,
) -> Result<(), ApiError> {
    let target = store.collection_in(wtxn, coll)?;
    let subscription_events = target.evaluate_subscriptions(wtxn, op, id, before, after)?;
    live::collect_pending(&target, &*wtxn, coll, op, id, before, after)?;
    replication.capture(coll, op, id, before, after);
    let _ = subscription_events;
    let Some(event) = events::change_event(coll, op, id, before, after, config) else {
        return Ok(());
    };
    target.append_outbox_event(wtxn, event)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_by_id(
    store: &Store,
    wtxn: &mut nonodb_core::heed::RwTxn<'_>,
    target: &Collection<'_>,
    coll: &str,
    op: &str,
    id: &str,
    before: Option<&Value>,
    config: &events::EventConfig,
    replication: &mut ReplicationContext,
) -> Result<(), ApiError> {
    let after = target.find_one_in(&*wtxn, &json!({ "_id": id }), None)?;
    emit_event(
        store,
        wtxn,
        coll,
        op,
        id,
        before,
        after.as_ref(),
        config,
        replication,
    )
}

fn aggregate_target<'a>(
    store: &'a Store,
    wtxn: &mut nonodb_core::heed::RwTxn<'_>,
    target: &str,
) -> Result<Collection<'a>, ApiError> {
    if target == events::EVENTS_COLLECTION {
        return Err(ApiError {
            code: "invalid_query".into(),
            message: "$out/$merge cannot write to the internal `_events` collection".into(),
        });
    }
    standard_collection(store, wtxn, target)
}

fn merge_needs_unique_index(
    target: &Collection<'_>,
    wtxn: &nonodb_core::heed::RwTxn<'_>,
    on: &[String],
) -> Result<(), ApiError> {
    if on.len() == 1 && on[0] == "_id" {
        return Ok(());
    }
    let defs = target.index_defs_in(wtxn)?;
    let found = defs
        .iter()
        .any(|def| def.unique && def.current_format() && def.effective_fields() == on);
    if found {
        Ok(())
    } else {
        Err(ApiError {
            code: "invalid_query".into(),
            message: format!(
                "$merge requires a unique index on the `on` fields: {}",
                on.join(", ")
            ),
        })
    }
}

fn merge_filter(doc: &Value, on: &[String]) -> Result<Value, ApiError> {
    let mut filter = serde_json::Map::new();
    for field in on {
        filter.insert(
            field.clone(),
            nonodb_core::query::path::get(doc, field)
                .cloned()
                .unwrap_or(Value::Null),
        );
    }
    Ok(Value::Object(filter))
}

#[allow(clippy::too_many_arguments)]
fn aggregate_out(
    store: &Store,
    wtxn: &mut nonodb_core::heed::RwTxn<'_>,
    spec: &nonodb_core::aggregate::AggregateWriteSpec,
    docs: Vec<Value>,
    replication: &mut ReplicationContext,
) -> Result<Value, ApiError> {
    let target = aggregate_target(store, wtxn, &spec.target)?;
    let config = event_config(store, wtxn, &spec.target)?;
    let before = target.find_in(
        &*wtxn,
        &FindQuery {
            filter: json!({}),
            projection: None,
            sort: Vec::new(),
            skip: 0,
            limit: None,
            comment: None,
            max_time_ms: None,
            hint: None,
        },
    )?;
    target.delete_txn(wtxn, &json!({}), true)?;
    if let Some(config) = &config {
        for doc in &before {
            let id = id::key_from_document(doc).ok_or_else(|| ApiError {
                code: "invalid_id".into(),
                message: "document has no valid _id".into(),
            })?;
            emit_event(
                store,
                wtxn,
                &spec.target,
                "delete",
                &id,
                Some(doc),
                None,
                config,
                replication,
            )?;
        }
    }
    let mut inserted = 0usize;
    for doc in docs {
        let id = target.insert_one_txn(wtxn, doc)?;
        if let Some(config) = &config {
            emit_by_id(
                store,
                wtxn,
                &target,
                &spec.target,
                "insert",
                &id,
                None,
                config,
                replication,
            )?;
        }
        inserted += 1;
    }
    Ok(json!({
        "results": [],
        "target": spec.target,
        "deleted": before.len(),
        "inserted": inserted,
    }))
}

fn merge_when_matched_pipeline(
    existing: &Value,
    incoming: &Value,
    stages: &[Value],
    expressions: &[(String, Value)],
) -> Result<Value, ApiError> {
    let mut vars = BTreeMap::new();
    for (name, expression) in expressions {
        let value = nonodb_core::aggregate::eval_expression(incoming, expression, &vars)?;
        vars.insert(name.clone(), value);
    }
    vars.insert("new".to_string(), incoming.clone());
    let mut out = nonodb_core::aggregate::run_with_vars(vec![existing.clone()], stages, 1, &vars)?;
    if out.len() != 1 {
        return Err(ApiError {
            code: "invalid_query".into(),
            message: "$merge whenMatched pipeline must produce exactly one document".into(),
        });
    }
    let replacement = out.pop().expect("checked length");
    if !replacement.is_object() {
        return Err(ApiError {
            code: "invalid_query".into(),
            message: "$merge whenMatched pipeline must produce an object".into(),
        });
    }
    Ok(replacement)
}

#[allow(clippy::too_many_arguments)]
fn aggregate_merge(
    store: &Store,
    wtxn: &mut nonodb_core::heed::RwTxn<'_>,
    spec: &nonodb_core::aggregate::AggregateWriteSpec,
    docs: Vec<Value>,
    replication: &mut ReplicationContext,
) -> Result<Value, ApiError> {
    let target = aggregate_target(store, wtxn, &spec.target)?;
    merge_needs_unique_index(&target, &*wtxn, &spec.on)?;
    let config = event_config(store, wtxn, &spec.target)?;
    let mut matched = 0usize;
    let mut modified = 0usize;
    let mut inserted = 0usize;
    let mut skipped = 0usize;
    for doc in docs {
        let Some(obj) = doc.as_object() else {
            return Err(ApiError {
                code: "invalid_query".into(),
                message: "$merge inputs must be objects".into(),
            });
        };
        let filter = merge_filter(&doc, &spec.on)?;
        let existing = target.find_one_in(&*wtxn, &filter, None)?;
        if let Some(existing) = existing {
            matched += 1;
            let id = id::key_from_document(&existing).ok_or_else(|| ApiError {
                code: "invalid_id".into(),
                message: "document has no valid _id".into(),
            })?;
            let existing_id = existing.get("_id").cloned().ok_or_else(|| ApiError {
                code: "invalid_id".into(),
                message: "document has no valid _id".into(),
            })?;
            if let Some(result_id) = obj.get("_id") {
                if id::key_from_value(result_id).as_deref() != Some(id.as_str()) {
                    return Err(ApiError {
                        code: "invalid_query".into(),
                        message: "$merge cannot change an existing document `_id`".into(),
                    });
                }
            }
            match &spec.when_matched {
                nonodb_core::aggregate::MergeWhenMatched::KeepExisting => {
                    skipped += 1;
                }
                nonodb_core::aggregate::MergeWhenMatched::Fail => {
                    return Err(ApiError {
                        code: "invalid_query".into(),
                        message: "$merge matched an existing document and whenMatched is `fail`"
                            .into(),
                    });
                }
                nonodb_core::aggregate::MergeWhenMatched::Replace => {
                    let mut replacement = doc.clone();
                    replacement
                        .as_object_mut()
                        .expect("object checked above")
                        .insert("_id".to_string(), existing_id.clone());
                    let update = json!([{"$replaceWith": replacement}]);
                    let out = target.update_opts_txn_filtered(
                        wtxn,
                        &json!({"_id": existing_id}),
                        &update,
                        false,
                        false,
                        &[],
                    )?;
                    modified += out.modified;
                    if let Some(config) = &config {
                        emit_by_id(
                            store,
                            wtxn,
                            &target,
                            &spec.target,
                            "update",
                            &id,
                            Some(&existing),
                            config,
                            replication,
                        )?;
                    }
                }
                nonodb_core::aggregate::MergeWhenMatched::Pipeline(stages) => {
                    let mut replacement =
                        merge_when_matched_pipeline(&existing, &doc, stages, &spec.let_vars)?;
                    let replacement_id = replacement.get("_id");
                    if let Some(result_id) = replacement_id {
                        if id::key_from_value(result_id).as_deref() != Some(id.as_str()) {
                            return Err(ApiError {
                                code: "invalid_query".into(),
                                message: "$merge cannot change an existing document `_id`".into(),
                            });
                        }
                    } else {
                        replacement
                            .as_object_mut()
                            .expect("pipeline output is an object")
                            .insert("_id".to_string(), existing_id.clone());
                    }
                    let update = json!([{"$replaceWith": replacement}]);
                    let out = target.update_opts_txn_filtered(
                        wtxn,
                        &json!({"_id": existing_id}),
                        &update,
                        false,
                        false,
                        &[],
                    )?;
                    modified += out.modified;
                    if let Some(config) = &config {
                        emit_by_id(
                            store,
                            wtxn,
                            &target,
                            &spec.target,
                            "update",
                            &id,
                            Some(&existing),
                            config,
                            replication,
                        )?;
                    }
                }
                nonodb_core::aggregate::MergeWhenMatched::Merge => {
                    let mut merged = existing.clone();
                    let merged_obj = merged
                        .as_object_mut()
                        .expect("existing document is an object");
                    for (key, value) in obj {
                        if key != "_id" {
                            merged_obj.insert(key.clone(), value.clone());
                        }
                    }
                    let update = json!([{"$replaceWith": merged}]);
                    let out = target.update_opts_txn_filtered(
                        wtxn,
                        &json!({"_id": id}),
                        &update,
                        false,
                        false,
                        &[],
                    )?;
                    modified += out.modified;
                    if let Some(config) = &config {
                        emit_by_id(
                            store,
                            wtxn,
                            &target,
                            &spec.target,
                            "update",
                            &id,
                            Some(&existing),
                            config,
                            replication,
                        )?;
                    }
                }
            }
        } else {
            match spec.when_not_matched {
                nonodb_core::aggregate::MergeWhenNotMatched::Discard => skipped += 1,
                nonodb_core::aggregate::MergeWhenNotMatched::Fail => {
                    return Err(ApiError {
                        code: "invalid_query".into(),
                        message: "$merge found no matching document and whenNotMatched is `fail`"
                            .into(),
                    });
                }
                nonodb_core::aggregate::MergeWhenNotMatched::Insert => {
                    let id = target.insert_one_txn(wtxn, doc)?;
                    if let Some(config) = &config {
                        emit_by_id(
                            store,
                            wtxn,
                            &target,
                            &spec.target,
                            "insert",
                            &id,
                            None,
                            config,
                            replication,
                        )?;
                    }
                    inserted += 1;
                }
            }
        }
    }
    Ok(json!({
        "results": [],
        "target": spec.target,
        "matched": matched,
        "modified": modified,
        "inserted": inserted,
        "skipped": skipped,
    }))
}

fn aggregate_write(
    store: &Store,
    wtxn: &mut nonodb_core::heed::RwTxn<'_>,
    coll: &str,
    pipeline: &[Value],
    max_time_ms: Option<u64>,
    database: &str,
    replication: &mut ReplicationContext,
) -> Result<Value, ApiError> {
    let spec = nonodb_core::aggregate::write_stage_spec(pipeline)?.ok_or_else(|| ApiError {
        code: "invalid_query".into(),
        message: "aggregation has no write stage".into(),
    })?;
    if let Some(target_database) = &spec.database {
        if target_database != database {
            return Err(ApiError {
                code: "invalid_query".into(),
                message: "cross-database $out/$merge is not supported".into(),
            });
        }
    }
    let docs = match store.collection_at(&*wtxn, coll)? {
        Some(source) => {
            if source.profile().is_vault() {
                return Err(vault::unsupported("aggregation on vault collections").into());
            }
            source.aggregate_in_with_options(
                &*wtxn,
                &pipeline[..pipeline.len() - 1],
                max_time_ms,
            )?
        }
        None => Vec::new(),
    };
    match spec.kind {
        nonodb_core::aggregate::AggregateWriteKind::Out => {
            aggregate_out(store, wtxn, &spec, docs, replication)
        }
        nonodb_core::aggregate::AggregateWriteKind::Merge => {
            aggregate_merge(store, wtxn, &spec, docs, replication)
        }
    }
}

fn execute(
    store: &Store,
    wtxn: &mut nonodb_core::heed::RwTxn<'_>,
    req: &WriteReq,
    replication: &mut ReplicationContext,
) -> Result<Value, ApiError> {
    match req {
        WriteReq::CreateCollection {
            coll,
            profile,
            ttl_seconds,
            ttl_purge_history,
            timeseries,
        } => {
            let c = store.create_collection_with_ttl_and_purge_in(
                wtxn,
                coll,
                *profile,
                *ttl_seconds,
                *ttl_purge_history,
            )?;
            if let Some(settings) = timeseries {
                c.set_timeseries_settings_in(wtxn, settings)?;
            }
            replication.capture_op(ReplicationOp::CollectionCreate {
                name: coll.clone(),
                profile: profile.as_str().to_string(),
                ttl_seconds: *ttl_seconds,
                ttl_field: None,
                ttl_purge_history: *ttl_purge_history,
                timeseries: timeseries
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(nonodb_core::Error::from)?,
            });
            Ok(json!({
                "created": coll,
                "profile": profile.as_str(),
                "ttl_seconds": c.ttl_default_seconds(),
                "purge_history": c.ttl_purge_history(),
            }))
        }
        WriteReq::DropCollection { coll } => {
            let dropped = store.drop_collection_in(wtxn, coll)?;
            replication.capture_op(ReplicationOp::CollectionDrop { name: coll.clone() });
            Ok(json!({"dropped": dropped, "coll": coll}))
        }
        WriteReq::SetCollectionTtl {
            coll,
            ttl_seconds,
            purge_history,
        } => {
            store.set_collection_ttl_with_purge_in(wtxn, coll, *ttl_seconds, *purge_history)?;
            replication.capture_op(ReplicationOp::CollectionTtl {
                coll: coll.clone(),
                ttl_seconds: *ttl_seconds,
                ttl_field: None,
                purge_history: *purge_history,
            });
            Ok(json!({
                "coll": coll,
                "ttl_seconds": ttl_seconds,
                "purge_history": purge_history,
            }))
        }
        WriteReq::RetainTimeSeries { coll, before_ms } => {
            let report = store.retain_timeseries_in(wtxn, coll, *before_ms)?;
            replication.capture_op(ReplicationOp::TimeSeriesRetain {
                coll: coll.clone(),
                before_ms: *before_ms,
            });
            Ok(json!({
                "coll": coll,
                "before_ms": before_ms,
                "report": report,
            }))
        }
        WriteReq::SetCollectionTtlAfter {
            coll,
            field,
            ttl_seconds,
            purge_history,
        } => {
            store.set_collection_ttl_after_in(wtxn, coll, field, *ttl_seconds, *purge_history)?;
            replication.capture_op(ReplicationOp::CollectionTtl {
                coll: coll.clone(),
                ttl_seconds: Some(*ttl_seconds),
                ttl_field: Some(field.clone()),
                purge_history: *purge_history,
            });
            Ok(json!({
                "coll": coll,
                "ttl_seconds": ttl_seconds,
                "ttl_field": field,
                "purge_history": purge_history,
            }))
        }
        WriteReq::BackfillCollectionTtl {
            coll,
            field,
            ttl_seconds,
            after,
            limit,
            dry_run,
        } => {
            let mut page = store.backfill_collection_ttl_in(
                wtxn,
                coll,
                field,
                *ttl_seconds,
                after.as_ref(),
                *limit,
                *dry_run,
            )?;
            let entries = std::mem::take(&mut page.entries);
            if !dry_run && !entries.is_empty() {
                replication.capture_op(ReplicationOp::TtlSet {
                    coll: coll.clone(),
                    entries,
                });
            }
            Ok(json!({
                "coll": coll,
                "field": field,
                "dry_run": dry_run,
                "result": page,
            }))
        }
        WriteReq::SetCollectionHistory { coll, max_versions } => {
            store.set_collection_history_in(wtxn, coll, *max_versions)?;
            replication.capture_op(ReplicationOp::CollectionHistory {
                coll: coll.clone(),
                max_versions: *max_versions,
            });
            Ok(json!({ "coll": coll, "max_versions": max_versions }))
        }
        WriteReq::SetCollectionEvents {
            coll,
            enabled,
            fields,
            ops,
            filter,
            include_before,
            include_after,
            projection,
        } => {
            let config = events::EventConfig {
                enabled: *enabled,
                fields: fields.clone(),
                ops: ops.clone(),
                filter: filter.clone(),
                include_before: *include_before,
                include_after: *include_after,
                projection: projection.clone(),
            };
            store.set_collection_events_in(wtxn, coll, &config)?;
            replication.capture_op(ReplicationOp::CollectionEvents {
                coll: coll.clone(),
                config: config.clone(),
            });
            Ok(json!({
                "coll": coll,
                "enabled": enabled,
                "fields": fields,
                "ops": ops,
                "filter": filter,
                "include_before": include_before,
                "include_after": include_after,
                "projection": projection,
            }))
        }
        WriteReq::CreateSubscription { coll, subscription } => {
            let out = store.create_subscription_in(wtxn, coll, subscription)?;
            replication.capture_op(ReplicationOp::SubscriptionUpsert {
                coll: coll.clone(),
                subscription: out.clone(),
            });
            Ok(serde_json::to_value(out).map_err(nonodb_core::Error::from)?)
        }
        WriteReq::RenewSubscription {
            coll,
            name,
            lease_seconds,
        } => {
            let out = store.renew_subscription_in(wtxn, coll, name, *lease_seconds)?;
            if let Some(subscription) = &out {
                replication.capture_op(ReplicationOp::SubscriptionUpsert {
                    coll: coll.clone(),
                    subscription: subscription.clone(),
                });
            }
            Ok(json!({ "subscription": out }))
        }
        WriteReq::PruneSubscriptions => {
            let removed = store.prune_expired_subscriptions_in(wtxn)?;
            Ok(json!({ "removed": removed }))
        }
        WriteReq::DeleteSubscription { coll, name } => {
            let deleted = store.delete_subscription_in(wtxn, coll, name)?;
            if deleted {
                replication.capture_op(ReplicationOp::SubscriptionDelete {
                    coll: coll.clone(),
                    name: name.clone(),
                });
            }
            Ok(json!({ "deleted": deleted }))
        }
        WriteReq::CreateWebhook { rule } => {
            rule.validate().map_err(|message| ApiError {
                code: "invalid_query".into(),
                message,
            })?;
            let _ = store.collection_in(wtxn, &rule.coll)?;
            crate::webhooks::ensure_events(store, wtxn, &rule.coll).map_err(ApiError::internal)?;
            let mut rule = rule.clone();
            if rule.start_cursor.is_empty() {
                let events = store.collection_in(wtxn, events::EVENTS_COLLECTION)?;
                let latest = events.find_in(
                    &*wtxn,
                    &FindQuery {
                        filter: json!({}),
                        projection: Some(json!({"_id": 1})),
                        sort: vec![("_id".to_string(), -1)],
                        skip: 0,
                        limit: Some(1),
                        comment: None,
                        max_time_ms: None,
                        hint: None,
                    },
                )?;
                rule.start_cursor = latest
                    .first()
                    .and_then(|doc| doc.get("_id"))
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
            }
            let collection = store.collection_in(wtxn, crate::webhooks::WEBHOOKS_COLLECTION)?;
            let _ = collection.delete_txn(wtxn, &json!({"_id": rule.name}), false)?;
            let mut doc = serde_json::to_value(&rule).map_err(nonodb_core::Error::from)?;
            doc["_id"] = json!(rule.name);
            collection.insert_one_txn(wtxn, doc)?;
            Ok(serde_json::to_value(&rule).map_err(nonodb_core::Error::from)?)
        }
        WriteReq::DeleteWebhook { name } => {
            let collection = store.collection_in(wtxn, crate::webhooks::WEBHOOKS_COLLECTION)?;
            let deleted = collection.delete_txn(wtxn, &json!({"_id": name}), false)? > 0;
            Ok(json!({ "deleted": deleted }))
        }
        WriteReq::RetryWebhookDelivery { id } => {
            let collection = store.collection_in(wtxn, crate::webhooks::DELIVERIES_COLLECTION)?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let retried = collection.update_txn(
                wtxn,
                &json!({"_id": id}),
                &json!({"$set": {"status": "pending", "next_attempt_ms": 0, "updated_at": now}}),
                false,
            )?;
            Ok(json!({ "retried": retried > 0 }))
        }
        WriteReq::Expire { entries } => {
            let mut expired = 0usize;
            for entry in entries {
                if store.collection_profile_in_rw(wtxn, &entry.coll)?.is_none() {
                    store.ttl_remove_in(wtxn, &entry.coll, &entry.id)?;
                    continue;
                }
                let c = store.collection_in(wtxn, &entry.coll)?;
                let config = event_config(store, wtxn, &entry.coll)?;
                let vault_before = if c.profile().is_vault() {
                    c.vault_stored_txn(&*wtxn, &entry.id)?
                } else {
                    None
                };
                if let Some(before) =
                    c.expire_txn_with_doc(wtxn, &entry.id, entry.expected_ttl_at)?
                {
                    if let Some(before) = vault_before.as_ref() {
                        replication.capture_vault(
                            &entry.coll,
                            "delete",
                            &entry.id,
                            Some(before),
                            None,
                        );
                    }
                    if let Some(config) = config.as_ref() {
                        if !c.profile().is_vault() {
                            emit_event(
                                store,
                                wtxn,
                                &entry.coll,
                                "delete",
                                &entry.id,
                                Some(&before),
                                None,
                                config,
                                replication,
                            )?;
                        }
                    }
                    expired += 1;
                }
            }
            Ok(json!({ "expired": expired }))
        }
        WriteReq::PruneHistory { limit } => {
            let deleted = store.prune_history_in(wtxn, *limit)?;
            Ok(json!({ "pruned": deleted }))
        }
        WriteReq::Insert { coll, doc, vault } => {
            let c = write_collection(store, wtxn, coll, vault.as_ref())?;
            let config = event_config(store, wtxn, coll)?;
            if c.profile().is_vault() {
                let auth = vault.as_ref().ok_or_else(vault::key_required)?;
                let id =
                    c.insert_vault_txn(wtxn, doc.clone(), &auth.key, &auth.tenant, &auth.kid)?;
                if let Some(after) = c.vault_stored_txn(&*wtxn, &id)? {
                    replication.capture_vault(coll, "insert", &id, None, Some(&after));
                }
                Ok(json!({ "inserted_id": id }))
            } else {
                if vault.is_some() {
                    return Err(vault::key_unexpected().into());
                }
                let inserted_id = doc.get("_id").cloned();
                let id = c.insert_one_txn(wtxn, doc.clone())?;
                if let Some(config) = &config {
                    emit_by_id(
                        store,
                        wtxn,
                        &c,
                        coll,
                        "insert",
                        &id,
                        None,
                        config,
                        replication,
                    )?;
                }
                let inserted_id = inserted_id.unwrap_or(Value::String(id));
                Ok(json!({ "inserted_id": inserted_id }))
            }
        }
        WriteReq::InsertMany { coll, docs, vault } => {
            let c = write_collection(store, wtxn, coll, vault.as_ref())?;
            let config = event_config(store, wtxn, coll)?;
            if c.profile().is_vault() {
                let auth = vault.as_ref().ok_or_else(vault::key_required)?;
                let ids =
                    c.insert_many_vault_txn(wtxn, docs, &auth.key, &auth.tenant, &auth.kid)?;
                for id in &ids {
                    if let Some(after) = c.vault_stored_txn(&*wtxn, id)? {
                        replication.capture_vault(coll, "insert", id, None, Some(&after));
                    }
                }
                Ok(json!({ "inserted_ids": ids }))
            } else {
                if vault.is_some() {
                    return Err(vault::key_unexpected().into());
                }
                let mut ids = Vec::with_capacity(docs.len());
                let mut inserted_ids = Vec::with_capacity(docs.len());
                for doc in docs {
                    let inserted_id = doc.get("_id").cloned();
                    let id = c.insert_one_txn(wtxn, doc.clone())?;
                    inserted_ids.push(inserted_id.unwrap_or_else(|| Value::String(id.clone())));
                    ids.push(id);
                }
                if let Some(config) = &config {
                    for id in &ids {
                        emit_by_id(
                            store,
                            wtxn,
                            &c,
                            coll,
                            "insert",
                            id,
                            None,
                            config,
                            replication,
                        )?;
                    }
                }
                Ok(json!({ "inserted_ids": inserted_ids }))
            }
        }
        WriteReq::Update {
            coll,
            filter,
            update,
            multi,
            upsert,
            array_filters,
            default_id,
            vault,
        } => {
            let c = write_collection(store, wtxn, coll, vault.as_ref())?;
            let config = event_config(store, wtxn, coll)?;
            let before_docs = if config.is_some() && !c.profile().is_vault() {
                c.find_in(
                    &*wtxn,
                    &FindQuery {
                        filter: filter.clone(),
                        projection: None,
                        sort: Vec::new(),
                        skip: 0,
                        limit: if *multi { None } else { Some(1) },
                        comment: None,
                        max_time_ms: None,
                        hint: None,
                    },
                )?
            } else {
                Vec::new()
            };
            let vault_before = if c.profile().is_vault() {
                let auth = vault.as_ref().ok_or_else(vault::key_required)?;
                let id = vault::exact_id_filter(filter, &auth.tenant).ok();
                match id {
                    Some(id) => c.vault_stored_txn(&*wtxn, &id)?.map(|doc| (id, doc)),
                    None => None,
                }
            } else {
                None
            };
            let out = if c.profile().is_vault() {
                let auth = vault.as_ref().ok_or_else(vault::key_required)?;
                c.update_vault_txn_filtered(
                    wtxn,
                    filter,
                    update,
                    *multi,
                    *upsert,
                    &auth.key,
                    &auth.tenant,
                    &auth.kid,
                    array_filters,
                )?
            } else {
                if vault.is_some() {
                    return Err(vault::key_unexpected().into());
                }
                c.update_opts_txn_filtered_with_id(
                    wtxn,
                    filter,
                    update,
                    *multi,
                    *upsert,
                    array_filters,
                    default_id.as_ref(),
                )?
            };
            if let Some(config) = &config {
                if !c.profile().is_vault() {
                    if let Some(id) = &out.upserted_id {
                        emit_by_id(
                            store,
                            wtxn,
                            &c,
                            coll,
                            "insert",
                            id,
                            None,
                            config,
                            replication,
                        )?;
                    } else {
                        for before in &before_docs {
                            let id = id::key_from_document(before).ok_or_else(|| ApiError {
                                code: "invalid_id".into(),
                                message: "document has no valid _id".into(),
                            })?;
                            if out.moved.iter().any(|(old, _)| old == &id) {
                                continue;
                            }
                            emit_by_id(
                                store,
                                wtxn,
                                &c,
                                coll,
                                "update",
                                &id,
                                Some(before),
                                config,
                                replication,
                            )?;
                        }
                        // Time-series rows that moved buckets: replay as delete+insert.
                        for (old, new_id) in &out.moved {
                            emit_event(
                                store,
                                wtxn,
                                coll,
                                "delete",
                                old,
                                None,
                                None,
                                config,
                                replication,
                            )?;
                            emit_by_id(
                                store,
                                wtxn,
                                &c,
                                coll,
                                "insert",
                                new_id,
                                None,
                                config,
                                replication,
                            )?;
                        }
                    }
                }
            }
            if c.profile().is_vault() {
                let id = out
                    .upserted_id
                    .clone()
                    .or_else(|| vault_before.as_ref().map(|(id, _)| id.clone()));
                if let Some(id) = id {
                    let after = c.vault_stored_txn(&*wtxn, &id)?;
                    let op = if vault_before.is_some() {
                        "update"
                    } else {
                        "insert"
                    };
                    replication.capture_vault(
                        coll,
                        op,
                        &id,
                        vault_before.as_ref().map(|(_, doc)| doc),
                        after.as_ref(),
                    );
                }
            }
            Ok(json!({
                "matched": out.matched,
                "modified": out.modified,
                "upserted_id": out.upserted_id,
            }))
        }
        WriteReq::FindAndModify {
            coll,
            filter,
            update,
            sort,
            new,
            upsert,
            remove,
            array_filters,
            projection,
            default_id,
            vault,
        } => {
            let c = write_collection(store, wtxn, coll, vault.as_ref())?;
            let config = event_config(store, wtxn, coll)?;
            let before = if config.is_some() && !c.profile().is_vault() {
                c.find_in(
                    &*wtxn,
                    &FindQuery {
                        filter: filter.clone(),
                        projection: None,
                        sort: sort.clone(),
                        skip: 0,
                        limit: Some(1),
                        comment: None,
                        max_time_ms: None,
                        hint: None,
                    },
                )?
                .into_iter()
                .next()
            } else {
                None
            };
            let opts = FindAndModifyOptions {
                filter,
                update: update.as_ref(),
                sort,
                new: *new,
                upsert: *upsert,
                remove: *remove,
                array_filters,
                projection: projection.as_ref(),
            };
            let vault_before = if c.profile().is_vault() {
                let auth = vault.as_ref().ok_or_else(vault::key_required)?;
                let id = vault::exact_id_filter(filter, &auth.tenant).ok();
                match id {
                    Some(id) => c.vault_stored_txn(&*wtxn, &id)?.map(|doc| (id, doc)),
                    None => None,
                }
            } else {
                None
            };
            let out = if c.profile().is_vault() {
                let auth = vault.as_ref().ok_or_else(vault::key_required)?;
                c.find_and_modify_vault_txn(wtxn, &opts, &auth.key, &auth.tenant, &auth.kid)?
            } else {
                if vault.is_some() {
                    return Err(vault::key_unexpected().into());
                }
                c.find_and_modify_txn_with_id(wtxn, &opts, default_id.as_ref())?
            };
            if let Some(config) = &config {
                if !c.profile().is_vault() {
                    if let Some(id) = &out.upserted_id {
                        emit_by_id(
                            store,
                            wtxn,
                            &c,
                            coll,
                            "insert",
                            id,
                            None,
                            config,
                            replication,
                        )?;
                    } else if let Some(before) = &before {
                        let id =
                            before
                                .get("_id")
                                .and_then(Value::as_str)
                                .ok_or_else(|| ApiError {
                                    code: "invalid_id".into(),
                                    message: "document has no string _id".into(),
                                })?;
                        if *remove {
                            emit_event(
                                store,
                                wtxn,
                                coll,
                                "delete",
                                id,
                                Some(before),
                                None,
                                config,
                                replication,
                            )?;
                        } else if out.modified > 0 {
                            let moved = out.new_id.as_deref().is_some_and(|new_id| new_id != id);
                            if moved {
                                emit_event(
                                    store,
                                    wtxn,
                                    coll,
                                    "delete",
                                    id,
                                    Some(before),
                                    None,
                                    config,
                                    replication,
                                )?;
                                if let Some(new_id) = &out.new_id {
                                    emit_by_id(
                                        store,
                                        wtxn,
                                        &c,
                                        coll,
                                        "insert",
                                        new_id,
                                        None,
                                        config,
                                        replication,
                                    )?;
                                }
                            } else {
                                emit_by_id(
                                    store,
                                    wtxn,
                                    &c,
                                    coll,
                                    "update",
                                    id,
                                    Some(before),
                                    config,
                                    replication,
                                )?;
                            }
                        }
                    }
                }
            }
            if c.profile().is_vault() {
                let id = out
                    .upserted_id
                    .clone()
                    .or_else(|| vault_before.as_ref().map(|(id, _)| id.clone()));
                if let Some(id) = id {
                    let after = c.vault_stored_txn(&*wtxn, &id)?;
                    let op = if *remove {
                        "delete"
                    } else if vault_before.is_some() {
                        "update"
                    } else {
                        "insert"
                    };
                    replication.capture_vault(
                        coll,
                        op,
                        &id,
                        vault_before.as_ref().map(|(_, doc)| doc),
                        after.as_ref(),
                    );
                }
            }
            Ok(json!({
                "value": out.value,
                "matched": out.matched,
                "modified": out.modified,
                "upserted_id": out.upserted_id,
            }))
        }
        WriteReq::Delete {
            coll,
            filter,
            multi,
            vault,
        } => {
            let c = write_collection(store, wtxn, coll, vault.as_ref())?;
            let config = event_config(store, wtxn, coll)?;
            let before_docs = if config.is_some() && !c.profile().is_vault() {
                c.find_in(
                    &*wtxn,
                    &FindQuery {
                        filter: filter.clone(),
                        projection: None,
                        sort: Vec::new(),
                        skip: 0,
                        limit: if *multi { None } else { Some(1) },
                        comment: None,
                        max_time_ms: None,
                        hint: None,
                    },
                )?
            } else {
                Vec::new()
            };
            let vault_before = if c.profile().is_vault() {
                let auth = vault.as_ref().ok_or_else(vault::key_required)?;
                let id = vault::exact_id_filter(filter, &auth.tenant).ok();
                match id {
                    Some(id) => c.vault_stored_txn(&*wtxn, &id)?.map(|doc| (id, doc)),
                    None => None,
                }
            } else {
                None
            };
            let n = if c.profile().is_vault() {
                let auth = vault.as_ref().ok_or_else(vault::key_required)?;
                c.delete_vault_txn(wtxn, filter, *multi, &auth.key, &auth.tenant, &auth.kid)?
            } else {
                if vault.is_some() {
                    return Err(vault::key_unexpected().into());
                }
                c.delete_txn(wtxn, filter, *multi)?
            };
            if let Some(config) = &config {
                if !c.profile().is_vault() {
                    for before in &before_docs {
                        let id = id::key_from_document(before).ok_or_else(|| ApiError {
                            code: "invalid_id".into(),
                            message: "document has no valid _id".into(),
                        })?;
                        emit_event(
                            store,
                            wtxn,
                            coll,
                            "delete",
                            &id,
                            Some(before),
                            None,
                            config,
                            replication,
                        )?;
                    }
                }
            }
            if c.profile().is_vault() {
                if let Some((id, before)) = vault_before.as_ref() {
                    replication.capture_vault(coll, "delete", id, Some(before), None);
                }
            }
            Ok(json!({ "deleted": n }))
        }
        WriteReq::CreateIndex {
            coll,
            fields,
            unique,
            include,
            geo,
        } => {
            let c = standard_collection(store, wtxn, coll)?;
            c.create_index_fields_txn(wtxn, fields, *unique, include, *geo)?;
            replication.capture_op(ReplicationOp::IndexCreate {
                coll: coll.clone(),
                fields: fields.clone(),
                unique: *unique,
                include: include.clone(),
                geo: *geo,
            });
            let name = fields.join("+");
            Ok(json!({
                "created": name,
                "fields": fields,
                "unique": unique,
                "include": include,
                "geo": geo
            }))
        }
        WriteReq::CreateIndexes { coll, indexes } => {
            let c = standard_collection(store, wtxn, coll)?;
            c.create_indexes_txn(wtxn, indexes)?;
            for index in indexes {
                replication.capture_op(ReplicationOp::IndexCreate {
                    coll: coll.clone(),
                    fields: index.fields.clone(),
                    unique: index.unique,
                    include: index.include.clone(),
                    geo: index.geo,
                });
            }
            let names: Vec<_> = indexes.iter().map(|index| index.fields.join("+")).collect();
            Ok(json!({ "created": names }))
        }
        WriteReq::CreateSearchIndex { coll, index } => {
            let c = standard_collection(store, wtxn, coll)?;
            c.create_search_index_txn(wtxn, index)?;
            replication.capture_op(ReplicationOp::SearchIndexCreate {
                coll: coll.clone(),
                index: index.clone(),
            });
            Ok(json!({
                "created": index.name,
                "kind": index.kind.as_str(),
                "fields": index.fields
            }))
        }
        WriteReq::DropSearchIndex { coll, kind, name } => {
            let c = standard_collection(store, wtxn, coll)?;
            let existed = c.drop_search_index_txn(wtxn, *kind, name)?;
            if existed {
                replication.capture_op(ReplicationOp::SearchIndexDrop {
                    coll: coll.clone(),
                    index_kind: *kind,
                    name: name.clone(),
                });
            }
            Ok(json!({ "dropped": existed, "kind": kind.as_str(), "name": name }))
        }
        WriteReq::DropIndex { coll, field } => {
            let c = standard_collection(store, wtxn, coll)?;
            let existed = c.drop_index_txn(wtxn, field)?;
            if existed {
                replication.capture_op(ReplicationOp::IndexDrop {
                    coll: coll.clone(),
                    field: field.clone(),
                });
            }
            Ok(json!({ "dropped": existed }))
        }
        WriteReq::CreateTextIndex { coll, field } => {
            let c = standard_collection(store, wtxn, coll)?;
            let index = nonodb_core::SearchIndexDef::single(nonodb_core::SearchKind::Text, field);
            c.create_search_index_txn(wtxn, &index)?;
            replication.capture_op(ReplicationOp::SearchIndexCreate {
                coll: coll.clone(),
                index,
            });
            Ok(json!({ "created": field, "kind": "text" }))
        }
        WriteReq::DropTextIndex { coll, field } => {
            let c = standard_collection(store, wtxn, coll)?;
            let existed = c.drop_search_index_txn(wtxn, nonodb_core::SearchKind::Text, field)?;
            if existed {
                replication.capture_op(ReplicationOp::SearchIndexDrop {
                    coll: coll.clone(),
                    index_kind: nonodb_core::SearchKind::Text,
                    name: field.clone(),
                });
            }
            Ok(json!({ "dropped": existed, "kind": "text" }))
        }
        WriteReq::CreateFuzzyIndex { coll, field } => {
            let c = standard_collection(store, wtxn, coll)?;
            let index = nonodb_core::SearchIndexDef::single(nonodb_core::SearchKind::Fuzzy, field);
            c.create_search_index_txn(wtxn, &index)?;
            replication.capture_op(ReplicationOp::SearchIndexCreate {
                coll: coll.clone(),
                index,
            });
            Ok(json!({ "created": field, "kind": "fuzzy" }))
        }
        WriteReq::DropFuzzyIndex { coll, field } => {
            let c = standard_collection(store, wtxn, coll)?;
            let existed = c.drop_search_index_txn(wtxn, nonodb_core::SearchKind::Fuzzy, field)?;
            if existed {
                replication.capture_op(ReplicationOp::SearchIndexDrop {
                    coll: coll.clone(),
                    index_kind: nonodb_core::SearchKind::Fuzzy,
                    name: field.clone(),
                });
            }
            Ok(json!({ "dropped": existed, "kind": "fuzzy" }))
        }
        WriteReq::CreateVectorIndex {
            coll,
            field,
            dims,
            metric,
        } => {
            let c = standard_collection(store, wtxn, coll)?;
            c.create_vector_index_txn(wtxn, field, *dims, *metric)?;
            replication.capture_op(ReplicationOp::VectorIndexCreate {
                coll: coll.clone(),
                field: field.clone(),
                dims: *dims,
                metric: *metric,
            });
            Ok(json!({
                "created": field,
                "kind": "vector",
                "dims": dims,
                "metric": metric.as_str()
            }))
        }
        WriteReq::Reindex { coll } => {
            let c = standard_collection(store, wtxn, coll)?;
            let report = c.reindex_txn(wtxn)?;
            Ok(json!({
                "reindexed": coll,
                "secondary": report.secondary,
                "text": report.text,
                "fuzzy": report.fuzzy,
                "vector": report.vector,
            }))
        }
        WriteReq::DropVectorIndex { coll, field } => {
            let c = standard_collection(store, wtxn, coll)?;
            let existed = c.drop_vector_index_txn(wtxn, field)?;
            if existed {
                replication.capture_op(ReplicationOp::VectorIndexDrop {
                    coll: coll.clone(),
                    field: field.clone(),
                });
            }
            Ok(json!({ "dropped": existed, "kind": "vector" }))
        }
        WriteReq::Sql { query, params } => {
            let prepared = nonodb_core::sql::PreparedSql::parse(query)?;
            if prepared.is_read_only() {
                return Err(ApiError {
                    code: "invalid_sql".into(),
                    message: "SQL writes require INSERT or UPDATE".into(),
                });
            }
            let c = standard_collection(store, wtxn, prepared.collection())?;
            let result = prepared.execute_write(&c, wtxn, params)?;
            Ok(json!({ "results": [result] }))
        }
        WriteReq::Aggregate {
            coll,
            pipeline,
            max_time_ms,
            database,
        } => aggregate_write(
            store,
            wtxn,
            coll,
            pipeline,
            *max_time_ms,
            database,
            replication,
        ),
        WriteReq::Batch { ops } => {
            let mut results = Vec::with_capacity(ops.len());
            for (idx, op) in ops.iter().enumerate() {
                match execute(store, wtxn, op, replication) {
                    Ok(value) => results.push(value),
                    Err(e) => {
                        return Err(ApiError {
                            code: e.code,
                            message: format!("write_batch operation {idx} failed: {}", e.message),
                        });
                    }
                }
            }
            Ok(json!({ "results": results }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nonodb_core::{IndexSpec, Store};
    use serde_json::json;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Store) {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path()).unwrap();
        (dir, store)
    }

    fn execute_commit(store: &Store, req: &WriteReq) -> Result<Value, ApiError> {
        let mut wtxn = store.write_txn().unwrap();
        let mut replication = ReplicationContext {
            origin: None,
            changes: Vec::new(),
            ops: Vec::new(),
        };
        let result = execute(store, &mut wtxn, req, &mut replication);
        match &result {
            Ok(_) => wtxn.commit().unwrap(),
            Err(_) => drop(wtxn),
        }
        result
    }

    fn all(store: &Store, coll: &str) -> Vec<Value> {
        let c = store.collection(coll).unwrap();
        c.find(&FindQuery {
            filter: json!({}),
            projection: None,
            sort: vec![("_id".to_string(), 1)],
            skip: 0,
            limit: None,
            comment: None,
            max_time_ms: None,
            hint: None,
        })
        .unwrap()
    }

    #[test]
    fn drop_collection_is_idempotent() {
        let (_dir, store) = setup();
        let c = store.collection("drop_me").unwrap();
        c.insert_one(json!({"_id": "a", "value": 1})).unwrap();

        let out = execute_commit(
            &store,
            &WriteReq::DropCollection {
                coll: "drop_me".to_string(),
            },
        )
        .unwrap();
        assert_eq!(out["dropped"], true);
        assert!(!store
            .collections()
            .unwrap()
            .iter()
            .any(|name| name == "drop_me"));

        let out = execute_commit(
            &store,
            &WriteReq::DropCollection {
                coll: "drop_me".to_string(),
            },
        )
        .unwrap();
        assert_eq!(out["dropped"], false);
    }

    #[test]
    fn out_replaces_target_transactionally() {
        let (_dir, store) = setup();
        let source = store.collection("source").unwrap();
        source
            .insert_one(json!({"_id": "a", "keep": true, "n": 1}))
            .unwrap();
        source
            .insert_one(json!({"_id": "b", "keep": false, "n": 2}))
            .unwrap();
        let target = store.collection("target").unwrap();
        target.insert_one(json!({"_id": "old", "n": 9})).unwrap();

        let out = execute_commit(
            &store,
            &WriteReq::Aggregate {
                coll: "source".to_string(),
                pipeline: vec![
                    json!({"$match": {"keep": true}}),
                    json!({"$project": {"keep": 1, "n": 1}}),
                    json!({"$out": "target"}),
                ],
                max_time_ms: None,
                database: "default".to_string(),
            },
        )
        .unwrap();
        assert_eq!(out["inserted"], 1);
        assert_eq!(out["deleted"], 1);
        let docs = all(&store, "target");
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["keep"], true);
        assert_eq!(docs[0]["n"], 1);
        assert!(docs[0].get("_id").is_some());

        let source_docs = all(&store, "source");
        assert_eq!(source_docs.len(), 2);
    }

    #[test]
    fn merge_updates_inserts_and_preserves_unique_index() {
        let (_dir, store) = setup();
        let source = store.collection("source").unwrap();
        source
            .insert_one(json!({"k": 1, "val": 2, "extra": "x"}))
            .unwrap();
        source.insert_one(json!({"k": 2, "val": 3})).unwrap();
        source.insert_one(json!({"k": 3, "val": 4})).unwrap();
        let target = store.collection("target").unwrap();
        target
            .insert_one(json!({"_id": "old", "k": 1, "keep": true, "val": 1}))
            .unwrap();
        target
            .create_indexes(&[IndexSpec {
                fields: vec!["k".to_string()],
                name: String::new(),
                directions: Vec::new(),
                unique: true,
                include: Vec::new(),
                geo: false,
            }])
            .unwrap();

        let out = execute_commit(
            &store,
            &WriteReq::Aggregate {
                coll: "source".to_string(),
                pipeline: vec![
                    json!({"$unset": "_id"}),
                    json!({"$merge": {
                        "into": "target",
                        "on": "k",
                        "whenMatched": "merge",
                        "whenNotMatched": "insert"
                    }}),
                ],
                max_time_ms: None,
                database: "default".to_string(),
            },
        )
        .unwrap();
        assert_eq!(out["matched"], 1);
        assert_eq!(out["inserted"], 2);
        let docs = all(&store, "target");
        assert_eq!(docs.len(), 3);
        let first = docs.iter().find(|doc| doc["k"] == 1).unwrap();
        assert_eq!(first["_id"], "old");
        assert_eq!(first["keep"], true);
        assert_eq!(first["val"], 2);
        assert_eq!(first["extra"], "x");

        let out = execute_commit(
            &store,
            &WriteReq::Aggregate {
                coll: "source".to_string(),
                pipeline: vec![
                    json!({"$unset": "_id"}),
                    json!({"$merge": {
                        "into": "target",
                        "on": "k",
                        "whenMatched": "replace",
                        "whenNotMatched": "discard"
                    }}),
                ],
                max_time_ms: None,
                database: "default".to_string(),
            },
        )
        .unwrap();
        assert_eq!(out["matched"], 3);
        assert_eq!(out["inserted"], 0);
        assert_eq!(out["skipped"], 0);
        let docs = all(&store, "target");
        assert_eq!(docs.len(), 3);
        let first = docs.iter().find(|doc| doc["k"] == 1).unwrap();
        assert_eq!(first["_id"], "old");
        assert_eq!(first["val"], 2);
        assert!(first.get("keep").is_none());

        let out = execute_commit(
            &store,
            &WriteReq::Aggregate {
                coll: "source".to_string(),
                pipeline: vec![
                    json!({"$unset": "_id"}),
                    json!({"$merge": {
                        "into": "target",
                        "on": "k",
                        "let": {"threshold": 100},
                        "whenMatched": [
                            {"$set": {
                                "val": {"$add": ["$val", "$$new.val"]},
                                "threshold": "$$threshold"
                            }},
                            {"$project": {"k": 1, "val": 1, "threshold": 1}}
                        ],
                        "whenNotMatched": "discard"
                    }}),
                ],
                max_time_ms: None,
                database: "default".to_string(),
            },
        )
        .unwrap();
        assert_eq!(out["matched"], 3);
        assert_eq!(out["modified"], 3);
        assert_eq!(out["inserted"], 0);
        let docs = all(&store, "target");
        assert_eq!(docs.len(), 3);
        for (k, expected) in [(1, 4), (2, 6), (3, 8)] {
            let doc = docs.iter().find(|doc| doc["k"] == k).unwrap();
            assert_eq!(doc["val"], expected);
            assert_eq!(doc["threshold"], 100);
        }

        let result = execute_commit(
            &store,
            &WriteReq::Aggregate {
                coll: "source".to_string(),
                pipeline: vec![
                    json!({"$unset": "_id"}),
                    json!({"$merge": {
                        "into": "target",
                        "on": "k",
                        "whenMatched": [{"$set": {"_id": "changed"}}]
                    }}),
                ],
                max_time_ms: None,
                database: "default".to_string(),
            },
        );
        assert!(result.is_err());
    }

    #[test]
    fn merge_failures_abort_the_transaction() {
        let (_dir, store) = setup();
        let source = store.collection("source").unwrap();
        source.insert_one(json!({"_id": "s1", "k": 1})).unwrap();
        let target = store.collection("target").unwrap();
        target.insert_one(json!({"_id": "old", "k": 1})).unwrap();
        target
            .create_indexes(&[IndexSpec {
                fields: vec!["k".to_string()],
                name: String::new(),
                directions: Vec::new(),
                unique: true,
                include: Vec::new(),
                geo: false,
            }])
            .unwrap();

        let result = execute_commit(
            &store,
            &WriteReq::Aggregate {
                coll: "source".to_string(),
                pipeline: vec![
                    json!({"$unset": "_id"}),
                    json!({"$merge": {
                        "into": "target",
                        "on": "k",
                        "whenMatched": "fail"
                    }}),
                ],
                max_time_ms: None,
                database: "default".to_string(),
            },
        );
        assert!(result.is_err());
        let docs = all(&store, "target");
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["_id"], "old");
    }
}
