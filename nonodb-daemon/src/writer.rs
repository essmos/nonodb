//! Jeden pisarz + group commit.
//!
//! All write operations go through an `mpsc` channel to one thread. The thread
//! collects a batch of requests (up to `MAX_BATCH` in a `BATCH_WINDOW`), executes
//! them in **one** LMDB transaction and performs **one** `commit` - that is, one
//! `fsync` per batch. The client response is sent only after the commit.

use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use nonodb_core::{
    events, vault, Collection, CollectionProfile, FindAndModifyOptions, FindQuery, Store,
};
use serde_json::{json, Value};
use tokio::sync::oneshot;

use crate::cache::QueryCache;
use crate::live;
use crate::metrics;
use crate::protocol::{ApiError, Durability, WriteReq};

/// Maximum number of operations in one transaction.
const MAX_BATCH: usize = 512;
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

fn run_batch(store: &Store, cache: &QueryCache, jobs: Vec<WriteJob>) {
    metrics::sub(&metrics::QUEUE_LEN, jobs.len() as u64);
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

    let mut results = Vec::with_capacity(jobs.len());
    for job in &jobs {
        results.push(execute(store, &mut wtxn, &job.req));
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
    let started = Instant::now();
    match execute(store, &mut wtxn, &job.req) {
        Ok(value) => match wtxn.commit_unsynced() {
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
        },
        Err(e) => {
            // Drop `wtxn` = abort; for `WriteReq::Batch` this guarantees
            // all-or-nothing.
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
            Some(nonodb_core::CollectionProfile::Standard) => {
                return Err(vault::key_unexpected().into())
            }
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
    if coll == events::EVENTS_COLLECTION
        || matches!(
            store.collection_profile_in_rw(wtxn, coll)?,
            Some(CollectionProfile::Vault)
        )
    {
        return Ok(None);
    }
    let config = store.collection_events_in_rw(wtxn, coll)?;
    if config.active()
        || store.has_subscriptions_in_rw(wtxn, coll)?
        || live::registry().has_coll(store.id(), coll)
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
) -> Result<(), ApiError> {
    let target = store.collection_in(wtxn, coll)?;
    target.evaluate_subscriptions(wtxn, op, id, before, after)?;
    live::collect_pending(&target, wtxn, coll, op, id, before, after)?;
    let Some(event) = events::change_event(coll, op, id, before, after, config) else {
        return Ok(());
    };
    let outbox = store.collection_in(wtxn, events::EVENTS_COLLECTION)?;
    outbox.insert_one_txn(wtxn, event)?;
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
) -> Result<(), ApiError> {
    let after = target.find_one_in(&*wtxn, &json!({ "_id": id }), None)?;
    emit_event(store, wtxn, coll, op, id, before, after.as_ref(), config)
}

fn execute(
    store: &Store,
    wtxn: &mut nonodb_core::heed::RwTxn<'_>,
    req: &WriteReq,
) -> Result<Value, ApiError> {
    match req {
        WriteReq::CreateCollection {
            coll,
            profile,
            ttl_seconds,
            ttl_purge_history,
        } => {
            let c = store.create_collection_with_ttl_and_purge_in(
                wtxn,
                coll,
                *profile,
                *ttl_seconds,
                *ttl_purge_history,
            )?;
            Ok(json!({
                "created": coll,
                "profile": profile.as_str(),
                "ttl_seconds": c.ttl_default_seconds(),
                "purge_history": c.ttl_purge_history(),
            }))
        }
        WriteReq::SetCollectionTtl {
            coll,
            ttl_seconds,
            purge_history,
        } => {
            store.set_collection_ttl_with_purge_in(wtxn, coll, *ttl_seconds, *purge_history)?;
            Ok(json!({
                "coll": coll,
                "ttl_seconds": ttl_seconds,
                "purge_history": purge_history,
            }))
        }
        WriteReq::SetCollectionHistory { coll, max_versions } => {
            store.set_collection_history_in(wtxn, coll, *max_versions)?;
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
            Ok(serde_json::to_value(out).map_err(nonodb_core::Error::from)?)
        }
        WriteReq::RenewSubscription {
            coll,
            name,
            lease_seconds,
        } => {
            let out = store.renew_subscription_in(wtxn, coll, name, *lease_seconds)?;
            Ok(json!({ "subscription": out }))
        }
        WriteReq::PruneSubscriptions => {
            let removed = store.prune_expired_subscriptions_in(wtxn)?;
            Ok(json!({ "removed": removed }))
        }
        WriteReq::DeleteSubscription { coll, name } => {
            let deleted = store.delete_subscription_in(wtxn, coll, name)?;
            Ok(json!({ "deleted": deleted }))
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
                let before = if config.is_some() && !c.profile().is_vault() {
                    c.find_one_in(&*wtxn, &json!({ "_id": entry.id }), None)?
                } else {
                    None
                };
                if c.expire_txn(wtxn, &entry.id, entry.expected_ttl_at)? {
                    if let (Some(config), Some(before)) = (&config, before.as_ref()) {
                        emit_event(
                            store,
                            wtxn,
                            &entry.coll,
                            "delete",
                            &entry.id,
                            Some(before),
                            None,
                            config,
                        )?;
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
                Ok(json!({ "inserted_id": id }))
            } else {
                if vault.is_some() {
                    return Err(vault::key_unexpected().into());
                }
                let id = c.insert_one_txn(wtxn, doc.clone())?;
                if let Some(config) = &config {
                    emit_by_id(store, wtxn, &c, coll, "insert", &id, None, config)?;
                }
                Ok(json!({ "inserted_id": id }))
            }
        }
        WriteReq::InsertMany { coll, docs, vault } => {
            let c = write_collection(store, wtxn, coll, vault.as_ref())?;
            let config = event_config(store, wtxn, coll)?;
            if c.profile().is_vault() {
                let auth = vault.as_ref().ok_or_else(vault::key_required)?;
                let ids =
                    c.insert_many_vault_txn(wtxn, docs, &auth.key, &auth.tenant, &auth.kid)?;
                Ok(json!({ "inserted_ids": ids }))
            } else {
                if vault.is_some() {
                    return Err(vault::key_unexpected().into());
                }
                let mut ids = Vec::with_capacity(docs.len());
                for doc in docs {
                    ids.push(c.insert_one_txn(wtxn, doc.clone())?);
                }
                if let Some(config) = &config {
                    for id in &ids {
                        emit_by_id(store, wtxn, &c, coll, "insert", id, None, config)?;
                    }
                }
                Ok(json!({ "inserted_ids": ids }))
            }
        }
        WriteReq::Update {
            coll,
            filter,
            update,
            multi,
            upsert,
            array_filters,
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
                    },
                )?
            } else {
                Vec::new()
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
                c.update_opts_txn_filtered(wtxn, filter, update, *multi, *upsert, array_filters)?
            };
            if let Some(config) = &config {
                if !c.profile().is_vault() {
                    if let Some(id) = &out.upserted_id {
                        emit_by_id(store, wtxn, &c, coll, "insert", id, None, config)?;
                    } else {
                        for before in &before_docs {
                            let id =
                                before.get("_id").and_then(Value::as_str).ok_or_else(|| {
                                    ApiError {
                                        code: "invalid_id".into(),
                                        message: "document has no string _id".into(),
                                    }
                                })?;
                            emit_by_id(store, wtxn, &c, coll, "update", id, Some(before), config)?;
                        }
                    }
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
            let out = if c.profile().is_vault() {
                let auth = vault.as_ref().ok_or_else(vault::key_required)?;
                c.find_and_modify_vault_txn(wtxn, &opts, &auth.key, &auth.tenant, &auth.kid)?
            } else {
                if vault.is_some() {
                    return Err(vault::key_unexpected().into());
                }
                c.find_and_modify_txn(wtxn, &opts)?
            };
            if let Some(config) = &config {
                if !c.profile().is_vault() {
                    if let Some(id) = &out.upserted_id {
                        emit_by_id(store, wtxn, &c, coll, "insert", id, None, config)?;
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
                            )?;
                        } else if out.modified > 0 {
                            emit_by_id(store, wtxn, &c, coll, "update", id, Some(before), config)?;
                        }
                    }
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
                    },
                )?
            } else {
                Vec::new()
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
                        let id =
                            before
                                .get("_id")
                                .and_then(Value::as_str)
                                .ok_or_else(|| ApiError {
                                    code: "invalid_id".into(),
                                    message: "document has no string _id".into(),
                                })?;
                        emit_event(store, wtxn, coll, "delete", id, Some(before), None, config)?;
                    }
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
            let names: Vec<_> = indexes.iter().map(|index| index.fields.join("+")).collect();
            Ok(json!({ "created": names }))
        }
        WriteReq::DropIndex { coll, field } => {
            let c = standard_collection(store, wtxn, coll)?;
            let existed = c.drop_index_txn(wtxn, field)?;
            Ok(json!({ "dropped": existed }))
        }
        WriteReq::CreateTextIndex { coll, field } => {
            let c = standard_collection(store, wtxn, coll)?;
            c.create_text_index_txn(wtxn, field)?;
            Ok(json!({ "created": field, "kind": "text" }))
        }
        WriteReq::DropTextIndex { coll, field } => {
            let c = standard_collection(store, wtxn, coll)?;
            let existed = c.drop_text_index_txn(wtxn, field)?;
            Ok(json!({ "dropped": existed, "kind": "text" }))
        }
        WriteReq::CreateFuzzyIndex { coll, field } => {
            let c = standard_collection(store, wtxn, coll)?;
            c.create_fuzzy_index_txn(wtxn, field)?;
            Ok(json!({ "created": field, "kind": "fuzzy" }))
        }
        WriteReq::DropFuzzyIndex { coll, field } => {
            let c = standard_collection(store, wtxn, coll)?;
            let existed = c.drop_fuzzy_index_txn(wtxn, field)?;
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
            Ok(json!({
                "created": field,
                "kind": "vector",
                "dims": dims,
                "metric": metric.as_str()
            }))
        }
        WriteReq::DropVectorIndex { coll, field } => {
            let c = standard_collection(store, wtxn, coll)?;
            let existed = c.drop_vector_index_txn(wtxn, field)?;
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
        WriteReq::Batch { ops } => {
            let mut results = Vec::with_capacity(ops.len());
            for (idx, op) in ops.iter().enumerate() {
                match execute(store, wtxn, op) {
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
