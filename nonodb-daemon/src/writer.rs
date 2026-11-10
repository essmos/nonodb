//! Jeden pisarz + group commit.
//!
//! All write operations go through an `mpsc` channel to one thread. The thread
//! collects a batch of requests (up to `MAX_BATCH` in a `BATCH_WINDOW`), executes
//! them in **one** LMDB transaction and performs **one** `commit` - that is, one
//! `fsync` per batch. The client response is sent only after the commit.

use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use nonodb_core::Store;
use serde_json::{json, Value};
use tokio::sync::oneshot;

use crate::cache::QueryCache;
use crate::metrics;
use crate::protocol::{ApiError, WriteReq};

/// Maximum number of operations in one transaction.
const MAX_BATCH: usize = 512;
/// Base batch collection window (short queue = low latency).
const BASE_WINDOW: Duration = Duration::from_millis(1);
/// Maximum window under heavy load (larger batch = fewer `fsync`s).
const MAX_WINDOW: Duration = Duration::from_millis(5);
/// How often the writer checks whether old revisions should be pruned.
const PRUNE_INTERVAL: Duration = Duration::from_secs(30);
/// Maximum revisions deleted by one background pruning pass.
const PRUNE_LIMIT: usize = 1000;

pub struct WriteJob {
    pub req: WriteReq,
    pub resp: oneshot::Sender<Result<Value, ApiError>>,
    /// This operation alone in the transaction (e.g. a long auto-index backfill).
    pub isolated: bool,
}

/// Writer thread loop. Ends when the channel is closed.
pub fn run(store: Arc<Store>, rx: Receiver<WriteJob>, cache: Arc<QueryCache>) {
    let mut last_prune = Instant::now();
    loop {
        let first = match rx.recv_timeout(PRUNE_INTERVAL) {
            Ok(job) => job,
            Err(RecvTimeoutError::Timeout) => {
                maybe_prune_history(&store, &cache, &mut last_prune);
                continue;
            }
            Err(RecvTimeoutError::Disconnected) => break,
        };
        let mut jobs = vec![first];
        // Adaptive window: the longer the queue, the larger the batch (fewer fsyncs),
        // but with an empty queue we return to 1 ms (low latency).
        let pending = metrics::QUEUE_LEN.load(std::sync::atomic::Ordering::Relaxed);
        // High thresholds: with few clients, 1 ms is enough and does not
        // add latency; we extend the window only under real pressure.
        let window = if pending >= 256 {
            MAX_WINDOW
        } else if pending >= 64 {
            Duration::from_millis(3)
        } else {
            BASE_WINDOW
        };
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

        run_batch(&store, &cache, jobs);
        maybe_prune_history(&store, &cache, &mut last_prune);
    }
}

fn maybe_prune_history(store: &Store, cache: &QueryCache, last_prune: &mut Instant) {
    if last_prune.elapsed() < PRUNE_INTERVAL || !store.history_config().active() {
        return;
    }
    *last_prune = Instant::now();
    match store.prune_history(PRUNE_LIMIT) {
        Ok(0) => {}
        Ok(n) => {
            cache.invalidate();
            tracing::debug!(deleted = n, "pruned old document revisions");
        }
        Err(e) => tracing::warn!(?e, "history pruning failed"),
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
    let mut wtxn = match store.write_txn() {
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
    match wtxn.commit() {
        Ok(()) => {
            metrics::add(&metrics::COMMIT_NS, started.elapsed().as_nanos() as u64);
            cache.invalidate();
            for (job, res) in jobs.into_iter().zip(results) {
                let _ = job.resp.send(res);
            }
        }
        Err(e) => {
            metrics::add(&metrics::COMMIT_NS, started.elapsed().as_nanos() as u64);
            let err = ApiError::from(nonodb_core::Error::from(e));
            for job in jobs {
                let _ = job.resp.send(Err(err.clone()));
            }
        }
    }
}

fn run_single(store: &Store, cache: &QueryCache, job: WriteJob) {
    metrics::inc(&metrics::WRITE_BATCHES);
    let mut wtxn = match store.write_txn() {
        Ok(wtxn) => wtxn,
        Err(e) => {
            let _ = job.resp.send(Err(ApiError::from(e)));
            return;
        }
    };
    let started = Instant::now();
    match execute(store, &mut wtxn, &job.req) {
        Ok(value) => match wtxn.commit() {
            Ok(()) => {
                metrics::add(&metrics::COMMIT_NS, started.elapsed().as_nanos() as u64);
                cache.invalidate();
                let _ = job.resp.send(Ok(value));
            }
            Err(e) => {
                metrics::add(&metrics::COMMIT_NS, started.elapsed().as_nanos() as u64);
                let _ = job
                    .resp
                    .send(Err(ApiError::from(nonodb_core::Error::from(e))));
            }
        },
        Err(e) => {
            // Drop `wtxn` = abort; for `WriteReq::Batch` this guarantees
            // all-or-nothing.
            drop(wtxn);
            let _ = job.resp.send(Err(e));
        }
    }
}

fn execute(
    store: &Store,
    wtxn: &mut nonodb_core::heed::RwTxn<'_>,
    req: &WriteReq,
) -> Result<Value, ApiError> {
    match req {
        WriteReq::Insert { coll, doc } => {
            let c = store.collection_in(wtxn, coll)?;
            let id = c.insert_one_txn(wtxn, doc.clone())?;
            Ok(json!({ "inserted_id": id }))
        }
        WriteReq::InsertMany { coll, docs } => {
            let c = store.collection_in(wtxn, coll)?;
            let mut ids = Vec::with_capacity(docs.len());
            for doc in docs {
                ids.push(c.insert_one_txn(wtxn, doc.clone())?);
            }
            Ok(json!({ "inserted_ids": ids }))
        }
        WriteReq::Update {
            coll,
            filter,
            update,
            multi,
            upsert,
        } => {
            let c = store.collection_in(wtxn, coll)?;
            let out = c.update_opts_txn(wtxn, filter, update, *multi, *upsert)?;
            Ok(json!({
                "matched": out.matched,
                "modified": out.modified,
                "upserted_id": out.upserted_id,
            }))
        }
        WriteReq::Delete {
            coll,
            filter,
            multi,
        } => {
            let c = store.collection_in(wtxn, coll)?;
            let n = c.delete_txn(wtxn, filter, *multi)?;
            Ok(json!({ "deleted": n }))
        }
        WriteReq::CreateIndex {
            coll,
            fields,
            unique,
            include,
        } => {
            let c = store.collection_in(wtxn, coll)?;
            c.create_index_fields_txn(wtxn, fields, *unique, include)?;
            let name = fields.join("+");
            Ok(json!({ "created": name, "fields": fields, "unique": unique, "include": include }))
        }
        WriteReq::DropIndex { coll, field } => {
            let c = store.collection_in(wtxn, coll)?;
            let existed = c.drop_index_txn(wtxn, field)?;
            Ok(json!({ "dropped": existed }))
        }
        WriteReq::CreateTextIndex { coll, field } => {
            let c = store.collection_in(wtxn, coll)?;
            c.create_text_index_txn(wtxn, field)?;
            Ok(json!({ "created": field, "kind": "text" }))
        }
        WriteReq::DropTextIndex { coll, field } => {
            let c = store.collection_in(wtxn, coll)?;
            let existed = c.drop_text_index_txn(wtxn, field)?;
            Ok(json!({ "dropped": existed, "kind": "text" }))
        }
        WriteReq::CreateFuzzyIndex { coll, field } => {
            let c = store.collection_in(wtxn, coll)?;
            c.create_fuzzy_index_txn(wtxn, field)?;
            Ok(json!({ "created": field, "kind": "fuzzy" }))
        }
        WriteReq::DropFuzzyIndex { coll, field } => {
            let c = store.collection_in(wtxn, coll)?;
            let existed = c.drop_fuzzy_index_txn(wtxn, field)?;
            Ok(json!({ "dropped": existed, "kind": "fuzzy" }))
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
