//! Jeden pisarz + group commit.
//!
//! Wszystkie operacje zapisu trafiają kanałem `mpsc` do jednego wątku. Wątek
//! zbiera paczkę żądań (do `MAX_BATCH` sztuk w oknie `BATCH_WINDOW`), wykonuje
//! je w **jednej** transakcji LMDB i robi **jeden** `commit` — czyli jeden
//! `fsync` na paczkę. Odpowiedź do klienta leci dopiero po commicie.

use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use nonodb_core::Store;
use serde_json::{json, Value};
use tokio::sync::oneshot;

use crate::cache::QueryCache;
use crate::metrics;
use crate::protocol::{ApiError, WriteReq};

/// Maksymalna liczba operacji w jednej transakcji.
const MAX_BATCH: usize = 512;
/// Bazowe okno zbierania paczki (niska kolejka = niska latencja).
const BASE_WINDOW: Duration = Duration::from_millis(1);
/// Maksymalne okno przy dużym obciążeniu (większy batch = mniej `fsync`).
const MAX_WINDOW: Duration = Duration::from_millis(5);

pub struct WriteJob {
    pub req: WriteReq,
    pub resp: oneshot::Sender<Result<Value, ApiError>>,
}

/// Pętla wątku piszącego. Kończy się, gdy kanał zostanie zamknięty.
pub fn run(store: Arc<Store>, rx: Receiver<WriteJob>, cache: Arc<QueryCache>) {
    while let Ok(first) = rx.recv() {
        let mut jobs = vec![first];
        // Adaptacyjne okno: im dłuższa kolejka, tym większy batch (mniej fsync),
        // ale przy pustej kolejce wracamy do 1 ms (niska latencja).
        let pending = metrics::QUEUE_LEN.load(std::sync::atomic::Ordering::Relaxed);
        // Progi wysokie: przy małej liczbie klientów 1 ms wystarcza i nie
        // dodajemy latencji; wydłużamy okno dopiero przy realnym natłoku.
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
    }
}

fn run_batch(store: &Store, cache: &QueryCache, jobs: Vec<WriteJob>) {
    metrics::sub(&metrics::QUEUE_LEN, jobs.len() as u64);
    // Atomowy `write_batch` nie może współdzielić transakcji z innymi
    // żądaniami: błąd jednej operacji musi wycofać całą jego transakcję, a nie
    // pozostałe zapisy z grupy. Dlatego taką grupę wykonujemy job po jobie.
    if jobs.iter().any(|j| j.req.is_atomic()) {
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
            // Drop `wtxn` = abort; dla `WriteReq::Batch` to gwarantuje
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
