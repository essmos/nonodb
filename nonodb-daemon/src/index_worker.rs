//! Background worker for deferred non-unique secondary indexes.
//!
//! Queue entries are committed together with the document, so they survive a
//! crash. The worker applies them through the normal single-writer channel. While
//! entries are pending, read planning bypasses non-unique secondary indexes and
//! falls back to collection scans, preserving read-after-write correctness.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;

use nonodb_core::Store;
use tokio::sync::oneshot;

use crate::metrics;
use crate::protocol::{Durability, WriteReq};
use crate::writer::WriteJob;

const BATCH_LIMIT: usize = 512;
const IDLE_SLEEP: Duration = Duration::from_millis(20);

pub fn start(
    store: Arc<Store>,
    write_tx: Sender<WriteJob>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("nonodb-index-worker".into())
        .spawn(move || {
            let disabled = std::env::var("NONODB_INDEX_WORKER")
                .ok()
                .is_some_and(|value| matches!(value.as_str(), "0" | "false" | "no" | "off"));
            while !stop.load(Ordering::Relaxed) {
                let pending = store
                    .read_txn()
                    .ok()
                    .and_then(|rtxn| store.pending_index_count(&rtxn).ok())
                    .unwrap_or(0);
                metrics::set(&metrics::INDEX_QUEUE_PENDING, pending);
                if pending == 0 || disabled {
                    std::thread::sleep(IDLE_SLEEP);
                    continue;
                }

                let (resp, rx) = oneshot::channel();
                if write_tx
                    .send(WriteJob {
                        req: WriteReq::ProcessIndexQueue {
                            limit: BATCH_LIMIT,
                        },
                        resp,
                        isolated: false,
                        durability: Durability::Full,
                    })
                    .is_err()
                {
                    break;
                }
                match rx.blocking_recv() {
                    Ok(Ok(value)) => {
                        let processed = value
                            .get("processed")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        metrics::add(&metrics::INDEX_QUEUE_PROCESSED, processed);
                    }
                    Ok(Err(err)) => {
                        tracing::warn!(code = %err.code, message = %err.message, "index queue batch failed");
                        std::thread::sleep(IDLE_SLEEP);
                    }
                    Err(_) => break,
                }
            }
        })
        .expect("spawn index worker")
}
