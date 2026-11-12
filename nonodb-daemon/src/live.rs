//! In-memory, connection-scoped live subscriptions.
//!
//! Live subscriptions never write to `_events`. The writer evaluates their
//! conditions and sends matching events directly to the owning SSE/NDJSON
//! stream. A guard removes the subscription when the stream ends; a janitor
//! removes half-open connections after a fixed idle grace period.

use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant},
};

use nonodb_core::{heed::RwTxn, subscriptions::Subscription, Collection};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::protocol::ApiError;

const IDLE_GRACE: Duration = Duration::from_secs(30);

static REGISTRY: OnceLock<Arc<LiveRegistry>> = OnceLock::new();

thread_local! {
    /// Live events collected by the writer transaction, dispatched only after a
    /// successful commit, or discarded when the transaction aborts.
    static PENDING: RefCell<Vec<PendingLive>> = const { RefCell::new(Vec::new()) };
}

struct LiveObserver;

impl nonodb_core::observer::ChangeObserver for LiveObserver {
    fn active(&self) -> bool {
        registry().has_any()
    }

    fn observe(
        &self,
        collection: &Collection<'_>,
        wtxn: &RwTxn<'_>,
        op: &str,
        id: &str,
        before: Option<&Value>,
        after: Option<&Value>,
    ) -> nonodb_core::Result<()> {
        collect_pending(collection, wtxn, collection.name(), op, id, before, after)
            .map_err(|err| nonodb_core::Error::Query(err.message))
    }
}

/// Installs the daemon observer used by SQL writes.
pub fn install_observer() {
    nonodb_core::observer::set_change_observer(Some(Arc::new(LiveObserver)));
}

pub fn registry() -> &'static Arc<LiveRegistry> {
    REGISTRY.get_or_init(LiveRegistry::new)
}

/// Collects live events for the current writer transaction.
pub fn collect_pending(
    target: &Collection<'_>,
    wtxn: &RwTxn<'_>,
    coll: &str,
    op: &str,
    doc_id: &str,
    before: Option<&Value>,
    after: Option<&Value>,
) -> Result<(), ApiError> {
    let pending = registry().collect(target, wtxn, coll, op, doc_id, before, after)?;
    if !pending.is_empty() {
        PENDING.with(|slot| slot.borrow_mut().extend(pending));
    }
    Ok(())
}

/// Sends collected live events after a successful commit.
pub fn dispatch_pending() {
    let pending = PENDING.with(|slot| std::mem::take(&mut *slot.borrow_mut()));
    if !pending.is_empty() {
        registry().dispatch(pending);
    }
}

/// Drops collected live events when the transaction aborts.
pub fn discard_pending() {
    PENDING.with(|slot| slot.borrow_mut().clear());
}

pub struct LiveRegistry {
    inner: Mutex<Inner>,
    next_id: AtomicU64,
}

struct Inner {
    subs: HashMap<u64, LiveSub>,
}

struct LiveSub {
    coll: String,
    subscription: Subscription,
    tx: mpsc::Sender<Value>,
    last_seen: Instant,
}

pub struct PendingLive {
    id: u64,
    event: Value,
}

pub struct LiveGuard {
    registry: Arc<LiveRegistry>,
    id: u64,
}

impl Drop for LiveGuard {
    fn drop(&mut self) {
        self.registry.remove(self.id);
    }
}

impl LiveRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                subs: HashMap::new(),
            }),
            next_id: AtomicU64::new(1),
        })
    }

    pub fn register(
        self: &Arc<Self>,
        coll: String,
        mut subscription: Subscription,
        tx: mpsc::Sender<Value>,
    ) -> (u64, LiveGuard) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if subscription.name.is_empty() {
            subscription.name = format!("live-{id}");
        }
        subscription.last_matched = false;
        let mut inner = self.inner.lock().expect("live registry poisoned");
        inner.subs.insert(
            id,
            LiveSub {
                coll,
                subscription,
                tx,
                last_seen: Instant::now(),
            },
        );
        let guard = LiveGuard {
            registry: self.clone(),
            id,
        };
        (id, guard)
    }

    pub fn has_any(&self) -> bool {
        let inner = self.inner.lock().expect("live registry poisoned");
        !inner.subs.is_empty()
    }

    pub fn has_coll(&self, coll: &str) -> bool {
        let inner = self.inner.lock().expect("live registry poisoned");
        inner.subs.values().any(|sub| sub.coll == coll)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn collect(
        &self,
        target: &Collection<'_>,
        wtxn: &RwTxn<'_>,
        coll: &str,
        op: &str,
        doc_id: &str,
        before: Option<&Value>,
        after: Option<&Value>,
    ) -> Result<Vec<PendingLive>, ApiError> {
        let mut inner = self.inner.lock().expect("live registry poisoned");
        if inner.subs.is_empty() {
            return Ok(Vec::new());
        }
        let mut pending = Vec::new();
        for (id, sub) in &mut inner.subs {
            if sub.coll != coll || !sub.subscription.watches_op(op) {
                continue;
            }
            let condition_doc = if op == "delete" { before } else { after };
            let doc_matches = sub.subscription.filter.is_null()
                || sub
                    .subscription
                    .filter
                    .as_object()
                    .is_some_and(|object| object.is_empty())
                || condition_doc
                    .is_some_and(|doc| nonodb_core::query::matches(doc, &sub.subscription.filter));
            let (count_matches, count) = match &sub.subscription.count {
                Some(condition) => {
                    let count = target.count_in(wtxn, &condition.filter)? as u64;
                    (condition.matches(count), Some(count))
                }
                None => (true, None),
            };
            let matched = doc_matches && count_matches;
            let should_emit = matched && (!sub.subscription.once || !sub.subscription.last_matched);
            if should_emit {
                pending.push(PendingLive {
                    id: *id,
                    event: sub
                        .subscription
                        .event(coll, op, doc_id, before, after, count),
                });
            }
            sub.subscription.last_matched = matched;
        }
        Ok(pending)
    }

    pub fn dispatch(&self, pending: Vec<PendingLive>) {
        for item in pending {
            let tx = {
                let mut inner = self.inner.lock().expect("live registry poisoned");
                inner.subs.get_mut(&item.id).map(|sub| {
                    sub.last_seen = Instant::now();
                    sub.tx.clone()
                })
            };
            let Some(tx) = tx else {
                continue;
            };
            if tx.try_send(item.event).is_err() {
                // Slow or disconnected stream: drop it rather than growing an
                // unbounded writer-side queue.
                self.remove(item.id);
            }
        }
    }

    pub fn touch(&self, id: u64) {
        let mut inner = self.inner.lock().expect("live registry poisoned");
        if let Some(sub) = inner.subs.get_mut(&id) {
            sub.last_seen = Instant::now();
        }
    }

    pub fn remove(&self, id: u64) {
        let mut inner = self.inner.lock().expect("live registry poisoned");
        inner.subs.remove(&id);
    }

    pub fn start_janitor(self: &Arc<Self>) {
        let registry = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let now = Instant::now();
                let mut inner = registry.inner.lock().expect("live registry poisoned");
                inner
                    .subs
                    .retain(|_, sub| now.duration_since(sub.last_seen) <= IDLE_GRACE);
            }
        });
    }
}
