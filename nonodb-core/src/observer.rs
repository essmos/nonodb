//! Optional daemon-side observer for writes performed inside core helpers.
//!
//! SQL writes call `Collection::append_change_event`, so the daemon installs a
//! small observer to route those changes to connection-scoped live
//! subscriptions without making `nonodb-core` depend on the daemon runtime.

use std::sync::{Arc, Mutex, OnceLock};

use heed::RwTxn;
use serde_json::Value;

use crate::{error::Result, Collection};

pub trait ChangeObserver: Send + Sync {
    /// Whether this observer currently needs write notifications.
    fn active(&self) -> bool {
        true
    }

    fn observe(
        &self,
        collection: &Collection<'_>,
        wtxn: &RwTxn<'_>,
        op: &str,
        id: &str,
        before: Option<&Value>,
        after: Option<&Value>,
    ) -> Result<()>;
}

static OBSERVER: OnceLock<Mutex<Option<Arc<dyn ChangeObserver>>>> = OnceLock::new();

fn slot() -> &'static Mutex<Option<Arc<dyn ChangeObserver>>> {
    OBSERVER.get_or_init(|| Mutex::new(None))
}

pub fn set_change_observer(observer: Option<Arc<dyn ChangeObserver>>) {
    *slot().lock().expect("change observer poisoned") = observer;
}

pub fn is_active() -> bool {
    slot().lock().expect("change observer poisoned").is_some()
}

pub(crate) fn notify(
    collection: &Collection<'_>,
    wtxn: &RwTxn<'_>,
    op: &str,
    id: &str,
    before: Option<&Value>,
    after: Option<&Value>,
) -> Result<()> {
    let observer = slot().lock().expect("change observer poisoned").clone();
    if let Some(observer) = observer {
        observer.observe(collection, wtxn, op, id, before, after)?;
    }
    Ok(())
}
