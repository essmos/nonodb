//! Server-side cursors for bounded, resumable reads.
//!
//! Cursors page by `_id`, which keeps memory bounded and avoids holding an LMDB
//! read transaction across HTTP requests. Find cursors support `_id` ordering;
//! aggregation cursors support only document-local stages after an optional
//! leading `$match` (no `$group`, `$unwind`, or non-`_id` `$sort`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::Value;

pub const DEFAULT_BATCH_SIZE: usize = 500;
pub const MAX_BATCH_SIZE: usize = 1_000;
const MAX_CURSORS: usize = 1_024;
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Clone)]
pub enum Kind {
    Find {
        coll: String,
        filter: Value,
        projection: Option<Value>,
        ascending: bool,
        skip: usize,
    },
    Aggregate {
        coll: String,
        filter: Value,
        stages: Vec<Value>,
    },
}

impl Kind {
    pub fn coll(&self) -> &str {
        match self {
            Self::Find { coll, .. } | Self::Aggregate { coll, .. } => coll,
        }
    }
}

#[derive(Clone)]
pub struct State {
    pub owner: String,
    pub database: String,
    pub kind: Kind,
    pub last_id: Option<String>,
    pub remaining: Option<usize>,
    pub batch_size: usize,
    pub skip: usize,
    busy: bool,
    last_used: Instant,
}

impl State {
    pub fn new(
        owner: String,
        database: String,
        kind: Kind,
        limit: Option<usize>,
        batch_size: Option<usize>,
    ) -> Self {
        let skip = match &kind {
            Kind::Find { skip, .. } => *skip,
            Kind::Aggregate { .. } => 0,
        };
        Self {
            owner,
            database,
            kind,
            last_id: None,
            remaining: limit,
            batch_size: normalize_batch(batch_size),
            skip,
            busy: false,
            last_used: Instant::now(),
        }
    }

    pub fn ascending(&self) -> bool {
        match &self.kind {
            Kind::Find { ascending, .. } => *ascending,
            Kind::Aggregate { .. } => true,
        }
    }
}

pub fn normalize_batch(batch_size: Option<usize>) -> usize {
    batch_size
        .unwrap_or(DEFAULT_BATCH_SIZE)
        .clamp(1, MAX_BATCH_SIZE)
}

pub struct Registry {
    inner: Mutex<HashMap<u64, State>>,
    next: AtomicU64,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
        }
    }

    pub fn open(&self, state: State) -> Result<u64, String> {
        let mut inner = self.inner.lock().expect("cursor lock");
        let now = Instant::now();
        inner.retain(|_, cursor| now.duration_since(cursor.last_used) < IDLE_TIMEOUT);
        if inner.len() >= MAX_CURSORS {
            return Err("too many open cursors".to_string());
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        inner.insert(id, state);
        Ok(id)
    }

    /// Marks the cursor busy and returns a copy. Concurrent `get_more` calls on
    /// the same cursor are rejected rather than returning duplicate pages.
    pub fn begin(&self, id: u64, owner: &str) -> Result<State, String> {
        let mut inner = self.inner.lock().expect("cursor lock");
        let Some(cursor) = inner.get_mut(&id) else {
            return Err("cursor not found".to_string());
        };
        if cursor.owner != owner {
            return Err("cursor not found".to_string());
        }
        if cursor.busy {
            return Err("cursor is already being read".to_string());
        }
        if cursor.last_used.elapsed() >= IDLE_TIMEOUT {
            inner.remove(&id);
            return Err("cursor expired".to_string());
        }
        cursor.busy = true;
        Ok(cursor.clone())
    }

    /// Stores the next state, or removes the cursor when exhausted.
    pub fn finish(&self, id: u64, mut state: State, has_more: bool) {
        let mut inner = self.inner.lock().expect("cursor lock");
        let Some(current) = inner.get_mut(&id) else {
            return;
        };
        if !has_more {
            inner.remove(&id);
            return;
        }
        state.busy = false;
        state.last_used = Instant::now();
        *current = state;
    }

    pub fn close(&self, id: u64, owner: &str) -> bool {
        let mut inner = self.inner.lock().expect("cursor lock");
        match inner.get(&id) {
            Some(cursor) if cursor.owner == owner => inner.remove(&id).is_some(),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn state(owner: &str) -> State {
        State::new(
            owner.to_string(),
            "default".to_string(),
            Kind::Find {
                coll: "items".to_string(),
                filter: json!({}),
                projection: None,
                ascending: true,
                skip: 0,
            },
            None,
            None,
        )
    }

    #[test]
    fn cursors_are_owner_scoped_and_single_reader() {
        let registry = Registry::new();
        let id = registry.open(state("alice")).unwrap();
        assert!(registry.begin(id, "bob").is_err());
        let taken = registry.begin(id, "alice").unwrap();
        assert!(registry.begin(id, "alice").is_err());
        registry.finish(id, taken, false);
        assert!(registry.begin(id, "alice").is_err());
    }

    #[test]
    fn close_only_affects_the_owner() {
        let registry = Registry::new();
        let id = registry.open(state("alice")).unwrap();
        assert!(!registry.close(id, "bob"));
        assert!(registry.close(id, "alice"));
        assert!(!registry.close(id, "alice"));
    }

    #[test]
    fn batch_size_is_clamped() {
        assert_eq!(normalize_batch(None), DEFAULT_BATCH_SIZE);
        assert_eq!(normalize_batch(Some(0)), 1);
        assert_eq!(normalize_batch(Some(usize::MAX)), MAX_BATCH_SIZE);
    }
}
