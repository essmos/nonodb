//! Optional read-result cache.
//!
//! The cache is shared by all connections. Every successful write
//! increments the version and clears entries, so clients never receive data from
//! before the commit. The version is also checked when inserting an entry: a read that
//! started before the commit cannot insert a stale result.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::Value;

use crate::metrics;

const DEFAULT_TTL_SECS: f64 = 5.0;
const DEFAULT_MAX_ENTRIES: usize = 512;
const DEFAULT_MAX_VALUE_BYTES: usize = 256 * 1024;

struct Entry {
    version: u64,
    expires: Instant,
    value: Value,
}

pub struct QueryCache {
    ttl: Duration,
    max_entries: usize,
    max_value_bytes: usize,
    version: AtomicU64,
    entries: Mutex<HashMap<String, Entry>>,
}

impl QueryCache {
    pub fn from_env() -> Self {
        let ttl = std::env::var("NONODB_CACHE_TTL")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .map(|v| {
                if v > 0.0 {
                    Duration::from_secs_f64(v)
                } else {
                    Duration::ZERO
                }
            })
            .unwrap_or_else(|| Duration::from_secs_f64(DEFAULT_TTL_SECS));
        let max_entries = std::env::var("NONODB_CACHE_ENTRIES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_MAX_ENTRIES);
        let max_value_bytes = std::env::var("NONODB_CACHE_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_MAX_VALUE_BYTES);
        Self::new(ttl, max_entries, max_value_bytes)
    }

    pub fn new(ttl: Duration, max_entries: usize, max_value_bytes: usize) -> Self {
        Self {
            ttl,
            max_entries,
            max_value_bytes,
            version: AtomicU64::new(0),
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub fn enabled(&self) -> bool {
        !self.ttl.is_zero()
    }

    pub fn key<T: Serialize>(&self, req: &T) -> Option<String> {
        self.enabled()
            .then(|| serde_json::to_string(req).ok())
            .flatten()
    }

    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        if !self.enabled() {
            return None;
        }
        let now = Instant::now();
        let version = self.version();
        let mut entries = self.entries.lock().expect("cache lock");
        if let Some(entry) = entries.get(key) {
            if entry.version == version && entry.expires > now {
                metrics::inc(&metrics::CACHE_HITS);
                return Some(entry.value.clone());
            }
        }
        entries.remove(key);
        metrics::inc(&metrics::CACHE_MISSES);
        None
    }

    pub fn put(&self, key: String, version: u64, value: Value) {
        if !self.enabled() || self.version() != version {
            return;
        }
        let Ok(encoded) = serde_json::to_vec(&value) else {
            return;
        };
        if encoded.len() > self.max_value_bytes {
            return;
        }
        let mut entries = self.entries.lock().expect("cache lock");
        if self.version() != version {
            return;
        }
        if entries.len() >= self.max_entries {
            entries.clear();
        }
        entries.insert(
            key,
            Entry {
                version,
                expires: Instant::now() + self.ttl,
                value,
            },
        );
    }

    pub fn invalidate(&self) {
        self.version.fetch_add(1, Ordering::AcqRel);
        self.entries.lock().expect("cache lock").clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cache_roundtrip_and_invalidate() {
        let cache = QueryCache::new(Duration::from_secs(60), 8, 4096);
        let key = cache.key(&json!({"op": "count", "coll": "x"})).unwrap();
        let version = cache.version();
        assert!(cache.get(&key).is_none());
        cache.put(key.clone(), version, json!({"count": 3}));
        assert_eq!(cache.get(&key), Some(json!({"count": 3})));

        cache.invalidate();
        assert!(cache.get(&key).is_none());
        // An old read cannot insert a result after invalidation.
        cache.put(key.clone(), version, json!({"count": 3}));
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn cache_respects_value_limit() {
        let cache = QueryCache::new(Duration::from_secs(60), 8, 16);
        let key = cache.key(&json!({"op": "find", "coll": "x"})).unwrap();
        cache.put(
            key.clone(),
            cache.version(),
            json!({"payload": "x".repeat(100)}),
        );
        assert!(cache.get(&key).is_none());
    }
}
