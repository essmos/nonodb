//! Collection change events and per-collection event settings.
//!
//! Change events are ordinary documents in the `_events` outbox collection, so
//! they use the normal indexes, TTL tools, `watch`, and `stream` APIs.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::query::path;

/// Default outbox collection for automatic collection change events.
pub const EVENTS_COLLECTION: &str = "_events";

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Optional operation filter. Empty means all operations.
    #[serde(default)]
    pub ops: Vec<String>,
    /// Optional MongoDB-style filter applied to the complete event document.
    #[serde(default)]
    pub filter: Value,
    /// Optional field list. Updates emit only when at least one field changed.
    #[serde(default)]
    pub fields: Vec<String>,
    #[serde(default = "default_true")]
    pub include_before: bool,
    #[serde(default = "default_true")]
    pub include_after: bool,
    /// Optional projection applied to `payload.before` and `payload.after`.
    #[serde(default)]
    pub projection: Option<Value>,
}

impl Default for EventConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ops: Vec::new(),
            filter: Value::Null,
            fields: Vec::new(),
            include_before: true,
            include_after: true,
            projection: None,
        }
    }
}

impl EventConfig {
    pub fn active(&self) -> bool {
        self.enabled
    }

    pub fn watches_field(&self, field: &str) -> bool {
        self.fields.is_empty() || self.fields.iter().any(|watched| watched == field)
    }

    pub fn watches_op(&self, op: &str) -> bool {
        self.ops.is_empty() || self.ops.iter().any(|candidate| candidate == op)
    }
}

/// Builds one automatic change event document.
pub fn change_event(
    coll: &str,
    op: &str,
    doc_id: &str,
    before: Option<&Value>,
    after: Option<&Value>,
    config: &EventConfig,
) -> Option<Value> {
    if !config.active() || !config.watches_op(op) {
        EMITTED_SUPPRESSED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return None;
    }
    let changed_fields = changed_fields(before, after, config);
    if op == "update" && !config.fields.is_empty() && changed_fields.is_empty() {
        EMITTED_SUPPRESSED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return None;
    }
    let projected_before = if config.include_before {
        Some(project_document(before, config.projection.as_ref()))
    } else {
        None
    };
    let projected_after = if config.include_after {
        Some(project_document(after, config.projection.as_ref()))
    } else {
        None
    };
    let mut event = json!({
        "_id": new_event_id(),
        "type": format!("{coll}.{op}"),
        "doc_id": doc_id,
        "payload": {
            "coll": coll,
            "op": op,
            "before": projected_before.unwrap_or(Value::Null),
            "after": projected_after.unwrap_or(Value::Null),
            "changed_fields": changed_fields,
        },
        "created_at": now_ms(),
    });
    if !config.filter.is_null()
        && config
            .filter
            .as_object()
            .is_some_and(|object| !object.is_empty())
        && !crate::query::matches(&event, &config.filter)
    {
        EMITTED_SUPPRESSED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return None;
    }
    EMITTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Some(std::mem::take(&mut event))
}

fn project_document(doc: Option<&Value>, projection: Option<&Value>) -> Value {
    match (doc, projection) {
        (Some(doc), Some(projection)) => crate::collection::project_document(doc, Some(projection)),
        (Some(doc), None) => doc.clone(),
        (None, _) => Value::Null,
    }
}

static EMITTED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static EMITTED_SUPPRESSED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn stats() -> (u64, u64) {
    (
        EMITTED.load(std::sync::atomic::Ordering::Relaxed),
        EMITTED_SUPPRESSED.load(std::sync::atomic::Ordering::Relaxed),
    )
}

fn changed_fields(
    before: Option<&Value>,
    after: Option<&Value>,
    config: &EventConfig,
) -> Vec<String> {
    let fields = if config.fields.is_empty() {
        let mut keys = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for doc in [before, after].into_iter().flatten() {
            if let Some(obj) = doc.as_object() {
                for key in obj.keys() {
                    if seen.insert(key.clone()) {
                        keys.push(key.clone());
                    }
                }
            }
        }
        keys
    } else {
        config.fields.clone()
    };
    fields
        .into_iter()
        .filter(|field| {
            let old = before.and_then(|doc| path::get(doc, field));
            let new = after.and_then(|doc| path::get(doc, field));
            old != new
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn changed_fields_are_limited_to_the_configured_list() {
        let before = json!({"status": "new", "total": 1, "other": 1});
        let after = json!({"status": "paid", "total": 2, "other": 1});
        let config = EventConfig {
            enabled: true,
            fields: vec!["status".into(), "total".into()],
            ..EventConfig::default()
        };
        assert_eq!(
            changed_fields(Some(&before), Some(&after), &config),
            vec!["status".to_string(), "total".to_string()]
        );
        assert_eq!(
            changed_fields(Some(&before), Some(&after), &EventConfig::default()),
            vec!["status".to_string(), "total".to_string()]
        );
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Generates a 26-character ULID-style event id, monotonic within the process.
pub fn new_event_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let timestamp = now_ms() & ((1u64 << 48) - 1);
    let suffix = ((sequence as u128) << 64) | rand::random::<u64>() as u128;
    let mut value = ((timestamp as u128) << 80) | (suffix & ((1u128 << 80) - 1));
    let mut chars = [b'0'; 26];
    for index in (0..26).rev() {
        chars[index] = CROCKFORD[(value & 31) as usize];
        value >>= 5;
    }
    String::from_utf8_lossy(&chars).into_owned()
}

#[cfg(test)]
mod id_tests {
    use super::*;

    #[test]
    fn event_ids_are_sortable_and_unique() {
        let a = new_event_id();
        let b = new_event_id();
        assert_eq!(a.len(), 26);
        assert_eq!(b.len(), 26);
        assert!(a < b);
    }
}
