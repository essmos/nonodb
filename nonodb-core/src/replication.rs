//! Durable logical replication log primitives.
//!
//! Each database gets an internal `_replication` sub-database. A replication
//! record stores one committed write transaction, preserving batch atomicity
//! and the single-writer order. This module only defines and persists the log;
//! transport, replica apply, ownership, and failover are separate layers.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Internal LMDB sub-database for replication records.
pub const REPLICATION_DB: &str = "_replication";

/// Metadata key holding the last allocated replication sequence.
pub(crate) const REPLICATION_SEQ_KEY: &[u8] = b"replication:seq";

/// One logical document change inside a committed write transaction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReplicationChange {
    pub collection: String,
    /// `insert`, `update`, or `delete`.
    pub op: String,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Value>,
}

/// One committed write transaction in the replication log.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReplicationTxn {
    /// Monotonic per-database sequence assigned by the single writer.
    pub seq: u64,
    /// Stable node identity that produced the transaction.
    pub origin: String,
    /// Role epoch active on the writer when this transaction was produced.
    #[serde(default)]
    pub epoch: u64,
    pub created_at_ms: u64,
    pub changes: Vec<ReplicationChange>,
}

impl ReplicationTxn {
    pub fn new(seq: u64, origin: impl Into<String>, changes: Vec<ReplicationChange>) -> Self {
        Self::new_with_epoch(seq, origin, 0, changes)
    }

    pub fn new_with_epoch(
        seq: u64,
        origin: impl Into<String>,
        epoch: u64,
        changes: Vec<ReplicationChange>,
    ) -> Self {
        Self {
            seq,
            origin: origin.into(),
            epoch,
            created_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or(0),
            changes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;

    #[test]
    fn replication_log_preserves_transaction_groups_and_order() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.replication_head().unwrap(), 0);

        let mut wtxn = store.write_txn().unwrap();
        let first = store
            .append_replication_txn_in(
                &mut wtxn,
                "node-a",
                vec![ReplicationChange {
                    collection: "orders".into(),
                    op: "insert".into(),
                    id: "o1".into(),
                    before: None,
                    after: Some(serde_json::json!({"_id": "o1", "total": 10})),
                }],
            )
            .unwrap();
        wtxn.commit().unwrap();

        let mut wtxn = store.write_txn().unwrap();
        let second = store
            .append_replication_txn_in(
                &mut wtxn,
                "node-a",
                vec![
                    ReplicationChange {
                        collection: "orders".into(),
                        op: "update".into(),
                        id: "o1".into(),
                        before: Some(serde_json::json!({"_id": "o1", "total": 10})),
                        after: Some(serde_json::json!({"_id": "o1", "total": 20})),
                    },
                    ReplicationChange {
                        collection: "payments".into(),
                        op: "insert".into(),
                        id: "p1".into(),
                        before: None,
                        after: Some(serde_json::json!({"_id": "p1", "total": 20})),
                    },
                ],
            )
            .unwrap();
        wtxn.commit().unwrap();

        assert_eq!(first.seq, 1);
        assert_eq!(second.seq, 2);
        assert_eq!(store.replication_head().unwrap(), 2);
        let entries = store.replication_after(0, 10).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], first);
        assert_eq!(entries[1], second);
        assert_eq!(store.replication_after(1, 10).unwrap(), vec![second]);

        assert_eq!(store.prune_replication_through(1).unwrap(), 1);
        assert_eq!(store.replication_after(0, 10).unwrap().len(), 1);
    }

    #[test]
    fn apply_rejects_stale_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let change = |id: &str| ReplicationChange {
            collection: "orders".into(),
            op: "insert".into(),
            id: id.into(),
            before: None,
            after: Some(serde_json::json!({"_id": id})),
        };
        let first = ReplicationTxn::new_with_epoch(1, "node-a", 2, vec![change("o1")]);
        assert!(store.apply_replication_txn(&first).unwrap());
        assert_eq!(store.replication_applied_epoch("node-a").unwrap(), 2);

        let stale = ReplicationTxn::new_with_epoch(2, "node-a", 1, vec![change("o2")]);
        let error = store.apply_replication_txn(&stale).unwrap_err().to_string();
        assert!(error.contains("stale replication epoch"), "{error}");
        assert_eq!(store.replication_applied("node-a").unwrap(), 1);
    }

    #[test]
    fn apply_replication_txn_is_atomic_and_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let insert = ReplicationTxn::new(
            1,
            "node-a",
            vec![ReplicationChange {
                collection: "orders".into(),
                op: "insert".into(),
                id: "o1".into(),
                before: None,
                after: Some(serde_json::json!({"_id": "o1", "total": 10})),
            }],
        );
        assert!(store.apply_replication_txn(&insert).unwrap());
        assert!(!store.apply_replication_txn(&insert).unwrap());
        assert_eq!(store.replication_applied("node-a").unwrap(), 1);
        assert_eq!(
            store
                .collection("orders")
                .unwrap()
                .find_one(&serde_json::json!({"_id": "o1"}))
                .unwrap()
                .unwrap()["total"],
            10
        );

        let update = ReplicationTxn::new(
            2,
            "node-a",
            vec![ReplicationChange {
                collection: "orders".into(),
                op: "update".into(),
                id: "o1".into(),
                before: Some(serde_json::json!({"_id": "o1", "total": 10})),
                after: Some(serde_json::json!({"_id": "o1", "total": 20})),
            }],
        );
        assert!(store.apply_replication_txn(&update).unwrap());
        assert_eq!(store.replication_applied("node-a").unwrap(), 2);
        assert_eq!(
            store
                .collection("orders")
                .unwrap()
                .find_one(&serde_json::json!({"_id": "o1"}))
                .unwrap()
                .unwrap()["total"],
            20
        );

        let gap = ReplicationTxn::new(4, "node-a", update.changes.clone());
        assert!(store.apply_replication_txn(&gap).is_err());

        let delete = ReplicationTxn::new(
            3,
            "node-a",
            vec![ReplicationChange {
                collection: "orders".into(),
                op: "delete".into(),
                id: "o1".into(),
                before: Some(serde_json::json!({"_id": "o1", "total": 20})),
                after: None,
            }],
        );
        assert!(store.apply_replication_txn(&delete).unwrap());
        assert!(store
            .collection("orders")
            .unwrap()
            .find_one(&serde_json::json!({"_id": "o1"}))
            .unwrap()
            .is_none());
    }
}
