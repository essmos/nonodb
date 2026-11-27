//! Streams: writer-assigned offsets and durable consumer groups.

use nonodb_core::streams::{ResetTo, StartAt};
use nonodb_core::{Error, Store};
use serde_json::{json, Value};
use tempfile::TempDir;

fn store() -> (TempDir, Store) {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    (dir, store)
}

fn stream_store() -> (TempDir, Store) {
    let (dir, store) = store();
    store.collection("_events").unwrap();
    let mut def = nonodb_core::streams::StreamDef::new("_events", "_events");
    def.retention.max_events = Some(1_000);
    store.set_stream(&def).unwrap();
    (dir, store)
}

#[test]
fn inserts_get_monotonic_offsets_and_reserved_field_is_rejected() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();

    events.insert_one(json!({"type": "a"})).unwrap();
    events.insert_one(json!({"type": "b"})).unwrap();
    let mut wtxn = store.write_txn().unwrap();
    events
        .insert_many_txn(&mut wtxn, &[json!({"type": "c"}), json!({"type": "d"})])
        .unwrap();
    wtxn.commit().unwrap();

    let query = nonodb_core::FindQuery {
        sort: vec![("_seq".to_string(), 1)],
        ..Default::default()
    };
    let docs = events.find(&query).unwrap();
    let seqs: Vec<u64> = docs
        .iter()
        .map(|doc| doc.get("_seq").and_then(|value| value.as_u64()).unwrap())
        .collect();
    assert_eq!(seqs, vec![1, 2, 3, 4]);
    assert_eq!(store.stream_head("_events").unwrap(), 4);

    let err = events.insert_one(json!({"_seq": 99})).unwrap_err();
    assert!(matches!(err, Error::Query(message) if message.contains("reserved")));

    // Without a stream definition the field stays a normal user field.
    let (_dir2, plain) = self::store();
    let users = plain.collection("users").unwrap();
    users.insert_one(json!({"name": "ada"})).unwrap();
    let doc = &users.find(&nonodb_core::FindQuery::default()).unwrap()[0];
    assert!(doc.get("_seq").is_none());
}

#[test]
fn group_commits_are_fenced_and_monotonic() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    for n in 0..3 {
        events.insert_one(json!({"n": n})).unwrap();
    }

    let group = store
        .create_stream_group("_events", "orders", StartAt::Latest)
        .unwrap();
    assert_eq!(group.committed_seq, 3);

    events.insert_one(json!({"n": 3})).unwrap();
    events.insert_one(json!({"n": 4})).unwrap();

    let claimed = store
        .claim_stream_group("_events", "orders", "worker-1", 60_000)
        .unwrap();
    let lease = claimed.lease_id.clone().unwrap();
    assert_eq!(claimed.committed_seq, 3);

    // Another member cannot steal an active lease.
    let err = store
        .claim_stream_group("_events", "orders", "worker-2", 60_000)
        .unwrap_err();
    assert!(matches!(err, Error::StaleLease(_)), "{err:?}");

    // A commit from a lease that does not own the batch is rejected.
    let err = store
        .commit_stream_group("_events", "orders", 5, Some("not-the-lease"))
        .unwrap_err();
    assert!(matches!(err, Error::StaleLease(_)), "{err:?}");

    let committed = store
        .commit_stream_group("_events", "orders", 5, Some(&lease))
        .unwrap();
    assert_eq!(committed.committed_seq, 5);
    assert!(committed.lease_id.is_none());

    // Moving backwards is rejected; committing past the head is rejected.
    let err = store
        .commit_stream_group("_events", "orders", 4, None)
        .unwrap_err();
    assert!(matches!(err, Error::CommitOutOfOrder(_)), "{err:?}");
    let err = store
        .commit_stream_group("_events", "orders", 99, None)
        .unwrap_err();
    assert!(matches!(err, Error::CommitOutOfOrder(_)), "{err:?}");

    // A second member takes over after the commit cleared the lease.
    let next = store
        .claim_stream_group("_events", "orders", "worker-2", 60_000)
        .unwrap();
    assert_eq!(next.committed_seq, 5);
    assert_eq!(next.member.as_deref(), Some("worker-2"));
}

#[test]
fn committed_offset_survives_reopen() {
    let (dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    for n in 0..4 {
        events.insert_one(json!({"n": n})).unwrap();
    }
    store
        .create_stream_group("_events", "restart", StartAt::Earliest)
        .unwrap();
    let claimed = store
        .claim_stream_group("_events", "restart", "worker-1", 60_000)
        .unwrap();
    store
        .commit_stream_group("_events", "restart", 2, claimed.lease_id.as_deref())
        .unwrap();
    drop(store);

    let reopened = Store::open(dir.path()).unwrap();
    let group = reopened
        .stream_group("_events", "restart")
        .unwrap()
        .unwrap();
    assert_eq!(group.committed_seq, 2);
    // The definition and its counter survive too.
    assert!(reopened.stream("_events").unwrap().is_some());
    assert_eq!(reopened.stream_head("_events").unwrap(), 4);

    // Writes after a reopen continue the offset sequence instead of restarting.
    reopened
        .collection("_events")
        .unwrap()
        .insert_one(json!({"n": 4}))
        .unwrap();
    assert_eq!(reopened.stream_head("_events").unwrap(), 5);
}

#[test]
fn stats_match_a_manual_drain_and_reset_moves_the_group() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    for n in 0..5 {
        events.insert_one(json!({"n": n})).unwrap();
    }
    store
        .create_stream_group("_events", "stats", StartAt::Earliest)
        .unwrap();

    // Manual drain: read events above the committed offset until caught up.
    let mut committed = 0u64;
    let mut delivered = 0usize;
    loop {
        let after = committed;
        let query = nonodb_core::FindQuery {
            filter: json!({"_seq": {"$gt": after}}),
            sort: vec![("_seq".to_string(), 1)],
            limit: Some(2),
            ..Default::default()
        };
        let page = events.find(&query).unwrap();
        if page.is_empty() {
            break;
        }
        delivered += page.len();
        committed = page
            .last()
            .and_then(|doc| doc.get("_seq"))
            .and_then(|value| value.as_u64())
            .unwrap();
    }
    assert_eq!(delivered, 5);
    assert_eq!(committed, 5);

    let head = store.stream_head("_events").unwrap();
    let group = store.stream_group("_events", "stats").unwrap().unwrap();
    assert_eq!(nonodb_core::streams::lag(head, group.committed_seq), 5);

    let reset = store
        .reset_stream_group("_events", "stats", ResetTo::Latest)
        .unwrap();
    assert_eq!(reset.committed_seq, head);
    assert!(reset.lease_id.is_none());

    let reset = store
        .reset_stream_group("_events", "stats", ResetTo::Seq(2))
        .unwrap();
    assert_eq!(reset.committed_seq, 2);

    let err = store
        .reset_stream_group("_events", "stats", ResetTo::Seq(99))
        .unwrap_err();
    assert!(matches!(err, Error::Query(_)), "{err:?}");
}

#[test]
fn earliest_group_starts_at_zero_and_duplicates_are_rejected() {
    let (_dir, store) = stream_store();
    store
        .collection("_events")
        .unwrap()
        .insert_one(json!({"n": 0}))
        .unwrap();
    let group = store
        .create_stream_group("_events", "dup", StartAt::Earliest)
        .unwrap();
    assert_eq!(group.committed_seq, 0);

    let err = store
        .create_stream_group("_events", "dup", StartAt::Earliest)
        .unwrap_err();
    assert!(matches!(err, Error::DuplicateKey(_)), "{err:?}");

    let err = store
        .create_stream_group("_events", "late", StartAt::Seq(50))
        .unwrap_err();
    assert!(matches!(err, Error::Query(_)), "{err:?}");
}

#[test]
fn dropping_a_stream_keeps_data_and_groups() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    events.insert_one(json!({"n": 0})).unwrap();
    store
        .create_stream_group("_events", "keep", StartAt::Earliest)
        .unwrap();

    let mut wtxn = store.write_txn().unwrap();
    assert!(store.drop_stream_in(&mut wtxn, "_events").unwrap());
    wtxn.commit().unwrap();

    assert!(store.stream("_events").unwrap().is_none());
    assert!(store.stream_group("_events", "keep").unwrap().is_some());
    assert_eq!(events.count(&json!({})).unwrap(), 1);

    // Without the definition, new inserts are no longer offset-tagged; the
    // documents written while the stream existed keep their offsets.
    events.insert_one(json!({"n": 1})).unwrap();
    let docs = events.find(&nonodb_core::FindQuery::default()).unwrap();
    let tagged: Vec<&Value> = docs
        .iter()
        .filter(|doc| doc.get("_seq").is_some())
        .collect();
    assert_eq!(tagged.len(), 1);
    assert!(docs
        .iter()
        .any(|doc| doc.get("n") == Some(&json!(1)) && doc.get("_seq").is_none()));
}
