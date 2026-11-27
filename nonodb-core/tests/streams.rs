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
fn batches_are_fenced_and_commit_in_order() {
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

    // Two members share one group: each gets a distinct contiguous range.
    let first = store
        .claim_stream_lease("_events", "orders", "worker-1", 1, 60_000)
        .unwrap()
        .unwrap();
    let second = store
        .claim_stream_lease("_events", "orders", "worker-2", 1, 60_000)
        .unwrap()
        .unwrap();
    assert_eq!((first.from_seq, first.to_seq), (4, 4));
    assert_eq!((second.from_seq, second.to_seq), (5, 5));

    // A commit from a lease that does not own the batch is rejected.
    let err = store
        .ack_stream_lease("_events", "orders", "not-a-lease", 4)
        .unwrap_err();
    assert!(matches!(err, Error::StaleLease(_)), "{err:?}");

    // Committing outside the leased range is rejected.
    let first_lease = first.lease_id.clone().unwrap();
    let err = store
        .ack_stream_lease("_events", "orders", &first_lease, 9)
        .unwrap_err();
    assert!(matches!(err, Error::CommitOutOfOrder(_)), "{err:?}");

    // The later range can finish first, but the committed offset only moves
    // when the earlier range finishes too.
    let second_lease = second.lease_id.clone().unwrap();
    store
        .ack_stream_lease("_events", "orders", &second_lease, 5)
        .unwrap();
    assert_eq!(
        store
            .stream_group("_events", "orders")
            .unwrap()
            .unwrap()
            .committed_seq,
        3
    );
    store
        .ack_stream_lease("_events", "orders", &first_lease, 4)
        .unwrap();
    let group = store.stream_group("_events", "orders").unwrap().unwrap();
    assert_eq!(group.committed_seq, 5);
    assert_eq!(group.next_seq, 5);

    // Committing the same batch twice is rejected.
    let err = store
        .ack_stream_lease("_events", "orders", &first_lease, 4)
        .unwrap_err();
    assert!(matches!(err, Error::StaleLease(_)), "{err:?}");

    // The group is caught up: no batch to claim.
    assert!(store
        .claim_stream_lease("_events", "orders", "worker-1", 10, 60_000)
        .unwrap()
        .is_none());
}

#[test]
fn expired_lease_is_redelivered_and_dead_letters_after_max_attempts() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    for n in 0..3 {
        events.insert_one(json!({"n": n})).unwrap();
    }
    let mut group = store
        .create_stream_group("_events", "retry", StartAt::Earliest)
        .unwrap();
    group.max_attempts = 2;
    let mut wtxn = store.write_txn().unwrap();
    store.put_stream_group_in(&mut wtxn, &group).unwrap();
    wtxn.commit().unwrap();

    // First delivery, then the member "dies" without committing.
    let first = store
        .claim_stream_lease("_events", "retry", "worker-1", 3, 1_000)
        .unwrap()
        .unwrap();
    assert_eq!(first.attempts, 1);
    std::thread::sleep(std::time::Duration::from_millis(1_200));

    // The expired range is redelivered with a higher attempt counter.
    let second = store
        .claim_stream_lease("_events", "retry", "worker-2", 3, 1_000)
        .unwrap()
        .unwrap();
    assert_eq!((second.from_seq, second.to_seq), (1, 3));
    assert_eq!(second.attempts, 2);
    std::thread::sleep(std::time::Duration::from_millis(1_200));

    // Attempts are exhausted: the batch becomes a dead letter and the group is
    // not blocked by it.
    assert!(store
        .claim_stream_lease("_events", "retry", "worker-1", 3, 1_000)
        .unwrap()
        .is_none());
    let dead = store
        .stream_dead_letters(Some("_events"), Some("retry"))
        .unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!((dead[0].from_seq, dead[0].to_seq), (1, 3));
    assert_eq!(
        store
            .stream_group("_events", "retry")
            .unwrap()
            .unwrap()
            .committed_seq,
        3
    );

    // Retrying a dead letter puts it back in the queue; dropping removes it.
    assert!(store.retry_stream_dead("_events", "retry", 1).unwrap());
    assert!(store
        .stream_dead_letters(Some("_events"), Some("retry"))
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .claim_stream_lease("_events", "retry", "worker-1", 3, 1_000)
            .unwrap()
            .unwrap()
            .attempts,
        1
    );
}

#[test]
fn nack_releases_a_batch_before_attempts_run_out() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    events.insert_one(json!({"n": 0})).unwrap();
    store
        .create_stream_group("_events", "nack", StartAt::Earliest)
        .unwrap();

    let claim = store
        .claim_stream_lease("_events", "nack", "worker-1", 1, 60_000)
        .unwrap()
        .unwrap();
    let lease_id = claim.lease_id.clone().unwrap();
    let released = store
        .nack_stream_lease("_events", "nack", &lease_id, 0)
        .unwrap();
    assert_eq!(released.attempts, 1);
    assert_eq!(
        store
            .stream_leases("_events", "nack")
            .unwrap()
            .first()
            .unwrap()
            .status,
        nonodb_core::streams::LeaseStatus::Available
    );

    let again = store
        .claim_stream_lease("_events", "nack", "worker-2", 1, 60_000)
        .unwrap()
        .unwrap();
    assert_eq!(again.attempts, 2);
    store
        .ack_stream_lease("_events", "nack", again.lease_id.as_deref().unwrap(), 1)
        .unwrap();
    assert_eq!(
        store
            .stream_group("_events", "nack")
            .unwrap()
            .unwrap()
            .committed_seq,
        1
    );
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
        .claim_stream_lease("_events", "restart", "worker-1", 2, 60_000)
        .unwrap()
        .unwrap();
    store
        .ack_stream_lease(
            "_events",
            "restart",
            claimed.lease_id.as_deref().unwrap(),
            2,
        )
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
    assert_eq!(reset.next_seq, head);
    assert!(store.stream_leases("_events", "stats").unwrap().is_empty());

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

#[test]
fn offset_retention_waits_for_the_slowest_group() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    for n in 0..20 {
        events.insert_one(json!({"n": n})).unwrap();
    }
    let mut def = store.stream("_events").unwrap().unwrap();
    // `max_events` keeps retention active while the consumer offset pins it.
    def.retention.max_events = Some(5);
    store.set_stream(&def).unwrap();

    let mut group = store
        .create_stream_group("_events", "slow", StartAt::Earliest)
        .unwrap();
    assert_eq!(group.committed_seq, 0);

    // A consumer that has not committed pins everything.
    assert!(store
        .prune_stream_in(&mut store.write_txn().unwrap(), "_events", 1_000)
        .unwrap()
        .is_empty());
    assert_eq!(
        events
            .find(&nonodb_core::FindQuery::default())
            .unwrap()
            .len(),
        20
    );

    // After a commit the window moves up to the committed offset.
    let claim = store
        .claim_stream_lease("_events", "slow", "w1", 10, 60_000)
        .unwrap()
        .unwrap();
    store
        .ack_stream_lease("_events", "slow", claim.lease_id.as_deref().unwrap(), 10)
        .unwrap();
    let dropped = {
        let mut wtxn = store.write_txn().unwrap();
        let rows = store.prune_stream_in(&mut wtxn, "_events", 1_000).unwrap();
        wtxn.commit().unwrap();
        rows
    };
    assert_eq!(dropped.len(), 10);
    let oldest = nonodb_core::FindQuery {
        sort: vec![("_seq".to_string(), 1)],
        limit: Some(1),
        ..Default::default()
    };
    assert_eq!(
        events.find(&oldest).unwrap()[0]["_seq"].as_u64(),
        Some(11),
        "retention may only drop below the slowest committed offset"
    );

    // The group keeps its offset and is not marked truncated.
    group = store.stream_group("_events", "slow").unwrap().unwrap();
    assert_eq!(group.committed_seq, 10);
    assert_eq!(group.truncated_below, None);

    // A second pass is a no-op while the consumer stays at 10.
    let mut wtxn = store.write_txn().unwrap();
    assert!(store
        .prune_stream_in(&mut wtxn, "_events", 1_000)
        .unwrap()
        .is_empty());
}

#[test]
fn group_behind_the_window_gets_stream_truncated() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    for n in 0..20 {
        events.insert_one(json!({"n": n})).unwrap();
    }
    let mut def = store.stream("_events").unwrap().unwrap();
    def.retention.max_events = Some(5);
    store.set_stream(&def).unwrap();

    // One consumer keeps up, the window advances, then a lagging group appears
    // behind the retained range.
    store
        .create_stream_group("_events", "fast", StartAt::Earliest)
        .unwrap();
    let claim = store
        .claim_stream_lease("_events", "fast", "w1", 20, 60_000)
        .unwrap()
        .unwrap();
    store
        .ack_stream_lease("_events", "fast", claim.lease_id.as_deref().unwrap(), 20)
        .unwrap();
    let mut wtxn = store.write_txn().unwrap();
    // `max_events = 5` keeps the newest five events, so 15 of 20 are dropped.
    assert_eq!(
        store
            .prune_stream_in(&mut wtxn, "_events", 1_000)
            .unwrap()
            .len(),
        15
    );
    wtxn.commit().unwrap();

    // New events arrive; the group created from the start is behind the window.
    events.insert_one(json!({"n": 20})).unwrap();
    let lagging = store
        .create_stream_group("_events", "late", StartAt::Earliest)
        .unwrap();
    assert_eq!(lagging.committed_seq, 0);
    let err = store
        .claim_stream_lease("_events", "late", "w1", 10, 60_000)
        .unwrap_err();
    assert!(matches!(err, Error::StreamTruncated(_)), "{err:?}");
    let lagging = store.stream_group("_events", "late").unwrap().unwrap();
    assert_eq!(lagging.truncated_below, Some(16));
    assert_eq!(lagging.next_seq, 0);

    // Resetting the group clears the marker and it can consume again.
    let reset = store
        .reset_stream_group("_events", "late", ResetTo::Earliest)
        .unwrap();
    assert_eq!(reset.truncated_below, None);
    assert_eq!(
        reset.committed_seq, 15,
        "reset resumes at the oldest survivor"
    );
    let claim = store
        .claim_stream_lease("_events", "late", "w1", 10, 60_000)
        .unwrap();
    let claim = claim.unwrap();
    assert_eq!((claim.from_seq, claim.to_seq), (16, 21));
}

#[test]
fn max_age_retention_drops_old_events() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    for n in 0..3 {
        // Age retention needs a timestamp; without one the event is treated as
        // "unknown age" and is never dropped.
        events
            .insert_one(json!({"n": n, "created_at": 1_000i64}))
            .unwrap();
    }
    let mut def = store.stream("_events").unwrap().unwrap();
    def.retention.max_events = None;
    def.retention.max_age_ms = Some(1);
    store.set_stream(&def).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));

    let mut wtxn = store.write_txn().unwrap();
    assert_eq!(
        store
            .prune_stream_in(&mut wtxn, "_events", 1_000)
            .unwrap()
            .len(),
        3
    );
    wtxn.commit().unwrap();
    assert!(events
        .find(&nonodb_core::FindQuery::default())
        .unwrap()
        .is_empty());
}

#[test]
fn dead_lettering_advances_the_offset_even_when_more_work_is_pending() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    for n in 0..6 {
        events.insert_one(json!({"n": n})).unwrap();
    }
    let mut group = store
        .create_stream_group("_events", "mixed", StartAt::Earliest)
        .unwrap();
    group.max_attempts = 1;
    let mut wtxn = store.write_txn().unwrap();
    store.put_stream_group_in(&mut wtxn, &group).unwrap();
    wtxn.commit().unwrap();

    // The first range expires and is dead-lettered...
    let first = store
        .claim_stream_lease("_events", "mixed", "w1", 3, 1_000)
        .unwrap()
        .unwrap();
    assert_eq!((first.from_seq, first.to_seq), (1, 3));
    std::thread::sleep(std::time::Duration::from_millis(1_200));

    // ...while a later range is claimed in the same pass. The claim must not
    // write a stale group row back over the advanced committed offset.
    let second = store
        .claim_stream_lease("_events", "mixed", "w2", 3, 60_000)
        .unwrap()
        .unwrap();
    assert_eq!((second.from_seq, second.to_seq), (4, 6));
    assert_eq!(
        store
            .stream_group("_events", "mixed")
            .unwrap()
            .unwrap()
            .committed_seq,
        3
    );
    assert_eq!(
        store
            .stream_dead_letters(Some("_events"), Some("mixed"))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn partial_commit_is_rejected_and_loses_nothing() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    for n in 0..5 {
        events.insert_one(json!({"n": n})).unwrap();
    }
    store
        .create_stream_group("_events", "partial", StartAt::Earliest)
        .unwrap();
    let claim = store
        .claim_stream_lease("_events", "partial", "w1", 5, 60_000)
        .unwrap()
        .unwrap();
    assert_eq!((claim.from_seq, claim.to_seq), (1, 5));
    let lease_id = claim.lease_id.clone().unwrap();

    // Acknowledging the middle of the range must not mark the whole range done.
    let err = store
        .ack_stream_lease("_events", "partial", &lease_id, 2)
        .unwrap_err();
    assert!(matches!(err, Error::CommitOutOfOrder(_)), "{err:?}");
    let group = store.stream_group("_events", "partial").unwrap().unwrap();
    assert_eq!(group.committed_seq, 0, "a partial ack must not advance");
    // The batch is still leased, so the same member can finish it.
    assert_eq!(
        store
            .stream_leases("_events", "partial")
            .unwrap()
            .iter()
            .filter(|lease| lease.status == nonodb_core::streams::LeaseStatus::Leased)
            .count(),
        1
    );
    store
        .ack_stream_lease("_events", "partial", &lease_id, 5)
        .unwrap();
    assert_eq!(
        store
            .stream_group("_events", "partial")
            .unwrap()
            .unwrap()
            .committed_seq,
        5
    );
}

#[test]
fn empty_retained_window_still_truncates_a_lagging_group() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    for n in 0..4 {
<<<<<<< HEAD
        events
            .insert_one(json!({"n": n, "created_at": 1_000i64}))
            .unwrap();
=======
        events.insert_one(json!({"n": n})).unwrap();
>>>>>>> 761ae58 (fix(streams): reject partial commits, truncate empty windows, per-group counters)
    }
    let mut def = store.stream("_events").unwrap().unwrap();
    // Age retention with a window that removes everything.
    def.retention.max_events = None;
    def.retention.max_age_ms = Some(1);
    store.set_stream(&def).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    let mut wtxn = store.write_txn().unwrap();
    assert_eq!(
        store
            .prune_stream_in(&mut wtxn, "_events", 1_000)
            .unwrap()
            .len(),
        4
    );
    wtxn.commit().unwrap();
    assert!(events
        .find(&nonodb_core::FindQuery::default())
        .unwrap()
        .is_empty());

    // The group was created after the prune and is behind the head.
    let lagging = store
        .create_stream_group("_events", "behind", StartAt::Earliest)
        .unwrap();
    assert_eq!(lagging.committed_seq, 0);
    let err = store
        .claim_stream_lease("_events", "behind", "w1", 10, 60_000)
        .unwrap_err();
    assert!(matches!(err, Error::StreamTruncated(_)), "{err:?}");
    let lagging = store.stream_group("_events", "behind").unwrap().unwrap();
    assert_eq!(lagging.truncated_below, Some(5));
    assert_eq!(lagging.truncations, 1);
    assert!(store.stream_leases("_events", "behind").unwrap().is_empty());

    // A fresh group at `latest` is not affected.
    store
        .create_stream_group("_events", "fresh", StartAt::Latest)
        .unwrap();
    events.insert_one(json!({"n": 5})).unwrap();
    let claim = store
        .claim_stream_lease("_events", "fresh", "w1", 10, 60_000)
        .unwrap()
        .unwrap();
    assert_eq!(claim.from_seq, 5);
}

#[test]
fn redeliveries_are_counted_per_group() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    events.insert_one(json!({"n": 0})).unwrap();
    store
        .create_stream_group("_events", "counted", StartAt::Earliest)
        .unwrap();
    assert_eq!(
        store
            .stream_group("_events", "counted")
            .unwrap()
            .unwrap()
            .redeliveries,
        0
    );
    let claim = store
        .claim_stream_lease("_events", "counted", "w1", 1, 60_000)
        .unwrap()
        .unwrap();
    store
        .nack_stream_lease("_events", "counted", claim.lease_id.as_deref().unwrap(), 0)
        .unwrap();
    let again = store
        .claim_stream_lease("_events", "counted", "w2", 1, 60_000)
        .unwrap()
        .unwrap();
    let group = store.stream_group("_events", "counted").unwrap().unwrap();
    assert_eq!(group.redeliveries, 1);
    assert_eq!(again.attempts, 2);
}
<<<<<<< HEAD

#[test]
fn updates_cannot_rewrite_or_remove_the_reserved_offset() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    let id = events.insert_one(json!({"n": 1})).unwrap();
    assert!(events.find(&nonodb_core::FindQuery::default()).unwrap()[0]
        .get("_seq")
        .is_some());

    // `update`, `$unset`, and find-and-modify must all refuse to touch `_seq`.
    for update in [
        json!({"$set": {"_seq": 100}}),
        json!({"$unset": {"_seq": ""}}),
        json!({"$inc": {"_seq": 1}}),
    ] {
        let err = events.update(&json!({}), &update, false).unwrap_err();
        assert!(matches!(err, Error::Query(_)), "{update}: {err:?}");
    }
    let err = events
        .update_opts(
            &json!({"_id": id}),
            &json!({"$set": {"_seq": 7}}),
            false,
            true,
        )
        .unwrap_err();
    assert!(matches!(err, Error::Query(_)), "{err:?}");

    // The offset is unchanged and a normal update still works.
    events
        .update(&json!({"_id": id}), &json!({"$set": {"n": 2}}), false)
        .unwrap();
    let doc = &events.find(&nonodb_core::FindQuery::default()).unwrap()[0];
    assert_eq!(doc["_seq"], json!(1), "{doc}");
    assert_eq!(doc["n"], json!(2), "{doc}");
}

#[test]
fn deleting_a_group_removes_its_leases_and_dead_letters() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    for n in 0..10 {
        events.insert_one(json!({"n": n})).unwrap();
    }
    let mut group = store
        .create_stream_group("_events", "gone", StartAt::Earliest)
        .unwrap();
    group.max_attempts = 1;
    let mut wtxn = store.write_txn().unwrap();
    store.put_stream_group_in(&mut wtxn, &group).unwrap();
    wtxn.commit().unwrap();

    let claim = store
        .claim_stream_lease("_events", "gone", "w1", 3, 1_000)
        .unwrap()
        .unwrap();
    assert_eq!((claim.from_seq, claim.to_seq), (1, 3));
    std::thread::sleep(std::time::Duration::from_millis(1_200));
    // Expire it through the janitor so a dead letter exists too.
    store.recover_stream_leases().unwrap();
    assert_eq!(
        store
            .stream_dead_letters(Some("_events"), Some("gone"))
            .unwrap()
            .len(),
        1
    );

    assert!(store.delete_stream_group("_events", "gone").unwrap());
    assert!(store.stream_leases("_events", "gone").unwrap().is_empty());
    assert!(store
        .stream_dead_letters(Some("_events"), Some("gone"))
        .unwrap()
        .is_empty());

    // A recreated group starts at `latest` and must not see the old batches.
    let fresh = store
        .create_stream_group("_events", "gone", StartAt::Latest)
        .unwrap();
    assert_eq!(fresh.committed_seq, 10);
    assert!(store
        .claim_stream_lease("_events", "gone", "w1", 10, 60_000)
        .unwrap()
        .is_none());
}

#[test]
fn the_janitor_recovers_expired_leases_without_a_claim() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    events.insert_one(json!({"n": 0})).unwrap();
    store
        .create_stream_group("_events", "janitor", StartAt::Earliest)
        .unwrap();
    store
        .claim_stream_lease("_events", "janitor", "w1", 1, 1_000)
        .unwrap()
        .unwrap();
    assert_eq!(
        store.stream_leases("_events", "janitor").unwrap()[0].status,
        nonodb_core::streams::LeaseStatus::Leased
    );

    // Nothing has expired yet.
    assert_eq!(store.recover_stream_leases().unwrap(), (1, 0));
    std::thread::sleep(std::time::Duration::from_millis(1_200));
    let (groups, recovered) = store.recover_stream_leases().unwrap();
    assert_eq!((groups, recovered), (1, 1));
    let lease = &store.stream_leases("_events", "janitor").unwrap()[0];
    assert_eq!(lease.status, nonodb_core::streams::LeaseStatus::Available);
    assert!(lease.lease_id.is_none());
    // Recovered once; a second pass finds nothing to do.
    assert_eq!(store.recover_stream_leases().unwrap(), (1, 0));
}

#[test]
fn automatic_change_events_get_offsets() {
    let (_dir, store) = stream_store();
    let orders = store.collection("orders").unwrap();
    let events = store.collection("_events").unwrap();

    // Automatic change events are written through `append_outbox_event`; they
    // must carry offsets like any other stream event, otherwise consumer groups
    // over `_events` would silently skip the main outbox producer.
    let mut wtxn = store.write_txn().unwrap();
    orders
        .append_outbox_event(
            &mut wtxn,
            json!({"type": "orders.insert", "payload": {"_id": "o1"}}),
        )
        .unwrap();
    wtxn.commit().unwrap();
    let docs = events.find(&nonodb_core::FindQuery::default()).unwrap();
    assert_eq!(docs.len(), 1, "{docs:?}");
    assert_eq!(docs[0]["_seq"], json!(1), "{docs:?}");
    assert_eq!(store.stream_head("_events").unwrap(), 1);

    // Direct and automatic inserts share one counter.
    events.insert_one(json!({"type": "manual"})).unwrap();
    let mut wtxn = store.write_txn().unwrap();
    orders
        .append_outbox_event(
            &mut wtxn,
            json!({"type": "orders.update", "payload": {"_id": "o1"}}),
        )
        .unwrap();
    wtxn.commit().unwrap();
    let query = nonodb_core::FindQuery {
        sort: vec![("_seq".to_string(), 1)],
        ..Default::default()
    };
    let seqs: Vec<u64> = events
        .find(&query)
        .unwrap()
        .iter()
        .map(|doc| doc["_seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, vec![1, 2, 3]);
    assert_eq!(store.stream_head("_events").unwrap(), 3);

    // A consumer group delivers them in order.
    store
        .create_stream_group("_events", "outbox", StartAt::Earliest)
        .unwrap();
    let claim = store
        .claim_stream_lease("_events", "outbox", "w1", 10, 60_000)
        .unwrap()
        .unwrap();
    assert_eq!((claim.from_seq, claim.to_seq), (1, 3));
}

#[test]
fn a_retried_dead_letter_can_be_committed_after_the_group_moved_on() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    for n in 0..3 {
        events.insert_one(json!({"n": n})).unwrap();
    }
    let mut group = store
        .create_stream_group("_events", "retry-behind", StartAt::Earliest)
        .unwrap();
    group.max_attempts = 1;
    let mut wtxn = store.write_txn().unwrap();
    store.put_stream_group_in(&mut wtxn, &group).unwrap();
    wtxn.commit().unwrap();

    // First batch expires and is dead-lettered, advancing the offset to 3.
    store
        .claim_stream_lease("_events", "retry-behind", "w1", 3, 1_000)
        .unwrap()
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1_200));
    store.recover_stream_leases().unwrap();
    assert_eq!(
        store
            .stream_group("_events", "retry-behind")
            .unwrap()
            .unwrap()
            .committed_seq,
        3
    );
    assert_eq!(
        store
            .stream_dead_letters(Some("_events"), Some("retry-behind"))
            .unwrap()
            .len(),
        1
    );

    // More work is done, so the group is now ahead of the dead range.
    for n in 3..6 {
        events.insert_one(json!({"n": n})).unwrap();
    }
    let second = store
        .claim_stream_lease("_events", "retry-behind", "w1", 3, 60_000)
        .unwrap()
        .unwrap();
    store
        .ack_stream_lease(
            "_events",
            "retry-behind",
            second.lease_id.as_deref().unwrap(),
            6,
        )
        .unwrap();
    assert_eq!(
        store
            .stream_group("_events", "retry-behind")
            .unwrap()
            .unwrap()
            .committed_seq,
        6
    );

    // Retrying the old batch must be committable even though it is below the
    // committed offset; the offset must not move backwards.
    assert!(store
        .retry_stream_dead("_events", "retry-behind", 1)
        .unwrap());
    let replay = store
        .claim_stream_lease("_events", "retry-behind", "w2", 10, 60_000)
        .unwrap()
        .unwrap();
    assert_eq!((replay.from_seq, replay.to_seq), (1, 3));
    assert!(replay.retried);
    store
        .ack_stream_lease(
            "_events",
            "retry-behind",
            replay.lease_id.as_deref().unwrap(),
            3,
        )
        .unwrap();
    let group = store
        .stream_group("_events", "retry-behind")
        .unwrap()
        .unwrap();
    assert_eq!(
        group.committed_seq, 6,
        "a replay must not rewind the offset"
    );
    assert!(store
        .stream_leases("_events", "retry-behind")
        .unwrap()
        .is_empty());
    // The group is caught up again.
    assert!(store
        .claim_stream_lease("_events", "retry-behind", "w1", 10, 60_000)
        .unwrap()
        .is_none());
}

#[test]
fn a_promoted_node_redelivers_claimed_but_uncommitted_ranges() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    for n in 1..=5 {
        events.insert_one(json!({ "n": n })).unwrap();
    }
    store
        .create_stream_group_with_attempts("_events", "g", StartAt::Earliest, 3)
        .unwrap();

    let mut wtxn = store.write_txn().unwrap();
    let lease = store
        .claim_stream_lease_in(&mut wtxn, "_events", "g", "a", 3, 60_000)
        .unwrap()
        .unwrap();
    wtxn.commit().unwrap();
    assert_eq!((lease.from_seq, lease.to_seq), (1, 3));
    assert_eq!(
        store
            .stream_group("_events", "g")
            .unwrap()
            .unwrap()
            .next_seq,
        3
    );

    // A promoted replica never saw the claim: leases are node-local, so it
    // starts from its own (empty) lease table.
    assert_eq!(store.clear_stream_leases("_events", "g").unwrap(), 1);
    let mut wtxn = store.write_txn().unwrap();
    let lease = store
        .claim_stream_lease_in(&mut wtxn, "_events", "g", "b", 3, 60_000)
        .unwrap()
        .unwrap();
    wtxn.commit().unwrap();
    assert_eq!(
        (lease.from_seq, lease.to_seq),
        (1, 3),
        "ranges claimed but never committed must be redelivered, not skipped"
    );
}

#[test]
fn claim_readiness_is_read_only_and_false_when_caught_up() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    events.insert_one(json!({ "n": 1 })).unwrap();
    events.insert_one(json!({ "n": 2 })).unwrap();
    store
        .create_stream_group_with_attempts("_events", "g", StartAt::Earliest, 3)
        .unwrap();

    assert!(store.stream_claim_ready("_events", "g").unwrap());
    let mut wtxn = store.write_txn().unwrap();
    let lease = store
        .claim_stream_lease_in(&mut wtxn, "_events", "g", "a", 10, 60_000)
        .unwrap()
        .unwrap();
    wtxn.commit().unwrap();

    // Leased but not committed: nothing new to claim, and no write happens.
    let before = store.stream_group("_events", "g").unwrap().unwrap();
    assert!(!store.stream_claim_ready("_events", "g").unwrap());
    assert!(store
        .claim_stream_lease("_events", "g", "b", 10, 60_000)
        .unwrap()
        .is_none());
    let after = store.stream_group("_events", "g").unwrap().unwrap();
    assert_eq!(
        (before.next_seq, before.updated_at),
        (after.next_seq, after.updated_at),
        "an idle claim must not write the group row"
    );

    store
        .ack_stream_lease(
            "_events",
            "g",
            lease.lease_id.as_deref().unwrap(),
            lease.to_seq,
        )
        .unwrap();
    assert!(!store.stream_claim_ready("_events", "g").unwrap());
    events.insert_one(json!({ "n": 3 })).unwrap();
    assert!(store.stream_claim_ready("_events", "g").unwrap());
}

#[test]
fn streams_require_a_standard_collection() {
    let (_dir, store) = store();
    store
        .create_timeseries_collection("metrics", &Default::default())
        .unwrap();
    let mut def = nonodb_core::streams::StreamDef::new("metrics", "metrics");
    def.retention.max_events = None;
    let err = store.set_stream(&def).unwrap_err();
    assert!(
        matches!(err, Error::Query(ref message) if message.contains("only standard collections")),
        "unexpected error: {err:?}"
    );
}

#[test]
fn max_age_retention_never_drops_a_fresh_event_behind_an_out_of_order_one() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    let ids: Vec<String> = (0..4)
        .map(|n| events.insert_one(json!({ "n": n })).unwrap())
        .collect();
    // 1 and 2 are old; 3 claims to be fresh although it sits after them; the
    // newest document (4) is deleted by hand, so the counter head no longer has
    // a document behind it.
    for (id, created_at) in [
        (&ids[0], 1_000i64),
        (&ids[1], 1_000),
        (&ids[2], 4_000_000_000_000),
    ] {
        events
            .update(
                &json!({ "_id": id }),
                &json!({ "$set": { "created_at": created_at } }),
                false,
            )
            .unwrap();
    }
    events.delete(&json!({ "_id": &ids[3] }), false).unwrap();
    let mut def = stream("_events", &store);
    def.retention.max_events = None;
    def.retention.max_age_ms = Some(60_000);
    store.set_stream(&def).unwrap();

    let mut wtxn = store.write_txn().unwrap();
    let dropped = store.prune_stream_in(&mut wtxn, "_events", 1_000).unwrap();
    wtxn.commit().unwrap();
    assert_eq!(
        dropped.len(),
        2,
        "only the provably old prefix is dropped, and the deleted head cannot widen the boundary"
    );
    let remaining = remaining_seqs(&store, &events);
    assert_eq!(remaining, vec![3], "the fresh event 3 must survive");
}

#[test]
fn max_age_retention_stops_at_a_missing_timestamp() {
    let (_dir, store) = stream_store();
    let events = store.collection("_events").unwrap();
    let ids: Vec<String> = (0..3)
        .map(|n| events.insert_one(json!({ "n": n })).unwrap())
        .collect();
    events
        .update(
            &json!({ "_id": &ids[0] }),
            &json!({ "$set": { "created_at": 1_000i64 } }),
            false,
        )
        .unwrap();
    events
        .update(
            &json!({ "_id": &ids[1] }),
            &json!({ "$unset": { "created_at": "" } }),
            false,
        )
        .unwrap();
    let mut def = stream("_events", &store);
    def.retention.max_events = None;
    def.retention.max_age_ms = Some(1);
    store.set_stream(&def).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));

    let mut wtxn = store.write_txn().unwrap();
    let dropped = store.prune_stream_in(&mut wtxn, "_events", 1_000).unwrap();
    wtxn.commit().unwrap();
    assert_eq!(
        dropped.len(),
        1,
        "an event without `created_at` is not known to be old, so nothing after it may be dropped"
    );
}

fn stream(name: &str, store: &Store) -> nonodb_core::streams::StreamDef {
    store.stream(name).unwrap().unwrap()
}

fn remaining_seqs(store: &Store, events: &nonodb_core::Collection<'_>) -> Vec<u64> {
    let _ = store;
    let mut seqs: Vec<u64> = events
        .find(&nonodb_core::FindQuery {
            sort: vec![("_seq".to_string(), 1)],
            ..Default::default()
        })
        .unwrap()
        .iter()
        .filter_map(|doc| doc.get("_seq").and_then(Value::as_u64))
        .collect();
    seqs.sort_unstable();
    seqs
}
=======
>>>>>>> 761ae58 (fix(streams): reject partial commits, truncate empty windows, per-group counters)
