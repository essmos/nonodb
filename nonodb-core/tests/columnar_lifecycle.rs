//! TTL and history lifecycle tests for the experimental columnar profile.

use nonodb_core::{FindQuery, Store};
use serde_json::json;

fn columnar(store: &Store) -> nonodb_core::Collection<'_> {
    store
        .create_columnar_collection("docs", &nonodb_core::columnar::Settings::default())
        .unwrap()
}

fn date(ms: u64) -> serde_json::Value {
    json!({"$date": {"$numberLong": ms.to_string()}})
}

#[test]
fn columnar_ttl_update_expire_and_visibility_follow_standard_rules() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    columnar(&store);
    store
        .set_collection_ttl_after("docs", "expireAt", 60, Some(false))
        .unwrap();
    store.set_collection_history("docs", 10).unwrap();
    let c = store.collection("docs").unwrap();
    let now = nonodb_core::ttl::now_ms();
    c.insert_one(json!({"_id": "old", "expireAt": date(now - 120_000), "v": 1}))
        .unwrap();
    c.insert_one(json!({"_id": "new", "expireAt": date(now + 120_000), "v": 2}))
        .unwrap();
    assert_eq!(c.find(&FindQuery::default()).unwrap().len(), 1);

    c.update(
        &json!({"_id": "old"}),
        &json!({"$set": {"expireAt": date(now + 120_000)}}),
        false,
    )
    .unwrap();
    assert_eq!(c.find(&FindQuery::default()).unwrap().len(), 2);

    c.update(
        &json!({"_id": "new"}),
        &json!({"$set": {"expireAt": date(now - 120_000)}}),
        false,
    )
    .unwrap();
    assert_eq!(c.find(&FindQuery::default()).unwrap().len(), 1);
    assert_eq!(c.lookup("_id", &[json!("new")], None).unwrap(), vec![None]);

    let due = store.ttl_due(now, 10).unwrap();
    let entry = due.iter().find(|entry| entry.id == "new").unwrap().clone();
    let mut wtxn = store.write_txn().unwrap();
    let expired = c
        .expire_txn_with_doc(&mut wtxn, &entry.id, entry.expire_at)
        .unwrap();
    assert!(expired.is_some());
    wtxn.commit().unwrap();
    assert!(c.find_by_key("new").unwrap().is_none());
    assert_eq!(c.find(&FindQuery::default()).unwrap().len(), 1);
}

#[test]
fn columnar_ttl_purge_removes_history_and_leaves_other_docs() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    columnar(&store);
    store
        .set_collection_ttl_after("docs", "expireAt", 60, Some(true))
        .unwrap();
    store.set_collection_history("docs", 10).unwrap();
    let c = store.collection("docs").unwrap();
    let now = nonodb_core::ttl::now_ms();
    c.insert_one(json!({"_id": "expired", "expireAt": date(now - 120_000), "v": 1}))
        .unwrap();
    c.insert_one(json!({"_id": "live", "expireAt": date(now + 60_000), "v": 2}))
        .unwrap();
    assert_eq!(c.history("expired", None, false).unwrap().len(), 1);
    let due = store.ttl_due(now, 10).unwrap();
    let entry = due
        .iter()
        .find(|entry| entry.id == "expired")
        .unwrap()
        .clone();
    let mut wtxn = store.write_txn().unwrap();
    assert!(c.expire_txn(&mut wtxn, &entry.id, entry.expire_at).unwrap());
    wtxn.commit().unwrap();
    assert!(c.history("expired", None, false).unwrap().is_empty());
    assert_eq!(c.find(&FindQuery::default()).unwrap().len(), 1);
    assert_eq!(c.find_by_key("live").unwrap().unwrap()["v"], 2);
}

#[test]
fn columnar_date_ttl_backfill_pages_are_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let c = columnar(&store);
    let now = nonodb_core::ttl::now_ms();
    for index in 0..10 {
        c.insert_one(json!({
            "_id": format!("d{index:02}"),
            "expireAt": date(now + 60_000 + index as u64),
        }))
        .unwrap();
    }
    let mut after = None;
    let mut pages = 0;
    loop {
        let mut wtxn = store.write_txn().unwrap();
        let page = store
            .backfill_collection_ttl_in(&mut wtxn, "docs", "expireAt", 60, after.as_ref(), 3, false)
            .unwrap();
        wtxn.commit().unwrap();
        pages += 1;
        after = page.next_after.clone();
        if page.done {
            break;
        }
    }
    assert_eq!(pages, 4);
    let due = store.ttl_due(now + 300_000, 100).unwrap();
    assert_eq!(due.len(), 10);
}
