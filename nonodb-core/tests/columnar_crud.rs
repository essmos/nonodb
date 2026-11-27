//! Differential CRUD tests for the experimental columnar profile.

use nonodb_core::{Collection, CollectionProfile, FindAndModifyOptions, FindQuery, Store};
use serde_json::{json, Value};

fn query(filter: Value, sort: Vec<(String, i32)>, limit: Option<usize>) -> FindQuery {
    FindQuery {
        filter,
        projection: None,
        sort,
        skip: 0,
        limit,
        comment: None,
        max_time_ms: None,
        hint: None,
    }
}

fn standard_collection(store: &Store) -> Collection<'_> {
    store
        .create_collection("docs", CollectionProfile::Standard)
        .unwrap()
}

fn columnar_collection(store: &Store) -> Collection<'_> {
    store
        .create_columnar_collection("docs", &nonodb_core::columnar::Settings::default())
        .unwrap()
}

fn fixture() -> Vec<Value> {
    vec![
        json!({"_id": "a", "n": 1, "group": "x", "nested": {"v": true}, "tags": ["a", "b"]}),
        json!({"_id": "b", "n": 2, "group": "x", "nested": {"v": false}, "tags": ["b"]}),
        json!({"_id": "c", "n": 3, "group": "y", "nested": {"v": true}, "extra": null}),
    ]
}

#[test]
fn columnar_matches_standard_for_crud_and_find() {
    let std_dir = tempfile::tempdir().unwrap();
    let col_dir = tempfile::tempdir().unwrap();
    let std_store = Store::open(std_dir.path()).unwrap();
    let col_store = Store::open(col_dir.path()).unwrap();
    let std_coll = standard_collection(&std_store);
    let col_coll = columnar_collection(&col_store);

    for doc in fixture() {
        std_coll.insert_one(doc.clone()).unwrap();
        col_coll.insert_one(doc).unwrap();
    }
    assert_eq!(
        std_coll
            .find(&query(json!({}), vec![("_id".into(), 1)], None))
            .unwrap(),
        col_coll
            .find(&query(json!({}), vec![("_id".into(), 1)], None))
            .unwrap()
    );
    assert_eq!(
        std_coll
            .find(&query(
                json!({"n": {"$gte": 2}}),
                vec![("n".into(), -1)],
                None
            ))
            .unwrap(),
        col_coll
            .find(&query(
                json!({"n": {"$gte": 2}}),
                vec![("n".into(), -1)],
                None
            ))
            .unwrap()
    );
    assert_eq!(
        std_coll.count(&json!({"group": "x"})).unwrap(),
        col_coll.count(&json!({"group": "x"})).unwrap()
    );

    let update = json!({"$inc": {"n": 10}, "$set": {"nested.v": false}});
    assert_eq!(
        std_coll
            .update(&json!({"group": "x"}), &update, true)
            .unwrap(),
        col_coll
            .update(&json!({"group": "x"}), &update, true)
            .unwrap()
    );
    assert_eq!(
        std_coll
            .find(&query(json!({}), vec![("_id".into(), 1)], None))
            .unwrap(),
        col_coll
            .find(&query(json!({}), vec![("_id".into(), 1)], None))
            .unwrap()
    );

    let find_update = json!({"$set": {"updated": true}});
    let std_outcome = std_coll
        .find_and_modify(&FindAndModifyOptions {
            filter: &json!({"group": "y"}),
            update: Some(&find_update),
            sort: &[("_id".into(), 1)],
            new: true,
            upsert: false,
            remove: false,
            array_filters: &[],
            projection: None,
        })
        .unwrap();
    let col_outcome = col_coll
        .find_and_modify(&FindAndModifyOptions {
            filter: &json!({"group": "y"}),
            update: Some(&find_update),
            sort: &[("_id".into(), 1)],
            new: true,
            upsert: false,
            remove: false,
            array_filters: &[],
            projection: None,
        })
        .unwrap();
    assert_eq!(std_outcome.value, col_outcome.value);

    assert_eq!(std_coll.delete(&json!({"group": "x"}), true).unwrap(), 2);
    assert_eq!(col_coll.delete(&json!({"group": "x"}), true).unwrap(), 2);
    assert_eq!(
        std_coll
            .find(&query(json!({}), vec![("_id".into(), 1)], None))
            .unwrap(),
        col_coll
            .find(&query(json!({}), vec![("_id".into(), 1)], None))
            .unwrap()
    );

    let upsert_filter = json!({"_id": "upserted"});
    let upsert_update = json!({"$set": {"n": 99, "group": "z"}});
    let std_up = std_coll
        .find_and_modify(&FindAndModifyOptions {
            filter: &upsert_filter,
            update: Some(&upsert_update),
            sort: &[],
            new: true,
            upsert: true,
            remove: false,
            array_filters: &[],
            projection: None,
        })
        .unwrap();
    let col_up = col_coll
        .find_and_modify(&FindAndModifyOptions {
            filter: &upsert_filter,
            update: Some(&upsert_update),
            sort: &[],
            new: true,
            upsert: true,
            remove: false,
            array_filters: &[],
            projection: None,
        })
        .unwrap();
    assert_eq!(std_up.upserted_id, col_up.upserted_id);
    assert_eq!(std_up.value, col_up.value);
}

#[test]
fn rollback_and_duplicate_failures_leave_no_partial_documents() {
    let std_dir = tempfile::tempdir().unwrap();
    let col_dir = tempfile::tempdir().unwrap();
    let std_store = Store::open(std_dir.path()).unwrap();
    let col_store = Store::open(col_dir.path()).unwrap();
    let std_coll = standard_collection(&std_store);
    let col_coll = columnar_collection(&col_store);
    std_coll
        .insert_one(json!({"_id": "a", "value": 1}))
        .unwrap();
    col_coll
        .insert_one(json!({"_id": "a", "value": 1}))
        .unwrap();

    assert!(std_coll
        .insert_one(json!({"_id": "a", "value": 2}))
        .is_err());
    assert!(col_coll
        .insert_one(json!({"_id": "a", "value": 2}))
        .is_err());
    assert_eq!(std_coll.count(&json!({})).unwrap(), 1);
    assert_eq!(col_coll.count(&json!({})).unwrap(), 1);

    let mut std_wtxn = std_store.write_txn().unwrap();
    let std_txn_coll = std_store.collection_in(&mut std_wtxn, "docs").unwrap();
    std_txn_coll
        .insert_one_txn(&mut std_wtxn, json!({"_id": "rolled-back", "value": 3}))
        .unwrap();
    drop(std_wtxn);

    let mut col_wtxn = col_store.write_txn().unwrap();
    let col_txn_coll = col_store.collection_in(&mut col_wtxn, "docs").unwrap();
    col_txn_coll
        .insert_one_txn(&mut col_wtxn, json!({"_id": "rolled-back", "value": 3}))
        .unwrap();
    drop(col_wtxn);

    assert!(std_coll.find_by_key("rolled-back").unwrap().is_none());
    assert!(col_coll.find_by_key("rolled-back").unwrap().is_none());
}

#[test]
fn columnar_roundtrips_bson_scalars_and_nested_values() {
    let std_dir = tempfile::tempdir().unwrap();
    let col_dir = tempfile::tempdir().unwrap();
    let std_store = Store::open(std_dir.path()).unwrap();
    let col_store = Store::open(col_dir.path()).unwrap();
    let std_coll = standard_collection(&std_store);
    let col_coll = columnar_collection(&col_store);
    let doc = json!({
        "_id": "bson",
        "date": {"$date": {"$numberLong": "1700000000123"}},
        "long": {"$numberLong": "9007199254740993"},
        "decimal": {"$numberDecimal": "1.2500"},
        "oid": {"$oid": "000000000000000000000001"},
        "binary": {"$binary": {"base64": "AQID", "subType": "00"}},
        "nested": {"empty": {}, "array": [1, {"x": null}, [true, false]]},
    });
    std_coll.insert_one(doc.clone()).unwrap();
    col_coll.insert_one(doc).unwrap();
    assert_eq!(
        std_coll.find_by_key("bson").unwrap(),
        col_coll.find_by_key("bson").unwrap()
    );
}
