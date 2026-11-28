//! Differential index tests for the experimental columnar profile.

use nonodb_core::{CollectionProfile, FindQuery, IndexSpec, Store};
use serde_json::json;

fn columnar(store: &Store) -> nonodb_core::Collection<'_> {
    store
        .create_columnar_collection("docs", &nonodb_core::columnar::Settings::default())
        .unwrap()
}

#[test]
fn columnar_secondary_unique_and_compound_indexes_backfill_and_maintain() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let coll = columnar(&store);
    for (id, g, a, b) in [("a", "x", 1, 10), ("b", "y", 2, 20), ("c", "z", 3, 30)] {
        coll.insert_one(json!({"_id": id, "g": g, "a": a, "b": b, "c": id}))
            .unwrap();
    }

    coll.create_index("g", true).unwrap();
    assert_eq!(coll.list_indexes().unwrap(), vec![("g".to_string(), true)]);
    assert_eq!(
        coll.lookup(
            "g",
            &[json!("x"), json!("missing")],
            Some(&json!({"_id": 1}))
        )
        .unwrap(),
        vec![Some(json!({"_id": "a"})), None]
    );
    assert!(coll.insert_one(json!({"_id": "dup", "g": "x"})).is_err());
    assert!(coll.find_by_key("dup").unwrap().is_none());
    assert!(coll
        .update(&json!({"_id": "b"}), &json!({"$set": {"g": "x"}}), false)
        .is_err());
    assert_eq!(coll.find_by_key("b").unwrap().unwrap()["g"], "y");
    assert_eq!(coll.delete(&json!({"_id": "a"}), false).unwrap(), 1);
    assert!(coll
        .insert_one(json!({"_id": "reuse", "g": "x"}))
        .unwrap()
        .eq("reuse"));

    coll.create_indexes(&[IndexSpec {
        fields: vec!["a".into(), "b".into()],
        name: "ab_idx".into(),
        directions: vec![1, -1],
        unique: false,
        sparse: true,
        partial_filter_expression: Some(json!({"a": {"$gt": 0}})),
        include: vec!["c".into()],
        geo: false,
    }])
    .unwrap();
    let indexes = coll.list_indexes().unwrap();
    assert!(indexes
        .iter()
        .any(|(name, unique)| name == "ab_idx" && !unique));

    let report = coll.reindex().unwrap();
    assert!(report.secondary.iter().any(|name| name == "ab_idx"));
    assert_eq!(
        coll.lookup("g", &[json!("x")], None).unwrap(),
        vec![Some(json!({"_id": "reuse", "g": "x"}))]
    );

    assert!(coll.drop_index("ab_idx").unwrap());
    assert!(coll.drop_index("g").unwrap());
    assert!(coll.list_indexes().unwrap().is_empty());
    assert_eq!(
        coll.find(&FindQuery {
            filter: json!({"g": "x"}),
            ..Default::default()
        })
        .unwrap()
        .len(),
        1
    );
}

#[test]
fn columnar_geo_index_creates_and_reindexes() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let coll = columnar(&store);
    coll.insert_one(json!({"_id": "p", "loc": {"type": "Point", "coordinates": [1.0, 2.0]}}))
        .unwrap();
    coll.create_geo_index("loc").unwrap();
    assert!(coll
        .list_indexes()
        .unwrap()
        .iter()
        .any(|(name, _)| name == "loc"));
    let report = coll.reindex().unwrap();
    assert!(report.secondary.iter().any(|name| name == "loc"));
}

#[test]
fn columnar_indexes_do_not_change_standard_behavior() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let standard = store
        .create_collection("docs", CollectionProfile::Standard)
        .unwrap();
    standard.insert_one(json!({"_id": "a", "g": "x"})).unwrap();
    standard.create_index("g", true).unwrap();
    assert_eq!(
        standard.lookup("g", &[json!("x")], None).unwrap(),
        vec![Some(json!({"_id": "a", "g": "x"}))]
    );
}

#[test]
fn columnar_geo_near_matches_standard() {
    let sd = tempfile::tempdir().unwrap();
    let cd = tempfile::tempdir().unwrap();
    let ss = Store::open(sd.path()).unwrap();
    let cs = Store::open(cd.path()).unwrap();
    let sc = ss
        .create_collection("docs", CollectionProfile::Standard)
        .unwrap();
    let cc = columnar(&cs);
    for doc in [
        json!({"_id": "krakow", "loc": {"type": "Point", "coordinates": [19.94, 50.06]}}),
        json!({"_id": "warsaw", "loc": {"type": "Point", "coordinates": [21.01, 52.23]}}),
        json!({"_id": "far", "loc": {"type": "Point", "coordinates": [-74.0, 40.7]}}),
    ] {
        sc.insert_one(doc.clone()).unwrap();
        cc.insert_one(doc).unwrap();
    }
    sc.create_geo_index("loc").unwrap();
    cc.create_geo_index("loc").unwrap();
    let q = FindQuery {
        filter: json!({
            "loc": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [19.94, 50.06]},
                    "$maxDistance": 500
                }
            }
        }),
        ..Default::default()
    };
    assert_eq!(cc.find(&q).unwrap(), sc.find(&q).unwrap());
    assert_eq!(cc.count(&q.filter).unwrap(), sc.count(&q.filter).unwrap());
}

#[test]
fn failed_columnar_unique_backfill_rolls_back_index() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let coll = columnar(&store);
    coll.insert_one(json!({"_id": "a", "g": "same"})).unwrap();
    coll.insert_one(json!({"_id": "b", "g": "same"})).unwrap();
    assert!(coll.create_index("g", true).is_err());
    assert!(coll.list_indexes().unwrap().is_empty());
    assert_eq!(coll.delete(&json!({"_id": "b"}), false).unwrap(), 1);
    coll.create_index("g", true).unwrap();
    assert_eq!(
        coll.lookup("g", &[json!("same")], None).unwrap(),
        vec![Some(json!({"_id": "a", "g": "same"}))]
    );
}

#[test]
fn columnar_index_backfill_registers_finished_progress() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let coll = store
        .create_columnar_collection("progress_docs", &nonodb_core::columnar::Settings::default())
        .unwrap();
    for (id, g) in [("a", "x"), ("b", "y"), ("c", "z")] {
        coll.insert_one(json!({"_id": id, "g": g})).unwrap();
    }
    coll.create_index("g", false).unwrap();
    let snapshot = nonodb_core::progress::snapshot();
    let entry = snapshot
        .iter()
        .find(|entry| entry.collection == "progress_docs")
        .expect("columnar index build progress");
    assert_eq!(entry.kind, "index");
    assert_eq!(entry.total, 3);
    assert_eq!(entry.processed, 3);
    assert!(entry.finished);
}
