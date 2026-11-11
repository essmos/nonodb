use nonodb_core::{Store, VectorMetric};
use serde_json::json;
use tempfile::TempDir;

fn store() -> (TempDir, Store) {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let c = store.collection("vectors").unwrap();
    for doc in [
        json!({"_id": "a", "tenant": "acme", "embedding": [1.0, 0.0]}),
        json!({"_id": "b", "tenant": "acme", "embedding": [0.9, 0.1]}),
        json!({"_id": "c", "tenant": "other", "embedding": [-1.0, 0.0]}),
        json!({"_id": "missing", "tenant": "acme"}),
        json!({"_id": "bad", "tenant": "acme", "embedding": [1.0]}),
    ] {
        c.insert_one(doc).unwrap();
    }
    c.create_vector_index("embedding", 2, VectorMetric::Cosine)
        .unwrap();
    c.create_index("tenant", false).unwrap();
    (dir, store)
}

#[test]
fn vector_index_backfills_and_ranks_by_distance() {
    let (_dir, store) = store();
    let c = store.collection("vectors").unwrap();

    let results = c
        .vector_search("embedding", &[1.0, 0.0], &json!({}), 0, Some(10))
        .unwrap();
    let ids: Vec<_> = results
        .iter()
        .map(|doc| doc["_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["a", "b", "c"]);
    assert!(results[0]["_distance"].as_f64().unwrap() < 1e-6);
    assert!(results[1]["_distance"].as_f64().unwrap() < 1.0);
}

#[test]
fn vector_search_applies_filter_before_ranking() {
    let (_dir, store) = store();
    let c = store.collection("vectors").unwrap();

    let results = c
        .vector_search(
            "embedding",
            &[1.0, 0.0],
            &json!({"tenant": "other"}),
            0,
            Some(10),
        )
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["_id"], "c");
}

#[test]
fn vector_search_uses_id_and_index_filter_fast_paths() {
    let (_dir, store) = store();
    let c = store.collection("vectors").unwrap();

    let results = c
        .vector_search("embedding", &[1.0, 0.0], &json!({"_id": "b"}), 0, Some(10))
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["_id"], "b");

    let results = c
        .vector_search(
            "embedding",
            &[1.0, 0.0],
            &json!({"tenant": "acme"}),
            1,
            Some(1),
        )
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["_id"], "b");

    let results = c
        .vector_search(
            "embedding",
            &[1.0, 0.0],
            &json!({"tenant": "absent"}),
            0,
            Some(10),
        )
        .unwrap();
    assert!(results.is_empty());
}

#[test]
fn vector_index_tracks_updates_deletes_and_dimension_errors() {
    let (_dir, store) = store();
    let c = store.collection("vectors").unwrap();

    let mut wtxn = store.write_txn().unwrap();
    c.update_opts_txn_filtered(
        &mut wtxn,
        &json!({"_id": "c"}),
        &json!({"$set": {"embedding": [1.0, 0.0]}}),
        false,
        false,
        &[],
    )
    .unwrap();
    wtxn.commit().unwrap();

    let results = c
        .vector_search("embedding", &[1.0, 0.0], &json!({}), 0, Some(3))
        .unwrap();
    let ids: Vec<_> = results
        .iter()
        .map(|doc| doc["_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["a", "c", "b"]);

    let mut wtxn = store.write_txn().unwrap();
    c.delete_txn(&mut wtxn, &json!({"_id": "a"}), false)
        .unwrap();
    wtxn.commit().unwrap();

    let results = c
        .vector_search("embedding", &[1.0, 0.0], &json!({}), 0, Some(3))
        .unwrap();
    assert_eq!(results[0]["_id"], "c");
    assert!(results.iter().all(|doc| doc["_id"] != "a"));

    assert!(c
        .vector_search("embedding", &[1.0], &json!({}), 0, Some(3))
        .is_err());
}

#[test]
fn vector_index_metadata_and_persistence_survive_reopen() {
    let (dir, store) = store();
    let defs = store
        .collection("vectors")
        .unwrap()
        .list_vector_indexes()
        .unwrap();
    assert_eq!(defs.len(), 1);
    assert_eq!(defs[0].field, "embedding");
    assert_eq!(defs[0].dims, 2);
    assert_eq!(defs[0].metric, VectorMetric::Cosine);

    drop(store);
    let reopened = Store::open(dir.path()).unwrap();
    let results = reopened
        .collection("vectors")
        .unwrap()
        .vector_search("embedding", &[1.0, 0.0], &json!({}), 0, Some(1))
        .unwrap();
    assert_eq!(results[0]["_id"], "a");
}

#[test]
fn l2_and_dot_indexes_use_their_metric() {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let c = store.collection("vectors").unwrap();
    c.insert_one(json!({"_id": "a", "embedding": [1.0, 0.0]}))
        .unwrap();
    c.insert_one(json!({"_id": "b", "embedding": [0.0, 1.0]}))
        .unwrap();
    c.create_vector_index("embedding", 2, VectorMetric::L2)
        .unwrap();
    let results = c
        .vector_search("embedding", &[1.0, 0.0], &json!({}), 0, Some(2))
        .unwrap();
    assert_eq!(results[0]["_id"], "a");
}
