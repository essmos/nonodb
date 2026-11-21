use nonodb_core::{SearchEngine, Store};
use serde_json::json;
use tempfile::TempDir;

fn ids(docs: &[serde_json::Value]) -> Vec<String> {
    let mut ids: Vec<_> = docs
        .iter()
        .map(|doc| doc["_id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    ids
}

#[test]
fn migrates_legacy_text_and_fuzzy_indexes_to_segments() {
    std::env::set_var("NONODB_SEGMENT_TEXT", "0");
    std::env::set_var("NONODB_SEGMENT_FUZZY", "0");

    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let c = store.collection("docs").unwrap();
    for doc in [
        json!({"_id": "a", "body": "the quick brown fox"}),
        json!({"_id": "b", "body": "quick brown dogs"}),
        json!({"_id": "c", "body": "slow turtle"}),
    ] {
        c.insert_one(doc).unwrap();
    }
    c.create_text_index("body").unwrap();
    c.create_fuzzy_index("body").unwrap();

    {
        let rtxn = store.read_txn().unwrap();
        let defs = c.search_index_defs_in(&rtxn).unwrap();
        assert!(defs.iter().all(|def| def.engine == SearchEngine::Lmdb));
    }
    assert_eq!(
        ids(&c.search("body", "quick", &json!({}), 0, Some(10)).unwrap()),
        ["a", "b"]
    );
    assert_eq!(
        ids(&c
            .fuzzy_search("body", "quik", 1, &json!({}), Some(10))
            .unwrap()),
        ["a", "b"]
    );

    assert_eq!(c.migrate_search_indexes_to_segment().unwrap(), 2);
    {
        let rtxn = store.read_txn().unwrap();
        let defs = c.search_index_defs_in(&rtxn).unwrap();
        assert!(defs.iter().all(|def| def.engine == SearchEngine::Segment));
    }
    assert_eq!(
        ids(&c.search("body", "quick", &json!({}), 0, Some(10)).unwrap()),
        ["a", "b"]
    );
    assert_eq!(
        ids(&c
            .fuzzy_search("body", "quik", 1, &json!({}), Some(10))
            .unwrap()),
        ["a", "b"]
    );

    c.update(
        &json!({"_id": "c"}),
        &json!({"$set": {"body": "quick turtle"}}),
        false,
    )
    .unwrap();
    c.delete(&json!({"_id": "a"}), false).unwrap();
    assert_eq!(
        ids(&c.search("body", "quick", &json!({}), 0, Some(10)).unwrap()),
        ["b", "c"]
    );
    assert_eq!(
        ids(&c
            .fuzzy_search("body", "quik", 1, &json!({}), Some(10))
            .unwrap()),
        ["b", "c"]
    );

    std::env::remove_var("NONODB_SEGMENT_TEXT");
    std::env::remove_var("NONODB_SEGMENT_FUZZY");
}
