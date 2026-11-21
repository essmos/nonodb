use nonodb_core::{SearchEngine, SearchIndexDef, SearchKind, Store, VectorMetric};
use serde_json::json;
use tempfile::TempDir;

#[test]
fn rebuilds_missing_text_fuzzy_and_vector_sidecars() {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let c = store.collection("docs").unwrap();
    for doc in [
        json!({"_id": "a", "body": "the quick brown fox", "embedding": [1.0, 0.0]}),
        json!({"_id": "b", "body": "quick brown dogs", "embedding": [0.9, 0.1]}),
        json!({"_id": "c", "body": "slow turtle", "embedding": [0.0, 1.0]}),
    ] {
        c.insert_one(doc).unwrap();
    }
    let mut text = SearchIndexDef::single(SearchKind::Text, "body");
    text.engine = SearchEngine::Segment;
    c.create_search_index(&text).unwrap();
    let mut fuzzy = SearchIndexDef::single(SearchKind::Fuzzy, "body");
    fuzzy.engine = SearchEngine::Segment;
    c.create_search_index(&fuzzy).unwrap();
    c.create_vector_index("embedding", 2, VectorMetric::Cosine)
        .unwrap();
    c.build_vector_ann_index("embedding", 2).unwrap();

    for base in [dir.path().to_path_buf(), dir.path().join("default")] {
        for name in ["search", "vector-ann"] {
            let path = base.join(name);
            if path.exists() {
                std::fs::remove_dir_all(path).unwrap();
            }
        }
    }

    assert_eq!(c.rebuild_missing_search_sidecars().unwrap(), 3);
    let rtxn = store.read_txn().unwrap();
    let report = c.verify_in(&rtxn).unwrap();
    rtxn.commit().unwrap();
    assert_eq!(report.documents, 3);
    assert_eq!(report.text_indexes, vec!["body"]);
    assert_eq!(report.fuzzy_indexes, vec!["body"]);
    assert_eq!(report.vector_indexes, vec!["embedding"]);
    assert_eq!(
        c.search("body", "quick", &json!({}), 0, Some(10))
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        c.fuzzy_search("body", "quik", 1, &json!({}), Some(10))
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        c.vector_search("embedding", &[1.0, 0.0], &json!({}), 0, Some(3))
            .unwrap()
            .len(),
        3
    );
    assert_eq!(c.rebuild_missing_search_sidecars().unwrap(), 0);
}
