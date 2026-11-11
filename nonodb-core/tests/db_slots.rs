use nonodb_core::{FindQuery, Store};
use serde_json::json;
use tempfile::TempDir;

#[test]
fn index_create_drop_cycles_reuse_sub_database_slots() {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let c = store.collection("slots").unwrap();
    c.insert_one(json!({"_id": "a", "kind": "x", "n": 1, "title": "hello world"}))
        .unwrap();

    // More than the old 256-sub-database ceiling. Slots must be reused after drop.
    for i in 0..300 {
        let field = format!("f{i}");
        c.create_index(&field, false).unwrap();
        c.drop_index(&field).unwrap();
    }
    c.create_index("kind", false).unwrap();
    assert_eq!(
        c.find(&FindQuery {
            filter: json!({"kind": "x"}),
            ..Default::default()
        })
        .unwrap()
        .len(),
        1
    );
    assert!(c.drop_index("kind").unwrap());

    // Reusing the same slot must not resurrect stale index entries.
    c.update(&json!({"_id": "a"}), &json!({"$set": {"kind": "y"}}), false)
        .unwrap();
    c.create_index("kind", false).unwrap();
    assert_eq!(
        c.find(&FindQuery {
            filter: json!({"kind": "x"}),
            ..Default::default()
        })
        .unwrap()
        .len(),
        0
    );
    assert_eq!(
        c.find(&FindQuery {
            filter: json!({"kind": "y"}),
            ..Default::default()
        })
        .unwrap()
        .len(),
        1
    );
}

#[test]
fn text_and_fuzzy_create_drop_cycles_reuse_slots() {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let c = store.collection("search_slots").unwrap();
    c.insert_one(json!({"_id": "a", "title": "hello world", "body": "hello fuzzy"}))
        .unwrap();

    for i in 0..270 {
        let field = format!("t{i}");
        c.create_text_index(&field).unwrap();
        c.drop_text_index(&field).unwrap();
    }
    for i in 0..270 {
        let field = format!("z{i}");
        c.create_fuzzy_index(&field).unwrap();
        c.drop_fuzzy_index(&field).unwrap();
    }

    c.create_text_index("title").unwrap();
    assert_eq!(
        c.search("title", "hello", &json!({}), 0, Some(10))
            .unwrap()
            .len(),
        1
    );
    assert!(c.drop_text_index("title").unwrap());

    // Recreating a text index in a reused slot must not retain old postings.
    c.update(
        &json!({"_id": "a"}),
        &json!({"$set": {"title": "new text"}}),
        false,
    )
    .unwrap();
    c.create_text_index("title").unwrap();
    assert_eq!(
        c.search("title", "hello", &json!({}), 0, Some(10))
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        c.search("title", "new", &json!({}), 0, Some(10))
            .unwrap()
            .len(),
        1
    );
    c.drop_text_index("title").unwrap();

    c.create_fuzzy_index("body").unwrap();
    assert_eq!(
        c.fuzzy_search("body", "helo", 2, &json!({}), Some(10))
            .unwrap()
            .len(),
        1
    );
    assert!(c.drop_fuzzy_index("body").unwrap());
}

#[test]
fn slots_survive_reopen_and_can_be_reused() {
    let dir = TempDir::new().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        let c = store.collection("reopen_slots").unwrap();
        c.insert_one(json!({"_id": "a", "kind": "x", "title": "hello"}))
            .unwrap();
        c.create_index("kind", false).unwrap();
        c.drop_index("kind").unwrap();
        c.create_text_index("title").unwrap();
        c.drop_text_index("title").unwrap();
    }
    let store = Store::open(dir.path()).unwrap();
    let c = store.collection("reopen_slots").unwrap();
    c.create_index("kind", false).unwrap();
    assert_eq!(
        c.find_one(&json!({"kind": "x"})).unwrap().unwrap()["_id"],
        "a"
    );
    c.create_text_index("title").unwrap();
    assert_eq!(
        c.search("title", "hello", &json!({}), 0, Some(10))
            .unwrap()
            .len(),
        1
    );
}
