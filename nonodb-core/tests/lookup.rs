use nonodb_core::{Store, MAX_LOOKUP_VALUES};
use serde_json::json;
use tempfile::TempDir;

fn fixture() -> (TempDir, Store) {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let c = store.collection("users").unwrap();
    for doc in [
        json!({
            "_id": "a",
            "email": "ada@example.com",
            "name": "Ada",
            "status": "active",
            "tenant": "acme",
            "extra": 1,
        }),
        json!({
            "_id": "b",
            "email": "grace@example.com",
            "name": "Grace",
            "status": "inactive",
            "tenant": "acme",
            "extra": 2,
        }),
        json!({
            "_id": "c",
            "email": "linus@example.com",
            "name": "Linus",
            "status": "active",
            "tenant": "acme",
            "extra": 3,
        }),
        json!({"_id": "missing", "name": "No email"}),
    ] {
        c.insert_one(doc).unwrap();
    }
    c.create_index_fields(&["email".into()], true, &["name".into(), "status".into()])
        .unwrap();
    (dir, store)
}

#[test]
fn lookup_by_unique_index_preserves_order_and_missing_values() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();

    let results = c
        .lookup(
            "email",
            &[
                json!("grace@example.com"),
                json!("absent@example.com"),
                json!("ada@example.com"),
            ],
            Some(&json!({"name": 1})),
        )
        .unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].as_ref().unwrap()["_id"], "b");
    assert_eq!(results[0].as_ref().unwrap()["name"], "Grace");
    assert!(results[0].as_ref().unwrap().get("status").is_none());
    assert!(results[1].is_none());
    assert_eq!(results[2].as_ref().unwrap()["_id"], "a");
}

#[test]
fn lookup_by_id_is_ordered_and_supports_duplicates() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();

    let results = c
        .lookup(
            "_id",
            &[json!("c"), json!("absent"), json!("a"), json!("c")],
            None,
        )
        .unwrap();

    let ids: Vec<_> = results
        .iter()
        .map(|doc| doc.as_ref().map(|doc| doc["_id"].as_str().unwrap()))
        .collect();
    assert_eq!(ids, vec![Some("c"), None, Some("a"), Some("c")]);
    assert_eq!(results[0].as_ref().unwrap()["name"], "Linus");
}

#[test]
fn lookup_falls_back_to_document_for_uncovered_projection() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();

    let results = c
        .lookup(
            "email",
            &[json!("ada@example.com")],
            Some(&json!({"name": 1, "extra": 1})),
        )
        .unwrap();
    let doc = results[0].as_ref().unwrap();
    assert_eq!(doc["_id"], "a");
    assert_eq!(doc["name"], "Ada");
    assert_eq!(doc["extra"], 1);
}

#[test]
fn lookup_requires_a_unique_single_field_index() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();

    let err = c
        .lookup("name", &[json!("Ada")], None)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("unique single-field or compound index"),
        "{err}"
    );

    c.create_index("status", false).unwrap();
    let err = c
        .lookup("status", &[json!("active")], None)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("unique single-field or compound index"),
        "{err}"
    );
}

#[test]
fn lookup_validates_input_size_and_empty_input() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();

    assert!(c.lookup("_id", &[], None).unwrap().is_empty());
    assert!(c.lookup("", &[json!("a")], None).is_err());

    let too_many = vec![json!("a"); MAX_LOOKUP_VALUES + 1];
    let err = c.lookup("_id", &too_many, None).unwrap_err().to_string();
    assert!(err.contains("at most"), "{err}");
}

#[test]
fn lookup_compound_unique_index_aligns_values_and_requires_exact_order() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();
    c.create_index_fields(&["tenant".into(), "email".into()], true, &["name".into()])
        .unwrap();

    let results = c
        .lookup_fields(
            &["tenant".to_string(), "email".to_string()],
            &[
                json!(["acme", "grace@example.com"]),
                json!(["acme", "missing@example.com"]),
                json!(["other", "ada@example.com"]),
                json!(["acme", "ada@example.com"]),
                json!("not-an-array"),
                json!(["acme"]),
            ],
            Some(&json!({"name": 1})),
        )
        .unwrap();

    assert_eq!(results[0].as_ref().unwrap()["_id"], "b");
    assert_eq!(results[0].as_ref().unwrap()["name"], "Grace");
    assert!(results[1].is_none());
    assert!(results[2].is_none());
    assert_eq!(results[3].as_ref().unwrap()["_id"], "a");
    assert!(results[4].is_none());
    assert!(results[5].is_none());

    let err = c
        .lookup_fields(
            &["email".to_string(), "tenant".to_string()],
            &[json!(["ada@example.com", "acme"])],
            None,
        )
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("unique single-field or compound index"),
        "{err}"
    );
}

#[test]
fn lookup_supports_projection_without_id_and_deduplicates_results() {
    let (_dir, store) = fixture();
    let c = store.collection("users").unwrap();

    let results = c
        .lookup(
            "email",
            &[
                json!("ada@example.com"),
                json!("ada@example.com"),
                json!("grace@example.com"),
            ],
            Some(&json!({"name": 1, "_id": 0})),
        )
        .unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Some(json!({"name": "Ada"})));
    assert_eq!(results[1], Some(json!({"name": "Ada"})));
    assert_eq!(results[2], Some(json!({"name": "Grace"})));
}

#[test]
fn lookup_rejects_oversized_result_payload() {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut wtxn = store.write_txn().unwrap();
    let c = store.collection_in(&mut wtxn, "big").unwrap();
    let blob = "x".repeat(1_040_000);
    for index in 0..65 {
        c.insert_one_txn(
            &mut wtxn,
            json!({"_id": format!("big{index}"), "blob": blob}),
        )
        .unwrap();
    }
    wtxn.commit().unwrap();
    let c = store.collection("big").unwrap();
    let ids: Vec<_> = (0..65).map(|index| json!(format!("big{index}"))).collect();
    let err = c.lookup("_id", &ids, None).unwrap_err().to_string();
    assert!(err.contains("result too large"), "{err}");
}
