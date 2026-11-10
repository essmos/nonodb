use nonodb_core::heed::types::Bytes;
use nonodb_core::{index, CollectionProfile, FindQuery, Store, META_DB};
use serde_json::json;
use tempfile::TempDir;

fn store() -> (TempDir, Store) {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    (dir, store)
}

fn raw_put(store: &Store, coll: &str, id: &str, doc: serde_json::Value) {
    let mut wtxn = store.write_txn().unwrap();
    let db = store
        .env()
        .create_database::<Bytes, Bytes>(&mut wtxn, Some(coll))
        .unwrap();
    db.put(
        &mut wtxn,
        id.as_bytes(),
        serde_json::to_vec(&doc).unwrap().as_slice(),
    )
    .unwrap();
    wtxn.commit().unwrap();
}

fn find(c: &nonodb_core::Collection<'_>, filter: serde_json::Value) -> Vec<serde_json::Value> {
    c.find(&FindQuery {
        filter,
        ..Default::default()
    })
    .unwrap()
}

#[test]
fn inserts_store_canonical_utc_dates() {
    let (_dir, store) = store();
    let c = store.collection("events").unwrap();
    c.insert_one(json!({
        "_id": "a",
        "at": {"$date": "2026-01-01T01:00:00+01:00"}
    }))
    .unwrap();

    let docs = find(&c, json!({"_id": "a"}));
    assert_eq!(
        docs[0]["at"],
        json!({"$date": {"$numberLong": "1767225600000"}})
    );
}

#[test]
fn ranges_compare_instants_not_strings() {
    let (_dir, store) = store();
    let c = store.collection("events").unwrap();
    c.insert_one(json!({"_id": "a", "at": {"$date": "2026-01-01T00:00:00Z"}}))
        .unwrap();
    c.insert_one(json!({"_id": "b", "at": {"$date": "2026-01-01T02:00:00+01:00"}}))
        .unwrap();
    c.insert_one(json!({"_id": "c", "at": {"$date": "2026-01-02T00:00:00Z"}}))
        .unwrap();

    let docs = find(
        &c,
        json!({"at": {"$gte": {"$date": "2026-01-01T01:00:00+01:00"}}}),
    );
    let ids: Vec<&str> = docs.iter().filter_map(|d| d["_id"].as_str()).collect();
    assert_eq!(ids, vec!["a", "b", "c"]);

    let docs = find(
        &c,
        json!({"at": {"$gt": {"$date": "2026-01-01T01:00:00+01:00"}}}),
    );
    let ids: Vec<&str> = docs.iter().filter_map(|d| d["_id"].as_str()).collect();
    assert_eq!(ids, vec!["b", "c"]);

    let docs = find(
        &c,
        json!({"at": {"$lt": {"$date": "2026-01-01T02:00:00+01:00"}}}),
    );
    let ids: Vec<&str> = docs.iter().filter_map(|d| d["_id"].as_str()).collect();
    assert_eq!(ids, vec!["a"]);
}

#[test]
fn date_queries_do_not_match_strings() {
    let (_dir, store) = store();
    let c = store.collection("events").unwrap();
    c.insert_one(json!({"_id": "string", "at": "2026-01-01T00:00:00Z"}))
        .unwrap();
    c.insert_one(json!({"_id": "date", "at": {"$date": "2026-01-01T00:00:00Z"}}))
        .unwrap();

    let docs = find(
        &c,
        json!({"at": {"$eq": {"$date": "2026-01-01T00:00:00Z"}}}),
    );
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["_id"], "date");
}

#[test]
fn range_index_and_count_work_on_dates() {
    let (_dir, store) = store();
    let c = store.collection("events").unwrap();
    c.create_index("at", false).unwrap();
    for i in 0..5 {
        c.insert_one(json!({
            "_id": format!("e{i}"),
            "at": {"$date": {"$numberLong": (1_767_225_600_000i64 + i * 1000).to_string()}}
        }))
        .unwrap();
    }

    let filter = json!({
        "at": {
            "$gte": {"$date": "2026-01-01T01:00:00+01:00"},
            "$lt": {"$date": {"$numberLong": "1767225603000"}}
        }
    });
    let docs = find(&c, filter.clone());
    let ids: Vec<&str> = docs.iter().filter_map(|d| d["_id"].as_str()).collect();
    assert_eq!(ids, vec!["e0", "e1", "e2"]);
    assert_eq!(c.count(&filter).unwrap(), 3);
}

#[test]
fn sorting_by_date_is_chronological() {
    let (_dir, store) = store();
    let c = store.collection("events").unwrap();
    c.create_index("at", false).unwrap();
    c.insert_one(json!({"_id": "late", "at": {"$date": "2026-01-01T00:00:00Z"}}))
        .unwrap();
    c.insert_one(json!({"_id": "early", "at": {"$date": "2025-12-31T23:00:00Z"}}))
        .unwrap();

    let docs = c
        .find(&FindQuery {
            sort: vec![("at".to_string(), 1)],
            ..Default::default()
        })
        .unwrap();
    let ids: Vec<&str> = docs.iter().filter_map(|d| d["_id"].as_str()).collect();
    assert_eq!(ids, vec!["early", "late"]);
}

#[test]
fn unique_date_index_uses_the_instant() {
    let (_dir, store) = store();
    let c = store.collection("events").unwrap();
    c.create_index("at", true).unwrap();
    c.insert_one(json!({"_id": "a", "at": {"$date": "2026-01-01T00:00:00Z"}}))
        .unwrap();
    let err = c
        .insert_one(json!({"_id": "b", "at": {"$date": "2026-01-01T01:00:00+01:00"}}))
        .unwrap_err();
    assert!(matches!(err, nonodb_core::Error::DuplicateKey(_)));
}

#[test]
fn updates_normalize_dates_and_invalid_dates_are_rejected() {
    let (_dir, store) = store();
    let c = store.collection("events").unwrap();
    c.insert_one(json!({"_id": "a"})).unwrap();
    c.update(
        &json!({"_id": "a"}),
        &json!({"$set": {"at": {"$date": "2026-01-01T01:00:00+01:00"}}}),
        false,
    )
    .unwrap();
    let docs = find(&c, json!({"_id": "a"}));
    assert_eq!(
        docs[0]["at"],
        json!({"$date": {"$numberLong": "1767225600000"}})
    );

    let err = c
        .insert_one(json!({"_id": "bad", "at": {"$date": "2026-01-01T00:00:00"}}))
        .unwrap_err();
    assert!(matches!(err, nonodb_core::Error::InvalidDate(_)));
    assert!(find(&c, json!({"_id": "bad"})).is_empty());
}

#[test]
fn aggregation_match_and_sort_use_date_order() {
    let (_dir, store) = store();
    let c = store.collection("events").unwrap();
    c.insert_one(json!({"_id": "a", "at": {"$date": "2026-01-02T00:00:00Z"}}))
        .unwrap();
    c.insert_one(json!({"_id": "b", "at": {"$date": "2026-01-01T01:00:00+01:00"}}))
        .unwrap();

    let out = c
        .aggregate(&[
            json!({"$match": {"at": {"$gte": {"$date": "2026-01-01T00:00:00Z"}}}}),
            json!({"$sort": {"at": 1}}),
        ])
        .unwrap();
    let ids: Vec<&str> = out.iter().filter_map(|d| d["_id"].as_str()).collect();
    assert_eq!(ids, vec!["b", "a"]);
}

#[test]
fn compound_index_uses_date_range_after_equality() {
    let (_dir, store) = store();
    let c = store.collection("events").unwrap();
    c.create_index_fields(&["tenant".into(), "at".into()], false, &[])
        .unwrap();
    for (id, tenant, at) in [
        ("a", "acme", "2026-01-01T00:00:00Z"),
        ("b", "acme", "2026-01-02T00:00:00Z"),
        ("c", "other", "2026-01-02T00:00:00Z"),
    ] {
        c.insert_one(json!({
            "_id": id,
            "tenant": tenant,
            "at": {"$date": at}
        }))
        .unwrap();
    }

    let filter = json!({
        "tenant": "acme",
        "at": {"$gte": {"$date": "2026-01-02T00:00:00Z"}}
    });
    let docs = find(&c, filter.clone());
    let ids: Vec<&str> = docs.iter().filter_map(|d| d["_id"].as_str()).collect();
    assert_eq!(ids, vec!["b"]);
    assert_eq!(c.count(&filter).unwrap(), 1);
}

#[test]
fn plain_date_equality_uses_index_and_matches_across_offsets() {
    let (_dir, store) = store();
    let c = store.collection("events").unwrap();
    c.create_index("at", false).unwrap();
    c.insert_one(json!({"_id": "a", "at": {"$date": "2026-01-01T00:00:00Z"}}))
        .unwrap();
    c.insert_one(json!({"_id": "b", "at": {"$date": "2026-01-01T01:00:00+01:00"}}))
        .unwrap();

    let filter = json!({"at": {"$date": "2025-12-31T19:00:00-05:00"}});
    let docs = find(&c, filter.clone());
    assert_eq!(docs.len(), 2);
    assert_eq!(c.count(&filter).unwrap(), 2);
}

#[test]
fn legacy_documents_recognize_extended_json_not_iso_strings() {
    let (_dir, store) = store();
    store
        .create_collection("legacy", CollectionProfile::Standard)
        .unwrap();
    raw_put(
        &store,
        "legacy",
        "string",
        json!({"_id": "string", "at": "2026-01-01T00:00:00Z"}),
    );
    raw_put(
        &store,
        "legacy",
        "relaxed",
        json!({"_id": "relaxed", "at": {"$date": "2026-01-01T01:00:00+01:00"}}),
    );
    raw_put(
        &store,
        "legacy",
        "canonical",
        json!({"_id": "canonical", "at": {"$date": {"$numberLong": "1767225600000"}}}),
    );
    raw_put(
        &store,
        "legacy",
        "invalid",
        json!({"_id": "invalid", "at": {"$date": "not-a-date"}}),
    );

    let c = store.collection("legacy").unwrap();
    let date_filter = json!({"at": {"$gte": {"$date": "2026-01-01T00:00:00Z"}}});
    let docs = find(&c, date_filter.clone());
    let mut ids: Vec<String> = docs
        .iter()
        .filter_map(|d| d["_id"].as_str().map(str::to_owned))
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["canonical", "relaxed"]);
    assert_eq!(c.count(&date_filter).unwrap(), 2);

    // ISO strings stay strings and do not match date values.
    let docs = find(&c, json!({"at": "2026-01-01T00:00:00Z"}));
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["_id"], "string");

    // A new index backfills legacy Extended JSON dates correctly.
    c.create_index("at", false).unwrap();
    let mut ids: Vec<String> = find(&c, date_filter.clone())
        .iter()
        .filter_map(|d| d["_id"].as_str().map(str::to_owned))
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["canonical", "relaxed"]);
    assert_eq!(c.count(&date_filter).unwrap(), 2);

    // Reads do not rewrite legacy values; the next write canonicalizes them.
    let relaxed = find(&c, json!({"_id": "relaxed"}));
    assert_eq!(
        relaxed[0]["at"],
        json!({"$date": "2026-01-01T01:00:00+01:00"})
    );
    c.update(
        &json!({"_id": "relaxed"}),
        &json!({"$set": {"seen": true}}),
        false,
    )
    .unwrap();
    let relaxed = find(&c, json!({"_id": "relaxed"}));
    assert_eq!(
        relaxed[0]["at"],
        json!({"$date": {"$numberLong": "1767225600000"}})
    );
}

#[test]
fn pre_date_indexes_are_skipped_for_date_queries() {
    let (_dir, store) = store();
    let c = store.collection("legacy_index").unwrap();
    c.create_index("at", false).unwrap();
    c.insert_one(json!({"_id": "a", "at": {"$date": "2026-01-01T00:00:00Z"}}))
        .unwrap();
    c.insert_one(json!({"_id": "b", "at": {"$date": "2026-01-02T00:00:00Z"}}))
        .unwrap();

    // Simulate an index created before date support: format 0 and no date keys.
    let mut wtxn = store.write_txn().unwrap();
    let meta = store
        .env()
        .create_database::<Bytes, Bytes>(&mut wtxn, Some(META_DB))
        .unwrap();
    let key = b"index:legacy_index:at";
    let raw = meta.get(&wtxn, key).unwrap().unwrap();
    let mut def: serde_json::Value = serde_json::from_slice(raw).unwrap();
    def["format"] = json!(0);
    meta.put(&mut wtxn, key, serde_json::to_vec(&def).unwrap().as_slice())
        .unwrap();
    let idx = store
        .env()
        .create_database::<Bytes, Bytes>(&mut wtxn, Some(&index::db_name("legacy_index", "at")))
        .unwrap();
    idx.clear(&mut wtxn).unwrap();
    wtxn.commit().unwrap();

    let filter = json!({"at": {"$gt": {"$date": "2026-01-01T12:00:00Z"}}});
    let docs = find(&c, filter.clone());
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["_id"], "b");
    assert_eq!(c.count(&filter).unwrap(), 1);

    // Recreating the index upgrades the format and restores the indexed path.
    c.drop_index("at").unwrap();
    c.create_index("at", false).unwrap();
    assert!(c
        .list_index_defs()
        .unwrap()
        .iter()
        .any(|d| d.name() == "at" && d.current_format()));
}

#[test]
fn date_indexes_survive_reopen_and_track_updates_and_deletes() {
    let dir = TempDir::new().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        let c = store.collection("persisted_dates").unwrap();
        c.create_index("at", false).unwrap();
        c.insert_one(json!({"_id": "a", "at": {"$date": "2026-01-01T00:00:00Z"}}))
            .unwrap();
        c.insert_one(json!({"_id": "b", "at": {"$date": "2026-01-02T00:00:00Z"}}))
            .unwrap();
        c.insert_one(json!({"_id": "c", "at": {"$date": "2026-01-03T00:00:00Z"}}))
            .unwrap();
        c.update(
            &json!({"_id": "a"}),
            &json!({"$set": {"at": {"$date": "2026-01-04T00:00:00Z"}}}),
            false,
        )
        .unwrap();
        c.delete(&json!({"_id": "b"}), false).unwrap();
    }

    let store = Store::open(dir.path()).unwrap();
    let c = store.collection("persisted_dates").unwrap();
    let old_range = json!({"at": {
        "$gte": {"$date": "2026-01-01T00:00:00Z"},
        "$lt": {"$date": "2026-01-04T00:00:00Z"}
    }});
    assert_eq!(c.count(&old_range).unwrap(), 1);
    let docs = find(&c, old_range);
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["_id"], "c");

    let all = c
        .find(&FindQuery {
            sort: vec![("at".to_string(), -1)],
            ..Default::default()
        })
        .unwrap();
    let ids: Vec<&str> = all.iter().filter_map(|d| d["_id"].as_str()).collect();
    assert_eq!(ids, vec!["a", "c"]);
}

#[test]
fn nested_and_array_dates_are_normalized_and_queryable() {
    let (_dir, store) = store();
    let c = store.collection("nested_dates").unwrap();
    c.insert_one(json!({
        "_id": "a",
        "meta": {"created_at": {"$date": "2026-01-01T01:00:00+01:00"}},
        "timeline": [
            {"at": {"$date": "2025-12-31T00:00:00Z"}},
            {"at": {"$date": "2026-01-02T00:00:00Z"}}
        ],
        "dates": [
            {"$date": "2025-12-30T00:00:00Z"},
            {"$date": "2026-01-03T00:00:00Z"}
        ]
    }))
    .unwrap();

    let doc = find(&c, json!({"_id": "a"})).pop().unwrap();
    assert_eq!(
        doc["meta"]["created_at"],
        json!({"$date": {"$numberLong": "1767225600000"}})
    );
    assert_eq!(
        doc["timeline"][0]["at"],
        json!({"$date": {"$numberLong": "1767139200000"}})
    );

    let filter = json!({"meta.created_at": {"$gte": {"$date": "2026-01-01T00:00:00Z"}}});
    assert_eq!(c.count(&filter).unwrap(), 1);
    let filter = json!({"dates": {"$date": "2026-01-03T00:00:00Z"}});
    assert_eq!(c.count(&filter).unwrap(), 1);
}

#[test]
fn negative_and_fractional_dates_are_supported() {
    let (_dir, store) = store();
    let c = store.collection("old_dates").unwrap();
    c.insert_one(json!({"_id": "negative", "at": {"$date": "1969-12-31T23:59:59Z"}}))
        .unwrap();
    c.insert_one(json!({"_id": "fraction", "at": {"$date": "1970-01-01T00:00:00.1239Z"}}))
        .unwrap();

    let docs = c
        .find(&FindQuery {
            sort: vec![("at".to_string(), 1)],
            ..Default::default()
        })
        .unwrap();
    assert_eq!(docs[0]["_id"], "negative");
    assert_eq!(docs[0]["at"], json!({"$date": {"$numberLong": "-1000"}}));
    assert_eq!(docs[1]["at"], json!({"$date": {"$numberLong": "123"}}));
    assert_eq!(
        c.count(&json!({"at": {"$eq": {"$date": "1970-01-01T00:00:00.123Z"}}}))
            .unwrap(),
        1
    );
}
