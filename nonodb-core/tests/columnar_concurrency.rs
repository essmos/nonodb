//! Concurrent mixed workload for the columnar profile.

use std::sync::Arc;
use std::thread;

use nonodb_core::{CollectionProfile, FindQuery, Store};
use serde_json::json;

#[test]
fn concurrent_insert_update_delete_seal_compact_and_reads_are_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    store
        .create_columnar_collection(
            "docs",
            &nonodb_core::columnar::Settings {
                bucket_max_docs: 32,
                ..Default::default()
            },
        )
        .unwrap();

    thread::scope(|scope| {
        for writer in 0..4 {
            let store = Arc::clone(&store);
            scope.spawn(move || {
                let coll = store.collection("docs").unwrap();
                for index in 0..120 {
                    coll.insert_one(json!({
                        "_id": format!("w{writer}-{index:03}"),
                        "writer": writer,
                        "seq": index,
                        "value": index,
                        "tag": format!("{writer}:{index}"),
                    }))
                    .unwrap();
                }
                for index in (0..120).step_by(10) {
                    coll.delete(&json!({"_id": format!("w{writer}-{index:03}")}), false)
                        .unwrap();
                }
                for index in 0..120 {
                    if index % 10 == 0 {
                        continue;
                    }
                    coll.update(
                        &json!({"_id": format!("w{writer}-{index:03}")}),
                        &json!({"$inc": {"value": 1000}}),
                        false,
                    )
                    .unwrap();
                }
            });
        }

        {
            let store = Arc::clone(&store);
            scope.spawn(move || {
                for _ in 0..25 {
                    store.seal_columnar("docs").unwrap();
                    store.compact_columnar("docs").unwrap();
                }
            });
        }

        for _reader in 0..4 {
            let store = Arc::clone(&store);
            scope.spawn(move || {
                let coll = store.collection("docs").unwrap();
                for _ in 0..200 {
                    let rows = coll
                        .find(&FindQuery {
                            filter: json!({"writer": {"$gte": 0}}),
                            sort: vec![("_id".into(), 1)],
                            ..Default::default()
                        })
                        .unwrap();
                    for row in rows {
                        assert!(row["_id"].as_str().unwrap().starts_with('w'));
                    }
                    let _ = coll.count(&json!({"value": {"$gte": 0}})).unwrap();
                }
            });
        }
    });

    let coll = store.collection("docs").unwrap();
    assert_eq!(coll.count(&json!({})).unwrap(), 432);
    for writer in 0..4 {
        for index in 0..120 {
            let found = coll.find_by_key(&format!("w{writer}-{index:03}")).unwrap();
            if index % 10 == 0 {
                assert!(found.is_none(), "deleted w{writer}-{index}");
            } else {
                let doc = found.unwrap();
                assert_eq!(doc["value"], json!(index + 1000));
                assert_eq!(doc["tag"], json!(format!("{writer}:{index}")));
            }
        }
    }
    store.seal_columnar("docs").unwrap();
    assert_eq!(coll.count(&json!({})).unwrap(), 432);
    assert_eq!(
        store.collection_profile("docs").unwrap(),
        Some(CollectionProfile::Columnar)
    );
}
