//! Transactional failpoint rollback drills for the columnar profile.

use nonodb_core::{Store, VectorMetric};
use serde_json::json;

const FAILPOINT: &str = "NONODB_COLUMNAR_FAILPOINT";

fn with_failpoint<T>(name: &str, run: impl FnOnce() -> T) -> T {
    std::env::set_var(FAILPOINT, name);
    let out = run();
    std::env::remove_var(FAILPOINT);
    out
}

fn columnar<'a>(store: &'a Store, name: &'a str) -> nonodb_core::Collection<'a> {
    store
        .create_columnar_collection(name, &nonodb_core::columnar::Settings::default())
        .unwrap()
}

#[test]
fn columnar_transactional_failpoints_roll_back_consistently() {
    for failpoint in [
        "insert_after_row",
        "insert_after_id",
        "insert_after_index",
        "insert_after_ttl",
        "insert_after_history",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let c = columnar(&store, "docs");
        c.create_index("n", true).unwrap();
        c.insert_one(json!({"_id": "seed", "n": 1})).unwrap();
        let error = with_failpoint(failpoint, || {
            c.insert_one(json!({"_id": "x", "n": 2})).unwrap_err()
        });
        assert!(
            error.to_string().contains(failpoint),
            "{failpoint}: {error}"
        );
        assert_eq!(c.count(&json!({})).unwrap(), 1, "{failpoint}");
        assert!(c.find_by_key("x").unwrap().is_none(), "{failpoint}");
        assert_eq!(
            c.lookup("n", &[json!(2)], None).unwrap()[0],
            None,
            "{failpoint} index"
        );
    }

    for failpoint in [
        "update_after_write",
        "update_after_index",
        "update_after_ttl",
        "update_after_history",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let c = columnar(&store, "docs");
        c.create_index("n", true).unwrap();
        c.insert_one(json!({"_id": "seed", "n": 1})).unwrap();
        let error = with_failpoint(failpoint, || {
            c.update(&json!({"_id": "seed"}), &json!({"$set": {"n": 2}}), false)
                .unwrap_err()
        });
        assert!(
            error.to_string().contains(failpoint),
            "{failpoint}: {error}"
        );
        assert_eq!(
            c.find_by_key("seed").unwrap().unwrap()["n"],
            1,
            "{failpoint}"
        );
        assert_eq!(
            c.lookup("n", &[json!(1)], None).unwrap()[0]
                .as_ref()
                .unwrap()["_id"],
            "seed",
            "{failpoint} index"
        );
    }

    for failpoint in [
        "delete_after_delete",
        "delete_after_id",
        "delete_after_index",
        "delete_after_ttl",
        "delete_after_history",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let c = columnar(&store, "docs");
        c.create_index("n", true).unwrap();
        c.insert_one(json!({"_id": "seed", "n": 1})).unwrap();
        let error = with_failpoint(failpoint, || {
            c.delete(&json!({"_id": "seed"}), false).unwrap_err()
        });
        assert!(
            error.to_string().contains(failpoint),
            "{failpoint}: {error}"
        );
        assert!(c.find_by_key("seed").unwrap().is_some(), "{failpoint}");
        assert_eq!(
            c.lookup("n", &[json!(1)], None).unwrap()[0]
                .as_ref()
                .unwrap()["_id"],
            "seed",
            "{failpoint} index"
        );
    }

    for failpoint in ["seal_after_bucket", "seal_after_pointer"] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let c = columnar(&store, "docs");
        c.insert_one(json!({"_id": "seed", "n": 1})).unwrap();
        let error = with_failpoint(failpoint, || store.seal_columnar("docs").unwrap_err());
        assert!(
            error.to_string().contains(failpoint),
            "{failpoint}: {error}"
        );
        assert_eq!(c.count(&json!({})).unwrap(), 1, "{failpoint}");
    }

    for failpoint in [
        "compact_before_clear",
        "compact_after_clear",
        "compact_after_rebuild",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let c = columnar(&store, "docs");
        c.create_index("n", true).unwrap();
        c.create_vector_index("vec", 2, VectorMetric::Cosine)
            .unwrap();
        c.insert_one(json!({"_id": "seed", "n": 1, "vec": [1.0, 0.0]}))
            .unwrap();
        store.seal_columnar("docs").unwrap();
        c.update(&json!({"_id": "seed"}), &json!({"$set": {"n": 2}}), false)
            .unwrap();
        let error = with_failpoint(failpoint, || store.compact_columnar("docs").unwrap_err());
        assert!(
            error.to_string().contains(failpoint),
            "{failpoint}: {error}"
        );
        assert_eq!(c.count(&json!({})).unwrap(), 1, "{failpoint}");
        assert_eq!(
            c.find_by_key("seed").unwrap().unwrap()["n"],
            2,
            "{failpoint}"
        );
        assert_eq!(
            c.lookup("n", &[json!(2)], None).unwrap()[0]
                .as_ref()
                .unwrap()["_id"],
            "seed",
            "{failpoint} index"
        );
        assert_eq!(
            c.vector_search("vec", &[1.0, 0.0], &json!({}), 0, Some(1))
                .unwrap()[0]["_id"],
            "seed",
            "{failpoint} vector"
        );
    }
}
