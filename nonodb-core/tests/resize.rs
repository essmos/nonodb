use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use nonodb_core::{FindQuery, Store, PAGE_SIZE};
use serde_json::json;
use tempfile::tempdir;

#[test]
fn online_resize_grows_map_and_preserves_data() {
    let dir = tempdir().unwrap();
    let store = Store::open_with_map_size(dir.path(), PAGE_SIZE * 64).unwrap();
    let coll = store.collection("items").unwrap();
    for i in 0..20 {
        coll.insert_one(json!({"_id": i.to_string(), "value": i}))
            .unwrap();
    }

    let old = store.map_size();
    let grown = old + PAGE_SIZE * 16;
    assert_eq!(store.resize_map_size(grown).unwrap(), old);
    assert_eq!(store.map_size(), grown);
    assert!(store.map_used_bytes() <= grown);

    for i in 20..40 {
        coll.insert_one(json!({"_id": i.to_string(), "value": i}))
            .unwrap();
    }
    assert_eq!(coll.count(&json!({})).unwrap(), 40);
    assert_eq!(
        coll.find(&FindQuery {
            filter: json!({"_id": "39"}),
            ..Default::default()
        })
        .unwrap()
        .len(),
        1
    );
}

#[test]
fn map_full_can_be_recovered_by_online_growth() {
    let dir = tempdir().unwrap();
    let store = Store::open_with_map_size(dir.path(), PAGE_SIZE * 64).unwrap();
    let coll = store.collection("big").unwrap();
    let payload = "x".repeat(8 * 1024);

    let mut failed = false;
    for i in 0..500 {
        match coll.insert_one(json!({"_id": i.to_string(), "payload": payload})) {
            Ok(_) => {}
            Err(_) => {
                failed = true;
                break;
            }
        }
    }
    assert!(failed, "small map should fill before 500 documents");

    let old = store.map_size();
    store.resize_map_size(old * 2).unwrap();

    coll.insert_one(json!({"_id": "after-resize", "payload": "ok"}))
        .unwrap();
    assert_eq!(
        coll.find(&FindQuery {
            filter: json!({"_id": "after-resize"}),
            ..Default::default()
        })
        .unwrap()
        .len(),
        1
    );
}

#[test]
fn resize_waits_for_active_transaction() {
    let dir = tempdir().unwrap();
    let store = Arc::new(Store::open_with_map_size(dir.path(), PAGE_SIZE * 64).unwrap());
    store
        .collection("items")
        .unwrap()
        .insert_one(json!({"a": 1}))
        .unwrap();

    let rtxn = store.read_txn().unwrap();
    let target = store.map_size() + PAGE_SIZE;
    let (tx, rx) = mpsc::channel();
    let worker_store = store.clone();
    std::thread::spawn(move || {
        let result = worker_store.resize_map_size(target);
        tx.send(result).unwrap();
    });

    // The resize must remain queued while the read transaction is alive.
    std::thread::sleep(Duration::from_millis(100));
    assert!(rx.try_recv().is_err());
    rtxn.commit().unwrap();

    let old = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("resize did not resume after transaction commit")
        .unwrap();
    assert_eq!(store.map_size(), old + PAGE_SIZE);
}

#[test]
fn resize_rejects_invalid_sizes() {
    let dir = tempdir().unwrap();
    let store = Store::open_with_map_size(dir.path(), PAGE_SIZE * 64).unwrap();
    let old = store.map_size();

    assert!(store.resize_map_size(PAGE_SIZE - 1).is_err());
    assert!(store.resize_map_size(old - PAGE_SIZE).is_err());
    assert!(store.resize_map_size(old + 1).is_err());
    assert_eq!(store.resize_map_size(old).unwrap(), old);
    assert_eq!(store.map_size(), old);
}

#[test]
fn concurrent_io_survives_repeated_resizes() {
    let dir = tempdir().unwrap();
    let store = Arc::new(Store::open_with_map_size(dir.path(), PAGE_SIZE * 256).unwrap());
    store
        .collection("items")
        .unwrap()
        .insert_one(json!({"seed": true}))
        .unwrap();

    let mut workers = Vec::new();
    for worker in 0..4 {
        let store = store.clone();
        workers.push(std::thread::spawn(move || {
            let coll = store.collection("items").unwrap();
            for i in 0..25 {
                coll.insert_one(json!({"worker": worker, "i": i})).unwrap();
                let _ = coll
                    .find(&FindQuery {
                        filter: json!({"worker": worker}),
                        ..Default::default()
                    })
                    .unwrap();
            }
        }));
    }

    let resizer_store = store.clone();
    let resizer = std::thread::spawn(move || {
        for _ in 0..12 {
            let target = resizer_store.map_size() + PAGE_SIZE;
            resizer_store.resize_map_size(target).unwrap();
            std::thread::yield_now();
        }
    });

    for worker in workers {
        worker.join().unwrap();
    }
    resizer.join().unwrap();

    let count = store
        .collection("items")
        .unwrap()
        .count(&json!({}))
        .unwrap();
    assert_eq!(count, 1 + 4 * 25);
}
