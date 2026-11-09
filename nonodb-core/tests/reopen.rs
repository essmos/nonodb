use nonodb_core::Store;
use serde_json::json;

#[test]
fn reopen_and_read() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        let c = store.collection("w").unwrap();
        c.insert_one(json!({"a": 1})).unwrap();
        assert_eq!(c.count(&json!({})).unwrap(), 1);
    }
    eprintln!("--- reopened ---");
    let store = Store::open(dir.path()).unwrap();
    eprintln!("collections: {:?}", store.collections());
    let c = store.collection("w").unwrap();
    match c.count(&json!({})) {
        Ok(n) => eprintln!("count ok: {n}"),
        Err(e) => panic!("count failed: {e:?}"),
    }
}
