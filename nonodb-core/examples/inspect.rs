use nonodb_core::Store;
use serde_json::json;
use std::path::PathBuf;

fn main() {
    let path = PathBuf::from(std::env::args().nth(1).expect("usage: inspect <dir>"));
    let store = match Store::open(&path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("open failed: {e:?}");
            return;
        }
    };
    eprintln!("open ok");
    let collections = store.collections().unwrap_or_default();
    eprintln!("collections: {collections:?}");
    // Kolekcja z argumentu albo pierwsza z bazy (wygodne przy inspekcji kopii).
    let name = std::env::args()
        .nth(2)
        .or_else(|| collections.first().cloned())
        .unwrap_or_else(|| "w".to_string());
    let c = match store.collection(&name) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("collection `{name}` failed: {e:?}");
            return;
        }
    };
    eprintln!("collection `{name}` ok");
    eprintln!("count: {:?}", c.count(&json!({})));
    eprintln!("find: {:?}", c.find(&Default::default()).map(|v| v.len()));
}
