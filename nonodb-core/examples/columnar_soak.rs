//! Bounded-memory soak harness for the columnar profile.
//!
//! Run with: `NONODB_SOAK_DOCS=10000000 cargo run --release -p nonodb-core --example columnar_soak`
//! Results are printed to stdout; they are not committed to the repository.

use nonodb_core::columnar::Settings;
use nonodb_core::Store;
use serde_json::{json, Value};

const BATCH: usize = 1_000;

fn doc(index: usize) -> Value {
    json!({
        "_id": format!("soak-{index:010}"),
        "ts": 1_700_000_000_000i64 + index as i64,
        "group": (["a", "b", "c", "d"][index % 4]),
        "value": (index as f64) / 7.0,
        "qty": index % 1000,
        "flag": index.is_multiple_of(2),
        "tags": [index % 7, (index + 1) % 7],
        "nested": {"a": index % 100, "b": format!("txt-{}", index % 500)},
    })
}

fn status_kib(prefix: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(value) = line.strip_prefix(prefix) {
            return value
                .split_whitespace()
                .next()
                .and_then(|value| value.parse().ok());
        }
    }
    None
}

fn peak_rss_kib() -> Option<u64> {
    status_kib("VmHWM:")
}

fn anonymous_kib() -> Option<u64> {
    status_kib("RssAnon:")
}

fn main() {
    let docs: usize = std::env::var("NONODB_SOAK_DOCS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000_000);
    let dir = match std::env::var_os("NONODB_SOAK_DIR") {
        Some(path) => {
            let path = std::path::PathBuf::from(path);
            std::fs::create_dir_all(&path).unwrap();
            tempfile::Builder::new()
                .prefix("nonodb-soak-")
                .tempdir_in(path)
                .unwrap()
        }
        None => tempfile::tempdir().unwrap(),
    };
    let store = Store::open(dir.path()).unwrap();
    let coll = store
        .create_columnar_collection(
            "soak",
            &Settings {
                bucket_max_docs: 512,
                ..Default::default()
            },
        )
        .unwrap();
    let started = std::time::Instant::now();
    let mut peak_anon_kib = 0u64;
    for start in (0..docs).step_by(BATCH) {
        let batch: Vec<Value> = (start..(start + BATCH).min(docs)).map(doc).collect();
        let mut wtxn = store.write_txn().unwrap();
        let tx_coll = store.collection_in(&mut wtxn, coll.name()).unwrap();
        tx_coll.insert_many_txn(&mut wtxn, &batch).unwrap();
        wtxn.commit().unwrap();
        let inserted = start + batch.len();
        peak_anon_kib = peak_anon_kib.max(anonymous_kib().unwrap_or(0));
        if inserted.is_multiple_of(1_000_000) {
            eprintln!(
                "progress inserted={inserted} elapsed_s={:.1} peak_rss_mib={} peak_anon_mib={}",
                started.elapsed().as_secs_f64(),
                peak_rss_kib().map_or(0, |kib| kib / 1024),
                peak_anon_kib / 1024,
            );
        }
    }
    store.seal_columnar("soak").unwrap();
    let stats = store.storage_stats().unwrap();
    let collection = &stats.collections[0];
    peak_anon_kib = peak_anon_kib.max(anonymous_kib().unwrap_or(0));
    println!(
        "docs={docs} elapsed_s={:.1} peak_rss_mib={} peak_anon_mib={} data_bytes={} index_bytes={} records={}",
        started.elapsed().as_secs_f64(),
        peak_rss_kib().map_or(0, |kib| kib / 1024),
        peak_anon_kib / 1024,
        collection.data_bytes,
        collection.index_bytes,
        collection.records,
    );
}
