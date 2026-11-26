use std::time::Instant;

use nonodb_core::Store;
use serde_json::{json, Value};
use tempfile::TempDir;

fn median(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(|left, right| left.partial_cmp(right).unwrap());
    samples[samples.len() / 2]
}

fn run(collection: &nonodb_core::Collection<'_>, pipeline: &[Value]) -> f64 {
    let mut samples = Vec::new();
    for _ in 0..3 {
        let start = Instant::now();
        let output = collection.aggregate(pipeline).unwrap();
        assert_eq!(output.len(), 10_000);
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    median(samples)
}

fn main() {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let collection = store.collection("window_group_bench").unwrap();
    for index in 0..10_000 {
        collection
            .insert_one(json!({
                "_id": format!("d{index}"),
                "i": index,
                "amount": (index as f64 + 1.0) / 7.0
            }))
            .unwrap();
    }
    let pipeline = vec![json!({
        "$setWindowFields": {
            "sortBy": {"i": 1},
            "output": {
                "a": {"$avg": "$amount", "window": {"documents": [-599, 0]}},
                "b": {"$avg": "$amount", "window": {"documents": [-599, 0]}},
                "c": {"$avg": "$amount", "window": {"documents": [-599, 0]}},
                "d": {"$avg": "$amount", "window": {"documents": [-599, 0]}}
            }
        }
    })];

    std::env::remove_var("NONODB_WINDOW_FAST_FLOAT");
    std::env::set_var("NONODB_WINDOW_SEQUENTIAL", "1");
    let exact_sequential = run(&collection, &pipeline);

    std::env::remove_var("NONODB_WINDOW_SEQUENTIAL");
    std::env::set_var("NONODB_WINDOW_FAST_FLOAT", "1");
    let fast_grouped = run(&collection, &pipeline);

    std::env::remove_var("NONODB_WINDOW_FAST_FLOAT");
    println!(
        "exact sequential outputs: {exact_sequential:.3} ms\n\
         fast grouped outputs:    {fast_grouped:.3} ms\n\
         speedup:                 {:.2}x",
        exact_sequential / fast_grouped
    );
}
