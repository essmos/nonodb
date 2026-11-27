use std::time::Instant;

use nonodb_core::{Store, TimeSeriesGranularity, TimeSeriesSettings};
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
    let settings = TimeSeriesSettings {
        time_field: "ts".into(),
        meta_field: Some("symbol".into()),
        granularity: TimeSeriesGranularity::Minutes,
        ..TimeSeriesSettings::default()
    };
    let collection = store
        .create_timeseries_collection("window_group_bench", &settings)
        .unwrap();
    for index in 0..10_000 {
        let base = (index as f64 + 1.0) / 7.0;
        collection
            .insert_one(json!({
                "_id": format!("d{index}"),
                "ts": {"$date": {"$numberLong": (1_700_000_000_000i64 + index * 60_000).to_string()}},
                "symbol": "BTC/USD",
                "a": base,
                "b": base * 2.0,
                "c": base * 3.0,
                "d": base * 4.0
            }))
            .unwrap();
    }
    let pipeline = vec![json!({
        "$setWindowFields": {
            "sortBy": {"ts": 1},
            "output": {
                "a": {"$sum": "$a", "window": {"documents": ["unbounded", "current"]}},
                "b": {"$sum": "$b", "window": {"documents": ["unbounded", "current"]}},
                "c": {"$sum": "$c", "window": {"documents": ["unbounded", "current"]}},
                "d": {"$sum": "$d", "window": {"documents": ["unbounded", "current"]}}
            }
        }
    })];

    std::env::remove_var("NONODB_WINDOW_FAST_FLOAT");
    std::env::set_var("NONODB_WINDOW_SEQUENTIAL", "1");
    let exact_sequential = run(&collection, &pipeline);

    std::env::remove_var("NONODB_WINDOW_SEQUENTIAL");
    let exact_grouped = run(&collection, &pipeline);

    println!(
        "exact sequential outputs: {exact_sequential:.3} ms\n\
         exact grouped outputs:    {exact_grouped:.3} ms\n\
         speedup:                  {:.2}x",
        exact_sequential / exact_grouped
    );
}
