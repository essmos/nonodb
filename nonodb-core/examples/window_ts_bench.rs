use std::time::Instant;

use nonodb_core::{Store, TimeSeriesGranularity, TimeSeriesSettings};
use serde_json::{json, Value};
use tempfile::TempDir;

fn median(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(|left, right| left.partial_cmp(right).unwrap());
    samples[samples.len() / 2]
}

fn bench(collection: &nonodb_core::Collection<'_>, pipeline: &[Value]) -> f64 {
    let mut samples = Vec::new();
    for _ in 0..3 {
        let start = Instant::now();
        let output = collection.aggregate(pipeline).unwrap();
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
        assert!(!output.is_empty());
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
        .create_timeseries_collection("window_ts_bench", &settings)
        .unwrap();
    let base = 1_700_000_000_000i64;
    let minute = 60_000i64;
    let day = 86_400_000i64;
    for symbol in 0..8 {
        for index in 0..2_880i64 {
            let price = 100.0 + (index % 97) as f64 / 10.0;
            collection
                .insert_one(json!({
                    "ts": {"$date": {"$numberLong": (base + index * minute).to_string()}},
                    "symbol": format!("S{symbol}"),
                    "close": price,
                    "amount": price / 3.0,
                }))
                .unwrap();
        }
    }
    let end = base + 2 * day;
    let hour = 3_600_000i64;
    let one_day = json!({
        "$gte": {"$date": {"$numberLong": (end - day).to_string()}},
        "$lte": {"$date": {"$numberLong": end.to_string()}}
    });
    let rank = json!({"$setWindowFields": {
        "partitionBy": "$symbol",
        "sortBy": {"ts": 1},
        "output": {"rank": {"$rank": {}}}
    }});
    let range = json!({"$setWindowFields": {
        "partitionBy": "$symbol",
        "sortBy": {"ts": 1},
        "output": {"avg": {"$avg": "$close", "window": {"range": [-1, "current"], "unit": "hour"}}}
    }});
    let all_symbols = json!({
        "$match": {"ts": {
            "$gte": {"$date": {"$numberLong": (end - hour).to_string()}},
            "$lte": {"$date": {"$numberLong": end.to_string()}}
        }},
    });
    let all_window = json!({"$setWindowFields": {
        "partitionBy": "$symbol",
        "sortBy": {"ts": 1},
        "output": {
            "rank": {"$rank": {}},
            "ma60": {"$avg": "$close", "window": {"documents": [-59, 0]}}
        }
    }});
    let fast_pipeline = vec![
        json!({"$match": {"symbol": "S0", "ts": one_day}}),
        json!({"$setWindowFields": {
            "partitionBy": "$symbol",
            "sortBy": {"ts": 1},
            "output": {
                "sum1200": {"$sum": "$close", "window": {"documents": [-1199, 0]}},
                "avg1200": {"$avg": "$amount", "window": {"documents": [-1199, 0]}}
            }
        }}),
    ];
    std::env::remove_var("NONODB_WINDOW_FAST_FLOAT");
    let exact = bench(&collection, &fast_pipeline);
    std::env::set_var("NONODB_WINDOW_FAST_FLOAT", "1");
    let fast = bench(&collection, &fast_pipeline);
    std::env::remove_var("NONODB_WINDOW_FAST_FLOAT");
    println!(
        "rank_1d:       {:.3} ms\nrange_1h_1d:   {:.3} ms\nall_symbols_1h:{:.3} ms\nfast_float:    {:.3} ms exact -> {:.3} ms fast ({:.2}x)",
        bench(&collection, &[json!({"$match": {"symbol": "S0", "ts": one_day}}), rank]),
        bench(&collection, &[json!({"$match": {"symbol": "S0", "ts": one_day}}), range]),
        bench(&collection, &[all_symbols, all_window]),
        exact,
        fast,
        exact / fast,
    );
}
