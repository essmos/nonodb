use nonodb_core::Store;
use serde_json::{json, Value};
use tempfile::TempDir;

fn pipeline() -> Vec<Value> {
    vec![json!({
        "$setWindowFields": {
            "sortBy": {"i": 1},
            "output": {
                "sum": {"$sum": "$amount", "window": {"documents": [-9, 0]}},
                "avg": {"$avg": "$amount", "window": {"documents": [-9, 0]}}
            }
        }
    })]
}

fn amount_values(result: &[Value]) -> Vec<(f64, f64)> {
    result
        .iter()
        .map(|doc| (doc["sum"].as_f64().unwrap(), doc["avg"].as_f64().unwrap()))
        .collect()
}

#[test]
fn fast_float_is_opt_in_and_matches_within_tolerance() {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let collection = store.collection("fast_float").unwrap();
    let mut docs = Vec::new();
    for index in 0..80 {
        let amount = (index as f64 + 1.0) / 7.0;
        docs.push(json!({"_id": format!("d{index}"), "i": index, "amount": amount}));
    }
    for doc in &docs {
        collection.insert_one(doc.clone()).unwrap();
    }

    std::env::remove_var("NONODB_WINDOW_FAST_FLOAT");
    let exact = collection.aggregate(&pipeline()).unwrap();

    std::env::set_var("NONODB_WINDOW_FAST_FLOAT", "1");
    let fast = collection.aggregate(&pipeline()).unwrap();
    std::env::remove_var("NONODB_WINDOW_FAST_FLOAT");

    assert_eq!(exact.len(), docs.len());
    let exact_values = amount_values(&exact);
    let fast_values = amount_values(&fast);
    for ((exact_sum, exact_avg), (fast_sum, fast_avg)) in exact_values.into_iter().zip(fast_values)
    {
        assert!((exact_sum - fast_sum).abs() <= 1e-9 * exact_sum.abs().max(1.0));
        assert!((exact_avg - fast_avg).abs() <= 1e-9 * exact_avg.abs().max(1.0));
    }

    // The default path must stay bit-for-bit identical to direct sequential
    // accumulation; the fast path is isolated behind the environment flag.
    for doc in &exact {
        let position = doc["i"].as_i64().unwrap() as usize;
        let start = position.saturating_sub(9);
        let mut sum = 0.0;
        let mut count = 0u64;
        for value in &docs[start..=position] {
            sum += value["amount"].as_f64().unwrap();
            count += 1;
        }
        assert_eq!(doc["sum"].as_f64().unwrap(), sum);
        assert_eq!(doc["avg"].as_f64().unwrap(), sum / count as f64);
    }
}
