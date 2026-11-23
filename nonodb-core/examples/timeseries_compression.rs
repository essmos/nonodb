//! Compression report for time-series buckets.
//!
//! Generates a synthetic IoT-style dataset (fixed-rate samples, several series,
//! repeated enum strings, booleans, floating point and integer counters),
//! stores it in a real nonodb time-series collection and prints the encoded
//! size. It also prints a row-oriented zlib baseline (the shape a document
//! database with whole-row compression gets) for comparison.
//!
//! Run with:
//!
//! ```text
//! cargo run --release -p nonodb-core --example timeseries_compression
//! ```

use std::io::Write;
use std::time::Instant;

use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rows: usize = std::env::var("ROWS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(50_000);
    let series: usize = std::env::var("SERIES")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(10);
    let bucket_docs: usize = std::env::var("BUCKET_DOCS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(2_048);

    let dir = tempfile::tempdir()?;
    let store = nonodb_core::Store::open(dir.path())?;
    let settings = nonodb_core::timeseries::Settings {
        time_field: "ts".into(),
        meta_field: Some("device".into()),
        bucket_max_docs: bucket_docs,
        ..Default::default()
    };
    let collection = store.create_timeseries_collection("bench", &settings)?;

    let statuses = ["ok", "warn", "error", "offline"];
    let start_ts = 1_700_000_000_000i64;
    let mut raw_bytes = 0u64;
    let mut row_json = Vec::with_capacity(rows);
    let started = Instant::now();

    for idx in 0..rows {
        let series_idx = idx % series;
        let ts = start_ts + (idx as i64 / series as i64) * 1000;
        let doc = json!({
            "ts": {"$date": {"$numberLong": ts.to_string()}},
            "device": {"id": format!("sensor-{series_idx}"), "site": "plant-1"},
            "temperature": 21.0 + (idx % 97) as f64 * 0.01,
            "humidity": 40.0 + (idx % 53) as f64 * 0.05,
            "pressure": 1013.0 + (idx % 31) as f64 * 0.1,
            "status": statuses[idx % statuses.len()],
            "level": (idx % 8) as i64,
            "ok": idx % 3 != 0,
        });
        raw_bytes += serde_json::to_vec(&doc)?.len() as u64;
        row_json.push(serde_json::to_vec(&doc)?);
        collection.insert_one(doc)?;
    }
    let elapsed = started.elapsed();

    store.seal_timeseries("bench")?;
    let stats = collection.timeseries_stats()?.unwrap_or_default();
    let stored = stats.encoded_bytes;

    // Row-oriented baseline: newline-delimited JSON compressed with zlib level 6
    // (what a document store with whole-row compression effectively achieves).
    let mut plain = Vec::new();
    let plain_bytes: u64 = row_json.iter().map(|row| row.len() as u64).sum();
    for row in &row_json {
        plain.extend_from_slice(row);
        plain.push(b'\n');
    }
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::new(6));
    encoder.write_all(&plain)?;
    let row_compressed = encoder.finish()?;

    println!("rows                 : {rows}");
    println!("series               : {series}");
    println!("bucket_max_docs      : {bucket_docs}");
    println!("raw JSON bytes       : {raw_bytes}");
    println!("stored columnar bytes: {stored}");
    println!("bytes/row (stored)   : {:.2}", stored as f64 / rows as f64);
    println!(
        "compression vs raw   : {:.2}x",
        raw_bytes as f64 / stored.max(1) as f64
    );
    println!("buckets / rows       : {} / {}", stats.buckets, stats.rows);
    println!("insert wall time     : {:?}", elapsed);
    println!(
        "row-oriented zlib    : {} bytes ({:.2} bytes/row, {:.2}x vs raw)",
        row_compressed.len(),
        row_compressed.len() as f64 / row_json.len() as f64,
        plain_bytes as f64 / row_compressed.len() as f64
    );
    println!(
        "columnar vs zlib rows : {:.2}x smaller",
        row_compressed.len() as f64 / stored.max(1) as f64
    );
    Ok(())
}
