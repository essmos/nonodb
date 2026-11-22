//! Measures the memory-mapped ANN reader without HTTP or daemon overhead.
//!
//! Usage:
//!   cargo run --release -p nonodb-core --example ann_bench -- FILE [rounds] [nprobe]

use std::path::PathBuf;
use std::time::Instant;

use nonodb_core::vector_ann::AnnReader;

fn next_u64(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = PathBuf::from(std::env::args().nth(1).expect("ANN file path"));
    let rounds = std::env::args()
        .nth(2)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100);
    let nprobe = std::env::args()
        .nth(3)
        .and_then(|value| value.parse::<usize>().ok());
    let data_queries = std::env::args().nth(4).is_some();
    let reader = AnnReader::open(&path)?;
    let stats = reader.stats();
    println!(
        "entries={} clusters={} default_nprobe={} dims={} bytes={}",
        stats.entries,
        stats.clusters,
        stats.nprobe,
        reader.dimensions(),
        stats.bytes
    );
    let nprobe = nprobe.unwrap_or(stats.nprobe);
    let dims = reader.dimensions();
    let mut state = 0x9e3779b97f4a7c15u64;
    let mut latencies = Vec::with_capacity(rounds);
    let mut rows = 0usize;
    for round in 0..rounds + 3 {
        let mut query = if data_queries {
            (0..dims)
                .map(|j| ((round * 31 + j * 17) % 1000) as f32 / 1000.0)
                .collect::<Vec<_>>()
        } else {
            (0..dims)
                .map(|_| {
                    let value = next_u64(&mut state);
                    ((value >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
                })
                .collect::<Vec<_>>()
        };
        let norm = query.iter().map(|value| value * value).sum::<f32>().sqrt();
        if norm > f32::EPSILON {
            for value in &mut query {
                *value /= norm;
            }
        }
        let started = Instant::now();
        let result = reader.search_with_nprobe(&query, 10, nprobe)?;
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        rows = result.len();
        if round >= 3 {
            latencies.push(elapsed);
        }
    }
    latencies.sort_by(f64::total_cmp);
    let pct = |q: f64| latencies[((latencies.len() - 1) as f64 * q) as usize];
    let mean = latencies.iter().sum::<f64>() / latencies.len() as f64;
    println!(
        "nprobe={nprobe} rows={rows} p50={:.2}ms p95={:.2}ms p99={:.2}ms mean={:.2}ms qps={:.1}",
        pct(0.50),
        pct(0.95),
        pct(0.99),
        mean,
        1000.0 / mean
    );
    Ok(())
}
