//! Measures IVF recall@10 against a full sidecar scan.
//!
//! Usage:
//!   cargo run --release -p nonodb-core --example ann_recall -- FILE [queries] [nprobe]

use std::collections::HashSet;
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
    let queries = std::env::args()
        .nth(2)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10);
    let nprobe = std::env::args()
        .nth(3)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(2);
    let reader = AnnReader::open(&path)?;
    let stats = reader.stats();
    let dims = reader.dimensions();
    println!(
        "entries={} clusters={} dims={} nprobe={}",
        stats.entries, stats.clusters, dims, nprobe
    );
    let mut state = 0x9e3779b97f4a7c15u64;
    let mut total_hits = 0usize;
    let mut total_needed = 0usize;
    let mut approx_ms = Vec::with_capacity(queries);
    let mut exact_ms = Vec::with_capacity(queries);
    for query_index in 0..queries {
        let mut query = (0..dims)
            .map(|_| {
                let value = next_u64(&mut state);
                ((value >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
            })
            .collect::<Vec<_>>();
        let norm = query.iter().map(|value| value * value).sum::<f32>().sqrt();
        if norm > f32::EPSILON {
            for value in &mut query {
                *value /= norm;
            }
        }
        let started = Instant::now();
        let approximate = reader.search_with_nprobe(&query, 10, nprobe)?;
        let approx_elapsed = started.elapsed().as_secs_f64() * 1000.0;
        let started = Instant::now();
        let exact = reader.search_with_nprobe(&query, 10, stats.clusters)?;
        let exact_elapsed = started.elapsed().as_secs_f64() * 1000.0;
        let exact_ids = exact
            .into_iter()
            .map(|candidate| candidate.id)
            .collect::<HashSet<_>>();
        let approximate_ids = approximate
            .into_iter()
            .map(|candidate| candidate.id)
            .collect::<Vec<_>>();
        let hits = approximate_ids
            .iter()
            .filter(|id| exact_ids.contains(*id))
            .count();
        total_hits += hits;
        total_needed += exact_ids.len();
        println!(
            "query={query_index} hits={hits}/{} approx={approx_elapsed:.2}ms exact={exact_elapsed:.2}ms",
            exact_ids.len()
        );
        approx_ms.push(approx_elapsed);
        exact_ms.push(exact_elapsed);
    }
    approx_ms.sort_by(f64::total_cmp);
    exact_ms.sort_by(f64::total_cmp);
    let pct = |values: &[f64], q: f64| values[((values.len() - 1) as f64 * q) as usize];
    println!(
        "recall@10={:.4} approx_p50={:.2}ms approx_p95={:.2}ms exact_p50={:.2}ms",
        total_hits as f64 / total_needed.max(1) as f64,
        pct(&approx_ms, 0.5),
        pct(&approx_ms, 0.95),
        pct(&exact_ms, 0.5)
    );
    Ok(())
}
