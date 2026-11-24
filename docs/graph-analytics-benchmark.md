# Graph analytics benchmark

Deterministic benchmark for the background/paged analytics work. Raw data and
environment metadata are in
[`graph-analytics-benchmark.json`](graph-analytics-benchmark.json).

## Method

- Release daemon, local ext4 temporary directory.
- Seed `42`; zero-padded node ids; sparse random graph with **1.0 edge per
  node**.
- Batches of 2,000 documents; adjacency is built by `set_graph` before edge
  ingestion, so edge writes include adjacency maintenance.
- Times are wall-clock milliseconds from the client, one run per operation.
- `pagerank` uses 5 iterations; `label_propagation` uses 5 synchronous
  iterations; `k_core` uses both directions. All algorithms use both directions
  where applicable and a 1 GiB per-run memory budget.
- Peak RSS is `VmHWM` sampled from `/proc`; it is a process high-water mark, so
  later operations inherit earlier peaks and the value is an upper bound, not an
  incremental measurement.

Reproduce:

```bash
cargo build --release -p nonodb-daemon
NONODB_DAEMON=target/release/nonodb-daemon \
  python3 scripts/graph_analytics_bench.py \
  --sizes 100000,500000,1000000 \
  --output docs/graph-analytics-benchmark.json
```

## Results

| Nodes | Edges | Ingest total | Degree | Degree distribution | PageRank (5 iter) | Label propagation (5 iter) | k-core | PageRank job |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100,000 | 100,000 | 2.58 s | 429 ms | 612 ms | 1.27 s | 1.12 s | 1.27 s | 1.36 s |
| 500,000 | 500,000 | 14.93 s | 2.11 s | 3.11 s | 8.74 s | 7.53 s | 8.62 s | 10.69 s |
| 1,000,000 | 1,000,000 | 30.45 s | 4.37 s | 8.02 s | 21.81 s | 16.77 s | 20.03 s | 25.45 s |

Ingest throughput:

| Nodes | Node ingest | Edge ingest + adjacency |
|---:|---:|---:|
| 100,000 | 198,278/s | 48,150/s |
| 500,000 | 204,055/s | 40,077/s |
| 1,000,000 | 205,935/s | 39,070/s |

Peak process RSS (`VmHWM`) after the algorithm sequence:

| Nodes | Max algorithm peak | After PageRank job |
|---:|---:|---:|
| 100,000 | 277 MB | 359 MB |
| 500,000 | 1.23 GB | 1.54 GB |
| 1,000,000 | 2.40 GB | 2.97 GB |

## Interpretation

- Background jobs add roughly 7–22% wall-clock overhead over the synchronous
  call at these sizes and keep the request loop responsive.
- Sparse 1M-node/1M-edge analytics complete in tens of seconds on this host;
  this is not a claim for dense graphs or multi-billion-edge datasets.
- Memory scales with nodes and adjacency entries. The 1M run stays below 3 GB
  but `k_core` and `label_propagation` are the largest consumers because they
  retain per-node maps and neighbor sets.
- These numbers are a baseline, not a comparison with Neo4j GDS. Use the same
  harness and host for regression tracking.
