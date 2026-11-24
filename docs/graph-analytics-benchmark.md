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
| 100,000 | 100,000 | 2.44 s | 361 ms | 595 ms | 1.32 s | 1.06 s | 1.34 s | 1.36 s |
| 500,000 | 500,000 | 14.56 s | 2.13 s | 3.18 s | 9.03 s | 6.75 s | 7.86 s | 9.73 s |
| 1,000,000 | 1,000,000 | 30.47 s | 4.14 s | 7.06 s | 21.05 s | 15.71 s | 18.85 s | 24.79 s |

Ingest throughput:

| Nodes | Node ingest | Edge ingest + adjacency |
|---:|---:|---:|
| 100,000 | 193,857/s | 51,843/s |
| 500,000 | 182,897/s | 42,297/s |
| 1,000,000 | 207,270/s | 38,993/s |

Peak process RSS (`VmHWM`) after the algorithm sequence:

| Nodes | Max algorithm peak | After PageRank job |
|---:|---:|---:|
| 100,000 | 277 MB | 295 MB |
| 500,000 | 1.23 GB | 1.54 GB |
| 1,000,000 | 2.40 GB | 2.97 GB |

## Interpretation

- Background jobs add roughly 1–18% wall-clock overhead over the synchronous
  call at these sizes and keep the request loop responsive.
- Sparse 1M-node/1M-edge analytics complete in tens of seconds on this host;
  this is not a claim for dense graphs or multi-billion-edge datasets.
- Memory scales with nodes and adjacency entries. The 1M run stays below 3 GB
  but `k_core` and `label_propagation` are the largest consumers because they
  retain per-node maps and neighbor sets.
- These numbers are a baseline, not a comparison with Neo4j GDS. Use the same
  harness and host for regression tracking.
