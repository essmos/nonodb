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
- `pagerank` uses 5 iterations and outgoing edges; `label_propagation` uses 5
  synchronous iterations over both directions; `k_core` and
  `degree_distribution` use both directions. All algorithms use a 1 GiB per-run
  memory budget.
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
| 100,000 | 100,000 | 2.52 s | 361 ms | 594 ms | 1.43 s | 1.96 s | 1.30 s | 1.52 s |
| 500,000 | 500,000 | 15.28 s | 2.37 s | 3.44 s | 10.64 s | 12.97 s | 8.90 s | 12.26 s |
| 1,000,000 | 1,000,000 | 32.46 s | 4.34 s | 7.77 s | 23.09 s | 26.49 s | 21.09 s | 26.31 s |

Ingest throughput:

| Nodes | Node ingest | Edge ingest + adjacency |
|---:|---:|---:|
| 100,000 | 221,221/s | 48,252/s |
| 500,000 | 182,900/s | 39,848/s |
| 1,000,000 | 182,415/s | 37,069/s |

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
