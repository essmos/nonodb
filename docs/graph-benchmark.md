# Graph benchmark: NonoDB vs Neo4j Community

This page documents the end-to-end graph comparison produced by
[`scripts/graph_neo4j_bench.py`](../scripts/graph_neo4j_bench.py). It answers a
narrow question: for a small in-memory-resident graph and the same traversal
operations, what does request latency look like through each engine's public
API?

It is **not** a general graph-database ranking, a durability benchmark, or a
replication benchmark.

## Environment

| Component | Value |
|---|---|
| Host | 8 vCPU Haswell, 22 GiB RAM |
| Filesystem | ext4 (`/var/tmp`) |
| NonoDB | `v0.32.0`, release build, Unix socket |
| Neo4j | `neo4j:5-community`, HTTP transactional Cypher |
| Python client | `httpx` for Neo4j, official NonoDB Python client |
| Runs | 3 independent runs; table reports the median of run-level values |

The Neo4j container used its default configuration. No JVM tuning, page-cache
tuning, or Neo4j Enterprise features were enabled.

## Dataset

NonoDB edge documents were mapped to Neo4j `:EDGE` relationships with `id` and
`label` properties. The dataset is deterministic (`random.Random(42)`):

- **2,000 nodes**, `n0` through `n1999`;
- **8,000 directed edges**;
- a ring (`nI -> n(I+1)`) guarantees connectivity;
- the remaining edges are random, with labels `A`, `B`, or `C`;
- ingest uses batches of 1,000 edges;
- 20 reachable shortest-path pairs with hop distance 2–4;
- traversal measurements use 20 random roots, repeated five times (100 samples).

The graph is small enough to be cache-resident. This removes most disk I/O
differences and makes the test primarily a comparison of graph traversal and
request-path overhead.

## Operations measured

| Operation | NonoDB | Neo4j |
|---|---|---|
| Ingest | `insert_many` for nodes and edges, adjacency maintained during edge insert | `UNWIND` + `CREATE` nodes, then `MATCH` + `CREATE` relationships |
| Traverse depth 1 | `traverse(start, max_depth=1, projection={"_id":1})` | single-hop pattern, `count(DISTINCT m.id)` |
| Traverse depth 3 | `traverse(start, max_depth=3, projection={"_id":1})` | variable-length pattern `*1..3`, `count(DISTINCT m.id)` |
| Shortest path | BFS by hops, `max_depth=6`, projected nodes | `shortestPath()` with `*..6`, returns path length |

The benchmark validates reachability for one sample root against both engines
before reporting timings. It does not compare weighted shortest paths, filtered
traversals, hybrid vector search, or concurrent clients.

## Results

Median of three runs, release builds, after warmup. Latency is end-to-end
request time in milliseconds; smaller is better.

| Metric | NonoDB | Neo4j Community | NonoDB advantage |
|---|---:|---:|---:|
| Ingest nodes + edges | **166 ms** (~48k edges/s) | 2,323 ms (~3.4k edges/s) | **~14.0×** |
| Traverse depth 1 p50 / p95 | **0.75 / 1.36 ms** | 8.64 / 13.21 ms | **~11.6×** p50 |
| Traverse depth 3 p50 / p95 | **1.32 / 2.34 ms** | 8.21 / 11.96 ms | **~6.2×** p50 |
| Shortest path by hops p50 / p95 | **0.80 / 1.52 ms** | 6.15 / 12.72 ms | **~7.6×** p50 |

Observed run-to-run range:

| Metric | NonoDB p50 | Neo4j p50 |
|---|---:|---:|
| Traverse depth 1 | 0.73–0.76 ms | 7.76–9.03 ms |
| Traverse depth 3 | 1.24–1.54 ms | 7.31–11.00 ms |
| Shortest path | 0.78–0.81 ms | 5.93–8.62 ms |
| Ingest | 154–168 ms | 2,207–2,426 ms |

## How to read the numbers

- **Transport differs.** NonoDB used a Unix socket with JSON messages; Neo4j
  used HTTP transactional Cypher. A meaningful part of Neo4j's fixed latency is
  the HTTP transaction and JVM request path, not the traversal itself.
- **Response work differs.** NonoDB materialized projected node documents;
  Neo4j returned only a count or path length. The NonoDB side did more response
  work, so the gap is not explained by NonoDB returning less data.
- **The graph is small.** On a larger-than-RAM graph, storage and page-cache
  behavior would matter more. The relative gap may narrow or widen depending on
  graph shape and indexes.
- **Neo4j Community is single-node.** It has no built-in multi-node replication;
  clustering is an Enterprise feature. This page says nothing about replication
  lag or failover.
- **Ingest includes different work.** NonoDB maintained adjacency during edge
  ingestion; Neo4j created native relationships and maintained its relationship
  store. Both used the same 1,000-edge batch size.
- **No concurrency.** Both engines were measured with one client request at a
  time. Group commit, JVM GC, and connection pooling under load are out of scope.

## Replication is not covered

NonoDB graph replication is covered separately by
[`scripts/graph_replication_test.py`](../scripts/graph_replication_test.py) and
[`scripts/graph_rebuild_crash_test.py`](../scripts/graph_rebuild_crash_test.py),
not by this benchmark. Those tests verify:

- edge insert/update/delete replication;
- graph definition replication;
- generation propagation after an online adjacency rebuild;
- resuming an online rebuild after `kill -9`.

A fair replication benchmark would need a multi-node NonoDB deployment and, for
Neo4j, a clustering-capable edition. That is outside this comparison.

## Reproduce

```bash
cargo build --release --bins
docker pull neo4j:5-community
NONODB_DAEMON=target/release/nonodb-daemon \
  python3 scripts/graph_neo4j_bench.py
```

Optional environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `NONODB_BENCH_NODES` | `2000` | Node count |
| `NONODB_BENCH_EDGES` | `8000` | Edge count |
| `NONODB_BENCH_BATCH` | `1000` | Ingest batch size |
| `NONODB_BENCH_DIR` | `/var/tmp` | NonoDB data directory; use real disk, not tmpfs |
| `NONODB_DAEMON` | `target/release/nonodb-daemon` | Daemon binary |
| `NEO4J_IMAGE` | `neo4j:5-community` | Docker image |

The script starts and removes its own Neo4j container and uses a temporary
NonoDB data directory. It does not touch production data.
