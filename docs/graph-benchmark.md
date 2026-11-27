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
| All shortest paths | `all_shortest_paths(..., limit_paths=10)` | `allShortestPaths()` with `*..6`, returns path count |
| Pattern match depth 2 | `graph_match` with `min_hops=1`, `max_hops=2` | `MATCH p=...*1..2` returns path count |
| Cypher pattern depth 2 | `cypher("MATCH p=(a:Node)-[*1..2]->(b) RETURN count(p) AS c")` | same Cypher text |
| Cypher shortest path | `cypher("... MATCH p=shortestPath((a)-[*..6]->(b)) RETURN length(p)")` | same Cypher text |

The benchmark validates reachability, all-shortest-path counts, and pattern-match
counts for one sample against both engines before reporting timings. It does not
compare weighted shortest paths, filtered traversals, hybrid vector search, graph
algorithms (Neo4j Community has no built-in GDS), or concurrent clients.

## Results

Median of three runs, release builds, after warmup. Latency is end-to-end
request time in milliseconds; smaller is better.

| Metric | NonoDB | Neo4j Community | NonoDB advantage |
|---|---:|---:|---:|
| Ingest nodes + edges | **166 ms** (~48k edges/s) | 2,335 ms (~3.4k edges/s) | **~14.0×** |
| Traverse depth 1 p50 / p95 | **0.72 / 1.30 ms** | 9.41 / 18.60 ms | **~13.1×** p50 |
| Traverse depth 3 p50 / p95 | **1.53 / 2.84 ms** | 8.41 / 15.07 ms | **~5.5×** p50 |
| Shortest path by hops p50 / p95 | **0.78 / 1.53 ms** | 6.76 / 12.38 ms | **~8.7×** p50 |
| All shortest paths p50 / p95 | **0.73 / 1.36 ms** | 6.18 / 11.35 ms | **~8.5×** p50 |
| Pattern match depth 2 p50 / p95 | **1.09 / 1.92 ms** | 5.50 / 9.85 ms | **~5.0×** p50 |
| Cypher pattern depth 2 p50 / p95 | **1.48 / 2.06 ms** | 33.61 / 39.52 ms | **~22.8×** p50 |
| Cypher shortest path p50 / p95 | **0.99 / 1.45 ms** | 5.85 / 10.88 ms | **~5.9×** p50 |

Observed run-to-run range:

| Metric | NonoDB p50 | Neo4j p50 |
|---|---:|---:|
| Traverse depth 1 | 0.66–0.75 ms | 8.90–9.63 ms |
| Traverse depth 3 | 1.50–1.62 ms | 6.20–9.04 ms |
| Shortest path | 0.73–0.82 ms | 6.33–8.27 ms |
| All shortest paths | 0.70–0.77 ms | 5.79–6.96 ms |
| Pattern match depth 2 | 1.06–1.12 ms | 5.44–6.44 ms |
| Cypher pattern depth 2 | 1.38–1.63 ms | 32.76–33.97 ms |
| Cypher shortest path | 0.97–1.03 ms | 5.01–6.43 ms |
| Ingest | 159–177 ms | 2,301–2,338 ms |

## How to read the numbers

- **Transport differs.** NonoDB used a Unix socket with JSON messages; Neo4j
  used HTTP transactional Cypher. A meaningful part of Neo4j's fixed latency is
  the HTTP transaction and JVM request path, not the traversal itself.
- **Response work differs.** NonoDB materialized projected node documents;
  Neo4j returned only a count or path length. The NonoDB side did more response
  work, so the gap is not explained by NonoDB returning less data.
- **Pattern semantics are close, not identical.** NonoDB `graph_match` uses a
  linear JSON pattern with relationship uniqueness. Neo4j `MATCH p=...*1..2`
  uses Cypher path semantics. The two Cypher rows run the same read-only query
  text in both engines and validate the returned count/length before timing.
- **Graph algorithms are not benchmarked against Neo4j.** PageRank, degree
  centrality, connected components, and node similarity are implemented in
  NonoDB, but Neo4j Community has no built-in GDS equivalent without installing
  a plugin. There is no apples-to-apples latency row for those operations.
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
