# Read-only Cypher compatibility

NonoDB exposes a read-only Cypher query surface over the document graph. The
front end is the MIT-licensed [`cypher-rs-zk`](https://crates.io/crates/cypher-rs-zk)
parser/planner; NonoDB supplies the graph adapter, expression evaluation, and
storage execution.

This is a compatibility subset, not full Cypher. Write clauses are rejected.

## API

```json
{"op":"cypher",
 "coll":"nodes",
 "query":"MATCH (p:Person)-[:KNOWS]->(f) WHERE p.name = $name RETURN f.name AS name ORDER BY name",
 "params":{"name":"Alice"},
 "max_rows":1000,
 "max_operations":1000000,
 "timeout_ms":2000,
 "max_depth":64}
```

The response contains `columns`, `rows`, `row_count`, and `truncated`.
`rows` is an array of column/value objects. Node values expose `_id` and
`_labels`; relationship values expose `_id`, `_type`, `_start`, and `_end`.
Labels are read from `_labels` (or `labels`); relationship types come from the
graph definition's edge label field.

Python:

```python
rows = nodes.cypher(
    "MATCH (n:Person) WHERE n.age > $age RETURN n.name AS name ORDER BY name",
    params={"age": 20},
)
```

## Compatibility matrix

| Surface | Status | Notes |
|---|---|---|
| `MATCH`, `OPTIONAL MATCH`, `WHERE`, `RETURN` | Supported | labels, properties, direction, multiple patterns; a leading `OPTIONAL MATCH` is supported |
| `WITH`, `UNWIND`, `ORDER BY`, `SKIP`, `LIMIT`, `DISTINCT` | Partial | deterministic ordering for equal keys; `WITH ... SKIP` followed by another `MATCH` is explicitly unsupported |
| Aggregations | Supported | `count`, `sum`, `avg`, `min`, `max`, `collect`; `DISTINCT`; null handling |
| Expressions | Partial | list/map/string/math functions, `CASE`, comprehensions, map projections; some parser edge cases (long comparison chains, parameterised list slices) fail explicitly |
| Variable-length paths | Supported | bounded by `max_depth`; relationship uniqueness |
| `shortestPath`, `allShortestPaths` | Supported | unweighted; depth bounded |
| Pattern comprehensions / pattern expressions | Partial | selected TCK scope; unsupported forms fail explicitly |
| Existential subqueries (`EXISTS { ... }`) | Supported | correlated and nested forms; `CALL {}` is not supported and fails explicitly |
| Temporal values | Partial | constructors and accessors; some functions and extreme ranges fail explicitly |
| `CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE` | Rejected | read-only mode |
| `LOAD CSV`, schema commands, APOC, Bloom | Rejected | out of scope |
| Arbitrary procedures | Rejected | only the seven `gds.*` procedures below |

Unsupported syntax returns `invalid_query`; it is never silently ignored.


## GDS-style procedures

The following procedures are available through `CALL ... YIELD`:

| Procedure | Yields |
|---|---|
| `gds.pagerank.stream()` | `nodeId`, `score` |
| `gds.degree.stream()` | `nodeId`, `degree`, `in`, `out` |
| `gds.connectedComponents.stream()` | `nodeId`, `componentId`, `size` |
| `gds.nodeSimilarity.stream()` | `node1`, `node2`, `similarity` |
| `gds.labelPropagation.stream()` | `nodeId`, `label` |
| `gds.kCore.stream()` | `nodeId`, `core` |
| `gds.degreeDistribution.stream()` | `degree`, `count` |

Definitions:

- **PageRank**: iterative PageRank with damping `0.85`, at most 100 iterations,
  convergence at `1e-6`; scores normalized to sum to 1 across processed nodes.
- **Degree centrality**: total degree (`in + out`) for the configured graph
  direction; yields `degree`, `in`, and `out`.
- **Connected components**: weakly connected components over both edge
  directions; component IDs are deterministic and `size` is the component size.
- **Node similarity**: Jaccard similarity between neighbor sets, using both edge
  directions; pairs with similarity `<= 0` are omitted; results sorted by
  descending similarity.
- **Label propagation**: deterministic synchronous propagation with label ids
  initialized to node ids; ties choose the lexicographically smallest label.
- **k-core**: deterministic peeling core number using both edge directions.
- **Degree distribution**: histogram of `in + out` total degree.

They run over the configured graph, are bounded by `max_rows`/`max_operations`,
and use the same read-only transaction and authorization path as other graph
operations. Procedure arguments are currently accepted but ignored; the graph
definition supplies the labels/types/filters.

```cypher
CALL gds.pagerank.stream() YIELD nodeId, score
RETURN nodeId, score ORDER BY score DESC
```

## Safety

- exactly one read-only statement per request;
- write clauses are rejected before planning;
- `max_rows` (default 1000, max 100000), `max_operations` (default 1,000,000,
  max 100,000,000), `max_memory_bytes` (default 64 MiB), and `timeout_ms`
  (default 30 s, max 5 min) bound execution; operation, row, and memory budgets
  are charged during execution, including `UNWIND`/`DISTINCT`/`SORT`/`UNION`, so
  cancellation returns a structured error;
- `max_depth` bounds variable-length expansion (hard maximum 64);
- literal/pattern nesting is capped at 16 levels to prevent parser stack
  exhaustion;
- the GDS procedures use configurable `gds_max_nodes` (default 100,000, max
  1,000,000), `gds_max_edges` (default 1,000,000, max 10,000,000),
  `gds_max_memory_bytes` (default 256 MiB, max 1 GiB), and `gds_timeout_ms`
  (falling back to `timeout_ms` when unset); they emit at most `max_rows` and
  set `truncated:true` when clipped;
- GDS node, edge, memory, and timeout overruns fail explicitly with
  `invalid_query` instead of returning an incomplete graph;
- unsupported syntax fails explicitly with `invalid_query`, never a silent
  wrong result;
- vault collections are rejected, and graph reads require scope on the node
  and edge collections.

## TCK baseline

The harness pins the openCypher TCK at commit
`677cbafabb8c3c5eed458fd3b1ec0daec8d67d23`.

Selection is deliberately broad and reproducible: every result-table scenario
with a read-only query, including graph fixtures (`having executed`), named
graphs (`the binary-tree-1 graph`), parameters, `MATCH`/`OPTIONAL MATCH`, and
pattern/path queries. Expected-error scenarios, `Scenario Outline` expansion,
write queries, custom `test.*` procedures, and unsupported fixture syntax are
excluded; each excluded scenario and its reason are listed in the committed
[`tck-report.json`](tck-report.json), not silently skipped.

Current baseline: **859 / 874 pass (98.28%)**, with **0 known wrong-result
mismatches** and **15 explicit unsupported cases**. Unsupported syntax returns
`invalid_query`, never a silent success. Each unsupported scenario and reason is
listed in `tck-report.json`.

The remaining unsupported cases are deliberate safety/scope limits and parser
edge cases: nesting above 16 levels, long chained comparisons, parameterised
list slices, two temporal functions, two temporal fixture forms, and one
`WITH ... SKIP`/`MATCH` pipeline shape. They are reported per scenario rather
than silently skipped.

```bash
scripts/fetch-opencypher-tck.sh /tmp/opencypher-tck
cargo build --release --bins
python3 scripts/cypher_tck.py \
  --tck-dir /tmp/opencypher-tck \
  --daemon target/release/nonodb-daemon \
  --output tck-report.json
```

The harness runs in CI and fails below 90% or on any known wrong-result mismatch.
