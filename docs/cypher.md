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
| `MATCH`, `OPTIONAL MATCH`, `WHERE`, `RETURN` | Supported | labels, properties, direction, multiple patterns |
| `WITH`, `UNWIND`, `ORDER BY`, `SKIP`, `LIMIT`, `DISTINCT` | Supported | deterministic ordering for equal keys |
| Aggregations | Supported | `count`, `sum`, `avg`, `min`, `max`, `collect`; `DISTINCT`; null handling |
| Expressions | Supported | list/map/string/math functions, `CASE`, comprehensions, map projections |
| Variable-length paths | Supported | bounded by `max_depth`; relationship uniqueness |
| `shortestPath`, `allShortestPaths` | Supported | unweighted; depth bounded |
| Pattern comprehensions / pattern expressions | Partial | selected TCK scope; unsupported forms fail explicitly |
| Subqueries | Partial | `CALL {}` is not part of the selected TCK baseline and fails explicitly |
| Temporal values | Partial | constructors and `toString`; extreme ranges rejected |
| `CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE` | Rejected | read-only mode |
| `LOAD CSV`, schema commands, APOC, Bloom | Rejected | out of scope |
| Arbitrary procedures | Rejected | only the four `gds.*` procedures below |

Unsupported syntax returns `invalid_query`; it is never silently ignored.


## GDS-style procedures

The following procedures are available through `CALL ... YIELD`:

| Procedure | Yields |
|---|---|
| `gds.pagerank.stream()` | `nodeId`, `score` |
| `gds.degree.stream()` | `nodeId`, `degree`, `in`, `out` |
| `gds.connectedComponents.stream()` | `nodeId`, `componentId`, `size` |
| `gds.nodeSimilarity.stream()` | `node1`, `node2`, `similarity` |

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
- `max_rows` (default 1000, max 100000), `max_operations` (default 1,000,000),
  and `timeout_ms` bound execution; the operation budget is checked per row and
  per expansion, so cancellation returns a structured error;
- `max_depth` bounds variable-length expansion (hard maximum 64);
- literal/pattern nesting is capped at 16 levels to prevent parser stack
  exhaustion;
- the GDS procedures process at most 1,000 nodes and emit at most `max_rows`;
- unsupported syntax fails explicitly with `invalid_query`, never a silent
  wrong result;
- vault collections are rejected, and graph reads require scope on the node
  and edge collections.

## TCK baseline

The harness pins the openCypher TCK at commit
`677cbafabb8c3c5eed458fd3b1ec0daec8d67d23`.

Baseline selection is intentionally narrow and documented in
`scripts/cypher_tck.py`:

- scenarios starting with `Given any graph`;
- no setup writes (`having executed`);
- no parameters;
- no query that writes or calls an arbitrary procedure;
- result-table scenarios only (no expected-error/outline scenarios).

Current baseline: **270 / 293 pass (92.15%)**, **0 wrong-result failures**, and
23 explicit unsupported cases (parser edge cases, mixed-type min/max, extreme
temporal ranges, and deeply nested literals). See `docs/tck-baseline.json` and
run the harness for the full per-scenario report.

```bash
scripts/fetch-opencypher-tck.sh /tmp/opencypher-tck
cargo build --release --bins
python3 scripts/cypher_tck.py \
  --tck-dir /tmp/opencypher-tck \
  --daemon target/release/nonodb-daemon \
  --output tck-report.json
```

The full 90% gate for the broader read-only corpus is an ongoing goal; the
baseline above is not a claim of full TCK conformance.
