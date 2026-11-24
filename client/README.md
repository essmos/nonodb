# nonodb clients

- [Python](nonodb.py) — packaged as `nonodb-client`; sync client with optional cache, retries, and FastAPI example; `AsyncNonoDB` covers async watch/streams.
- [Rust](../nonodb-client) — sync crate using `reqwest`; Unix socket or TCP/TLS.
- [Node.js](node) — zero-dependency async client; Unix socket or TCP/TLS.
- [Go](go) — context-aware client; Unix socket or TCP/TLS.

See [docs/clients.md](../docs/clients.md) for installation, TCP/TLS, mTLS, and
examples. All clients accept `NONODB_TLS_CA`, `NONODB_TLS_CLIENT_CERT`, and
`NONODB_TLS_CLIENT_KEY` when connecting to a daemon that requires client
certificates.

## Write routing

All clients accept a separate primary endpoint for writes and can discover it
from a replica's `/readyz`. Reads continue to use the socket/base URL. See
[Clients](../docs/clients.md#routing-writes-to-the-primary).

```bash
export NONODB_WRITE_URL=https://primary.internal:7878
export NONODB_AUTO_PRIMARY=1
export NONODB_TOKEN='nodb_...'
```

## Databases

All clients accept an explicit database and read `NONODB_DB` as a fallback. The
default is `default`. Collections and queries never span databases.

```python
db = NonoDB("/run/nonodb.sock", database="app1")
```

```rust
let db = Client::builder()
    .socket_path("/run/nonodb.sock")
    .database("app1")
    .build()?;
```

```js
const db = new NonoDB({ socketPath: '/run/nonodb.sock', database: 'app1' });
```

```go
db := nonodb.New(nonodb.WithDatabase("app1"))
```

See [Multiple databases](../docs/multi-database.md).

## Batch lookups

All clients expose `lookup`/`lookup_one` for ordered point lookups by `_id` or a
matching unique single-field or compound index. They support covering fields,
projection objects, and a 64 MiB result budget. Missing keys produce `null`;
`get_many` keeps its old filtering behavior. See
[Batch lookups](../docs/clients.md#batch-lookups) for examples in Python, Rust,
Node.js, and Go.

## Vector search

All clients support exact vector search with `cosine`, `dot`, and `l2` metrics.
Use the client-specific `create_vector_index` / `vector_search` helpers; see
[Vector search](../docs/clients.md#vector-search) for examples in Python, Rust,
Node.js, and Go.

## Graph

```python
nodes = db.collection("nodes")
nodes.set_graph("edges")
tree = nodes.traverse("alice", direction="out", max_depth=2)
flat = nodes.traverse("alice", max_depth=1, tree=False)["items"]
seed = nodes.traverse(
    seed={"vector": {"field": "embedding", "vector": [0.1, 0.9, 0.0], "k": 5}},
    max_depth=2,
)
path = nodes.shortest_path("alice", "carol", weighted=True)
paths = nodes.all_shortest_paths("alice", "carol", limit_paths=10)
matches = nodes.graph_match(
    "alice",
    [{"direction": "out", "labels": ["KNOWS"], "min_hops": 1, "max_hops": 3}],
)
ranked = nodes.pagerank(limit=20)
degrees = nodes.degree_centrality(limit=20)
components = nodes.connected_components()
nodes.reindex_graph()
nodes.drop_graph()
```

The result contains `nodes`, `edges`, `truncated`, and `missing`; repeated
arrivals are marked with `cycle`. See [Graph traversal](../docs/graph.md).

## Document history

Enable history per collection with `set_collection_history(coll, max_versions)`. All clients expose
history, exact-revision helpers, and aggregation.

## Document TTL

Collection defaults and per-document `_ttl` overrides are available through
the normal API and the Python `db.create_collection(..., ttl_seconds=...)`,
`set_collection_ttl`, and `get_collection_ttl` helpers. The Python client also
exposes `set_collection_ttl_after`, `backfill_collection_ttl`, and
`get_collection_ttl_field` for date-field TTL. See
[Document TTL](../docs/ttl.md).

## Vault collections

Use `db.vault(tenant, key_id, key)` for a handle that sends the three vault
headers on every request. Vault collections support CRUD by `_id`, list/count
by `tenant_id`, and history; search, indexes, aggregation, `batch`, and
`write_batch` are rejected. See [Vault collections](../docs/vault.md).

## SQL

All official clients expose read-only `db.sql(query, params)` / `client.sql(...)`.
See [SQL queries](../docs/sql.md) and [Clients](../docs/clients.md#sql) for
language-specific examples.
