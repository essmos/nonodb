# Graph traversal

nonodb can treat ordinary documents as a property graph. There is no separate
node or edge storage format:

- a **node** is a document in a node collection;
- an **edge** is a document in an edge collection;
- the graph definition names the edge collection and the fields used for the
  source, target, optional label, and optional weight.

Graphs are exposed through the HTTP/JSON API only. They are not part of the
MongoDB wire protocol.

## Model

```json
// nodes
{"_id":"alice","name":"Alice","embedding":[1,0,0]}
{"_id":"bob","name":"Bob","embedding":[0,1,0]}

// edges
{"_id":"e1","from":"alice","to":"bob","label":"KNOWS","weight":0.8}
```

Node and edge `_id` values must be strings or ObjectIds. Edge endpoints use the
same identity rules as `_id` lookups. Missing or malformed endpoint fields make
the write fail; once a collection is used as an edge collection, malformed edge
documents are rejected instead of being stored without an adjacency entry.

## Configuration

```json
{"op":"set_graph",
 "coll":"nodes",
 "def":{
   "node_collection":"nodes",
   "edge_collection":"edges",
   "from_field":"from",
   "to_field":"to",
   "label_field":"label",
   "weight_field":"weight",
   "directed":true
 }}
```

`node_collection` is always set to `coll`. The other fields default to the
values above. `set_graph` validates every existing edge, writes both the
node-side definition (`graph:<node_collection>`) and the edge-side mapping
(`graph_edge:<edge_collection>`), and rebuilds the adjacency index in the same
write transaction. A malformed existing edge aborts the whole change.

| Operation | Purpose |
|---|---|
| `set_graph` | Create or replace a graph definition and rebuild adjacency |
| `get_graph` | Read the definition; returns `null` when not configured |
| `drop_graph` | Remove the definition and clear adjacency |
| `reindex_graph` | Rebuild adjacency from the edge collection |

```json
{"op":"get_graph","coll":"nodes"}
{"ok":true,"result":{"graph":{...}}}
```

`drop_graph` returns `{"dropped":true}` or `false` when no graph exists.
`reindex_graph` returns the number of edge documents scanned.

## Traverse

```json
{"op":"traverse",
 "coll":"nodes",
 "start":["alice"],
 "direction":"out",
 "labels":["KNOWS"],
 "max_depth":2,
 "algorithm":"bfs",
 "tree":true,
 "include_edges":false,
 "limit_nodes":1000,
 "limit_edges":10000,
 "max_bytes":67108864}
```

| Field | Default | Meaning |
|---|---|---|
| `start` | required unless `seed` | Node ids to start from |
| `seed` | – | Vector seed, see below |
| `direction` | `out` | `out`, `in`, or `both` |
| `labels` | all | Edge labels to follow |
| `max_depth` | `3` | Maximum traversal depth (0–64) |
| `algorithm` | `bfs` | `bfs` (queue) or `dfs` (stack) |
| `tree` | `true` | Nested tree or flat `items` |
| `include_edges` | `false` | Include the full edge document in `via` |
| `limit_nodes` | `1000` | Maximum node records, including starts and cycle markers |
| `limit_edges` | `10000` | Maximum adjacency entries inspected |
| `max_bytes` | 64 MiB | Serialized node/edge budget |

The whole traversal reads one LMDB read transaction, so it sees one consistent
snapshot. Each BFS level scans the adjacency index once; edges are not fetched
with one request per node. A node is expanded once. When a later edge reaches an
already visited node, the result contains a marker with `"cycle":true` and no
children.

Tree response:

```json
{"ok":true,"result":{
  "roots":[{
    "node":{"_id":"alice","name":"Alice"},
    "depth":0,
    "children":[{
      "node":{"_id":"bob","name":"Bob"},
      "depth":1,
      "via":{"_id":"e1","label":"KNOWS","weight":0.8},
      "children":[]
    }]
  }],
  "nodes":2,
  "edges":1,
  "truncated":false,
  "missing":[]
}}
```

With `"tree":false` the response uses `items`; each item contains `node`,
`depth`, `cycle`, optional `via`, and `parent` (the parent id). `truncated`
becomes true when a node, edge, or byte budget is reached. `missing` lists start
ids and neighbour ids that do not resolve to documents; unresolved neighbours
are not expanded.

## Shortest path

```json
{"op":"shortest_path",
 "coll":"nodes",
 "start":"alice",
 "target":"carol",
 "direction":"out",
 "labels":[],
 "max_depth":8,
 "limit_nodes":1000,
 "weighted":false}
```

`weighted:false` uses breadth-first search and reports the hop count in
`weight`. `weighted:true` uses Dijkstra over the configured `weight_field`;
missing, non-numeric, negative, or non-finite weights fail with
`invalid_query`. The response is:

```json
{"found":true,
 "path":[{"node":{"_id":"alice"}},
         {"node":{"_id":"bob"},"via":{"_id":"e1",...}}],
 "weight":0.8,
 "hops":1}
```

`{"found":false}` means no path exists within the limits. `path` includes the
full edge document in `via`.

## Vector seed

`traverse` can start from the top-K results of an existing vector index:

```json
{"op":"traverse",
 "coll":"nodes",
 "seed":{"vector":{
   "field":"embedding",
   "vector":[0.1,0.9,0.0],
   "k":5,
   "filter":{}
 }},
 "max_depth":2}
```

`start` and `seed` are mutually exclusive. The vector index must exist and the
query must have the indexed number of dimensions. Results become start ids in
vector order; `k` is limited to 1–1000.

## Adjacency index

Each configured graph maintains derived adjacency databases through the normal
reusable slot pool:

- `graph:<edge_collection>:out` — key `from`, label, target, edge id;
- `graph:<edge_collection>:in` — key `to`, label, source, edge id.

Entries are written and removed in the same LMDB transaction as the edge
document. This covers insert, update, delete, pipeline updates, TTL expiry,
replication apply, and `write_batch`. A crashed or interrupted transaction
cannot leave adjacency ahead of or behind the edge document.

Adjacency is derived data. It is rebuildable with `reindex_graph`, is not
included in document history, and is not treated as a source of truth by
backup or replication. Dropping a graph clears its adjacency slots.

When `directed` is false, traversals follow both the out and in sides in either
direction; edge documents are still stored once.

## Security and limits

- `get_graph`, `traverse`, and `shortest_path` require a read scope on the node
  collection.
- `set_graph` requires collection-admin scope on both the node and edge
  collections. `drop_graph` and `reindex_graph` require admin scope on the node
  collection.
- Graph operations are rejected on vault collections.
- Graph definitions and traversals are database-scoped; cross-database edges are
  not supported.

| Limit | Value |
|---|---|
| `start` ids | 10,000 |
| `max_depth` | 64 |
| default `limit_nodes` | 1,000 |
| default `limit_edges` | 10,000 |
| default `max_bytes` | 64 MiB |
| vector seed `k` | 1–1,000 |

Reaching a node, edge, or byte limit returns the partial result with
`"truncated":true`; it is not an error.

## GUI

The **Graph** tab configures a graph, rebuilds adjacency, and traverses from one
or more start ids. Results are rendered as a tree. Expanding a node issues a
one-level `traverse` request, so large result sets stay bounded and interactive.

## Limitations

- HTTP/JSON only; no MongoDB wire graph operations.
- BFS, DFS, and Dijkstra only. No A*, PageRank, all-paths, or subgraph matching.
- No cross-database traversal and no edges that reference another database.
- One graph definition per node collection.
- Graph operations are read-only except for configuration and adjacency
  rebuild. Edges and nodes are edited with the ordinary document API.
