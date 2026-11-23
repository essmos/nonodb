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
   "valid_from_field":"",
   "valid_to_field":"",
   "directed":true
 }}
```

`node_collection` is always set to `coll`. The other fields default to the
values above; empty `valid_from_field`/`valid_to_field` disable temporal
filtering. `set_graph` validates every existing edge, writes both the node-side
definition (`graph:<node_collection>`) and the edge-side mapping
(`graph_edge:<edge_collection>`), and rebuilds the adjacency index in the same
write transaction. A malformed existing edge aborts the whole change. Vault
collections cannot be used as node or edge collections.

| Operation | Purpose |
|---|---|
| `set_graph` | Create or replace a graph definition and rebuild adjacency |
| `get_graph` | Read the definition; returns `null` when not configured |
| `drop_graph` | Remove the definition and clear adjacency |
| `reindex_graph` | Rebuild adjacency synchronously, or start an online rebuild with `"online":true` |
| `graph_rebuild_status` | Read online rebuild progress |
| `cancel_graph_rebuild` | Discard an in-progress online rebuild |

```json
{"op":"get_graph","coll":"nodes"}
{"ok":true,"result":{"graph":{...}}}
```

`drop_graph` returns `{"dropped":true}` or `false` when no graph exists.
Synchronous `reindex_graph` returns `{"reindexed":coll,"edges":N}`. With
`"online":true` it returns immediately:

```json
{"active_generation":0,"target_generation":1,"scanned":0,"done":false}
```

The maintenance worker scans bounded chunks through the single writer thread.
Readers keep using the active generation; edge writes update both generations.
On completion the metadata is bumped and the old slots are dropped in one
transaction. On error the in-progress state and old graph remain intact and can
be resumed or cancelled.

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
 "edge_filter":{"weight":{"$gte":0.5}},
 "node_filter":{"status":"active"},
 "as_of":1735689600000,
 "projection":{"name":1,"status":1},
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
| `edge_filter` | – | Filter full edge documents before following them |
| `node_filter` | – | Include and expand only matching node documents |
| `as_of` | – | Unix ms or BSON date; follow only edges valid at that instant |
| `projection` | – | Flat projection for returned nodes; `_id` is always retained |
| `limit_nodes` | `1000` | Maximum node records, including starts and cycle markers |
| `limit_edges` | `10000` | Maximum adjacency entries inspected |
| `max_bytes` | 64 MiB | Serialized node/edge budget |

The whole traversal reads one LMDB read transaction, so it sees one consistent
snapshot. Each BFS level scans the adjacency index once; compact adjacency
values carry only edge id, label, weight, and validity, so full edge documents
are fetched only for `include_edges` or `edge_filter`. `node_filter` prunes
nodes before projection and expansion. A node is expanded once. When a later edge reaches an
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

`shortest_path` accepts the same `edge_filter`, `node_filter`, `as_of`, and
`projection` fields as `traverse`. `weighted:false` uses breadth-first search
and reports the hop count in `weight`. `weighted:true` uses Dijkstra over the configured `weight_field`;
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

## Neighbors

```json
{"op":"neighbors",
 "coll":"nodes",
 "start":"alice",
 "direction":"out",
 "labels":[],
 "edge_filter":{},
 "node_filter":{},
 "as_of":null,
 "projection":{},
 "include_edges":false,
 "limit":100}
```

Returns one-hop `edges` as `{node, via}`, plus `nodes`, `missing`, and
`truncated`. It uses the same filters as `traverse` but never builds a tree.

## Hybrid rerank

`hybrid_search` combines top-K vector hits with graph proximity:

```json
{"op":"hybrid_search",
 "coll":"nodes",
 "field":"embedding",
 "vector":[0.1,0.9,0.0],
 "k":10,
 "filter":{},
 "direction":"out",
 "max_depth":2,
 "limit":20,
 "vector_weight":0.7,
 "graph_weight":0.3,
 "projection":{"name":1}}
```

Each candidate receives `vector_score` (cosine relevance in `[0,1]`),
`graph_score` (`1/(1+depth)`), and their normalized weighted `score`. Results
are sorted by score descending with `_id` tie-breaking, so repeated requests
are deterministic. `edge_filter`, `node_filter`, `labels`, and `as_of` apply
during expansion.

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

Values are a compact versioned binary summary containing only the edge id,
label, weight, and validity timestamps; the edge JSON is not duplicated. Slot
names carry an active generation (`graph:<edge>:out` for generation 0,
`graph:<edge>:out:gN` afterwards) so an online rebuild can write new slots
without touching the graph readers see.

Entries are written and removed in the same LMDB transaction as the edge
document. This covers insert, update, delete, pipeline updates, TTL expiry,
replication apply, and `write_batch`. A crashed or interrupted transaction
cannot leave adjacency ahead of or behind the edge document.

Adjacency is derived data. It is rebuildable with `reindex_graph`, is not
included in document history, and is not treated as a source of truth by
backup or replication. Releases before compact adjacency stored the full edge
document in each slot; those values are still read transparently, and a
`reindex_graph` rewrites them in the compact format. Dropping a graph clears its
adjacency slots. During an online rebuild every edge mutation mirrors into both the active and target
generation, so the swapped-in graph includes writes that happened while it was
being built.

When `directed` is false, traversals follow both the out and in sides in either
direction; edge documents are still stored once.

## Security and limits

- `get_graph`, `traverse`, `shortest_path`, `neighbors`, and `hybrid_search`
  require a read scope on the node collection **and** the edge collection.
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

## `$graphLookup`

When a `$graphLookup` stage uses the collection's configured edge collection,
`connectFromField`/`connectToField` match the graph definition, the graph is
directed, and there is no `restrictSearchWithMatch`, aggregation uses the
adjacency index directly instead of repeated `find` joins. Any mismatch or
invalid start value falls back to the normal lookup path; results and limits are
the same.

## GUI

The **Graph** tab configures a graph (including validity fields), rebuilds
adjacency, traverses from one or more start ids with optional edge/node filters,
temporal `as_of`, and projection, and renders results as a tree. Expanding a node issues a
one-level `traverse` request, so large result sets stay bounded and interactive.

## Limitations

- HTTP/JSON only; no MongoDB wire graph operations.
- BFS, DFS, and Dijkstra only. No A*, PageRank, all-paths, or subgraph matching.
- No cross-database traversal and no edges that reference another database.
- One graph definition per node collection.
- Graph operations are read-only except for configuration and adjacency
  rebuild. Edges and nodes are edited with the ordinary document API.
