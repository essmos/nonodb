# Search and Atlas Search compatibility

NonoDB has three search engines built into the same database:

- BM25 full-text search over a text index,
- fuzzy trigram/Levenshtein search over a fuzzy index,
- exact vector search over a vector index, with an optional IVF accelerator.

The existing `search`, `fuzzy`, and `vector_search` operations remain
available. `search_all` is the unified, higher-level API. It can search several
index fields, combine lexical and vector results with reciprocal-rank fusion
(RRF), and return common scoring metadata.

For applications migrating from MongoDB Atlas Search, the daemon also accepts a
small compatibility subset of the `$search`, `$searchMeta`, and `$vectorSearch`
aggregation stages. This is a migration bridge, not full Atlas Search parity.

## Create the indexes

```json
{"op":"create_text_index","coll":"products","field":"title"}
{"op":"create_text_index","coll":"products","field":"description"}
{"op":"create_fuzzy_index","coll":"products","field":"title"}
{"op":"create_vector_index","coll":"products","field":"embedding","dims":384,"metric":"cosine"}
```

Each text or fuzzy index covers exactly one field. Vector indexes store one
vector per document; clients send ready vectors. NonoDB does not generate
embeddings.

## Unified search_all API

```json
{
  "op": "search_all",
  "coll": "products",
  "mode": "hybrid",
  "query": "coffee grinder",
  "fields": ["title", "description"],
  "vector": [0.12, -0.03, 0.44],
  "vector_field": "embedding",
  "filter": {"status": "active"},
  "skip": 0,
  "limit": 20,
  "rrf_k": 60
}
```

Modes:

| Mode | Required input | Index |
|---|---|---|
| `lexical` | `query` | one or more text indexes |
| `fuzzy` | `query` | one or more fuzzy indexes |
| `vector` | `vector` | one vector index or `vector_field` |
| `hybrid` | `query` + `vector` | text indexes + vector index |

If `fields` is omitted, all text indexes are used for `lexical`/`hybrid`, and
all fuzzy indexes for `fuzzy`. If `vector_field` is omitted and there is exactly
one vector index, it is selected automatically; multiple vector indexes require
an explicit field.

`filter` is a normal document filter applied by each source before pagination.
`skip` and `limit` apply after fusion. `limit` defaults to 10 and is capped at
1,000.

Response:

```json
{
  "results": [
    {
      "_id": "p1",
      "title": "Coffee grinder",
      "_score": 0.016393,
      "_sources": {
        "lexical:title": {"rank": 1, "score": 2.41},
        "vector:embedding": {"rank": 2, "distance": 0.08}
      }
    }
  ],
  "meta": {
    "mode": "hybrid",
    "rrf_k": 60,
    "candidate_count": 1,
    "sources": ["lexical:title", "vector:embedding"]
  }
}
```

`_score` is reciprocal-rank fusion, not a raw BM25 or distance value. Source
scores and distances are preserved in `_sources`. RRF is useful when lexical and
vector scores live on different scales:

```text
score = sum(1 / (rrf_k + rank)) for every source that returned the document
```

A higher `_score` is better. Ties are ordered by `_id`.

### Python

```python
from nonodb import NonoDB

products = NonoDB().collection("products")
results = products.search_all(
    mode="hybrid",
    query="coffee grinder",
    fields=["title", "description"],
    vector=[0.12, -0.03, 0.44],
    vector_field="embedding",
    filter={"status": "active"},
    limit=20,
)
```

### Node.js

```javascript
const results = await products.searchAll({
  mode: 'hybrid',
  query: 'coffee grinder',
  fields: ['title', 'description'],
  vector: [0.12, -0.03, 0.44],
  vectorField: 'embedding',
  filter: { status: 'active' },
  limit: 20,
});
```

### Rust

```rust
let results = products.search_all(json!({
    "mode": "hybrid",
    "query": "coffee grinder",
    "fields": ["title", "description"],
    "vector": [0.12, -0.03, 0.44],
    "vector_field": "embedding",
    "filter": {"status": "active"},
    "limit": 20
}))?;
```

`search_all_with_meta` returns the full `{results, meta}` value.

### Go

```go
results, err := products.SearchAll(ctx, nonodb.SearchAllOptions{
    Mode:        "hybrid",
    Query:       "coffee grinder",
    Fields:      []string{"title", "description"},
    Vector:      []float32{0.12, -0.03, 0.44},
    VectorField: "embedding",
    Filter:      map[string]any{"status": "active"},
    Limit:       20,
})
```

### CLI

```bash
nonodb-cli search-all products \
  --mode hybrid \
  --query 'coffee grinder' \
  --fields title,description \
  --vector '[0.12,-0.03,0.44]' \
  --vector-field embedding \
  --filter '{"status":"active"}' \
  --limit 20
```

## Atlas Search compatibility subset

The compatibility stages run inside the daemon's aggregation pipeline. They
work through the HTTP `aggregate` operation and the MongoDB wire aggregation
path, so an existing driver can send an aggregation pipeline without changing
the transport.

### `$search`

Supported operators:

| Operator | Subset |
|---|---|
| `text` | `query`, `path` (string or array), optional `fuzzy.maxEdits` |
| `phrase` | `query`, `path` |
| `equals` | `path`, `value` |
| `range` | `path`, `gt`/`gte`/`lt`/`lte` |
| `compound` | `must`, `should`, `mustNot`, `filter` |

Example:

```json
{"$search": {
  "index": "default",
  "compound": {
    "must": [
      {"text": {"query": "coffee", "path": ["title", "description"]}}
    ],
    "should": [
      {"text": {"query": "grinder", "path": "title", "fuzzy": {"maxEdits": 1}}}
    ],
    "filter": [
      {"equals": {"path": "status", "value": "active"}}
    ],
    "mustNot": [
      {"equals": {"path": "deleted", "value": true}}
    ]
  }
}}
```

Operator mapping:

- `text` without `fuzzy` → unified `lexical` mode,
- `text.fuzzy` → unified `fuzzy` mode,
- `phrase` → lexical search with a quoted phrase,
- `equals` and `range` → document filters,
- `compound.must` → intersection,
- `compound.should` → union,
- `compound.mustNot` → exclusion,
- `compound.filter` → intersection with filter matches.

`compound` can be nested. `index` is accepted and ignored; NonoDB has no
separate Atlas index definitions.

### `$searchMeta`

```json
{"$searchMeta": {
  "search": {"text": {"query": "coffee", "path": "title"}},
  "count": {"type": "total"}
}}
```

Response:

```json
{"count": {"total": 42}, "meta": {"count": {"total": 42}, "capped": false}}
```

The count is exact up to the compatibility stage cap of 1,000 documents. If
`capped` is true, the result is a lower bound. Facets are not supported.

### `$vectorSearch`

```json
{"$vectorSearch": {
  "index": "default",
  "path": "embedding",
  "queryVector": [0.12, -0.03, 0.44],
  "numCandidates": 100,
  "limit": 10,
  "filter": {"status": "active"}
}}
```

`numCandidates` must be at least `limit`. The implementation uses the exact
vector path (or the existing IVF sidecar when no filter is active), so it is a
compatibility stage rather than an Atlas ANN planner. Each result receives
`vectorSearchScore` in the range `0..1`, where higher is better. `_distance`
remains available for diagnostics.

## Atlas migration recipes

### Text search

Atlas:

```json
{"$search": {"text": {"query": "coffee", "path": "title"}}}
```

NonoDB native:

```json
{"op":"search_all","coll":"products","mode":"lexical","query":"coffee","fields":["title"]}
```

### Fuzzy text search

Atlas:

```json
{"$search": {"text": {
  "query": "cofee",
  "path": "title",
  "fuzzy": {"maxEdits": 1}
}}}
```

NonoDB native:

```json
{"op":"search_all","coll":"products","mode":"fuzzy","query":"cofee","fields":["title"],"max_distance":1}
```

### Vector search

Atlas `$vectorSearch` is translated by the compatibility stage. Native call:

```json
{"op":"search_all","coll":"products","mode":"vector","vector":[0.12,-0.03,0.44],"vector_field":"embedding","limit":10}
```

### Hybrid search

Atlas users often combine lexical and vector results in application code. In
NonoDB use `mode: "hybrid"`; RRF is handled by the daemon.

### Filters and pagination

Use normal MongoDB-style filters. `skip` and `limit` are applied after ranking
in `search_all`, or as separate aggregation stages after `$search`.

### Unsupported Atlas Search features

The following are intentionally not implemented:

- custom analyzers, tokenizers, synonyms, and thesauri,
- `autocomplete`,
- `facet` and rich `$searchMeta` facets,
- `moreLikeThis`, `queryString`, `regex`, `wildcard`, `span`,
  `embeddedDocument`,
- geo search operators,
- `count` beyond the 1,000-document compatibility cap,
- full Lucene scoring and ranking parity.

Unsupported operators return an `invalid_query` error naming the operator. They
do not silently fall back to a different query.

## Limits and operations

- Text and fuzzy indexes are one field per index.
- Fuzzy candidate evaluation is capped at 5,000 documents.
- `search_all` returns at most 1,000 results per request.
- Hybrid search uses RRF, not a learned reranker.
- Vector search requires ready vectors; no embedding generation is built in.
- Search is read-only and never runs in the writer.
- A `$search`/`$searchMeta`/`$vectorSearch` stage must be first in the pipeline.
- The compatibility stages do not accept writes; `$out` and `$merge` after
  search are outside this subset.

## Validation

```bash
NONODB_DAEMON=target/debug/nonodb-daemon \
NONODB_CLI=target/debug/nonodb-cli \
python3 scripts/search_all_test.py
```

TTL is enforced before search results are returned. Expired documents are
filtered from lexical, fuzzy, vector, hybrid, `$search`, `$searchMeta`, and
`$vectorSearch` results, and the search paths over-fetch when necessary so an
expired top hit does not consume the requested limit. This is controlled by the
daemon-wide `NONODB_TTL_STRICT_READS` setting (strict by default).

The test covers native lexical/fuzzy/vector/hybrid modes, filters, pagination,
`range`, RRF metadata, `$search`, `$searchMeta`, `$vectorSearch`, numeric
Extended JSON options over the MongoDB wire path, CLI output, and wire
aggregation when `pymongo` is installed.

A small latency smoke test over 1,000 documents:

```bash
NONODB_DAEMON=target/debug/nonodb-daemon python3 scripts/search_bench.py
```

It prints p50/p95 for lexical, vector, and hybrid search and fails if p95 exceeds
2 seconds. It is a regression guard, not a competitive benchmark.
