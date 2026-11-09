# HTTP API

nonodb exposes a single JSON endpoint for data operations, plus health and
metrics endpoints.

- Base URL over a Unix socket: `http://localhost`
- Default socket: `/run/nonodb.sock`
- Data endpoint: `POST /v1/exec`
- Health: `GET /healthz`
- Metrics: `GET /metrics`

The API is JSON over HTTP/1.1. It is **not** MongoDB wire-protocol compatible.

## Envelope

Every `/v1/exec` request is a JSON object with an `op` field. A successful
call returns:

```json
{"ok": true, "result": ...}
```

A failed call returns:

```json
{
  "ok": false,
  "error": {
    "code": "invalid_query",
    "message": "unsupported update operator $rename"
  }
}
```

The HTTP status is usually `200` for success and one of `400`, `401`, `413`,
or `500` for errors, but clients should rely on the JSON envelope.

## Authentication

Authentication is disabled by default. If the daemon has
`NONODB_AUTH_TOKEN` set, every `/v1/exec` and `/metrics` request must include:

```http
Authorization: Bearer <token>
```

`GET /healthz` is always public. TCP listener mode requires a token.

## Limits

| Limit | Value |
|---|---|
| Request body | 4 MiB (`413` when exceeded) |
| Server-side request timeout | 30 s (`timeout` error) |
| Document size | 1 MiB (`document_too_large`, HTTP 413) |
| Search/fuzzy `limit` | default 10, maximum 1000 |
| Fuzzy candidates | 5,000 per query |
| `write_batch` operations | bounded by request size |
| LMDB named databases | 256 (`MDB_DBS_FULL` when exhausted) |

The server timeout can be raised with `NONODB_REQUEST_TIMEOUT`; the Python
client timeout with `NONODB_TIMEOUT` or the `timeout=` argument.

## Operations

| `op` | Purpose |
|---|---|
| [`insert`](#insert) | Insert one document |
| [`insert_many`](#insert_many) | Insert multiple documents |
| [`find`](#find) | Query documents |
| [`find_one`](#find_one) | Return one document |
| [`update`](#update) | Update or upsert documents |
| [`delete`](#delete) | Delete documents |
| [`count`](#count) | Count matching documents |
| [`create_index`](#create_index) | Create a secondary index |
| [`drop_index`](#drop_index) | Drop a secondary index |
| [`list_indexes`](#list_indexes) | List secondary indexes |
| [`create_text_index`](#full-text-indexes) | Create a BM25 index |
| [`drop_text_index`](#full-text-indexes) | Drop a BM25 index |
| [`list_text_indexes`](#full-text-indexes) | List BM25 indexes |
| [`create_fuzzy_index`](#fuzzy-indexes) | Create a fuzzy index |
| [`drop_fuzzy_index`](#fuzzy-indexes) | Drop a fuzzy index |
| [`list_fuzzy_indexes`](#fuzzy-indexes) | List fuzzy indexes |
| [`search`](#search) | BM25 full-text query |
| [`fuzzy`](#fuzzy) | Approximate string query |
| [`batch`](#read-batch) | Multiple read operations, one snapshot |
| [`write_batch`](#atomic-write-batch) | Atomic group of writes |
| [`list_collections`](#list_collections) | List known collections |

---

## insert

```json
{
  "op": "insert",
  "coll": "users",
  "doc": {"name": "Ada", "age": 36}
}
```

Response:

```json
{"ok": true, "result": {"inserted_id": "4f0b5f6e-..."}}
```

- The document must be a JSON object.
- If `_id` is omitted, a UUID v4 string is generated.
- If `_id` is present it must be a string.
- A duplicate `_id` or a unique-index conflict fails with `duplicate_key`.
- `_id` is the primary key. Treat it as immutable.

## insert_many

```json
{
  "op": "insert_many",
  "coll": "users",
  "docs": [
    {"name": "Ada"},
    {"name": "Grace"}
  ]
}
```

Response:

```json
{"ok": true, "result": {"inserted_ids": ["...", "..."]}}
```

`insert_many` is a single write request, but it is not the API for guaranteed
all-or-nothing groups. Use [`write_batch`](#atomic-write-batch) when a group of
inserts must commit or roll back together.

Requests are limited to 4 MiB. For large imports, send chunks of roughly
500–1,000 documents and keep each request below the limit.

## find

```json
{
  "op": "find",
  "coll": "users",
  "filter": {"age": {"$gte": 18}},
  "projection": {"name": 1, "age": 1, "_id": 0},
  "sort": [["age", -1], ["name", 1]],
  "skip": 0,
  "limit": 20
}
```

Response: a JSON array of documents.

Fields:

| Field | Default | Meaning |
|---|---|---|
| `filter` | `{}` | Query predicate; missing/null/empty means match all |
| `projection` | none | Top-level include or exclude projection |
| `sort` | none | Array of `[field, direction]`; `1` ascending, `-1` descending |
| `skip` | `0` | Number of matching documents to skip |
| `limit` | none | Maximum number of results; unlimited when omitted |

Behavior:

- Without `sort`, results follow the cheapest scan order (index order or
  `_id` order), and execution stops after `skip + limit` matches.
- With `sort`, results are ordered by the requested fields. Missing values and
  `null` sort last. Ties are broken deterministically by `_id`.
- An index can serve the sort when its full field order matches the requested
  sort and all directions are the same. Otherwise a full scan and top-K sort
  are used.
- Nested paths use dots: `{"user.address.city": "London"}`.
- Projection is top-level only. It is either inclusive or exclusive; `_id` is
  included by default and can be disabled with `"_id": 0`.

## find_one

```json
{
  "op": "find_one",
  "coll": "users",
  "filter": {"email": "ada@example.com"},
  "projection": {"name": 1}
}
```

Response: the document or `null`.

`find_one` is `find` with `limit=1` and no sort. It stops as soon as a match is
found. An exact `_id` filter uses a direct primary-key lookup.

## update

```json
{
  "op": "update",
  "coll": "users",
  "filter": {"_id": "..."},
  "update": {"$set": {"active": true}, "$inc": {"logins": 1}},
  "multi": false,
  "upsert": false
}
```

Response:

```json
{
  "ok": true,
  "result": {
    "matched": 1,
    "modified": 1,
    "upserted_id": null
  }
}
```

| Field | Default | Meaning |
|---|---|---|
| `multi` | `false` | Update every match instead of only the first |
| `upsert` | `false` | Insert a new document when nothing matches |

### Update forms

An update is either:

1. an **operator object** — every top-level key starts with `$`; or
2. a **replacement document** — no top-level keys start with `$`.

Mixing operators and plain fields is an error. A replacement document keeps the
existing `_id`; the `_id` in the replacement (if any) does not replace the
stored key.

### Supported operators

| Operator | Behavior |
|---|---|
| `$set` | Set one or more paths; creates intermediate objects |
| `$unset` | Remove one or more paths; no error if absent |
| `$inc` | Add a number; missing field is treated as 0 |
| `$push` | Append one value or `{"$each": [...]}` to an array |
| `$pull` | Remove array elements exactly equal to the given value |
| `$addToSet` | Append values that are not already present; supports `$each` |
| `$setOnInsert` | Apply only when an upsert inserts a new document |

Examples:

```json
{"$set": {"profile.theme": "dark"}}
{"$inc": {"counters.requests": 1}}
{"$push": {"tags": "rust"}}
{"$push": {"ports": {"$each": [25, 587]}}}
{"$addToSet": {"roles": {"$each": ["admin", "editor", "admin"]}}}
{"$pull": {"tags": "deprecated"}}
```

Notes:

- `$push` and `$addToSet` replace a non-array existing value with a new array.
  This is destructive; ensure the field is intended to be an array.
- `$addToSet` uses the same equality semantics as filters. Numbers compare by
  value (`1 == 1.0`), and objects compare structurally.
- `$pull` removes exact matching values only. It does not support query
  predicates inside the array.
- Nested paths (`a.b.c`) work with `$set`, `$unset`, `$inc`, `$push`, `$pull`,
  and `$addToSet`.
- `_id` is immutable. Do not use `$set`/`$unset`/array operators on `_id`.
- Unsupported operators such as `$rename`, `$min`, `$max`, `$mul`,
  `$currentDate`, positional operators, and array filters return
  `invalid_query`.

### Upsert

When `upsert=true` and no document matches:

1. Equality conditions from the filter become the seed document.
   Conditions inside `$and` and `{"field": {"$eq": value}}` are included.
   Range conditions and other operators are ignored.
2. Operators are applied. `$setOnInsert` runs only for this insert.
3. A filter `_id` is preserved.
4. The new document is inserted and its id is returned in `upserted_id`.

For a replacement upsert, equality fields from the filter are merged into the
replacement document unless the replacement already defines them.

Example:

```json
{
  "op": "update",
  "coll": "settings",
  "filter": {"tenant": "acme", "key": "smtp"},
  "update": {
    "$set": {"host": "smtp.example.com"},
    "$setOnInsert": {"created_at": "2026-11-09T00:00:00Z"}
  },
  "upsert": true
}
```

Result when inserting:

```json
{"matched": 0, "modified": 0, "upserted_id": "..."}
```

The inserted document contains `tenant`, `key`, `host`, and `created_at`.

## delete

```json
{
  "op": "delete",
  "coll": "sessions",
  "filter": {"expires_at": {"$lt": "2026-01-01T00:00:00Z"}},
  "multi": true
}
```

Response:

```json
{"ok": true, "result": {"deleted": 42}}
```

`multi` defaults to `false`, which deletes only the first match. Deletion
removes secondary, text, and fuzzy index entries in the same write transaction.

## count

```json
{"op": "count", "coll": "users", "filter": {"active": true}}
```

Response:

```json
{"ok": true, "result": {"count": 120}}
```

Optimizations:

- `filter: {}` / `null` uses LMDB's entry count in O(1).
- A filter exactly matching the fields of an index can be counted from the
  index range without reading documents.
- Other filters perform a full scan, but `count` does not materialize results.

## Filters

A filter is a JSON object. Multiple fields are combined with AND. An empty
object, `null`, or an omitted filter matches every document.

### Field conditions

| Condition | Meaning |
|---|---|
| `{"a": 1}` | Equality |
| `{"a": null}` | Field is `null` **or missing** |
| `{"a": {"$eq": 1}}` | Equality |
| `{"a": {"$ne": 1}}` | Not equal |
| `{"a": {"$gt": 1}}` | Greater than |
| `{"a": {"$gte": 1}}` | Greater than or equal |
| `{"a": {"$lt": 1}}` | Less than |
| `{"a": {"$lte": 1}}` | Less than or equal |
| `{"a": {"$in": [1, 2]}}` | Equal to one of the values |
| `{"a": {"$nin": [1, 2]}}` | Equal to none of the values |
| `{"a": {"$exists": true}}` | Field is present |
| `{"a": {"$not": {...}}}` | Negate a field condition |

### Logical operators

| Operator | Meaning |
|---|---|
| `{"$and": [f1, f2]}` | All filters match |
| `{"$or": [f1, f2]}` | At least one filter matches |
| `{"$nor": [f1, f2]}` | None of the filters match |
| `{"$not": f}` | Negate a filter (top level) |

### Semantics

- **Numbers compare by value.** `30` equals `30.0`, including in ranges.
- **Type-aware ranges.** A numeric `$gt` does not match strings or booleans.
- **Missing fields are `null` for equality.** `{"missing": null}` matches absent
  fields; `$exists` distinguishes present `null` from absent.
- **Array membership.** `{"tags": "rust"}` matches a document where `tags` is
  `["rust", "lmdb"]`.
- **Nested paths.** Dots traverse nested objects: `{"a.b.c": 7}`.
- **No regex, `$size`, `$elemMatch`, `$type`, `$where`, or `$text` operator.**
  Full-text search is a separate operation.

## Projection

Projection is a top-level include/exclude map:

```json
{"name": 1, "email": 1, "_id": 0}
```

- If any non-`_id` field has a truthy value, the projection is inclusive.
- Otherwise it is exclusive; listed fields are removed.
- `_id` is included by default. `"_id": 0` excludes it.
- Nested projections are not supported.
- Covering indexes can satisfy an inclusive projection using indexed and
  `include`d fields without fetching the document.

## Sorting

`sort` is an array of pairs:

```json
"sort": [["priority", -1], ["created_at", 1]]
```

Directions: `1` ascending, `-1` descending. Missing values and `null` sort
last. Equal keys are ordered deterministically by `_id`.

An index can serve the sort only when the requested sort matches the full index
field order and all directions are the same. Otherwise nonodb performs a scan
and bounded top-K sort. Full-collection sorts are parallelized for shallow
queries.

---

## create_index

Single-field index:

```json
{"op": "create_index", "coll": "users", "field": "email", "unique": true}
```

Compound index with covering fields:

```json
{
  "op": "create_index",
  "coll": "events",
  "fields": ["tenant", "kind", "created_at"],
  "unique": false,
  "include": ["payload", "status"]
}
```

Response:

```json
{
  "ok": true,
  "result": {
    "created": "tenant+kind+created_at",
    "fields": ["tenant", "kind", "created_at"],
    "unique": false,
    "include": ["payload", "status"]
  }
}
```

Rules:

- Provide either `field` (single) or `fields` (ordered list). `fields` is
  preferred for new code.
- The index name is the fields joined by `+`.
- Indexable values are scalars: string, number, boolean. Arrays and objects are
  skipped.
- A unique index enforces uniqueness of the full key. For compound indexes,
  that means the combination, not each field individually.
- Creating an index on an existing collection performs a synchronous backfill.
  Writers are blocked while the backfill write transaction is active.
- `include` copies extra fields into index values for covering projections.
  Included fields are not part of the index key or uniqueness constraint.
- Indexes are maintained transactionally on insert, update, and delete.

## drop_index

```json
{"op": "drop_index", "coll": "users", "field": "email"}
```

For a compound index, pass the generated name (`"grp+age"`). Response:

```json
{"ok": true, "result": {"dropped": true}}
```

`dropped` is `false` if the index did not exist. The index contents are
removed, but the named LMDB sub-database remains registered.

## list_indexes

```json
{"op": "list_indexes", "coll": "users"}
```

Response:

```json
{
  "ok": true,
  "result": {
    "indexes": [
      {
        "field": "email",
        "fields": ["email"],
        "unique": true,
        "include": []
      }
    ]
  }
}
```

## Full-text indexes

Create, drop, and list:

```json
{"op": "create_text_index", "coll": "articles", "field": "body"}
{"op": "drop_text_index", "coll": "articles", "field": "body"}
{"op": "list_text_indexes", "coll": "articles"}
```

Responses:

```json
{"ok": true, "result": {"created": "body", "kind": "text"}}
{"ok": true, "result": {"dropped": true, "kind": "text"}}
{"ok": true, "result": {"text_indexes": ["body"]}}
```

The index is maintained in the same transaction as document writes. Creation
backfills existing documents synchronously.

## Fuzzy indexes

```json
{"op": "create_fuzzy_index", "coll": "articles", "field": "title"}
{"op": "drop_fuzzy_index", "coll": "articles", "field": "title"}
{"op": "list_fuzzy_indexes", "coll": "articles"}
```

Responses mirror text indexes:

```json
{"ok": true, "result": {"created": "title", "kind": "fuzzy"}}
{"ok": true, "result": {"fuzzy_indexes": ["title"]}}
```

## search

```json
{
  "op": "search",
  "coll": "articles",
  "field": "body",
  "query": "rust async \"zero copy\"",
  "filter": {"published": true},
  "skip": 0,
  "limit": 10
}
```

Response:

```json
{
  "ok": true,
  "result": {
    "results": [
      {"_id": "...", "body": "...", "_score": 4.82}
    ]
  }
}
```

Semantics:

- The text index must exist; otherwise `invalid_query`.
- Terms are ORed. BM25 ranks documents by relevance.
- Duplicate terms in the query are removed.
- Quoted phrases must occur as contiguous token sequences.
- `filter` is a residual document filter applied before pagination.
- `skip` and `limit` apply after filtering and phrase checks.
- `limit` defaults to 10 and is capped at 1,000.
- `_score` is added to each result. Higher is better.
- Tokenization lowercases and folds diacritics. `zazolc` matches `zażółć`.

## fuzzy

```json
{
  "op": "fuzzy",
  "coll": "articles",
  "field": "title",
  "query": "postgress",
  "max_distance": 2,
  "filter": {"published": true},
  "limit": 10
}
```

Response:

```json
{
  "ok": true,
  "result": {
    "results": [
      {"_id": "...", "title": "PostgreSQL", "_distance": 1}
    ]
  }
}
```

Semantics:

- The fuzzy index must exist; otherwise `invalid_query`.
- Candidate generation uses shared trigrams; final ranking uses bounded
  Levenshtein distance.
- Distance is measured against the whole normalized field value or the closest
  token.
- `max_distance` defaults to 2.
- `_distance` is added to each result; lower is better.
- There is no `skip` parameter for fuzzy search.
- `limit` defaults to 10 and is capped at 1,000.
- At most 5,000 candidates are scored per query.
- Only scalar field values are indexed.

## Read batch

```json
{
  "op": "batch",
  "ops": [
    {"op": "find_one", "coll": "users", "filter": {"_id": "..."}},
    {"op": "count", "coll": "users", "filter": {"active": true}},
    {"op": "list_indexes", "coll": "users"}
  ]
}
```

Response:

```json
{
  "ok": true,
  "result": {
    "results": [
      {"ok": true, "result": {"_id": "...", "name": "Ada"}},
      {"ok": true, "result": {"count": 12}},
      {"ok": true, "result": {"indexes": []}}
    ]
  }
}
```

Rules:

- Allowed operations: `find`, `find_one`, `count`, `list_indexes`,
  `list_text_indexes`, `list_fuzzy_indexes`, `list_collections`, `search`,
  `fuzzy`.
- Every operation executes against **one** `RoTxn`, so the batch sees a single
  consistent snapshot.
- An error in one operation does not stop the others. Each result has its own
  `ok` and either `result` or `error`.
- A write operation is rejected per-operation with `invalid_query` and the
  message `only read ops are allowed in batch`.
- There is no transaction or snapshot shared with later requests.

## Atomic write batch

```json
{
  "op": "write_batch",
  "ops": [
    {"op": "insert", "coll": "users", "doc": {"_id": "u1", "name": "Ada"}},
    {"op": "update", "coll": "counters", "filter": {"_id": "hits"},
     "update": {"$inc": {"n": 1}}},
    {"op": "delete", "coll": "sessions", "filter": {"_id": "s1"}}
  ]
}
```

Response:

```json
{
  "ok": true,
  "result": {
    "results": [
      {"inserted_id": "u1"},
      {"matched": 1, "modified": 1, "upserted_id": null},
      {"deleted": 1}
    ]
  }
}
```

Rules:

- Allowed operations: `insert`, `insert_many`, `update`, `delete`.
- `update` supports `multi` and `upsert` inside the batch.
- The whole batch is executed in one LMDB write transaction. If any operation
  fails, the transaction is rolled back and no operation in the batch is
  visible.
- The error message identifies the failing operation by index:
  `write_batch operation 1 failed: ...`.
- `write_batch` is isolated from group commit. It does not share a transaction
  with unrelated requests, because its rollback must not affect them.
- Operations execute in order, and later operations in the same batch see
  earlier uncommitted changes. Use a read batch for independent reads.

## list_collections

```json
{"op": "list_collections"}
```

Response:

```json
{"ok": true, "result": {"collections": ["users", "settings"]}}
```

Collections are created on first write.

---

## Health and metrics

### GET /healthz

Public, no authentication.

```json
{"ok": true}
```

### GET /metrics

Requires the bearer token when authentication is enabled.

```json
{
  "requests_total": 12345,
  "errors_total": 2,
  "read_ops_total": 10000,
  "batch_ops_total": 3000,
  "write_ops_total": 2000,
  "write_batches_total": 250,
  "queue_len": 0,
  "queue_peak": 80,
  "commit_ns_total": 182000000,
  "avg_batch_size": 8.0,
  "avg_commit_us": 728.0
}
```

| Metric | Meaning |
|---|---|
| `requests_total` | HTTP `/v1/exec` requests |
| `errors_total` | Error responses |
| `read_ops_total` | Read operations handled |
| `batch_ops_total` | Operations inside read batches |
| `write_ops_total` | Write operations executed |
| `write_batches_total` | Write transactions committed |
| `queue_len` | Current writer queue length |
| `queue_peak` | Peak writer queue length since start |
| `commit_ns_total` | Cumulative commit time in nanoseconds |
| `avg_batch_size` | Write ops per committed write transaction |
| `avg_commit_us` | Average commit time in microseconds |

## Error codes

| Code | HTTP | Meaning |
|---|---|---|
| `invalid_query` | 400 | Invalid filter/update/index/query shape, write in a read batch, unsupported operator |
| `invalid_json` | 400 | Malformed JSON document or value |
| `invalid_collection` | 400 | Invalid collection name |
| `invalid_id` | 400 | `_id` is not a string |
| `duplicate_key` | 400 | Duplicate `_id` or unique-index conflict |
| `document_too_large` | 413 | Document exceeds 1 MiB |
| `unauthorized` | 401 | Missing/invalid bearer token |
| `timeout` | 500 | Request exceeded the server timeout |
| `storage` | 500 | LMDB/storage error |
| `io` | 500 | Filesystem or I/O error |
| `internal` | 500 | Internal daemon error |

## curl examples

```bash
S=/tmp/nonodb.sock

# insert
curl -s --unix-socket "$S" -H 'content-type: application/json' \
  -X POST http://localhost/v1/exec \
  -d '{"op":"insert","coll":"users","doc":{"name":"Ada","age":36}}'

# indexed range query
curl -s --unix-socket "$S" -H 'content-type: application/json' \
  -X POST http://localhost/v1/exec \
  -d '{"op":"find","coll":"users","filter":{"age":{"$gte":18,"$lt":65}},
       "sort":[["age",1]],"limit":10}'

# atomic increment
curl -s --unix-socket "$S" -H 'content-type: application/json' \
  -X POST http://localhost/v1/exec \
  -d '{"op":"update","coll":"counters","filter":{"_id":"hits"},
       "update":{"$inc":{"n":1}}}'

# full-text search
curl -s --unix-socket "$S" -H 'content-type: application/json' \
  -X POST http://localhost/v1/exec \
  -d '{"op":"search","coll":"articles","field":"body",
       "query":"rust async","limit":5}'

# metrics
curl -s --unix-socket "$S" http://localhost/metrics
```

See the [Python client](python-client.md) for a higher-level interface and the
[Operations](operations.md) guide for deployment and security.
