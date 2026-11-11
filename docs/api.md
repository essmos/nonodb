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

The HTTP status is usually `200` for success and one of `400`, `401`, `403`,
`413`, `429`, or `500` for errors, but clients should rely on the JSON envelope.

## Authentication

The Unix socket is a trusted local channel and uses the `local` principal with
full access.

TCP always requires `NONODB_AUTH_FILE`. Every `/v1/exec` and `/metrics` request
must use either a registered bearer token:

```http
Authorization: Bearer nodb_<kid>_<secret>
```

or a registered client certificate (mTLS). If both are supplied, they must
resolve to the same principal.

`GET /healthz` is public. `/metrics` requires `admin:db`. Authorization is
deny-by-default and checked per collection; `write_batch` is checked per
operation. Missing credentials return `401`; insufficient scope returns `403` with error
code `forbidden`. Repeated failed authentication can lock the peer IP and return
`429` with error code `rate_limited`.

See [Authentication and authorization](auth.md) for the registry format,
scopes, CLI management, mTLS mapping, reload behavior, and audit logging.

## Vault headers

Requests against a vault collection must also send all three headers:

```http
X-NonoDB-Tenant: acme
X-NonoDB-Key-Id: acme:v1
X-NonoDB-Key: <base64-32-byte-key>
```

The key is sent only in the header, never in the JSON body. Vault collections
support only a restricted subset of operations. See
[Vault collections](vault.md) for the full contract.

## Limits

| Limit | Value |
|---|---|
| Request body | 4 MiB (`413` when exceeded) |
| Server-side request timeout | 30 s (`timeout` error) |
| Document size | 1 MiB (`document_too_large`, HTTP 413) |
| Search/fuzzy/vector `limit` | default 10, maximum 1000 |
| `lookup` values | maximum 10,000 |
| `lookup` result budget | 64 MiB (`result_too_large`) |
| Fuzzy candidates | 5,000 per query |
| Vector dimensions | 1–65,536 |
| `write_batch` operations | bounded by request size |
| LMDB named databases | 1024 (`MDB_DBS_FULL` when exhausted) |

The server timeout can be raised with `NONODB_REQUEST_TIMEOUT`; the Python
client timeout with `NONODB_TIMEOUT` or the `timeout=` argument.

## Operations

| `op` | Purpose |
|---|---|
| [`insert`](#insert) | Insert one document |
| [`insert_many`](#insert_many) | Insert multiple documents |
| [`find`](#find) | Query documents |
| [`find_one`](#find_one) | Return one document |
| [`lookup`](#lookup) | Ordered point lookups by `_id` or a unique index |
| [`watch`](#watch) | Long-poll append-only events by sortable `_id` |
| [`explain`](#explain) | Show the selected plan, bounds, and actual counts |
| [`update`](#update) | Update or upsert documents |
| [`find_and_modify`](#find_and_modify) | Atomically update/remove one document and return it |
| [`delete`](#delete) | Delete documents |
| [`count`](#count) | Count matching documents |
| [`aggregate`](#aggregation) | Read-only aggregation pipeline |
| [`sql`](#sql) | Read-only SQL `SELECT` over one collection |
| [`explain_sql`](#explain_sql) | Show the plan for a SQL `SELECT` |
| [`create_index`](#create_index) | Create a secondary index |
| [`create_indexes`](#create_indexes) | Create several indexes in one scan |
| [`drop_index`](#drop_index) | Drop a secondary index |
| [`list_indexes`](#list_indexes) | List secondary indexes |
| [`create_text_index`](#full-text-indexes) | Create a BM25 index |
| [`drop_text_index`](#full-text-indexes) | Drop a BM25 index |
| [`list_text_indexes`](#full-text-indexes) | List BM25 indexes |
| [`create_fuzzy_index`](#fuzzy-indexes) | Create a fuzzy index |
| [`drop_fuzzy_index`](#fuzzy-indexes) | Drop a fuzzy index |
| [`list_fuzzy_indexes`](#fuzzy-indexes) | List fuzzy indexes |
| [`create_vector_index`](#vector-indexes) | Create an exact vector index |
| [`drop_vector_index`](#vector-indexes) | Drop a vector index |
| [`list_vector_indexes`](#vector-indexes) | List vector indexes |
| [`search`](#search) | BM25 full-text query |
| [`fuzzy`](#fuzzy) | Approximate string query |
| [`vector_search`](#vector-search) | Exact top-K vector query |
| [`batch`](#read-batch) | Multiple read operations, one snapshot |
| [`write_batch`](#atomic-write-batch) | Atomic group of writes |
| [`history`](#document-history) | List revisions of one document |
| [`get_version`](#document-history) | Read one exact document revision |
| [`list_collections`](#list_collections) | List known collections |
| [`create_collection`](#create_collection) | Create a standard or vault collection |
| [`set_collection_ttl`](#document-ttl) | Set or clear the default document TTL |
| [`get_collection_ttl`](#document-ttl) | Read the default document TTL |
| [`map_info`](#lmdb-map-size) | Read map size and high-water mark |
| [`resize_map`](#lmdb-map-size) | Grow the LMDB map online |

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
- The reserved top-level `_ttl` field sets a per-document expiry in seconds.
  `_ttl: 0` or `null` disables the collection default. The field is removed
  before storage. See [Document TTL](#document-ttl).
- Extended JSON `{"$date": ...}` values are normalized to canonical UTC
  milliseconds before storage. See [Dates and time zones](dates.md).
- Extended JSON numeric objects are validated and stored in canonical form:
  `$numberInt`, `$numberLong`, `$numberDouble`, and `$numberDecimal`.
  See [Numbers and decimals](numerics.md).
- `{"$binary": ...}` / `{"$uuid": ...}` and `{"$oid": ...}` are normalized to
  canonical Extended JSON. See [BSON scalar types](bson-types.md).

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
- Equality or range conditions on `_id` use the primary-key B+ tree directly.
  Sorting by a single `_id` field can stream that range forwards or backwards.
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

## lookup

Ordered point lookups by `_id` or a unique index. This is the efficient batch
alternative to N separate `find_one` calls.

```json
{
  "op": "lookup",
  "coll": "users",
  "by": "email",
  "values": ["ada@example.com", "missing@example.com", "grace@example.com"],
  "fields": ["name", "status"]
}
```

Response:

```json
{
  "ok": true,
  "result": {
    "results": [
      {"_id": "u1", "name": "Ada", "status": "active"},
      null,
      {"_id": "u2", "name": "Grace", "status": "inactive"}
    ],
    "found": 2
  }
}
```

Semantics:

- `by` defaults to `_id`.
- A non-`_id` lookup requires a **unique index** whose field order exactly
  matches `by`; otherwise the request returns `invalid_query`.
- `by` may be an array for a compound unique index. Each input value must then
  be an array with the same number of elements, in the same order:

  ```json
  {
    "op": "lookup",
    "coll": "users",
    "by": ["tenant_id", "email"],
    "values": [
      ["acme", "ada@example.com"],
      ["other", "ada@example.com"],
      ["acme", "missing@example.com"]
    ]
  }
  ```

  Compound values with the wrong shape return `null`.
- Results are returned in input order. Missing values and values that cannot be
  encoded return `null`; `found` is the number of non-null results.
- Duplicate input values produce duplicate result entries. Repeated keys are
  looked up only once internally.
- `values` is limited to 10,000 entries per request.
- `fields` is a top-level inclusive projection. If every requested field is
  available in a covering index created with `include`, the document is not
  read from the main collection. Otherwise the normal document fetch is used.
- `projection` accepts a normal flat projection and allows excluding `_id`:

  ```json
  {
    "op": "lookup",
    "coll": "users",
    "by": "email",
    "values": ["ada@example.com"],
    "projection": {"name": 1, "_id": 0}
  }
  ```

  `fields` and `projection` are mutually exclusive. A covering index is used
  only for an inclusive `projection` (or `fields`).
- `_id` is included by default when using `fields`.
- The result budget is 64 MiB, measured from stored document/index bytes before
  JSON encoding. Larger responses return `result_too_large`; request fewer keys
  or use a smaller projection.
- `lookup` does not accept `filter`, `sort`, `skip`, or `limit`. Use `find` for
  arbitrary predicates.
- Non-unique lookups and lookups without a matching index are not supported yet.
- Vault collections reject `lookup`.

## watch

Long-polls an append-only event collection by sortable `_id`. Use it for outbox
event collections such as `_events`; it is not an automatic change stream for
arbitrary updates and deletes.

```json
{
  "op": "watch",
  "coll": "_events",
  "after": "01J...",
  "limit": 100,
  "timeout_ms": 1000,
  "types": ["order.paid"],
  "filter": {"payload.total": {"$gte": 1000}},
  "projection": {"type": 1, "payload": 1}
}
```

Response:

```json
{
  "events": [
    {"_id": "01J...", "type": "order.paid", "payload": {"total": 1200}}
  ],
  "next": "01J...",
  "timed_out": false,
  "has_more": false,
  "truncated": false,
  "oldest": null
}
```

- `after` is optional. Omit it to start from the beginning of the collection.
- `limit` defaults to `100`; values are clamped to `1..=1000`.
- `timeout_ms` defaults to `1000`; values are clamped to `0..=25000`.
- `types` filters event documents by their `type` field.
- `filter` applies an additional MongoDB-style filter.
- `projection` is optional; `_id` is always retained for the resume token.
- `next` is the last returned `_id`, or `after` when no event arrived.
- `timed_out` is true only when the timeout expired with no events.
- `has_more` is true when another page is immediately available.
- `truncated` is true when `after` is older than retained events; `oldest` is the
  oldest event id still available.
- `watch` requires read scope on `coll`, is not cacheable, and is rejected inside
  a read `batch`. Vault collections are rejected.

### Continuous stream

`POST /v1/watch` accepts the same body and streams events continuously. The
default response is `application/x-ndjson`; `Accept: text/event-stream` selects
SSE. Optional fields:

- `heartbeat_ms` — keepalive interval, default `15,000`, range `100..60,000`;
- `timeout_ms` — maximum stream lifetime; absent or `0` means no limit;
- `overlap_ms` — late-commit rescan window, default `2,000`, max `60,000`.

NDJSON starts with `{"stream":{"truncated":false,"oldest":null}}`, then sends one
event per line and periodic `{"heartbeat":true,"ts_ms":...}` frames. SSE emits
`ready`, `message`, `heartbeat`, and `error` events. See
[Streaming outbox events](streams.md) for full client examples, reconnect
handling, and checkpoint guidance.

## explain

Returns the access path chosen for a `find`-shaped query, index and bounds
information, and exact document counts.

```json
{
  "op": "explain",
  "coll": "users",
  "filter": {"age": {"$gte": 18}},
  "sort": [["age", 1]],
  "skip": 0,
  "limit": 20
}
```

Example result:

```json
{
  "collection": "users",
  "access_path": "index_scan",
  "index_used": true,
  "index": {"fields": ["age"], "unique": false, "include": []},
  "bounds": {
    "lower": {"value": "06...", "inclusive": true},
    "upper": null
  },
  "candidates": 42,
  "estimated_documents": 42,
  "actual_matched": 40,
  "actual_returned": 20,
  "sort_covered": true,
  "filter": {"age": {"$gte": 18}},
  "sort": [["age", 1]],
  "skip": 0,
  "limit": 20
}
```

Fields:

| Field | Meaning |
|---|---|
| `access_path` | `id_lookup`, `id_range`, `index_scan`, or `collection_scan` |
| `index_used` | Whether a primary/secondary index supplies candidates |
| `index` | Index fields, uniqueness, and included fields (`null` for scans) |
| `bounds` | Encoded lower/upper index keys; `_id` bounds are literal strings |
| `candidates` | Index entries or documents considered before filtering |
| `estimated_documents` | Same candidate count, provided as an estimate field |
| `actual_matched` | Documents matching the filter |
| `actual_returned` | Documents returned after `skip`/`limit` |
| `sort_covered` | Whether the access path already provides the requested order |

`explain` executes the query to produce exact `actual_*` counts. Use a small
`limit` when explaining a broad query. It is read-only and does not create or
modify indexes. Vault collections do not support `explain`.

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
| `$push` | Append one value or `{"$each": [...]}`; supports `$position`, `$slice`, `$sort` |
| `$pull` | Remove array elements matching a value or query predicate |
| `$pullAll` | Remove all occurrences of the listed values |
| `$addToSet` | Append values that are not already present; supports `$each` |
| `$setOnInsert` | Apply only when an upsert inserts a new document |

Examples:

```json
{"$set": {"profile.theme": "dark"}}
{"$inc": {"counters.requests": 1}}
{"$push": {"tags": "rust"}}
{"$push": {"ports": {"$each": [25, 587]}}}
{"$push": {"top": {"$each": [9, 3, 7], "$sort": 1, "$slice": 2}}}
{"$push": {"queue": {"$each": ["high"], "$position": 0}}}
{"$addToSet": {"roles": {"$each": ["admin", "editor", "admin"]}}}
{"$pull": {"tags": "deprecated"}}
{"$pull": {"scores": {"$gte": 90}}}
{"$pullAll": {"tags": ["draft", "obsolete"]}}
```

Array paths support numeric indexes and positional updates:

```json
{"$set": {"items.0.qty": 5}}
{"$set": {"items.$.qty": 5}}
{"$inc": {"items.$[].qty": 1}}
{"$set": {"items.$[i].checked": true}}
```

The positional `$` uses the first array element matching the update filter.
Use `$elemMatch` on the array field so the filter can select an element:

```json
filter: {"items": {"$elemMatch": {"sku": "y"}}}
update: {"$set": {"items.$.qty": 5}}
```

`$[identifier]` requires a matching entry in `arrayFilters`. Via the Python
client:

```python
coll.update(
    {"_id": "a"},
    {"$set": {"items.$[i].checked": True}},
    array_filters=[{"i.sku": "x", "i.qty": {"$lt": 5}}],
)
```

Raw protocol updates accept an optional `array_filters` array. Each entry is
an object using exactly one identifier; keys are either the identifier itself
or `identifier.path`. Malformed placeholders, missing identifiers, mixed
identifiers, and invalid `$push` modifiers return `invalid_query` before any
write.

Notes:

- `$push` and `$addToSet` replace a non-array existing value with a new array.
  This is destructive; ensure the field is intended to be an array.
- `$addToSet` uses the same equality semantics as filters. Numbers compare by
  value (`1 == 1.0`), and objects compare structurally.
- `$pull` accepts either a value or a query predicate (for example `$gte`,
  `$in`, or a field condition for array elements that are objects).
- `$push` modifier ordering follows MongoDB: insert at `$position`, then apply
  `$sort`, then apply `$slice` (positive keeps the first N, negative keeps the
  last N, zero empties the array).
- Nested paths (`a.b.c`), numeric array indexes (`a.0.b`), positional `$`,
  `$[]`, and `$[identifier]` work with the update operators above. Assigning
  beyond the end of an array pads it with `null`; `$unset` on an array index
  sets that element to `null`.
- `_id` is immutable. Do not use `$set`/`$unset`/array operators on `_id`.
- Unsupported operators such as `$rename`, `$min`, `$max`, `$mul`,
  `$currentDate`, and pipeline updates return `invalid_query`.

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

## find_and_modify

Atomically selects one document, updates or removes it, and returns its old or
new image in the same write transaction.

```json
{
  "op": "find_and_modify",
  "coll": "jobs",
  "filter": {"state": "queued"},
  "update": {"$set": {"state": "running"}, "$inc": {"attempts": 1}},
  "sort": [["priority", -1], ["_id", 1]],
  "new": true,
  "array_filters": [{"i.ready": true}],
  "projection": {"_id": 1, "state": 1, "attempts": 1}
}
```

Response:

```json
{
  "value": {"_id": "job-1", "state": "running", "attempts": 1},
  "matched": 1,
  "modified": 1,
  "upserted_id": null
}
```

Fields:

| Field | Behavior |
|---|---|
| `filter` | Selects the candidate document |
| `update` | Update document; required unless `remove=true` |
| `sort` | Chooses the first matching document by sort order |
| `new` | Return the post-image instead of the pre-image |
| `upsert` | Insert a filter-seeded document when nothing matches |
| `remove` | Remove the selected document instead of updating it |
| `array_filters` | MongoDB-style filters for `$[identifier]` update paths |
| `projection` | Flat top-level projection applied to the returned image |

With `upsert=true` and no match, the default (`new=false`) response has
`"value": null`; with `new=true` it returns the inserted document. For an
insert, `matched` and `modified` are `0` and `upserted_id` is set.

`remove=true` cannot be combined with `update`, `upsert`, or `new`. Invalid
combinations and malformed array filters return `invalid_query` without
changing the document.

Vault collections support the same operation with their normal restriction:
the filter must identify exactly one `_id` and projection is not supported.

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
removes secondary, text, fuzzy, and vector index entries in the same write transaction.

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

## aggregate

```json
{
  "op": "aggregate",
  "coll": "orders",
  "pipeline": [
    {"$match": {"status": "paid"}},
    {"$group": {
      "_id": "$tenant",
      "total": {"$sum": "$amount"},
      "n": {"$sum": 1}
    }},
    {"$sort": {"total": -1}},
    {"$limit": 10}
  ]
}
```

Response:

```json
{
  "ok": true,
  "result": {
    "results": [
      {"_id": "acme", "total": 4200, "n": 37}
    ]
  }
}
```

Aggregation is **read-only**. It runs in one LMDB read snapshot and is allowed
in the read-only `batch` operation. A missing collection produces an empty
`results` array.

### Supported stages

| Stage | Description |
|---|---|
| `$match` | Filter documents with the same filter language as `find` |
| `$project` | Include/exclude fields, rename fields, or compute expressions |
| `$addFields` / `$set` | Add or replace fields with expression results |
| `$unset` | Remove one field or an array of fields |
| `$unwind` | Expand an array into one document per element |
| `$group` | Group by an expression and compute accumulators |
| `$sort` | Sort by one or more fields |
| `$skip` | Skip documents |
| `$limit` | Limit documents |
| `$count` | Replace the stream with one document containing the row count |

A leading `$match` is pushed into the normal read path, so indexes and `_id`
ranges can be used before the remaining pipeline runs. The daemon also pushes
a contiguous prefix of `$sort`+`$skip`/`$limit`, simple `$project`, and `$skip`
into the query planner when stage order makes that safe. A lone `$count` after
`$match` is executed as a `count` instead of materializing documents.
`$sort` without a following `$limit` stays in the in-memory path, where the
plain sort is cheaper than a full-size TopK heap.

### Expressions

Field references use `"$field"` or `"$nested.path"`. `$$ROOT` / `$$CURRENT`
refer to the current document. Literals can be wrapped in `{"$literal": ...}`.

Supported operators:

- `$ifNull`, `$cond`
- `$add`, `$subtract`, `$multiply`, `$divide`
- `$concat`
- `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`

Projection follows MongoDB-style include/exclude rules. `{"_id": 0}` removes
`_id`; `{"alias": "$field"}` renames a field; `{"total": {"$sum": ...}}` is not
valid in `$project` (use `$group` for accumulators).

### Group accumulators

`$group` requires `_id` and accepts one accumulator per output field:

```json
{
  "$group": {
    "_id": "$tenant",
    "total": {"$sum": "$amount"},
    "average": {"$avg": "$amount"},
    "first": {"$first": "$created_at"},
    "last": {"$last": "$created_at"},
    "min": {"$min": "$amount"},
    "max": {"$max": "$amount"},
    "all_ids": {"$push": "$_id"},
    "unique_ids": {"$addToSet": "$_id"},
    "rows": {"$count": {}}
  }
}
```

`$sum` and `$avg` ignore non-numeric values. `$addToSet` deduplicates by JSON
value equality.

### `$unwind`

```json
{"$unwind": "$tags"}
{"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": true, "includeArrayIndex": "tag_index"}}
```

### `$sort`

The object form is accepted:

```json
{"$sort": {"total": -1, "tenant": 1}}
```

Because JSON object key order is not guaranteed by all clients, the array form
preserves explicit multi-field order:

```json
{"$sort": [["total", -1], ["tenant", 1]]}
```

### Limits

- At most **100,000 documents** may exist in an intermediate result. The call
  returns `invalid_query` if the limit is exceeded.
- The normal request timeout (30 seconds by default) applies.
- Response size is still limited by the HTTP response path and the client's
  practical memory; use `$limit` and `$project` for large collections.
- Not supported: `$lookup`, `$graphLookup`, `$facet`, `$bucket`,
  `$bucketAuto`, `$merge`, `$out`, window functions, and arbitrary JavaScript.

## sql

Runs a read-only SQL `SELECT` over one collection. The collection name is
parsed from the statement for authorization.

```json
{
  "op": "sql",
  "query": "SELECT team, COUNT(*) AS n, SUM(score) AS total FROM users WHERE age >= $1 GROUP BY team ORDER BY total DESC LIMIT 10",
  "params": [18]
}
```

Response:

```json
{
  "ok": true,
  "result": {
    "results": [
      {"team": "core", "n": 3, "total": 60},
      {"team": "kernel", "n": 2, "total": 30}
    ]
  }
}
```

Supported clauses and limitations are documented in [SQL queries](sql.md):

- `SELECT [DISTINCT]`, `*`, aliases, `CASE`, scalar functions, and arithmetic;
- `WHERE` with comparisons, `AND`/`OR`/`NOT`, parentheses, `IN`, `BETWEEN`,
  `IS NULL`, and `LIKE`;
- `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, and `OFFSET`;
- aggregate functions `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`;
- positional parameters with `?` or `$1`;
- one collection, no joins, subqueries, or writes.

Simple field-only selects use the normal `find` path. Grouping, `DISTINCT`,
computed expressions, and `HAVING` use the aggregation pipeline. SQL is
read-only and can be used in a read `batch`. Parse and planning errors return
`invalid_sql` (HTTP 400). A missing collection returns an empty `results`
array.

## explain_sql

Returns the selected plan for a SQL `SELECT`.

```json
{
  "op": "explain_sql",
  "query": "SELECT team, COUNT(*) AS n FROM users GROUP BY team HAVING COUNT(*) > 1",
  "params": []
}
```

For simple field-only selects this is the normal `find` explain object, including
`access_path`, `index`, `bounds`, `candidates`, `actual_matched`, and
`actual_returned`. For grouped, distinct, or computed queries it returns
`access_path` (`aggregation_group`, `aggregation_distinct`, or `aggregation`)
and the generated `pipeline`.

`explain_sql` is read-only, available in read `batch`, cacheable, and requires
`read:<collection>`.

## Cursors

Cursors page large result sets through `_id` keyset pagination. They keep
server memory bounded and do not hold an LMDB read transaction between requests.
Batch size defaults to 500 and is clamped to 1–1000.

### cursor_find

```json
{
  "op": "cursor_find",
  "coll": "events",
  "filter": {"tenant": "acme"},
  "projection": {"type": 1, "at": 1},
  "sort": [["_id", 1]],
  "skip": 0,
  "limit": 100000,
  "batch_size": 500
}
```

`sort` must be omitted/empty or contain exactly one `_id` field with `1` or
`-1`. Use a regular `find` for arbitrary sorts. `limit` is the total number of
documents the cursor may return, not a batch size.

### cursor_aggregate

```json
{
  "op": "cursor_aggregate",
  "coll": "events",
  "pipeline": [
    {"$match": {"tenant": "acme"}},
    {"$project": {"type": 1, "at": 1}}
  ],
  "batch_size": 500
}
```

Cursor aggregation supports an optional leading `$match`, then only
document-local stages that preserve one output per input: `$project`,
`$addFields`/`$set`, and `$unset`. Blocking stages (`$group`, `$unwind`,
`$sort`, `$count`, `$skip`, `$limit`) require a regular `aggregate`.

Both cursor operations return the first page:

```json
{
  "cursor_id": 7,
  "docs": [{"_id": "a", "type": "login"}],
  "has_more": true
}
```

### get_more

```json
{"op": "get_more", "cursor_id": 7, "batch_size": 500}
```

Returns `{cursor_id, docs, has_more}`. A cursor is single-consumer: concurrent
`get_more` calls return `cursor_busy`. Idle cursors expire after five minutes.

### close_cursor

```json
{"op": "close_cursor", "cursor_id": 7}
```

Returns `{cursor_id, closed}`. Closing an unknown or already-closed cursor is
not an error. Cursors are isolated by principal and are not supported for vault
collections.

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
| `{"a": {"$all": [1, 2]}}` | Array contains all listed values |
| `{"a": {"$size": 2}}` | Array has exactly this length |
| `{"a": {"$elemMatch": {...}}}` | At least one array element matches |
| `{"a": {"$regex": "^foo", "$options": "i"}}` | String matches a regular expression |
| `{"a": {"$type": "string"}}` | Value has a BSON type |
| `{"a": {"$not": {...}}}` | Negate a field condition |

### Logical operators

| Operator | Meaning |
|---|---|
| `{"$and": [f1, f2]}` | All filters match |
| `{"$or": [f1, f2]}` | At least one filter matches |
| `{"$nor": [f1, f2]}` | None of the filters match |
| `{"$not": f}` | Negate a filter (top level) |

### Dates

Dates are values, not strings. Use Extended JSON in the document and in the
filter operand:

```json
{
  "op": "find",
  "coll": "events",
  "filter": {
    "created_at": {
      "$gte": {"$date": "2026-01-01T00:00:00Z"},
      "$lt": {"$date": "2026-02-01T00:00:00+01:00"}
    }
  }
}
```

Standard `$eq`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, and `$nin` compare dates
by instant. Different RFC 3339 offsets for the same instant are equal. Date
values do not match string values. See [Dates and time zones](dates.md).

### Numbers and decimals

Ordinary JSON numbers, Decimal128, and special doubles share one numeric type
bracket. Use Extended JSON for decimals:

```json
{
  "op": "find",
  "coll": "products",
  "filter": {
    "price": {
      "$gte": {"$numberDecimal": "10.00"},
      "$lt": {"$numberDecimal": "100.00"}
    }
  }
}
```

`{"$numberDecimal": "199.99"}` is a finite Decimal128 value.
`{"$numberDouble": "NaN"}`, `"Infinity"`, and `"-Infinity"` represent values
that JSON numbers cannot express. `{"$numberInt": "42"}` and
`{"$numberLong": "9007199254740993"}` preserve their BSON integer type.
Integer comparison and index keys are exact, including values above `2^53`.
See [Numbers and decimals](numerics.md).

### BSON scalar types and `$type`

Use Extended JSON for binary, UUID, and ObjectId values:

```json
{"payload": {"$binary": {"base64": "YQ==", "subType": "00"}}}
{"payload": {"$uuid": "00112233-4455-6677-8899-aabbccddeeff"}}
{"ref": {"$oid": "000000000000000000000001"}}
```

`$uuid` is normalized to binary subtype `04`. ObjectId is currently a field
value only; `_id` remains a string.

The `$type` operator accepts names, numeric BSON codes, or an array:

```json
{"op": "find", "coll": "items", "filter": {"payload": {"$type": "binData"}}}
{"op": "find", "coll": "items", "filter": {"ref": {"$type": ["objectId", 2]}}}
```

Type names include `double`, `string`, `object`, `array`, `binData`,
`objectId`, `bool`, `date`, `null`, `int`, `long`, and `decimal`; `number`
matches any numeric type. Arrays match when the array itself or any element has
the requested type. See [BSON scalar types](bson-types.md).

### Regular expressions

`$regex` accepts a pattern string and optional `$options`:

```json
{"name": {"$regex": "^ada", "$options": "i"}}
{"tags": {"$regex": "^rus"}}
```

Supported options are `i` (case-insensitive), `m` (multiline), `s` (dot
matches newline), `x` (ignore whitespace), and `u` (Unicode; already the
default). Arrays match when any string element matches. Only string values can
match; missing fields, numbers, booleans, dates, binary, and ObjectId values do
not.

The compiled-pattern form is also accepted:

```json
{"name": {"$regex": {"$regularExpression": {"pattern": "^ada", "options": "i"}}}}
```

Patterns use Rust's non-backtracking regex engine and are limited to 4 KiB.
Invalid patterns or options return `invalid_query`. Case-sensitive patterns
anchored with `^` can use a string-index prefix range; the residual matcher
still verifies every candidate.

### Arrays

`$all` requires the field to contain every listed value:

```json
{"tags": {"$all": ["rust", "lmdb"]}}
```

`$all` is equivalent to equality for a single value, so a scalar field also
matches. An empty `$all` matches nothing.

`$size` requires an exact array length:

```json
{"tags": {"$size": 2}}
```

`$elemMatch` requires at least one array element to satisfy all conditions.
For scalar elements, use field operators; for object elements, use a normal
filter:

```json
{"scores": {"$elemMatch": {"$gte": 8, "$lt": 10}}}
{"items": {"$elemMatch": {"sku": "x", "qty": {"$gt": 5}}}}
```

Malformed arguments return `invalid_query`: `$all` must be an array, `$size` a
non-negative integer, and `$elemMatch` an object. Multikey indexes serve array
equality, `$all`, ranges, and simple `$elemMatch` conditions; `$size` still
requires a scan. Regex values inside `$all`/`$elemMatch` are not supported.

### Semantics

- **Numbers compare by value and exactly.** `30` equals `30.0`; Decimal128 and
  other numeric types participate in the same bracket. Large integers are not
  rounded through `f64`.
- **Type-aware ranges.** A numeric `$gt` does not match strings or booleans.
- **Dates compare by instant.** Offsets are ignored after parsing; date and
  string values remain different types.
- **Missing fields are `null` for equality.** `{"missing": null}` matches absent
  fields; `$exists` distinguishes present `null` from absent.
- **Array membership.** `{"tags": "rust"}` matches a document where `tags` is
  `["rust", "lmdb"]`.
- **Nested paths.** Dots traverse nested objects (`{"a.b.c": 7}`) and arrays
  of objects (`{"items.price": {"$gte": 10}}`).
- **No `$where` or `$text` operator.** Full-text search is a separate
  operation.

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
field order and all directions are the same. A multikey sort uses the lowest
array element for ascending order and the highest for descending order, with
same-document duplicates removed. Otherwise nonodb performs a scan and bounded
top-K sort. Full-collection sorts and selective unsorted scans are parallelized
for shallow queries.

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

### create_indexes

Creates several indexes in one collection scan. Parsing and key encoding run in
parallel; all index writes still happen in one LMDB write transaction.

```json
{
  "op": "create_indexes",
  "coll": "events",
  "indexes": [
    {"field": "tenant", "unique": false},
    {"fields": ["tenant", "created_at"], "unique": false},
    {"field": "tags", "include": ["status"]}
  ]
}
```

Response:

```json
{"ok": true, "result": {"created": ["tenant", "tenant+created_at", "tags"]}}
```

Each entry accepts either `field` or `fields`, plus optional `unique` and
`include`. Use this for migrations that add multiple indexes at once; it avoids
re-reading and re-parsing the whole collection for every index.

Rules:

- Provide either `field` (single) or `fields` (ordered list). `fields` is
  preferred for new code.
- The index name is the fields joined by `+`.
- Indexable values are string, number, Decimal128, special doubles, boolean,
  Extended JSON dates, binary/UUID, and ObjectId. Ordinary objects remain
  unindexed, but arrays create multikey entries: one key per scalar element.
  Dotted fields expand arrays of objects, so an index on `items.price` indexes
  every `price` value.
- A compound index may contain at most one array field. Parallel arrays are
  rejected during index creation or write with `invalid_query`.
- A unique index enforces uniqueness of the full key. For compound indexes,
  that means the combination, not each field individually. A unique multikey
  index accepts repeated elements within the same document (`tags: ["x","x"]`)
  but rejects the same key in another document.
- Creating an index on an existing collection performs a synchronous backfill.
  Writers are blocked while the backfill write transaction is active.
- `include` copies extra fields into index values for covering projections.
  Included fields are not part of the index key or uniqueness constraint.
- Indexes are maintained transactionally on insert, update, and delete.
- Dates are indexed by instant and support normal range plans, sorting, and
  unique constraints. No index type annotation is required.
- Numbers use an exact, order-preserving decimal encoding. Decimal128 shares
  the numeric type tag, so `1`, `1.0`, and `NumberDecimal("1")` share a key.
- Index format version 5 adds multikey array entries. Older indexes do not
  contain array documents and are not used for planning; recreate them after
  upgrading. See [BSON scalar types](bson-types.md).
- Multikey filter ranges deduplicate document ids. `$all` intersects one index
  range per value, and `$elemMatch` can use a dotted index on the matching
  element path. `$size` still requires a scan.

### Geo index

Create a single-field GeoJSON point index:

```json
{"op": "create_index", "coll": "places", "field": "loc", "geo": true}
```

Documents use a GeoJSON `Point`:

```json
{"_id": "waw", "loc": {"type": "Point", "coordinates": [21.01, 52.23]}}
```

The `create_indexes` form accepts the same `geo` flag:

```json
{"op": "create_indexes", "coll": "places",
 "indexes": [{"field": "loc", "geo": true}]}
```

Geo indexes must have exactly one field and cannot be unique or covering.
They are maintained transactionally on insert, update, and delete.

## Geospatial queries

`$near` / `$nearSphere` returns matches ordered by distance. Distances are in
meters and use a spherical Earth model:

```json
{
  "op": "find",
  "coll": "places",
  "filter": {
    "loc": {
      "$near": {
        "$geometry": {"type": "Point", "coordinates": [21.01, 52.23]},
        "$minDistance": 0,
        "$maxDistance": 5000
      }
    }
  },
  "limit": 20
}
```

With `$maxDistance`, a matching geo index narrows the candidate cells. Without
`$maxDistance`, the query still returns distance order but falls back to a
collection scan.

`$geoWithin` accepts:

```json
{"loc": {"$geoWithin": {"$box": [[20.0, 51.0], [22.0, 53.0]]}}}
{"loc": {"$geoWithin": {"$centerSphere": [[21.01, 52.23], 0.01]}}}
{"loc": {"$geoWithin": {"$polygon": [[20, 51], [22, 51], [22, 53], [20, 53]]}}}
{"loc": {"$geoWithin": {"$geometry": {"type": "Polygon", "coordinates": [[[20,51],[22,51],[22,53],[20,53],[20,51]]]}}}}
```

`$centerSphere` uses radians for its radius. `$geoIntersects` accepts
`{"$geometry": ...}` and matches point documents against the supplied geometry.

Geo index rules and limitations:

- Only GeoJSON `Point` documents are indexed. `Polygon` and `MultiPolygon`
  geometries are supported in query arguments.
- Point-in-polygon checks use lon/lat planar geometry; distance checks use
  haversine meters. This is accurate for ordinary city/region queries but is
  not a full spherical topology engine.
- A geo index uses a hierarchy of lon/lat grid cells. Large queries may choose
  a coarser level, so the residual filter always verifies the exact result.
- `$near` cannot be combined with an explicit `sort`.

## drop_index

```json
{"op": "drop_index", "coll": "users", "field": "email"}
```

For a compound index, pass the generated name (`"grp+age"`). Response:

```json
{"ok": true, "result": {"dropped": true}}
```

`dropped` is `false` if the index did not exist. The index contents are
removed and its reusable LMDB slot is returned to the pool.

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
        "include": [],
        "format": 5,
        "geo": false,
        "multikey_fields": [],
        "multikey_all_docs": true
      }
    ]
  }
}
```

`multikey_fields` lists indexed fields that produced array entries.
`multikey_all_docs` is `true` only while every document has at least one entry,
which is required for an index to provide multikey sort order.

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
- String, boolean, and all numeric scalar values are searched, including
  `$numberInt`, `$numberLong`, `$numberDouble`, and `$numberDecimal`. Arrays,
  objects, dates, binary/UUID, and ObjectId values are skipped.

## Vector indexes

Create, drop, and list:

```json
{"op": "create_vector_index", "coll": "articles", "field": "embedding", "dims": 384, "metric": "cosine"}
{"op": "drop_vector_index", "coll": "articles", "field": "embedding"}
{"op": "list_vector_indexes", "coll": "articles"}
```

Responses:

```json
{"ok": true, "result": {"created": "embedding", "kind": "vector", "dims": 384, "metric": "cosine"}}
{"ok": true, "result": {"dropped": true, "kind": "vector"}}
{"ok": true, "result": {"vector_indexes": [{"field": "embedding", "dims": 384, "metric": "cosine", "format": 1}]}}
```

Semantics:

- `dims` is required and must be between 1 and 65,536.
- `metric` is `cosine` (default), `dot`, or `l2`.
- The document field must be a JSON array of exactly `dims` finite numbers.
  Missing, malformed, or wrong-length vectors are not indexed; writes still
  succeed.
- One vector per document per index is supported.
- The index is maintained in the same transaction as document inserts,
  updates, deletes, TTL expiry, and restores.
- Creating the index backfills existing documents synchronously.

## vector_search

```json
{
  "op": "vector_search",
  "coll": "articles",
  "field": "embedding",
  "vector": [0.12, -0.03, 0.44],
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
      {"_id": "...", "title": "...", "embedding": [0.12, -0.03, 0.44], "_distance": 0.08}
    ]
  }
}
```

Semantics:

- The vector index must exist; otherwise `invalid_query`.
- The query vector must have exactly the indexed `dims`.
- Search is exact and scans the whole index: `O(N × dims)` per query. There is
  no approximate index or recall loss.
- `_distance` is added to each result; lower is better:
  - `cosine`: `1 - cosine_similarity` (0 is identical),
  - `dot`: negative dot product,
  - `l2`: squared Euclidean distance.
- Results are sorted by `_distance`, then `_id`.
- `filter` is a normal document filter applied before final selection. If it is
  an exact `_id` or can use a secondary index with at most 50,000 candidates,
  the matching documents are checked first. Otherwise the vector index is
  scanned and candidates are checked until enough matches are found.
- A highly selective filter on an unindexed field can inspect most documents
  and approach a full collection scan. Create a secondary index for selective
  filters.
- `skip` and `limit` apply after ranking. `limit` defaults to 10 and is capped
  at 1,000.
- Vectors are stored in LMDB; no external vector service is used.

## Document history

History is opt-in on the daemon (`NONODB_HISTORY=1`) and is stored separately
from the current collection. Searches, indexes, `find`, `count`, `update`, and
`delete` always operate on current documents only. Every write receives a
global monotonic `rev`; revisions for one document are therefore ordered but
not necessarily consecutive.

History records inserts, updates, and delete tombstones. The write is atomic
with the current-document change.

### history

```json
{
  "op": "history",
  "coll": "users",
  "id": "0b7f...",
  "limit": 50,
  "include_docs": true
}
```

Result, newest first:

```json
{
  "revisions": [
    {
      "rev": 1042,
      "op": "update",
      "at": 1710000000123,
      "deleted": false,
      "doc": {"_id": "0b7f...", "name": "Ala"}
    },
    {
      "rev": 1001,
      "op": "insert",
      "at": 1710000000000,
      "deleted": false,
      "doc": {"_id": "0b7f...", "name": "Alicja"}
    }
  ]
}
```

`limit` defaults to 50 and is capped at 1000. With `include_docs: false`,
metadata is returned without full documents.

### get_version

```json
{
  "op": "get_version",
  "coll": "users",
  "id": "0b7f...",
  "version": 1001
}
```

`rev` is accepted as an alias for `version`. The result is one revision object,
or `null` if that revision no longer exists. A deleted document has a tombstone
with `"deleted": true` and no document body.

History retention is configured with `NONODB_HISTORY_MAX_VERSIONS` (default
50). The daemon maintenance worker submits bounded pruning jobs to the single
writer; pruning never scans history on the hot write path.

## Document TTL

TTL is optional and eventual. A document may remain readable briefly after its
expiry until the maintenance sweeper commits the delete.

Collection default:

```json
{"op":"create_collection","coll":"sessions","ttl_seconds":86400}
```

```json
{"op":"set_collection_ttl","coll":"sessions","ttl_seconds":3600}
{"op":"set_collection_ttl","coll":"sessions","ttl_seconds":null}
{"op":"set_collection_ttl","coll":"sessions","ttl_seconds":3600,"purge_history":true}
{"op":"get_collection_ttl","coll":"sessions"}
```

`set_collection_ttl` response:

```json
{"ok":true,"result":{"coll":"sessions","ttl_seconds":3600,"purge_history":false}}
```

`get_collection_ttl` response:

```json
{"ok":true,"result":{"coll":"sessions","ttl_seconds":3600,"purge_history":false}}
```

Per-document override:

```json
{"op":"insert","coll":"sessions","doc":{"_id":"s1","user":7,"_ttl":300}}
```

Rules:

- `_ttl` overrides the collection default; `_ttl: 0` or `null` disables expiry.
- A missing `_ttl` inherits the collection default; without either, the
  document does not expire.
- `ttl_seconds` must be positive. Negative, fractional, or non-numeric `_ttl`
  values return `invalid_ttl`.
- Updates preserve the existing expiry unless the update result contains an
  explicit `_ttl`. Replacement updates and upserts follow the same rule.
- `$unset` of `_ttl` does not clear an expiry; use `$set` with `0` or `null`.
- Expiry deletes the document through the normal write path and appends a
  history tombstone when history is enabled. With `purge_history: true`, stored
  revisions are deleted instead of adding a tombstone.
- TTL works for vault collections; the sweeper deletes the ciphertext without
  decrypting it.

See [Document TTL](ttl.md) for maintenance, metrics, and operational details.

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

- Allowed operations: `find`, `find_one`, `lookup`, `count`, `list_indexes`,
  `list_text_indexes`, `list_fuzzy_indexes`, `list_vector_indexes`,
  `list_collections`, `history`, `get_version`, `search`, `fuzzy`,
  `vector_search`.
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

## create_collection

```json
{
  "op": "create_collection",
  "coll": "vault",
  "profile": "vault"
}
```

Response:

```json
{"ok": true, "result": {"created": "vault", "profile": "vault", "ttl_seconds": null, "purge_history": false}}
```

`profile` is `standard` (default) or `vault`. An existing collection cannot be
converted between profiles; the request fails with
`vault_profile_immutable`.

An optional `ttl_seconds` sets the default document TTL for new inserts.
`ttl_purge_history` controls whether TTL expiry deletes stored history:

```json
{
  "op": "create_collection",
  "coll": "sessions",
  "profile": "standard",
  "ttl_seconds": 86400,
  "ttl_purge_history": false
}
```

## list_collections

```json
{"op": "list_collections"}
```

Response:

```json
{"ok": true, "result": {"collections": ["users", "settings"]}}
```

Standard collections are created on first write. Vault collections should be
created explicitly with `create_collection`.

### Vault operation summary

For a vault collection, `insert`, `insert_many`, `find`, `find_one`, `count`,
`update`, `delete`, `history`, and `get_version` require the vault headers.
Only exact `_id` / `tenant_id` filters are accepted, update/delete require one
exact `_id`, and sort is limited to `_id`. Search, fuzzy search, aggregation,
indexes, `batch`, and `write_batch` are rejected with
`unsupported_encrypted_operation`.

See [Vault collections](vault.md).

---

## LMDB map size

### map_info

```json
{"op": "map_info"}
```

Response:

```json
{"ok": true, "result": {
  "map_size": 1073741824,
  "used_bytes": 7340032,
  "free_bytes": 1066401792,
  "page_size": 4096
}}
```

`map_size` is the address-space limit, not disk or RAM usage. `used_bytes` is
the LMDB high-water mark.

### resize_map

```json
{"op": "resize_map", "size": 4294967296}
```

Response:

```json
{"ok": true, "result": {"old_size": 1073741824, "map_size": 4294967296}}
```

`size` is in bytes and must be a multiple of `page_size` and at least the
current map size. The operation waits for active transactions and temporarily
blocks new ones. It only grows the map; shrinking requires an offline restart.
See [Operations](operations.md#capacity-and-memory).

## Health and metrics

### GET /healthz

Public, no authentication.

```json
{"ok": true}
```

### GET /metrics

Requires the `admin:db` scope. The Unix socket authenticates as the local
administrator; TCP requires a bearer token or registered client certificate.

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
| `ttl_sweeps_total` | TTL sweeps performed |
| `ttl_expired_total` | Documents deleted by TTL |
| `ttl_sweep_errors_total` | Failed TTL sweeps |
| `ttl_backlog` | Due entries observed by the last sweep |
| `index_builds` | Active/recent index, text, fuzzy, and vector backfills with processed/total/elapsed |
| `avg_batch_size` | Write ops per committed write transaction |
| `avg_commit_us` | Average commit time in microseconds |

## Error codes

| Code | HTTP | Meaning |
|---|---|---|
| `invalid_query` | 400 | Invalid filter/update/index/query shape, write in a read batch, unsupported operator |
| `invalid_sql` | 400 | SQL parse/planning error or unsupported SQL clause |
| `invalid_json` | 400 | Malformed JSON document or value |
| `invalid_collection` | 400 | Invalid collection name |
| `invalid_id` | 400 | `_id` is not a string |
| `duplicate_key` | 400 | Duplicate `_id` or unique-index conflict |
| `document_too_large` | 413 | Document exceeds 1 MiB |
| `result_too_large` | 413 | `lookup` result budget exceeds 64 MiB |
| `invalid_ttl` | 400 | Invalid collection default or document `_ttl` value |
| `invalid_date` | 400 | Invalid `$date` payload, RFC 3339 string, or missing timezone |
| `invalid_numeric` | 400 | Invalid Extended JSON number or decimal arithmetic overflow |
| `invalid_binary` | 400 | Invalid `$binary`/`$uuid` payload or base64 |
| `invalid_object_id` | 400 | `$oid` is not 24 hexadecimal characters |
| `unauthorized` | 401 | Missing/invalid/disabled credential |
| `forbidden` | 403 | Principal lacks the required scope |
| `rate_limited` | 429 | Too many failed logins from this IP; wait and retry |
| `timeout` | 500 | Request exceeded the server timeout |
| `cursor_not_found` | 400 | Cursor is unknown, expired, closed, or belongs to another principal |
| `cursor_busy` | 400 | Another `get_more` is already reading this cursor |
| `storage` | 500 | LMDB/storage error |
| `io` | 500 | Filesystem or I/O error |
| `internal` | 500 | Internal daemon error |
| `vault_key_required` | 400 | Vault headers are missing |
| `vault_key_unexpected` | 400 | Vault headers sent to a standard collection |
| `invalid_vault_key` | 400 | Invalid or wrong-length vault key |
| `tenant_mismatch` | 400 | Tenant/document/filter mismatch |
| `decryption_failed` | 400 | Wrong key, tenant, key id, or corrupted envelope |
| `unsupported_encrypted_operation` | 400 | Operation not available for vault collections |
| `invalid_vault_filter` | 400 | Vault filter or multi update/delete is not allowed |
| `invalid_vault_document` | 400 | Invalid vault document shape |
| `vault_profile_immutable` | 400 | Collection profile cannot be changed |
| `invalid_collection_profile` | 400 | Unknown collection profile |
| `not_a_vault_collection` | 400 | Collection does not exist or is not a vault collection |

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
