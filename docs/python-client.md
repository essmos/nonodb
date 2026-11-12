# Python client

`client/nonodb.py` is a thin, synchronous Python client for nonodb. It speaks
HTTP/JSON to the daemon over a Unix socket or TCP/TLS. Cursors are explicit
server-side objects created through `find_cursor` and `aggregate_cursor`.

The client is intentionally small: query semantics live in the daemon, so
Python, curl, the CLI, and any future language client all see the same behavior.

## Requirements and installation

- Python 3.9+
- [`httpx`](https://www.python-httpx.org/)
- Optional: [`orjson`](https://github.com/ijson/ijson) for faster JSON encoding
  and decoding

The client is a single module, not yet published on PyPI. Add the `client`
directory to your import path or vendor `nonodb.py` into your project:

```bash
export PYTHONPATH="/path/to/nonodb/client:$PYTHONPATH"
```

Install dependencies with your package manager, for example:

```bash
pip install httpx orjson
```

## Connecting

### Unix socket

```python
from nonodb import NonoDB

db = NonoDB("/run/nonodb/nonodb.sock")
```

If `socket_path` is omitted, the client uses `NONODB_SOCK`, then
`/run/nonodb.sock`.

### TCP and TLS

```python
db = NonoDB(base_url="https://db.internal:7878", token="secret")
```

- `base_url` switches the client from Unix-socket transport to HTTP over TCP.
- `token` supplies the bearer token. If omitted, `NONODB_TOKEN` is used.
- Certificate verification is on by default. For local testing with a
  self-signed certificate:

```python
db = NonoDB(base_url="https://127.0.0.1:7878", token="secret", verify=False)
```

`NONODB_TLS_VERIFY=0` (or `false`/`no`) has the same effect. Do not disable
verification in production.

### Mutual TLS

Pass a client certificate and key when the daemon requires mTLS. `ca_cert`
overrides the CA bundle used to verify the daemon certificate.

```python
db = NonoDB(
    base_url="https://db.internal:7878",
    token="secret",
    ca_cert="/etc/nonodb/ca.crt",
    client_cert="/etc/nonodb/client.crt",
    client_key="/etc/nonodb/client.key",
)
```

The environment fallbacks are `NONODB_TLS_CA`, `NONODB_TLS_CLIENT_CERT`, and
`NONODB_TLS_CLIENT_KEY`. The certificate and key must both be provided. See
[Mutual TLS](mtls.md) for CA generation, server setup, and revocation.

### Timeouts

The default request timeout is 10 seconds. It can be set per client or through
the environment:

```python
db = NonoDB("/run/nonodb.sock", timeout=60)   # seconds
```

```bash
export NONODB_TIMEOUT=300
```

Use a larger timeout for index creation and large backfills.

### Lifecycle

`NonoDB` owns an `httpx.Client`. Close it when the process or application
lifespan ends, or use a context manager:

```python
with NonoDB("/run/nonodb/nonodb.sock") as db:
    ...
```

## Collections

Standard collections are created on first write. Vault collections must be
created explicitly:

```python
users = db.collection("users")            # standard
db.create_collection("vault", "vault")   # encrypted, restricted API
```

A `Collection` is a lightweight handle around the same `NonoDB` connection.

### Collection TTL

```python
db.create_collection("sessions", ttl_seconds=86400)
db.set_collection_ttl("sessions", 3600)
assert db.get_collection_ttl("sessions") == 3600
db.set_collection_ttl("sessions", None)   # clear the default
db.set_collection_ttl("sessions", 3600, purge_history=True)

sessions = db.collection("sessions")
sessions.insert({"_id": "s1", "user": 7, "_ttl": 300})  # per-document override
sessions.update({"_id": "s1"}, {"$set": {"_ttl": 0}})   # disable expiry
```

An update without `_ttl` preserves the current expiry. `purge_history=True`
removes stored revisions when TTL deletes a document instead of appending a
tombstone. Expiry is eventual and handled by the daemon maintenance worker.
See [Document TTL](ttl.md).

## Vault collections

Use `NonoDB.vault(tenant, key_id, key)` for a tenant/key-bound handle. It sends the
three vault headers on every request and bypasses the client-side read cache.

```python
import os

key = os.environ["ACME_VAULT_KEY"]
vault = db.vault("acme", "acme:v1", key)
docs = vault.collection("vault")

doc_id = docs.insert({
    "tenant_id": "acme",
    "type": "invoice",
    "notes": "sensitive",
})
print(docs.find_one({"_id": doc_id}))
docs.update({"_id": doc_id}, {"$set": {"notes": "changed"}})
docs.delete({"_id": doc_id})
```

Vault collections support only CRUD by `_id`, list/count by `tenant_id`, and
history. Search, fuzzy, aggregation, indexes, `batch`, and `write_batch` are not
available. See [Vault collections](vault.md).

## Inserting

```python
user_id = users.insert({"name": "Ada", "age": 36})
```

`insert` returns the `_id` string. If the document has no `_id`, the daemon
generates a UUID v4.

```python
ids = users.insert_many([
    {"name": "Grace", "age": 41},
    {"name": "Linus", "age": 55},
])
```

`insert_many` returns ids in input order. Keep requests below the daemon's
4 MiB body limit; for large imports, insert in chunks of roughly 500–1,000
documents.

Custom string ids are allowed:

```python
users.insert({"_id": "user:ada", "name": "Ada"})
```

## Finding documents

```python
adults = users.find(
    {"age": {"$gte": 18}},
    projection={"name": 1, "age": 1, "_id": 0},
    sort=[("age", -1)],
    skip=0,
    limit=20,
)
```

Signature:

```python
Collection.find(
    filter=None,
    *,
    projection=None,
    sort=None,      # list[tuple[str, int]]
    skip=0,
    limit=None,
) -> list[dict]
```

`find(None)` and `find({})` match everything.

```python
user = users.find_one({"email": "ada@example.com"})
user = users.find_one({"_id": user_id}, projection={"name": 1})
```

`find_one` returns a document or `None`.

### Point lookups

`lookup` fetches many documents by `_id` or a unique index in one request.
Results keep input order and use `None` for missing keys.

```python
users.create_index("email", unique=True, include=["name", "status"])

rows = users.lookup(
    "email",
    ["ada@example.com", "missing@example.com"],
    fields=["name", "status"],
)
# [{"_id": ..., "name": "Ada", "status": "active"}, None]

user = users.lookup_one(
    "email",
    "ada@example.com",
    projection={"name": 1, "_id": 0},
)

# Compound unique lookup.
users.create_index(fields=["tenant_id", "email"], unique=True)
rows = users.lookup(
    ["tenant_id", "email"],
    [
        ["acme", "ada@example.com"],
        ["other", "ada@example.com"],
        ["acme", "missing@example.com"],
    ],
)
```

A non-`_id` lookup requires a unique index with the same field order as `by`.
For a compound lookup each value must be an array of the same length. Values are
limited to 10,000 per request and the result budget to 64 MiB.

`fields` is a top-level inclusive projection; a covering index can satisfy it
without reading the document. `projection` accepts a normal flat projection,
allows `"_id": 0`, and cannot be combined with `fields`. Duplicate keys are
looked up once internally but still produce duplicate results. See
[HTTP API: lookup](api.md#lookup).

```python
n = users.count({"active": True})
n = users.count()                 # O(1) total count
```

`count` returns an integer.

### Explaining queries

```python
plan = users.explain(
    {"age": {"$gte": 18}},
    sort=[("age", 1)],
    limit=20,
)
print(plan["access_path"], plan["index_used"], plan["actual_matched"])
```

`explain` reports the selected access path, index fields and bounds,
`candidates`, `actual_matched`, `actual_returned`, and whether the sort is
covered by the access path. It executes the query to obtain exact counts.

## Aggregation

```python
rows = users.aggregate([
    {"$match": {"active": True}},
    {"$group": {
        "_id": "$tenant",
        "total": {"$sum": "$amount"},
        "n": {"$sum": 1},
    }},
    {"$sort": {"total": -1}},
    {"$limit": 10},
])
```

`aggregate` is read-only and returns the pipeline result rows. The supported
stages, expressions, accumulators, and the 100,000-document intermediate limit
are documented in [Aggregation](api.md#aggregation).

## SQL

```python
rows = db.sql(
    "SELECT team, COUNT(*) AS n, SUM(score) AS total "
    "FROM users WHERE age >= $1 GROUP BY team HAVING COUNT(*) > 1 "
    "ORDER BY total DESC LIMIT 10",
    [18],
)
plan = db.explain_sql(
    "SELECT DISTINCT team FROM users WHERE age >= $1",
    [18],
)
```

`sql` returns result objects. `SELECT` is read-only; `INSERT` and `UPDATE` use
the single writer and return `inserted`/`inserted_ids` or `matched`/`modified`.
`explain_sql` returns the selected `find` or aggregation plan for a `SELECT`.
Parameters can use `?` or `$1` placeholders. See [SQL queries](sql.md) for the
supported grammar, `DISTINCT`, `HAVING`, `CASE`, scalar functions, writes, and
limitations.

```python
inserted = db.sql(
    "INSERT INTO users (name, age) VALUES ($1, $2)",
    ["Ada", 36],
)
updated = db.sql(
    "UPDATE users SET age = $1 WHERE name = $2",
    [37, "Ada"],
)
```

## Cursors

Use cursors for result sets that are too large for one response. Pages are
fetched by `_id` and held in server memory only between requests.

```python
with users.find_cursor(
    {"active": True},
    projection={"name": 1, "email": 1},
    sort=[("_id", 1)],
    batch_size=500,
) as cursor:
    for user in cursor:
        process(user)
```

`find_cursor` requires an empty sort or a single `_id` sort. For aggregation,
the first stage may be `$match`; the remaining stages must be document-local
(`$project`, `$addFields`/`$set`, `$unset`):

```python
cursor = users.aggregate_cursor(
    [
        {"$match": {"active": True}},
        {"$project": {"name": 1, "total": {"$add": ["$a", "$b"]}}},
    ],
    batch_size=1000,
)
for row in cursor:
    process(row)
cursor.close()
```

`next_batch()` returns the next list of documents and `close()` releases the
server-side cursor. Iterating automatically fetches more batches and stops when
`has_more` is false. Idle cursors expire after five minutes.

### Filters

The full filter language is documented in the [HTTP API](api.md#filters). In
Python, pass ordinary dictionaries and lists:

```python
{
    "tenant": "acme",
    "age": {"$gte": 18, "$lt": 65},
    "role": {"$in": ["admin", "editor"]},
    "deleted_at": None,
    "$or": [
        {"region": "eu"},
        {"region": "us"},
    ],
}
```

Nested paths use dots:

```python
users.find({"profile.address.country": "PL"})
```

## Updating

### Basic update

```python
modified = users.update(
    {"_id": user_id},
    {"$set": {"active": True}, "$inc": {"logins": 1}},
)
```

`update` returns the number of modified documents. By default it updates only
the first match:

```python
users.update({"tenant": "acme"}, {"$set": {"plan": "pro"}}, multi=True)
```

### Update result and upsert

Use `update_result` when you need `matched`, `modified`, and `upserted_id`:

```python
result = users.update_result(
    {"tenant": "acme", "key": "smtp"},
    {
        "$set": {"host": "smtp.example.com"},
        "$setOnInsert": {"created_at": "2026-11-09T00:00:00Z"},
    },
    upsert=True,
)
# {"matched": 0, "modified": 0, "upserted_id": "..."} on insert
# {"matched": 1, "modified": 1, "upserted_id": None} on update
```

Upsert equality conditions from the filter are copied into the new document.
`$setOnInsert` is ignored when an existing document matches.

### Arrays

```python
users.update({"_id": user_id}, {"$push": {"tags": "rust"}})
users.update({"_id": user_id}, {"$push": {"ports": {"$each": [25, 587]}}})
users.update({"_id": user_id}, {"$push": {"top": {"$each": [9, 3, 7], "$sort": 1, "$slice": 2}}})
users.update({"_id": user_id}, {"$addToSet": {"roles": {"$each": ["admin", "editor"]}}})
users.update({"_id": user_id}, {"$pull": {"scores": {"$gte": 90}}})
users.update({"_id": user_id}, {"$pullAll": {"tags": ["draft", "obsolete"]}})

# Positional updates. The filter selects the array element for `$`.
users.update(
    {"items": {"$elemMatch": {"sku": "A-1"}}},
    {"$set": {"items.$.qty": 5}},
)
users.update({"_id": user_id}, {"$inc": {"items.$[].qty": 1}})
users.update(
    {"_id": user_id},
    {"$set": {"items.$[i].checked": True}},
    array_filters=[{"i.sku": "A-1", "i.qty": {"$lt": 5}}],
)
```

### Atomic find-and-modify

`find_and_modify` selects, updates or removes, and returns one document in a
single write transaction:

```python
old = users.find_and_modify(
    {"state": "queued"},
    {"$set": {"state": "running"}, "$inc": {"attempts": 1}},
    sort=[("priority", -1)],
)

new = users.find_and_modify(
    {"_id": user_id},
    {"$inc": {"credits": -1}},
    new=True,
    projection={"_id": 1, "credits": 1},
)

removed = users.find_and_modify({"_id": user_id}, remove=True)
```

Use `find_and_modify_result(...)` when you also need `matched`, `modified`, or
`upserted_id`. `array_filters=` is supported for `$[identifier]` paths. The
call is safe for work queues and counters because no read can interleave
between selection and the write.

### Replacement

An update with no `$` operators replaces the whole document while preserving
`_id`:

```python
users.update({"_id": user_id}, {"name": "Ada Lovelace", "age": 37})
```

### Caveats

- `_id` is immutable. Do not modify it with update operators.
- `$push`/`$addToSet` replace a non-array value with a new array.
- `$pull` accepts values or query predicates; `$pullAll` removes listed values.
- Numeric array indexes (`items.0.qty`), positional `$`, `$[]`, and
  `$[identifier]` with `array_filters=` are supported.
- `$rename`, `$min`, `$max`, `$mul`, `$currentDate`, and pipeline updates are
  not implemented.

## Deleting

```python
deleted = users.delete({"_id": user_id})
deleted = users.delete({"tenant": "acme", "expired": True}, multi=True)
```

`delete` returns the number of deleted documents.

## Indexes

### Single-field

```python
users.create_index("email", unique=True)
users.create_index("tenant")
```

The legacy positional `field` argument is still accepted:

```python
users.create_index(field="age")
```

### GeoJSON points

```python
places.create_index("loc", geo=True)
```

A geo index supports `$near` and `$geoWithin` filters. It is always
single-field and cannot be unique or covering.

### Compound and covering

```python
events.create_index(fields=["tenant", "kind", "created_at"])
events.create_index(
    fields=["tenant", "created_at"],
    include=["payload", "status"],
)
```

### Batch creation

```python
created = events.create_indexes([
    {"field": "tenant"},
    {"fields": ["tenant", "created_at"]},
    {"field": "tags", "include": ["status"]},
])
# ["tenant", "tenant+created_at", "tags"]
```

All indexes are built in one collection scan and one write transaction. This
avoids re-reading and re-parsing the collection once per index.

### Inspect and drop

```python
indexes = users.list_indexes()
# [{"field": "email", "fields": ["email"], "unique": True, "include": []}]

users.drop_index("email")
users.drop_index("tenant+created_at")
```

Creating an index backfills existing documents synchronously. On large
collections this can take seconds to minutes and blocks writers; raise
`timeout` and `NONODB_REQUEST_TIMEOUT` accordingly.

## Full-text search

```python
articles = db.collection("articles")
articles.create_text_index("body")

results = articles.search("body", "rust async", limit=10)
for doc in results:
    print(doc["_score"], doc["title"])
```

Signature:

```python
Collection.search(
    field,
    query,
    *,
    filter=None,
    skip=0,
    limit=10,
) -> list[dict]
```

- Results are ranked by BM25, highest `_score` first.
- Terms are ORed.
- Quoted phrases require contiguous tokens:

```python
articles.search("body", 'rust "zero copy"', limit=5)
```

- `filter`, `skip`, and `limit` are applied after ranking and phrase checks.
- The index must exist.

## Fuzzy search

```python
articles.create_fuzzy_index("title")

results = articles.fuzzy("title", "postgress", max_distance=2, limit=10)
for doc in results:
    print(doc["_distance"], doc["title"])
```

Signature:

```python
Collection.fuzzy(
    field,
    query,
    *,
    max_distance=2,
    filter=None,
    limit=10,
) -> list[dict]
```

- Results are sorted by `_distance`, lowest first.
- Distance is measured against the whole value or the closest token.
- There is no `skip` parameter.
- The index must exist.

## Vector search

Vectors are computed outside NonoDB, by the application or a local embedding
service. NonoDB stores the ready vectors and performs exact top-K search.

```python
articles.create_vector_index("embedding", 384, metric="cosine")

articles.insert({
    "title": "Async Rust",
    "embedding": embed("Async Rust"),
})

results = articles.vector_search(
    "embedding",
    embed("rust async"),
    filter={"published": True},
    limit=10,
)
for doc in results:
    print(doc["_distance"], doc["title"])
```

Signatures:

```python
Collection.create_vector_index(
    field,
    dims,
    metric="cosine",       # "cosine", "dot", or "l2"
) -> bool

Collection.vector_search(
    field,
    vector,
    *,
    filter=None,
    skip=0,
    limit=10,
) -> list[dict]
```

- `_distance` is lower-is-better: `1 - cosine`, `-dot`, or squared L2.
- Search is exact and scans the whole index; it is not approximate.
- One vector per document per index.
- Missing or wrong-length vectors are not indexed.
- The same model and dimensions must be used for stored vectors and queries.

## Listing indexes

```python
articles.list_text_indexes()    # ["body"]
articles.list_fuzzy_indexes()   # ["title"]
articles.list_vector_indexes()  # [{"field": "embedding", "dims": 384, ...}]
```

## Read batches

A read batch sends several read operations in one request and executes them
against one LMDB snapshot. Each entry contains either a result or an error.

```python
results = db.batch([
    {"op": "find_one", "coll": "users", "filter": {"_id": user_id}},
    {"op": "count", "coll": "users", "filter": {"active": True}},
    {"op": "list_indexes", "coll": "users"},
])
```

Document history is available when the daemon runs with `NONODB_HISTORY=1`:

```python
revisions = users.history(id, limit=20, include_docs=True)
old = users.get_version(id, revisions[-1]["rev"])
```

`history` returns revisions newest first. `rev` values are global and
monotonic, not per-document `1, 2, 3`. See
[Document history](api.md#document-history).

A common shortcut is `get_many`, which fetches documents by id through one
`lookup` request:

```python
docs = users.get_many([id1, id2, id3])
```

Only read operations are allowed in `batch`; writes are rejected per operation.

## Atomic write batches

`write_batch` executes writes in one LMDB transaction. If any operation fails,
the whole batch is rolled back.

Database-level batch (each operation specifies its collection):

```python
results = db.write_batch([
    {"op": "insert", "coll": "users", "doc": {"_id": "u1", "name": "Ada"}},
    {"op": "update", "coll": "counters", "filter": {"_id": "hits"},
     "update": {"$inc": {"n": 1}}},
    {"op": "delete", "coll": "sessions", "filter": {"_id": "s1"}},
])
```

Collection-level batch (the collection name is added automatically):

```python
users.write_batch([
    {"op": "insert", "doc": {"_id": "u2", "name": "Grace"}},
    {"op": "update", "filter": {"_id": "u2"}, "update": {"$set": {"active": True}}},
])
```

Operations run in order, and later operations see earlier changes in the same
transaction. On failure, `NonoDBError` is raised and nothing from the batch is
committed.

## Cache

The client has an optional per-process read cache, disabled by default. Enable
it with a TTL in seconds:

```python
db = NonoDB("/run/nonodb/nonodb.sock", cache_ttl=0.5)
```

Behavior:

- Cached operations: reads understood by the client (`find`, `find_one`,
  `lookup`, `count`, `aggregate`, `sql`, `explain_sql`, list operations, `search`, `fuzzy`, `batch`).
- Any write through **this client instance** clears the entire cache.
- Writes from other processes are not observed until the TTL expires.
- Values are stored as serialized JSON; each cache hit deserializes a fresh
  object, so mutating a returned value cannot corrupt the cache.
- `db.clear_cache()` clears it manually.

Use `cache_ttl=0` for strictly fresh reads.

## Retries and errors

The client retries **read operations** up to two times after a transport error,
with a short backoff. It does **not** retry writes, because a lost response may
mean the write succeeded and a retry could duplicate it.

Daemon errors raise `NonoDBError`:

```python
from nonodb import NonoDBError

try:
    users.insert({"_id": "u1", "name": "Ada"})
except NonoDBError as exc:
    print(exc.code)     # e.g. "duplicate_key"
    print(exc.message)
```

Transport failures are reported as `NonoDBError("connection", ...)`. Invalid
HTTP responses are `invalid_response`.

Common error codes are listed in the [HTTP API](api.md#error-codes).

## Health and raw requests

```python
db.health()        # bool
db.collections()   # list[str]
db.map_info()      # {"map_size": ..., "used_bytes": ..., "page_size": ...}
db.resize_map(4 * 1024**3)  # grow the map online, returns old/new sizes
```

For operations not wrapped by the client, send a raw request:

```python
result = db.exec({
    "op": "find",
    "coll": "users",
    "filter": {"age": {"$gte": 18}},
    "limit": 10,
})
```

`exec` returns the `result` field from the response envelope.

## FastAPI and uvicorn

Create one client per worker process, typically in the application lifespan.
Do not create a client per request.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from nonodb import NonoDB


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = NonoDB("/run/nonodb/nonodb.sock")
    yield
    app.state.db.close()


app = FastAPI(lifespan=lifespan)


@app.get("/users/{user_id}")
def get_user(user_id: str, request: Request):
    db: NonoDB = request.app.state.db
    return db.collection("users").find_one({"_id": user_id})
```

The client is synchronous. In an async endpoint, either call it in a thread
pool or keep the endpoint synchronous (`def`, not `async def`) so FastAPI runs
it in its worker thread pool.

The daemon is designed for exactly this deployment: many preforked application
workers, one database owner.

## Thread safety

`NonoDB` is safe to share between threads in one process: `httpx.Client` handles
concurrent requests and the cache has its own lock. For simplicity and
predictable connection behavior, many applications create one client per
worker process or per thread.

## Complete example

```python
from nonodb import NonoDB, NonoDBError

with NonoDB("/tmp/nonodb.sock", timeout=30, cache_ttl=0.25) as db:
    users = db.collection("users")
    users.create_index("email", unique=True)
    users.create_index(fields=["tenant", "age"])

    try:
        user_id = users.insert({
            "tenant": "acme",
            "email": "ada@example.com",
            "name": "Ada",
            "age": 36,
            "tags": ["admin"],
        })
    except NonoDBError as exc:
        if exc.code != "duplicate_key":
            raise
        user_id = users.find_one({"email": "ada@example.com"})["_id"]

    users.update({"_id": user_id}, {"$addToSet": {"tags": "founder"}})

    print(users.find({"tenant": "acme", "age": {"$gte": 18}},
                     sort=[("age", -1)], limit=10))
    print(users.count({"tenant": "acme"}))
```
