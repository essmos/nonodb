# Python client

`client/nonodb.py` is a thin, synchronous Python client for nonodb. It speaks
HTTP/JSON to the daemon over a Unix socket or TCP/TLS and keeps no sessions or
cursors.

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

```python
n = users.count({"active": True})
n = users.count()                 # O(1) total count
```

`count` returns an integer.

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
users.update({"_id": user_id}, {"$addToSet": {"roles": {"$each": ["admin", "editor"]}}})
users.update({"_id": user_id}, {"$pull": {"tags": "deprecated"}})
```

### Replacement

An update with no `$` operators replaces the whole document while preserving
`_id`:

```python
users.update({"_id": user_id}, {"name": "Ada Lovelace", "age": 37})
```

### Caveats

- `_id` is immutable. Do not modify it with update operators.
- `$push`/`$addToSet` replace a non-array value with a new array.
- `$pull` removes exact values only; it does not accept query predicates.
- `$rename`, `$min`, `$max`, `$mul`, `$currentDate`, positional operators, and
  array filters are not implemented.

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

### Compound and covering

```python
events.create_index(fields=["tenant", "kind", "created_at"])
events.create_index(
    fields=["tenant", "created_at"],
    include=["payload", "status"],
)
```

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

## Listing indexes

```python
articles.list_text_indexes()   # ["body"]
articles.list_fuzzy_indexes()  # ["title"]
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
batch:

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
  `count`, `aggregate`, list operations, `search`, `fuzzy`, `batch`).
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
