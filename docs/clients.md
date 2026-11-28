# Clients

nonodb has thin official clients for Python, Rust, Node.js, and Go. They all
speak the same HTTP/JSON API and keep no state beyond one HTTP connection. The
daemon remains responsible for queries, indexes, updates, search, and
transactions.

| Language | Location | Style | Unix socket | TCP/TLS | mTLS |
|---|---|---|---|---|---|
| Python | [`client/nonodb.py`](../client/nonodb.py) | sync + async watch | yes | yes | yes |
| Rust | [`nonodb-client`](../nonodb-client) | sync | yes (Unix) | yes | yes |
| Node.js | [`client/node`](../client/node) | async / Promise | yes (Unix) | yes | yes |
| Go | [`client/go`](../client/go) | context-aware | yes (Unix) | yes | yes |

## Common configuration

On Linux and macOS the clients use the Unix socket by default. Set
`NONODB_SOCK` to change the path. A base URL switches a client to TCP/TLS:

```bash
export NONODB_SOCK=/run/nonodb.sock   # default Unix socket
export NONODB_TOKEN='nodb_...'         # required for TCP unless mTLS identifies you
export NONODB_DB=app1                  # optional database inside one daemon
# or:
export NONODB_URL=https://127.0.0.1:7878
export NONODB_TLS_CA=/etc/nonodb/ca.crt
export NONODB_TLS_VERIFY=1
```

Mutual TLS uses the same three variables in every client:

```bash
export NONODB_TLS_CLIENT_CERT=/etc/nonodb/client.crt
export NONODB_TLS_CLIENT_KEY=/etc/nonodb/client.key
```

`NONODB_TLS_VERIFY=0` disables certificate verification and must only be used
for local development. See [Mutual TLS](mtls.md) for a complete mTLS setup,
revocation, and rotation guide.

Windows has no Unix socket listener in the daemon. Use a base URL and TCP there.
TCP always requires an auth registry. Use `NONODB_TOKEN`, a registered client
certificate, or both (they must resolve to the same principal). The Unix socket
does not require a token and maps to the local administrative principal.

All clients return a typed error for daemon errors (`ok: false`). Read
operations are retried after a transport failure. Writes are never retried after
an ambiguous transport error or `5xx`, because the write may have committed and
only the response was lost. Use `write_batch` when several writes must succeed
or fail together.

## Routing writes to the primary

A replica rejects writes with HTTP 403 and `error.code = "read_only"`. The
response includes the current primary in the `Location` header and in
`error.write_url`. Clients can use a custom write endpoint, or discover it from
the replica automatically:

```bash
export NONODB_WRITE_URL=https://primary.internal:7878
export NONODB_AUTO_PRIMARY=1
export NONODB_TOKEN='nodb_...'
```

When `write_url` is configured, reads keep using the socket/base URL and writes
go directly to the primary. When `auto_primary` is enabled, the client reads
`/readyz.replication.write_url`; if a stale write endpoint still answers
`read_only`, the client retries that single write once against the advertised
primary.

A retry only happens after an explicit `403 read_only`, which guarantees the
replica did not apply the write. It never happens after a timeout, a connection
error, or `5xx`. The primary endpoint normally requires a bearer token, so a
Unix-socket client must also be given `NONODB_TOKEN` or `token`/`WithToken` to
write remotely.

```python
db = NonoDB(
    "/run/nonodb.sock",             # reads from the local replica
    write_url="https://primary.internal:7878",
    token="nodb_...",
    auto_primary=True,              # or omit write_url and discover it
)
```

```rust
let db = Client::builder()
    .socket_path("/run/nonodb.sock")
    .write_url("https://primary.internal:7878")
    .token("nodb_...")
    .auto_primary(true)
    .build()?;
```

```js
const db = new NonoDB({
  socketPath: '/run/nonodb.sock',
  writeUrl: 'https://primary.internal:7878',
  token: 'nodb_...',
  autoPrimary: true,
});
```

```go
db := nonodb.New(
    nonodb.WithSocketPath("/run/nonodb.sock"),
    nonodb.WithWriteURL("https://primary.internal:7878"),
    nonodb.WithToken("nodb_..."),
    nonodb.WithAutoPrimary(true),
)
```

Cursor operations (`cursor_find`, `cursor_aggregate`, `get_more`,
`close_cursor`) and subscriptions stay on the node that created them; route
those workloads to the same node. A mixed `batch` containing any write is sent
to the primary as a whole.

## Databases

One daemon can host several independent databases. Every official client accepts
an explicit database and falls back to `NONODB_DB`, then `default`. Requests are
routed to `/<database>/v1/exec`; `default` may also use `/v1/exec`.

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
db := nonodb.New(
    nonodb.WithSocketPath("/run/nonodb.sock"),
    nonodb.WithDatabase("app1"),
)
```

Database names are case-sensitive and are not collections. Collections,
indexes, subscriptions, history, TTL, and events are isolated per database.
There are no cross-database queries, transactions, or joins. See
[Multiple databases](multi-database.md) for routing, management, limits, and
backup commands.

## Dates

Clients send dates as Extended JSON objects. Convert native datetimes to an
RFC 3339 string with `Z` or a numeric offset before calling the client:

```python
{"at": {"$date": "2026-01-01T01:00:00+01:00"}}
```

Epoch milliseconds are also accepted. The daemon normalizes the value to
canonical UTC milliseconds. Filter with the same representation and normal
operators (`$gte`, `$lt`, ...); see [Dates and time zones](dates.md).

## Numbers and decimals

Send Decimal128 values as Extended JSON objects; no client-side decimal
conversion is required:

```python
{"price": {"$numberDecimal": "199.99"}}
```

Use the same form in filters and update values. Ordinary JSON integers and
floats participate in the same numeric bracket. For `NaN` and `Infinity`, use
`{"$numberDouble": "NaN"}` or `{"$numberDouble": "Infinity"}`. Integer forms
`{"$numberInt": "42"}` and `{"$numberLong": "9007199254740993"}` are accepted.
See [Numbers and decimals](numerics.md).

## Binary, UUID, and ObjectId

Send binary, UUID, and ObjectId values as Extended JSON objects:

```python
{"payload": {"$binary": {"base64": "YQ==", "subType": "00"}}}
{"payload": {"$uuid": "00112233-4455-6677-8899-aabbccddeeff"}}
{"ref": {"$oid": "000000000000000000000001"}}
```

Use the same form in filters. `$uuid` is canonicalized to binary subtype `04`.
Query types with `{"$type": "binData"}` or `{"$type": "objectId"}`. See
[BSON scalar types](bson-types.md).

## Regular expressions

Use `$regex` with optional `$options`:

```python
users.find({"name": {"$regex": "^ada", "$options": "i"}})
users.find({"tags": {"$regex": "^rus"}})
```

Only string values match; arrays are checked element by element. Invalid
patterns/options return `invalid_query`. See [API filters](api.md#filters).

## Arrays

Array filters use the normal MongoDB operators:

```python
items.find({"tags": {"$all": ["rust", "lmdb"]}})
items.find({"tags": {"$size": 2}})
items.find({"items": {"$elemMatch": {"sku": "x", "qty": {"$gt": 5}}}})
```

Malformed operator arguments return `invalid_query`. Arrays are supported by
multikey secondary indexes; `$size` still requires a scan.

## Python

The Python client is documented separately in [Python client](python-client.md).

```python
from nonodb import NonoDB

with NonoDB("/run/nonodb.sock") as db:
    users = db.collection("users")
    users.insert({"name": "Ada", "age": 36})
    print(users.find({"age": {"$gte": 18}}))
    print(users.aggregate([
        {"$group": {"_id": "$tenant", "n": {"$sum": 1}}},
    ]))
```

## Rust

Add the client from this repository:

```toml
[dependencies]
nonodb-client = { git = "https://github.com/essmos/nonodb", package = "nonodb-client" }
serde_json = "1"
```

```rust
use nonodb_client::{Client, FindQuery};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Client::unix("/run/nonodb.sock")?;
    // TCP/TLS alternative:
    // let db = Client::builder()
    //     .base_url("https://127.0.0.1:7878")
    //     .token("secret")
    //     .build()?;

    let users = db.collection("users");
    let id = users.insert(&json!({"name": "Ada", "age": 36}))?;
    let query = FindQuery::new(json!({"age": {"$gte": 18}}))
        .sort("age", -1)
        .limit(10);
    let adults = users.find(&query)?;
    let rows = users.aggregate(&[
        json!({"$group": {"_id": "$tenant", "n": {"$sum": 1}}}),
    ])?;
    println!("{id}: {adults:?} {rows:?}");
    Ok(())
}
```

The Rust client exposes `exec`, `batch`, `write_batch`, `lookup`, `get_many`,
history, aggregation, indexes, text search, fuzzy search, vector search, and
online map resize.
`Client::from_env()` reads `NONODB_SOCK`,
`NONODB_URL`, `NONODB_TOKEN`, `NONODB_DB`, `NONODB_TLS_VERIFY`,
`NONODB_TLS_CA`, `NONODB_TLS_CLIENT_CERT`, and `NONODB_TLS_CLIENT_KEY`. For
mTLS:

```rust
let db = Client::builder()
    .base_url("https://127.0.0.1:7878")
    .ca_cert("/etc/nonodb/ca.crt")
    .client_cert("/etc/nonodb/client.crt", "/etc/nonodb/client.key")
    .build()?;
```

## Node.js

The Node client has no runtime dependencies and requires Node 18 or newer. It is
not yet published to npm; install it from this repository:

```bash
npm install /path/to/nonodb/client/node
```

```js
const { NonoDB } = require('nonodb-client');

async function main() {
  const db = new NonoDB(); // NONODB_SOCK/NONODB_URL and NONODB_DB
  const users = db.collection('users');

  const id = await users.insert({ name: 'Ada', age: 36 });
  const adults = await users.find(
    { age: { $gte: 18 } },
    {
      sort: [['age', -1]],
      limit: 10,
      comment: 'dashboard',
      maxTimeMS: 5000,
      hint: 'age',
    },
  );
  const total = await users.count({}, { maxTimeMS: 5000 });
  console.log(id, adults, total);
  db.close();
}

main().catch(console.error);
```

For TCP/TLS or mTLS:

```js
const db = new NonoDB({
  baseUrl: 'https://127.0.0.1:7878',
  token: 'secret',
  verify: true,
  caCert: '/etc/nonodb/ca.crt',
  clientCert: '/etc/nonodb/client.crt',
  clientKey: '/etc/nonodb/client.key',
});
```

## Go

```bash
go get github.com/essmos/nonodb/client/go@main
```

```go
package main

import (
    "context"
    "fmt"

    nonodb "github.com/essmos/nonodb/client/go"
)

func main() {
    ctx := context.Background()
    db := nonodb.New() // NONODB_SOCK/NONODB_URL and NONODB_DB
    users := db.Collection("users")

    id, err := users.Insert(ctx, map[string]any{"name": "Ada", "age": 36})
    if err != nil {
        panic(err)
    }

    adults, err := users.Find(ctx, nonodb.FindQuery{
        Filter: map[string]any{"age": map[string]any{"$gte": 18}},
        Sort:   []nonodb.SortField{{Field: "age", Direction: -1}},
    })
    if err != nil {
        panic(err)
    }
    fmt.Println(id, adults)
}
```

TCP/TLS or mTLS:

```go
db := nonodb.New(
    nonodb.WithBaseURL("https://127.0.0.1:7878"),
    nonodb.WithToken("secret"),
    nonodb.WithTLSVerify(true),
    nonodb.WithTLSCA("/etc/nonodb/ca.crt"),
    nonodb.WithTLSClientCert("/etc/nonodb/client.crt", "/etc/nonodb/client.key"),
)
```

The Go client methods take a `context.Context`; use it for cancellation and
deadlines. `FindQuery` also accepts `Comment`, `MaxTimeMS`, and `Hint`; use
`CountWithOptions`, `AggregateWithOptions`, and `FindOneWithOptions` for the
corresponding read options. `Exec` is available for operations not covered by a
convenience method.

## Write durability

All write payloads accept an optional `durability` field:

```json
{"op":"insert_many","coll":"imports","docs":[...],"durability":"relaxed"}
```

Python helpers expose it directly:

```python
users.insert(doc, durability="relaxed")
users.insert_many(docs, durability="relaxed")
users.update(filter, {"$set": {"x": 1}}, durability="full")
db.write_batch(ops, durability="relaxed")
users.aggregate([{"$merge": {"into": "totals"}}], durability="relaxed")
db.sync()
```

Clients classify a terminal `$out` or `$merge` aggregation as a write and route
it to the primary automatically. Rust, Node.js, and Go clients can send the same
field through their normal `exec`/`Exec` payload. `full` is the default. `unsafe` requires
`NONODB_ALLOW_UNSAFE_WRITES=1` on the daemon. See
[Write durability](api.md#write-durability) for guarantees and risks.

## Batch lookups

`lookup` fetches documents by `_id` or a unique index in one request,
preserving input order and returning `null` for missing keys. It is the
recommended replacement for N separate `find_one` calls. A non-`_id` lookup
requires a unique index. `by` may be an array for a compound unique index; each
value must then be an array in the same field order.

Python:

```python
articles.create_index("slug", unique=True, include=["title", "status"])

docs = articles.lookup(
    "slug",
    ["async-rust", "missing", "lmdb"],
    fields=["title", "status"],
)
# [doc, None, doc]

article = articles.lookup_one("slug", "async-rust", projection={"title": 1})
ids = articles.get_many(["a", "b", "missing"])
chunked = articles.lookup_chunked(
    "slug",
    large_slug_list,
    chunk_size=1000,
    fields=["title", "status"],
)

# Compound unique index: tenant_id + slug.
articles.create_index(fields=["tenant_id", "slug"], unique=True)
rows = articles.lookup(
    ["tenant_id", "slug"],
    [["acme", "async-rust"], ["other", "async-rust"], ["acme", "missing"]],
)
```

Rust:

```rust
use serde_json::json;

let slugs = vec!["async-rust", "missing", "lmdb"];
let docs = articles.lookup(
    "slug",
    &slugs,
    Some(&["title".to_string(), "status".to_string()]),
)?;
let article = articles.lookup_one(
    "slug",
    &"async-rust",
    None,
)?;

// Compound lookup or a projection excluding `_id`.
let keys = vec![json!(["acme", "async-rust"]), json!(["acme", "missing"])];
let rows = articles.lookup_with(
    &["tenant_id", "slug"],
    &keys,
    None,
    Some(&json!({"title": 1, "_id": 0})),
)?;
let ids = articles.get_many(["a", "b", "missing"])?;
let chunked = articles.lookup_chunked(
    "slug",
    &large_slug_list,
    Some(&["title".to_string(), "status".to_string()]),
    1000,
)?;
```

Node.js:

```js
await articles.createIndex('slug', { unique: true, include: ['title', 'status'] });

const docs = await articles.lookup('slug', ['async-rust', 'missing', 'lmdb'], {
  fields: ['title', 'status'],
});
const article = await articles.lookupOne('slug', 'async-rust', {
  projection: { title: 1, _id: 0 },
});
const ids = await articles.getMany(['a', 'b', 'missing']);
const chunked = await articles.lookupChunked('slug', largeSlugList, {
  chunkSize: 1000,
  fields: ['title', 'status'],
});

await articles.createIndex(['tenant_id', 'slug'], { unique: true });
const rows = await articles.lookup(
  ['tenant_id', 'slug'],
  [['acme', 'async-rust'], ['other', 'async-rust'], ['acme', 'missing']],
);
```

Go:

```go
_, err := articles.CreateIndex(ctx, []string{"slug"}, nonodb.IndexOptions{
    Unique:  true,
    Include: []string{"title", "status"},
})

docs, err := articles.Lookup(ctx, "slug", []any{
    "async-rust", "missing", "lmdb",
}, nonodb.LookupOptions{Fields: []string{"title", "status"}})

article, err := articles.LookupOne(ctx, "slug", "async-rust", nonodb.LookupOptions{
    Projection: map[string]any{"title": 1, "_id": 0},
})
ids, err := articles.GetMany(ctx, []string{"a", "b", "missing"})
chunked, err := articles.LookupChunked(ctx, "slug", largeSlugList,
    nonodb.LookupOptions{Fields: []string{"title", "status"}}, 1000)

_, err = articles.CreateIndex(ctx, []string{"tenant_id", "slug"}, nonodb.IndexOptions{
    Unique: true,
})
rows, err := articles.Lookup(ctx, []string{"tenant_id", "slug"}, []any{
    []any{"acme", "async-rust"},
    []any{"other", "async-rust"},
    []any{"acme", "missing"},
}, nonodb.LookupOptions{})
```

### Client-side joins

`lookup_join` attaches a matching document from another collection. It is a
thin client-side helper over `lookup_chunked`, not a server-side `$lookup`: it
fetches local documents once, looks up their keys in bounded batches, and adds
one field to each copy. Input order and length are preserved. A missing local
key or a missing foreign document becomes `null`/`Value::Null`/`nil`.

Python:

```python
posts = db.collection("posts")
local = posts.find({})
joined = posts.lookup_join(
    users,
    local,
    local_field="author_id",
    foreign_by="_id",
    as_field="author",
    fields=["name", "email"],
    chunk_size=1000,
)
```

Rust:

```rust
let local = posts.find(&FindQuery::new(json!({})))?;
let joined = posts.lookup_join(&users, &local, "author_id", Default::default())?;
```

Node.js:

```js
const local = await posts.find({});
const joined = await posts.lookupJoin(users, local, {
  localField: 'author_id',
  foreignBy: '_id',
  asField: 'author',
  fields: ['name', 'email'],
  chunkSize: 1000,
});
```

Go:

```go
local, err := posts.Find(ctx, map[string]any{})
joined, err := posts.LookupJoin(ctx, users, local, nonodb.LookupJoinOptions{
    LocalField: "author_id",
    AsField:    "author",
    Fields:     []string{"name", "email"},
    ChunkSize:  1000,
})
```

The helper does not modify the input documents and does not deduplicate local
rows. If several local documents use the same key, they all receive the same
foreign document. The foreign lookup still requires `_id` or a unique index.

Shared rules:

- `by` defaults to `_id`.
- `values` is capped at 10,000 per request; the result budget is 64 MiB
  (`result_too_large`).
- `lookup_chunked`/`lookupChunked`/`LookupChunked` split larger key lists into
  sequential requests. `chunk_size` defaults to 1,000 where optional and must
  be between 1 and 10,000. The concatenated output preserves input order and
  keeps one `null`/`Value::Null`/`nil` per missing key across chunk boundaries.
- Missing or malformed compound values become `null`; `get_many` keeps its old
  behavior and filters them out.
- `fields` is a top-level inclusive projection. `projection` accepts a normal
  flat projection, can exclude `_id`, and cannot be combined with `fields`.
  With a covering index, an inclusive projection avoids reading the document.
- Duplicate input keys produce duplicate results. Repeated keys are looked up
  only once internally.
- `lookup` itself is not a join and does not accept a filter or sort; use
  `lookup_join` for the client-side attachment described above, or `find` for
  arbitrary predicates. Non-unique lookups are not supported yet.

See [HTTP API: lookup](api.md#lookup) for the wire format.

## SQL

SQL returns an array of result objects. Parameters are positional; use `?` or
`$1` placeholders. `SELECT` is read-only; `INSERT`, `UPDATE`, and `DELETE` use the single
writer and return `inserted`/`inserted_ids`, `matched`/`modified`, or
`deleted`. `UPDATE SET` supports document expressions. `explain_sql` returns
the selected `SELECT` plan. See [SQL queries](sql.md) for the full
grammar and limitations.

Python:

```python
rows = db.sql(
    "SELECT DISTINCT team FROM users WHERE age >= $1 ORDER BY team",
    [18],
)
plan = db.explain_sql(
    "SELECT team, COUNT(*) AS n FROM users GROUP BY team HAVING COUNT(*) > 1"
)
inserted = db.sql(
    "INSERT INTO users (name, age) VALUES ($1, $2)",
    ["Ada", 36],
)
updated = db.sql(
    "UPDATE users SET age = age + 1 WHERE name = $1",
    ["Ada"],
)
deleted = db.sql("DELETE FROM users WHERE age < $1", [18])
```

Rust:

```rust
let rows = db.sql(
    "SELECT team, COUNT(*) AS n FROM users GROUP BY team HAVING COUNT(*) > $1",
    &[json!(1)],
)?;
let plan = db.explain_sql("SELECT * FROM users WHERE age >= $1", &[json!(18)])?;
let inserted = db.sql(
    "INSERT INTO users (name, age) VALUES ($1, $2)",
    &[json!("Ada"), json!(36)],
)?;
```

Node.js:

```js
const rows = await db.sql(
  "SELECT DISTINCT team FROM users GROUP BY team HAVING COUNT(*) > 1",
);
const plan = await db.explainSql('SELECT * FROM users WHERE age >= $1', [18]);
const inserted = await db.sql(
  'INSERT INTO users (name, age) VALUES ($1, $2)',
  ['Ada', 36],
);
```

Go:

```go
rows, err := db.SQL(ctx,
    "SELECT DISTINCT team FROM users WHERE age >= $1 ORDER BY team",
    18,
)
plan, err := db.ExplainSQL(ctx, "SELECT * FROM users WHERE age >= $1", 18)
inserted, err := db.SQL(ctx,
    "INSERT INTO users (name, age) VALUES ($1, $2)",
    "Ada", 36,
)
```

## Vector search

All four clients expose exact vector search. Embeddings are computed outside
NonoDB, by the application or a local embedding service. The indexed field must
be a JSON array of exactly `dims` finite numbers; one vector per document per
indexed field is supported. Vectors are maintained in the same transaction as
document writes.

Python:

```python
articles.create_vector_index("embedding", 384, metric="cosine")
articles.insert({"title": "Async Rust", "embedding": embed("Async Rust")})

hits = articles.vector_search(
    "embedding",
    embed("rust async"),
    filter={"published": True},
    skip=0,
    limit=10,
)
for hit in hits:
    print(hit["_distance"], hit["title"])

print(articles.list_vector_indexes())
articles.drop_vector_index("embedding")
```

Rust:

```rust
use serde_json::json;

let embedding = vec![0.12_f32, -0.03, 0.44];
articles.create_vector_index("embedding", 3, "cosine")?;
articles.insert(&json!({
    "title": "Async Rust",
    "embedding": embedding.clone(),
}))?;

let hits = articles.vector_search(
    "embedding",
    &embedding,
    Some(&json!({"published": true})),
    0,   // skip
    10,  // limit
)?;
for hit in &hits {
    println!("{} {}", hit["_distance"], hit["title"]);
}

let indexes = articles.list_vector_indexes()?;
articles.drop_vector_index("embedding")?;
```

Node.js:

```js
const queryVector = [0.12, -0.03, 0.44];

await articles.createVectorIndex('embedding', 3, 'cosine');
await articles.insert({ title: 'Async Rust', embedding: queryVector });

const hits = await articles.vectorSearch('embedding', queryVector, {
  filter: { published: true },
  skip: 0,
  limit: 10,
});
for (const hit of hits) {
  console.log(hit._distance, hit.title);
}

const indexes = await articles.listVectorIndexes();
await articles.dropVectorIndex('embedding');
```

Go:

```go
queryVector := []float32{0.12, -0.03, 0.44}

_, err := articles.CreateVectorIndex(ctx, "embedding", 3, "cosine")
_, err = articles.Insert(ctx, map[string]any{
    "title": "Async Rust", "embedding": queryVector,
})

hits, err := articles.VectorSearch(ctx, "embedding", queryVector, nonodb.VectorOptions{
    Filter: map[string]any{"published": true},
    Skip:   0,
    Limit:  10,
})
for _, hit := range hits {
    fmt.Println(hit["_distance"], hit["title"])
}

indexes, err := articles.ListVectorIndexes(ctx)
_, err = articles.DropVectorIndex(ctx, "embedding")
```

Behavior common to all clients:

- `metric` is `cosine` (default), `dot`, or `l2`.
- `_distance` is lower-is-better: `1 - cosine_similarity`, negative dot
  product, or squared Euclidean distance.
- Search is exact by default and scans the whole vector index.
- An optional IVF sidecar accelerates unfiltered queries and is approximate;
  filtered queries and post-write queries use the exact path. Recall depends on
  the data and probe count.
- Missing, malformed, or wrong-length vectors are skipped; writes still succeed.
- Creating an index backfills existing documents synchronously. Auto-index does
  not create vector indexes.
- Exact `_id` and indexed filters use a document-first path. A highly selective
  filter on an unindexed field can approach a full collection scan.
- Use the same embedding model, dimensions, and metric for stored vectors and
  queries. Changing the model requires re-embedding documents and rebuilding the
  index.

See [HTTP API: vector indexes](api.md#vector-indexes) and
[HTTP API: vector_search](api.md#vector_search) for the raw protocol.

## Vault collections

Vault collections require the three encryption headers on every request. Each
client has a credential-bound handle that adds them automatically.

Python:

```python
vault = db.vault("acme", "acme:v1", os.environ["ACME_VAULT_KEY"])
docs = vault.collection("vault")
```

Rust:

```rust
let vault = db.vault("acme", "acme:v1", std::env::var("ACME_VAULT_KEY")?);
let docs = vault.collection("vault");
```

Node.js:

```js
const vault = db.vault('acme', 'acme:v1', process.env.ACME_VAULT_KEY);
const docs = vault.collection('vault');
```

Go:

```go
vault := client.Vault("acme", "acme:v1", os.Getenv("ACME_VAULT_KEY"))
docs := vault.Collection("vault")
```

All four handles use the normal CRUD methods. In a vault collection, only
`_id`/`tenant_id` filters and single-document update/delete are valid. `batch`,
`write_batch`, search, fuzzy, aggregation, and indexes are rejected by the
daemon. See [Vault collections](vault.md).

## API coverage

All clients expose the common document operations:

- `create_collection` for explicit standard/vault/time-series/columnar
  collection creation, with an optional default TTL (`create_collection_with_ttl`
  in Rust, optional arguments in Python/Node.js, `CreateCollectionWithTTL` in
  Go). For time-series, pass the `timeseries` settings object with the required
  `timeField` (and optional `metaField`, `granularity`, `expireAfterSeconds`).
  For columnar, pass `columnar` with optional `bucket_max_docs`,
  `bucket_max_bytes`, and `compression`;
- `get_collection_columnar` / `set_collection_columnar` through the wire API,
  Rust/Python/Node.js/Go clients, and CLI;
- `get_collection_timeseries` / `set_collection_timeseries` and
  `retain_timeseries` through the wire API and CLI;
- `set_collection_ttl`/`get_collection_ttl` for insertion-based collection TTL
  defaults and optional history purging on expiry; the Python client also
  exposes `set_collection_ttl_after`, `backfill_collection_ttl`, and
  `get_collection_ttl_field` for date-field TTL;
- `capabilities` for the public, versioned `/v1/capabilities` document;
- `insert`, `insert_many`, `find`, `find_one`, `update`, `delete`, `count`;
- `aggregate` for reporting pipelines, plus transactional `$out`/`$merge`
  write stages;
- `batch` (read-only) and `write_batch` (atomic writes);
- `lookup`/`lookup_one` for ordered point lookups by `_id` or a unique index,
  `lookup_chunked`/`lookupChunked`/`LookupChunked` for bounded batches, plus
  `get_many` for fetching documents by `_id`;
- `watch`/`watch_once` for append-only outbox events, with filters, projections,
  resume tokens, and reconnect helpers; Python also exposes `watch_stream` for
  continuous NDJSON and `AsyncNonoDB` for async watch/stream consumption
  ([details](streams.md));
- `history` and `get_version` for document revisions (per-collection history enabled);
- single-field, compound, unique, and covering indexes;
- full-text (`search`), fuzzy search, and exact vector search;
- text/fuzzy/vector index listing and deletion;
- collection listing and health checks;
- LMDB map information/growth in Python and Rust (or raw `exec` elsewhere);
- vault collection handles in Python, Rust, Node.js, and Go.

Use `exec`/`Exec`/`exec` for an operation that does not have a typed wrapper.
