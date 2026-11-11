# nonodb-client (Rust)

Thin synchronous Rust client for [`nonodb`](../README.md). It uses the daemon's
HTTP/JSON API over a Unix socket or TCP/TLS and keeps no query logic locally.

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
    let users = db.collection("users");

    let id = users.insert(&json!({"name": "Ada", "age": 36}))?;
    let adults = users.find(&FindQuery::new(json!({"age": {"$gte": 18}})))?;
    println!("{id}: {adults:?}");
    Ok(())
}
```

TCP/TLS:

```rust
let db = Client::builder()
    .base_url("https://127.0.0.1:7878")
    .token("secret")
    .ca_cert("/etc/nonodb/ca.crt")
    .client_cert("/etc/nonodb/client.crt", "/etc/nonodb/client.key")
    .build()?;
```

`Client::from_env()` reads `NONODB_SOCK`, `NONODB_URL`, `NONODB_TOKEN`,
`NONODB_TLS_VERIFY`, `NONODB_TLS_CA`, `NONODB_TLS_CLIENT_CERT`, and
`NONODB_TLS_CLIENT_KEY`. See [docs/clients.md](../docs/clients.md) for the full
client guide.

## Document history

When the daemon is started with `NONODB_HISTORY=1`, use `history()` and
`get_version()` on a collection.

## Document TTL

```rust
client.create_collection_with_ttl("sessions", "standard", Some(86_400))?;
client.set_collection_ttl("sessions", Some(3_600))?;
let ttl = client.collection_ttl("sessions")?;
```

Documents use the reserved `_ttl` field for a per-document override. See
[Document TTL](../docs/ttl.md).

## Aggregation

```rust
let rows = users.aggregate(&[
    json!({"$match": {"active": true}}),
    json!({"$group": {"_id": "$tenant", "total": {"$sum": "$amount"}}}),
    json!({"$sort": {"total": -1}}),
    json!({"$limit": 10}),
])?;
```

## Batch lookups

```rust
use serde_json::json;

articles.create_index(&["slug"], true, &["title", "status"])?;

let slugs = vec!["async-rust", "missing", "lmdb"];
let fields = ["title".to_string(), "status".to_string()];
let documents = articles.lookup("slug", &slugs, Some(&fields))?;
let article = articles.lookup_one(
    "slug",
    &"async-rust",
    None,
)?;
let by_id = articles.get_many(["a", "b", "missing"])?;

// Compound unique index or projection excluding `_id`.
let keys = vec![json!(["acme", "async-rust"]), json!(["acme", "missing"])];
let rows = articles.lookup_with(
    &["tenant_id", "slug"],
    &keys,
    None,
    Some(&json!({"title": 1, "_id": 0})),
)?;
```

Results preserve input order and use `Value::Null` for missing keys. A non-`_id`
lookup requires a unique index with matching field order; compound values are
arrays. The result budget is capped at 64 MiB. See
[Batch lookups](../docs/clients.md#batch-lookups).

## Vector search

```rust
use serde_json::json;

let articles = db.collection("articles");
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
let indexes = articles.list_vector_indexes()?;
articles.drop_vector_index("embedding")?;
```

`_distance` is lower-is-better. Search is exact and scans the index. Missing or
wrong-length vectors are skipped. See [Vector search](../docs/clients.md#vector-search)
for common semantics and the other clients.

## Vault collections

```rust
let vault = db.vault("acme", "acme:v1", std::env::var("ACME_VAULT_KEY")?);
let docs = vault.collection("vault");
```

See [Vault collections](../docs/vault.md).
