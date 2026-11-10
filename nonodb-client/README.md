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
    .build()?;
```

`Client::from_env()` reads `NONODB_SOCK`, `NONODB_URL`, `NONODB_TOKEN`, and
`NONODB_TLS_VERIFY`. See [docs/clients.md](../docs/clients.md) for the full
client guide.

## Document history

When the daemon is started with `NONODB_HISTORY=1`, use `history()` and
`get_version()` on a collection.

## Aggregation

```rust
let rows = users.aggregate(&[
    json!({"$match": {"active": true}}),
    json!({"$group": {"_id": "$tenant", "total": {"$sum": "$amount"}}}),
    json!({"$sort": {"total": -1}}),
    json!({"$limit": 10}),
])?;
```
