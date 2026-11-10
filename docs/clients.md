# Clients

nonodb has thin official clients for Python, Rust, Node.js, and Go. They all
speak the same HTTP/JSON API and keep no state beyond one HTTP connection. The
daemon remains responsible for queries, indexes, updates, search, and
transactions.

| Language | Location | Style | Unix socket | TCP/TLS | mTLS |
|---|---|---|---|---|---|
| Python | [`client/nonodb.py`](../client/nonodb.py) | sync | yes | yes | yes |
| Rust | [`nonodb-client`](../nonodb-client) | sync | yes (Unix) | yes | yes |
| Node.js | [`client/node`](../client/node) | async / Promise | yes (Unix) | yes | yes |
| Go | [`client/go`](../client/go) | context-aware | yes (Unix) | yes | yes |

## Common configuration

On Linux and macOS the clients use the Unix socket by default. Set
`NONODB_SOCK` to change the path. A base URL switches a client to TCP/TLS:

```bash
export NONODB_SOCK=/run/nonodb.sock   # default Unix socket
export NONODB_TOKEN='...'             # required if auth is enabled
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
`NONODB_TOKEN` is required because TCP always requires a bearer token.

All clients return a typed error for daemon errors (`ok: false`). Writes are not
retried automatically; read operations are retried after a transport failure,
because retrying a write could duplicate it. Use `write_batch` when several
writes must succeed or fail together.

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

The Rust client exposes `exec`, `batch`, `write_batch`, `get_many`, history,
aggregation, indexes, text search, and fuzzy search. `Client::from_env()` reads `NONODB_SOCK`,
`NONODB_URL`, `NONODB_TOKEN`, `NONODB_TLS_VERIFY`, `NONODB_TLS_CA`,
`NONODB_TLS_CLIENT_CERT`, and `NONODB_TLS_CLIENT_KEY`. For mTLS:

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
  const db = new NonoDB(); // NONODB_SOCK on Unix, or NONODB_URL on Windows
  const users = db.collection('users');

  const id = await users.insert({ name: 'Ada', age: 36 });
  const adults = await users.find(
    { age: { $gte: 18 } },
    { sort: [['age', -1]], limit: 10 },
  );
  console.log(id, adults);
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
    db := nonodb.New() // NONODB_SOCK, or NONODB_URL on Windows
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
deadlines. `Exec` is available for operations not covered by a convenience
method.

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

- `create_collection` for explicit standard/vault collection creation, with an
  optional default TTL (`create_collection_with_ttl` in Rust, an optional
  argument in Python/Node.js, `CreateCollectionWithTTL` in Go);
- `set_collection_ttl`/`get_collection_ttl` for collection TTL defaults and
  optional history purging on expiry;
- `insert`, `insert_many`, `find`, `find_one`, `update`, `delete`, `count`;
- `aggregate` for read-only reporting pipelines;
- `batch` (read-only) and `write_batch` (atomic writes);
- `get_many` for fetching documents by `_id`;
- `history` and `get_version` for document revisions (daemon history enabled);
- single-field, compound, unique, and covering indexes;
- full-text (`search`) and fuzzy search;
- text/fuzzy index listing and deletion;
- collection listing and health checks;
- vault collection handles in Python, Rust, Node.js, and Go.

Use `exec`/`Exec`/`exec` for an operation that does not have a typed wrapper.
