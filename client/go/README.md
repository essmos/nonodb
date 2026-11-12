# nonodb-client (Go)

Thin context-aware Go client for [`nonodb`](../../README.md).

```bash
go get github.com/essmos/nonodb/client/go@main
```

```go
db := nonodb.New() // NONODB_SOCK on Unix, NONODB_URL on Windows
users := db.Collection("users")

id, err := users.Insert(ctx, map[string]any{"name": "Ada", "age": 36})
```

TCP/TLS:

```go
db := nonodb.New(
    nonodb.WithBaseURL("https://127.0.0.1:7878"),
    nonodb.WithToken("secret"),
    nonodb.WithTLSVerify(true),
    nonodb.WithTLSCA("/etc/nonodb/ca.crt"),
    nonodb.WithTLSClientCert("/etc/nonodb/client.crt", "/etc/nonodb/client.key"),
)
```

The same values are read from `NONODB_TLS_CA`, `NONODB_TLS_CLIENT_CERT`, and
`NONODB_TLS_CLIENT_KEY`.

See [docs/clients.md](../../docs/clients.md) for the full client guide. Run
`NONODB_SOCK=/run/nonodb.sock go run ./cmd/smoke` against a daemon for an
end-to-end check.

## Document history

Enable history per collection with `set_collection_history(coll, max_versions)`, then use `History()` and
`GetVersion()` on a collection.

## Document TTL

```go
ttl := uint64(86400)
err := client.CreateCollectionWithTTL(ctx, "sessions", "standard", &ttl)
err = client.SetCollectionTTL(ctx, "sessions", nil)
current, err := client.CollectionTTL(ctx, "sessions")
```

Documents use the reserved `_ttl` field for a per-document override. See
[Document TTL](../../docs/ttl.md).

## Aggregation

```go
rows, err := users.Aggregate(ctx, []any{
    map[string]any{"$match": map[string]any{"active": true}},
    map[string]any{"$group": map[string]any{
        "_id": "$tenant", "total": map[string]any{"$sum": "$amount"},
    }},
    map[string]any{"$sort": map[string]any{"total": -1}},
    map[string]any{"$limit": 10},
})
```

## SQL

```go
rows, err := db.SQL(ctx,
    "SELECT name, age FROM users WHERE age >= $1 ORDER BY age DESC LIMIT 10",
    18,
)
```

SQL supports `SELECT`, `INSERT`, `UPDATE`, and `DELETE` over one collection. `db.ExplainSQL(ctx, query, params...)`
returns the selected `SELECT` plan. See [SQL queries](../../docs/sql.md).

## Batch lookups

```go
_, err := articles.CreateIndex(ctx, []string{"slug"}, nonodb.IndexOptions{
    Unique:  true,
    Include: []string{"title", "status"},
})

documents, err := articles.Lookup(ctx, "slug", []any{
    "async-rust", "missing", "lmdb",
}, nonodb.LookupOptions{Fields: []string{"title", "status"}})

article, err := articles.LookupOne(ctx, "slug", "async-rust", nonodb.LookupOptions{
    Projection: map[string]any{"title": 1, "_id": 0},
})
byID, err := articles.GetMany(ctx, []string{"a", "b", "missing"})

_, err = articles.CreateIndex(ctx, []string{"tenant_id", "slug"}, nonodb.IndexOptions{
    Unique: true,
})
rows, err := articles.Lookup(ctx, []string{"tenant_id", "slug"}, []any{
    []any{"acme", "async-rust"},
    []any{"other", "async-rust"},
    []any{"acme", "missing"},
}, nonodb.LookupOptions{})
```

Results preserve input order and use `nil` for missing keys. A non-`_id` lookup
requires a unique index with matching field order; compound values are arrays.
`Projection` cannot be combined with `Fields`, and the result budget is capped
at 64 MiB. See [Batch lookups](../../docs/clients.md#batch-lookups).

## Vector search

```go
articles := db.Collection("articles")
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

`_distance` is lower-is-better. Search is exact and scans the index. Missing or
wrong-length vectors are skipped. See [Vector search](../../docs/clients.md#vector-search)
for common semantics and the other clients.

## Vault collections

```go
vault := db.Vault("acme", "acme:v1", os.Getenv("ACME_VAULT_KEY"))
docs := vault.Collection("vault")
```

See [Vault collections](../../docs/vault.md).
