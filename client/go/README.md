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
)
```

See [docs/clients.md](../../docs/clients.md) for the full client guide. Run
`NONODB_SOCK=/run/nonodb.sock go run ./cmd/smoke` against a daemon for an
end-to-end check.

## Document history

When the daemon is started with `NONODB_HISTORY=1`, use `History()` and
`GetVersion()` on a collection.

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
