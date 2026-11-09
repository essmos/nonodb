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
