# nonodb clients

- [Python](nonodb.py) — sync client with optional cache, retries, and FastAPI example.
- [Rust](../nonodb-client) — sync crate using `reqwest`; Unix socket or TCP/TLS.
- [Node.js](node) — zero-dependency async client; Unix socket or TCP/TLS.
- [Go](go) — context-aware client; Unix socket or TCP/TLS.

See [docs/clients.md](../docs/clients.md) for installation and examples.

## Document history

When the daemon is started with `NONODB_HISTORY=1`, all clients expose
history, exact-revision helpers, and aggregation.

## Vault collections

Use `db.vault(tenant, key_id, key)` for a handle that sends the three vault
headers on every request. Vault collections support CRUD by `_id`, list/count
by `tenant_id`, and history; search, indexes, aggregation, `batch`, and
`write_batch` are rejected. See [Vault collections](../docs/vault.md).
