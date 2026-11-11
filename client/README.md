# nonodb clients

- [Python](nonodb.py) — sync client with optional cache, retries, and FastAPI example.
- [Rust](../nonodb-client) — sync crate using `reqwest`; Unix socket or TCP/TLS.
- [Node.js](node) — zero-dependency async client; Unix socket or TCP/TLS.
- [Go](go) — context-aware client; Unix socket or TCP/TLS.

See [docs/clients.md](../docs/clients.md) for installation, TCP/TLS, mTLS, and
examples. All clients accept `NONODB_TLS_CA`, `NONODB_TLS_CLIENT_CERT`, and
`NONODB_TLS_CLIENT_KEY` when connecting to a daemon that requires client
certificates.

## Batch lookups

All clients expose `lookup`/`lookup_one` for ordered point lookups by `_id` or a
matching unique single-field or compound index. They support covering fields,
projection objects, and a 64 MiB result budget. Missing keys produce `null`;
`get_many` keeps its old filtering behavior. See
[Batch lookups](../docs/clients.md#batch-lookups) for examples in Python, Rust,
Node.js, and Go.

## Vector search

All clients support exact vector search with `cosine`, `dot`, and `l2` metrics.
Use the client-specific `create_vector_index` / `vector_search` helpers; see
[Vector search](../docs/clients.md#vector-search) for examples in Python, Rust,
Node.js, and Go.

## Document history

When the daemon is started with `NONODB_HISTORY=1`, all clients expose
history, exact-revision helpers, and aggregation.

## Document TTL

Collection defaults and per-document `_ttl` overrides are available through
the normal API and the Python `db.create_collection(..., ttl_seconds=...)`,
`set_collection_ttl`, and `get_collection_ttl` helpers. See
[Document TTL](../docs/ttl.md).

## Vault collections

Use `db.vault(tenant, key_id, key)` for a handle that sends the three vault
headers on every request. Vault collections support CRUD by `_id`, list/count
by `tenant_id`, and history; search, indexes, aggregation, `batch`, and
`write_batch` are rejected. See [Vault collections](../docs/vault.md).

## SQL

All official clients expose read-only `db.sql(query, params)` / `client.sql(...)`.
See [SQL queries](../docs/sql.md) and [Clients](../docs/clients.md#sql) for
language-specific examples.
