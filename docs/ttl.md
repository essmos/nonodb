# Document TTL

nonodb supports optional document expiry with a collection default and
per-document overrides. TTL is **eventual**: a document remains readable until
the maintenance worker sweeps it, normally within a few seconds of its expiry.
There is no strict read-time filtering.

TTL is supported for both standard and vault collections. For vault
collections the expiry metadata is plaintext; the document body is never
decrypted by the sweeper.

## Collection default

Set a default TTL in seconds when creating a collection:

```json
{
  "op": "create_collection",
  "coll": "sessions",
  "ttl_seconds": 86400
}
```

Or change it later:

```json
{"op": "set_collection_ttl", "coll": "sessions", "ttl_seconds": 3600}
```

Clear the default with `null` (or omit `ttl_seconds`):

```json
{"op": "set_collection_ttl", "coll": "sessions", "ttl_seconds": null}
```

Read the current default:

```json
{"op": "get_collection_ttl", "coll": "sessions"}
```

Response:

```json
{"ok": true, "result": {"coll": "sessions", "ttl_seconds": 3600, "purge_history": false}}
```

`ttl_purge_history`/`purge_history` controls whether an expired document's
stored revisions are deleted instead of appending a delete tombstone:

```json
{
  "op": "create_collection",
  "coll": "sessions",
  "ttl_seconds": 86400,
  "ttl_purge_history": true
}
```

```json
{"op":"set_collection_ttl","coll":"sessions","ttl_seconds":3600,"purge_history":true}
```

`purge_history` defaults to `false`; omitting it on `set_collection_ttl`
preserves the current setting.

A collection default applies only to documents inserted **after** the default
is configured. Changing or clearing the default does not rewrite existing
documents. `ttl_seconds` must be a positive integer.

## Per-document TTL

Use the reserved top-level `_ttl` field in an inserted document:

```json
{
  "op": "insert",
  "coll": "sessions",
  "doc": {"_id": "s1", "user_id": 7, "_ttl": 300}
}
```

Rules:

- `_ttl` is a positive integer number of seconds and overrides the collection
  default.
- `_ttl: 0` or `_ttl: null` disables expiry for that document, even when the
  collection has a default.
- If `_ttl` is absent, the collection default applies.
- If neither is set, the document never expires.
- Negative, fractional, or non-numeric values are rejected with
  `invalid_ttl`.
- `_ttl` is removed before the document is stored and is not returned by reads.
  It is an input directive, not a document field.

## Updates

A normal update preserves the existing expiry:

```json
{
  "op": "update",
  "coll": "sessions",
  "filter": {"_id": "s1"},
  "update": {"$set": {"last_seen": "2026-01-01T00:00:00Z"}}
}
```

Set a new TTL relative to the update time:

```json
{"op": "update", "coll": "sessions", "filter": {"_id": "s1"}, "update": {"$set": {"_ttl": 900}}}
```

Disable expiry:

```json
{"op": "update", "coll": "sessions", "filter": {"_id": "s1"}, "update": {"$set": {"_ttl": 0}}}
```

Replacement updates and upserts follow the same rule: an explicit `_ttl` in the
replacement sets the expiry; otherwise an existing expiry is preserved. A
document created by an upsert uses the normal insert semantics and therefore
inherits the collection default. `$unset: {"_ttl": ...}` does not clear an
expiry; use `$set` with `0` or `null`.

## Deletion and history

An expired document is deleted through the normal write path:

- the document is removed from the primary collection;
- secondary, text, and fuzzy index entries are removed;
- TTL metadata is removed;
- when document history is enabled, a `delete` tombstone is appended, unless
  the collection has `purge_history: true`, in which case all stored revisions
  for that document are removed instead.

The delete and its history change are committed in one LMDB transaction.
Purging history is irreversible and intended for short-lived data where old
versions must not outlive the document.

## Maintenance and visibility

The daemon has one background maintenance worker. It never writes to LMDB
directly; expiry jobs are submitted to the single writer thread.

```text
TTL sub-database (sorted by expiry)
        │
        ▼
maintenance worker ── Expire job ──▶ writer thread ──▶ LMDB commit
```

The worker:

- scans due TTL entries every `NONODB_TTL_SWEEP_INTERVAL` seconds;
- deletes at most `NONODB_TTL_SWEEP_LIMIT` entries per pass;
- re-checks the expected expiry under the write transaction, so a concurrent
  update that refreshed the TTL is not deleted by a stale sweep;
- removes orphaned TTL entries when a collection no longer exists.

Because expiry is eventual, a document may be returned by reads for a short
time after its `_ttl_at` timestamp. This is deliberate: strict expiry would
require every `find`, `count`, `search`, `aggregate`, and index path to filter
expired documents.

## Vault collections

Collection defaults and `_ttl` work for vault collections. The stored envelope
is deleted without decryption, so the sweeper does not need the tenant key.
TTL metadata contains only the collection name, `_id`, and expiry timestamp.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `NONODB_TTL_SWEEP_INTERVAL` | `5` | Seconds between TTL sweeps |
| `NONODB_TTL_SWEEP_LIMIT` | `256` | Maximum due documents processed per sweep |
| `NONODB_MAINTENANCE_INTERVAL_MS` | `1000` | Maintenance loop heartbeat in milliseconds |

Metrics expose `ttl_sweeps_total`, `ttl_expired_total`,
`ttl_sweep_errors_total`, and the last due backlog as `ttl_backlog` on
`/metrics`.

## Interaction with snapshots and backups

TTL timestamps are stored in the same LMDB environment as documents and are
included in snapshots and backups. Restoring an older snapshot restores its TTL
entries as well. Documents whose expiry passed during the gap are removed by the
next maintenance sweep after the daemon starts.

## Limits

- TTL is eventual, not strict.
- The sweeper deletes bounded batches; a very large expiry burst is drained
  over multiple passes.
- TTL is not a substitute for access control or retention compliance. Use normal
  deletes when immediate removal is required.
