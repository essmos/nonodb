# Per-database quotas

Each database can have independent storage limits. Quotas protect a shared
instance from one application filling the disk while leaving other databases
usable.

## Resources

| Resource | What it covers |
|---|---|
| `data_bytes` | Stored documents and TTL metadata |
| `index_bytes` | Secondary and vector index storage |
| `history_bytes` | Document revision history |
| `sidecar_bytes` | Search and vector sidecar files |

Usage is the reported stored size, not the size of the JSON payload. Values are
in bytes. A limit applies only to its own resource; setting a data limit does
not restrict index creation.

Search and vector sidecars are counted separately from indexes because they are
stored as immutable files. WAL and PITR snapshots have their own retention
limits and are not part of the quota.

## Configuration precedence

A database uses, in order:

1. limits saved with `set_quota` in that database;
2. environment defaults for limits that were never saved.

The environment defaults are read when a database is first opened:

| Variable | Default |
|---|---|
| `NONODB_QUOTA_DATA_BYTES` | unlimited |
| `NONODB_QUOTA_INDEX_BYTES` | unlimited |
| `NONODB_QUOTA_HISTORY_BYTES` | unlimited |
| `NONODB_QUOTA_SIDECAR_BYTES` | unlimited |
| `NONODB_QUOTA_SOFT_PERCENT` | `80` |

Sizes accept bytes or suffixes such as `512MiB`, `10GiB`, and `2GB`. A saved
configuration survives restarts. Setting a limit to `null` or `0` clears it.
Saving an empty configuration disables all quotas for that database and does not
re-apply environment defaults later.

Example:

```bash
NONODB_QUOTA_DATA_BYTES=20GiB \
NONODB_QUOTA_INDEX_BYTES=5GiB \
NONODB_QUOTA_HISTORY_BYTES=2GiB \
NONODB_QUOTA_SIDECAR_BYTES=5GiB \
  nonodb-daemon
```

## Reading usage

```json
{"op":"get_quota"}
```

```json
{
  "ok": true,
  "result": {
    "database": "app1",
    "limits": {
      "data_bytes": 21474836480,
      "sidecar_bytes": 5368709120
    },
    "usage": {
      "data_bytes": 19327352832,
      "index_bytes": 2097152,
      "history_bytes": 0,
      "sidecar_bytes": 1048576
    },
    "status": "warning",
    "soft_limit_percent": 80,
    "warnings": [
      {
        "resource": "data",
        "used_bytes": 19327352832,
        "limit_bytes": 21474836480,
        "used_percent": 90,
        "severity": "warning",
        "message": "data usage is 19327352832 of 21474836480 bytes (90%)"
      }
    ]
  }
}
```

`status` is `ok`, `warning`, or `exceeded`. A resource starts warning at the
soft percentage and is exceeded when usage reaches the hard limit. The
`database_stats` response includes the same `quota` object.

## Changing limits

```json
{
  "op": "set_quota",
  "limits": {
    "data_bytes": 21474836480,
    "index_bytes": 5368709120,
    "history_bytes": 2147483648,
    "sidecar_bytes": 5368709120
  }
}
```

The document replaces the whole saved configuration. Omitted fields, `null`, and
`0` mean unlimited. The operation requires a database administrator scope.

From the CLI:

```bash
nonodb-cli --db app1 quota
nonodb-cli --db app1 quota set --data 20GiB --index 5GiB --history 2GiB --sidecar 5GiB
nonodb-cli --db app1 quota set --data 20GiB
nonodb-cli --db app1 quota set --data unlimited
nonodb-cli --db app1 quota reset
```

`quota set` changes only the named limits and keeps the others.

## Enforcement

When a growing write would cross a hard limit, the write fails with HTTP `507`
and error code `quota_exceeded`. The failed write is not partially applied. This
includes inserts, updates, index builds, text/fuzzy/vector index creation, and
SQL or aggregation writes that add storage.

Deletes, collection and index drops, history pruning, TTL expiry, and other
space-releasing operations remain allowed after a quota is reached, so an
operator can recover without raising the limit. Once usage is below the limit,
or the limit is raised or cleared, writes resume.

The check is conservative for grouped writes: a request may be rejected when
the limit is already exhausted even if that particular request would not have
added bytes to the exhausted resource. Retry after freeing space.

The soft warning appears in `get_quota`, `database_stats`, and the CLI, and the
daemon logs a warning. Rejections are logged as errors.
