# Command-line interface

`nonodb-cli` is a small administrative client. On Linux and macOS, commands
that talk to the daemon use the Unix socket (`NONODB_SOCK`, default
`/run/nonodb.sock`) and the optional bearer token (`NONODB_TOKEN`). On Windows,
daemon-facing commands use plaintext HTTP over TCP (`NONODB_URL`, default
`http://127.0.0.1:7878`). The CLI does not support TLS or mTLS; use an official
client for encrypted connections. File commands (`backup`, `inspect`, `restore`)
operate directly on an LMDB data directory on every platform.

```bash
nonodb-cli <command> [arguments]
```

Run `nonodb-cli help` for a summary.

## ping

Check that the daemon is reachable and healthy:

```bash
NONODB_SOCK=/tmp/nonodb.sock nonodb-cli ping
```

```json
{"ok": true}
```

## collections

List known collections:

```bash
nonodb-cli collections
```

## count

Count documents in a collection, optionally with a filter:

```bash
nonodb-cli count users
nonodb-cli count users '{"active": true}'
nonodb-cli count users '{"age": {"$gte": 18}}'
```

The filter argument is JSON. It defaults to `{}`.

## find

Query documents:

```bash
nonodb-cli find users
nonodb-cli find users '{"age": {"$gte": 18}}'
nonodb-cli find users '{}' --sort age:desc --limit 20
nonodb-cli find users '{"tenant":"acme"}' --sort created_at:desc --skip 40 --limit 20
```

The filter must come before options. Defaults: no filter, `limit=100`, `skip=0`.
The output is pretty-printed JSON.

## indexes

List secondary indexes for a collection:

```bash
nonodb-cli indexes users
```

The output includes the index name, fields, uniqueness, and `include` fields.

## map

Show the LMDB address-space limit, high-water mark, and page size:

```bash
nonodb-cli map
```

```json
{
  "map_size": 1073741824,
  "used_bytes": 7340032,
  "free_bytes": 1066401792,
  "page_size": 4096
}
```

## resize

Grow the LMDB map without restarting the daemon:

```bash
nonodb-cli resize 4GiB
nonodb-cli resize 4294967296
```

```json
{"old_size": 1073741824, "map_size": 4294967296}
```

The size must be page-aligned and at least the current map size. The command
waits for active transactions; use a longer `NONODB_TIMEOUT` if a large write
is in progress. It only grows the map. To shrink the reservation, stop the
daemon, lower `NONODB_MAP_SIZE`, and restart; the file itself only shrinks via a
compacting backup/restore. Do not run `backup` or `inspect` concurrently with
an online resize.

## exec

Send an arbitrary `/v1/exec` request:

```bash
nonodb-cli exec '{"op":"create_text_index","coll":"articles","field":"body"}'
nonodb-cli exec '{"op":"search","coll":"articles","field":"body","query":"rust","limit":5}'
nonodb-cli exec '{"op":"write_batch","ops":[
  {"op":"insert","coll":"audit","doc":{"event":"login"}},
  {"op":"update","coll":"counters","filter":{"_id":"logins"},"update":{"$inc":{"n":1}}}
]}'
```

This is the escape hatch for operations not wrapped by a dedicated command. It
prints the full response envelope.

## backup

Create a consistent, compacted copy of an LMDB data directory:

```bash
nonodb-cli backup ./data ./backup
```

```text
backup: ./data -> ./backup/data.mdb
manifest: ./backup/manifest.json (sha256 ...)
```

- The source is opened read-only.
- The copy can be made while the daemon is running.
- `CompactionOption::Enabled` omits free pages, so the backup is usually smaller
  than the live `data.mdb`.
- The output directory is created if needed.
- Only `data.mdb` is copied. `lock.mdb` is not part of the backup.
- The copy is synced to disk before the command exits.
- `manifest.json` records the source, timestamp, file size, and SHA-256.

The resulting directory is a valid nonodb data directory. To inspect and verify it:

```bash
nonodb-cli inspect ./backup
nonodb-cli verify-backup ./backup
```

## verify

Deep-check a data directory without a manifest:

```bash
nonodb-cli verify ./data
nonodb-cli verify ./data --json
```

The command opens the database read-only and checks:

- the schema version and collection registry;
- every document's JSON and `_id` consistency;
- every secondary index entry against its document;
- full-text postings, document lengths, and total token counts;
- fuzzy trigram posting counts;
- history and TTL entry counts.

Legacy index formats are counted but not reconstructed exactly.

## verify-backup

Verify `manifest.json`, the SHA-256 checksum, and the full database:

```bash
nonodb-cli verify-backup ./backup
nonodb-cli verify-backup ./backup --json
```

The JSON form returns both the manifest and the verification report. A non-zero
exit status means the backup must not be used.

```text
manifest: source=./data created_at_ms=... size=... sha256=...
verified: ./backup
schema: 1
collections: 2
documents: 1000
...
```

## restore drill

`scripts/restore_drill.py` automates the full backup → verify → restore →
query cycle. See [Operations](operations.md#restore-drill).

## inspect

Print database and per-collection document counts without starting the daemon:

```bash
nonodb-cli inspect ./data
nonodb-cli inspect ./data users
```

```text
data: ./data
collections: 2
  users: 1000
  settings: 12
```

If a collection is given, only that collection is printed. The command opens
the environment read-only with room for all named databases. The daemon should
normally be stopped, or at least not writing, while inspecting; LMDB still
provides a consistent read snapshot.

## restore

Restore a backup into a data directory:

```bash
# Stop the daemon first.
systemctl stop nonodb

nonodb-cli restore ./backup ./data --force
nonodb-cli restore ./backup ./data --force --verify
```

```text
restore: ./backup/data.mdb -> ./data/data.mdb
verified: ./data
...
```

Rules and warnings:

- **Stop the daemon before restoring.** Replacing `data.mdb` under a running
  process is unsafe.
- `--force` is required when the destination `data.mdb` already exists.
- The command removes a stale `lock.mdb` from the destination.
- The backup can be either a directory containing `data.mdb` or a direct path
  to a `data.mdb` file.
- If the backup directory has `manifest.json`, its size and SHA-256 are checked
  before copying.
- `--verify` deep-checks the restored directory after the copy.
- Start the daemon after the restore and verify counts and indexes.

## Environment

| Variable | Default | Used by |
|---|---|---|
| `NONODB_SOCK` | `/run/nonodb.sock` | Daemon-facing commands on Linux/macOS |
| `NONODB_URL` | `http://127.0.0.1:7878` | Daemon-facing commands on Windows |
| `NONODB_TOKEN` | — | Bearer token for daemon-facing commands |

`backup`, `inspect`, and `restore` do not use the socket or token.

## Typical procedures

### Online backup

```bash
TS=$(date -u +%Y%m%dT%H%M%SZ)
nonodb-cli backup /var/lib/nonodb /var/backups/nonodb/$TS
nonodb-cli inspect /var/backups/nonodb/$TS
```

### Restore after an incident

```bash
systemctl stop nonodb
nonodb-cli restore /var/backups/nonodb/20261109T120000Z /var/lib/nonodb --force
systemctl start nonodb
NONODB_SOCK=/run/nonodb/nonodb.sock nonodb-cli count users
```

### Raw query for automation

```bash
nonodb-cli exec '{"op":"find","coll":"jobs","filter":{"state":"queued"},"limit":1}'
```

The CLI exits with a non-zero status when a daemon or file operation fails.
