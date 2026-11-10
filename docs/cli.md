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
```

- The source is opened read-only.
- The copy can be made while the daemon is running.
- `CompactionOption::Enabled` omits free pages, so the backup is usually smaller
  than the live `data.mdb`.
- The output directory is created if needed.
- Only `data.mdb` is copied. `lock.mdb` is not part of the backup.
- The copy is synced to disk before the command exits.

The resulting directory is a valid nonodb data directory. To verify it:

```bash
nonodb-cli inspect ./backup
NONODB_DATA=./backup NONODB_SOCK=/tmp/nonodb-restore.sock nonodb-daemon
```

Stop that verification daemon before restoring the copy over production data.

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
```

```text
restore: ./backup/data.mdb -> ./data/data.mdb
```

Rules and warnings:

- **Stop the daemon before restoring.** Replacing `data.mdb` under a running
  process is unsafe.
- `--force` is required when the destination `data.mdb` already exists.
- The command removes a stale `lock.mdb` from the destination.
- The backup can be either a directory containing `data.mdb` or a direct path
  to a `data.mdb` file.
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
