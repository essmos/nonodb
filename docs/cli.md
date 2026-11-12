# Command-line interface

`nonodb-cli` is a small administrative client. On Linux and macOS, commands
that talk to the daemon use the Unix socket (`NONODB_SOCK`, default
`/run/nonodb.sock`) and, for TCP-backed setups, a bearer token
(`NONODB_TOKEN`). On Windows, daemon-facing commands use plaintext HTTP over TCP
(`NONODB_URL`, default `http://127.0.0.1:7878`). The CLI does not support TLS or
mTLS; use an official client for encrypted connections. File commands
(`backup`, `inspect`, `restore`) operate directly on an LMDB data directory on
every platform. The `auth` commands edit the auth registry file directly and do
not contact the daemon.

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

## sql

Run a SQL statement through the daemon:

```bash
nonodb-cli sql "SELECT team, COUNT(*) AS n FROM users GROUP BY team"
nonodb-cli sql "SELECT name FROM users WHERE age >= $1" --param 18
nonodb-cli sql --file report.sql
```

`--param` is repeatable and accepts JSON values; unquoted values are treated as
strings. `--file` reads the statement from a file. The command prints the
`result` envelope and exits non-zero on an error.

Use `shell` for an interactive line-oriented SQL prompt:

```bash
nonodb-cli shell
nonodb> SELECT COUNT(*) AS n FROM users;
nonodb> .quit
```

Each line is one statement. Use `.quit`, `.exit`, `quit`, or `exit` to leave.

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

## events

Show or configure automatic collection change events:

```bash
nonodb-cli events orders                  # show
nonodb-cli events orders on status total  # enable, only status/total updates
nonodb-cli events orders on               # enable for any field
nonodb-cli events orders off              # disable
nonodb-cli events tail                    # poll _events forever
nonodb-cli events tail --type orders.update --filter '{"payload.after.status":"paid"}'
nonodb-cli events tail --once             # print one batch and exit
```

Events are written to `_events` in the same transaction as the data change.
See [Outbox events](events.md).

## history

Show or set per-collection revision history retention:

```bash
nonodb-cli history users       # show the current setting
nonodb-cli history users 50    # retain up to 50 revisions per document
nonodb-cli history users 0     # disable new history writes
```

History is configured per collection. There is no daemon-wide history switch.

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
query cycle. See [Backup and restore](backup.md#restore-drills).

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

## auth

Manage the file-based principal registry. The default path is
`NONODB_AUTH_FILE`, or `/etc/nonodb/auth.json` on Unix (`auth.json` on Windows).
All commands validate the complete file and write it atomically.

Create a registry with an `admin` principal (`*:*`):

```bash
nonodb-cli auth init
```

It does not create a token. Create one next:

```bash
nonodb-cli auth token create admin
```

The printed token (`nodb_<kid>_<secret>`) is shown only once. The file stores
`kid` plus SHA-256 of the secret.

Principals and scopes:

```bash
nonodb-cli auth principal list
nonodb-cli auth principal add app read:orders,write:orders
nonodb-cli auth principal scopes app read:orders,write:orders,read:customers
```

Tokens:

```bash
nonodb-cli auth token list
nonodb-cli auth token create app [kid]
nonodb-cli auth token disable <kid>
nonodb-cli auth token enable <kid>
```

Certificates (SHA-256 of the DER-encoded leaf certificate, lowercase hex):

```bash
nonodb-cli auth cert list
nonodb-cli auth cert add <sha256> app
nonodb-cli auth cert disable <sha256>
nonodb-cli auth cert enable <sha256>
```

Validate the file without changing it:

```bash
nonodb-cli auth check
```

Use `auth init --force` only when you intentionally want to replace the whole
registry. The daemon reloads valid changes within about one second; an invalid
change is rejected and the daemon keeps the last known good configuration.

See [Authentication and authorization](auth.md) for the file schema, scope
grammar, audit log, and mTLS setup.

## Environment

| Variable | Default | Used by |
|---|---|---|
| `NONODB_SOCK` | `/run/nonodb.sock` | Daemon-facing commands on Linux/macOS |
| `NONODB_URL` | `http://127.0.0.1:7878` | Daemon-facing commands on Windows |
| `NONODB_TOKEN` | — | Bearer token for daemon-facing commands |
| `NONODB_AUTH_FILE` | `/etc/nonodb/auth.json` (Unix) | Registry used by `auth` commands |

`backup`, `inspect`, `restore`, and `auth` do not use the socket or token.
`auth` commands edit the local auth file directly.

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
