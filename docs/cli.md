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

Every daemon-facing command accepts `--db <name>` or `NONODB_DB=<name>` to select
a database. Without it, `default` is used. See
[Multiple databases](multi-database.md).

When several daemons run on one host, select one explicitly:

```bash
nonodb-cli instances
nonodb-cli --instance 11727 server
nonodb-cli --socket /tmp/nonodb.sock replication status
```

`--instance` accepts a PID or replication node id from the instance registry.
`--socket` sets the Unix socket directly. The registry is per-user and lives in
`$XDG_RUNTIME_DIR/nonodb/instances`, `$HOME/.nonodb/instances`, or the directory
named by `NONODB_INSTANCE_DIR`.

## databases

Manage databases in one daemon:

```bash
nonodb-cli databases list
nonodb-cli databases create app1
nonodb-cli --db app1 find orders '{}'
nonodb-cli databases drop app1
```

`list`, `create`, and `drop` call `list_databases`, `create_database`, and
`drop_database`. A non-default database cannot be dropped while another active
request is using it.

## gui

Starts the embedded local web console and opens the browser:

```bash
nonodb-cli gui
nonodb-cli gui --port 8788
nonodb-cli gui --no-open
nonodb-cli gui --host 0.0.0.0 --allow-remote
nonodb-cli gui --url http://127.0.0.1:7879
nonodb-cli gui --cluster primary=http://127.0.0.1:7879,replica=http://127.0.0.1:7880
```

`--url` makes the GUI proxy use daemon TCP with bearer-token authentication;
the Unix socket path is treated as trusted local admin.

`--cluster` enables the GUI **Cluster** tab and accepts a comma-separated list
of `name=url` nodes. The same list can be supplied through
`NONODB_GUI_CLUSTER`. Cluster probes use `NONODB_CLUSTER_TOKEN`, falling back to
`NONODB_TOKEN`. Cluster URLs currently must use `http://`; terminate TLS in a
local reverse proxy if the nodes require HTTPS.

The server listens on `127.0.0.1` by default. Non-loopback binds require
`--allow-remote` and should sit behind TLS/auth. It proxies `/v1/exec`, database-scoped
`/<database>/v1/exec`, `/v1/watch`, `/healthz`, `/metrics`, and replication API
paths to the daemon. The React assets are embedded in the CLI binary. See
[GUI console](gui.md).

## ping

Check that the daemon is reachable and healthy:

```bash
NONODB_SOCK=/tmp/nonodb.sock nonodb-cli ping
```

```json
{"ok": true}
```

## server

Show the daemon's **actual listener addresses**, including the resolved port
when `NONODB_TCP` uses `:0`, the Unix socket path, TLS state, and the advertised
replication URL:

```bash
NONODB_SOCK=/tmp/nonodb.sock nonodb-cli server
```

```json
{
  "ok": true,
  "result": {
    "version": "0.30.0",
    "tcp": {
      "configured": "127.0.0.1:7902",
      "bound": "127.0.0.1:7902",
      "tls": false
    },
    "unix": "/tmp/nonodb.sock",
    "advertise_url": "http://10.0.0.10:7902"
  }
}
```

This is the reliable way to distinguish a process that works locally but is not
reachable from another host. `configured` is the environment value; `bound` is
what the kernel actually accepted.

## instances

List daemons that registered themselves in the per-user runtime registry:

```bash
nonodb-cli instances
nonodb-cli instances --json
nonodb-cli instances prune
```

Each row includes PID, node id, role, liveness, Unix socket, TCP listener,
advertised replication URL, and data directory. `prune` removes records whose
process has exited. Registry files are runtime metadata only; they are not
backups and never contain tokens.

Choose one instance for a command with either selector:

```bash
nonodb-cli --instance 11727 replication status
nonodb-cli --instance liveapisec-local replication status
nonodb-cli --socket /Users/me/nd-replica/run/nonodb.sock replication status
```

`--instance` and `--socket` are global options and can be placed before the
command.

## ready

Check readiness: the writer thread is running and LMDB can serve a read
transaction. Docker uses this command as its `HEALTHCHECK`.

```bash
NONODB_SOCK=/tmp/nonodb.sock nonodb-cli ready
```

```json
{"ok":true,"checks":{"writer":true,"storage":true,"storage_error":null}}
```

Exit status is non-zero when the daemon returns HTTP `503`.

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

## reindex

Rebuild all secondary, text, fuzzy, and vector indexes for a collection:

```bash
nonodb-cli reindex users
```

This is the one-click migration/repair path after an index-format upgrade. It
scans every document and blocks writes while the rebuild transaction runs. The
command prints the rebuilt index names for each index type.

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

## subscriptions

Persistent conditions over collection state, evaluated by the writer:

```bash
nonodb-cli subscribe orders paid --filter '{"status":"paid"}' --lease 600
nonodb-cli subscribe orders many --count '{"filter":{"total":{"$gt":100}},"gt":100}'
nonodb-cli subscribe orders high_once --filter '{"total":{"$gt":100}}' --once
nonodb-cli subscribe orders changed --ops update --type custom.orders_changed
nonodb-cli subscription-ping orders paid 600
nonodb-cli subscriptions orders
nonodb-cli unsubscribe orders paid
```

By default every matching write emits. `--once` is optional and switches to
edge-triggered (`false -> true`). Persistent subscriptions have a client lease
(default 600 seconds); run `subscription-ping` before it expires. Events are
stored in `_events` with type `subscription.<name>` unless `--type` is given. See
[Conditional subscriptions](subscriptions.md).

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

## sync

Force an fsync of the selected database:

```bash
nonodb-cli --db app1 sync
```

```json
{"synced": true}
```

Use it after a bulk import written with `durability="relaxed"` or `"unsafe"`.
The operation requires database-admin scope and is separate from per-request
durability in the HTTP API.

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

## backup-keygen

Generate an age X25519 identity and recipient for encrypted backups:

```bash
nonodb-cli backup-keygen /etc/nonodb/backup-identity.txt
```

The identity is written to the file with mode `0600`; the public
`age1...` recipient is printed to stdout. Store the identity outside the daemon
and set `NONODB_BACKUP_IDENTITY_FILE` when restoring an encrypted backup.

## branch

Create an offline, independent clone of a database. Stop the daemon first, or
branch from a snapshot:

```bash
nonodb-cli --db app1 branch app1-ci --data /var/lib/nonodb --verify
nonodb-cli branch app1-ci --from-snapshot /snapshots/app1/data-1767225600000.mdb \
  --data /var/lib/nonodb --json
```

The target is `<data>/<target>/`. Reflink/clonefile is used when the filesystem
supports it; otherwise the command falls back to a full copy and reports
`method: "copy"`. `--force` replaces an existing target only after the new clone
is complete, and `--verify` runs a deep check. Missing text/fuzzy/ANN sidecars are
rebuilt automatically, including for data-only snapshots. `--no-sidecars` skips
copying and rebuilding them; text/fuzzy/vector queries then fail until a daemon
restart with `NONODB_REBUILD_MISSING_SEARCH=1`. Auth and instance-wide files are
not copied. See
[Branching a database](branching.md).

## backup

Create a consistent, compacted copy of one database. In the multi-database
layout the source is `./data/<database>/` (or legacy `./data/`):

```bash
nonodb-cli --db app1 backup ./data ./backup
```

```text
backup: ./data/app1 -> ./backup/data.mdb
manifest: ./backup/manifest.json (sha256 ...)
```

- The source is opened read-only.
- The copy can be made while the daemon is running.
- `CompactionOption::Enabled` omits free pages, so the backup is usually smaller
  than the live `data.mdb`.
- The output directory is created if needed.
- The selected database's `data.mdb` is copied. `lock.mdb` is not part of the
  backup.
- Search sidecars (`search/`, `vector-ann/`) are copied by default; use
  `--without-search-sidecars` to omit them.
- The copy is synced to disk before the command exits.
- `manifest.json` records the source, timestamp, file size, and SHA-256.

The resulting directory is a one-database backup, not an instance root. To
inspect and verify it directly:

```bash
nonodb-cli inspect ./backup
nonodb-cli verify-backup ./backup
```

## backup-all

Back up every database under an instance root:

```bash
nonodb-cli backup-all /var/lib/nonodb /var/backups/nonodb/instance
```

It writes `instance/manifest.json` plus `<database>/data.mdb`,
`<database>/manifest.json`, and search sidecars for each database. Each database
copy is consistent on its own, but the set is not atomic across databases. The
auth registry is not copied. Use `--without-search-sidecars` to omit sidecars.

## restore-all

Restore every database listed in an instance manifest:

```bash
nonodb-cli restore-all /var/backups/nonodb/instance /var/lib/nonodb --force --verify
```

Stop the daemon first. Existing database files require `--force`; databases not
listed in the manifest are left untouched.

## verify

Deep-check a data directory without a manifest:

```bash
nonodb-cli --db app1 verify ./data
nonodb-cli --db app1 verify ./data --json
```

The command opens the database read-only and checks:

- the schema version and collection registry;
- every document's JSON and `_id` consistency;
- every secondary index entry against its document;
- text and fuzzy segment manifests, files, and openable readers;
- vector index entries against the stored document vectors;
- history and TTL entry counts.

Legacy secondary index formats are counted but not reconstructed exactly.

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
nonodb-cli inspect ./data              # default database
nonodb-cli --db app1 inspect ./data
nonodb-cli inspect ./data users
```

```text
data: ./data
collections: 2
  users: 1000
  settings: 12
```

With a multi-database instance root, add `--db` or pass the database directory
directly (for example `./data/app1`). Without `--db`, the command resolves the
legacy root or `default/`.

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
restore: ./backup/data.mdb -> ./data/default/data.mdb
verified: ./data/default
...
```

Rules and warnings:

- **Stop the daemon before restoring.** Replacing `data.mdb` under a running
  process is unsafe.
- `--force` is required when the destination `data.mdb` already exists.
- With a multi-database root, the default target is `default/data.mdb`; use
  `--db` to restore another database.
- The command removes a stale `lock.mdb` from the destination.
- The backup can be either a directory containing `data.mdb` or a direct path
  to a `data.mdb` file.
- If the backup directory has `manifest.json`, its size and SHA-256 are checked
  before copying.
- `--verify` deep-checks the restored directory after the copy.
- Start the daemon after the restore and verify counts and indexes.

### Restore from a timestamped snapshot

If the daemon writes periodic snapshots, select the newest snapshot at or before
a recovery time and restore it:

```bash
nonodb-cli --db app1 restore --to 2026-01-01T12:00:00Z \
  /var/backups/nonodb/snapshots /var/lib/nonodb --force --verify
```

The first positional path is a snapshot root containing either
`<root>/<database>/data-<unix-ms>.mdb` or `data-<unix-ms>.mdb` directly. The
selected database comes from `--db`/`NONODB_DB`; the default is `default`.
`--to` also accepts Unix milliseconds or seconds (values below
`100000000000` are treated as seconds).

This is snapshot selection, not WAL point-in-time recovery: data written after
the selected snapshot is not replayed. A timestamp with no snapshot at or
before it fails, as does a timestamp in the future. Without `--to`, `restore`
keeps its existing backup-directory behavior.

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

The same principals, tokens, and certificates can be managed with the
`/v1/auth` HTTP API or the GUI **Authorization** tab when the daemon has an
enabled `admin:db` credential. CLI commands keep working directly on the file
and are the recovery path.

See [Authentication and authorization](auth.md) for the file schema, scope
grammar, audit log, and mTLS setup.

## replication

Inspect the durable log and replica status for the selected database:

```bash
nonodb-cli replication status
nonodb-cli replication primary
nonodb-cli replication changes 0 100
```

Inspect replica acknowledgements:

```bash
nonodb-cli replication acks
```

Prune a known-acked range, refusing to pass the minimum replica ack:

```bash
nonodb-cli replication prune 1234 --require-acks
```

Pause, resume, or disconnect a replica without deleting data:

```bash
nonodb-cli replication pause
nonodb-cli replication resume
nonodb-cli replication disconnect
```

Remove a stale acknowledgement from the primary after a replica is disconnected (GUI: **Remove replica**):

```bash
nonodb-cli replication forget replica-prod
```

This deletes the ACK row and blocks further ACKs from that node id until a new
pairing code is redeemed. A still-running replica will receive
`403 replica_removed`; use `pause` or `disconnect` on that replica to stop it.

`pause` keeps the source and token; `resume` continues from the stored source.
`disconnect` removes both and leaves the node read-only. Promote it separately
with `replication promote <epoch>` if it should become standalone.

Change the pull source without restarting the daemon:

```bash
nonodb-cli replication source https://new-primary:7878 default,app1 1
```

Create a one-time 8-character code on the primary and enroll a new replica:

```bash
# primary
nonodb-cli replication add-replica replica-eu default
# alias: nonodb-cli replication pair replica-eu default

# new replica, after starting its daemon with NONODB_REPLICATION_ROLE=replica
nonodb-cli replication join https://primary.example.com:7878 K7M4-P9QX
```

Use the primary's daemon API URL, not the GUI proxy. The default API port is
`7878`; if `NONODB_TCP` uses another port, use that port (the GUI **Write URL**
field shows the effective value). The CLI prints the same URL when it creates a
pairing code.

`replication join` is a **local admin command**: the CLI talks to the local
replica daemon, and that daemon redeems the code against `<primary-api-url>`.
It does not connect directly to the primary URL. If you see:

```text
nonodb-cli: connect: No such file or directory (os error 2)
```

the local daemon socket is missing or `NONODB_SOCK` points at the wrong path.
Start the replica daemon first, or set `NONODB_SOCK` to its actual socket
(e.g. `/run/nonodb-replica.sock` or `/tmp/nonodb.sock`). On macOS/Linux the CLI
uses the Unix socket; `NONODB_URL` is the Windows TCP path and is ignored on
Unix. If the daemon runs in Docker, run the command inside the container with
the container's socket path.

On success the CLI prints a short confirmation instead of raw JSON:

```text
Connected to the cluster.
  node_id:   replica-eu
  source:    https://primary.example.com:7878
  databases: default,app1
  role:      replica
  pulling:   https://primary.example.com:7878
The primary GUI shows this replica after its first acknowledgement.
```

Set `NONODB_REPLICATION_ADVERTISE_URL` on the replica if the primary GUI should
automatically probe its metrics in the Cluster view.

The code can be omitted from `join`; the CLI prompts for it. List and revoke
pending codes with:

```bash
nonodb-cli replication pairings
nonodb-cli replication revoke K7M4-P9QX
```

The code is single-use, valid for 10 minutes, and only bootstraps a cluster
credential.

Manually switch roles with an expected epoch:

```bash
nonodb-cli replication promote 0
nonodb-cli replication promote 0 --force   # only when the old source is unreachable
nonodb-cli replication demote 1 replica
nonodb-cli replication demote 1 replica https://new-primary:7878
```

The optional successor URL is stored on the demoted node. Replicas pulling
from it switch their source automatically.

A stale epoch is rejected with `409`. The role is persisted in the hidden node
metadata collection `<NONODB_DATA>/.node`; it wins over
`NONODB_REPLICATION_ROLE` after the first start.

`NONODB_DB=app1` or `--db app1` selects another database. Status includes role,
read-only mode, source URL, bootstrap mode, applied sources, lag, promotion
readiness, and the last transport/apply error. Pruning is an administrative retention action; do not
prune sequences that replicas still need.

## Environment

| Variable | Default | Used by |
|---|---|---|
| `NONODB_SOCK` | `/run/nonodb.sock` | Daemon-facing commands on Linux/macOS |
| `NONODB_INSTANCE_DIR` | user runtime dir | Override the local instance registry directory |
| `NONODB_URL` | `http://127.0.0.1:7878` | Daemon-facing commands on Windows |
| `NONODB_TOKEN` | — | Bearer token for daemon-facing commands |
| `NONODB_AUTH_FILE` | `/etc/nonodb/auth.json` (Unix) | Registry used by `auth` commands |
| `NONODB_DB` | `default` | Database selected by daemon-facing commands |

`backup`, `inspect`, `restore`, and `auth` do not use the socket or token.
`auth` commands edit the local auth file directly.

## Typical procedures

### Online backup

```bash
TS=$(date -u +%Y%m%dT%H%M%SZ)
nonodb-cli backup-all /var/lib/nonodb /var/backups/nonodb/$TS
nonodb-cli inspect /var/backups/nonodb/$TS/default
```

For a single database, use `nonodb-cli --db app1 backup ...`.

### Restore after an incident

```bash
systemctl stop nonodb
nonodb-cli restore-all \
  /var/backups/nonodb/20261109T120000Z /var/lib/nonodb --force --verify
systemctl start nonodb
NONODB_SOCK=/run/nonodb/nonodb.sock nonodb-cli --db app1 count users
```

### Raw query for automation

```bash
nonodb-cli exec '{"op":"find","coll":"jobs","filter":{"state":"queued"},"limit":1}'
```

The CLI exits with a non-zero status when a daemon or file operation fails.
