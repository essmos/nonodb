# Operations guide

This guide covers configuration, deployment, security, backup, monitoring,
capacity planning, and troubleshooting for a nonodb daemon.

## Process and filesystem model

A nonodb deployment has:

- one `nonodb-daemon` process per data directory;
- one LMDB environment in that directory (`data.mdb`, `lock.mdb`);
- one or more application processes connecting over a Unix socket or TCP;
- optionally `nonodb-cli` for administration and backups.

Linux and macOS support both Unix sockets and TCP. Windows has no Unix socket
listener; the daemon requires `NONODB_TCP` and `NONODB_AUTH_FILE` there. The
Windows CLI connects over plaintext HTTP/TCP using `NONODB_URL`.

The daemon must be the only process that opens the LMDB environment for
writing. Do not run two daemons against the same data directory. Do not place
the data directory on NFS, SMB, or other network filesystems that do not provide
reliable POSIX locking and mmap semantics.

The daemon creates the socket with mode `0660`. The application workers must be
able to reach the socket path and must share the daemon's group (or be allowed
by the uid/gid allow-list).

## Configuration

All daemon configuration is through environment variables.

| Variable | Default | Description |
|---|---|---|
| `NONODB_DATA` | `./data` | Directory containing `data.mdb` and `lock.mdb` |
| `NONODB_SOCK` | `/run/nonodb.sock` | Unix socket path |
| `NONODB_MAP_SIZE` | `1GiB` | LMDB address-space limit; accepts `4GiB`, `2048MiB`, bytes |
| `NONODB_REQUEST_TIMEOUT` | `30` | Server-side request timeout in seconds |
| `NONODB_CACHE_TTL` | `5` | Read-cache TTL in seconds; `0` disables the cache |
| `NONODB_CACHE_ENTRIES` | `512` | Maximum number of cached read results |
| `NONODB_CACHE_MAX_BYTES` | `262144` | Maximum serialized size of one cached result |
| `NONODB_HISTORY` | `0` | Enables append-only document revision history |
| `NONODB_HISTORY_MAX_VERSIONS` | `50` | Revisions retained per document (`0` disables history writes) |
| `NONODB_TTL_SWEEP_INTERVAL` | `5` | Seconds between document TTL sweeps |
| `NONODB_TTL_SWEEP_LIMIT` | `256` | Maximum due documents processed per TTL sweep |
| `NONODB_MAINTENANCE_INTERVAL_MS` | `1000` | Maintenance worker heartbeat in milliseconds |
| `NONODB_AUTO_INDEX` | `0` | Enables automatic indexes for frequent read shapes |
| `NONODB_AUTO_INDEX_MIN_HITS` | `50` | Required cache misses in one window before creating an index |
| `NONODB_AUTO_INDEX_WINDOW` | `300` | Frequency window in seconds |
| `NONODB_AUTO_INDEX_MIN_DOCS` | `10000` | Minimum collection size for automatic indexing |
| `NONODB_AUTO_INDEX_MAX` | `8` | Maximum number of automatic indexes |
| `NONODB_AUTO_INDEX_MAX_FIELDS` | `3` | Maximum fields in an automatic compound index |
| `NONODB_AUTO_INDEX_TTL` | `604800` | Drop an automatic index after this many seconds without demand |
| `NONODB_AUTO_INDEX_MIN_AGE` | `3600` | Minimum age before an automatic index can be dropped |
| `NONODB_AUTO_INDEX_INTERVAL` | `5` | Maintenance tick interval in seconds |
| `NONODB_AUTH_FILE` | — | Token/certificate registry; mandatory for TCP |
| `NONODB_AUTH_MAX_FAILURES` | `10` | Failed logins before an IP block; `0` disables |
| `NONODB_AUTH_FAILURE_WINDOW` | `60` | Sliding failure-count window in seconds |
| `NONODB_AUTH_BLOCK_SECS` | `300` | IP block duration in seconds; `0` disables |
| `NONODB_AUDIT_LOG` | — | Append-only JSONL audit log path |
| `NONODB_AUDIT_READS` | `0` | Set to `1` to audit reads too |
| `NONODB_AUDIT_FSYNC` | `0` | Set to `1` to fsync every audit line |
| `NONODB_ALLOWED_UIDS` | — | Comma-separated Unix uid allow-list for UDS |
| `NONODB_ALLOWED_GIDS` | — | Comma-separated Unix gid allow-list for UDS |
| `NONODB_TCP` | — | TCP listen address, e.g. `127.0.0.1:7878` |
| `NONODB_TLS_CERT` | — | PEM certificate chain for TCP TLS |
| `NONODB_TLS_KEY` | — | PEM private key for TCP TLS |
| `NONODB_TLS_CLIENT_CA` | — | PEM CA bundle used to verify client certificates (mTLS) |
| `NONODB_TLS_CLIENT_AUTH` | `required` when client CA is set | `required`, `optional`, or `off` |
| `NONODB_SNAPSHOT_DIR` | — | Enables periodic LMDB snapshots in this directory |
| `NONODB_SNAPSHOT_INTERVAL` | `300` | Snapshot interval in seconds |
| `NONODB_SNAPSHOT_KEEP` | `24` | Number of snapshot files to retain |
| `NONODB_DRAIN_TIMEOUT` | `30` | Maximum seconds to wait for connections and queued writes during shutdown |
| `NONODB_LOG_FORMAT` | `text` | `text` for human-readable logs or `json` for structured logs |
| `RUST_LOG` | `info,nonodb=debug` | `tracing` log filter |

Client-side environment variables:

| Variable | Default | Description |
|---|---|---|
| `NONODB_SOCK` | `/run/nonodb.sock` | Default Unix socket for the Python client and CLI (Unix) |
| `NONODB_URL` | `http://127.0.0.1:7878` | HTTP base URL for Rust, Node.js, Go, and the Windows CLI |
| `NONODB_TOKEN` | — | Bearer token used by the Python client and CLI |
| `NONODB_TIMEOUT` | `10` | Client request timeout in seconds |
| `NONODB_TLS_VERIFY` | `1` | Set to `0`/`false`/`no` to disable certificate verification |
| `NONODB_TLS_CA` | — | Custom CA bundle for verifying the daemon certificate |
| `NONODB_TLS_CLIENT_CERT` | — | Client certificate for mTLS |
| `NONODB_TLS_CLIENT_KEY` | — | Private key for `NONODB_TLS_CLIENT_CERT` |

`NONODB_MAP_SIZE` controls the maximum size of the memory-mapped database. It
is **not** a disk or RAM reservation. The file grows as data is written. If the
database reaches the map limit, writes fail. Grow it online:

```bash
nonodb-cli map
nonodb-cli resize 4GiB
```

`resize_map` waits for active LMDB transactions and briefly blocks new ones; it
does not restart the daemon or rewrite the data. It can only grow the map. The
new size must be page-aligned. If the request timeout is shorter than a
long-running transaction, the client may time out even though the resize
completes afterwards.

The daemon has a shared read cache for `find`, `find_one`, `count`, search,
fuzzy, and list operations. It is enabled by default for 5 seconds. Every
successful write commit increments an internal version and clears the cache, so
read-after-write through the same daemon is fresh. The cache is bounded by
`NONODB_CACHE_ENTRIES` and `NONODB_CACHE_MAX_BYTES`; oversized results are not
stored. Set `NONODB_CACHE_TTL=0` to disable it. Metrics expose
`cache_hits_total` and `cache_misses_total`.

### Document history

Document history is opt-in with `NONODB_HISTORY=1`. When enabled, inserts,
updates, and deletes also append an immutable revision to the separate
`history` LMDB sub-database. Current-document writes and history appends share
one LMDB transaction, so a crash cannot leave them inconsistent.

Searches and secondary indexes never see history entries. This keeps query
results current and avoids index maintenance for old revisions.

`NONODB_HISTORY_MAX_VERSIONS` controls how many revisions are retained per
document (default `50`). The maintenance worker submits a bounded pruning job
every 30 seconds; the writer deletes old revisions in small batches rather than
during the hot write path. Set the value to `0` to stop recording new history.

History is global to the store, not per collection. Enabling it after a
database already contains documents creates history only for writes performed
after the daemon restart; there is no retroactive backfill. The `history` and
`get_version` API operations are documented in [HTTP API](api.md#document-history).

History increases write amplification and disk usage by approximately one
document copy per write for history-enabled stores. Size `NONODB_MAP_SIZE` and
backup retention accordingly. Use a smaller `NONODB_HISTORY_MAX_VERSIONS` or
disable history for write-heavy collections.

### Document TTL

TTL is configured per collection (`ttl_seconds`) and optionally per document
with the reserved `_ttl` field. Expiry metadata is stored in the `ttl`
sub-database and written atomically with the document and its indexes.

The maintenance worker submits expiry deletes to the single writer. It processes
at most `NONODB_TTL_SWEEP_LIMIT` due documents every
`NONODB_TTL_SWEEP_INTERVAL` seconds. TTL is eventual: a document may remain
visible briefly after its timestamp. Expiry uses the normal delete path,
including a history tombstone when history is enabled. Collections can set
`purge_history` to delete stored revisions instead.

See [Document TTL](ttl.md) for semantics, update behavior, vault support, and
metrics.

## Automatic indexes

Automatic indexes are opt-in (`NONODB_AUTO_INDEX=1`). The daemon records only
the *shape* of cache-missed `find`, `find_one`, and `count` requests: collection,
indexable fields, and equality/range operators. It never records document
values for this purpose.

An index is created only when:

- the shape reaches `NONODB_AUTO_INDEX_MIN_HITS` within the window;
- the collection exists and has at least `NONODB_AUTO_INDEX_MIN_DOCS` documents;
- the same index does not already exist;
- the automatic-index limit has not been reached;
- the write queue is not backlogged.

Only simple string/number equality and range predicates are considered.
`_id`, boolean/null conditions, arrays, `$or`, `$ne`, `$nin`, regex, and
unsupported operators are ignored. Up to `NONODB_AUTO_INDEX_MAX_FIELDS`
equality fields are used, followed by one range field when space remains.

Automatic index creation and eviction are submitted to the normal writer by
the daemon maintenance loop. Creation is skipped while more than 32 writes are
queued. Queries keep using the existing scan/cache path until the index is
available. For large collections, prefer a scheduled manual index build over
automatic creation.

The daemon maintenance worker drives automatic index creation and eviction.
Automatic indexes are tracked in `auto-index.json` in `NONODB_DATA`. They are
dropped after `NONODB_AUTO_INDEX_TTL` seconds without demand, but only when
they are at least `NONODB_AUTO_INDEX_MIN_AGE` old. Manual indexes are never
dropped automatically. The state file stores timestamps and index definitions,
not document values.

## Deployment with systemd

The repository includes [`deploy/nonodb.service`](../deploy/nonodb.service).

```ini
[Unit]
Description=nonodb document database daemon
After=network.target

[Service]
Type=simple
User=nonodb
Group=nonodb
RuntimeDirectory=nonodb
Environment=NONODB_DATA=/var/lib/nonodb
Environment=NONODB_SOCK=/run/nonodb/nonodb.sock
Environment=RUST_LOG=info
ExecStart=/usr/local/bin/nonodb-daemon
Restart=on-failure
RestartSec=2

NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/nonodb /run/nonodb

[Install]
WantedBy=multi-user.target
```

Typical installation:

```bash
cargo build --release
sudo install -m 0755 target/release/nonodb-daemon /usr/local/bin/
sudo install -m 0755 target/release/nonodb-cli /usr/local/bin/
sudo useradd --system --home /var/lib/nonodb --shell /usr/sbin/nologin nonodb
sudo mkdir -p /var/lib/nonodb /run/nonodb
sudo chown nonodb:nonodb /var/lib/nonodb /run/nonodb
sudo install -m 0644 deploy/nonodb.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now nonodb
```

Grant application workers access:

```bash
sudo usermod -aG nonodb www-data
```

The user must log out and back in (or the service must restart) for group
changes to take effect.

`RuntimeDirectory=nonodb` makes systemd create `/run/nonodb` and remove it on
stop. The socket itself is recreated by the daemon at startup.

## Docker

A public Docker image is not currently published. Build the image from the
repository:

```bash
git clone https://github.com/essmos/nonodb.git
cd nonodb
docker build -t nonodb:local .
```

The `.dockerignore` file keeps `target/`, git metadata, local data, sockets, and
Python caches out of the build context. The Dockerfile builds in a Rust image
and copies only `nonodb-daemon` and `nonodb-cli` into a slim Debian runtime.

The image runs as the unprivileged `nonodb` user and sets:

```text
NONODB_DATA=/var/lib/nonodb
NONODB_SOCK=/run/nonodb/nonodb.sock
```

### Host access over TCP

The simplest way for a host process to reach a containerized daemon is a
loopback-only TCP listener with an auth registry mounted read-only:

```bash
export NONODB_AUTH_FILE="$PWD/nonodb-auth.json"
nonodb-cli auth init
export TOKEN=$(nonodb-cli auth token create admin)

docker volume create nonodb-data
docker run -d --name nonodb \
  -e NONODB_AUTH_FILE=/etc/nonodb/auth.json \
  -e NONODB_TCP=0.0.0.0:7878 \
  -p 127.0.0.1:7878:7878 \
  --group-add "$(id -g)" \
  -v "$NONODB_AUTH_FILE:/etc/nonodb/auth.json:ro" \
  -v nonodb-data:/var/lib/nonodb \
  nonodb:local
```

Verify it:

```bash
curl -s http://127.0.0.1:7878/healthz
docker logs nonodb
```

Use it from the Python client:

```python
from nonodb import NonoDB

db = NonoDB(
    base_url="http://127.0.0.1:7878",
    token=os.environ["TOKEN"],
)
print(db.collection("users").count())
db.close()
```

TCP requires `NONODB_AUTH_FILE`. Keep the published port on `127.0.0.1` unless
you add TLS and intentionally expose the service. See
[TCP and TLS](#tcp-and-tls).

The named volume persists the LMDB data directory. Removing the container does
not remove the volume; remove it explicitly when you really want to delete the
database.

### Docker Compose

The repository includes [`deploy/docker-compose.yml`](../deploy/docker-compose.yml):

```bash
export NONODB_AUTH_FILE="$PWD/nonodb-auth.json"
nonodb-cli auth init
nonodb-cli auth token create admin
docker compose -f deploy/docker-compose.yml up -d --build
```

Check and stop it:

```bash
docker compose -f deploy/docker-compose.yml logs -f nonodb
docker compose -f deploy/docker-compose.yml down
```

`down` keeps the named volumes. Add `-v` only when you intend to delete the
database and socket volumes.

### Unix socket between containers

To use the Unix socket instead of TCP, share a socket directory volume:

```bash
docker volume create nonodb-data
docker volume create nonodb-run

docker run -d --name nonodb \
  -v nonodb-data:/var/lib/nonodb \
  -v nonodb-run:/run/nonodb \
  nonodb:local
```

The client container mounts the same `nonodb-run` volume and sets:

```text
NONODB_SOCK=/run/nonodb/nonodb.sock
```

The socket is created with mode `0660`. The client process must run with a uid
or gid that can open it. Running both containers as the same user/gid is the
simplest option. Do not bind-mount a single socket file: mount the containing
directory, because the daemon unlinks and recreates the socket on startup.

### TLS in Docker

Mount the certificate and key read-only and set both variables:

```bash
docker run -d --name nonodb \
  -e NONODB_AUTH_FILE=/etc/nonodb/auth.json \
  -e NONODB_TCP=0.0.0.0:7878 \
  -e NONODB_TLS_CERT=/certs/server.crt \
  -e NONODB_TLS_KEY=/certs/server.key \
  -p 127.0.0.1:7878:7878 \
  --group-add "$(id -g)" \
  -v "$NONODB_AUTH_FILE:/etc/nonodb/auth.json:ro" \
  -v /host/certs:/certs:ro \
  -v nonodb-data:/var/lib/nonodb \
  nonodb:local
```

Without a certificate and key pair, TCP runs in plaintext and logs a warning.
With only one of the two variables set, the daemon refuses to start.

### Backup from Docker

Back up into a separate mounted directory:

```bash
docker run --rm \
  -v nonodb-data:/var/lib/nonodb:ro \
  -v /host/backups:/backups \
  nonodb:local \
  nonodb-cli backup /var/lib/nonodb /backups/$(date -u +%Y%m%dT%H%M%SZ)
```

The backup command opens the source read-only and writes a compacted
`data.mdb` into the destination directory.

## Permissions

Recommended Unix permissions:

- data directory: owned by `nonodb:nonodb`, mode `0750` or stricter;
- `data.mdb` and `lock.mdb`: owned by `nonodb`;
- socket: created by the daemon with mode `0660`;
- application processes: members of the `nonodb` group.

The daemon performs optional peer credential checks on Unix sockets. If either
`NONODB_ALLOWED_UIDS` or `NONODB_ALLOWED_GIDS` is set, a connection is allowed
when its uid is in the uid list **or** its primary gid is in the gid list.
Otherwise the connection is closed.

The gid check uses `SO_PEERCRED` and the peer's primary gid; supplementary
groups are not considered. If you rely on group membership, prefer socket file
permissions with a shared group rather than the allow-list.

## Authentication

NoNoDB has a file-based principal registry with per-collection scopes. The file
is outside LMDB, is never part of database backups, and stores only token
hashes and certificate fingerprints.

```bash
export NONODB_AUTH_FILE=/etc/nonodb/auth.json
nonodb-cli auth init
nonodb-cli auth principal add app read:orders,write:orders
nonodb-cli auth token create app
```

- Unix socket: trusted local `local` principal with `*:*`.
- TCP: `NONODB_AUTH_FILE` is mandatory; bearer token or registered client
  certificate required.
- There is no global static token fallback.
- Deny-by-default; `write` does not imply `read`.
- `write_batch` is authorized per operation.
- `/healthz` is public; `/metrics` requires `admin:db`.
- Disabled tokens/certificates are re-checked on every request.
- Failed logins can block a TCP peer IP; thresholds and block duration are
  configurable.
- The daemon reloads a valid changed auth file within about one second. An
  invalid reload keeps the last known good configuration.

Full registry format, scope semantics, CLI commands, audit fields, rotation, and
failure behavior: [Authentication and authorization](auth.md).

## TCP and TLS

Enable TCP only when needed:

```bash
export NONODB_AUTH_FILE=/etc/nonodb/auth.json
export NONODB_TCP=127.0.0.1:7878
export NONODB_TLS_CERT=/etc/nonodb/server.crt
export NONODB_TLS_KEY=/etc/nonodb/server.key
```

- TCP refuses to start without `NONODB_AUTH_FILE`.
- If both certificate and key are provided, TLS is enabled using rustls.
- If neither is provided, TCP runs in plaintext and logs a warning.
- Providing only one of cert/key is a startup error.
- Mutual TLS is optional. Set `NONODB_TLS_CLIENT_CA` to verify client
  certificates; it defaults to `required` mode.
- `NONODB_TLS_CLIENT_AUTH=optional` accepts both certificate-authenticated and
  anonymous clients; anonymous clients still need a bearer token.
- Client certificates and bearer tokens live in the same `NONODB_AUTH_FILE`;
  there is no legacy allow-list or global token.
- There is no CRL/OCSP. Revocation is handled by disabling the registry entry
  and using short-lived certificates.

Client example:

```python
from nonodb import NonoDB

db = NonoDB(
    base_url="https://db.internal:7878",
    token="nodb_...",
    ca_cert="/etc/nonodb/tls/client-ca.crt",
)
```

For certificate generation, mTLS setup, rotation, and troubleshooting, see
[Mutual TLS](mtls.md).

## Snapshots

Periodic snapshots are enabled with:

```bash
export NONODB_SNAPSHOT_DIR=/var/backups/nonodb/snapshots
export NONODB_SNAPSHOT_INTERVAL=300
export NONODB_SNAPSHOT_KEEP=24
```

The daemon writes compacted LMDB snapshots named `data-<unix-ms>.mdb`.
Snapshots are consistent copies, not a write-ahead log. They provide recovery
points at the configured interval, not point-in-time recovery to an arbitrary
operation.

Retention removes the oldest files after each successful snapshot. Snapshot
files are not automatically verified; test restores regularly.

Snapshots use `copy_to_file(CompactionOption::Enabled)` and `fsync`. On large
databases, expect the copy to consume I/O and temporary page cache. It does not
block readers or the writer for the whole copy, but it does add background I/O.

## Backup and restore

### Online backup

`nonodb-cli backup` opens the data directory read-only and creates a compacted
copy:

```bash
nonodb-cli backup /var/lib/nonodb /var/backups/nonodb/20261109T120000Z
```

This is safe while the daemon is running. The output is a directory containing
`data.mdb` and `manifest.json`. The manifest records the source path, timestamp,
file size, and SHA-256 checksum.

Verify the backup before relying on it:

```bash
nonodb-cli verify-backup /var/backups/nonodb/20261109T120000Z
nonodb-cli verify-backup /var/backups/nonodb/20261109T120000Z --json
```

Verification checks the manifest size and checksum, then streams every document
and validates `_id` consistency, secondary index entries, and full-text/fuzzy
posting counts. It is read-only and can run while the daemon is serving. For
large databases it is I/O-heavy; schedule it like a backup.

`nonodb-cli verify <data-dir>` performs the same deep check without a manifest.
It is useful for a restored directory or a backup produced by an older release.

### Restore

Stop the daemon first:

```bash
systemctl stop nonodb
nonodb-cli restore /var/backups/nonodb/20261109T120000Z /var/lib/nonodb --force
systemctl start nonodb
```

`restore` copies `data.mdb`, removes a stale `lock.mdb`, and requires `--force`
when the destination already exists. If the backup has a manifest, restore
checks its size and SHA-256 before copying. Add `--verify` to deep-check the
restored directory immediately:

```bash
nonodb-cli restore /var/backups/nonodb/20261109T120000Z /var/lib/nonodb --force --verify
```

Never restore over a live database.

After starting the daemon, verify queries and indexes:

```bash
NONODB_SOCK=/run/nonodb/nonodb.sock nonodb-cli count users
NONODB_SOCK=/run/nonodb/nonodb.sock nonodb-cli indexes users
```

### Restore drill

Run an automated drill on a schedule, not only after an incident:

```bash
cd /opt/nonodb
python3 scripts/restore_drill.py
```

The drill creates a temporary source database with documents, secondary,
text, fuzzy, history, and TTL state, takes a live backup, verifies the
manifest and index structure, restores with `--verify`, starts a daemon on the
restored directory, and checks counts, indexed ranges, full-text/fuzzy search,
history, TTL, and unique-index enforcement.

Example cron entry (weekly, Sunday 03:17):

```cron
17 3 * * 0 cd /opt/nonodb && NONODB_DAEMON=./target/release/nonodb-daemon NONODB_CLI=./target/release/nonodb-cli python3 scripts/restore_drill.py >>/var/log/nonodb-restore-drill.log 2>&1
```

A failed drill should page an operator. Do not treat snapshots or backups as
valid until a restore has been exercised.

### Backup strategy

- Take regular `nonodb-cli backup` copies, not just raw filesystem copies.
- Keep at least one backup off the host.
- Run the automated restore drill regularly and keep its logs.
- Periodically restore into a scratch directory and run `inspect`, `verify`, and
  a few application queries.
- Snapshot retention is not a substitute for an independent backup policy.
- If the database is large, backups are I/O-heavy; schedule them accordingly.

## Monitoring

Metrics are available at `GET /metrics` on the daemon socket (or TCP).
`/metrics` requires the `admin:db` scope; Unix-socket requests run as the local
administrator. Over TCP, send a token:

```bash
curl -s --unix-socket /run/nonodb/nonodb.sock http://localhost/metrics
curl -s -H "Authorization: Bearer $NONODB_TOKEN" http://127.0.0.1:7878/metrics
```

Useful signals:

| Signal | Interpretation |
|---|---|
| `requests_total` | Request volume |
| `errors_total` | Any error response; alert on sustained rate |
| `read_ops_total` | Read throughput |
| `write_ops_total` | Write throughput |
| `write_batches_total` | Number of committed write transactions |
| `avg_batch_size` | Requests amortized per commit; higher means better fsync amortization |
| `avg_commit_us` | Commit latency; storage health indicator |
| `queue_len` | Pending writes; sustained growth means writer saturation |
| `queue_peak` | Historical peak queue |
| `writer_busy` | `1` while a writer transaction is executing |
| `cache_hits_total` / `cache_misses_total` | Read-cache effectiveness |
| `auto_index_candidates` | Query shapes currently above the frequency threshold |
| `auto_index_active` | Automatic indexes currently tracked |
| `auto_index_created_total` / `auto_index_dropped_total` | Automatic index churn |
| `auto_index_build_ns_total` | Time spent building automatic indexes |
| `auto_index_build_errors_total` | Failed automatic index builds |
| `ttl_sweeps_total` | TTL sweeps performed |
| `ttl_expired_total` | Documents deleted by TTL |
| `ttl_sweep_errors_total` | Failed TTL sweeps |
| `ttl_backlog` | Due entries observed by the last sweep |
| `auth_failures_total` | Failed authentication attempts |
| `auth_blocks_total` | IP lockouts started |
| `auth_blocked_requests_total` | Requests rejected while the IP was blocked |
| `auth_blocked_clients` | Currently blocked IPs |

The health endpoint is public and cheap:

```bash
curl -s --unix-socket /run/nonodb/nonodb.sock http://localhost/healthz
```

Logs go to stderr and are controlled by `RUST_LOG`. The default format is
human-readable text. Set `NONODB_LOG_FORMAT=json` for one JSON object per line:

```bash
NONODB_LOG_FORMAT=json RUST_LOG=info ./nonodb-daemon 2>>/var/log/nonodb.log
```

JSON logs keep the standard `timestamp`, `level`, `target`, and `message`
fields. Completed `/v1/exec` requests also log `http.method`, `http.path`,
`http.status`, and `duration_ms`; `/healthz` and `/metrics` probes are omitted
to avoid noise.

## Capacity and memory

### Disk

The LMDB file is approximately the size of the document data plus indexes plus
free pages. Indexes and full-text/fuzzy structures add substantial overhead.
The file **does not shrink** after deletes. A compacting backup produces a
smaller copy; restoring it reduces the live file size.

`map_size` must be larger than the database high-water mark. If writes fail when
the database grows, grow the map online with `nonodb-cli resize`, or stop the
daemon, raise `NONODB_MAP_SIZE`, and start it again. Neither procedure requires
restoring a backup. Shrinking the map reservation requires a restart, and the
file itself only shrinks through a compacting backup/restore. Do not run an
external `backup` or `inspect` process at the same time as an online resize;
retry administrative file commands after it completes.

### Named databases

Collections and the fixed metadata/history/TTL databases consume named LMDB
sub-databases. Index, text, and fuzzy resources share a reusable slot pool:
dropping an index returns its slot, and later indexes reuse it. The environment
is opened with `MAX_DBS=1024`, leaving 960 slots for index/search resources.
Legacy resources from older releases may still hold named databases; a
compacting backup/restore reclaims them.

### Memory

RSS reflects the pages touched by the process. Virtual size includes the
`map_size` reservation and is not a memory leak.

Representative measurements on the benchmark host (8 vCPU, 22.9 GiB RAM):

| State | RSS | PSS |
|---|---|---|
| 1,000 documents, 3 indexes, idle | ~5.1–5.4 MB | ~2.6–3.0 MB |
| 1M documents, 3 indexes, idle after restart | ~5.1 MB | ~2.9 MB |
| 1M documents, after a few reads | ~6.4 MB | ~4.0 MB |
| 1M documents, after a full scan + sort | ~278 MB | ~276 MB |
| 1M documents, peak during parallel index backfill | ~552 MB | — |
| 100k text/fuzzy documents | up to ~788 MB peak | — |
| 10,000 small documents (older test) | ~10 MB | — |

The large numbers are mostly mapped pages and page cache, not Rust heap. A
restart drops process RSS to a few MB; the OS may keep pages cached.

### Working-set guidance

- Keep hot indexed queries and small collections in memory naturally via page
  cache.
- Avoid unbounded full scans on large collections in latency-sensitive paths.
- Deep `skip` on a sorted full scan is intentionally single-threaded.
- Parallel full sorts are used only when `skip + limit <= 50,000`.
- Parallel index backfill uses up to 16 worker threads; peak memory grows with
  the number of entries buffered for a chunk.
- If `map_size` is much larger than needed, virtual size grows but RSS does not
  automatically do so.

## Timeouts and long operations

The daemon defaults to a 30-second request timeout. Large index builds and
backfills can exceed it.

For a one-off large index creation:

```bash
export NONODB_REQUEST_TIMEOUT=600
export NONODB_TIMEOUT=600
```

Restart the daemon after changing `NONODB_REQUEST_TIMEOUT`. The Python client
also accepts `timeout=`.

Creating a text or fuzzy index backfills synchronously and blocks writers while
the final write transaction is active. Schedule large index builds during
maintenance windows.

## Graceful shutdown

`SIGTERM` and `SIGINT` trigger a graceful drain:

1. listeners stop accepting new connections;
2. the Unix socket file is removed;
3. HTTP/1 connections stop keep-alive and finish in-flight requests;
4. the writer queue and the currently executing write transaction drain;
5. the process exits.

No new work is accepted once draining starts. An already accepted request is
allowed to finish and receive its response. The total wait is bounded by
`NONODB_DRAIN_TIMEOUT` (default 30 seconds); connections still active after the
timeout are aborted and this is logged.

`systemctl stop nonodb` sends `SIGTERM`, so normal service restarts and
deployments use this path. Do not use `SIGKILL` for routine restarts: LMDB
remains crash-safe, but in-flight clients may lose their responses.

## Crash recovery

After an unclean shutdown:

1. Start the daemon normally.
2. Check `/healthz` and logs.
3. Run a few counts and indexed queries.
4. If the database does not open, do not attempt to edit LMDB files manually.
   Restore the most recent verified backup.

LMDB is designed so that committed transactions survive and interrupted writes
do not corrupt the environment. The daemon clears stale reader slots on open.

A stale `lock.mdb` from a stopped process is normal. `restore` removes it;
normal daemon startup handles it. Do not copy `lock.mdb` into or out of
backups.

## Upgrades and reindexing

- Stop the daemon before replacing the binary.
- Keep a backup before upgrading.
- Reopen the data directory with the new binary and verify counts.
- If tokenization or search analysis changes between versions, drop and recreate
  text and fuzzy indexes. Old index entries use the old analyzer and will not
  match new normalization rules.
- Schema version is stored in metadata. There is currently no automatic
  migration framework; changes are handled by explicit commands and releases.

## Troubleshooting

| Symptom | Likely cause | Action |
|---|---|---|
| `connection` error in Python | Wrong socket path, daemon down, permissions | Check `NONODB_SOCK`, socket mode/group, daemon logs |
| `unauthorized` (401) | Token missing/wrong, or credential disabled | Check `NONODB_TOKEN`, `NONODB_AUTH_FILE`, and credential state |
| `forbidden` (403) | Principal lacks the required scope | Grant `read:`, `write:`, `admin:<coll>`, or `admin:db` |
| `rate_limited` (429) | Too many failed logins from this IP | Wait for `NONODB_AUTH_BLOCK_SECS`; tune lockout thresholds |
| `timeout` | Long index build or scan exceeds 30 s | Raise `NONODB_REQUEST_TIMEOUT` and client timeout |
| `document_too_large` (413) | Document > 1 MiB or request > 4 MiB | Split document/import; batch inserts |
| `duplicate_key` | Duplicate `_id` or unique-index conflict | Use a new id or update the existing document |
| `invalid_query` on search | Text/fuzzy index missing | Create the index first |
| `MDB_DBS_FULL` | Named database limit reached | Reduce collections/indexes; recreate database with fewer names |
| Writes fail as data grows | `map_size` reached | `nonodb-cli resize 4GiB`, or raise `NONODB_MAP_SIZE` and restart |
| Socket not found after restart | Directory removed | Ensure `RuntimeDirectory=nonodb` or create the socket parent |
| High RSS after a scan | Mapped pages touched | Expected; restart lowers RSS, OS cache remains |
| Disk usage does not fall after delete | LMDB high-water mark | Compact via `nonodb-cli backup`, restore |
| TLS handshake fails | Certificate/key mismatch or verification | Check PEM files, hostname, `verify` setting |
| mTLS client rejected | Missing client cert, untrusted chain, or fingerprint not registered | Check `NONODB_TLS_CLIENT_CA`, client cert/key, and `NONODB_AUTH_FILE` |
| Revoked client still gets data | Auth file reload has not completed | Wait about one second and retry; check auth reload logs |
| TCP listener refuses to start | No auth registry | Set `NONODB_AUTH_FILE` and create a principal/token |

## Security checklist

- Run the daemon as a dedicated unprivileged user.
- Keep the data directory private (`0750` or stricter).
- Restrict the socket directory and use a shared group for workers.
- Protect `NONODB_AUTH_FILE` (`0640`, not in the repository or database backup).
- Use per-principal scopes for applications; reserve `*:*` for administration.
- Configure failed-login lockout and monitor `auth_failures_total` and
  `auth_blocked_clients`.
- Use TCP only with the auth registry and TLS.
- Do not disable certificate verification in production.
- Back up regularly and test restores. Keep vault encryption keys in a separate
  secret manager, not next to the LMDB backup.
- Remember that vault encryption is applied inside the daemon; it protects data
  at rest, not plaintext in daemon memory. See [Vault collections](vault.md).
- Monitor `errors_total`, `queue_len`, and disk space.
- Keep the OS, Rust toolchain, and dependencies patched.
