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
listener; the daemon requires `NONODB_TCP` and `NONODB_AUTH_TOKEN` there. The
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
| `NONODB_AUTO_INDEX` | `0` | Enables automatic indexes for frequent read shapes |
| `NONODB_AUTO_INDEX_MIN_HITS` | `50` | Required cache misses in one window before creating an index |
| `NONODB_AUTO_INDEX_WINDOW` | `300` | Frequency window in seconds |
| `NONODB_AUTO_INDEX_MIN_DOCS` | `10000` | Minimum collection size for automatic indexing |
| `NONODB_AUTO_INDEX_MAX` | `8` | Maximum number of automatic indexes |
| `NONODB_AUTO_INDEX_MAX_FIELDS` | `3` | Maximum fields in an automatic compound index |
| `NONODB_AUTO_INDEX_TTL` | `604800` | Drop an automatic index after this many seconds without demand |
| `NONODB_AUTO_INDEX_MIN_AGE` | `3600` | Minimum age before an automatic index can be dropped |
| `NONODB_AUTO_INDEX_INTERVAL` | `5` | Maintenance tick interval in seconds |
| `NONODB_AUTH_TOKEN` | — | Enables bearer-token authentication |
| `NONODB_ALLOWED_UIDS` | — | Comma-separated Unix uid allow-list for UDS |
| `NONODB_ALLOWED_GIDS` | — | Comma-separated Unix gid allow-list for UDS |
| `NONODB_TCP` | — | TCP listen address, e.g. `127.0.0.1:7878` |
| `NONODB_TLS_CERT` | — | PEM certificate chain for TCP TLS |
| `NONODB_TLS_KEY` | — | PEM private key for TCP TLS |
| `NONODB_SNAPSHOT_DIR` | — | Enables periodic LMDB snapshots in this directory |
| `NONODB_SNAPSHOT_INTERVAL` | `300` | Snapshot interval in seconds |
| `NONODB_SNAPSHOT_KEEP` | `24` | Number of snapshot files to retain |
| `RUST_LOG` | `info,nonodb=debug` | `tracing` log filter |

Client-side environment variables:

| Variable | Default | Description |
|---|---|---|
| `NONODB_SOCK` | `/run/nonodb.sock` | Default Unix socket for the Python client and CLI (Unix) |
| `NONODB_URL` | `http://127.0.0.1:7878` | HTTP base URL for Rust, Node.js, Go, and the Windows CLI |
| `NONODB_TOKEN` | — | Bearer token used by the Python client and CLI |
| `NONODB_TIMEOUT` | `10` | Client request timeout in seconds |
| `NONODB_TLS_VERIFY` | `1` | Set to `0`/`false`/`no` to disable certificate verification |

`NONODB_MAP_SIZE` controls the maximum size of the memory-mapped database. It
is **not** a disk or RAM reservation. The file grows as data is written. If the
database reaches the map limit, writes fail; increase the limit and restart the
daemon. Existing data remains valid.

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
document (default `50`). The writer runs a bounded pruning pass every 30
seconds; old revisions are removed in small batches rather than during the hot
write path. Set the value to `0` to stop recording new history.

History is global to the store, not per collection. Enabling it after a
database already contains documents creates history only for writes performed
after the daemon restart; there is no retroactive backfill. The `history` and
`get_version` API operations are documented in [HTTP API](api.md#document-history).

History increases write amplification and disk usage by approximately one
document copy per write for history-enabled stores. Size `NONODB_MAP_SIZE` and
backup retention accordingly. Use a smaller `NONODB_HISTORY_MAX_VERSIONS` or
disable history for write-heavy collections.

### Automatic indexes

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

The index is built by the normal writer, in one LMDB write transaction, so it
temporarily blocks other writes. The build job is isolated from group commit,
so it does not share a transaction with unrelated user writes. Creation is
skipped while more than 32 writes are queued. For large collections, prefer a
scheduled manual index build over automatic creation.

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
loopback-only TCP listener with a bearer token:

```bash
docker volume create nonodb-data

docker run -d --name nonodb \
  -e NONODB_AUTH_TOKEN='replace-with-a-long-random-token' \
  -e NONODB_TCP=0.0.0.0:7878 \
  -p 127.0.0.1:7878:7878 \
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
    token="replace-with-a-long-random-token",
)
print(db.collection("users").count())
db.close()
```

TCP requires `NONODB_AUTH_TOKEN`. Keep the published port on `127.0.0.1` unless
you add TLS and intentionally expose the service. See
[TCP and TLS](#tcp-and-tls).

The named volume persists the LMDB data directory. Removing the container does
not remove the volume; remove it explicitly when you really want to delete the
database.

### Docker Compose

The repository includes [`deploy/docker-compose.yml`](../deploy/docker-compose.yml):

```bash
export NONODB_AUTH_TOKEN='replace-with-a-long-random-token'
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
  -e NONODB_AUTH_TOKEN='replace-with-a-long-random-token' \
  -e NONODB_TCP=0.0.0.0:7878 \
  -e NONODB_TLS_CERT=/certs/server.crt \
  -e NONODB_TLS_KEY=/certs/server.key \
  -p 127.0.0.1:7878:7878 \
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

Set `NONODB_AUTH_TOKEN` to require:

```http
Authorization: Bearer <token>
```

on `/v1/exec` and `/metrics`. `/healthz` remains public.

Notes:

- The token is global; there are no users or roles.
- Comparison is constant-time.
- TCP listener mode refuses to start without a token.
- The Python client reads `NONODB_TOKEN` or accepts `token=`.
- The CLI reads `NONODB_TOKEN`.
- When authentication is disabled (no token), any process that can open the
  socket can read and write all data. Protect the socket with filesystem
  permissions.

For stronger deployment isolation, use a private Unix socket directory or
network namespace.

## TCP and TLS

Enable TCP only when you need it:

```bash
export NONODB_AUTH_TOKEN='long-random-token'
export NONODB_TCP=127.0.0.1:7878
export NONODB_TLS_CERT=/etc/nonodb/server.crt
export NONODB_TLS_KEY=/etc/nonodb/server.key
```

- TCP always requires `NONODB_AUTH_TOKEN`.
- If both certificate and key are provided, TLS is enabled using rustls.
- If neither is provided, TCP runs in plaintext and logs a warning.
- Providing only one of cert/key is a startup error.
- There is no mutual TLS and no certificate-based client identity.

Client:

```python
from nonodb import NonoDB

db = NonoDB(
    base_url="https://db.internal:7878",
    token="long-random-token",
    timeout=30,
)
```

Certificate verification is enabled by default. Disable it only for local
testing.

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
`data.mdb`, ready to be restored or opened by another daemon.

### Restore

Stop the daemon first:

```bash
systemctl stop nonodb
nonodb-cli restore /var/backups/nonodb/20261109T120000Z /var/lib/nonodb --force
systemctl start nonodb
```

`restore` copies `data.mdb`, removes a stale `lock.mdb`, and requires `--force`
when the destination already exists. Never restore over a live database.

After restore, verify:

```bash
NONODB_SOCK=/run/nonodb/nonodb.sock nonodb-cli count users
NONODB_SOCK=/run/nonodb/nonodb.sock nonodb-cli indexes users
```

### Backup strategy

- Take regular `nonodb-cli backup` copies, not just raw filesystem copies.
- Keep at least one backup off the host.
- Periodically restore into a scratch directory and run `inspect` and a few
  queries.
- Snapshot retention is not a substitute for an independent backup policy.
- If the database is large, backups are I/O-heavy; schedule them accordingly.

## Monitoring

Metrics are available at `GET /metrics` on the daemon socket (or TCP) and
require the bearer token when auth is enabled:

```bash
curl -s --unix-socket /run/nonodb/nonodb.sock http://localhost/metrics
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
| `cache_hits_total` / `cache_misses_total` | Read-cache effectiveness |
| `auto_index_candidates` | Query shapes currently above the frequency threshold |
| `auto_index_active` | Automatic indexes currently tracked |
| `auto_index_created_total` / `auto_index_dropped_total` | Automatic index churn |
| `auto_index_build_ns_total` | Time spent building automatic indexes |
| `auto_index_build_errors_total` | Failed automatic index builds |

The health endpoint is public and cheap:

```bash
curl -s --unix-socket /run/nonodb/nonodb.sock http://localhost/healthz
```

Logs go to stderr and are controlled by `RUST_LOG`.

## Capacity and memory

### Disk

The LMDB file is approximately the size of the document data plus indexes plus
free pages. Indexes and full-text/fuzzy structures add substantial overhead.
The file **does not shrink** after deletes. A compacting backup produces a
smaller copy; restoring it reduces the live file size.

`map_size` must be larger than the database high-water mark. If writes fail when
the database grows, increase `NONODB_MAP_SIZE` and restart. There is no online
resize.

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
| `unauthorized` (401) | Token missing or wrong | Set `NONODB_TOKEN`/`token=` to match daemon |
| `timeout` | Long index build or scan exceeds 30 s | Raise `NONODB_REQUEST_TIMEOUT` and client timeout |
| `document_too_large` (413) | Document > 1 MiB or request > 4 MiB | Split document/import; batch inserts |
| `duplicate_key` | Duplicate `_id` or unique-index conflict | Use a new id or update the existing document |
| `invalid_query` on search | Text/fuzzy index missing | Create the index first |
| `MDB_DBS_FULL` | Named database limit reached | Reduce collections/indexes; recreate database with fewer names |
| Writes fail as data grows | `map_size` reached | Raise `NONODB_MAP_SIZE`, restart |
| Socket not found after restart | Directory removed | Ensure `RuntimeDirectory=nonodb` or create the socket parent |
| High RSS after a scan | Mapped pages touched | Expected; restart lowers RSS, OS cache remains |
| Disk usage does not fall after delete | LMDB high-water mark | Compact via `nonodb-cli backup`, restore |
| TLS handshake fails | Certificate/key mismatch or verification | Check PEM files, hostname, `verify` setting |
| TCP listener refuses to start | No auth token | Set `NONODB_AUTH_TOKEN` |

## Security checklist

- Run the daemon as a dedicated unprivileged user.
- Keep the data directory private (`0750` or stricter).
- Restrict the socket directory and use a shared group for workers.
- Use `NONODB_AUTH_TOKEN` even on Unix sockets if multiple tenants share the
  host.
- Use TCP only with a token and TLS.
- Do not disable certificate verification in production.
- Back up regularly and test restores.
- Monitor `errors_total`, `queue_len`, and disk space.
- Keep the OS, Rust toolchain, and dependencies patched.
