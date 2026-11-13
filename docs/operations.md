# Operations guide

This guide covers configuration, deployment, security, backup, monitoring,
capacity planning, and troubleshooting for a nonodb daemon.

## Process and filesystem model

A nonodb deployment has:

- one `nonodb-daemon` process per instance root (`NONODB_DATA`);
- one or more LMDB environments under that directory, each in its own
  subdirectory (`<database>/data.mdb`, `<database>/lock.mdb`); the `default`
  database is always present;
- one or more application processes connecting over a Unix socket or TCP;
- optionally `nonodb-cli` for administration and backups.

Linux and macOS support both Unix sockets and TCP. Windows has no Unix socket
listener; the daemon requires `NONODB_TCP` and `NONODB_AUTH_FILE` there. The
Windows CLI connects over plaintext HTTP/TCP using `NONODB_URL`.

The daemon must be the only process that opens the LMDB environments for
writing. Do not run two daemons against the same instance root. Do not place
the instance root on NFS, SMB, or other network filesystems that do not provide
reliable POSIX locking and mmap semantics.

The daemon creates the socket with mode `0660`. The application workers must be
able to reach the socket path and must share the daemon's group (or be allowed
by the uid/gid allow-list).

## Configuration

All daemon configuration is through environment variables.

| Variable | Default | Description |
|---|---|---|
| `NONODB_DATA` | `./data` | Instance root containing `<database>/data.mdb` and `<database>/lock.mdb` |
| `NONODB_MAX_OPEN_DATABASES` | `64` | Maximum open LMDB environments/writers; idle databases are evicted |
| `NONODB_ALLOW_UNSAFE_WRITES` | unset | Enables opt-in `durability:"unsafe"` writes using LMDB `MDB_NOSYNC` |
| `NONODB_SOCK` | `/run/nonodb.sock` | Unix socket path |
| `NONODB_MAP_SIZE` | `1GiB` | LMDB address-space limit; accepts `4GiB`, `2048MiB`, bytes |
| `NONODB_REQUEST_TIMEOUT` | `30` | Server-side request timeout in seconds for normal operations |
| `NONODB_INDEX_TIMEOUT` | `3600` | Server-side timeout in seconds for index/text/fuzzy/vector builds |
| `NONODB_AGGREGATE_MAX_DOCS` | `100000` | Maximum intermediate aggregation result size |
| `NONODB_SCAN_WORKERS` | min(CPUs, 16) | Workers for parallel full scans/sorts; capped at 256 |
| `NONODB_BACKFILL_WORKERS` | min(CPUs, 16) | Workers for index/text/fuzzy/vector backfill parsing; capped at 256 |
| `NONODB_AGGREGATE_WORKERS` | min(CPUs, 16) | Workers for parallel order-independent `$group`; capped at 256 |
| `NONODB_SQL_SLOW_MS` | `0` | Log SQL statements slower than this; `0` disables slow-SQL logging |
| `NONODB_BLOCKING_THREADS` | `64` | Tokio blocking threads used by LMDB reads/writes |
| `NONODB_CACHE_TTL` | `5` | Read-cache TTL in seconds; `0` disables the cache |
| `NONODB_CACHE_ENTRIES` | `512` | Maximum number of cached read results |
| `NONODB_CACHE_MAX_BYTES` | `262144` | Maximum serialized size of one cached result |
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
fuzzy, vector search, and list operations. It is enabled by default for 5 seconds. Every
successful write commit increments an internal version and clears the cache, so
read-after-write through the same daemon is fresh. The cache is bounded by
`NONODB_CACHE_ENTRIES` and `NONODB_CACHE_MAX_BYTES`; oversized results are not
stored. Set `NONODB_CACHE_TTL=0` to disable it. Metrics expose
`cache_hits_total` and `cache_misses_total`.

### Document history

Document history is configured per collection with one setting: the maximum
number of revisions retained per document. `0` disables new history writes for
that collection. There is no daemon-wide history switch.

```bash
nonodb-cli history users 50     # retain up to 50 revisions per document
nonodb-cli history users 0      # disable new history writes
nonodb-cli history users        # show the current setting
```

When enabled, inserts, updates, and deletes also append an immutable revision to
the separate `history` LMDB sub-database. Current-document writes and history
appends share one LMDB transaction, so a crash cannot leave them inconsistent.

Searches and secondary indexes never see history entries. This keeps query
results current and avoids index maintenance for old revisions.

The maintenance worker submits a bounded pruning job every 30 seconds; the
writer deletes old revisions in small batches rather than during the hot write
path. Retention limits are evaluated per collection.

Enabling history after a collection already contains documents creates history
only for writes performed after the setting is applied; there is no retroactive
backfill. The `history` and `get_version` API operations are documented in
[HTTP API](api.md#document-history).

History increases write amplification and disk usage by approximately one
document copy per write for history-enabled collections. Size
`NONODB_MAP_SIZE` and backup retention accordingly. Use a smaller retention
value or disable history for write-heavy collections.

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

Candidates for the same collection are built together with `create_indexes`,
using one collection scan instead of one scan per index. The build still runs
through the single writer, so inserts and updates wait until its transaction
commits.

The daemon maintenance worker drives automatic index creation and eviction.
Automatic indexes are tracked in `auto-index.json` in each database directory
(for example `NONODB_DATA/default/auto-index.json`). They are
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

### Install a prebuilt binary

Download the archive from the matching GitHub Release, verify its checksum, and
install the two binaries:

```bash
TAG=v0.21.0
ARCHIVE="nonodb-$TAG-linux-x86_64.tar.gz"
curl -LO "https://github.com/essmos/nonodb/releases/download/$TAG/$ARCHIVE"
curl -LO "https://github.com/essmos/nonodb/releases/download/$TAG/$ARCHIVE.sha256"
sha256sum -c "$ARCHIVE.sha256"
tar -xzf "$ARCHIVE"
cd "nonodb-$TAG-linux-x86_64"

sudo install -m 0755 nonodb-daemon /usr/local/bin/
sudo install -m 0755 nonodb-cli /usr/local/bin/
sudo useradd --system --home /var/lib/nonodb --shell /usr/sbin/nologin nonodb
sudo mkdir -p /var/lib/nonodb /run/nonodb
sudo chown nonodb:nonodb /var/lib/nonodb /run/nonodb
```

The release archive includes `docs/` but not the repository's `deploy/`
directory. Install the systemd unit from a repository checkout, or copy the
unit shown above into `/etc/systemd/system/nonodb.service`:

```bash
git clone --depth 1 https://github.com/essmos/nonodb.git
sudo install -m 0644 nonodb/deploy/nonodb.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now nonodb
```

### Build from source (alternative)

Use the prebuilt archive above unless you are developing nonodb or need a
custom target. Build and install from a repository checkout:

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

The Docker workflow publishes versioned images to GitHub Container Registry for
Linux `amd64` and `arm64`. Pull a published tag:

```bash
# Pin a version in production; latest tracks stable releases.
docker pull ghcr.io/essmos/nonodb:0.21.0
```

If the GHCR package is private, log in before pulling:

```bash
echo "$GHCR_TOKEN" | docker login ghcr.io -u YOUR_GITHUB_USER --password-stdin
```

To build the image from source instead, use:

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
  ghcr.io/essmos/nonodb:0.21.0
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
docker compose -f deploy/docker-compose.yml up -d
```

Set `NONODB_IMAGE` to pin a different tag. For a locally built image, use
`NONODB_IMAGE=nonodb:local docker compose -f deploy/docker-compose.yml up -d`.

To start the optional local GUI sidecar on `127.0.0.1:8787`:

```bash
docker compose -f deploy/docker-compose.yml --profile gui up -d
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
  ghcr.io/essmos/nonodb:0.21.0
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
  ghcr.io/essmos/nonodb:0.21.0
```

Without a certificate and key pair, TCP runs in plaintext and logs a warning.
With only one of the two variables set, the daemon refuses to start.

### Backup from Docker

Back up into a separate mounted directory:

```bash
docker run --rm \
  -v nonodb-data:/var/lib/nonodb:ro \
  -v /host/backups:/backups \
  ghcr.io/essmos/nonodb:0.21.0 \
  nonodb-cli backup-all /var/lib/nonodb /backups/$(date -u +%Y%m%dT%H%M%SZ)
```

`backup-all` opens each database read-only and writes a compacted `data.mdb`
plus a per-database `manifest.json`, then an instance manifest. For one
database, use `nonodb-cli --db app1 backup /var/lib/nonodb OUT`.

## Permissions

Recommended Unix permissions:

- data directory: owned by `nonodb:nonodb`, mode `0750` or stricter;
- `<database>/data.mdb` and `<database>/lock.mdb`: owned by `nonodb`;
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

Periodic snapshots are enabled with `NONODB_SNAPSHOT_DIR`,
`NONODB_SNAPSHOT_INTERVAL`, and `NONODB_SNAPSHOT_KEEP`. Each database writes
compacted, consistent copies below its own directory:
`<snapshot-dir>/<database>/data-<unix-ms>.mdb`. Snapshots are not point-in-time
recovery, are not atomic across databases, and are not verified automatically.

See [Periodic snapshots](backup.md#periodic-snapshots) for configuration,
retention, and restore details.

## Backup and restore

The full operational guide is [Backup and restore](backup.md). It covers online
backups, verification, restore, restore drills, retention, Docker, vault
collections, and the separate authorization state.

Quick reference:

```bash
TS=$(date -u +%Y%m%dT%H%M%SZ)
nonodb-cli backup-all /var/lib/nonodb "/var/backups/nonodb/$TS"
nonodb-cli verify-backup "/var/backups/nonodb/$TS/default"
# Verify every other <database>/manifest.json the same way.

systemctl stop nonodb
nonodb-cli restore-all "/var/backups/nonodb/$TS" /var/lib/nonodb --force --verify
chown -R nonodb:nonodb /var/lib/nonodb
systemctl start nonodb
```

For one database, use `nonodb-cli --db app1 backup ...` and
`nonodb-cli --db app1 restore ...`. Never restore over a live database.
`auth.json` and the audit log are not part of a database backup; protect them
separately.

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
| `watch_streams_total` | Continuous `/v1/watch` connections opened |
| `watch_active` | Currently open continuous event streams |
| `watch_events_total` | Events delivered by watch/stream |
| `watch_timeouts_total` | Long-poll watch requests that timed out |
| `watch_truncated_total` | Watch requests with an expired resume cursor |
| `watch_heartbeats_total` | Keepalive frames sent on streams |
| `watch_threshold_emitted_total` | Threshold events sent by streams |
| `watch_threshold_suppressed_total` | Events suppressed by stream thresholds |
| `collection_events_emitted_total` | Automatic collection events written |
| `collection_events_suppressed_total` | Events suppressed by `ops`, fields, or filter |
| `sql_select_total` / `sql_insert_total` / `sql_update_total` / `sql_delete_total` | SQL statements by category |
| `sql_errors_total` | SQL parse/execution errors |
| `sql_duration_ns_total` | Total SQL execution time in nanoseconds |
| `index_builds` | Active/recent index backfills and processed/total/elapsed progress |
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
free pages. Indexes, full-text/fuzzy, and vector structures add substantial overhead.
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
sub-databases. Index, text, fuzzy, and vector resources share a reusable slot pool:
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
- Avoid unbounded full scans on large collections in latency-sensitive paths;
  use `find_cursor`/`aggregate_cursor` for large result sets.
- Deep `skip` on a sorted full scan is intentionally single-threaded.
- Parallel full sorts are used only when `skip + limit <= 50,000`.
- Parallel index backfill uses up to 16 worker threads; peak memory grows with
  the number of entries buffered for a chunk.
- Aggregation top-K bounds `$sort`+`$limit` memory, and a leading `$group`
  streams input, but group state is held in memory. Lower
  `NONODB_AGGREGATE_MAX_DOCS` or reduce group cardinality for constrained hosts.
- If `map_size` is much larger than needed, virtual size grows but RSS does not
  automatically do so.

## Timeouts and long operations

The daemon defaults to a 30-second timeout for normal requests and a separate
3600-second timeout for index, text-index, fuzzy-index, and vector-index builds. The Python
client must still allow enough time for the response; set `NONODB_TIMEOUT` or
`NonoDB(timeout=...)` above the expected build duration.

```bash
export NONODB_INDEX_TIMEOUT=7200   # daemon, index builds
export NONODB_TIMEOUT=7500         # Python client
```

Restart the daemon after changing timeout environment variables. Progress for
active backfills is exposed as `index_builds` in `/metrics`, including
`processed`, `total`, `elapsed_ms`, and `finished`. The build remains one LMDB
write transaction, so it becomes visible and durable only after completion; an
interrupted build is rolled back and must be restarted.

Creating a text, fuzzy, or vector index backfills synchronously and blocks writers while
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
| `timeout` | Long scan exceeds 30 s, or index build exceeds 1 h | Raise `NONODB_REQUEST_TIMEOUT`, `NONODB_INDEX_TIMEOUT`, and client timeout |
| `document_too_large` (413) | Document > 1 MiB or request > 4 MiB | Split document/import; batch inserts |
| `duplicate_key` | Duplicate `_id` or unique-index conflict | Use a new id or update the existing document |
| `invalid_query` on search | Text/fuzzy/vector index missing | Create the index first |
| `MDB_DBS_FULL` | Named database limit reached | Reduce collections/indexes; recreate database with fewer names |
| Writes fail as data grows | `map_size` reached | `nonodb-cli resize 4GiB`, or raise `NONODB_MAP_SIZE` and restart |
| Socket not found after restart | Directory removed | Ensure `RuntimeDirectory=nonodb` or create the socket parent |
| High RSS after a scan | Mapped pages touched | Expected; restart lowers RSS, OS cache remains |
| Disk usage does not fall after delete | LMDB high-water mark | Compact via `nonodb-cli backup`, restore |
| TLS handshake fails | Certificate/key mismatch or verification | Check PEM files, hostname, `verify` setting |
| mTLS client rejected | Missing client cert, untrusted chain, or fingerprint not registered | Check `NONODB_TLS_CLIENT_CA`, client cert/key, and `NONODB_AUTH_FILE` |
| Revoked client still gets data | Auth file reload has not completed | Wait about one second and retry; check auth reload logs |
| TCP listener refuses to start | No auth registry | Set `NONODB_AUTH_FILE` and create a principal/token |

## Write durability

Writes are always ACID-atomic inside LMDB. Atomicity is not configurable; the
optional durability modes control whether an acknowledged write has been forced
to disk.

| Mode | Request value | Effect |
|---|---|---|
| Durable | `full` (default) | `fsync` data and metadata after the writer commit |
| Relaxed | `relaxed` | `MDB_NOMETASYNC`: data flushed, metadata not forced every commit |
| Unsafe | `unsafe` | No forced sync; requires `NONODB_ALLOW_UNSAFE_WRITES=1` |

`relaxed` can lose the most recent commits after a power failure while keeping
the database consistent. `unsafe` can lose more recent writes and should only be
used for imports, caches, telemetry, or rebuildable data. `full` remains the
default for every normal insert/update/delete and for all background workers.

Group commit uses the strictest request in the batch: if any operation asks for
`full`, the whole batch is fsynced. Use the `sync` operation to force a
checkpoint after relaxed/unsafe bulk writes. The authorization registry and
vault key material are outside LMDB and are never affected by this setting.

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
