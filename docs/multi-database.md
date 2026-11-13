# Multiple databases

One nonodb daemon can host several independent databases. A database is a
separate LMDB environment with its own `data.mdb`, `lock.mdb`, writer thread,
cache, automatic indexes, TTL/history/event settings, and persistent
subscriptions. There are no cross-database transactions.

## Layout

`NONODB_DATA` is the instance root:

```text
/var/lib/nonodb/
├── default/
│   ├── data.mdb
│   ├── lock.mdb
│   └── auto-index.json
└── app1/
    ├── data.mdb
    ├── lock.mdb
    └── auto-index.json
```

The reserved `default` database always exists. The legacy single-database
layout is migrated automatically on startup:

```text
/var/lib/nonodb/data.mdb  ->  /var/lib/nonodb/default/data.mdb
/var/lib/nonodb/lock.mdb  ->  /var/lib/nonodb/default/lock.mdb
```

Stop the daemon before upgrading and take a backup first. If both the legacy
`data.mdb` and `default/data.mdb` exist, the daemon refuses to guess and exits
with an explicit error.

## URL routing

Database-name path prefix:

```text
POST /{database}/v1/exec
POST /{database}/v1/watch
```

Examples:

```bash
curl -s http://127.0.0.1:7878/v1/exec \
  -H 'content-type: application/json' \
  -d '{"op":"find","coll":"orders","filter":{}}'

curl -s http://127.0.0.1:7878/app1/v1/exec \
  -H 'content-type: application/json' \
  -d '{"op":"find","coll":"orders","filter":{}}'
```

`/v1/exec` is an alias for `/default/v1/exec`, and `/v1/watch` is an alias for
`/default/v1/watch`. `/healthz` and `/metrics` remain instance-wide and are not
prefixed.

HTTP clients that build URLs from a base URL can select a database there:

```python
db = NonoDB(base_url="http://127.0.0.1:7878/app1", token=token)
```

All official clients expose a database option and also read `NONODB_DB`:

```python
db = NonoDB("/run/nonodb.sock", database="app1")
```

```js
const db = new NonoDB({ socketPath: '/run/nonodb.sock', database: 'app1' });
```

```go
db := nonodb.New(nonodb.WithSocketPath("/run/nonodb.sock"), nonodb.WithDatabase("app1"))
```

```rust
let db = Client::builder()
    .socket_path("/run/nonodb.sock")
    .database("app1")
    .build()?;
```

Database names are case-sensitive, 1–64 characters, start with an ASCII letter
or digit, and may contain letters, digits, `_`, and `-`. The names `v1`,
`healthz`, `metrics`, `gui`, `db`, and `api` are reserved.

## Management API

List databases:

```json
{"op":"list_databases"}
```

Create a database:

```json
{"op":"create_database","name":"app1"}
```

Drop a non-default database:

```json
{"op":"drop_database","name":"app1"}
```

`drop_database` fails while the database is selected by another active request.
It stops that database's writer and maintenance thread, then removes its files.
The default database cannot be dropped.

## CLI

The CLI accepts `--db <name>` or `NONODB_DB=<name>`:

```bash
nonodb-cli databases list
nonodb-cli databases create app1
nonodb-cli --db app1 exec '{"op":"list_collections"}'
nonodb-cli --db app1 find orders '{}'
nonodb-cli databases drop app1
```

`--db` is removed from the argument list and applies to every data operation.
`ping` remains instance-wide.

## GUI

The embedded GUI has a **Database** field in Advanced connection settings and a
database selector in the collection sidebar. The browser always talks to one
GUI origin and one daemon port; the selected database is added as
`/<database>/v1/exec` and `/<database>/v1/watch`. The `default` database may also
use the unprefixed aliases.

## Backup and restore

Each database is a normal LMDB directory. Select one explicitly:

```bash
nonodb-cli --db app1 backup /var/lib/nonodb /var/backups/nonodb/app1
nonodb-cli verify-backup /var/backups/nonodb/app1
nonodb-cli --db app1 restore /var/backups/nonodb/app1 /var/lib/nonodb --force
```

Back up every database in one command:

```bash
nonodb-cli backup-all /var/lib/nonodb /var/backups/nonodb/instance
nonodb-cli restore-all /var/backups/nonodb/instance /var/lib/nonodb --force --verify
```

`backup-all` writes:

```text
instance/manifest.json
instance/default/data.mdb
instance/default/manifest.json
instance/app1/data.mdb
instance/app1/manifest.json
```

The instance manifest lists every database and its per-database SHA-256. Each
database copy is consistent, but `backup-all` is sequential and not an atomic
snapshot across databases. The auth registry is intentionally not part of the
backup; keep `NONODB_AUTH_FILE` under separate backup.

For a restore, stop the daemon first. Without `--db`, the single-database
commands resolve a legacy `data.mdb` root, then `default/data.mdb`, then
`ROOT/<NONODB_DB>/data.mdb`.

## Open database lifecycle

Databases are discovered at startup but opened lazily. The `default` database is
always open. Idle databases are closed when another database needs to be opened
and the process reaches the limit.

| Variable | Default | Meaning |
|---|---|---|
| `NONODB_MAX_OPEN_DATABASES` | `64` | Maximum simultaneously open LMDB environments/writers |

An environment can only be evicted when no request or live stream holds it. If
all slots are busy, the daemon returns HTTP `503` with error code
`database_limit`. Raise the limit when many databases are used concurrently.

## Current limits

- Collection scopes may be database-qualified:
  `read:app1/orders`, `write:app1/*`, `admin:app1/orders`. Unqualified scopes
  such as `read:orders` apply to every database. Database management still
  requires global `admin:db`.
- A per-database administrator role is planned; `admin:db` is instance-wide.
- Each open database has one writer thread. A shared sharded writer pool is a
  later scaling step. The open-database limit bounds total writer threads.
- There are no cross-database queries, transactions, or joins.
- Snapshot directories are created per database when `NONODB_SNAPSHOT_DIR` is
  set: `<snapshot-dir>/<database>/data-<timestamp>.mdb`.
