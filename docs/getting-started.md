# Getting started

This is the shortest path from zero to a running NonoDB with one document and
one query. Choose the deployment that fits your environment:

- [Docker Compose](#docker-compose-recommended) — recommended for a first run;
- [prebuilt binary](#prebuilt-binary) — no Docker;
- [build from source](#build-from-source) — for development.

For the full feature inventory, see the [documentation index](README.md).

## Docker Compose (recommended)

You need Docker Engine and the Compose plugin.

```bash
docker version
docker compose version
```

Clone the repository:

```bash
git clone --depth 1 https://github.com/essmos/nonodb.git
cd nonodb
```

Create the auth registry and first admin token:

```bash
docker compose -f deploy/docker-compose.quickstart.yml \
  --profile init run --rm auth-init
```

The command prints a bearer token. Copy it and keep it secret.

Start the daemon:

```bash
docker compose -f deploy/docker-compose.quickstart.yml up -d
```

Check health and logs:

```bash
curl -s http://127.0.0.1:7878/healthz
docker compose -f deploy/docker-compose.quickstart.yml logs -f nonodb
```

Insert and query a document:

```bash
export TOKEN='paste-token-here'

curl -s -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -X POST http://127.0.0.1:7878/v1/exec \
  -d '{"op":"insert","coll":"users","doc":{"name":"Ada","age":36}}'

curl -s -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -X POST http://127.0.0.1:7878/v1/exec \
  -d '{"op":"find","coll":"users","filter":{"age":{"$gte":18}}}'
```

The database is stored in the `nonodb-data` Docker volume. Stop the stack with:

```bash
docker compose -f deploy/docker-compose.quickstart.yml down
```

`down` keeps the volumes. Use `down -v` only when you want to delete the
database and credentials.

Optional GUI:

```bash
docker compose -f deploy/docker-compose.quickstart.yml --profile gui up -d
```

Open `http://127.0.0.1:8787`.

Full Docker guide: [Docker Compose quick start](docker-compose.md).

## Prebuilt binary

Every release contains binaries for Linux, macOS, and Windows. Download the
archive for your platform from the
[releases page](https://github.com/essmos/nonodb/releases), verify its
`.sha256`, and extract it.

Linux example:

```bash
TAG=v0.26.1
curl -LO "https://github.com/essmos/nonodb/releases/download/$TAG/nonodb-$TAG-linux-x86_64.tar.gz"
curl -LO "https://github.com/essmos/nonodb/releases/download/$TAG/nonodb-$TAG-linux-x86_64.tar.gz.sha256"
sha256sum -c "nonodb-$TAG-linux-x86_64.tar.gz.sha256"
tar -xzf "nonodb-$TAG-linux-x86_64.tar.gz"
cd "nonodb-$TAG-linux-x86_64"
```

Start the daemon in the foreground:

```bash
mkdir -p /tmp/nonodb-data /tmp/nonodb-run

NONODB_DATA=/tmp/nonodb-data \
NONODB_SOCK=/tmp/nonodb-run/nonodb.sock \
RUST_LOG=info \
./nonodb-daemon
```

Leave that terminal running. In a second terminal:

```bash
export NONODB_SOCK=/tmp/nonodb-run/nonodb.sock

./nonodb-cli ping
./nonodb-cli insert users '{"name":"Ada","age":36}'
./nonodb-cli find users '{"age":{"$gte":18}}'
```

Platform notes:

- Linux: [Run on Linux](../README.md#run-on-linux)
- macOS: [Run on macOS](../README.md#run-on-macos)
- Windows: [Run on Windows](../README.md#run-on-windows-tcp-only)

## Build from source

Requirements:

- Rust stable with Cargo;
- a C compiler for LMDB;
- Node.js only if you need to rebuild the embedded GUI.

```bash
git clone https://github.com/essmos/nonodb.git
cd nonodb
cargo build --release
```

Start the daemon:

```bash
mkdir -p /tmp/nonodb-data /tmp/nonodb-run

NONODB_DATA=/tmp/nonodb-data \
NONODB_SOCK=/tmp/nonodb-run/nonodb.sock \
RUST_LOG=info \
./target/release/nonodb-daemon
```

Use the CLI from another terminal:

```bash
NONODB_SOCK=/tmp/nonodb-run/nonodb.sock \
  ./target/release/nonodb-cli count users '{}'
```

## First concepts

- **Daemon**: one `nonodb-daemon` owns the LMDB files. Do not start two daemons
  against the same data directory.
- **Database**: one daemon can host multiple independent databases under
  `NONODB_DATA`.
- **Collection**: a named group of JSON documents.
- **Document**: a JSON object. `_id` is generated if omitted.
- **Index**: optional secondary index for filters, sorting, unique constraints,
  text, fuzzy, vector, or geospatial queries.
- **Auth**: TCP requires `NONODB_AUTH_FILE`. Unix sockets can use peer
  credentials instead.

Configuration essentials:

| Variable | Purpose |
|---|---|
| `NONODB_DATA` | Instance root containing database directories |
| `NONODB_SOCK` | Unix socket path |
| `NONODB_TCP` | TCP listen address, e.g. `127.0.0.1:7878` |
| `NONODB_AUTH_FILE` | Auth registry; required for TCP |
| `NONODB_MAP_SIZE` | LMDB address-space limit, not a RAM reservation |

See [Operations](operations.md) for all environment variables.

## Next steps

- [HTTP API](api.md) — operations, filters, updates, aggregation, errors.
- [SQL queries](sql.md) — SQL alongside the document API.
- [Indexes and search](api.md#create_index) — secondary, text, fuzzy, vector.
- [MongoDB compatibility](mongodb-compatibility.md) — official drivers and the
  supported subset.
- [Authentication](auth.md) — principals, tokens, certificates, lockout.
- [Backup and restore](backup.md) — backups, encryption, verification, drills.
- [Operations](operations.md) — deployment, monitoring, capacity, troubleshooting.
- [Docker Compose](docker-compose.md) — self-contained stack and GUI.
- [Raspberry Pi](raspberry-pi.md) — 64-bit Pi 4/5 with Docker.
