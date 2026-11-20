# Docker Compose quick start

This page uses the self-contained
[`deploy/docker-compose.quickstart.yml`](../deploy/docker-compose.quickstart.yml)
stack. It needs only Docker and Docker Compose; you do not need to install
`nonodb-cli` on the host.

The stack:

- runs the daemon as the unprivileged `nonodb` user;
- stores database files in the `nonodb-data` volume;
- stores the auth registry in the separate `nonodb-auth` volume;
- initializes the auth registry and first admin token with a one-shot
  `auth-init` service;
- exposes the HTTP API on `127.0.0.1:7878`;
- offers an optional GUI profile on `127.0.0.1:8787`.

TCP always requires authentication. NonoDB has no default password, so the
first run must create the registry and a token.

## 1. Create auth and the first token

From the repository root:

```bash
docker compose -f deploy/docker-compose.quickstart.yml \
  --profile init run --rm auth-init
```

The command prints a bearer token. Copy it and keep it secret:

```text
created /etc/nonodb/auth.json; now run: nonodb-cli auth token create admin
<token>
```

The `auth-init` service runs as root only for this one-shot setup. It writes the
registry into the `nonodb-auth` volume and then changes ownership to the
unprivileged `nonodb` user. The daemon itself never runs as root.

Run this command only once per auth volume. If it reports that the file already
exists, the registry is already initialized.

## 2. Start the daemon

```bash
docker compose -f deploy/docker-compose.quickstart.yml up -d
```

Check health and logs:

```bash
curl -s http://127.0.0.1:7878/healthz
docker compose -f deploy/docker-compose.quickstart.yml logs -f nonodb
```

## 3. Use the API

Set the token printed by `auth-init`:

```bash
export TOKEN='paste-token-here'

curl -s -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -X POST http://127.0.0.1:7878/v1/exec \
  -d '{"op":"insert","coll":"users","doc":{"name":"Ada","age":36}}'

curl -s -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -X POST http://127.0.0.1:7878/v1/exec \
  -d '{"op":"find","coll":"users","filter":{}}'
```

Python client:

```python
from nonodb import NonoDB

db = NonoDB(base_url="http://127.0.0.1:7878", token="paste-token-here")
print(db.collection("users").find({}))
db.close()
```

## Optional GUI

Start the GUI profile:

```bash
docker compose -f deploy/docker-compose.quickstart.yml --profile gui up -d
```

Open `http://127.0.0.1:8787`. The GUI talks to the daemon over the shared Unix
socket and does not need the bearer token.

## Stop, update, and remove

Stop containers but keep data and auth:

```bash
docker compose -f deploy/docker-compose.quickstart.yml down
```

Update the image:

```bash
docker compose -f deploy/docker-compose.quickstart.yml pull
docker compose -f deploy/docker-compose.quickstart.yml up -d
```

Pin a version in production. The default example uses `0.27.0`; override the
image explicitly if needed:

```bash
NONODB_IMAGE=ghcr.io/essmos/nonodb:0.27.0 \
  docker compose -f deploy/docker-compose.quickstart.yml up -d
```

Remove containers **and volumes** only when you intend to delete the database
and all credentials:

```bash
docker compose -f deploy/docker-compose.quickstart.yml down -v
```

## Backups and auth

Back up the database with the normal backup API, scheduled backup jobs, or
`nonodb-cli backup`. See [Backup and restore](backup.md).

The auth registry is in the separate `nonodb-auth` volume. Back it up if you
want to preserve existing tokens and principals. If it is lost, the database is
still intact, but you must initialize a new registry and issue new tokens.

For filesystem compression, put `nonodb-data` on a ZFS/Btrfs bind mount instead
of a named volume; see [Volume compression](volume-compression.md).

## Troubleshooting

### `auth-init` says the file already exists

The `nonodb-auth` volume already has a registry. Skip the init step and start
the daemon. If you intentionally want to reset all credentials, stop the stack
and remove only the auth volume:

```bash
docker compose -f deploy/docker-compose.quickstart.yml down
docker volume rm nonodb-auth
```

This invalidates all existing bearer tokens.

### Port already in use

Change the host side of the port mapping, for example:

```yaml
ports:
  - "127.0.0.1:7879:7878"
```

Then use `http://127.0.0.1:7879` in clients.

### Permission denied in the auth volume

Use the provided `auth-init` service rather than writing `auth.json` by hand.
It sets ownership to the user the daemon runs as. Do not mount the auth volume
read-write into the daemon.

### I want a host auth file instead of a volume

The repository also includes [`deploy/docker-compose.yml`](../deploy/docker-compose.yml),
which uses a Compose secret from a host file. That variant is useful when the
auth registry is managed by the host or another secret manager.

## Related documentation

- [Operations — Docker](operations.md#docker)
- [Raspberry Pi with Docker](raspberry-pi.md)
- [Authentication and permissions](auth.md)
- [Backup and restore](backup.md)
- [Volume compression](volume-compression.md)
- [HTTP API](api.md)
