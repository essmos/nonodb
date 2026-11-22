# Raspberry Pi with Docker

NonoDB has a published `linux/arm64` container image, so a 64-bit Raspberry Pi
can run it without a separate ARM binary. This is the supported path for
Raspberry Pi 4 and Raspberry Pi 5.

This page is a practical deployment note, not a promise that every Pi model is
suitable. For real data, use a 64-bit OS and an SSD.

## What works

| Setup | Status |
|---|---|
| Raspberry Pi 4 / 5, 64-bit Raspberry Pi OS | Recommended |
| Raspberry Pi 4 / 5, 4 GB RAM or more | Recommended |
| Raspberry Pi 3, 64-bit OS, small data set | Possible, but slow |
| 32-bit Raspberry Pi OS | Not supported by the image |
| Raspberry Pi Zero / 1 | Not recommended |
| SD card as the only data disk | Not recommended for writes |

Check the architecture first:

```bash
uname -m
```

Expected:

```text
aarch64
```

If it prints `armv7l` or `armv6l`, you are on a 32-bit system. The published
container is `linux/arm64`; reinstall with the 64-bit Raspberry Pi OS.

## Prerequisites

- Raspberry Pi 4 or 5 with 4 GB RAM or more;
- 64-bit Raspberry Pi OS Bookworm;
- Docker Engine with the Compose plugin;
- an SSD or USB SSD for the database directory;
- a reliable power supply.

Install Docker using the official Docker instructions for Debian/Raspberry Pi
OS. Verify both pieces:

```bash
docker version
docker compose version
```

Add your user to the `docker` group if needed:

```bash
sudo usermod -aG docker "$USER"
newgrp docker
```

## Get the Compose file

Clone the repository or download the quickstart file:

```bash
git clone --depth 1 https://github.com/essmos/nonodb.git
cd nonodb
```

The stack is:

```text
deploy/docker-compose.quickstart.yml
```

Pin the image explicitly in production. The file defaults to `0.29.1`; you can
override it:

```bash
export NONODB_IMAGE=ghcr.io/essmos/nonodb:0.29.1
```

## Initialize auth once

The quickstart stack creates the auth registry and first admin token in a Docker
volume. You do not need `nonodb-cli` on the Pi:

```bash
docker compose -f deploy/docker-compose.quickstart.yml \
  --profile init run --rm auth-init
```

Copy the printed bearer token and keep it secret.

## Start NonoDB

```bash
docker compose -f deploy/docker-compose.quickstart.yml up -d
```

Check it:

```bash
curl -s http://127.0.0.1:7878/healthz
docker compose -f deploy/docker-compose.quickstart.yml logs -f nonodb
```

The default port mapping binds only to loopback:

```text
127.0.0.1:7878:7878
```

That is intentional. To use the API from another machine, use an SSH tunnel:

```bash
ssh -L 7878:127.0.0.1:7878 pi@raspberrypi.local
```

Then use `http://127.0.0.1:7878` on your workstation. Do not expose port `7878`
directly to the internet without TLS and a reverse proxy.

## Optional GUI

```bash
docker compose -f deploy/docker-compose.quickstart.yml --profile gui up -d
```

The GUI also binds to loopback on port `8787`. Use an SSH tunnel:

```bash
ssh -L 8787:127.0.0.1:8787 -L 7878:127.0.0.1:7878 pi@raspberrypi.local
```

Then open `http://127.0.0.1:8787`.

## Put the database on an SSD

The quickstart file uses a Docker named volume, which normally lives under
`/var/lib/docker`. On a Pi, that is often the SD card.

For production, replace the data volume in
`deploy/docker-compose.quickstart.yml` with a bind mount on the SSD:

```yaml
services:
  nonodb:
    volumes:
      - /mnt/ssd/nonodb:/var/lib/nonodb
      - nonodb-auth:/etc/nonodb:ro
```

Create the directory and set ownership to the user inside the image. Get the
numeric IDs with:

```bash
docker run --rm --entrypoint sh \
  ghcr.io/essmos/nonodb:0.29.1 \
  -c 'id -u; id -g'
```

Then:

```bash
sudo mkdir -p /mnt/ssd/nonodb
sudo chown -R UID:GID /mnt/ssd/nonodb
```

Use the values printed by the command above. Keep `nonodb-auth` as a separate
volume.

Do not put the live LMDB files on NFS or SMB. A local SSD is required for
reliable locking, mmap, and fsync behavior.

## Updates

```bash
docker compose -f deploy/docker-compose.quickstart.yml pull
docker compose -f deploy/docker-compose.quickstart.yml up -d
```

Use a fixed image tag, not `latest`, so an unexpected pull cannot change the
version under you.

## Backups

Do not rely on the SD card or a single SSD. Configure backups to an external
destination and test a restore. See [Backup and restore](backup.md) and
[Docker Compose quick start](docker-compose.md).

## Performance expectations

A Raspberry Pi is useful for small, local workloads:

- tenant settings, feature flags, small catalogs;
- background jobs and event queues;
- local edge caches;
- low-to-moderate indexed reads and writes.

Avoid using a Pi as the primary host for:

- large full-collection scans;
- heavy aggregation;
- very large BM25 or vector indexes;
- high write throughput on an SD card;
- workloads that need low p99 latency under sustained load.

Measure with your own data. A Pi 5 with an SSD is in a different class from a Pi 3
with an SD card.

## Troubleshooting

### `exec format error`

You are probably running 32-bit Raspberry Pi OS. Check:

```bash
uname -m
```

It must be `aarch64`. Reinstall the 64-bit OS.

### `no matching manifest for linux/arm/v7`

The image is published for `linux/arm64`, not 32-bit ARM. Use a 64-bit OS or run
NonoDB on another host.

### Container starts, then restarts

Check logs:

```bash
docker compose -f deploy/docker-compose.quickstart.yml logs nonodb
```

The most common causes are a missing auth file, wrong ownership on a bind mount,
or a port already in use.

### Auth init says the file already exists

The auth volume is already initialized. Skip the init step and start the stack.
If you intentionally want to reset all credentials, remove only the auth volume:

```bash
docker compose -f deploy/docker-compose.quickstart.yml down
docker volume rm deploy_nonodb-auth
```

The volume name depends on the Compose project name. Check it with:

```bash
docker volume ls
```

Removing the auth volume invalidates all existing bearer tokens.

## Related documentation

- [Docker Compose quick start](docker-compose.md)
- [Operations](operations.md)
- [Backup and restore](backup.md)
- [Volume compression](volume-compression.md)
