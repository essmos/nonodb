# Free-tier and low-cost VM deployment

This guide covers free-tier VMs such as Oracle Cloud Always Free, Google Cloud
free tier, and AWS free tier, as well as low-cost VMs such as a DigitalOcean
Droplet. Each shape uses a persistent block volume, Docker, and backups to
S3-compatible object storage.

Despite its broad feature set, NonoDB is designed to run well on low-resource
servers. A database with 1 million documents takes only about 5 MB.

## Who this is for

Use this shape when:

- the database is small enough to fit comfortably on the VM's disk;
- one writer is enough;
- a short maintenance window for restores is acceptable;
- you want to stay on a free tier or a very small paid VM.

Typical targets:

| Provider | Shape | Notes |
|---|---|---|
| Oracle Cloud Always Free | Ampere A1, 1–4 OCPU, 6–24 GB RAM | ARM64 image; generous for NonoDB |
| Google Cloud free tier | `e2-micro`, 1 GB RAM, 30 GB standard PD | Only in eligible regions |
| AWS free tier | `t3.micro`/`t4g.micro`, EBS volume | Limits and eligibility change; check current terms |
| Azure free account | B1s, 750 hours/month for 12 months | Not always free; managed disks and egress may be billed separately |
| DigitalOcean | Small Droplet + block volume | Not free tier; low monthly cost; Spaces for backups |

Free-tier limits change. Verify the current CPU, RAM, disk, transfer, and
always-free conditions before relying on them. Use a persistent block volume,
not an instance-local scratch disk.

## Sizing and profiles

NonoDB has a small runtime profile that fills only variables you did not set
explicitly:

```bash
NONODB_PROFILE=small
```

| Setting | `small` | `tiny` |
|---|---:|---:|
| Tokio blocking threads | 8 | 4 |
| Open LMDB environments | 8 | 4 |
| Read-cache entries | 128 | 64 |
| Max cached result | 64 KiB | 32 KiB |
| Scan workers | 2 | 1 |
| Backfill workers | 1 | 1 |
| Aggregate workers | 1 | 1 |
| TTL sweep interval | 10 s | 15 s |
| TTL sweep limit | 128 | 64 |

Use `small` on a 1–2 GB VM. Use `tiny` on a 512 MB–1 GB VM or when the daemon
must share the host with an application. Explicit environment variables always
win over the profile.

`NONODB_MAP_SIZE` is an address-space reservation, not a RAM allocation. It
defaults to `1GiB`. If the database file approaches that limit, raise it online:

```bash
nonodb-cli map
nonodb-cli resize 4GiB
```

Leave enough disk for the live database, one compacted backup copy, Docker
images, and the operating system. A 30 GB volume is a reasonable minimum for a
small deployment.

## Quick start on a Google Cloud free-tier VM

Create an eligible `e2-micro` instance with a persistent standard disk:

```bash
gcloud compute instances create nonodb \
  --zone=us-central1-a \
  --machine-type=e2-micro \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=30GB \
  --boot-disk-type=pd-standard \
  --tags=nonodb
```

Use an eligible free-tier region such as `us-central1`, `us-west1`, or
`us-east1`. Install Docker Engine and the
Compose plugin using the official Docker instructions for Ubuntu, then verify:

```bash
docker version
docker compose version
```

Clone the repository on the VM:

```bash
git clone https://github.com/essmos/nonodb.git
cd nonodb
mkdir -p config
```

Create the authorization registry. The commands below use the published
container so the host does not need a Rust toolchain. Replace the image tag if
you pin a different release:

```bash
export NONODB_AUTH_FILE="$PWD/config/auth.json"

docker run --rm -u "$(id -u):$(id -g)" \
  -e NONODB_AUTH_FILE=/config/auth.json \
  -v "$PWD/config:/config" \
  ghcr.io/essmos/nonodb:0.27.0 \
  nonodb-cli auth init

docker run --rm -u "$(id -u):$(id -g)" \
  -e NONODB_AUTH_FILE=/config/auth.json \
  -v "$PWD/config:/config" \
  ghcr.io/essmos/nonodb:0.27.0 \
  nonodb-cli auth token create admin
```

Store the printed admin token in a password manager or secret store. Do not
commit it. For applications, create a separate principal and token with only
the collection scopes it needs.

Create a host `.env` file next to the Compose file. Compose reads it
automatically:

```dotenv
NONODB_AUTH_FILE=/home/ubuntu/nonodb/config/auth.json
NONODB_PROFILE=small
NONODB_BIND=127.0.0.1
RUST_LOG=info
```

Start the daemon:

```bash
docker compose -f deploy/docker-compose.free-tier.yml up -d
docker compose -f deploy/docker-compose.free-tier.yml ps
docker compose -f deploy/docker-compose.free-tier.yml logs -f nonodb
```

Verify readiness:

```bash
docker exec nonodb nonodb-cli ready
docker exec nonodb nonodb-cli ping
```

The Compose file publishes port `7878` only on `127.0.0.1`. Applications on the
same VM can connect through `http://127.0.0.1:7878`. Applications in another
Compose service can use the service name `nonodb:7878` on a shared Docker
network.

### Remote access

For occasional administration, use an SSH tunnel instead of exposing the port:

```bash
ssh -L 7878:127.0.0.1:7878 ubuntu@VM_IP
```

Then point the client or CLI at `http://127.0.0.1:7878`.

If remote applications must connect directly, put a TLS reverse proxy such as
Caddy or nginx in front of the loopback listener. Terminate TLS at the proxy,
keep the NonoDB port firewalled, and still use NonoDB bearer tokens. Plaintext
TCP on a public interface is not acceptable.

### Oracle Cloud Always Free

The same Compose flow works on Oracle Ampere A1. The published image is
multi-architecture and includes `linux/arm64`.

1. Create an Ubuntu 22.04/24.04 Ampere instance with a block volume.
2. Install Docker and the Compose plugin.
3. Allow inbound SSH only in the security list.
4. Follow the Google Cloud steps above for auth, `.env`, and Compose.

Do not open port `7878` to the internet unless you have a TLS proxy and a
restrictive firewall rule.

### Azure free account (12 months)

Azure's free account includes 750 hours per month of a B1s Linux VM for the
first 12 months. This is useful if you already use Azure, but it is not an
always-free tier: after 12 months normal rates apply, and managed disks, public
IPs, bandwidth, and backups may be billed separately.

Use a separate managed data disk for the database. The VM's temporary disk is
ephemeral and must not hold the only copy.

1. Create a B1s Ubuntu 24.04 VM. Keep the OS disk small and attach a separate
   managed data disk for `/mnt/nonodb-data`.
2. In the Azure network security group, allow inbound SSH only. Do not open
   `7878` to the internet.
3. On the VM, find the data disk:

```bash
lsblk
ls -l /dev/disk/by-id/
```

4. Format and mount the new disk. Replace `/dev/disk/by-id/...` with the data
   disk, never the OS disk:

```bash
sudo mkfs.ext4 /dev/disk/by-id/DATA_DISK_ID
sudo mkdir -p /mnt/nonodb-data
sudo mount /dev/disk/by-id/DATA_DISK_ID /mnt/nonodb-data

echo '/dev/disk/by-id/DATA_DISK_ID /mnt/nonodb-data ext4 defaults,nofail,discard 0 2' \
  | sudo tee -a /etc/fstab
```

5. Install Docker Engine and the Compose plugin.
6. Follow the Google Cloud steps above for auth, `.env`, and Compose, but add
   the data-disk path:

```dotenv
NONODB_DATA_DIR=/mnt/nonodb-data
```

7. Make the mounted volume writable by the container user:

```bash
docker run --rm --user root \
  -v /mnt/nonodb-data:/var/lib/nonodb \
  ghcr.io/essmos/nonodb:0.27.0 \
  chown -R nonodb:nonodb /var/lib/nonodb
```

8. Start and verify with the same Compose commands. If you delete and recreate
   the VM, keep or reattach the managed data disk; do not delete it with the VM.

## DigitalOcean

DigitalOcean is a practical low-cost option when a free-tier VM is unavailable.
Use a Droplet with a separate block volume for the database. Do not keep the
only copy on the Droplet's local disk.

### Create the Droplet and volume

1. Create an Ubuntu 24.04 Droplet in a region where Spaces is available. Use
   the smallest size that comfortably fits the database and backups.
2. Create a block volume in the same region and attach it to the Droplet.
3. Format and mount the volume:

```bash
lsblk

sudo mkfs.ext4 /dev/disk/by-id/scsi-0DO_Volume_nonodb-data
sudo mkdir -p /mnt/nonodb-data
sudo mount /dev/disk/by-id/scsi-0DO_Volume_nonodb-data /mnt/nonodb-data

echo '/dev/disk/by-id/scsi-0DO_Volume_nonodb-data /mnt/nonodb-data ext4 defaults,nofail,discard 0 2' \
  | sudo tee -a /etc/fstab
```

4. Install Docker Engine and the Compose plugin.
5. Clone the repository and create the auth registry as in the Google Cloud
   section above.
6. Make the mounted volume writable by the container user:

```bash
docker run --rm --user root \
  -v /mnt/nonodb-data:/var/lib/nonodb \
  ghcr.io/essmos/nonodb:0.27.0 \
  chown -R nonodb:nonodb /var/lib/nonodb
```

7. Point Compose at the block volume in the host `.env`:

```dotenv
NONODB_DATA_DIR=/mnt/nonodb-data
NONODB_AUTH_FILE=/root/nonodb/config/auth.json
NONODB_PROFILE=small
NONODB_BIND=127.0.0.1
RUST_LOG=info
```

8. Start and verify:

```bash
docker compose -f deploy/docker-compose.free-tier.yml up -d
docker exec nonodb nonodb-cli ready
```

Create a DigitalOcean Cloud Firewall rule that allows SSH only. Put application
traffic on a private VPC or reach the loopback listener through an SSH tunnel.
Do not expose plaintext port `7878` publicly.

## Cloud Run: read replicas only

Cloud Run is useful for **read-only replicas**, not for the primary writer.
Every Cloud Run instance has an ephemeral filesystem and can be stopped at any
time. A primary database cannot live there safely.

A Cloud Run read replica uses the existing replication feature:

```dotenv
NONODB_DATA=/tmp/nonodb
NONODB_AUTH_FILE=/etc/nonodb/auth.json
NONODB_TCP=0.0.0.0:7878
NONODB_PROFILE=small

NONODB_REPLICATION_ROLE=replica
NONODB_REPLICATION_NODE_ID=cloudrun-read-1
NONODB_REPLICATION_SOURCE=https://primary.example.com:7878
NONODB_REPLICATION_SOURCE_TOKEN=REPLICATION_ADMIN_TOKEN
NONODB_REPLICATION_DATABASES=default
NONODB_REPLICATION_BOOTSTRAP=auto
NONODB_REPLICATION_ADVERTISE_URL=https://nonodb-read-....run.app
```

Mount `auth.json` from Secret Manager. The source token should also come from
Secret Manager. `NONODB_MAX_OPEN_DATABASES` and worker counts are supplied by
the `small` profile.

Operational notes:

- `NONODB_DATA` must be under `/tmp` or another writable path. The local
  database is rebuilt from the primary snapshot after a cold start.
- Keep the instance memory larger than the downloaded database. `/tmp` uses
  memory on Cloud Run.
- `min-instances=0` is suitable only for demos; every cold start pays the
  snapshot download. Use `min-instances=1` for predictable latency.
- Scale read replicas horizontally; they are read-only and do not accept
  writes.
- If the primary is unavailable, a fresh replica cannot bootstrap.
- Do not use this section to run the primary writer on Cloud Run with
  object-storage checkpoints. That has a non-zero recovery point and a much
  weaker durability story.

See [Replication and cluster guide](replication-cluster-guide.md) for pairing,
TLS, promotion, and failover.

## Backups to DigitalOcean Spaces

Spaces is S3-compatible, so NonoDB's native backup jobs work without a special
backend. Use a Space in the same region as the Droplet.

### Create the Space and credentials

1. Create a Space, for example `nonodb-backups`, in the Droplet's region.
2. Create a Spaces access key with read/write access to that Space.
3. Note the endpoint and region, for example:

```text
endpoint: https://fra1.digitaloceanspaces.com
region:   fra1
```

4. Put the credentials in the host `.env`, not in `backups.json`:

```dotenv
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=fra1
```

The Compose file passes these variables to the daemon.

### Configure a daily backup

The example job is in
[`deploy/backups-digitalocean.example.json`](../deploy/backups-digitalocean.example.json).
Copy it into the data volume and edit the endpoint, bucket, region, and prefix:

```bash
cp deploy/backups-digitalocean.example.json config/backups.json
vi config/backups.json

docker cp config/backups.json nonodb:/var/lib/nonodb/backups.json
docker compose -f deploy/docker-compose.free-tier.yml restart nonodb
```

The scheduler runs a due job shortly after startup. Use the GUI **Backups**
tab and **Run now** for the first run, then inspect `backups-state.json` in the
data volume. The daemon writes it next to `backups.json`.

`keep_last` controls local retention, not objects in Spaces. Use a Spaces
lifecycle rule to expire old backups. Keep at least one recent backup outside
the automatic deletion window.

### Restore from Spaces

Download the desired backup directory and restore it while the daemon is
stopped. The manifest and compressed data file are in the same object prefix:

```bash
# Example for a single-database, non-multi job:
aws s3 cp \
  --endpoint-url https://fra1.digitaloceanspaces.com \
  --recursive \
  s3://nonodb-backups/nonodb/daily-default/<timestamp>/ \
  ./restore

docker compose -f deploy/docker-compose.free-tier.yml stop nonodb

docker run --rm \
  -v "${NONODB_DATA_DIR:-nonodb-data}:/var/lib/nonodb" \
  -v "$PWD/restore:/restore" \
  ghcr.io/essmos/nonodb:0.27.0 \
  nonodb-cli restore-all /restore /var/lib/nonodb --force --verify

docker compose -f deploy/docker-compose.free-tier.yml up -d
```

`restore-all` handles gzip-compressed backups. Encrypted age backups require
`NONODB_BACKUP_IDENTITY_FILE` and the private identity file. Verify the restore
before reopening the service to clients.

`auth.json` and the audit log are not part of database backups. Keep a separate
protected copy of `config/auth.json`; otherwise a restore of the data volume
does not restore tokens or certificate registrations.

## Operations checklist

- Keep the data volume on persistent block storage, not on NFS/SMB.
- Back up `auth.json` separately from the database.
- Run `nonodb-cli backup-all` or a scheduled Spaces backup job daily.
- Test one restore on a disposable volume before trusting the backup.
- Monitor disk usage; LMDB files do not shrink after deletes.
- Watch `/readyz` and the daemon logs after every reboot.
- Keep Docker and the NonoDB image patched.
- Do not run two daemons against the same data volume.
- Do not expose plaintext TCP to the internet.

## Benchmark before promising numbers

The free-tier claim should be measured on the actual provider. Run
`scripts/bench.py` from a client container that shares the NonoDB socket volume,
or start a temporary local daemon with `NONODB_PROFILE=small` and point
`NONODB_SOCK` at its socket. Example shape:

```bash
NONODB_SOCK=/tmp/nonodb.sock \
  python3 scripts/bench.py --workers 2 --writes 2000 --reads 5000
```

Record:

- VM shape, RAM, disk type, and provider region;
- daemon RSS after startup and after the test;
- write and read throughput, p50/p95/p99;
- database file size;
- cold-start time;
- backup duration and restore duration.

Publish those numbers with the provider and date. They are more useful than a
generic "works on free tier" claim.
