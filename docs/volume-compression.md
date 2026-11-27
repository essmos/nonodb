# Volume compression

NonoDB stores each database as LMDB files (`<database>/data.mdb` and
`<database>/lock.mdb`). There is currently no document compression inside the
daemon. The lowest-risk way to reduce disk usage is to put the data directory on
a filesystem that compresses blocks transparently, such as ZFS or Btrfs.

This page covers:

- filesystem and volume setup;
- Docker bind mounts versus named volumes;
- verification and benchmarking;
- limitations and migration.

It does **not** describe backup compression. Backup jobs have their own
`compression` setting; see [Backup and restore](backup.md).

## Why filesystem compression

Filesystem compression:

- requires no NonoDB configuration or data-format change;
- compresses documents, indexes, text/fuzzy/vector structures, and free pages;
- keeps the daemon and clients unchanged;
- is transparent on reads and writes;
- can be enabled per dataset or subvolume.

The trade-off is CPU. Reads still decompress data, and full scans still pay that
cost. Compression is most useful for cold or read-mostly collections, and for
deployments where disk capacity or disk throughput is the bottleneck.

Use `zstd` for a good balance. Use `lz4` when CPU is constrained and a lower
ratio is acceptable.

## ZFS

Create a dedicated dataset with compression enabled:

```bash
sudo zfs create -o compression=zstd tank/nonodb
```

The dataset is normally mounted at `/tank/nonodb`. Verify it:

```bash
zfs get compression,compressratio tank/nonodb
zfs list -o name,used,compressratio,compression tank/nonodb
```

`compression=zstd` uses the OpenZFS default zstd level. `zstd-1` is faster;
`zstd-9` or higher compress better but cost more CPU. On older OpenZFS versions
without zstd support, use `compression=lz4`.

Set ownership to the user that runs the daemon or container. For the published
container image, the `nonodb` account is a system user; get its numeric IDs with:

```bash
docker run --rm --entrypoint sh ghcr.io/essmos/nonodb:0.35.0 \
  -c 'id -u; id -g'
```

Then apply them to the dataset root, for example:

```bash
sudo chown -R 999:999 /tank/nonodb
```

Use the values printed by the command, not the example numbers.

Leave the ZFS `recordsize` at its default initially. Smaller records can reduce
read amplification for random access but may reduce the compression ratio.
Benchmark before changing it.

## Btrfs

Create a dedicated subvolume and enable compression:

```bash
sudo btrfs subvolume create /srv/nonodb
sudo btrfs property set /srv/nonodb compression zstd
```

If the property is not available on your kernel, mount the filesystem with
`compress=zstd:3` instead, or set the compression flag on the empty directory
before NonoDB writes any files:

```bash
sudo chattr +c /srv/nonodb
```

Verify:

```bash
btrfs property get /srv/nonodb compression
btrfs filesystem du -s /srv/nonodb
```

`btrfs filesystem du` reports filesystem usage and shared extents; it is more
useful than `ls -l` for compressed data.

Set ownership as described for ZFS.

## Docker

### Recommended: bind mount a compressed dataset

A bind mount is the simplest and most predictable option. The container sees a
normal directory; the host filesystem performs compression.

```yaml
services:
  nonodb:
    image: ghcr.io/essmos/nonodb:0.35.0
    volumes:
      - /tank/nonodb:/var/lib/nonodb
```

For Btrfs, use `/srv/nonodb:/var/lib/nonodb`.

The container does not need `--privileged` and does not need access to the host
filesystem tools. Only the host administrator needs access to create and
configure the dataset.

### Named volumes

A Docker named volume is stored under the Docker root directory, usually
`/var/lib/docker/volumes`. Check the mountpoint:

```bash
docker volume inspect nonodb-data
```

Compression applies only if that underlying directory is on a compressed
filesystem. A typical host with `/var/lib/docker` on ext4 or XFS will **not**
compress the volume.

You can make a named volume point at the compressed dataset:

```yaml
volumes:
  nonodb-data:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /tank/nonodb
```

This is still a bind mount underneath. A direct bind mount is usually clearer.

### Docker Desktop

Docker Desktop on macOS and Windows runs a Linux VM. Database files normally
live on the VM's ext4 filesystem, not directly on the host filesystem. Host
compression such as APFS or NTFS does not automatically apply to VM writes.

For production, use a Linux host with ZFS or Btrfs. If Docker Desktop is
required, put the data on a dedicated disk or VM volume that is formatted with
a compressed filesystem and mounted into the Docker VM.

### Docker Compose

The repository's Compose file uses a named volume by default:

```bash
docker compose -f deploy/docker-compose.yml up -d
```

To use a compressed dataset, replace the data volume with a bind mount in a
local override file:

```yaml
services:
  nonodb:
    volumes:
      - /tank/nonodb:/var/lib/nonodb
```

Keep the socket volume as it is. Do not bind-mount a single socket file; mount
the containing directory.

## Kubernetes

Use a PersistentVolume backed by a StorageClass that provides ZFS or Btrfs
compression, and mount it at `/var/lib/nonodb`. NonoDB needs no special
configuration.

Do not assume that the node's root filesystem is compressed. Check the
StorageClass and the underlying volume, not just the pod spec.

## Verify that compression is active

For ZFS:

```bash
zfs get compression,compressratio tank/nonodb
zfs list -o name,used,logicalused,compressratio tank/nonodb
```

For Btrfs:

```bash
btrfs property get /srv/nonodb compression
btrfs filesystem du -s /srv/nonodb
```

The logical size of `data.mdb` does not change when filesystem compression is
enabled. Use the filesystem tools, not `ls -l`, to see physical usage.

The compression ratio may remain `1.00` until enough data has been written.
Filesystem compression is applied to new writes; existing blocks are not
rewritten automatically just because the property changed.

## Benchmark before enabling in production

Compression trades disk I/O for CPU. Measure on a copy of real data:

- `find_one` by `_id`;
- indexed `find` with a small result set;
- indexed range scans;
- full collection scans;
- `count` and `distinct`;
- aggregation and sorting;
- writes and updates;
- text, fuzzy, and vector search.

Record p50, p95, and p99 latency, plus CPU usage and physical disk usage.
Full scans decompress all matching data and are the most likely to regress.
Index-only and covered queries often see little or no change.

On fast NVMe storage, CPU-bound decompression can reduce throughput. On slower
disks or network-backed storage, reading fewer bytes can improve throughput.

## Migrating an existing database

Stop the daemon before copying LMDB files. Never copy a live LMDB environment
while another process is writing it.

For a direct host deployment:

1. Stop `nonodb-daemon`.
2. Create the compressed dataset or subvolume.
3. Copy the instance root, preserving ownership and permissions.
4. Start the daemon with `NONODB_DATA` pointing at the new path.
5. Verify collections, indexes, and a backup/restore drill.

For Docker:

1. Stop the container.
2. Copy the old volume contents to the compressed dataset.
3. Change the Compose volume to a bind mount.
4. Start the container and verify health.

A compacting backup/restore is also a safe way to move to a new volume and can
reduce the logical `data.mdb` size at the same time. See
[Backup and restore](backup.md).

## Limitations

- `ext4` and `XFS` do not provide transparent general-purpose file compression.
  Enabling compression there requires a new filesystem or volume.
- Network filesystems such as NFS and SMB are not supported for the NonoDB data
  directory. Do not use them for this purpose.
- Vault ciphertext and already-compressed data usually gain little.
- `data.mdb` does not shrink in place after deletes or when compression is
  enabled. A compacting backup/restore is still required to reclaim logical
  space.
- Filesystem compression changes physical usage, not LMDB's `map_size`.
- Compression is not a replacement for backups, snapshots, or replication.

## Related documentation

- [Operations](operations.md) — deployment, disk capacity, and memory guidance
- [Backup and restore](backup.md) — backup compression is separate from volume compression
- [Architecture](architecture.md) — LMDB layout and write path
- [Benchmarks](benchmarks.md) — benchmark methodology and results
