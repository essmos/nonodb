# Encryption at rest

NonoDB stores standard collections as plaintext JSON in LMDB. It has no native
volume or filesystem encryption. Protect the data directory with operating
system or cloud volume encryption, and use [vault collections](vault.md) when
individual document bodies also need application-level encryption.

These layers solve different problems:

| Layer | Protects | Does not protect |
|---|---|---|
| LUKS, ZFS encryption, or cloud volume encryption | The whole data directory, LMDB free pages, indexes, sidecars, and locally staged backups while the volume is locked | Data while the daemon is running and the volume is unlocked; keys stored on the same unlocked disk |
| [Vault collections](vault.md) | Document bodies at rest, including inside backups and snapshots, without the tenant key | Metadata, `_id`, `tenant_id`, indexes, daemon memory, query patterns, or the running daemon's plaintext view |
| age or restic backup encryption | Copies in the backup destination or object store | The live database volume or an already-compromised host |
| TLS / mTLS | Data in transit | Data at rest or secrets in process memory |

Use volume encryption as the baseline even when vault collections are enabled.
Use backup encryption in addition to both; an encrypted live volume does not
make an unencrypted copy in object storage safe.

## Threat model and boundaries

Volume encryption protects a powered-off or removed disk. It does not protect
against a host administrator, a compromised daemon, a process that can read the
unlocked filesystem, or data in memory. The daemon sees plaintext for standard
collections and sees plaintext plus the raw tenant key in memory for vault
reads.

Also cover places where plaintext can leak outside LMDB:

- swap and hibernation files;
- daemon core dumps;
- temporary backup staging under `NONODB_BACKUP_STAGING`;
- copied `data.mdb` files outside the encrypted volume;
- backups and their keys;
- logs and tracing output that include document content;
- Docker's root directory when a named volume is not on an encrypted disk.

Disable core dumps for the daemon (`LimitCORE=0` in systemd) and disable swap or
encrypt it. Keep the daemon's Unix socket and TCP listener protected with normal
filesystem permissions and [authentication](auth.md).

## LUKS on Linux

LUKS encrypts a block device or partition. The filesystem and NonoDB are
unchanged.

```bash
# Replace /dev/sdX with the dedicated data device. This destroys its contents.
sudo cryptsetup luksFormat /dev/sdX
sudo cryptsetup open /dev/sdX nonodb-crypt
sudo mkfs.ext4 /dev/mapper/nonodb-crypt
sudo mkdir -p /var/lib/nonodb
sudo mount /dev/mapper/nonodb-crypt /var/lib/nonodb
```

Mount it before starting the daemon and before any backup or restore operation.
For boot-time unlocking, add an entry to `/etc/crypttab` and mount
`/dev/mapper/nonodb-crypt` from `/etc/fstab`.

A key file can support unattended unlocking, but a key stored on the same
unencrypted root disk largely defeats the protection. Prefer a passphrase
entered at boot, a TPM-bound key with a documented recovery path, or a key
delivered from a separate secret store. If regular reboots are unattended,
document what happens when the unlock fails.

For Docker, encrypt the host directory or block device that backs the bind
mount. Do not put the data only in an unencrypted container layer:

```yaml
services:
  nonodb:
    image: ghcr.io/essmos/nonodb:0.31.0
    volumes:
      - /var/lib/nonodb:/var/lib/nonodb
```

If you use a Docker named volume, verify that its mountpoint is on encrypted
storage. See [Volume compression](volume-compression.md#docker) for the
bind-mount versus named-volume distinction; the same placement rules apply.

Before rotating a LUKS key or re-encrypting the device, take a verified backup,
stop the daemon, and test the unlock and restore procedure in a scratch
environment. Keep at least one recovery key or passphrase outside the host.

## ZFS native encryption

ZFS can encrypt a dedicated dataset while still using compression:

```bash
sudo zfs create \
  -o compression=zstd \
  -o encryption=on \
  -o keyformat=passphrase \
  -o keylocation=prompt \
  tank/nonodb
sudo zfs mount tank/nonodb
```

Load the key after a reboot:

```bash
sudo zfs load-key tank/nonodb
sudo zfs mount tank/nonodb
```

Check the state with:

```bash
zfs get encryption,keystatus,compression tank/nonodb
```

A raw key file (`-o keyformat=raw -o keylocation=file:///path`) can be used, but
the key file must be available before the dataset is mounted and must not live
on the same unlocked pool. Store it in a separate secret manager or on an
encrypted system volume with a documented recovery copy.

ZFS snapshots of an encrypted dataset remain encrypted when sent with raw `zfs
send -w`, but a filesystem snapshot is not a substitute for a consistent
NonoDB backup. Use the [backup and restore](backup.md) procedure for the
database, and treat ZFS snapshots as an additional host-level recovery layer.

## Cloud block volumes

Cloud providers encrypt block storage by default in many configurations, for
example EBS, Google Persistent Disk, and Azure managed disks. Confirm that the
data disk, Docker root, swap, and backup destinations are all covered. Use a
customer-managed key when the threat model requires control over key policy,
rotation, and revocation, and make sure snapshots inherit the expected key.

Provider-managed encryption protects against physical media loss but not against
a compromised instance with the volume attached. The same daemon-memory and
plaintext-view boundaries apply.

## Backup keys

Encrypted backups are only as recoverable as their keys. NonoDB supports native
age encryption and restic repositories; see
[Backup and restore](backup.md#encryption) and the
[restic engine](backup.md#restic-engine).

For native age backups:

```bash
nonodb-cli backup-keygen /etc/nonodb/backup-identity.txt
# stdout is the public recipient; store it in the job's recipient_file.
```

- The identity file is the private restore key. Keep it out of the backup
  destination and out of the database volume.
- Restore with `NONODB_BACKUP_IDENTITY_FILE` pointing at the identity.
- `NONODB_BACKUP_KEY_DIR` controls where the GUI writes generated key files; the
  default is `<NONODB_DATA>/backup-keys`. A key directory on the same volume as
  the database is not a separate protection domain. Mount a separate key volume
  when the data volume is what might be lost.
- A lost age identity makes encrypted native backups unrecoverable.

For restic, keep the repository password in a protected file or secret manager
and use `RESTIC_PASSWORD_FILE` or the job's password-file setting. A restic
repository cannot be opened without that password, even if the repository is
intact.

A practical key policy:

1. Keep volume keys, vault tenant keys, backup keys, TLS private keys, and
   authentication tokens in different scopes.
2. Escrow a recovery copy in a different failure domain from the data.
3. Test key recovery during the regular restore drill, not only after an
   incident.
4. Rotate keys on a schedule and after suspected exposure. Keep old keys until
   every backup and vault revision that needs them has expired.
5. Record who can restore, where the keys live, and how to revoke them.

## Vault boundaries

Vault collections encrypt the whole document body with a per-tenant key. The
stored envelope keeps `_id`, `tenant_id`, and `kid` in plaintext so the primary
key and tenant isolation work; indexes, collection metadata, TTL metadata, and
query access patterns are outside the encrypted payload. Vault is not
end-to-end encryption: the daemon receives the plaintext and the raw key in
memory while serving a request.

Vault also has operational limits:

- no search, fuzzy search, vector search, aggregation, secondary indexes, or
  sorting over encrypted documents;
- `batch` and `write_batch` are rejected for vault collections;
- date-field TTL is not supported;
- key rotation is an offline read-with-old-key/write-with-new-key procedure; the
  daemon does not keep a keyring of retired keys.

Back up tenant keys separately from the database. A vault backup without the
tenant key still contains `_id`, `tenant_id`, and `kid`, but not readable
document bodies. Losing the key means losing those bodies. See
[Vault collections](vault.md#cache-snapshots-and-backups).

For data that must be unreadable to the daemon itself, use client-side
encryption and store the resulting ciphertext as an ordinary field. NonoDB can
then index and search only the fields you deliberately leave in clear.

## Checklist

- [ ] The data directory, Docker volume, swap, and backup staging are on
      encrypted storage.
- [ ] The volume unlocks before the daemon starts, and an unlock failure is
      monitored.
- [ ] Volume keys are stored outside the encrypted data volume and have an
      escrow or recovery path.
- [ ] Backups use age or restic encryption, with keys stored separately from the
      backup destination.
- [ ] Vault tenant keys are backed up separately and key recovery is tested.
- [ ] Core dumps are disabled and logs do not contain document bodies or keys.
- [ ] A restore drill decrypts a real backup on a clean host and runs real
      queries.
- [ ] Key rotation and revocation are documented, including the retention period
      for old backups and vault revisions.
- [ ] Deleting a volume or destroying its key is understood as irreversible
      crypto-shredding; verified backups exist before that step.
