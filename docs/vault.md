# Vault collections: optional encrypted storage

Vault collections are an **opt-in** feature for data that must not be stored as
plaintext in LMDB, backups, or snapshots. They are not the default and they do
not change the behaviour of standard collections.

The trade-off is deliberate: a vault collection gives up most query features in
exchange for whole-document encryption and per-tenant keys.

Use vault collections when:

- you store tenant secrets, credentials, personal notes, payment metadata, or
  other sensitive payloads;
- you need encryption at rest and can accept lookup by `_id` or `tenant_id` only;
- you want a small number of encrypted resources without running a separate
  secret store;
- you accept that full-text search, fuzzy search, aggregation, secondary
  indexes, arbitrary filters, and sorting are unavailable.

Do **not** use vault collections when:

- you need full-text or fuzzy search over the encrypted data;
- you need range queries, sorting, or aggregation on encrypted fields;
- you need per-field encryption while keeping some fields queryable (this MVP
  encrypts the whole document body);
- you need true end-to-end encryption where the daemon never sees plaintext.

## Profiles

Every collection has one immutable profile:

| Profile | Behaviour |
|---|---|
| `standard` | Default. Plaintext documents, full query/index/search features. |
| `vault` | Encrypted documents, restricted API, per-request key. |

A collection cannot be converted from `standard` to `vault` or back in place.
Create a new collection and migrate the data instead.

Standard collections are unchanged. The vault code path is only used when the
collection metadata says `profile: "vault"`.

## Creating a vault collection

```json
{
  "op": "create_collection",
  "coll": "vault",
  "profile": "vault"
}
```

Response:

```json
{"ok": true, "result": {"created": "vault", "profile": "vault"}}
```

An existing collection with a different profile is rejected with
`vault_profile_immutable`.

Standard collections can still be created implicitly on first write, or
explicitly with `profile: "standard"`.

## Credentials: headers, not JSON bodies

Every request against a vault collection must include all three headers:

```http
X-NonoDB-Tenant: acme
X-NonoDB-Key-Id: acme:v1
X-NonoDB-Key: <base64-encoded-32-byte-key>
```

| Header | Meaning |
|---|---|
| `X-NonoDB-Tenant` | Tenant id; must match the document's plaintext `tenant_id`. |
| `X-NonoDB-Key-Id` | Key version identifier stored in the envelope; used for rotation and diagnostics. |
| `X-NonoDB-Key` | 32-byte tenant key, standard or URL-safe base64. |

Rules:

- The key is **never** placed in the request body.
- The daemon never logs these headers.
- Missing headers return `vault_key_required`.
- Sending vault headers to a standard collection returns `vault_key_unexpected`.
- A malformed or wrong-length key returns `invalid_vault_key`.
- A wrong key, wrong tenant on a decrypting operation, or corrupted envelope returns `decryption_failed`.
- A lookup by `_id` under a different tenant simply returns no document.

The key id is not a secret. The key itself is.

Generate a key:

```bash
python3 -c 'import base64, os; print(base64.b64encode(os.urandom(32)).decode())'
```

Store it in your secret manager or environment. Do not commit it to the
repository.

## Stored format

A vault document is stored as:

```json
{
  "_id": "8f2c...",
  "tenant_id": "acme",
  "_enc": {
    "v": 1,
    "alg": "xchacha20poly1305",
    "kid": "acme:v1",
    "n": "base64-nonce",
    "ct": "base64-ciphertext"
  }
}
```

Details:

- `_id` and `tenant_id` remain plaintext so the primary key and tenant isolation
  work without decrypting every document.
- The complete original document, including its own `_id` and `tenant_id`, is
  encrypted into `_enc.ct`.
- `alg` is XChaCha20-Poly1305.
- `n` is a random 24-byte nonce.
- `kid` is the key id from the request.
- Associated data (AAD) binds the ciphertext to the collection, tenant, `_id`,
  and key id. Moving an envelope to another document or tenant fails
  decryption.
- The envelope version is `v: 1` for future migration.

## Supported operations

| Operation | Vault support |
|---|---|
| `create_collection` | ✅ with `profile: "vault"` |
| `insert` | ✅ with headers |
| `insert_many` | ✅ with headers, same tenant |
| `find` | ✅ only `_id` / `tenant_id` filters |
| `find_one` | ✅ only `_id` / `tenant_id` filters |
| `count` | ✅ only `_id` / `tenant_id` filters |
| `update` | ✅ exactly one document, by `_id` |
| `delete` | ✅ exactly one document, by `_id` |
| `history` | ✅ with headers; snapshots are decrypted |
| `get_version` | ✅ with headers; snapshot is decrypted |
| `set_collection_ttl` / `get_collection_ttl` | ✅ expiry metadata is plaintext |
| `search` | ❌ `unsupported_encrypted_operation` |
| `fuzzy` | ❌ `unsupported_encrypted_operation` |
| `aggregate` | ❌ `unsupported_encrypted_operation` |
| `create_index` | ❌ `unsupported_encrypted_operation` |
| `create_text_index` | ❌ `unsupported_encrypted_operation` |
| `create_fuzzy_index` | ❌ `unsupported_encrypted_operation` |
| `list_indexes` / text / fuzzy | ❌ `unsupported_encrypted_operation` |
| `batch` | ❌ vault collections cannot be read through `batch` |
| `write_batch` | ❌ vault collections cannot be written through `write_batch` |
| `list_collections` | ✅ shows collection names |

`multi` update/delete is not supported for vault collections. Update and delete
require an exact `_id`; list the documents first if you need to operate on more
than one.

## Insert

```bash
curl -s --unix-socket /tmp/nonodb.sock \
  -H 'content-type: application/json' \
  -H 'X-NonoDB-Tenant: acme' \
  -H 'X-NonoDB-Key-Id: acme:v1' \
  -H "X-NonoDB-Key: $ACME_KEY" \
  -X POST http://localhost/v1/exec \
  -d '{
    "op": "insert",
    "coll": "vault",
    "doc": {
      "tenant_id": "acme",
      "type": "invoice",
      "amount": 12000,
      "notes": "VIP customer",
      "payload": {"iban": "PL...", "ssn": "123-45-6789"}
    }
  }'
```

If `tenant_id` is omitted, the daemon injects the tenant from the header. If it is
present, it must match the header exactly. A mismatch returns `tenant_mismatch`.

## Find and count

By `_id`:

```json
{
  "op": "find_one",
  "coll": "vault",
  "filter": {"_id": "8f2c..."}
}
```

By tenant:

```json
{
  "op": "find",
  "coll": "vault",
  "filter": {"tenant_id": "acme"},
  "limit": 50
}
```

Only equality on `_id` and `tenant_id` is allowed. The daemon always injects the
authenticated tenant, so a filter cannot read another tenant's data. A filter
on any other field is rejected with `invalid_vault_filter`.

Sorting is limited to `_id` (ascending or descending) or no sort. Projection is
not supported. Listing a tenant performs a scan over the plaintext `tenant_id`
field, so use `limit` when the tenant can have many documents.

Count works without decrypting documents:

```json
{"op": "count", "coll": "vault", "filter": {"tenant_id": "acme"}}
```

## Update

```json
{
  "op": "update",
  "coll": "vault",
  "filter": {"_id": "8f2c..."},
  "update": {"$set": {"notes": "VIP, do not call after 18:00"}}
}
```

The daemon:

1. looks up the stored document by `_id`;
2. checks the plaintext `tenant_id` and decrypts the envelope;
3. applies the update to the plaintext document;
4. rejects any change to `_id` or `tenant_id`;
5. encrypts the complete updated document with a fresh nonce;
6. writes the document and its history entry in one LMDB transaction.

Upsert with an exact `_id` filter is supported:

```json
{
  "op": "update",
  "coll": "vault",
  "filter": {"_id": "8f2c..."},
  "update": {"$set": {"notes": "new"}, "$setOnInsert": {"type": "note"}},
  "upsert": true
}
```

## Delete

```json
{
  "op": "delete",
  "coll": "vault",
  "filter": {"_id": "8f2c..."}
}
```

The daemon decrypts the document before deleting it. This verifies the key and
tenant, and catches corrupted envelopes instead of deleting data that cannot be
read. The history entry is a tombstone and contains no document snapshot.

## TTL

Vault collections support collection default TTL and the reserved `_ttl` field.
The document body is encrypted as usual, while the expiry timestamp and
collection/`_id` remain in the plaintext TTL sub-database. The maintenance
sweeper deletes an expired vault document without decrypting its envelope. See
[Document TTL](ttl.md).

## History

When document history is enabled (`NONODB_HISTORY=1`), vault history stores
**ciphertext snapshots** in LMDB. The `history` and `get_version` responses
decrypt snapshots for the authenticated tenant:

```json
{
  "op": "history",
  "coll": "vault",
  "id": "8f2c...",
  "include_docs": true
}
```

The `doc` field in each revision is the decrypted document. Without the correct
key, history requests fail with `decryption_failed`.

History retention is global, as for standard collections. Key rotation must
account for old revisions that are still encrypted with an old key id.

## Multi-tenant layout

Prefer one shared vault collection with a plaintext `tenant_id`:

```text
vault
  _id: ...
  tenant_id: acme
  _enc: ...
```

Why:

- one collection serves many tenants (organizations, users, workspaces);
- `MAX_DBS` is 256, so collection-per-tenant would exhaust the limit quickly;
- `_id` is globally unique, so random UUIDs avoid collisions;
- the tenant header and AAD prevent cross-tenant reads and ciphertext moves.

Do not allow a client to choose `tenant_id` independently from the key. The header
is authoritative and the daemon validates the document against it.

## Security model and limitations

This is **daemon-side encryption**, not end-to-end encryption:

- the daemon receives the plaintext document and the raw key in memory;
- the daemon decrypts before returning documents to clients;
- a compromised daemon, root user, or process memory dump can read plaintext
  and keys;
- network protection still relies on Unix socket permissions or TLS;
- the key is not stored in LMDB, so a stolen data file without the key does not
  expose document bodies;
- `_id` and `tenant_id` remain visible in the data file.

Vault collections intentionally do not provide:

- searchable encryption;
- blind indexes;
- range queries over encrypted values;
- per-field encryption with mixed plaintext/encrypted fields;
- encrypted `_id` or tenant metadata;
- protection against an attacker who can read process memory.

If you need the daemon to see only ciphertext, you need client-side encryption
and a different, more limited search design.

## Key rotation

The envelope carries `kid`, but this MVP does not store old keys inside the
daemon. Practical rotation options:

1. **Offline re-encryption:** read with the old key, write with the new key id
   and key, then retire the old key.
2. **Keyring at the application layer:** keep old keys available while old
   revisions exist, and use the matching `kid` for each document.
3. **Disable history for vault:** this shortens the retention window for old
   ciphertext but does not remove current documents.

A wrong key id is rejected before decryption. A correct key id with the wrong
key bytes returns `decryption_failed`.

## Cache, snapshots, and backups

- Vault reads are **not** stored in the daemon read cache.
- Automatic index creation is skipped for vault collections.
- Snapshots and backups contain ciphertext plus `_id`, `tenant_id`, and `kid`.
- A backup without the tenant key is not readable.
- Back up keys separately from the database, using a secret manager and a
  documented recovery procedure.
- Losing the key means losing the document bodies. `_id` and `tenant_id` remain,
  but the encrypted payload cannot be recovered.

## Error codes

| Code | Meaning |
|---|---|
| `vault_key_required` | One or more vault headers are missing. |
| `vault_key_unexpected` | Vault headers were sent to a standard collection. |
| `invalid_vault_key` | Key is not valid base64, not 32 bytes, or context is invalid. |
| `tenant_mismatch` | Header tenant, filter tenant, or document `tenant_id` disagree. |
| `decryption_failed` | Wrong key, wrong tenant, corrupted envelope, or key id mismatch. |
| `unsupported_encrypted_operation` | Operation is not available for vault collections. |
| `invalid_vault_filter` | Filter is not exact `_id` / `tenant_id`, or multi-update/delete was requested. |
| `invalid_vault_document` | `tenant_id` is not a string or the document shape is invalid. |
| `vault_profile_immutable` | Attempted to change an existing collection's profile. |
| `invalid_collection_profile` | Unknown `profile` value. |
| `not_a_vault_collection` | Collection does not exist or is not a vault collection. |

## Python client

```python
import os
from nonodb import NonoDB

key = os.environ["ACME_VAULT_KEY"]
db = NonoDB("/run/nonodb.sock")
db.create_collection("vault", profile="vault")

vault = db.vault("acme", "acme:v1", key)
docs = vault.collection("vault")

doc_id = docs.insert({
    "tenant_id": "acme",
    "type": "invoice",
    "amount": 12000,
    "notes": "VIP customer",
})

print(docs.find_one({"_id": doc_id}))
print(docs.find({"_id": doc_id}))
docs.update({"_id": doc_id}, {"$set": {"notes": "updated"}})
docs.delete({"_id": doc_id})
```

The `VaultDB` wrapper sends the three headers on every request and bypasses the
client-side read cache. `batch` and `write_batch` raise
`unsupported_encrypted_operation` for vault handles.

## Testing and benchmarks

End-to-end tests (daemon must be running):

```bash
NONODB_SOCK=/tmp/nonodb.sock python3 scripts/vault_test.py
NONODB_SOCK=/tmp/nonodb.sock python3 scripts/vault_regression_test.py
```

The regression suite covers profile immutability, header validation, tenant
isolation, key/key-id failures, allowed filters, CRUD, upsert, history, and
rejections for search/index/aggregate/batch operations.

Performance comparison (disable the daemon cache first for fair standard
measurements):

```bash
NONODB_CACHE_TTL=0 NONODB_SOCK=/tmp/nonodb.sock \
  python3 scripts/vault_bench.py --docs 5000 --iters 1000 --payload 256
```

See [Benchmarks](benchmarks.md#vault-encryption-overhead) for representative
results and interpretation.

## Best-fit summary

Use a **vault collection** for a small, sensitive payload where you need
encryption at rest and can live with lookup by id or tenant.

Use a **standard collection** when you need search, indexes, aggregation,
sorting, or flexible filters. For mixed data, keep sensitive payloads in a
vault collection and store only non-sensitive queryable metadata in a standard
collection keyed by the same `_id`.
