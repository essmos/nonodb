# Migrating and importing data

nonodb has no native dump format and no bundled `mongoimport` command.
MongoDB's own `mongoexport`, `mongodump`, `mongoimport`, and `mongorestore`
tools can target the optional MongoDB wire listener; see
[MongoDB compatibility](mongodb-compatibility.md#export-and-portability).
Migration through the native importer is a small, explicit pipeline:

```text
source database
      │
      ▼
NDJSON files with MongoDB Extended JSON values
      │
      ▼
import manifest (version 1)
      │
      ▼
scripts/import_manifest.py
      │
      ▼
nonodb collections, documents, indexes, optional TTL
```

This keeps the source format independent from nonodb and makes the conversion
reviewable before anything is written.

The reference importer is
[`scripts/import_manifest.py`](../scripts/import_manifest.py). It uses the
normal HTTP/JSON write path, so authentication, collection profiles, indexes,
dates, decimals, and BSON scalar validation behave exactly as they do for a
normal client.

## Automated MongoDB migration

For a quiesced MongoDB source, use the reference migration helper. It reads
through PyMongo, writes through the native NonoDB HTTP/JSON client, and verifies
each collection before creating its indexes:

```bash
python3 scripts/nonodb-migrate.py \
  --mongo-uri 'mongodb://127.0.0.1:27017/app' \
  --source-db app \
  --socket /run/nonodb/nonodb.sock \
  --database app \
  --report /tmp/app-migration.json
```

Use `--url https://db.internal:7878 --token "$NONODB_TOKEN"` for TCP/TLS instead
of `--socket`. The target database is created automatically unless
`--no-create-database` is passed. `--dry-run` discovers collections, counts
documents, and validates `_id` values without writing.

What the helper does:

- discovers collections and common scalar, compound, unique, geo, and
  single-field text indexes;
- preserves `_id` strings and ObjectIds and converts BSON dates, Decimal128,
  binary values, and other supported values to Extended JSON;
- maps `expireAfterSeconds` to date-field TTL on the indexed field, backfills
  existing documents, and accepts `sparse` as a compatibility no-op for unique
  indexes;
- copies documents in bounded batches, checks source and target counts, and
  compares a SHA-256 content checksum for every collection;
- writes a JSON report with counts, checksums, translated indexes, TTL values,
  warnings, and the redacted source URI.

Useful options:

| Option | Meaning |
|---|---|
| `--include NAME` | Migrate only this collection; repeatable |
| `--exclude NAME` | Skip this collection; repeatable |
| `--batch-size N` | Maximum documents per `insert_many` (default 500) |
| `--max-bytes N` | Maximum encoded batch size (default 3,000,000) |
| `--drop` | Drop a non-empty target collection before copying |
| `--dry-run` | Discover and validate without writing |
| `--report PATH` | Write the JSON migration report |

The source must be stopped or at a consistent snapshot. This is not a live CDC
tool: writes made during the copy are not replayed. Unsupported `_id` types and
multiple TTL indexes fail with an error before the affected collection is
copied. Partial/wildcard indexes and unsupported text/geo forms are skipped with
a warning in the report. Source authentication is taken from the MongoDB URI;
never put credentials in the report or commit them.

TTL translation matches MongoDB's date-field semantics: a document expires at
the indexed date plus `expireAfterSeconds`. The helper reports both
`ttl_seconds` and `ttl_field`, and backfills TTL metadata after copying the
documents. Documents with a missing or non-date TTL field never expire.

## Important limits

| Limit | Value |
|---|---|
| Document size | 1 MiB |
| HTTP request body | 4 MiB |
| `_id` type | string or ObjectId |
| Atomicity | Per `insert_many` request; not across collections |
| Vault collections | Not supported by the generic importer |
| Unsupported BSON | regex values, Code, Symbol, DBPointer |

The importer defaults to 500 documents or 3,000,000 encoded bytes per request,
whichever comes first. Lower `--batch-size` if individual documents are large.

## Input format: NDJSON

Use one UTF-8 JSON object per line. Do not wrap documents in a JSON array and do
not add commas between lines:

```json
{"_id":"u1","name":"Ada","age":36,"created_at":{"$date":"2026-01-01T00:00:00Z"}}
{"_id":"u2","name":"Grace","balance":{"$numberDecimal":"120.50"}}
{"_id":"u3","labels":["admin","beta"],"active":true}
```

The importer accepts MongoDB Extended JSON values:

```json
{"$date":"2026-01-01T00:00:00Z"}
{"$numberLong":"9007199254740993"}
{"$numberDecimal":"199.9900"}
{"$binary":{"base64":"YQ==","subType":"00"}}
{"$uuid":"00112233-4455-6677-8899-aabbccddeeff"}
{"$oid":"000000000000000000000001"}
```

See [Dates](dates.md), [Numbers](numerics.md), and
[BSON scalar types](bson-types.md) for the exact accepted forms.

### `_id` handling

`_id` may be a string or an ObjectId. The importer currently converts common
source ObjectId forms to a stable string for HTTP imports; use the MongoDB wire
listener or the Extended JSON ObjectId form when the ID type must be preserved.

| Source `_id` | Imported `_id` |
|---|---|
| `"u1"` | `"u1"` |
| `{"$oid":"000000000000000000000001"}` | `"000000000000000000000001"` |
| `42` | `"42"` |
| `{"$uuid":"00112233-..."}` | canonical UUID text |
| `{"$binary":...}` | base64 text |

Other `_id` shapes are rejected. Convert them explicitly before import if
needed. Preserve `_id` values whenever possible; otherwise a missing `_id` gets
a generated UUID and re-importing the same file creates new documents.

## Manifest schema

The version 1 schema is
[`docs/import-manifest.schema.json`](import-manifest.schema.json). A minimal
manifest:

```json
{
  "version": 1,
  "source": {
    "kind": "mongodb",
    "database": "app",
    "exported_at": "2026-01-01T00:00:00Z"
  },
  "collections": [
    {
      "name": "users",
      "file": "users.jsonl",
      "ttl_seconds": null,
      "indexes": [
        {"fields": ["email"], "unique": true},
        {"fields": ["tenant_id", "created_at"]}
      ]
    },
    {
      "name": "orders",
      "file": "orders.jsonl",
      "indexes": [
        {"fields": ["user_id", "created_at"]},
        {"fields": ["tenant_id", "status"], "include": ["total"]}
      ]
    }
  ]
}
```

| Field | Required | Meaning |
|---|---|---|
| `version` | yes | Must be `1` |
| `source` | no | Non-secret migration metadata; not interpreted by the importer |
| `collections` | yes | Non-empty array of collection entries |
| `collections[].name` | yes | Target collection name |
| `collections[].file` | yes | NDJSON path, relative to the manifest or absolute |
| `collections[].ttl_seconds` | no | Collection TTL default for imported and future documents |
| `collections[].indexes` | no | Indexes created after the collection is imported |
| `indexes[].fields` | yes | Single or compound index fields |
| `indexes[].unique` | no | Enforce uniqueness; defaults to `false` |
| `indexes[].include` | no | Covering-index fields copied into the index |

Do not put database passwords, bearer tokens, private keys, or document
contents in `source`. Keep credentials in environment variables or the client
configuration.

Indexes are created **after** documents are loaded. This is faster for large
imports, but a unique index fails if the source contains duplicates or `null`
values that collide. Deduplicate before import, or create the unique index
first in a small validation pass.

## Run the importer

The importer and Python client live in the repository, not in the release
archive. Clone the repository or copy `scripts/import_manifest.py` and
`client/nonodb.py` together:

```bash
git clone --depth 1 https://github.com/essmos/nonodb.git
cd nonodb
pip install httpx
```

Validate first. This parses every line and checks the manifest without writing:

```bash
python3 scripts/import_manifest.py /path/to/import-manifest.json --dry-run
```

Import over the local Unix socket:

```bash
python3 scripts/import_manifest.py /path/to/import-manifest.json \
  --socket /run/nonodb/nonodb.sock \
  --batch-size 500
```

Import over TCP/TLS:

```bash
export NONODB_TOKEN='nodb_...'
python3 scripts/import_manifest.py /path/to/import-manifest.json \
  --url https://db.internal:7878 \
  --token "$NONODB_TOKEN"
```

Useful options:

| Option | Meaning |
|---|---|
| `--dry-run` | Validate manifest, files, JSON, and document sizes; write nothing |
| `--socket PATH` | Unix socket path; defaults to `NONODB_SOCK` or `/run/nonodb.sock` |
| `--url URL` | HTTP/TLS base URL instead of a Unix socket |
| `--token TOKEN` | Bearer token; defaults to `NONODB_TOKEN` |
| `--batch-size N` | Maximum documents per `insert_many` request |
| `--max-bytes N` | Maximum encoded bytes per request |
| `--skip-indexes` | Import documents without creating manifest indexes |

The importer uses `insert_many`, not `write_batch`. A failed request can
therefore leave the documents from earlier successful requests in place. For a
clean cutover, import into an empty data directory or a new collection name.
The whole migration is not a cross-collection transaction.

## Source recipes

### MongoDB

Use canonical Extended JSON to avoid losing numeric, date, binary, and
ObjectId types:

```bash
mongoexport \
  --uri 'mongodb://localhost:27017/app' \
  --collection users \
  --jsonFormat=canonical \
  --out users.jsonl

mongoexport \
  --uri 'mongodb://localhost:27017/app' \
  --collection orders \
  --jsonFormat=canonical \
  --out orders.jsonl
```

Then create the manifest. ObjectId `_id` values are converted to their 24-digit
hex string by the importer. ObjectIds in other fields stay as `$oid` values.
If an application needs to preserve an ObjectId-shaped `_id` in a specific
format, transform it before import and update the application lookup path.

Regular-expression document values,
`Code`, `Symbol`, and `DBPointer` are not supported and must be dropped or
converted to supported values first.

### PostgreSQL

`psql` can emit one JSON object per line with `-A -t` (unaligned, tuples only).
Build Extended JSON explicitly for dates and exact numerics:

```bash
psql "$DATABASE_URL" -A -t -v ON_ERROR_STOP=1 > users.jsonl <<'SQL'
SELECT json_build_object(
  '_id', id::text,
  'email', email,
  'name', name,
  'active', active,
  'created_at', json_build_object(
    '$date',
    to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
  ),
  'balance', json_build_object('$numberDecimal', balance::text)
)
FROM users;
SQL
```

Mapping advice:

- cast integer primary keys to text for `_id`;
- convert `timestamptz` to UTC Extended JSON `$date`;
- use `$numberDecimal` for `numeric` when exact decimal semantics matter;
- convert `json`/`jsonb` columns deliberately: keep them as embedded objects,
  or serialize them to a string if the application expects text;
- convert `bytea` to `$binary` with base64;
- convert SQL `NULL` to JSON `null`.

### SQLite

SQLite CLI output is not stable enough for a migration contract, so use a short
Python conversion with the standard library:

```python
import json
import sqlite3

conn = sqlite3.connect("app.db")
conn.row_factory = sqlite3.Row

with open("users.jsonl", "w", encoding="utf-8") as out:
    for row in conn.execute("SELECT id, email, created_at FROM users"):
        doc = {
            "_id": str(row["id"]),
            "email": row["email"],
            "created_at": {"$date": row["created_at"]},
        }
        out.write(json.dumps(doc, separators=(",", ":")) + "\n")
```

SQLite stores dates and booleans in application-specific ways. Convert them
explicitly; do not assume a column named `created_at` is already a BSON date.

### CSV

CSV has no types. Parse each field deliberately before writing NDJSON:

```python
import csv
import json

with open("users.csv", newline="", encoding="utf-8") as source:
    rows = csv.DictReader(source)
    with open("users.jsonl", "w", encoding="utf-8") as out:
        for row in rows:
            doc = {
                "_id": row["id"],
                "email": row["email"],
                "age": int(row["age"]) if row["age"] else None,
                "active": row["active"].lower() == "true",
                "created_at": {"$date": row["created_at"]},
            }
            out.write(json.dumps(doc, separators=(",", ":")) + "\n")
```

If a numeric value is money or another exact decimal, use
`{"$numberDecimal": "..."}` instead of a float.

## Validation and cutover

1. Export a consistent source snapshot. Stop writes or record a cutover point.
2. Convert to NDJSON. Keep the source `_id` whenever possible.
3. Write the manifest and run `--dry-run`.
4. Import into an empty nonodb data directory or a new collection name.
5. Compare counts and spot-check documents, dates, decimals, binary values, and
   nulls:
   ```bash
   NONODB_SOCK=/run/nonodb/nonodb.sock nonodb-cli count users '{}'
   NONODB_SOCK=/run/nonodb/nonodb.sock nonodb-cli find users '{}' --limit 5
   NONODB_SOCK=/run/nonodb/nonodb.sock nonodb-cli indexes users
   ```
6. Create or verify unique indexes before switching traffic.
7. Run the application test suite against the migrated database.
8. Back up the source and the new nonodb data directory before cutover.
9. Switch clients, then remove the old source only after the rollback window.

For a low-downtime migration, use a snapshot plus a short write freeze. There is
no change-stream or logical-replication feed in nonodb. If the source must keep
writing during the bulk copy, add an application-level dual-write or a
change-capture mechanism outside nonodb, then replay the delta before cutover.

## Vault collections

The generic importer does not send vault headers and does not write encrypted
collections. For vault collections, use `VaultDB` from the Python client and
`insert_many` with the tenant, key ID, and key headers. See
[Vault collections](vault.md). Vault `_id` and `tenant_id` rules still apply,
and `batch`/`write_batch` are intentionally unsupported for vault collections.
