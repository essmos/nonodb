# Dates and time zones

nonodb implements dates as a first-class, BSON-compatible value rather than as
ordinary strings. The model follows MongoDB:

- a date is an **instant**, stored as signed 64-bit Unix milliseconds in UTC;
- no timezone is stored with the value;
- RFC 3339 offsets are normalized when the document is written;
- comparison, sorting, and indexes use the instant, not the original text.

Dates use [MongoDB Extended JSON](https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/)
syntax. This keeps HTTP requests, documents, and backups plain JSON.

## Input forms

Relaxed Extended JSON:

```json
{"created_at": {"$date": "2026-01-01T01:00:00+01:00"}}
```

Canonical Extended JSON:

```json
{"created_at": {"$date": {"$numberLong": "1767225600000"}}}
```

Numeric convenience form, also interpreted as Unix milliseconds:

```json
{"created_at": {"$date": 1767225600000}}
```

Strings must be RFC 3339 and must include `Z` or a numeric offset. Naive local
times are rejected with `invalid_date` because their instant is ambiguous:

```json
{"created_at": {"$date": "2026-01-01T00:00:00"}}
```

Date-only strings such as `"2026-01-01"` are also rejected. Store an explicit
instant (`2026-01-01T00:00:00Z`) if that is what you mean.

All accepted forms are normalized to canonical UTC milliseconds before storage:

```json
{"created_at": {"$date": {"$numberLong": "1767225600000"}}}
```

## Filtering

Dates use the normal comparison operators. There are no separate `$dateGt` or
similar operators:

```json
{
  "op": "find",
  "coll": "events",
  "filter": {
    "created_at": {
      "$gte": {"$date": "2026-01-01T00:00:00Z"},
      "$lt": {"$date": "2026-02-01T00:00:00Z"}
    }
  }
}
```

Because comparison is by instant, these are equal:

```text
2026-01-01T00:00:00Z
2026-01-01T01:00:00+01:00
2026-01-01T02:00:00+02:00
```

`$eq`, `$ne`, `$in`, and `$nin` also compare dates by instant. Date values and
strings are different types: a date query does not match an ISO string field,
and a string query does not match a date field.

## Indexes and sorting

Secondary indexes encode dates as order-preserving signed integers. A normal
index on the field is enough; no type annotation is required:

```json
{"op": "create_index", "coll": "events", "fields": ["tenant", "created_at"]}
```

This supports:

- range filters on `created_at` after an equality on `tenant`;
- chronological sorting by `created_at`;
- unique indexes that treat different offsets for the same instant as duplicates;
- covering projections that include date fields.

Indexes and filters work with negative timestamps (dates before 1970).

## Updates

Dates can be set or replaced with normal update operators:

```json
{
  "op": "update",
  "coll": "events",
  "filter": {"_id": "e1"},
  "update": {"$set": {"created_at": {"$date": "2026-01-01T00:00:00Z"}}}
}
```

`$currentDate` is not implemented. Write an explicit date, or generate the
current timestamp in the application.

## Time zones and civil dates

Timezone identifiers such as `Europe/Warsaw` are not part of the stored value.
Convert local times to an RFC 3339 instant in the client. This is also how the
MongoDB BSON Date type works.

Timezone-aware aggregation expressions (`$dateToString`, `$dateFromParts`,
`$dateTrunc`, `$dateAdd`, `$dateSubtract`, `$dateDiff`, and `$dateFromString`)
are not implemented yet. If you need "a local calendar day" rather than an
instant, store the intended local date or day key as a separate string field.

## Existing documents and imports

Reads are non-destructive: nonodb does not scan or rewrite stored values when
loading a document.

- A stored `{"$date": ...}` object is recognized as a date by filters,
  sorting, aggregation, and indexes, even if it was stored before this feature
  and is still in relaxed form. It is canonicalized on the next write.
- An ISO 8601 **string** remains a string. It does not match a date query and
  is not indexed as a date:

  ```json
  {"created_at": "2026-01-01T00:00:00Z"}
  ```

- Bulk imports through `insert_many` and document batches follow the same
  rules as `insert`: Extended JSON dates are normalized on write.
- Invalid stored `$date` objects are not silently treated as dates. They do not
  match date filters and are rejected if the document is written again.

### Migrating existing string dates

There is no automatic string-to-date migration, because guessing which string
fields are timestamps is unsafe. Convert selected fields explicitly, for
example by reading matching documents and updating each field to Extended JSON.
Use `{"$date": "..."}` with `Z` or an offset; a write will normalize it to
canonical UTC milliseconds.

After migration, update any index that should serve date queries.

### Indexes created before date support

Index definitions now carry an encoding format. Indexes created before date
support have format 0; they skipped `$date` objects during backfill. To avoid
returning incomplete results, nonodb skips such an index when the filter
contains a date and falls back to a scan.

Recreate the index to enable indexed date ranges, sorting, and `count`:

```json
{"op": "drop_index", "coll": "events", "field": "created_at"}
{"op": "create_index", "coll": "events", "fields": ["created_at"]}
```

Normal non-date queries can continue using the old index until it is recreated.

## Other compatibility rules

- New writes using `{"$date": ...}` are normalized and indexed as dates.
- Documents containing an object with exactly one `$date` key are reserved for
  this representation. Do not use `$date` as an ordinary application field.
- Date precision is milliseconds, matching BSON Date.
- Backups and snapshots preserve canonical dates without conversion.

## Errors

| Code | Meaning |
|---|---|
| `invalid_date` | Invalid `$date` payload, invalid RFC 3339 string, or missing timezone |
