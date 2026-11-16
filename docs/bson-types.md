# BSON scalar types

nonodb stores documents as JSON but accepts MongoDB Extended JSON for scalar
types that JSON cannot represent. This page covers integer forms, `$type`,
binary/UUID, ObjectId, Timestamp, and MinKey/MaxKey. Dates and numeric/Decimal128 semantics are documented
in [Dates and time zones](dates.md) and [Numbers and decimals](numerics.md).

## Extended JSON integer forms

Integer type information can be preserved explicitly:

```json
{"$numberInt": "42"}
{"$numberLong": "9007199254740993"}
```

Accepted payloads are decimal strings in the signed 32-bit / 64-bit range.
Values are stored in canonical form, participate in the same numeric bracket as
other numbers, and work with normal comparison operators and indexes. `$inc`
preserves an explicit `$numberInt`/`$numberLong` result type.

`$numberDouble` accepts finite doubles and the special values `NaN`,
`Infinity`, and `-Infinity`. See [Numbers and decimals](numerics.md).

## `$type`

The query operator `$type` matches BSON scalar types:

```json
{"price": {"$type": "decimal"}}
{"at": {"$type": "date"}}
{"payload": {"$type": ["binData", "string"]}}
{"v": {"$type": 16}}
```

Supported names and numeric codes:

| Name | Code | Matches |
|---|---:|---|
| `double` | 1 | `$numberDouble` or JSON floating-point number |
| `string` | 2 | string |
| `object` | 3 | ordinary object |
| `array` | 4 | array |
| `binData` | 5 | binary/UUID |
| `objectId` | 7 | ObjectId |
| `bool` | 8 | boolean |
| `date` | 9 | Extended JSON date |
| `null` | 10 | null |
| `int` | 16 | JSON integer in int32 range or `$numberInt` |
| `timestamp` | 17 | Extended JSON Timestamp |
| `long` | 18 | `$numberLong` or larger JSON integer |
| `decimal` | 19 | `$numberDecimal` |
| `minKey` | -1 | MinKey |
| `maxKey` | 127 | MaxKey |

`number` is an alias for all numeric types. A field containing an array matches
when the array itself has the requested type or any element does.

`$type` is verified by the residual matcher. When a secondary index exists, a
type-only condition can use its BSON type bracket; otherwise the query scans.

Full-text and fuzzy indexes index the textual form of `$numberInt`,
`$numberLong`, `$numberDouble`, and `$numberDecimal` values in addition to
strings and booleans. Dates, binary/UUID, ObjectId, Timestamp, MinKey, and
MaxKey values are not searchable text.

## Binary and UUID

Canonical binary form:

```json
{"$binary": {"base64": "YQ==", "subType": "00"}}
```

UUID shorthand is accepted and normalized to binary subtype `04`:

```json
{"$uuid": "00112233-4455-6677-8899-aabbccddeeff"}
```

After storage the UUID example becomes:

```json
{"$binary": {"base64": "ABEiM0RVZneImaq7zN3u/w==", "subType": "04"}}
```

Binary values support equality, ranges, sorting, unique indexes, compound
indexes, count, and `{ "$type": "binData" }`. Index order is:

1. subtype,
2. byte length,
3. byte contents.

Base64 is larger than the raw bytes; the normal 1 MiB document limit still
applies after JSON encoding.

## ObjectId

```json
{"$oid": "000000000000000000000001"}
```

The payload must be exactly 24 hexadecimal characters. ObjectIds are stored in
lowercase canonical form, compare and index by their 12 bytes, and support
`{ "$type": "objectId" }`.

ObjectId is supported as both a field value and a document `_id`. String IDs are
unchanged. ObjectIds use a reserved internal key prefix so they remain valid
UTF-8 LMDB keys and sort after strings, matching BSON's type order.

## Timestamp

```json
{"$timestamp": {"t": 1700000000, "i": 7}}
```

Both fields must be unsigned 32-bit integers. `t` is seconds since the Unix
epoch and `i` is an increment. Timestamps compare and index by `t`, then `i`.
Queries can use `{ "$type": "timestamp" }` or code `17`.

`$currentDate` can create one:

```json
{"$currentDate": {"updated_at": {"$type": "timestamp"}}}
```

The daemon currently writes the current second with increment `1`; it does not
provide MongoDB's per-process increment guarantees.

## MinKey and MaxKey

```json
{"$minKey": 1}
{"$maxKey": 1}
```

These sentinel values sort before and after every other BSON value. They can be
stored, queried, compared, and indexed, and are matched by
`{ "$type": "minKey" }` / `{ "$type": "maxKey" }` or codes `-1` / `127`.

## Index compatibility

Binary and ObjectId keys were added in index format version 4. Indexes created
with an older format are skipped for filters containing binary, ObjectId,
numbers, dates, Timestamps, MinKey, or MaxKey; the query falls back to a scan
until the index is recreated.

## Errors

| Code | Meaning |
|---|---|
| `invalid_numeric` | Invalid `$numberInt`, `$numberLong`, `$numberDouble`, `$numberDecimal`, or arithmetic overflow |
| `invalid_binary` | Invalid `$binary`/`$uuid` payload or base64 |
| `invalid_object_id` | `$oid` is not 24 hexadecimal characters |
| `invalid_timestamp` | `$timestamp` does not contain valid u32 `t` and `i` fields |

## Not implemented

The following BSON types are deliberately unsupported:

- regular-expression values. The MongoDB wire listener rejects BSON regex values
  on writes and filters with `BadValue` (code 2); a BSON regex is accepted only
  as the argument of the `$regex` operator,
- `Code`, `CodeWithScope`, `Symbol`, `DBPointer`, `Undefined`.

Stored-code types are not planned because nonodb does not execute document
content.
