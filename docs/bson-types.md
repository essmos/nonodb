# BSON scalar types

nonodb stores documents as JSON but accepts MongoDB Extended JSON for scalar
types that JSON cannot represent. This page covers integer forms, `$type`,
binary/UUID, and ObjectId. Dates and numeric/Decimal128 semantics are documented
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
| `long` | 18 | `$numberLong` or larger JSON integer |
| `decimal` | 19 | `$numberDecimal` |

`number` is an alias for all numeric types. A field containing an array matches
when the array itself has the requested type or any element does.

`$type` is evaluated by the residual matcher and does not use a secondary index
directly.

Full-text and fuzzy indexes index the textual form of `$numberInt`,
`$numberLong`, `$numberDouble`, and `$numberDecimal` values in addition to
strings and booleans. Dates, binary/UUID, and ObjectId values are not searchable
text.

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

ObjectId is currently supported as a field **value**. The document `_id` must
remain a string. Changing `_id` to accept ObjectId would be a separate contract
change.

## Index compatibility

Binary and ObjectId keys were added in index format version 4. Indexes created
with an older format are skipped for filters containing binary, ObjectId,
numbers, or dates; the query falls back to a scan until the index is recreated.

## Errors

| Code | Meaning |
|---|---|
| `invalid_numeric` | Invalid `$numberInt`, `$numberLong`, `$numberDouble`, `$numberDecimal`, or arithmetic overflow |
| `invalid_binary` | Invalid `$binary`/`$uuid` payload or base64 |
| `invalid_object_id` | `$oid` is not 24 hexadecimal characters |

## Not implemented

The following BSON types are deliberately unsupported:

- `Timestamp`,
- `MinKey` / `MaxKey`,
- regular-expression values,
- `Code`, `CodeWithScope`, `Symbol`, `DBPointer`, `Undefined`.

Timestamp and MinKey/MaxKey are candidates for a later compatibility pass.
Stored-code types are not planned because nonodb does not execute document
content.
