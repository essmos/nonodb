# Numbers and decimals

nonodb treats all numeric values as one MongoDB-compatible numeric type
bracket. JSON integers, JSON floating-point numbers, BSON Decimal128 values,
and the special IEEE-754 values `NaN`/`Infinity` compare with each other by
numeric value. `1`, `1.0`, and `NumberDecimal("1.00")` are equal and share the
same index key.

## JSON numbers

Ordinary JSON numbers are stored unchanged. Comparison and indexing are exact:
integers are not rounded through `f64`, so values above `2^53` remain distinct.

```json
{"_id": "a", "n": 9007199254740993}
```

Floating-point values use 17 significant decimal digits internally for
comparison and index ordering. That is enough to round-trip every finite
`f64` and preserves ordering. JSON itself cannot represent `NaN` or `Infinity`.

Canonical integer forms are also accepted and preserve their type for `$type`:

```json
{"$numberInt": "42"}
{"$numberLong": "9007199254740993"}
```

They still compare and index in the same numeric bracket as plain JSON numbers.
See [BSON scalar types](bson-types.md).

## Decimal128

Use MongoDB Extended JSON for decimal values:

```json
{"_id": "p1", "price": {"$numberDecimal": "199.9900"}}
```

- Accepted payload: a decimal string that fits BSON Decimal128.
- Stored form: `{"$numberDecimal": "<canonical string>"}`.
- Precision: up to 34 significant decimal digits, exponent range from
  Decimal128.
- Only finite decimals are accepted. `NaN`, `Infinity`, and `-Infinity` are
  rejected with `invalid_numeric`.
- Decimal strings are not parsed from ordinary JSON strings. A value like
  `"199.99"` remains a string.

Standard query operators work without any decimal-specific syntax, and
`$type` can distinguish `int`, `long`, `double`, and `decimal`:

```json
{
  "op": "find",
  "coll": "products",
  "filter": {
    "price": {
      "$gte": {"$numberDecimal": "10.00"},
      "$lt": {"$numberDecimal": "100.00"}
    }
  }
}
```

Decimal values participate in range scans, sorting, compound indexes, unique
indexes, count fast paths, and automatic-index candidates.

### Arithmetic

When a decimal is involved, `$inc`, `$sum`, `$avg`, `$add`, `$subtract`,
`$multiply`, and `$divide` use exact decimal arithmetic and return a
`$numberDecimal` value. Results are rounded to 34 significant digits with
banker's rounding and must fit Decimal128. A result outside the Decimal128
range fails with `invalid_numeric`.

```json
{
  "op": "update",
  "coll": "accounts",
  "filter": {"_id": "a"},
  "update": {"$inc": {"balance": {"$numberDecimal": "0.20"}}}
}
```

If neither operand is a decimal, arithmetic keeps the normal JSON number
behavior.

## NaN and Infinity

Use `$numberDouble` when a JSON number cannot express the value:

```json
{"_id": "nan", "n": {"$numberDouble": "NaN"}}
{"_id": "inf", "n": {"$numberDouble": "Infinity"}}
```

Accepted strings are `NaN`, `Infinity`, `-Infinity`, and finite decimal
strings. They are stored in canonical Extended JSON form.

Sorting and index order follow this numeric order:

```text
NaN < -Infinity < negative finite < 0 < positive finite < Infinity
```

`NaN` equals only `NaN`. These values are stored, filtered, and indexed, but
`$sum`/`$avg` ignore non-finite values and decimal arithmetic requires finite
operands.

## Index compatibility

Numeric index keys use an exact, order-preserving decimal encoding. Index
format version 3 introduced exact numeric keys; version 4 added binary/UUID
and ObjectId keys. Older indexes are skipped for filters containing numbers,
dates, binary, or ObjectId values; queries fall back to a scan until the index
is dropped and recreated. String/boolean filters can still use those older
indexes.

Decimal keys use the same numeric type tag as other numbers. There is no
separate decimal index type or collation.

## Long text

Long text needs no separate scalar type. Strings are stored as JSON strings,
and the practical limits are:

| Limit | Value |
|---|---|
| Document size | 1 MiB |
| Request body | 4 MiB |

For text larger than the document limit, split it across multiple documents or
fields and reassemble it in the application. Text and fuzzy indexes are
designed for searchable fields; storing multi-megabyte strings in an indexed
field increases index size and write cost. See [Limits](api.md#limits).
