# SQL queries

NonoDB supports a **read-only SQL `SELECT`** over one collection. SQL is a
translation layer on top of the normal read path, not a second storage engine:
it uses the same filters, indexes, TTL handling, LMDB snapshots, aggregation
limits, authorization, and response encoding as the JSON API.

Use SQL when a reporting job, BI tool, or ad-hoc operator already speaks SQL.
Use the JSON API for writes, aggregation stages that SQL does not expose, and
features such as full-text, fuzzy, geo, and vector search.

## Request

```json
{
  "op": "sql",
  "query": "SELECT name, age FROM users WHERE age >= $1 ORDER BY age DESC LIMIT 10",
  "params": [18]
}
```

Response:

```json
{
  "ok": true,
  "result": {
    "results": [
      {"name": "Ada", "age": 36},
      {"name": "Grace", "age": 31}
    ]
  }
}
```

`params` is optional and defaults to an empty array. Positional placeholders can
be `?` or `$1`-style; `$1` is the first parameter. Do not mix the two styles in
one statement.

## Supported syntax

```sql
SELECT [DISTINCT] select_list
FROM collection [AS alias]
[WHERE condition]
[GROUP BY field [, field ...]]
[HAVING condition]
[ORDER BY field [ASC | DESC] [, ...]]
[LIMIT count [OFFSET offset]]
[OFFSET offset]
```

The `select_list` is either `*` or a comma-separated list of:

- a field path such as `name`, `address.city`, or `u.name`;
- an aggregate function: `COUNT(*)`, `COUNT(field)`, `SUM(field)`,
  `AVG(field)`, `MIN(field)`, `MAX(field)`;
- a scalar expression (see below);
- an optional alias: `name AS person`, `COUNT(*) AS n`, `age + 1 AS next_age`.

The `FROM` table is a collection name. A table alias is optional and is
stripped from qualified field names (`u.name` becomes `name`).

### Conditions

```sql
age = 18
age != 18
age <> 18
age < 18
age <= 18
age > 18
age >= 18
age BETWEEN 18 AND 30
age NOT BETWEEN 18 AND 30
city IN ('London', 'New York')
city NOT IN ('London')
deleted_at IS NULL
deleted_at IS NOT NULL
name LIKE 'A%'
name NOT LIKE '%test%'
```

Conditions combine with `AND`, `OR`, `NOT`, and parentheses. Precedence is
`NOT`, then `AND`, then `OR`.

`LIKE` is case-sensitive. `%` matches any sequence and `_` matches one
character; the pattern is anchored to the whole value. Other regex
metacharacters are escaped, so `LIKE 'a.b'` matches the literal dot. Use the
JSON API's `$regex` for advanced regular expressions.

### Scalar expressions

Expressions can be used in `SELECT` and inside `CASE`:

```sql
age + 1
score * 2
price / 100
name || '!'
CASE WHEN age >= 65 THEN 'senior'
     WHEN age >= 18 THEN 'adult'
     ELSE 'minor' END
```

Supported operators:

- `+`, `-`, `*`, `/`;
- string concatenation with `||`;
- comparisons `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`;
- boolean `AND`, `OR`, `NOT`;
- `IN`, `NOT IN`, `BETWEEN`, `NOT BETWEEN`, `IS NULL`, `IS NOT NULL`
  in conditional expressions.

Supported functions:

| Function | Notes |
|---|---|
| `LOWER(s)`, `UPPER(s)` | Unicode case conversion |
| `LENGTH(s)` | Character count; arrays return their length |
| `SUBSTR(s, start, length)` / `SUBSTRING` | Zero-based character offsets |
| `TRIM(s)`, `LTRIM(s)`, `RTRIM(s)` | Whitespace trimming |
| `COALESCE(a, b, ...)` | First non-null value |
| `NULLIF(a, b)` | `NULL` when `a = b`, otherwise `a` |
| `ABS(n)`, `CEIL(n)` / `CEILING(n)`, `FLOOR(n)`, `ROUND(n [, digits])` | Numeric helpers |

Expressions without an alias use generated output names (`expr1`, `expr2`, ...).
Use `AS` for stable column names.

### Grouping and aggregates

```sql
SELECT team, COUNT(*) AS n, SUM(score) AS total
FROM users
WHERE active = TRUE
GROUP BY team
HAVING SUM(score) > 100
ORDER BY total DESC
LIMIT 20
```

Rules:

- An aggregate function without `GROUP BY` returns one row.
- Every non-aggregate selected field must appear in `GROUP BY`.
- `SELECT *` cannot be combined with `GROUP BY`, aggregate functions, or
  `DISTINCT`.
- `ORDER BY` in a grouped query must reference a selected output column. A
  `GROUP BY` field counts only when it is selected (possibly under an alias).
- Aggregate output columns use the alias when provided. Otherwise generated
  names are `count`, `sum_<field>`, `avg_<field>`, `min_<field>`, and
  `max_<field>`; duplicates get a numeric suffix.
- `HAVING` can reference selected columns, selected aggregate aliases, or
  aggregate calls. An unselected aggregate used in `HAVING` is computed as a
  hidden column and removed before returning results.
- `HAVING` comparisons must compare a column/aggregate with a literal or
  parameter. Comparing two aggregates directly is not supported yet.

### DISTINCT

```sql
SELECT DISTINCT team FROM users ORDER BY team;
SELECT DISTINCT LOWER(city) AS city FROM users WHERE city IS NOT NULL;
```

`DISTINCT` is applied before `ORDER BY`, `OFFSET`, and `LIMIT`. It requires an
explicit select list; `SELECT DISTINCT *` and `DISTINCT` with aggregates are not
supported yet.

### Projection semantics

- `SELECT *` returns full documents, including `_id`.
- An explicit select list returns only the selected fields.
- `_id` is excluded unless it is selected explicitly.
- Nested fields are selected by their dotted path. Output keys use the selected
  name or alias; for example, `SELECT address.city AS city` returns
  `{"city": "..."}`.
- A field reference that does not exist is omitted from the output object,
  matching JSON document projection behavior.

## EXPLAIN

Use `explain_sql` to inspect the selected plan without changing the query:

```json
{
  "op": "explain_sql",
  "query": "SELECT name FROM users WHERE age >= $1 LIMIT 10",
  "params": [18]
}
```

Simple field-only selects delegate to the normal `find` planner and return the
same `access_path`, `index`, `bounds`, `candidates`, and actual counts as
`explain`. Grouped, distinct, and computed queries return a SQL pipeline plan:

```json
{
  "collection": "users",
  "access_path": "aggregation_group",
  "distinct": false,
  "grouped": true,
  "has_having": true,
  "has_filter": false,
  "pipeline": [
    {"$group": {"_id": "$team", "n": {"$count": {}}}},
    {"$project": {"_id": 0, "team": "$_id", "n": "$n"}},
    {"$match": {"n": {"$gt": 1}}}
  ]
}
```

## Execution and limits

- Simple field-only `SELECT` statements use `find` directly. They are **not**
  subject to `NONODB_AGGREGATE_MAX_DOCS` and benefit from normal early exit.
- `DISTINCT`, grouping, computed columns, and `HAVING` use the aggregation
  engine. The `NONODB_AGGREGATE_MAX_DOCS` limit applies to intermediate
  aggregation documents; increase it if a large report needs more rows.
- A `WHERE` clause becomes a leading aggregation `$match`, so secondary
  indexes, `_id` ranges, and index-backed sorting can be used by the normal
  planner.
- SQL is **read-only**. `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, and
  transaction statements are rejected with `invalid_sql`.
- One statement reads one collection. Joins and subqueries are not supported.
- Scalar expressions in a grouped `SELECT` are not supported yet. Use a
  selected `GROUP BY` field or an aggregate alias.
- Ordering a grouped query by a `GROUP BY` field that is not selected is not
  supported yet.
- Results are returned as an array of JSON objects. The same request limits and
  timeouts as other read operations apply.
- A missing collection returns an empty `results` array, consistent with
  `aggregate`.
- SQL and `explain_sql` are available in read `batch` operations. They are
  cacheable in clients that enable the read cache.

## Authorization

The collection name is parsed from the statement before execution. A principal
must have `read:<collection>` (or a matching wildcard) for that collection.
Parse errors return `invalid_sql` with HTTP 400.

## Client examples

Python:

```python
rows = db.sql(
    "SELECT DISTINCT team FROM users WHERE age >= $1 ORDER BY team",
    [18],
)
plan = db.explain_sql(
    "SELECT team, COUNT(*) AS n FROM users GROUP BY team HAVING COUNT(*) > 1"
)
```

Rust:

```rust
let rows = db.sql(
    "SELECT team, COUNT(*) AS n FROM users GROUP BY team HAVING COUNT(*) > $1",
    &[json!(1)],
)?;
let plan = db.explain_sql(
    "SELECT * FROM users WHERE age >= $1",
    &[json!(18)],
)?;
```

Node.js:

```js
const rows = await db.sql(
  'SELECT CASE WHEN age >= 18 THEN \'adult\' ELSE \'minor\' END AS band FROM users',
);
const plan = await db.explainSql('SELECT * FROM users WHERE age >= $1', [18]);
```

Go:

```go
rows, err := db.SQL(ctx,
    "SELECT team, COUNT(*) AS n FROM users GROUP BY team HAVING COUNT(*) > $1",
    1,
)
plan, err := db.ExplainSQL(ctx, "SELECT * FROM users WHERE age >= $1", 18)
```

## Raw HTTP

```bash
curl -s --unix-socket /run/nonodb.sock \
  -H 'content-type: application/json' \
  -X POST http://localhost/v1/exec \
  -d '{"op":"sql","query":"SELECT name, age FROM users WHERE age >= ? ORDER BY age DESC","params":[18]}'
```

## Error codes

SQL parse and planning failures return `invalid_sql` (HTTP 400), for example:

```json
{
  "ok": false,
  "error": {
    "code": "invalid_sql",
    "message": "invalid SQL: expected keyword `SELECT`"
  }
}
```

Missing parameters, unsupported clauses, malformed values, and invalid
`GROUP BY`/`HAVING`/`ORDER BY` references all use this code.
