# nonodb-client (Node.js)

Zero-dependency async client for [`nonodb`](../../README.md). Requires Node 18+.

```bash
npm install /path/to/nonodb/client/node
```

```js
const { NonoDB } = require('nonodb-client');

const db = new NonoDB(); // NONODB_SOCK on Unix, NONODB_URL on Windows
const users = db.collection('users');

const id = await users.insert({ name: 'Ada', age: 36 });
const adults = await users.find(
  { age: { $gte: 18 } },
  { sort: [['age', -1]], limit: 10 },
);
console.log(id, adults);
db.close();
```

TCP/TLS:

```js
const db = new NonoDB({
  baseUrl: 'https://127.0.0.1:7878',
  token: 'secret',
  verify: true,
  caCert: '/etc/nonodb/ca.crt',
  clientCert: '/etc/nonodb/client.crt',
  clientKey: '/etc/nonodb/client.key',
});
```

The same values are read from `NONODB_TLS_CA`, `NONODB_TLS_CLIENT_CERT`, and
`NONODB_TLS_CLIENT_KEY`.

See [docs/clients.md](../../docs/clients.md) for the full client guide. Run
`NONODB_SOCK=/run/nonodb.sock node smoke_test.js` against a daemon for an
end-to-end check.

## Document history

When the daemon is started with `NONODB_HISTORY=1`, use `history()` and
`getVersion()` on a collection.

## Document TTL

```js
await db.createCollection('sessions', 'standard', 86400);
await db.setCollectionTtl('sessions', 3600);
const ttl = await db.collectionTtl('sessions');
```

Documents use the reserved `_ttl` field for a per-document override. See
[Document TTL](../../docs/ttl.md).

## Aggregation

Use `aggregate()` for a read-only reporting pipeline:

```js
const rows = await users.aggregate([
  { $match: { active: true } },
  { $group: { _id: '$tenant', total: { $sum: '$amount' } } },
  { $sort: { total: -1 } },
  { $limit: 10 },
]);
```

## Batch lookups

```js
await articles.createIndex(['slug'], {
  unique: true,
  include: ['title', 'status'],
});

const documents = await articles.lookup(
  'slug',
  ['async-rust', 'missing', 'lmdb'],
  { fields: ['title', 'status'] },
);
const article = await articles.lookupOne('slug', 'async-rust', {
  projection: { title: 1, _id: 0 },
});
const byId = await articles.getMany(['a', 'b', 'missing']);

await articles.createIndex(['tenant_id', 'slug'], { unique: true });
const rows = await articles.lookup(
  ['tenant_id', 'slug'],
  [['acme', 'async-rust'], ['other', 'async-rust'], ['acme', 'missing']],
);
```

Results preserve input order and use `null` for missing keys. A non-`_id` lookup
requires a unique index with matching field order; compound values are arrays.
`projection` cannot be combined with `fields`, and the result budget is capped
at 64 MiB. See [Batch lookups](../../docs/clients.md#batch-lookups).

## Vector search

```js
const articles = db.collection('articles');
const queryVector = [0.12, -0.03, 0.44];

await articles.createVectorIndex('embedding', 3, 'cosine');
await articles.insert({ title: 'Async Rust', embedding: queryVector });

const hits = await articles.vectorSearch('embedding', queryVector, {
  filter: { published: true },
  skip: 0,
  limit: 10,
});
const indexes = await articles.listVectorIndexes();
await articles.dropVectorIndex('embedding');
```

`_distance` is lower-is-better. Search is exact and scans the index. Missing or
wrong-length vectors are skipped. See [Vector search](../../docs/clients.md#vector-search)
for common semantics and the other clients.

## Vault collections

```js
const vault = db.vault('acme', 'acme:v1', process.env.ACME_VAULT_KEY);
const docs = vault.collection('vault');
```

See [Vault collections](../../docs/vault.md).
