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
});
```

See [docs/clients.md](../../docs/clients.md) for the full client guide. Run
`NONODB_SOCK=/run/nonodb.sock node smoke_test.js` against a daemon for an
end-to-end check.

## Document history

When the daemon is started with `NONODB_HISTORY=1`, use `history()` and
`getVersion()` on a collection.
