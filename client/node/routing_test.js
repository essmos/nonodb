'use strict';

const assert = require('node:assert/strict');
const http = require('node:http');
const { NonoDB, NonoDBError } = require('./index');

function start(role, primaryUrl) {
  const state = { role, primaryUrl, posts: 0, seen: [] };
  const server = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8');
      const payload = raw ? JSON.parse(raw) : {};
      const send = (status, body, location) => {
        const data = Buffer.from(JSON.stringify(body));
        res.writeHead(status, {
          'content-type': 'application/json',
          'content-length': String(data.length),
          ...(location ? { location } : {}),
        });
        res.end(data);
      };
      if (req.method === 'GET' && req.url === '/readyz') {
        send(200, { ok: true, replication: { write_url: primaryUrl } });
        return;
      }
      if (!req.url.endsWith('/v1/exec')) {
        send(404, { ok: false, error: { code: 'not_found', message: '' } });
        return;
      }
      if (role === 'replica' && !['find', 'search_all'].includes(payload.op)) {
        state.posts += 1;
        send(403, {
          ok: false,
          error: { code: 'read_only', message: 'replica', write_url: primaryUrl },
        }, primaryUrl);
        return;
      }
      state.posts += 1;
      state.seen.push(payload);
      const result = payload.op === 'find'
        ? []
        : payload.op === 'search_all'
          ? { results: [{ _id: 'doc-1' }] }
          : { inserted_id: 'doc-1' };
      send(200, { ok: true, result });
    });
  });
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      state.url = `http://127.0.0.1:${server.address().port}`;
      state.server = server;
      resolve(state);
    });
  });
}

async function main() {
  const primary = await start('primary', '');
  const replica = await start('replica', primary.url);
  primary.primaryUrl = primary.url;

  const db = new NonoDB({
    baseUrl: replica.url,
    writeUrl: primary.url,
    token: 'test',
  });
  await db.exec({ op: 'insert', coll: 'c', doc: { x: 1 } });
  assert.equal(primary.posts, 1);
  assert.equal(replica.posts, 0);
  await db.exec({ op: 'find', coll: 'c' });
  assert.equal(replica.posts, 1);
  const found = await db.collection('c').searchAll({ query: 'coffee' });
  assert.equal(found.length, 1);
  assert.equal(replica.posts, 2);
  assert.equal(primary.posts, 1);

  primary.posts = 0;
  replica.posts = 0;
  const auto = new NonoDB({ baseUrl: replica.url, autoPrimary: true, token: 'test' });
  await auto.exec({ op: 'insert', coll: 'c', doc: { x: 2 } });
  assert.equal(primary.posts, 1);
  assert.equal(replica.posts, 0);

  primary.posts = 0;
  replica.posts = 0;
  const stale = new NonoDB({ baseUrl: replica.url, writeUrl: replica.url, token: 'test' });
  await stale.exec({ op: 'insert', coll: 'c', doc: { x: 3 } });
  assert.equal(replica.posts, 1);
  assert.equal(primary.posts, 1);

  primary.posts = 0;
  replica.posts = 0;
  await db.exec({
    op: 'aggregate',
    coll: 'c',
    pipeline: [{ $out: 'd' }],
  });
  assert.equal(primary.posts, 1);
  assert.equal(replica.posts, 0);

  primary.posts = 0;
  replica.posts = 0;
  const plain = new NonoDB({ baseUrl: replica.url, token: 'test' });
  await assert.rejects(
    plain.exec({ op: 'insert', coll: 'c', doc: { x: 4 } }),
    (error) => error instanceof NonoDBError &&
      error.code === 'read_only' &&
      error.writeUrl === primary.url,
  );
  assert.equal(replica.posts, 1);
  assert.equal(primary.posts, 0);

  db.close();
  auto.close();
  stale.close();
  plain.close();
  primary.server.close();
  replica.server.close();
  console.log('OK: node write routing');
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
