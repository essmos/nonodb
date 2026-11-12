'use strict';

const assert = require('node:assert/strict');
const { NonoDB } = require('./index');

async function main() {
  const db = new NonoDB();
  assert.equal(await db.health(), true, 'daemon is not healthy');

  const name = `node_client_test_${Date.now()}`;
  const users = db.collection(name);

  const id = await users.insert({ name: 'Ada', age: 36, tags: ['db', 'node'] });
  assert.equal(typeof id, 'string');

  const all = await users.insertMany([
    { name: 'Grace', age: 41 },
    { name: 'Linus', age: 55 },
  ]);
  assert.equal(all.length, 2);
  assert.equal(await users.count({}), 3);

  const grouped = await users.aggregate([
    { $match: { age: { $gte: 40 } } },
    { $group: { _id: null, n: { $sum: 1 }, names: { $push: '$name' } } },
  ]);
  assert.equal(grouped.length, 1);
  assert.equal(grouped[0].n, 2);

  const sqlRows = await db.sql(`SELECT COUNT(*) AS n FROM ${name}`);
  assert.equal(sqlRows[0].n, 3);
  const sqlPlan = await db.explainSql(`SELECT * FROM ${name} WHERE age >= $1`, [40]);
  assert.equal(typeof sqlPlan.access_path, 'string');
  const sqlInsert = await db.sql(
    `INSERT INTO ${name} (name, age) VALUES ($1, $2)`,
    ['SQL', 22],
  );
  assert.equal(sqlInsert[0].inserted, 1);
  const sqlUpdate = await db.sql(
    `UPDATE ${name} SET age = $1 WHERE name = $2`,
    [23, 'SQL'],
  );
  assert.deepEqual(sqlUpdate[0], { matched: 1, modified: 1 });

  const adults = await users.find(
    { age: { $gte: 40 } },
    { sort: [['age', -1]], limit: 2 },
  );
  assert.equal(adults.length, 2);
  assert.equal(adults[0].name, 'Linus');

  const update = await users.updateResult(
    { name: 'Grace' },
    { $set: { active: true } },
    { upsert: true },
  );
  assert.equal(update.modified, 1);

  const upsert = await users.updateResult(
    { name: 'New' },
    { $set: { age: 20 } },
    { upsert: true },
  );
  assert.equal(upsert.matched, 0);
  assert.equal(typeof upsert.upserted_id, 'string');

  assert.equal((await users.getMany([id])).length, 1);

  if (['1', 'true', 'yes', 'on'].includes((process.env.NONODB_HISTORY || '').toLowerCase())) {
    const revisions = await users.history(id, { limit: 10, includeDocs: true });
    assert.ok(revisions.length >= 1);
    const first = await users.getVersion(id, revisions[revisions.length - 1].rev);
    assert.equal(first.doc.name, 'Ada');
  }

  await users.createIndex(['age', 'name'], { include: ['tags'] });
  assert.ok((await users.listIndexes()).length >= 1);
  await users.dropIndex('age+name');

  await users.createTextIndex('name');
  await users.createFuzzyIndex('name');
  const found = await users.search('name', 'grace');
  assert.equal(found.length, 1);
  const fuzzy = await users.fuzzy('name', 'grce');
  assert.ok(fuzzy.length >= 1);

  const atomic = await users.writeBatch([
    { op: 'insert', doc: { name: 'Atomic', age: 1 } },
    { op: 'update', filter: { name: 'Ada' }, update: { $inc: { age: 1 } } },
  ]);
  assert.equal(atomic.length, 2);

  const read = await db.batch([
    { op: 'count', coll: name, filter: {} },
    { op: 'find_one', coll: name, filter: { name: 'Ada' } },
  ]);
  assert.equal(read.length, 2);
  assert.equal(read[0].ok, true);

  const deleted = await users.delete({ name: 'Atomic' });
  assert.equal(deleted, 1);

  console.log('node client smoke test: ok');
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
