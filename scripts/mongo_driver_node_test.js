#!/usr/bin/env node
"use strict";

const { randomUUID } = require("crypto");
const {
  Binary,
  Decimal128,
  MaxKey,
  MinKey,
  ObjectId,
  Timestamp,
} = require("bson");
const { MongoClient } = require("mongodb");

const url =
  process.env.NONODB_MONGO_URL ||
  "mongodb://127.0.0.1:37017/?directConnection=true";
const databaseName = `node_driver_${randomUUID().replaceAll("-", "").slice(0, 12)}`;
const objectId = new ObjectId("64b64c000000000000000001");
const when = new Date("2024-01-01T00:00:00.000Z");

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function main() {
  const client = new MongoClient(url, {
    serverSelectionTimeoutMS: 5000,
  });
  try {
    await client.connect();
    const db = client.db(databaseName);
    await db.command({ ping: 1 });
    const items = db.collection("items");

    const inserted = await items.insertMany([
      {
        _id: "a",
        n: 1,
        tag: "x",
        nested: { v: true },
        arr: [1, 2, 3],
        when,
        dec: Decimal128.fromString("1.50"),
        bin: new Binary(Buffer.from([0, 1, 255])),
        ts: new Timestamp({ t: 1_700_000_000, i: 7 }),
        low: new MinKey(),
        high: new MaxKey(),
        oid: objectId,
      },
      { _id: "b", n: 2, tag: "y", arr: [2, 3, 4] },
      { _id: "c", n: 3, tag: "x", arr: [3, 4, 5] },
    ]);
    assert(inserted.insertedCount === 3, "insertMany count");

    const docs = await items.find().sort({ _id: 1 }).toArray();
    assert(docs.length === 3, "find all count");
    assert(docs[0].dec.toString() === "1.50", "Decimal128 round-trip");
    assert(docs[0].oid.equals(objectId), "ObjectId round-trip");
    assert(docs[0].ts.t === 1_700_000_000, "Timestamp round-trip");
    assert(docs[0].low instanceof MinKey, "MinKey round-trip");
    assert(docs[0].high instanceof MaxKey, "MaxKey round-trip");
    assert(Buffer.from(docs[0].bin.buffer).equals(Buffer.from([0, 1, 255])), "Binary round-trip");

    const filtered = await items
      .find({ n: { $gte: 2 }, arr: { $all: [3] } })
      .sort({ n: -1 })
      .toArray();
    assert(filtered.map((doc) => doc._id).join(",") === "c,b", "filter and sort");
    assert((await items.countDocuments({ tag: /^x/ })) === 2, "BSON regex filter");

    const updated = await items.updateOne(
      { _id: "a" },
      { $set: { status: "new" }, $inc: { n: 10 } }
    );
    assert(updated.matchedCount === 1 && updated.modifiedCount === 1, "update result");
    assert((await items.findOne({ _id: "a" })).n === 11, "update value");

    const grouped = await items
      .aggregate(
        [
          { $group: { _id: "$tag", total: { $sum: "$n" } } },
          { $sort: { _id: 1 } },
        ],
        { batchSize: 1 }
      )
      .toArray();
    assert(
      JSON.stringify(grouped) === JSON.stringify([
        { _id: "x", total: 14 },
        { _id: "y", total: 2 },
      ]),
      "aggregate group"
    );

    const projected = await items
      .aggregate(
        [
          { $match: { tag: "x" } },
          { $project: { _id: 1, n: 1, double: { $multiply: ["$n", 2] } } },
        ],
        { batchSize: 1 }
      )
      .toArray();
    assert(projected.length === 2, "streamed aggregate count");
    assert(projected[0]._id === "a" && projected[0].double === 22, "streamed aggregate");

    assert((await items.distinct("tag")).sort().join(",") === "x,y", "distinct");
    assert((await items.countDocuments({ tag: "x" })) === 2, "count");

    const indexName = await items.createIndex(
      { tag: 1, n: -1 },
      { name: "custom_tag_n" }
    );
    assert(indexName === "custom_tag_n", "create index");
    const indexes = await items.listIndexes().toArray();
    const customIndex = indexes.find((index) => index.name === "custom_tag_n");
    assert(customIndex !== undefined, "list custom index");
    assert(customIndex.key.tag === 1 && customIndex.key.n === -1, "index directions");

    const deleted = await items.deleteMany({ n: { $gte: 11 } });
    assert(deleted.deletedCount === 1, "deleteMany");

    const textItems = db.collection("text_items");
    await textItems.insertMany([
      { _id: "t1", n: 1, title: "Alpha beta", body: "gamma delta" },
      { _id: "t2", n: 2, title: "Alpha", body: "beta gamma" },
      { _id: "t3", n: 3, title: "Delta", body: "epsilon" },
      { _id: "t4", n: 4, title: "alpha beta gamma", body: "delta" },
    ]);
    const textIndex = await textItems.createIndex(
      { title: "text" },
      { default_language: "none" }
    );
    assert(textIndex === "title_text", "text index name");
    const phrase = await textItems
      .find(
        { $text: { $search: '"alpha beta"' } },
        { projection: { _id: 1, score: { $meta: "textScore" } } }
      )
      .sort({ score: { $meta: "textScore" } })
      .toArray();
    assert(phrase.map((doc) => doc._id).sort().join(",") === "t1,t4", "text phrase");
    assert(phrase.every((doc) => typeof doc.score === "number"), "text score");
    assert(
      (await textItems.countDocuments({ $text: { $search: "delta -alpha" } })) === 1,
      "text count and negation"
    );

    const upserts = db.collection("upserts");
    const upserted = await upserts.updateOne(
      { key: "generated" },
      { $set: { n: 1 } },
      { upsert: true }
    );
    assert(upserted.upsertedId instanceof ObjectId, "upsert ObjectId");
    const upsertedDoc = await upserts.findOne({ key: "generated" });
    assert(upsertedDoc._id.equals(upserted.upsertedId), "upsert document id");

    console.log("node mongodb driver test: ok");
  } finally {
    try {
      await client.db(databaseName).dropDatabase();
    } catch (_) {
      // Best-effort cleanup.
    }
    await client.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
