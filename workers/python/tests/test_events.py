"""Unit tests for the outbox event helper."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import nonodb_worker.events as events_module  # noqa: E402
from nonodb_worker import (  # noqa: E402
    EventConsumer,
    EventProducer,
    EventRunner,
    event_timestamp_ms,
    new_event_id,
)


class FakeEvents:
    name = "events"

    def __init__(self, docs: Optional[list[dict[str, Any]]] = None):
        self.docs = list(docs or [])
        self.batches: list[list[dict[str, Any]]] = []

    def write_batch(self, ops: list[dict[str, Any]]) -> list[Any]:
        self.batches.append(ops)
        for op in ops:
            if op["op"] == "insert" and op["coll"] == self.name:
                self.docs.append(op["doc"])
        return []

    def find(
        self,
        query: Optional[dict[str, Any]] = None,
        *,
        sort: Any = None,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        query = query or {}
        bounds = query.get("_id", {})
        wanted = query.get("type", {}).get("$in")
        docs = []
        for doc in self.docs:
            event_id = doc["_id"]
            if "$gt" in bounds and not event_id > bounds["$gt"]:
                continue
            if "$gte" in bounds and not event_id >= bounds["$gte"]:
                continue
            if "$lt" in bounds and not event_id < bounds["$lt"]:
                continue
            if "$lte" in bounds and not event_id <= bounds["$lte"]:
                continue
            if wanted is not None and doc["type"] not in wanted:
                continue
            docs.append(doc)
        docs.sort(key=lambda d: d["_id"])
        if limit is not None:
            docs = docs[:limit]
        return docs


class FakeCheckpoints:
    def __init__(self):
        self.docs: dict[str, dict[str, Any]] = {}

    def find_one(self, query: Mapping[str, Any], **_kwargs: Any):
        return self.docs.get(query["_id"])

    def update(
        self,
        query: Mapping[str, Any],
        update: Mapping[str, Any],
        **_kwargs: Any,
    ) -> int:
        doc = dict(self.docs.get(query["_id"], {"_id": query["_id"]}))
        doc.update(update.get("$set", {}))
        self.docs[query["_id"]] = doc
        return 1

    def delete(self, query: Mapping[str, Any]) -> int:
        return 1 if self.docs.pop(query["_id"], None) is not None else 0


class EventIdTests(unittest.TestCase):
    def setUp(self) -> None:
        events_module._last_ms = 0
        events_module._last_random = 0

    def test_ids_are_sortable_and_embed_time(self) -> None:
        first = new_event_id(1_700_000_000_000)
        second = new_event_id(1_700_000_000_001)
        self.assertLess(first, second)
        self.assertEqual(event_timestamp_ms(first), 1_700_000_000_000)
        self.assertEqual(event_timestamp_ms(second), 1_700_000_000_001)
        self.assertIsNone(event_timestamp_ms("bad"))

    def test_same_millisecond_is_monotonic_in_one_process(self) -> None:
        ids = [new_event_id(1_700_000_000_000) for _ in range(10)]
        self.assertEqual(ids, sorted(ids))
        self.assertEqual(len(ids), len(set(ids)))


class EventProducerTests(unittest.TestCase):
    def test_emit_is_atomic_with_data_ops(self) -> None:
        events = FakeEvents()
        producer = EventProducer(events, clock=lambda: 123, id_factory=lambda: "E1")
        event_id = producer.emit(
            "order.created",
            {"x": 1},
            doc_id="o1",
            data_ops=[{"op": "insert", "coll": "orders", "doc": {"_id": "o1"}}],
        )
        self.assertEqual(event_id, "E1")
        self.assertEqual(events.batches[0][0]["coll"], "orders")
        self.assertEqual(events.batches[0][1]["doc"]["created_at"], 123)
        self.assertEqual(events.batches[0][1]["doc"]["type"], "order.created")

    def test_data_ops_require_collection(self) -> None:
        producer = EventProducer(FakeEvents(), id_factory=lambda: "E1")
        with self.assertRaises(ValueError):
            producer.emit("x", data_ops=[{"op": "insert"}])


class EventConsumerRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        events_module._last_ms = 0
        events_module._last_random = 0

    def test_consumer_advances_and_runner_dispatches(self) -> None:
        events = FakeEvents(
            [
                {"_id": "A", "type": "a"},
                {"_id": "B", "type": "b"},
            ]
        )
        checkpoints = FakeCheckpoints()
        consumer = EventConsumer(events, checkpoints, "c1", clock=lambda: 5)
        runner = EventRunner(consumer)
        seen: list[str] = []
        runner.register("a", lambda event: seen.append(event["_id"]))
        runner.register("b", lambda event: seen.append(event["_id"]))
        self.assertEqual(runner.run_once(limit=10), 2)
        self.assertEqual(seen, ["A", "B"])
        self.assertEqual(consumer.checkpoint(), "B")
        self.assertEqual(runner.run_once(limit=10), 0)

    def test_failure_stops_checkpoint_before_failed_event(self) -> None:
        events = FakeEvents(
            [
                {"_id": "A", "type": "ok"},
                {"_id": "B", "type": "boom"},
                {"_id": "C", "type": "ok"},
            ]
        )
        checkpoints = FakeCheckpoints()
        consumer = EventConsumer(events, checkpoints, "c1")
        runner = EventRunner(consumer)
        runner.register("ok", lambda event: None)

        def boom(_event):
            raise RuntimeError("boom")

        runner.register("boom", boom)
        self.assertEqual(runner.run_once(limit=10), 1)
        self.assertEqual(consumer.checkpoint(), "A")

    def test_unknown_types_are_ignored_by_default(self) -> None:
        events = FakeEvents([{"_id": "A", "type": "new"}])
        consumer = EventConsumer(events, FakeCheckpoints(), "c1")
        runner = EventRunner(consumer)
        self.assertEqual(runner.run_once(limit=10), 1)
        self.assertEqual(consumer.checkpoint(), "A")

    def test_late_lower_id_is_rescanned_after_overlap(self) -> None:
        now = 1_700_000_010_000
        low = new_event_id(1_700_000_004_000)
        high = new_event_id(1_700_000_005_000)
        self.assertLess(low, high)
        events = FakeEvents(
            [
                {"_id": high, "type": "e"},
                {"_id": low, "type": "e"},
            ]
        )
        checkpoints = FakeCheckpoints()
        checkpoints.docs["c1"] = {"_id": "c1", "last_id": high}
        consumer = EventConsumer(
            events,
            checkpoints,
            "c1",
            clock=lambda: now,
            overlap_seconds=2.0,
        )
        fetched = consumer.fetch(limit=10)
        self.assertIn(low, [event["_id"] for event in fetched])
        consumer.advance(fetched)
        self.assertEqual(consumer.checkpoint(), high)
        self.assertTrue(consumer._checkpoint_doc()["late_cursor"])
        self.assertEqual(consumer.fetch(limit=10), [])


if __name__ == "__main__":
    unittest.main()
