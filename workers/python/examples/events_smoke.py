"""End-to-end smoke test for outbox events.

Run against a temporary daemon:
    PYTHONPATH=client:workers/python NONODB_SOCK=/tmp/nonodb.sock \
      python3 workers/python/examples/events_smoke.py
"""

from __future__ import annotations

import time

from nonodb import NonoDB
from nonodb_worker import EventConsumer, EventProducer, EventRunner

EVENTS = "_events_smoke"
CHECKPOINTS = "_event_checkpoints_smoke"
ORDERS = "_orders_smoke"


def main() -> int:
    db = NonoDB()
    try:
        events = db.collection(EVENTS)
        checkpoints = db.collection(CHECKPOINTS)
        orders = db.collection(ORDERS)
        for collection in (events, checkpoints, orders):
            collection.delete({}, multi=True)

        producer = EventProducer(events)
        first = producer.emit(
            "order.created",
            {"total": 1200},
            doc_id="o1",
            data_ops=[
                {
                    "op": "insert",
                    "coll": ORDERS,
                    "doc": {"_id": "o1", "status": "new", "total": 1200},
                }
            ],
        )
        second = producer.emit(
            "order.paid",
            {"total": 1200},
            doc_id="o1",
            data_ops=[
                {
                    "op": "update",
                    "coll": ORDERS,
                    "filter": {"_id": "o1"},
                    "update": {"$set": {"status": "paid"}},
                }
            ],
        )
        assert first < second

        consumer = EventConsumer(events, checkpoints, "smoke")
        runner = EventRunner(consumer)
        seen: list[str] = []
        runner.register("order.created", lambda event: seen.append(event["type"]))
        runner.register("order.paid", lambda event: seen.append(event["type"]))

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if runner.run_once(limit=10) == 0:
                break
        assert seen == ["order.created", "order.paid"], seen
        assert consumer.checkpoint() == second
        order = orders.find_one({"_id": "o1"})
        assert order and order["status"] == "paid", order
        print("events smoke OK")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
