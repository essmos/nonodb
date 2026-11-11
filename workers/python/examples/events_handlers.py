"""Example handlers for ``python -m nonodb_worker.events_worker``."""

from __future__ import annotations


def order_created(event) -> None:
    payload = event.get("payload", {})
    print(f"order created: {event.get('doc_id')} total={payload.get('total')}")


def register(runner) -> None:
    runner.register("order.created", order_created)
