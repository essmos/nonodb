"""Example handlers for ``python -m nonodb_worker``."""

from __future__ import annotations

from nonodb_worker import Job


def add_numbers(job: Job):
    payload = job.payload
    return {"sum": int(payload["a"]) + int(payload["b"])}


def register(runner) -> None:
    runner.register("add", add_numbers)
