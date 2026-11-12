"""End-to-end connection-scoped live SSE/NDJSON subscriptions.

    cargo build -p nonodb-daemon
    python3 scripts/live_watch_test.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB  # noqa: E402


def wait_for_socket(path: Path, proc: subprocess.Popen[bytes], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if proc.poll() is not None:
            raise RuntimeError("daemon exited before creating its socket")
        time.sleep(0.05)
    raise RuntimeError("timed out waiting for daemon socket")


class Stream:
    def __init__(self, client: NonoDB, payload: dict):
        self._ctx = client._http.stream(
            "POST",
            client.base_url + "/v1/watch",
            content=json.dumps(payload),
            headers={
                "content-type": "application/json",
                "accept": "application/x-ndjson",
            },
            timeout=None,
        )
        self.resp = self._ctx.__enter__()
        assert self.resp.status_code == 200, self.resp.read()
        self.lines = self.resp.iter_lines()
        self.ready = self.next_json()
        assert self.ready["stream"]["mode"] == "live", self.ready

    def next_json(self) -> dict:
        while True:
            line = next(self.lines).strip()
            if not line:
                continue
            item = json.loads(line)
            if "heartbeat" in item:
                continue
            return item

    def next_event(self, timeout: float = 5.0) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            item = self.next_json()
            if "stream" not in item:
                return item
        raise TimeoutError("no live event")

    def close(self) -> None:
        self._ctx.__exit__(None, None, None)


def main() -> int:
    daemon = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
    if not daemon.exists():
        print(f"missing daemon binary: {daemon}", file=sys.stderr)
        return 2
    tmp = Path(tempfile.mkdtemp(prefix="nonodb-live-"))
    sock = tmp / "nonodb.sock"
    log = tmp / "daemon.log"
    env = os.environ.copy()
    env.update({
        "NONODB_DATA": str(tmp / "data"),
        "NONODB_SOCK": str(sock),
        "NONODB_LOG": "warn",
    })
    fh = log.open("wb")
    proc = subprocess.Popen([str(daemon)], env=env, stdout=fh, stderr=subprocess.STDOUT)
    fh.close()
    try:
        wait_for_socket(sock, proc)
        db = NonoDB(str(sock), cache_ttl=0.0)
        db.create_collection("orders")
        orders = db.collection("orders")

        stream = Stream(db, {
            "op": "watch",
            "coll": "orders",
            "mode": "live",
            "filter": {"status": "paid"},
            "heartbeat_ms": 500,
        })
        orders.insert({"_id": "o1", "status": "new"})
        orders.insert({"_id": "o2", "status": "paid", "total": 10})
        event = stream.next_event()
        assert event["type"] == "subscription.live", event
        assert event["payload"]["op"] == "insert", event
        assert event["payload"]["after"]["_id"] == "o2", event
        orders.update({"_id": "o2"}, {"$set": {"total": 11}})
        event = stream.next_event()
        assert event["payload"]["op"] == "update", event
        assert event["payload"]["after"]["total"] == 11, event
        orders.delete({"_id": "o2"})
        event = stream.next_event()
        assert event["payload"]["op"] == "delete", event
        assert event["payload"]["before"]["_id"] == "o2", event
        stream.close()

        # Live events are never written to the outbox.
        assert db.collection("_events").count({"type": {"$regex": "^subscription\\."}}) == 0

        # SQL writes use the same writer-side live path.
        sql_stream = Stream(db, {
            "op": "watch",
            "coll": "orders",
            "mode": "live",
            "filter": {"status": "paid"},
            "name": "sql_live",
        })
        db.sql("INSERT INTO orders (_id, status) VALUES ('s1', 'new')")
        db.sql("UPDATE orders SET status = 'paid' WHERE _id = 's1'")
        event = sql_stream.next_event()
        assert event["type"] == "subscription.sql_live", event
        assert event["payload"]["op"] == "update", event
        sql_stream.close()

        # Count conditions are evaluated over collection state.
        count_stream = Stream(db, {
            "op": "watch",
            "coll": "orders",
            "mode": "live",
            "count": {"filter": {"status": "paid"}, "gt": 1},
        })
        orders.insert({"_id": "c1", "status": "paid"})
        orders.insert({"_id": "c2", "status": "paid"})
        event = count_stream.next_event()
        assert event["payload"]["count"] == 2, event
        count_stream.close()

        # A closed live stream removes the subscription. This write must not
        # emit anywhere, and the persistent registry must stay empty.
        orders.update({"_id": "c1"}, {"$set": {"total": 99}})
        assert db.list_subscriptions("orders") == []
        assert db.collection("_events").count({}) == 0

        print("live watch e2e: ok")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        if proc.returncode not in (0, -15):
            print(log.read_text(errors="replace"), file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
