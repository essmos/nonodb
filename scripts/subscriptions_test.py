"""End-to-end conditional collection subscriptions.

    cargo build -p nonodb-daemon
    python3 scripts/subscriptions_test.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

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


def events(db: NonoDB, type_: str) -> list[dict]:
    return db.collection("_events").find({"type": type_}, sort=[("_id", 1)])


def main() -> int:
    daemon = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
    if not daemon.exists():
        print(f"missing daemon binary: {daemon}", file=sys.stderr)
        return 2
    tmp = Path(tempfile.mkdtemp(prefix="nonodb-subscriptions-"))
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

        # Default: send every write whose condition is true. No flag needed.
        sub = db.create_subscription(
            "orders", "paid", filter={"status": "paid"}
        )
        assert sub["name"] == "paid" and sub["once"] is False, sub
        orders.insert({"_id": "o1", "status": "new", "a": 10})
        assert events(db, "subscription.paid") == []
        orders.insert({"_id": "o2", "status": "paid", "a": 20})
        orders.update({"_id": "o2"}, {"$set": {"a": 21}})
        rows = events(db, "subscription.paid")
        assert len(rows) == 2, rows
        assert rows[0]["payload"]["op"] == "insert", rows
        assert rows[0]["payload"]["after"]["_id"] == "o2", rows
        assert rows[1]["payload"]["op"] == "update", rows

        # Delete evaluates the before document.
        orders.delete({"_id": "o2"})
        rows = events(db, "subscription.paid")
        assert len(rows) == 3 and rows[-1]["payload"]["op"] == "delete", rows
        assert rows[-1]["payload"]["before"]["_id"] == "o2", rows

        # once=true is an optional flag: only false -> true transitions emit.
        db.create_subscription(
            "orders",
            "high_once",
            filter={"a": {"$gt": 100}},
            once=True,
        )
        orders.update({"_id": "o1"}, {"$set": {"a": 50}})
        assert events(db, "subscription.high_once") == []
        orders.update({"_id": "o1"}, {"$set": {"a": 150}})
        orders.update({"_id": "o1"}, {"$set": {"a": 200}})
        rows = events(db, "subscription.high_once")
        assert len(rows) == 1, rows
        orders.update({"_id": "o1"}, {"$set": {"a": 0}})
        orders.update({"_id": "o1"}, {"$set": {"a": 101}})
        rows = events(db, "subscription.high_once")
        assert len(rows) == 2, rows

        # count over collection state, not over the number of events.
        db.create_collection("count_orders")
        count_orders = db.collection("count_orders")
        db.create_subscription(
            "count_orders",
            "many",
            count={"filter": {"a": {"$gt": 100}}, "gt": 1},
        )
        count_orders.insert({"_id": "c1", "a": 150})
        assert events(db, "subscription.many") == []
        count_orders.insert({"_id": "c2", "a": 200})
        rows = events(db, "subscription.many")
        assert len(rows) == 1, rows
        assert rows[0]["payload"]["count"] == 2, rows[0]
        count_orders.delete({"_id": "c1"})
        assert len(events(db, "subscription.many")) == 1
        count_orders.insert({"_id": "c3", "a": 300})
        rows = events(db, "subscription.many")
        assert len(rows) == 2 and rows[-1]["payload"]["count"] == 2, rows

        # Ops filter and custom event type.
        db.create_subscription(
            "orders",
            "only_updates",
            ops=["update"],
            filter={"a": {"$gte": 0}},
            event_type="custom.orders_changed",
        )
        orders.insert({"_id": "u1", "a": 1})
        assert events(db, "custom.orders_changed") == []
        orders.update({"_id": "u1"}, {"$set": {"a": 2}})
        rows = events(db, "custom.orders_changed")
        assert len(rows) == 1 and rows[0]["payload"]["op"] == "update", rows

        listed = {s["name"]: s for s in db.list_subscriptions("orders")}
        assert set(listed) == {"paid", "high_once", "only_updates"}, listed
        assert {s["name"] for s in db.list_subscriptions("count_orders")} == {"many"}
        assert listed["high_once"]["once"] is True, listed
        assert db.delete_subscription("orders", "paid") is True
        assert db.delete_subscription("orders", "paid") is False
        assert "paid" not in {s["name"] for s in db.list_subscriptions("orders")}
        orders.update({"_id": "o1"}, {"$set": {"status": "paid"}})
        assert len(events(db, "subscription.paid")) == 3

        # Leases: the client must renew before expiry, otherwise the
        # subscription is removed and stops evaluating conditions.
        db.create_collection("lease_orders")
        lease_orders = db.collection("lease_orders")
        lease = db.create_subscription(
            "lease_orders",
            "lease",
            filter={"ok": True},
            lease_seconds=2,
        )
        assert lease["lease_seconds"] == 2 and lease["expires_at"] > 0, lease
        time.sleep(1.2)
        renewed = db.renew_subscription("lease_orders", "lease")
        assert renewed is not None and renewed["expires_at"] >= lease["expires_at"], renewed
        time.sleep(1.2)
        lease_orders.insert({"_id": "l1", "ok": True})
        rows = events(db, "subscription.lease")
        assert len(rows) == 1, rows
        time.sleep(2.2)
        lease_orders.insert({"_id": "l2", "ok": True})
        assert len(events(db, "subscription.lease")) == 1
        assert db.list_subscriptions("lease_orders") == []
        assert db.renew_subscription("lease_orders", "lease") is None

        # SQL writes evaluate the same writer-side conditions.
        db.create_collection("sql_orders")
        db.create_subscription(
            "sql_orders", "sql_paid", filter={"status": "paid"}
        )
        db.sql("INSERT INTO sql_orders (_id, status) VALUES ('s1', 'new')")
        assert events(db, "subscription.sql_paid") == []
        db.sql("UPDATE sql_orders SET status = 'paid' WHERE _id = 's1'")
        rows = events(db, "subscription.sql_paid")
        assert len(rows) == 1 and rows[0]["payload"]["op"] == "update", rows
        db.sql("DELETE FROM sql_orders WHERE _id = 's1'")
        rows = events(db, "subscription.sql_paid")
        assert len(rows) == 2 and rows[-1]["payload"]["op"] == "delete", rows

        # Subscription state survives restart.
        proc.terminate()
        proc.wait(timeout=10)
        proc = subprocess.Popen([str(daemon)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        wait_for_socket(sock, proc)
        db = NonoDB(str(sock), cache_ttl=0.0)
        listed = {s["name"]: s for s in db.list_subscriptions("orders")}
        assert "high_once" in listed, listed
        orders = db.collection("orders")
        before = len(events(db, "subscription.high_once"))
        orders.update({"_id": "o1"}, {"$set": {"a": 0}})
        orders.update({"_id": "o1"}, {"$set": {"a": 300}})
        assert len(events(db, "subscription.high_once")) == before + 1

        print("subscriptions e2e: ok")
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
