"""End-to-end automatic collection change events.

    cargo build -p nonodb-daemon -p nonodb-cli
    python3 scripts/events_auto_test.py
"""

from __future__ import annotations

import os
import shutil
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


def main() -> int:
    daemon = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
    if not daemon.exists():
        print(f"missing daemon binary: {daemon}", file=sys.stderr)
        return 2
    tmp = Path(tempfile.mkdtemp(prefix="nonodb-events-auto-"))
    sock = tmp / "nonodb.sock"
    log = tmp / "daemon.log"
    env = os.environ.copy()
    env.update({
        "NONODB_DATA": str(tmp / "data"),
        "NONODB_SOCK": str(sock),
        "NONODB_LOG": "warn",
        "NONODB_TTL_SWEEP_INTERVAL": "1",
    })
    fh = log.open("wb")
    proc = subprocess.Popen([str(daemon)], env=env, stdout=fh, stderr=subprocess.STDOUT)
    fh.close()
    try:
        wait_for_socket(sock, proc)
        db = NonoDB(str(sock), cache_ttl=0.0)
        try:
            db.create_collection("orders")
            off = db.get_collection_events("orders")
            assert off["coll"] == "orders" and off["enabled"] is False, off
            assert off["fields"] == [] and off["include_before"] is True, off

            orders = db.collection("orders")
            order_id = orders.insert({"_id": "o1", "status": "new", "total": 10})
            assert db.collection("_events").count({"type": "orders.insert"}) == 0

            enabled = db.set_collection_events("orders", True, ["status"])
            assert enabled["enabled"] is True and enabled["fields"] == ["status"], enabled
            orders.update({"_id": order_id}, {"$set": {"total": 11}})
            assert db.collection("_events").count({"type": "orders.update"}) == 0

            orders.update({"_id": order_id}, {"$set": {"status": "paid"}})
            rows = db.collection("_events").find({"type": "orders.update"}, sort=[("_id", 1)])
            assert len(rows) == 1, rows
            event = rows[0]
            assert event["payload"]["changed_fields"] == ["status"], event
            assert event["payload"]["before"]["status"] == "new", event
            assert event["payload"]["after"]["status"] == "paid", event

            handler = db.collection("orders_processed")
            db.set_collection_events("orders", True, [])
            orders.update({"_id": order_id}, {"$set": {"total": 12}})
            rows = db.collection("_events").find(
                {"type": "orders.update"}, sort=[("_id", 1)]
            )
            assert len(rows) == 2, rows
            assert rows[-1]["payload"]["changed_fields"] == ["total"], rows[-1]

            # Server-side watch filter receives only the matching topic and field.
            after = rows[-1]["_id"]
            batch = db.collection("_events").watch_once(
                after=after,
                types=["orders.update"],
                filter={"payload.after.status": "paid"},
                timeout_ms=100,
            )
            assert batch["events"] == [], batch

            # Threshold streaming: only one event is delivered after two matches.
            db.create_collection("threshold_orders")
            db.set_collection_events("threshold_orders", True, [])
            threshold_orders = db.collection("threshold_orders")
            threshold_orders.insert({"_id": "t1", "total": 0})
            threshold_orders.update({"_id": "t1"}, {"$set": {"total": 1}})
            threshold_orders.update({"_id": "t1"}, {"$set": {"total": 2}})
            stream = db.collection("_events").watch_stream(
                types=["threshold_orders.update"],
                threshold={"count": 2, "window_ms": 60_000},
                timeout_ms=2_000,
            )
            try:
                threshold_event = next(stream)
            finally:
                stream.close()
            assert threshold_event["threshold"]["count"] == 2, threshold_event
            assert threshold_event["payload"]["after"]["total"] == 2, threshold_event

            orders.delete({"_id": order_id})
            deleted = db.collection("_events").find({"type": "orders.delete"})
            assert len(deleted) == 1, deleted
            assert deleted[0]["payload"]["before"]["_id"] == order_id, deleted[0]

            # Source-side filtering: ops, event filter, payload projection.
            db.create_collection("filtered_orders")
            db.set_collection_events(
                "filtered_orders",
                True,
                ["status"],
                ops=["update"],
                filter={"payload.after.status": "paid"},
                include_before=False,
                projection={"status": 1, "total": 1},
            )
            filtered = db.collection("filtered_orders")
            filtered.insert({"_id": "f1", "status": "new", "total": 1, "secret": "x"})
            assert db.collection("_events").count({"type": "filtered_orders.insert"}) == 0
            filtered.update({"_id": "f1"}, {"$set": {"total": 2}})
            assert db.collection("_events").count({"type": "filtered_orders.update"}) == 0
            filtered.update({"_id": "f1"}, {"$set": {"status": "paid"}})
            filtered_event = db.collection("_events").find_one(
                {"type": "filtered_orders.update"}
            )
            assert filtered_event["payload"]["before"] is None, filtered_event
            assert "secret" not in filtered_event["payload"]["after"], filtered_event
            assert filtered_event["payload"]["after"]["status"] == "paid", filtered_event

            # find_and_modify emits update/delete events.
            db.create_collection("fm_orders")
            db.set_collection_events("fm_orders", True, [])
            fm = db.collection("fm_orders")
            fm.insert({"_id": "fm1", "status": "new", "n": 0})
            fm.find_and_modify({"_id": "fm1"}, update={"$set": {"n": 1}})
            assert db.collection("_events").count({"type": "fm_orders.update"}) == 1
            fm.find_and_modify({"_id": "fm1"}, remove=True)
            assert db.collection("_events").count({"type": "fm_orders.delete"}) == 1

            # TTL expiry emits the same delete event.
            db.create_collection("ttl_orders", ttl_seconds=1)
            db.set_collection_events("ttl_orders", True, [])
            db.collection("ttl_orders").insert({"_id": "ttl1", "n": 1})
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                if db.collection("_events").count({"type": "ttl_orders.delete"}) == 1:
                    break
                time.sleep(0.2)
            assert db.collection("_events").count({"type": "ttl_orders.delete"}) == 1

            # SQL writes use the same automatic event path.
            db.create_collection("sql_orders")
            db.set_collection_events("sql_orders", True, ["status"])
            db.sql("INSERT INTO sql_orders (_id, status, total) VALUES ('s1', 'new', 1)")
            assert db.collection("_events").count({"type": "sql_orders.insert"}) == 1
            db.sql("UPDATE sql_orders SET total = 2 WHERE _id = 's1'")
            assert db.collection("_events").count({"type": "sql_orders.update"}) == 0
            db.sql("UPDATE sql_orders SET status = 'paid' WHERE _id = 's1'")
            assert db.collection("_events").count({"type": "sql_orders.update"}) == 1
            db.sql("DELETE FROM sql_orders WHERE _id = 's1'")
            assert db.collection("_events").count({"type": "sql_orders.delete"}) == 1

            db.set_collection_events("orders", False)
            orders.insert({"_id": "o2", "status": "new"})
            assert db.collection("_events").count({"type": "orders.insert"}) == 0
            assert handler.count({}) == 0

            uds = httpx.Client(transport=httpx.HTTPTransport(uds=str(sock)))
            try:
                metrics = uds.get("http://localhost/metrics", timeout=10).json()
            finally:
                uds.close()
            assert metrics["collection_events_emitted_total"] > 0, metrics
            assert metrics["collection_events_suppressed_total"] > 0, metrics
            assert metrics["watch_threshold_emitted_total"] > 0, metrics
            assert metrics["watch_threshold_suppressed_total"] > 0, metrics
        finally:
            db.close()

        cli = ROOT / "target/debug/nonodb-cli"
        if cli.exists():
            cli_env = os.environ.copy()
            cli_env["NONODB_SOCK"] = str(sock)
            out = subprocess.run(
                [str(cli), "events", "orders"],
                env=cli_env,
                check=True,
                text=True,
                capture_output=True,
            ).stdout
            assert '"enabled": false' in out, out
            subprocess.run(
                [str(cli), "events", "orders", "on", "status", "total"],
                env=cli_env,
                check=True,
                stdout=subprocess.DEVNULL,
            )
            out = subprocess.run(
                [str(cli), "events", "orders"],
                env=cli_env,
                check=True,
                text=True,
                capture_output=True,
            ).stdout
            assert '"enabled": true' in out and '"status"' in out, out
            tail = subprocess.run(
                [str(cli), "events", "tail", "--once", "--type", "sql_orders.delete"],
                env=cli_env,
                check=True,
                text=True,
                capture_output=True,
            ).stdout
            assert "sql_orders.delete" in tail, tail

        print("OK: automatic per-collection events, field filters, delete/insert, watch filter, CLI tail")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
