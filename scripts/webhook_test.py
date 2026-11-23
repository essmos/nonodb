#!/usr/bin/env python3
"""Integration test for asynchronous webhooks.

Covers simple filters, count/sum thresholds, once, HMAC, idempotency keys,
retries, dead-letter/manual retry, and worker restart.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB  # noqa: E402

SECRET = "webhook-test-secret"
received: dict[str, list[dict]] = {}
fail_counts: dict[str, int] = {}
lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("content-length", "0"))
        body = self.rfile.read(length)
        path = self.path
        with lock:
            received.setdefault(path, []).append(
                {"headers": dict(self.headers), "body": json.loads(body or b"{}")}
            )
            count = fail_counts.get(path, 0)
            fail_counts[path] = count + 1
        if path == "/retry" and count < 2:
            self.send_response(500)
            self.end_headers()
            return
        if path == "/manual" and count == 0:
            self.send_response(500)
            self.end_headers()
            return
        if path == "/slow":
            time.sleep(0.6)
        signature = self.headers.get("x-nonodb-signature", "")
        expected = "sha256=" + hmac.new(
            SECRET.encode(), body, hashlib.sha256
        ).hexdigest()
        if path == "/simple" and signature != expected:
            self.send_response(400)
            self.end_headers()
            return
        self.send_response(200)
        self.end_headers()
        try:
            self.wfile.write(b"ok")
        except BrokenPipeError:
            pass

    def log_message(self, *args):
        pass


def binary(env: str, name: str) -> Path:
    raw = os.environ.get(env)
    path = Path(raw) if raw else ROOT / "target" / "debug" / name
    if not path.is_file():
        path = ROOT / "target" / "release" / name
    if not path.is_file():
        raise SystemExit(f"missing binary: {path}")
    return path


class Daemon:
    def __init__(self, binary_path: Path, data: Path, sock: Path, log: Path):
        env = os.environ.copy()
        env.update(
            NONODB_DATA=str(data),
            NONODB_SOCK=str(sock),
            NONODB_MAP_SIZE="64MiB",
            RUST_LOG="warn",
            WEBHOOK_SECRET=SECRET,
        )
        self.log_path = log
        self.handle = log.open("wb")
        self.process = subprocess.Popen(
            [str(binary_path)], env=env, stdout=self.handle, stderr=subprocess.STDOUT
        )
        for _ in range(200):
            if sock.exists():
                return
            if self.process.poll() is not None:
                break
            time.sleep(0.05)
        self.stop()
        raise RuntimeError(f"daemon did not start: {log.read_text(errors='replace')[-3000:]}")

    def stop(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.handle.close()


def wait_for(predicate, description: str, timeout: float = 20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for {description}")


def deliveries(db, rule: str):
    rows = db.exec({"op": "list_webhook_deliveries", "limit": 100})["deliveries"]
    return [row for row in rows if row.get("rule") == rule]


def main() -> int:
    daemon_bin = binary("NONODB_DAEMON", "nonodb-daemon")
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        with tempfile.TemporaryDirectory(prefix="nonodb-webhook-") as tmp_name:
            tmp = Path(tmp_name)
            data = tmp / "data"
            sock = tmp / "nonodb.sock"
            daemon = Daemon(daemon_bin, data, sock, tmp / "daemon.log")
            try:
                db = NonoDB(str(sock), timeout=30)
                db.exec(
                    {
                        "op": "create_webhook",
                        "name": "simple",
                        "coll": "orders",
                        "url": base + "/simple",
                        "enabled": True,
                        "filter": {"status": "paid"},
                        "ops": ["insert"],
                        "secret_env": "WEBHOOK_SECRET",
                    }
                )
                db.exec(
                    {
                        "op": "create_webhook",
                        "name": "sum-paid",
                        "coll": "orders",
                        "url": base + "/sum",
                        "enabled": True,
                        "filter": {"status": "paid"},
                        "threshold": {
                            "aggregate": "sum",
                            "field": "total",
                            "gte": 10,
                        },
                        "once": True,
                    }
                )
                db.exec(
                    {
                        "op": "create_webhook",
                        "name": "retry",
                        "coll": "orders",
                        "url": base + "/retry",
                        "enabled": True,
                        "filter": {"_id": "retry-1"},
                        "max_attempts": 5,
                    }
                )
                db.exec(
                    {
                        "op": "create_webhook",
                        "name": "manual",
                        "coll": "orders",
                        "url": base + "/manual",
                        "enabled": True,
                        "filter": {"_id": "manual-1"},
                        "max_attempts": 1,
                    }
                )
                db.exec(
                    {
                        "op": "create_webhook",
                        "name": "count-two",
                        "coll": "orders",
                        "url": base + "/count",
                        "enabled": True,
                        "filter": {"status": "count"},
                        "threshold": {"aggregate": "count", "gte": 2},
                        "once": True,
                    }
                )
                db.exec(
                    {
                        "op": "create_webhook",
                        "name": "avg-rate",
                        "coll": "orders",
                        "url": base + "/avg",
                        "enabled": True,
                        "filter": {"status": "rate"},
                        "threshold": {
                            "aggregate": "avg",
                            "field": "score",
                            "gte": 2,
                        },
                        "once": True,
                    }
                )
                db.exec(
                    {
                        "op": "create_webhook",
                        "name": "cooldown",
                        "coll": "orders",
                        "url": base + "/cool",
                        "enabled": True,
                        "filter": {"status": "cool"},
                        "cooldown_seconds": 300,
                    }
                )
                db.exec(
                    {
                        "op": "create_webhook",
                        "name": "missing-secret",
                        "coll": "orders",
                        "url": base + "/missing",
                        "enabled": True,
                        "filter": {"_id": "missing-1"},
                        "secret_env": "NOT_SET_WEBHOOK_SECRET",
                        "max_attempts": 1,
                    }
                )
                db.exec(
                    {
                        "op": "create_webhook",
                        "name": "slow",
                        "coll": "orders",
                        "url": base + "/slow",
                        "enabled": True,
                        "filter": {"_id": "slow-1"},
                        "max_attempts": 2,
                        "timeout_ms": 150,
                    }
                )
                # Let the async worker bootstrap threshold state before writes.
                time.sleep(0.7)
                orders = db.collection("orders")
                orders.insert({"_id": "count-1", "status": "count", "total": 1})
                time.sleep(0.7)
                assert not received.get("/count"), "count webhook fired below threshold"
                orders.insert({"_id": "count-2", "status": "count", "total": 1})
                wait_for(lambda: len(received.get("/count", [])) == 1, "count threshold")
                assert received["/count"][0]["body"]["webhook"]["threshold_value"] == 2.0

                orders.insert({"_id": "rate-1", "status": "rate", "score": 1})
                time.sleep(0.5)
                assert not received.get("/avg"), "avg webhook fired below threshold"
                orders.insert({"_id": "rate-2", "status": "rate", "score": 3})
                wait_for(lambda: len(received.get("/avg", [])) == 1, "avg threshold")
                assert received["/avg"][0]["body"]["webhook"]["threshold_value"] == 2.0

                orders.insert_many(
                    [
                        {"_id": "cool-1", "status": "cool", "total": 1},
                        {"_id": "cool-2", "status": "cool", "total": 1},
                    ]
                )
                wait_for(lambda: len(received.get("/cool", [])) == 1, "cooldown webhook")
                time.sleep(0.5)
                assert len(received.get("/cool", [])) == 1, "cooldown was not applied"

                orders.insert({"_id": "slow-1", "status": "slow", "total": 1})
                wait_for(
                    lambda: any(
                        item["status"] == "dead" for item in deliveries(db, "slow")
                    ),
                    "timed-out delivery",
                )

                orders.insert_many(
                    [
                        {"_id": "a", "status": "paid", "total": 4},
                        {"_id": "b", "status": "new", "total": 100},
                        {"_id": "c", "status": "paid", "total": 7},
                    ]
                )
                wait_for(lambda: len(received.get("/simple", [])) >= 2, "simple webhooks")
                wait_for(lambda: len(received.get("/sum", [])) == 1, "sum threshold")
                assert len(received["/sum"]) == 1, received["/sum"]
                assert received["/sum"][0]["body"]["webhook"]["threshold_value"] == 11.0
                simple = received["/simple"][0]
                assert simple["headers"]["idempotency-key"] == simple["body"]["_id"]
                assert simple["headers"]["x-nonodb-signature"].startswith("sha256=")

                orders.insert({"_id": "retry-1", "status": "paid", "total": 1})
                wait_for(
                    lambda: any(
                        item["status"] == "retry" for item in deliveries(db, "retry")
                    ),
                    "retry status",
                )
                wait_for(
                    lambda: any(
                        item["status"] == "delivered" and item["attempts"] >= 2
                        for item in deliveries(db, "retry")
                    ),
                    "retry delivery",
                )

                orders.insert({"_id": "missing-1", "status": "paid", "total": 1})
                missing = wait_for(
                    lambda: [
                        item
                        for item in deliveries(db, "missing-secret")
                        if item["status"] == "dead"
                    ],
                    "missing secret delivery",
                )
                assert not received.get("/missing"), "unsigned webhook was sent"
                assert "NOT_SET_WEBHOOK_SECRET" in (missing[0]["last_error"] or ""), missing

                slow = deliveries(db, "slow")
                assert slow and slow[0]["status"] == "dead", slow
                assert slow[0]["attempts"] == 2, slow
                assert slow[0]["timeout_ms"] == 150, slow
                assert slow[0]["last_error"], slow

                orders.insert({"_id": "manual-1", "status": "paid", "total": 1})
                dead = wait_for(
                    lambda: [
                        item for item in deliveries(db, "manual") if item["status"] == "dead"
                    ],
                    "dead delivery",
                )
                assert dead
                assert db.exec(
                    {"op": "retry_webhook_delivery", "id": dead[0]["_id"]}
                )["retried"]
                wait_for(
                    lambda: any(
                        item["status"] == "delivered" for item in deliveries(db, "manual")
                    ),
                    "manual retry delivery",
                )

                simple_delivery = next(
                    item
                    for item in deliveries(db, "simple")
                    if item["payload"]["doc_id"] == "a"
                )
                first_key = next(
                    item["headers"]["idempotency-key"]
                    for item in received["/simple"]
                    if item["body"]["doc_id"] == "a"
                )
                before_retry = len(received.get("/simple", []))
                assert db.exec(
                    {"op": "retry_webhook_delivery", "id": simple_delivery["_id"]}
                )["retried"]
                wait_for(
                    lambda: len(received.get("/simple", [])) > before_retry,
                    "manual redelivery",
                )
                assert (
                    received["/simple"][-1]["headers"]["idempotency-key"] == first_key
                ), "duplicate delivery changed the idempotency key"

                cli_path = binary("NONODB_CLI", "nonodb-cli")
                cli_env = os.environ.copy()
                cli_env["NONODB_SOCK"] = str(sock)

                def cli(*args):
                    result = subprocess.run(
                        [str(cli_path), *args],
                        env=cli_env,
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    assert result.returncode == 0, result.stderr or result.stdout
                    return result.stdout

                cli(
                    "webhooks",
                    "create",
                    "cli-hook",
                    "orders",
                    base + "/cli",
                    "--filter",
                    '{"status":"paid"}',
                    "--timeout-ms",
                    "5000",
                )
                assert "cli-hook" in cli("webhooks", "list")
                cli(
                    "webhooks",
                    "update",
                    "cli-hook",
                    "orders",
                    base + "/cli",
                    "--filter",
                    '{"status":"new"}',
                    "--disabled",
                )
                assert "cli-hook" in cli("webhooks", "list")
                cli("webhooks", "delete", "cli-hook")
                assert "cli-hook" not in cli("webhooks", "list")

                rules = db.exec({"op": "list_webhooks"})["webhooks"]
                assert {rule["name"] for rule in rules} >= {
                    "simple",
                    "sum-paid",
                    "retry",
                    "manual",
                    "count-two",
                    "avg-rate",
                    "cooldown",
                    "slow",
                    "missing-secret",
                }
                db.close()

                # Restart: durable state and rules must keep working.
                daemon.stop()
                daemon = Daemon(daemon_bin, data, sock, tmp / "daemon2.log")
                db = NonoDB(str(sock), timeout=30)
                before = len(received.get("/simple", []))
                db.collection("orders").insert(
                    {"_id": "after-restart", "status": "paid", "total": 3}
                )
                wait_for(
                    lambda: len(received.get("/simple", [])) > before,
                    "webhook after restart",
                )
                # Threshold state must survive restart: the once-only sum rule
                # must not fire again just because the collection is above 10.
                time.sleep(0.7)
                assert len(received.get("/sum", [])) == 1, "threshold re-armed after restart"
                db.close()
            finally:
                daemon.stop()
    finally:
        server.shutdown()

    print("webhook test: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
