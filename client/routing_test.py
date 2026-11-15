"""Write-routing tests for the Python client (standard library only)."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))
from nonodb import NonoDB, NonoDBError  # noqa: E402


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _json(self, status: int, body: dict, location: str | None = None):
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
        if location:
            self.send_header("location", location)
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path == "/readyz":
            self._json(200, {"ok": True, "replication": {
                "write_url": self.server.primary_url,
            }})
            return
        self._json(404, {"ok": False, "error": {"code": "not_found", "message": ""}})

    def do_POST(self):
        length = int(self.headers.get("content-length", "0"))
        payload = json.loads(self.rfile.read(length) or b"{}")
        if self.path.endswith("/v1/exec") is False:
            self._json(404, {"ok": False, "error": {"code": "not_found", "message": ""}})
            return
        if self.server.role == "replica" and payload.get("op") != "find":
            self.server.posts += 1
            self._json(
                403,
                {"ok": False, "error": {
                    "code": "read_only",
                    "message": "replica",
                    "write_url": self.server.primary_url,
                }},
                location=self.server.primary_url,
            )
            return
        self.server.posts += 1
        self.server.seen.append(payload)
        if payload.get("op") == "find":
            self._json(200, {"ok": True, "result": [{"x": 1}]})
        else:
            self._json(200, {"ok": True, "result": {"inserted_id": "doc-1"}})


def start(role: str, primary_url: str):
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.role = role
    server.primary_url = primary_url
    server.posts = 0
    server.seen = []
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_port}"


def main() -> None:
    primary, primary_url = start("primary", "")
    replica, replica_url = start("replica", primary_url)

    # Explicit write endpoint: reads use the replica, writes go straight to primary.
    db = NonoDB(base_url=replica_url, write_url=primary_url, token="test")
    assert db.exec({"op": "insert", "coll": "c", "doc": {"x": 1}})["inserted_id"] == "doc-1"
    assert primary.posts == 1 and replica.posts == 0
    db.exec({"op": "find", "coll": "c"})
    assert replica.posts == 1
    db.close()

    # Discovery: /readyz selects the primary before the first write.
    primary.posts = replica.posts = 0
    db = NonoDB(base_url=replica_url, auto_primary=True, token="test")
    db.exec({"op": "insert", "coll": "c", "doc": {"x": 2}})
    assert primary.posts == 1 and replica.posts == 0
    db.close()

    # Stale write endpoint: retry exactly once after explicit 403 read_only.
    primary.posts = replica.posts = 0
    db = NonoDB(base_url=replica_url, write_url=replica_url, token="test")
    db.exec({"op": "insert", "coll": "c", "doc": {"x": 3}})
    assert replica.posts == 1 and primary.posts == 1
    db.close()

    # Without routing the error exposes the primary and is not retried.
    primary.posts = replica.posts = 0
    db = NonoDB(base_url=replica_url, token="test")
    try:
        db.exec({"op": "insert", "coll": "c", "doc": {"x": 4}})
    except NonoDBError as exc:
        assert exc.code == "read_only"
        assert exc.write_url == primary_url
    else:
        raise AssertionError("read_only was not raised")
    assert replica.posts == 1 and primary.posts == 0
    db.close()

    primary.shutdown()
    replica.shutdown()
    print("OK: write routing")


if __name__ == "__main__":
    main()
