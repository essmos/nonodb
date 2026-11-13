"""End-to-end test for the embedded local GUI server.

    cargo build -p nonodb-daemon -p nonodb-cli
    python3 scripts/gui_test.py
"""

from __future__ import annotations

import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent.parent


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_file(path: Path, proc: subprocess.Popen[bytes], timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if proc.poll() is not None:
            raise RuntimeError("daemon exited before creating its socket")
        time.sleep(0.05)
    raise RuntimeError("timed out waiting for daemon socket")


def wait_http(url: str, proc: subprocess.Popen[bytes], timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("GUI server exited during startup")
        try:
            with urlopen(url, timeout=0.5) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("timed out waiting for GUI HTTP server")


def fetch(url: str, *, data: dict | None = None) -> bytes:
    body = None if data is None else json.dumps(data).encode()
    request = Request(url, data=body, headers={"content-type": "application/json"})
    with urlopen(request, timeout=10) as response:
        assert response.status == 200, (url, response.status)
        return response.read()


def main() -> int:
    daemon = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
    cli = Path(os.environ.get("NONODB_CLI", ROOT / "target/debug/nonodb-cli"))
    if not daemon.exists() or not cli.exists():
        print(f"missing binaries: {daemon} / {cli}", file=sys.stderr)
        return 2
    with tempfile.TemporaryDirectory(prefix="nonodb-gui-") as td:
        tmp = Path(td)
        sock = tmp / "nonodb.sock"
        port = free_port()
        env = os.environ.copy()
        env.update(
            NONODB_DATA=str(tmp / "data"),
            NONODB_SOCK=str(sock),
            NONODB_LOG="warn",
        )
        daemon_log = (tmp / "daemon.log").open("wb")
        daemon_proc = subprocess.Popen(
            [str(daemon)], env=env, stdout=daemon_log, stderr=subprocess.STDOUT
        )
        daemon_log.close()
        gui_env = env.copy()
        gui_proc = None
        try:
            wait_file(sock, daemon_proc)
            gui_proc = subprocess.Popen(
                [str(cli), "gui", "--port", str(port), "--no-open"],
                env=gui_env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            base = f"http://127.0.0.1:{port}"
            wait_http(base + "/", gui_proc)

            index = fetch(base + "/").decode()
            assert '<div id="root"></div>' in index, index[:200]
            match = re.search(r'src="([^"]+\.js)"', index)
            assert match, index[:500]
            asset = fetch(base + match.group(1))
            assert len(asset) > 1000
            for marker in (
                b"Save & run now",
                b"Select one or more databases",
                b"Backup type",
                b"Incremental + deduplication",
                b"Where",
                b"Compression",
                b"Gzip",
                b"Backup disclaimer",
                b"as-is, without warranty",
                b"I understand and accept",
                b"Browse backups",
                b"Verify & import as new database",
                b"/v1/import/browse",
                b"Excluded collections",
                b"Generate key",
                b"/v1/backups/keygen",
            ):
                assert marker in asset, marker
            assert b"New database name" not in asset

            result = json.loads(fetch(base + "/v1/exec", data={"op": "list_collections"}))
            assert result["ok"] is True, result
            fetch(base + "/v1/exec", data={"op": "create_collection", "coll": "gui_orders"})
            fetch(
                base + "/v1/exec",
                data={"op": "insert", "coll": "gui_orders", "doc": {"_id": "o1", "n": 1}},
            )
            result = json.loads(
                fetch(
                    base + "/v1/exec",
                    data={"op": "find", "coll": "gui_orders", "filter": {}},
                )
            )
            assert result["result"] == [{"_id": "o1", "n": 1}], result

            health = json.loads(fetch(base + "/healthz"))
            assert health, health
            ready = json.loads(fetch(base + "/readyz"))
            assert ready["ok"] is True, ready
            backups = json.loads(fetch(base + "/v1/backups"))
            assert "jobs" in backups["result"], backups
            try:
                fetch(base + "/v1/auth")
            except HTTPError as exc:
                # Authorization is intentionally disabled in this test. The
                # point is that the GUI proxy does not return its 501 fallback.
                assert exc.code == 400, exc.code
                assert exc.headers.get("content-type", "").startswith("application/json")
            else:
                raise AssertionError("auth endpoint unexpectedly succeeded without auth file")

            fetch(base + "/v1/exec", data={"op": "create_database", "name": "app1"})
            fetch(
                base + "/app1/v1/exec",
                data={"op": "insert", "coll": "gui_orders", "doc": {"_id": "a1", "n": 2}},
            )
            result = json.loads(
                fetch(
                    base + "/app1/v1/exec",
                    data={"op": "find", "coll": "gui_orders", "filter": {}},
                )
            )
            assert result["result"] == [{"_id": "a1", "n": 2}], result
            result = json.loads(
                fetch(
                    base + "/v1/exec",
                    data={"op": "find", "coll": "gui_orders", "filter": {}},
                )
            )
            assert result["result"] == [{"_id": "o1", "n": 1}], result
            print("gui e2e: ok")
            return 0
        finally:
            for proc in (gui_proc, daemon_proc):
                if proc is None:
                    continue
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())
