#!/usr/bin/env python3
"""NONODB_TTL_STRICT_READS=0 restores legacy eventual visibility."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB  # noqa: E402


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def daemon_binary() -> Path:
    configured = os.environ.get("NONODB_DAEMON")
    path = Path(configured) if configured else ROOT / "target" / "debug" / "nonodb-daemon"
    if not path.is_file():
        path = ROOT / "target" / "release" / "nonodb-daemon"
    if not path.is_file():
        raise SystemExit(f"missing daemon binary: {path}")
    return path


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="nonodb-ttl-disabled-") as tmp_name:
        tmp = Path(tmp_name)
        sock = tmp / "nonodb.sock"
        env = os.environ.copy()
        env.update(
            {
                "NONODB_DATA": str(tmp / "data"),
                "NONODB_SOCK": str(sock),
                "NONODB_MAP_SIZE": "64MiB",
                "NONODB_TTL_SWEEP_INTERVAL": "3600",
                "NONODB_TTL_STRICT_READS": "0",
                "RUST_LOG": "warn",
            }
        )
        log = (tmp / "daemon.log").open("wb")
        proc = subprocess.Popen(
            [str(daemon_binary())], env=env, stdout=log, stderr=subprocess.STDOUT
        )
        try:
            deadline = time.time() + 20
            while time.time() < deadline and not sock.exists():
                if proc.poll() is not None:
                    raise RuntimeError("daemon exited during startup")
                time.sleep(0.05)
            time.sleep(1.4)
            db = NonoDB(str(sock), timeout=30)
            c = db.collection("ttl_disabled")
            c.insert({"_id": "old", "v": 1, "_ttl": 1})
            time.sleep(1.2)
            assert c.find_one({"_id": "old"})["v"] == 1
            assert c.count({}) == 1
            db.close()
            print("OK: NONODB_TTL_STRICT_READS=0 keeps eventual reads")
            return 0
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            log.close()


if __name__ == "__main__":
    raise SystemExit(main())
