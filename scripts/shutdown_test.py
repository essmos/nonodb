"""End-to-end graceful shutdown/drain and JSON log test.

    python3 scripts/shutdown_test.py

Optional:
    NONODB_DAEMON=./target/release/nonodb-daemon
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB  # noqa: E402

COLL = "shutdown_drill"
DOCS = 20_000


def binary(env: str, name: str) -> Path:
    raw = os.environ.get(env)
    if raw:
        path = Path(raw)
    else:
        path = next(
            (p for p in (ROOT / "target/debug" / name, ROOT / "target/release" / name) if p.is_file()),
            ROOT / "target/debug" / name,
        )
    if not path.is_file():
        raise SystemExit(f"missing binary: {path}")
    return path


class Daemon:
    def __init__(self, daemon: Path, data: Path, sock: Path, log: Path):
        env = os.environ.copy()
        env.update(
            NONODB_DATA=str(data),
            NONODB_SOCK=str(sock),
            NONODB_MAP_SIZE="64MiB",
            NONODB_DRAIN_TIMEOUT="5",
            NONODB_LOG_FORMAT="json",
            RUST_LOG="info",
        )
        self.log = log
        self.fh = log.open("wb")
        self.proc = subprocess.Popen([str(daemon)], env=env, stdout=self.fh, stderr=subprocess.STDOUT)
        for _ in range(200):
            if sock.exists():
                return
            if self.proc.poll() is not None:
                break
            time.sleep(0.05)
        self.stop()
        raise RuntimeError(f"daemon did not start; log:\n{log.read_text(errors='replace')[-4000:]}")

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.fh.close()


def main() -> None:
    daemon = binary("NONODB_DAEMON", "nonodb-daemon")
    with tempfile.TemporaryDirectory() as tmp:
        data = Path(tmp) / "data"
        sock = Path(tmp) / "nonodb.sock"
        log = Path(tmp) / "daemon.log"
        proc = Daemon(daemon, data, sock, log)
        db = NonoDB(str(sock), timeout=60)
        coll = db.collection(COLL)
        coll.delete({}, multi=True)

        # A keep-alive connection with no in-flight request must not block drain.
        idle = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        idle.connect(str(sock))

        result: dict[str, object] = {}

        def atomic_write() -> None:
            try:
                result["value"] = db.write_batch(
                    [
                        {
                            "op": "insert_many",
                            "coll": COLL,
                            "docs": [{"i": i, "payload": "x" * 32} for i in range(DOCS)],
                        }
                    ]
                )
            except BaseException as exc:  # noqa: BLE001 - propagate to main thread
                result["error"] = exc

        writer = threading.Thread(target=atomic_write)
        writer.start()
        # Let the request reach the handler/writer so shutdown has real work to drain.
        time.sleep(0.1)

        started = time.monotonic()
        proc.proc.send_signal(signal.SIGTERM)
        proc.proc.wait(timeout=10)
        elapsed = time.monotonic() - started
        proc.fh.close()
        writer.join(timeout=10)

        assert not writer.is_alive(), "client request did not finish during drain"
        assert "error" not in result, result.get("error")
        assert len(result["value"]) == 1, result  # type: ignore[index]
        assert elapsed < 8, f"shutdown took {elapsed:.2f}s"
        assert not sock.exists(), "socket was not removed"
        idle.settimeout(1)
        try:
            assert idle.recv(1) == b""
        except OSError:
            pass
        idle.close()
        db.close()

        lines = [line for line in log.read_text(errors="replace").splitlines() if line.strip()]
        assert lines, "daemon produced no logs"
        for line in lines:
            json.loads(line)

        # Restart on the same data: the atomic batch must be fully durable.
        proc2 = Daemon(daemon, data, sock, Path(tmp) / "daemon2.log")
        db2 = NonoDB(str(sock), timeout=60)
        try:
            assert db2.collection(COLL).count({}) == DOCS
        finally:
            db2.close()
            proc2.stop()

    print("shutdown OK")


if __name__ == "__main__":
    main()
