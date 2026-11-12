"""End-to-end per-collection document history test.

    cargo build -p nonodb-daemon -p nonodb-cli
    python3 scripts/history_test.py
"""

from __future__ import annotations

import os
import shutil
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


def start(daemon: Path, env: dict[str, str], log: Path) -> subprocess.Popen[bytes]:
    fh = log.open("ab")
    proc = subprocess.Popen([str(daemon)], env=env, stdout=fh, stderr=subprocess.STDOUT)
    fh.close()
    wait_for_socket(Path(env["NONODB_SOCK"]), proc)
    return proc


def stop(proc: subprocess.Popen[bytes]) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def main() -> int:
    daemon = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
    if not daemon.exists():
        print(f"missing daemon binary: {daemon}", file=sys.stderr)
        return 2
    tmp = Path(tempfile.mkdtemp(prefix="nonodb-history-"))
    sock = tmp / "nonodb.sock"
    log = tmp / "daemon.log"
    env = os.environ.copy()
    env.update(
        {
            "NONODB_DATA": str(tmp / "data"),
            "NONODB_SOCK": str(sock),
            "NONODB_LOG": "warn",
        }
    )
    proc = start(daemon, env, log)
    try:
        db = NonoDB(str(sock), cache_ttl=0.0)
        try:
            db.create_collection("hist_a")
            db.create_collection("hist_b")
            db.create_collection("hist_off")
            assert db.get_collection_history("hist_a") == 0
            assert db.get_collection_history("hist_b") == 0

            off_id = db.collection("hist_off").insert({"_id": "off", "n": 0})
            assert db.collection("hist_off").history(off_id) == []

            db.set_collection_history("hist_a", 3)
            db.set_collection_history("hist_b", 1)
            assert db.get_collection_history("hist_a") == 3
            assert db.get_collection_history("hist_b") == 1

            a = db.collection("hist_a")
            a_id = a.insert({"_id": "a1", "n": 0})
            for n in range(1, 4):
                a.update({"_id": a_id}, {"$set": {"n": n}})
            assert len(a.history(a_id, include_docs=True)) == 4
            assert a.history(a_id, include_docs=True)[0]["doc"]["n"] == 3

            b = db.collection("hist_b")
            b_id = b.insert({"_id": "b1", "n": 0})
            b.update({"_id": b_id}, {"$set": {"n": 1}})
            assert len(b.history(b_id, include_docs=True)) == 2

            db.set_collection_history("hist_a", 0)
            a.update({"_id": a_id}, {"$set": {"n": 99}})
            assert len(a.history(a_id, include_docs=True)) == 4
            assert a.history(a_id, include_docs=True)[0]["doc"]["n"] == 3
        finally:
            db.close()

        stop(proc)
        proc = start(daemon, env, log)
        db = NonoDB(str(sock), cache_ttl=0.0)
        try:
            assert db.get_collection_history("hist_a") == 0
            assert db.get_collection_history("hist_b") == 1
            assert len(db.collection("hist_a").history("a1")) == 4
            assert len(db.collection("hist_b").history("b1")) == 2
            db.collection("hist_b").delete({"_id": "b1"})
            revs = db.collection("hist_b").history("b1")
            assert revs[0]["op"] == "delete", revs
        finally:
            db.close()

        cli = ROOT / "target/debug/nonodb-cli"
        if cli.exists():
            cli_env = os.environ.copy()
            cli_env["NONODB_SOCK"] = str(sock)
            subprocess.run(
                [str(cli), "history", "hist_b", "2"],
                env=cli_env,
                check=True,
                stdout=subprocess.DEVNULL,
            )
            out = subprocess.run(
                [str(cli), "history", "hist_b"],
                env=cli_env,
                check=True,
                text=True,
                capture_output=True,
            ).stdout
            assert '"max_versions": 2' in out, out

        print("OK: per-collection history defaults, enable/disable, isolation, restart, CLI")
        return 0
    finally:
        stop(proc)
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
