#!/usr/bin/env python3
"""End-to-end test for `nonodb-cli restore --to <timestamp>`.

Creates three valid snapshots with different document counts, restores the
latest snapshot at or before a requested timestamp, and verifies the result.

    NONODB_DAEMON=target/release/nonodb-daemon \
    NONODB_CLI=target/release/nonodb-cli \
    python3 scripts/restore_to_test.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB  # noqa: E402

COLL = "restore_to"
BASE_MS = 1_700_000_000_000


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
        self.log = log
        env = os.environ.copy()
        env.update(
            NONODB_DATA=str(data),
            NONODB_SOCK=str(sock),
            NONODB_MAP_SIZE="64MiB",
            RUST_LOG="warn",
        )
        self.handle = log.open("wb")
        self.process = subprocess.Popen(
            [str(binary_path)], env=env, stdout=self.handle, stderr=subprocess.STDOUT
        )
        for _ in range(300):
            if sock.exists():
                return
            if self.process.poll() is not None:
                break
            time.sleep(0.05)
        self.stop()
        raise RuntimeError(f"daemon did not start: {log.read_text(errors='replace')[-3000:]}")

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.handle.close()


def run_cli(cli: Path, *args: str) -> str:
    result = subprocess.run(
        [str(cli), *map(str, args)], cwd=ROOT, text=True, capture_output=True
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"nonodb-cli {' '.join(map(str, args))} failed:\n{result.stdout}\n{result.stderr}"
        )
    return result.stdout + result.stderr


def expect_failure(cli: Path, *args: str) -> str:
    result = subprocess.run(
        [str(cli), *map(str, args)], cwd=ROOT, text=True, capture_output=True
    )
    if result.returncode == 0:
        raise AssertionError(f"command unexpectedly succeeded: {args}")
    return result.stdout + result.stderr


def backup(cli: Path, data: Path, out: Path) -> None:
    run_cli(cli, "backup", data, out)


def main() -> int:
    daemon = binary("NONODB_DAEMON", "nonodb-daemon")
    cli = binary("NONODB_CLI", "nonodb-cli")
    with tempfile.TemporaryDirectory(prefix="nonodb-restore-to-") as tmp:
        tmp = Path(tmp)
        source = tmp / "source"
        snapshots = tmp / "snapshots"
        (snapshots / "default").mkdir(parents=True)
        restored = tmp / "restored"
        sock = tmp / "source.sock"
        d = Daemon(daemon, source, sock, tmp / "source.log")
        try:
            db = NonoDB(str(sock), timeout=30)
            db.collection(COLL).insert({"_id": "one", "phase": 1})
            for phase, timestamp in enumerate(
                (BASE_MS, BASE_MS + 1000, BASE_MS + 2000), start=1
            ):
                out = tmp / f"backup-{phase}"
                backup(cli, source, out)
                snapshot = snapshots / "default" / f"data-{timestamp}.mdb"
                snapshot.write_bytes((out / "data.mdb").read_bytes())
                if phase < 3:
                    db.collection(COLL).insert({"_id": f"phase-{phase}", "phase": phase})
            db.close()
        finally:
            d.stop()

        # Select the snapshot at 1.5s: the middle snapshot with two documents.
        run_cli(
            cli,
            "restore",
            "--to",
            str(BASE_MS + 1500),
            snapshots,
            restored,
            "--verify",
        )
        d2 = Daemon(daemon, restored, tmp / "restored.sock", tmp / "restored.log")
        try:
            db = NonoDB(str(tmp / "restored.sock"), timeout=30)
            docs = db.collection(COLL).find({}, sort=[("_id", 1)])
            assert [doc["_id"] for doc in docs] == ["one", "phase-1"], docs
            db.close()
        finally:
            d2.stop()

        expect_failure(
            cli,
            "restore",
            "--to",
            str(BASE_MS - 1),
            snapshots,
            tmp / "too-old",
            "--force",
        )
        expect_failure(
            cli,
            "restore",
            "--to",
            "4102444800000",
            snapshots,
            tmp / "future",
            "--force",
        )

    print("restore --to test: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
