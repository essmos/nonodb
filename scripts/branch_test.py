#!/usr/bin/env python3
"""End-to-end test for `nonodb-cli branch`.

Creates a database, stops the daemon, branches it from the stopped database and
from a snapshot file, starts the daemon again, and verifies data, indexes,
sidecars and write isolation.

    NONODB_DAEMON=target/release/nonodb-daemon \
    NONODB_CLI=target/release/nonodb-cli \
    python3 scripts/branch_test.py
"""

from __future__ import annotations

import hashlib
import json
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

COLL = "items"
SOURCE = "source"


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


def branch_json(cli: Path, *args: str) -> dict:
    output = run_cli(cli, "branch", *args, "--json")
    # The JSON document is the last object printed; the command may also emit
    # a warning on stderr, which run_cli combines into stdout.
    start = output.find("{")
    if start < 0:
        raise AssertionError(f"no JSON in branch output: {output}")
    return json.loads(output[start:])


def main() -> int:
    daemon = binary("NONODB_DAEMON", "nonodb-daemon")
    cli = binary("NONODB_CLI", "nonodb-cli")
    with tempfile.TemporaryDirectory(prefix="nonodb-branch-") as tmp:
        tmp = Path(tmp)
        data = tmp / "data"
        sock = tmp / "source.sock"

        help_text = run_cli(cli, "branch", "--help")
        assert "branch <target-db>" in help_text, help_text
        expect_failure(cli, "branch", "bad/name", "--data", str(data))
        empty = tmp / "empty"
        empty.mkdir()
        expect_failure(cli, "branch", "nope", "--data", str(empty))
        assert not (empty / "nope").exists()

        d = Daemon(daemon, data, sock, tmp / "source.log")
        try:
            admin = NonoDB(str(sock), timeout=30)
            admin.exec({"op": "create_database", "name": SOURCE})
            db = NonoDB(str(sock), database=SOURCE, timeout=30)
            collection = db.collection(COLL)
            collection.insert_many(
                [
                    {"_id": str(i), "n": i, "bio": "rust developer", "vec": [i, 1]}
                    for i in range(5)
                ]
            )
            collection.create_index("n")
            collection.create_text_index("bio")
            collection.create_fuzzy_index("bio")
            collection.create_vector_index("vec", 2, "cosine")
            db.close()
            admin.close()
        finally:
            d.stop()

        source_data = data / "source" / "data.mdb"
        source_hash = hashlib.sha256(source_data.read_bytes()).hexdigest()
        output = branch_json(
            cli, "branch", "--db", SOURCE, "--data", str(data), "--verify"
        )
        assert output["target"] == "branch", output
        assert output["method"] in {"reflink", "clonefile", "copy"}, output
        assert output["bytes"] > 0
        assert output["elapsed_ms"] >= 0
        if output["method"] == "copy":
            assert "reflink unavailable" in output["warning"], output
        assert output["verification"]["documents"] == 5, output
        assert output["verification"]["path"] == str(data / "branch"), output
        assert (data / "branch" / "search").is_dir(), output
        assert not (data / "branch" / "lock.mdb").exists(), output

        expect_failure(
            cli, "branch", "branch", "--db", SOURCE, "--data", str(data)
        )
        branch_json(cli, "branch", "--db", SOURCE, "--data", str(data), "--force", "--verify")
        no_sidecars = branch_json(
            cli,
            "nosidecars",
            "--db",
            SOURCE,
            "--data",
            str(data),
            "--no-sidecars",
            "--verify",
        )
        assert no_sidecars["sidecars"] == [], no_sidecars
        assert no_sidecars["verification"]["documents"] == 5, no_sidecars

        # Build the real daemon snapshot shape: only data-<ms>.mdb, no sidecars.
        backup = tmp / "backup"
        run_cli(cli, "--db", SOURCE, "backup", data, backup)
        snap_root = tmp / "snapshots" / SOURCE
        snap_root.mkdir(parents=True)
        snapshot = snap_root / "data-1700000000000.mdb"
        shutil.copy2(backup / "data.mdb", snapshot)
        snapshot.chmod(0o444)
        snapshot_output = branch_json(
            cli, "snapshot", "--from-snapshot", str(snapshot), "--data", str(data), "--verify"
        )
        assert snapshot_output["verification"]["documents"] == 5, snapshot_output
        assert snapshot_output["sidecars"] == ["search"], snapshot_output
        assert snapshot_output["sidecar_rebuilds"] >= 2, snapshot_output
        assert (data / "snapshot" / "search").is_dir(), snapshot_output
        assert hashlib.sha256(source_data.read_bytes()).hexdigest() == source_hash

        d2 = Daemon(daemon, data, tmp / "verify.sock", tmp / "verify.log")
        try:
            source_db = NonoDB(str(tmp / "verify.sock"), database=SOURCE, timeout=30)
            branch_db = NonoDB(str(tmp / "verify.sock"), database="branch", timeout=30)
            snapshot_db = NonoDB(str(tmp / "verify.sock"), database="snapshot", timeout=30)
            assert source_db.collection(COLL).count() == 5
            assert branch_db.collection(COLL).count() == 5
            assert snapshot_db.collection(COLL).count() == 5
            expected_ids = ["0", "1", "2", "3", "4"]
            assert [
                d["_id"] for d in branch_db.collection(COLL).search("bio", "rust")
            ] == expected_ids
            assert [
                d["_id"] for d in snapshot_db.collection(COLL).search("bio", "rust")
            ] == expected_ids
            assert len(snapshot_db.collection(COLL).fuzzy("bio", "rust")) == 5
            hits = snapshot_db.collection(COLL).vector_search("vec", [0, 1], limit=3)
            assert hits and hits[0]["_id"] == "0", hits
            branch_db.collection(COLL).insert({"_id": "branch-only", "n": 99})
            assert branch_db.collection(COLL).count() == 6
            assert source_db.collection(COLL).count() == 5
            assert snapshot_db.collection(COLL).count() == 5
            source_db.close()
            branch_db.close()
            snapshot_db.close()
        finally:
            d2.stop()

    print("branch test: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
