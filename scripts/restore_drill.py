"""End-to-end backup -> verify -> restore -> query drill.

    python3 scripts/restore_drill.py

Optional:
    NONODB_DAEMON=./target/release/nonodb-daemon
    NONODB_CLI=./target/release/nonodb-cli
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

from nonodb import NonoDB, NonoDBError  # noqa: E402

COLL = "restore_drill"


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
        self.log = log
        env = os.environ.copy()
        env.update(
            NONODB_DATA=str(data),
            NONODB_SOCK=str(sock),
            NONODB_MAP_SIZE="64MiB",
            NONODB_HISTORY="1",
            RUST_LOG="warn",
        )
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


def run_cli(cli: Path, *args: str, parse_json: bool = False):
    proc = subprocess.run(
        [str(cli), *map(str, args)],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"cli {' '.join(args)} failed:\n{proc.stdout}\n{proc.stderr}")
    if parse_json:
        return json.loads(proc.stdout)
    return proc.stdout + proc.stderr


def fixture(db: NonoDB) -> None:
    db.create_collection(COLL, ttl_seconds=3600)
    c = db.collection(COLL)
    c.insert_many(
        [
            {"_id": "u1", "grp": "a", "age": 30, "email": "u1@example.com", "bio": "Rust developer"},
            {"_id": "u2", "grp": "a", "age": 42, "email": "u2@example.com", "bio": "Go developer"},
            {"_id": "u3", "grp": "b", "age": 25, "email": "u3@example.com", "bio": "Rust and SQL"},
        ]
    )
    c.create_index("email", unique=True)
    c.create_index(fields=["grp", "age"])
    c.create_text_index("bio")
    c.create_fuzzy_index("bio")
    c.update({"_id": "u1"}, {"$inc": {"age": 1}})


def check_restored(sock: str) -> None:
    db = NonoDB(sock, timeout=30)
    c = db.collection(COLL)
    assert c.count({}) == 3
    assert [d["_id"] for d in c.find({"age": {"$gte": 31}}, sort=[("age", 1)])] == ["u1", "u2"]
    assert [d["_id"] for d in c.find({"grp": "a"})] == ["u1", "u2"]
    assert [d["_id"] for d in c.search("bio", "rust")] == ["u1", "u3"]
    assert [d["_id"] for d in c.fuzzy("bio", "developper")] == ["u1", "u2"]
    assert db.get_collection_ttl(COLL) == 3600
    assert len(c.history("u1")) >= 2
    try:
        c.insert({"_id": "u4", "email": "u1@example.com"})
    except NonoDBError as exc:
        assert exc.code == "duplicate_key", exc
    else:
        raise AssertionError("unique index was not restored")
    db.close()


def main() -> None:
    daemon = binary("NONODB_DAEMON", "nonodb-daemon")
    cli = binary("NONODB_CLI", "nonodb-cli")

    with tempfile.TemporaryDirectory(prefix="nonodb-restore-drill-") as tmp:
        tmp = Path(tmp)
        source = tmp / "source"
        backup = tmp / "backup"
        restored = tmp / "restored"
        sock1 = tmp / "source.sock"
        sock2 = tmp / "restored.sock"

        d1 = Daemon(daemon, source, sock1, tmp / "source.log")
        try:
            db = NonoDB(str(sock1), timeout=30)
            fixture(db)
            source_count = db.collection(COLL).count({})
            db.close()

            run_cli(cli, "backup", source, backup)
            checked = run_cli(cli, "verify-backup", backup, "--json", parse_json=True)
            assert checked["report"]["documents"] == source_count
            assert checked["report"]["collections"][COLL]["indexes"]
            assert checked["report"]["history_entries"] >= 3
            assert checked["report"]["ttl_entries"] >= 3

            # Manifest hash is also checked before the copy.
            run_cli(cli, "restore", backup, restored, "--verify")
            report = run_cli(cli, "verify", restored, "--json", parse_json=True)
            assert report["documents"] == source_count
            assert report["secondary_index_entries"] == checked["report"]["secondary_index_entries"]
        finally:
            d1.stop()

        d2 = Daemon(daemon, restored, sock2, tmp / "restored.log")
        try:
            check_restored(str(sock2))
        finally:
            d2.stop()

    print("restore drill OK")


if __name__ == "__main__":
    main()
