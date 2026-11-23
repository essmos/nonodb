"""End-to-end PITR: snapshot + segmented WAL, replay, retention and wal_full.

    cargo build -p nonodb-daemon -p nonodb-cli
    python3 scripts/pitr_test.py
"""

from __future__ import annotations

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

from nonodb import NonoDB, NonoDBError  # noqa: E402


def wait_socket(path: Path, proc: subprocess.Popen[bytes], timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if proc.poll() is not None:
            raise RuntimeError("daemon exited before creating its Unix socket")
        time.sleep(0.05)
    raise RuntimeError("timed out waiting for the daemon socket")


def start_daemon(tmp: Path, name: str, extra: dict[str, str]) -> tuple[subprocess.Popen[bytes], Path, Path]:
    data = tmp / f"{name}-data"
    sock = tmp / f"{name}.sock"
    log = (tmp / f"{name}.log").open("wb")
    env = os.environ.copy()
    env.update({"NONODB_DATA": str(data), "NONODB_SOCK": str(sock), "NONODB_LOG": "warn"})
    env.update(extra)
    proc = subprocess.Popen(
        [str(ROOT / "target/debug/nonodb-daemon")],
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    log.close()
    wait_socket(sock, proc)
    return proc, data, sock


def stop_daemon(proc: subprocess.Popen[bytes]) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def wait_for(predicate, timeout: float, label: str):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise RuntimeError(f"timed out waiting for {label}")


def pitr_restore(tmp: Path) -> None:
    extra = {
        "NONODB_PITR_ENABLED": "1",
        "NONODB_SNAPSHOT_DIR": str(tmp / "snapshots"),
        "NONODB_SNAPSHOT_INTERVAL": "1",
        "NONODB_SNAPSHOT_KEEP": "3",
        "NONODB_PITR_WINDOW_SECS": "3600",
        "NONODB_WAL_SEGMENT_BYTES": "4096",
        "NONODB_WAL_MAX_BYTES": "8MiB",
    }
    proc, data, sock = start_daemon(tmp, "pitr", extra)
    db = NonoDB(str(sock), timeout=30)
    try:
        docs = db.collection("docs")
        docs.insert({"_id": "a", "phase": "before"})
        created_a = time.time() * 1000
        snapshots = tmp / "snapshots" / "default"

        def snapshot_after_a():
            for path in snapshots.glob("data-*.mdb"):
                stamp = int(path.stem.split("-", 1)[1])
                if stamp >= created_a and path.with_suffix(".json").exists():
                    return stamp
            return None

        stamp = wait_for(snapshot_after_a, 15, "a snapshot after document a")
        target_ms = int(time.time() * 1000)
        if target_ms < stamp:
            time.sleep((stamp - target_ms) / 1000 + 0.05)
            target_ms = int(time.time() * 1000)
        time.sleep(0.15)
        docs.insert({"_id": "b", "phase": "after"})
        time.sleep(0.8)  # let the WAL archiver fsync b
    finally:
        db.close()
        stop_daemon(proc)

    wal = data / "default" / "wal"
    assert wal.is_dir(), list(data.rglob("*"))
    restored = tmp / "restored"
    result = subprocess.run(
        [
            str(ROOT / "target/debug/nonodb-cli"),
            "restore",
            str(tmp / "snapshots"),
            str(restored),
            "--to",
            str(target_ms),
            "--wal",
            str(wal),
            "--force",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "pitr: replayed" in result.stderr, result.stderr

    restore_proc, _, restore_sock = start_daemon(
        tmp, "restored", {"NONODB_DATA": str(restored)}
    )
    check = NonoDB(str(restore_sock), timeout=30)
    try:
        rows = check.collection("docs").find({}, sort=[("_id", 1)])
        ids = [row["_id"] for row in rows]
        assert "a" in ids, ids
        assert "b" not in ids, ids
        print(f"pitr replay: {ids} target={target_ms} snapshot={stamp}")
    finally:
        check.close()
        stop_daemon(restore_proc)


def startup_replay(tmp: Path) -> None:
    extra = {
        "NONODB_PITR_ENABLED": "1",
        "NONODB_SNAPSHOT_DIR": str(tmp / "startup-snapshots"),
        "NONODB_SNAPSHOT_INTERVAL": "1",
        "NONODB_SNAPSHOT_KEEP": "3",
        "NONODB_PITR_WINDOW_SECS": "3600",
        "NONODB_WAL_SEGMENT_BYTES": "4096",
        "NONODB_WAL_MAX_BYTES": "8MiB",
    }
    proc, data, sock = start_daemon(tmp, "startup", extra)
    db = NonoDB(str(sock), timeout=30)
    try:
        docs = db.collection("docs")
        docs.insert({"_id": "a", "phase": "before"})
        created_a = time.time() * 1000
        snapshots = tmp / "startup-snapshots" / "default"

        def snapshot_path_after_a():
            for path in snapshots.glob("data-*.mdb"):
                stamp = int(path.stem.split("-", 1)[1])
                if stamp >= created_a and path.with_suffix(".json").exists():
                    return path
            return None

        base_snapshot = wait_for(snapshot_path_after_a, 15, "a startup base snapshot")
        target_ms = int(time.time() * 1000)
        time.sleep(0.15)
        docs.insert({"_id": "b", "phase": "after"})
        time.sleep(0.8)
    finally:
        db.close()
        stop_daemon(proc)

    retained = sorted(
        (int(path.stem.split("-", 1)[1]), path)
        for path in snapshots.glob("data-*.mdb")
        if path.with_suffix(".json").exists()
        and int(path.stem.split("-", 1)[1]) <= target_ms
    )
    assert retained, "no retained snapshot at or before the startup replay target"
    base_snapshot = retained[0][1]
    restored = tmp / "startup-restored"
    result = subprocess.run(
        [
            str(ROOT / "target/debug/nonodb-cli"),
            "restore",
            str(base_snapshot),
            str(restored),
            "--force",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    target_db = restored / "default"
    shutil.copytree(data / "default" / "wal", target_db / "wal")

    replay_env = {
        "NONODB_DATA": str(restored),
        "NONODB_PITR_ENABLED": "1",
        "NONODB_PITR_REPLAY_TO": str(target_ms),
        "NONODB_SNAPSHOT_INTERVAL": "3600",
    }
    replay_proc, _, replay_sock = start_daemon(tmp, "startup-replay", replay_env)
    replay_db = NonoDB(str(replay_sock), timeout=30)
    try:
        ids = [row["_id"] for row in replay_db.collection("docs").find({}, sort=[("_id", 1)])]
        assert ids == ["a"], ids
        wait_for(
            lambda: list(target_db.glob("wal.superseded-*")),
            5,
            "the superseded WAL directory",
        )
        print(f"pitr startup replay: {ids} target={target_ms}")
    finally:
        replay_db.close()
        stop_daemon(replay_proc)


def wal_retention(tmp: Path) -> None:
    extra = {
        "NONODB_PITR_ENABLED": "1",
        "NONODB_SNAPSHOT_INTERVAL": "1",
        "NONODB_SNAPSHOT_KEEP": "3",
        "NONODB_PITR_WINDOW_SECS": "1",
        "NONODB_WAL_MAX_AGE_SECS": "3600",
        "NONODB_WAL_SEGMENT_BYTES": "1024",
        "NONODB_WAL_MAX_BYTES": "8MiB",
    }
    proc, data, sock = start_daemon(tmp, "retention", extra)
    db = NonoDB(str(sock), timeout=30)
    try:
        docs = db.collection("docs")
        payload = "x" * 500
        for index in range(30):
            docs.insert({"_id": f"r{index}", "payload": payload})
            time.sleep(0.1)
        wal = data / "default" / "wal"
        before = sum(path.stat().st_size for path in wal.glob("*.wal"))
        wait_for(
            lambda: sum(path.stat().st_size for path in wal.glob("*.wal")) < before,
            8,
            "snapshot-covered WAL pruning",
        )
        snapshots = list((tmp / "retention-snapshots").glob("**/data-*.mdb"))
        # The default snapshot directory is below the database when no
        # NONODB_SNAPSHOT_DIR is configured.
        snapshots.extend((data / "default" / "snapshots").glob("data-*.mdb"))
        print(f"pitr retention: wal {before} -> {sum(p.stat().st_size for p in wal.glob('*.wal'))}")
    finally:
        db.close()
        stop_daemon(proc)


def wal_full(tmp: Path) -> None:
    extra = {
        "NONODB_PITR_ENABLED": "1",
        "NONODB_SNAPSHOT_INTERVAL": "3600",
        "NONODB_SNAPSHOT_KEEP": "2",
        "NONODB_PITR_WINDOW_SECS": "3600",
        "NONODB_WAL_SEGMENT_BYTES": "1024",
        "NONODB_WAL_MAX_BYTES": "4096",
    }
    proc, _, sock = start_daemon(tmp, "full", extra)
    db = NonoDB(str(sock), timeout=30)
    try:
        docs = db.collection("docs")
        payload = "x" * 1500
        error = None
        # The archiver applies the hard limit asynchronously. Keep writing long
        # enough for the limit to take effect instead of relying on a fixed
        # number of fast writes.
        for index in range(500):
            try:
                docs.insert({"_id": f"d{index}", "payload": payload})
            except NonoDBError as exc:
                if exc.code == "wal_full":
                    error = exc
                    break
                raise
            if index % 10 == 0:
                time.sleep(0.01)
        assert error is not None, "WAL reached its hard limit without returning wal_full"
        print(f"pitr wal_full: {error.code}")
    finally:
        db.close()
        stop_daemon(proc)


def main() -> int:
    if not (ROOT / "target/debug/nonodb-daemon").exists():
        print("missing target/debug/nonodb-daemon; cargo build first", file=sys.stderr)
        return 2
    with tempfile.TemporaryDirectory(prefix="nonodb-pitr-") as td:
        tmp = Path(td)
        pitr_restore(tmp)
        startup_replay(tmp)
        wal_retention(tmp)
        wal_full(tmp)
    print("pitr: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
