"""End-to-end async watch/stream client tests.

    cargo build -p nonodb-daemon
    python3 scripts/async_watch_test.py
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))

from nonodb import AsyncNonoDB, NonoDB, StreamTruncatedError  # noqa: E402


def wait_for_socket(path: Path, proc: subprocess.Popen[bytes], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if proc.poll() is not None:
            raise RuntimeError("daemon exited before creating its socket")
        time.sleep(0.05)
    raise RuntimeError("timed out waiting for daemon socket")


async def run_checks(sock: str) -> None:
    sync = NonoDB(sock)
    try:
        sync.create_collection("async_events")
        events = sync.collection("async_events")
        events.insert({"_id": "01", "type": "a", "payload": {"i": 1}})
        events.insert({"_id": "02", "type": "b", "payload": {"i": 2}})
        events.insert({"_id": "03", "type": "b", "payload": {"i": 3}})
        sync.create_collection("async_orders")
        orders = sync.collection("async_orders")

        async with AsyncNonoDB(sock) as db:
            async_events = db.collection("async_events")

            batch = await async_events.watch_once(limit=10, timeout_ms=100)
            assert [event["_id"] for event in batch["events"]] == [
                "01",
                "02",
                "03",
            ], batch
            assert batch["next"] == "03", batch

            got: list[str] = []
            generator = async_events.watch(
                after="01", limit=2, timeout_ms=100, retry=False
            )
            try:
                async for event in generator:
                    got.append(event["_id"])
                    if len(got) == 2:
                        break
            finally:
                await generator.aclose()
            assert got == ["02", "03"], got

            got = []
            stream = async_events.watch_stream(
                after="01",
                timeout_ms=300,
                heartbeat_ms=100,
                retry=False,
            )
            try:
                async for event in stream:
                    got.append(event["_id"])
            finally:
                await stream.aclose()
            assert got == ["02", "03"], got

            truncated = await async_events.watch_once(
                after="0", limit=1, timeout_ms=100
            )
            assert truncated["truncated"] is True, truncated
            assert truncated["oldest"] == "01", truncated

            generator = async_events.watch(
                after="0", limit=1, timeout_ms=100, retry=False
            )
            try:
                await generator.__anext__()
            except StreamTruncatedError as exc:
                assert exc.oldest == "01", exc
            else:
                raise AssertionError("truncated watch should raise")
            finally:
                await generator.aclose()

            stream = async_events.watch_stream(
                after="0", timeout_ms=100, retry=False
            )
            try:
                await stream.__anext__()
            except StreamTruncatedError as exc:
                assert exc.oldest == "01", exc
            else:
                raise AssertionError("truncated stream should raise")
            finally:
                await stream.aclose()

            live = db.collection("async_orders").watch_live(
                filter={"status": "paid"},
                heartbeat_ms=200,
                retry=False,
            )
            pending = asyncio.create_task(live.__anext__())
            await asyncio.sleep(0.2)
            await asyncio.to_thread(
                orders.insert,
                {"_id": "o1", "status": "paid", "total": 10},
            )
            event = await asyncio.wait_for(pending, 5)
            assert event["type"] == "subscription.live", event
            assert event["payload"]["after"]["_id"] == "o1", event
            await live.aclose()
    finally:
        sync.close()


def main() -> int:
    daemon = Path(
        os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon")
    )
    if not daemon.exists():
        print(f"missing daemon binary: {daemon}", file=sys.stderr)
        return 2

    tmp = Path(tempfile.mkdtemp(prefix="nonodb-async-watch-"))
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
    fh = log.open("wb")
    proc = subprocess.Popen(
        [str(daemon)], env=env, stdout=fh, stderr=subprocess.STDOUT
    )
    fh.close()
    try:
        wait_for_socket(sock, proc)
        asyncio.run(run_checks(str(sock)))
        print("async watch e2e: ok")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        if proc.returncode not in (0, -15):
            print(log.read_text(errors="replace"), file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
