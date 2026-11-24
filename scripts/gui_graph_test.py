"""Browser test for the Graph tab's lazy tree.

Requires Playwright and a Chromium install:

    python3 -m pip install playwright
    python3 -m playwright install chromium
    cargo build -p nonodb-daemon -p nonodb-cli
    python3 scripts/gui_graph_test.py
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
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent.parent


def free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
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


def wait_http(url: str, proc: subprocess.Popen[bytes], timeout: float = 20.0) -> None:
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


def post(base: str, payload: dict) -> dict:
    request = Request(
        base + "/v1/exec",
        data=json.dumps(payload).encode(),
        headers={"content-type": "application/json"},
    )
    with urlopen(request, timeout=10) as response:
        return json.loads(response.read())


def main() -> int:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("playwright is not installed; skipping GUI graph test", file=sys.stderr)
        return 2

    daemon = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
    cli = Path(os.environ.get("NONODB_CLI", ROOT / "target/debug/nonodb-cli"))
    if not daemon.exists() or not cli.exists():
        print(f"missing binaries: {daemon} / {cli}", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="nonodb-gui-graph-") as td:
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
        gui_proc = None
        try:
            wait_file(sock, daemon_proc)
            base = f"http://127.0.0.1:{port}"
            gui_proc = subprocess.Popen(
                [str(cli), "gui", "--port", str(port), "--no-open"],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            wait_http(base + "/", gui_proc)

            for coll in ("gui_nodes", "gui_edges"):
                assert post(base, {"op": "create_collection", "coll": coll})["ok"]
            for doc in (
                {"_id": "a", "name": "A"},
                {"_id": "b", "name": "B"},
                {"_id": "c", "name": "C"},
            ):
                assert post(base, {"op": "insert", "coll": "gui_nodes", "doc": doc})["ok"]
            for edge in (
                {"_id": "e1", "from": "a", "to": "b", "label": "KNOWS"},
                {"_id": "e2", "from": "b", "to": "c", "label": "KNOWS"},
            ):
                assert post(base, {"op": "insert", "coll": "gui_edges", "doc": edge})["ok"]

            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(base + "/", wait_until="networkidle")
                page.get_by_role("button", name="Graph", exact=True).click()
                page.get_by_text("Graph definition").wait_for()

                page.get_by_label("Node collection").select_option(label="gui_nodes")
                page.get_by_label("Edge collection").select_option(label="gui_edges")
                page.get_by_role("button", name="Configure graph").click()
                page.get_by_text("Graph configured and adjacency rebuilt.").wait_for()

                page.get_by_label("Start ids (comma separated)").fill("a")
                page.get_by_role("button", name="Traverse", exact=True).click()

                def code_nodes():
                    return page.locator("code").filter(has_text=re.compile(r"^(a|b|c)$"))

                page.wait_for_function(
                    """() => Array.from(document.querySelectorAll('code'))
                        .some((node) => node.textContent === 'b')"""
                )
                assert code_nodes().count() >= 2, "a and b should be visible"
                assert not any(
                    code_nodes().nth(index).inner_text() == "c"
                    for index in range(code_nodes().count())
                ), "c must stay hidden until b is expanded"

                b_code = page.locator("code").filter(has_text=re.compile(r"^b$")).first
                b_code.locator("xpath=..").get_by_role("button").click()
                page.wait_for_function(
                    """() => Array.from(document.querySelectorAll('code'))
                        .some((node) => node.textContent === 'c')"""
                )
                assert code_nodes().count() >= 3, "lazy expansion should reveal c"
                browser.close()
            print("gui graph test passed")
            return 0
        finally:
            if gui_proc is not None:
                gui_proc.terminate()
                try:
                    gui_proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    gui_proc.kill()
            daemon_proc.terminate()
            try:
                daemon_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                daemon_proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())
