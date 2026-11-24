#!/usr/bin/env python3
"""Run a read-only subset of the pinned openCypher TCK.

Baseline selection: scenarios with `Given any graph`, no setup writes, no
parameters, and a result table. This is intentionally narrow; the report is a
machine-readable baseline, not a claim of full TCK conformance.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))
from nonodb import NonoDB  # noqa: E402

TCK_COMMIT = "677cbafabb8c3c5eed458fd3b1ec0daec8d67d23"


@dataclass
class Step:
    keyword: str
    text: str
    docstring: str | None = None
    table: list[list[str]] = field(default_factory=list)


@dataclass
class Scenario:
    feature: str
    name: str
    steps: list[Step]
    line: int
    outline: bool = False


def parse_feature(path: Path) -> list[Scenario]:
    scenarios: list[Scenario] = []
    current: Scenario | None = None
    step: Step | None = None
    docstring: list[str] | None = None
    feature = path.stem
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("Feature:"):
            feature = stripped.split(":", 1)[1].strip()
            continue
        if stripped.startswith("Scenario:") or stripped.startswith("Scenario Outline:"):
            current = Scenario(
                feature,
                stripped.split(":", 1)[1].strip(),
                [],
                number,
                stripped.startswith("Scenario Outline:"),
            )
            scenarios.append(current)
            step = None
            continue
        if current is None:
            continue
        if docstring is not None:
            if stripped == '"""':
                assert step is not None
                step.docstring = "\n".join(docstring)
                docstring = None
            else:
                docstring.append(line)
            continue
        if stripped == '"""':
            docstring = []
            continue
        if stripped.startswith("|"):
            if step is not None:
                cells = [cell.strip() for cell in split_table(stripped)]
                step.table.append(cells)
            continue
        match = re.match(r"^(Given|When|Then|And|But)\s+(.*)$", stripped)
        if match:
            keyword, text = match.groups()
            if keyword in ("And", "But") and step is not None:
                keyword = step.keyword
            step = Step(keyword, text)
            current.steps.append(step)
    return scenarios


def split_table(line: str) -> list[str]:
    body = line.strip()
    if body.startswith("|"):
        body = body[1:]
    if body.endswith("|"):
        body = body[:-1]
    cells: list[str] = []
    current: list[str] = []
    escaped = False
    for char in body:
        if escaped:
            current.append(char)
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == "|":
            cells.append("".join(current))
            current = []
        else:
            current.append(char)
    cells.append("".join(current))
    return cells


def select(scenario: Scenario) -> tuple[bool, str]:
    texts = [step.text for step in scenario.steps]
    if scenario.name.startswith("["):
        pass
    if not any(text == "any graph" or text.startswith("any graph") for text in texts):
        return False, "not_any_graph"
    if any("having executed" in text for text in texts):
        return False, "setup_write"
    if any("parameters are" in text for text in texts):
        return False, "parameters"
    if not any(text == "executing query:" for text in texts):
        return False, "no_query"
    if any("should be raised" in text for text in texts) or any(
        "should raise" in text for text in texts
    ):
        return False, "error_scenario"
    if not any("should be, in" in text for text in texts):
        return False, "no_result_table"
    query = scenario_query(scenario) or ""
    if scenario.outline or "<" in query:
        return False, "outline"
    if re.search(
        r"\b(CREATE|MERGE|SET|DELETE|REMOVE|FOREACH|LOAD\s+CSV|DROP|CALL|START)\b",
        query,
        re.IGNORECASE,
    ):
        return False, "write_or_procedure"
    return True, "selected"


def scenario_query(scenario: Scenario) -> str | None:
    for step in scenario.steps:
        if step.text == "executing query:" and step.docstring is not None:
            return step.docstring.strip()
    return None


def expected(scenario: Scenario) -> tuple[bool, list[str], list[list[str]]] | None:
    for step in scenario.steps:
        if step.text.startswith("the result should be, in"):
            ordered = "in order" in step.text
            if not step.table:
                return ordered, [], []
            return ordered, step.table[0], step.table[1:]
    return None


class LiteralParser:
    def __init__(self, text: str):
        self.text = text
        self.pos = 0

    def parse(self):
        value = self.value()
        self.ws()
        if self.pos != len(self.text):
            raise ValueError(f"trailing input: {self.text[self.pos:]}")
        return value

    def ws(self):
        while self.pos < len(self.text) and self.text[self.pos].isspace():
            self.pos += 1

    def peek(self):
        return self.text[self.pos] if self.pos < len(self.text) else ""

    def value(self):
        self.ws()
        if self.peek() == "(":
            return self.node()
        if self.peek() == "[" and self.text[self.pos : self.pos + 3] == "[: ":
            return self.relationship()
        if self.peek() == "[":
            if self.text[self.pos : self.pos + 2] in ("[:", "[-"):
                return self.relationship()
            return self.list()
        if self.peek() == "{":
            return self.map()
        if self.peek() == "'":
            return self.string()
        if self.text.startswith("true", self.pos):
            self.pos += 4
            return True
        if self.text.startswith("false", self.pos):
            self.pos += 5
            return False
        if self.text.startswith("null", self.pos):
            self.pos += 4
            return None
        number = re.match(r"-?(?:\d+\.\d+|\d+)(?:[eE][+-]?\d+)?", self.text[self.pos :])
        if number:
            self.pos += len(number.group(0))
            text = number.group(0)
            return float(text) if "." in text or "e" in text.lower() else int(text)
        word = re.match(r"[A-Za-z_][A-Za-z0-9_]*", self.text[self.pos :])
        if word:
            self.pos += len(word.group(0))
            return word.group(0)
        raise ValueError(f"cannot parse: {self.text[self.pos:]}")

    def string(self):
        quote = self.text[self.pos]
        self.pos += 1
        out = []
        while self.pos < len(self.text):
            char = self.text[self.pos]
            self.pos += 1
            if char == "\\" and self.pos < len(self.text):
                out.append(self.text[self.pos])
                self.pos += 1
            elif char == quote:
                return "".join(out)
            else:
                out.append(char)
        raise ValueError("unterminated string")

    def list(self):
        self.pos += 1
        out = []
        while True:
            self.ws()
            if self.peek() == "]":
                self.pos += 1
                return out
            out.append(self.value())
            self.ws()
            if self.peek() == ",":
                self.pos += 1
                continue
            if self.peek() == "]":
                self.pos += 1
                return out
            raise ValueError("bad list")

    def map(self):
        self.pos += 1
        out = {}
        while True:
            self.ws()
            if self.peek() == "}":
                self.pos += 1
                return out
            key = self.string() if self.peek() == "'" else self.value()
            self.ws()
            if self.peek() != ":":
                raise ValueError("bad map")
            self.pos += 1
            out[str(key)] = self.value()
            self.ws()
            if self.peek() == ",":
                self.pos += 1
                continue
            if self.peek() == "}":
                self.pos += 1
                return out
            raise ValueError("bad map")

    def node(self):
        self.pos += 1
        labels = []
        props = {}
        while True:
            self.ws()
            if self.peek() == ":":
                self.pos += 1
                name = re.match(r"[A-Za-z_][A-Za-z0-9_]*", self.text[self.pos :])
                if not name:
                    raise ValueError("bad label")
                labels.append(name.group(0))
                self.pos += len(name.group(0))
            elif self.peek() == "{":
                props = self.map()
            elif self.peek() == ")":
                self.pos += 1
                return {"__type": "node", "labels": labels, "properties": props}
            else:
                raise ValueError("bad node")

    def relationship(self):
        self.pos += 1
        if self.peek() == ":":
            self.pos += 1
        rel_type = ""
        name = re.match(r"[A-Za-z_][A-Za-z0-9_|]*", self.text[self.pos :])
        if name:
            rel_type = name.group(0)
            self.pos += len(rel_type)
        props = {}
        while True:
            self.ws()
            if self.peek() == "{":
                props = self.map()
            elif self.peek() == "]":
                self.pos += 1
                return {"__type": "rel", "type": rel_type, "properties": props}
            else:
                raise ValueError("bad relationship")


def parse_cell(text: str):
    text = text.strip()
    if text == "":
        return None
    return LiteralParser(text).parse()


def normalize_actual(value: Any):
    if isinstance(value, dict) and "nodes" in value and "relationships" in value:
        return {
            "__type": "path",
            "nodes": [normalize_actual(item) for item in value.get("nodes", [])],
            "relationships": [normalize_actual(item) for item in value.get("relationships", [])],
        }
    if isinstance(value, dict) and "_type" in value:
        return {"__type": "rel", "type": value.get("_type"), "properties": clean_props(value)}
    if isinstance(value, dict) and "_labels" in value:
        return {"__type": "node", "labels": value.get("_labels", []), "properties": clean_props(value)}
    if isinstance(value, list):
        return [normalize_actual(item) for item in value]
    return value


def clean_props(value: dict) -> dict:
    return {
        key: normalize_actual(item)
        for key, item in value.items()
        if not key.startswith("_") and key not in ("from", "to")
    }


def match_value(actual: Any, expected: Any) -> bool:
    if isinstance(expected, dict) and expected.get("__type") == "node":
        return (
            isinstance(actual, dict)
            and set(expected.get("labels", [])) == set(actual.get("labels", []))
            and all(
                key in actual.get("properties", {})
                and actual["properties"][key] == value
                for key, value in expected.get("properties", {}).items()
            )
        )
    if isinstance(expected, dict) and expected.get("__type") == "rel":
        return (
            isinstance(actual, dict)
            and expected.get("type") in (actual.get("type"), "")
            and all(
                key in actual.get("properties", {})
                and actual["properties"][key] == value
                for key, value in expected.get("properties", {}).items()
            )
        )
    if isinstance(expected, list) and isinstance(actual, list):
        return len(expected) == len(actual) and all(
            match_value(a, e) for a, e in zip(actual, expected)
        )
    if isinstance(expected, dict) and isinstance(actual, dict):
        return set(expected) <= set(actual) and all(
            match_value(actual[key], value) for key, value in expected.items()
        )
    if isinstance(expected, float) and isinstance(actual, (int, float)):
        return abs(float(actual) - expected) < 1e-9
    return actual == expected


def compare_rows(actual_rows: list[dict], headers: list[str], rows: list[list[str]], ordered: bool) -> tuple[bool, str]:
    expected_rows = [{header: parse_cell(cell) for header, cell in zip(headers, row)} for row in rows]
    normalized = [
        {key: normalize_actual(value) for key, value in row.items()} for row in actual_rows
    ]
    if len(normalized) != len(expected_rows):
        return False, f"row count {len(normalized)} != {len(expected_rows)}"
    if ordered:
        for index, (actual, expected_row) in enumerate(zip(normalized, expected_rows)):
            if not match_value(actual, expected_row):
                return False, f"row {index}: {actual!r} != {expected_row!r}"
        return True, ""
    remaining = list(normalized)
    for expected_row in expected_rows:
        for index, actual in enumerate(remaining):
            if match_value(actual, expected_row):
                remaining.pop(index)
                break
        else:
            return False, f"missing expected row: {expected_row!r}"
    return True, ""


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def start_daemon(daemon: Path):
    root = Path(tempfile.mkdtemp(prefix="nonodb-tck-", dir="/var/tmp"))
    sock = root / "n.sock"
    env = os.environ.copy()
    env.update(NONODB_DATA=str(root / "data"), NONODB_SOCK=str(sock), NONODB_LOG="warn")
    proc = subprocess.Popen(
        [str(daemon)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True
    )
    deadline = time.monotonic() + 20
    while not sock.exists() and time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(proc.stderr.read() if proc.stderr else "daemon exited")
        time.sleep(0.02)
    if not sock.exists():
        raise RuntimeError("timed out waiting for daemon socket")
    db = NonoDB(str(sock), cache_ttl=0.0)
    db.create_collection("nodes")
    db.create_collection("edges")
    nodes = db.collection("nodes")
    nodes.set_graph("edges")
    return proc, db, nodes


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tck-dir", default=os.environ.get("OPENCYPHER_TCK_DIR", "tck/opencypher"))
    parser.add_argument("--daemon", default=os.environ.get("NONODB_DAEMON", "target/release/nonodb-daemon"))
    parser.add_argument("--output", default="tck-report.json")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()
    tck = Path(args.tck_dir)
    if not (tck / "tck/features").is_dir():
        raise SystemExit(f"missing TCK at {tck}; run scripts/fetch-opencypher-tck.sh")
    daemon = Path(args.daemon)
    if not daemon.is_file():
        raise SystemExit(f"missing daemon binary: {daemon}")

    scenarios: list[Scenario] = []
    for feature in sorted((tck / "tck/features").rglob("*.feature")):
        scenarios.extend(parse_feature(feature))

    selected: list[tuple[Scenario, str, str, tuple]] = []
    for scenario in scenarios:
        ok, reason = select(scenario)
        if not ok:
            continue
        query = scenario_query(scenario)
        table = expected(scenario)
        if query is None or table is None:
            continue
        selected.append((scenario, str(scenario.line), query, table))
        if args.limit and len(selected) >= args.limit:
            break

    proc, db, nodes = start_daemon(daemon)
    results = []
    try:
        for scenario, location, query, table in selected:
            ordered, headers, rows = table
            try:
                value = nodes.cypher(
                    query,
                    max_rows=1000,
                    max_operations=100_000,
                    timeout_ms=2_000,
                )
                passed, detail = compare_rows(value.get("rows", []), headers, rows, ordered)
                status = "pass" if passed else "fail"
            except Exception as error:  # noqa: BLE001
                if "Connection refused" in str(error) or "Broken pipe" in str(error):
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    proc, db, nodes = start_daemon(daemon)
                    try:
                        value = nodes.cypher(
                            query,
                            max_rows=1000,
                            max_operations=100_000,
                            timeout_ms=2_000,
                        )
                        passed, detail = compare_rows(
                            value.get("rows", []), headers, rows, ordered
                        )
                        status = "pass" if passed else "fail"
                    except Exception as retry_error:  # noqa: BLE001
                        status = "unsupported"
                        detail = str(retry_error)
                else:
                    status = "unsupported"
                    detail = str(error)
            results.append(
                {
                    "feature": scenario.feature,
                    "scenario": scenario.name,
                    "line": location,
                    "query": query,
                    "status": status,
                    "detail": detail,
                }
            )
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

    counts = {key: sum(1 for row in results if row["status"] == key) for key in ("pass", "fail", "unsupported")}
    total = len(results)
    report = {
        "tck_commit": TCK_COMMIT,
        "selection": "Given any graph; no setup writes; no parameters; result-table scenarios",
        "total": total,
        "counts": counts,
        "pass_rate": round(counts["pass"] / total, 4) if total else 0.0,
        "results": results,
    }
    Path(args.output).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: report[key] for key in ("tck_commit", "total", "counts", "pass_rate")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
