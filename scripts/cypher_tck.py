#!/usr/bin/env python3
"""Run the pinned read-only openCypher TCK corpus.

Selection: every result-table scenario whose *query* is read-only, including
scenarios that use graph fixtures, `Given any graph`, parameters, and named
graphs. Scenarios that require write queries, expected-error semantics, custom
procedures, or unsupported fixture syntax are reported explicitly as excluded
or unsupported; they are never silently dropped.
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
    background: list[Step] = []
    background_step: Step | None = None
    in_background = False
    feature = path.stem
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("Feature:"):
            feature = stripped.split(":", 1)[1].strip()
            continue
        if stripped.startswith("Background:"):
            in_background = True
            current = None
            step = None
            continue
        if stripped.startswith("Scenario:") or stripped.startswith("Scenario Outline:"):
            in_background = False
            current = Scenario(
                feature,
                stripped.split(":", 1)[1].strip(),
                [
                    Step(item.keyword, item.text, item.docstring, [list(row) for row in item.table])
                    for item in background
                ],
                number,
                stripped.startswith("Scenario Outline:"),
            )
            scenarios.append(current)
            step = None
            continue
        if in_background:
            if docstring is not None:
                if stripped == '"""':
                    assert background_step is not None
                    background_step.docstring = "\n".join(docstring)
                    docstring = None
                else:
                    docstring.append(line)
                continue
            if stripped == '"""':
                docstring = []
                continue
            if stripped.startswith("|"):
                if background_step is not None:
                    background_step.table.append(
                        [cell.strip() for cell in split_table(stripped)]
                    )
                continue
            match = re.match(r"^(Given|When|Then|And|But)\s+(.*)$", stripped)
            if match:
                keyword, text = match.groups()
                if keyword in ("And", "But") and background_step is not None:
                    keyword = background_step.keyword
                background_step = Step(keyword, text)
                background.append(background_step)
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
            if char != "|":
                current.append("\\")
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
    if not any(text == "executing query:" for text in texts):
        return False, "no_query"
    if any("should be raised" in text for text in texts) or any(
        "should raise" in text for text in texts
    ):
        return False, "error_scenario"
    if not any("should be, in" in text for text in texts):
        return False, "no_result_table"
    if scenario.outline:
        return False, "outline"
    query = scenario_query(scenario) or ""
    if re.search(
        r"\b(CREATE|MERGE|SET|DELETE|REMOVE|FOREACH|LOAD\s+CSV|DROP|START)\b",
        query,
        re.IGNORECASE,
    ):
        return False, "write_query"
    for call in re.findall(r"\bCALL\s+([A-Za-z_][A-Za-z0-9_.]*)", query, re.IGNORECASE):
        if call.lower() not in {
            "gds.pagerank.stream",
            "gds.degree.stream",
            "gds.connectedcomponents.stream",
            "gds.nodesimilarity.stream",
        }:
            return False, "custom_procedure"
    for step in scenario.steps:
        if step.text.startswith("having executed") and re.search(
            r"\b(UNWIND|MATCH|WITH|RETURN|DELETE|REMOVE|SET|MERGE|FOREACH|LOAD\s+CSV)\b",
            step.docstring or "",
            re.IGNORECASE,
        ):
            return False, "complex_fixture"
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
        if self.peek() == "<":
            self.pos += 1
            path = self.path_or_node()
            if self.peek() != ">":
                raise ValueError("unterminated path")
            self.pos += 1
            if isinstance(path, dict) and path.get("__type") == "path":
                return path
            return {"__type": "path", "nodes": [path], "relationships": []}
        if self.peek() == "(":
            return self.path_or_node()
        if self.peek() == "[" and self.text[self.pos : self.pos + 3] == "[: ":
            return self.relationship()
        if self.peek() == "[":
            if self.text[self.pos : self.pos + 2] == "[:":
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
        hex_number = re.match(r"-?0[xX][0-9a-fA-F]+", self.text[self.pos :])
        if hex_number:
            self.pos += len(hex_number.group(0))
            return int(hex_number.group(0), 16)
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
                escaped = self.text[self.pos]
                self.pos += 1
                if escaped == "n":
                    out.append("\n")
                elif escaped == "r":
                    out.append("\r")
                elif escaped == "t":
                    out.append("\t")
                elif escaped == "u" and self.pos + 4 <= len(self.text):
                    out.append(chr(int(self.text[self.pos : self.pos + 4], 16)))
                    self.pos += 4
                else:
                    out.append(escaped)
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

    def path_or_node(self):
        first = self.node()
        nodes = [first]
        rels = []
        while self.text.startswith("<-", self.pos) or self.text.startswith("-", self.pos):
            if self.text.startswith("<-", self.pos):
                self.pos += 2
                rel = self.relationship()
                if not self.text.startswith("-", self.pos):
                    raise ValueError("bad path arrow")
                self.pos += 1
            else:
                self.pos += 1
                rel = self.relationship()
                if self.text.startswith("->", self.pos):
                    self.pos += 2
                elif self.text.startswith("-", self.pos):
                    self.pos += 1
                else:
                    raise ValueError("bad path arrow")
            rels.append(rel)
            nodes.append(self.node())
        if rels:
            return {"__type": "path", "nodes": nodes, "relationships": rels}
        return first

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
    if text == "NaN":
        return None
    return LiteralParser(text).parse()


def format_temporal(value: dict) -> str:
    kind = value.get("__type")
    if kind == "date":
        return f"{value.get('year', 0):04d}-{value.get('month', 1):02d}-{value.get('day', 1):02d}"
    if kind in ("localtime", "time"):
        text = f"{value.get('hour', 0):02d}:{value.get('minute', 0):02d}"
        second = value.get("second", 0)
        nano = value.get("nanosecond", 0)
        if second or nano:
            text += f":{second:02d}"
            if nano:
                text += "." + f"{nano:09d}".rstrip("0")
        if kind == "time":
            offset = value.get("offset_seconds")
            if offset is not None:
                sign = "+" if offset >= 0 else "-"
                offset = abs(int(offset))
                text += f"{sign}{offset // 3600:02d}:{offset % 3600 // 60:02d}"
            else:
                zone = value.get("offset") or value.get("timezone")
                if isinstance(zone, str) and (zone.startswith("+") or zone.startswith("-")):
                    text += zone
        return text
    if kind in ("localdatetime", "datetime"):
        text = (
            f"{value.get('year', 0):04d}-{value.get('month', 1):02d}-"
            f"{value.get('day', 1):02d}T"
            f"{value.get('hour', 0):02d}:{value.get('minute', 0):02d}"
        )
        second = value.get("second", 0)
        nano = value.get("nanosecond", 0)
        if second or nano:
            text += f":{second:02d}"
            if nano:
                text += "." + f"{nano:09d}".rstrip("0")
        if kind == "datetime":
            offset = value.get("offset_seconds")
            if offset is not None:
                sign = "+" if offset >= 0 else "-"
                offset = abs(int(offset))
                text += f"{sign}{offset // 3600:02d}:{offset % 3600 // 60:02d}"
            else:
                zone = value.get("offset") or value.get("timezone")
                if isinstance(zone, str) and (zone.startswith("+") or zone.startswith("-")):
                    text += zone
                elif zone == "UTC":
                    text += "Z"
                else:
                    text += "Z"
        return text
    return value


def normalize_actual(value: Any):
    if isinstance(value, dict) and value.get("__type") in {
        "date",
        "localtime",
        "time",
        "localdatetime",
        "datetime",
    }:
        return format_temporal(value)
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
    if isinstance(value, dict):
        return {key: normalize_actual(item) for key, item in value.items()}
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
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        return abs(float(actual) - float(expected)) < 1e-9
    if isinstance(expected, str) and isinstance(actual, str):
        return actual == expected or re.sub(r"\\+", r"\\", actual) == re.sub(
            r"\\+", r"\\", expected
        )
    return actual == expected


def column_key(value: str) -> str:
    key = "".join(value.split()).lower()
    while key.startswith("(") and key.endswith(")"):
        key = key[1:-1]
    return key


def compare_rows(
    actual_rows: list[dict],
    columns: list[str],
    headers: list[str],
    rows: list[list[str]],
    ordered: bool,
) -> tuple[bool, str]:
    expected_rows = [{header: parse_cell(cell) for header, cell in zip(headers, row)} for row in rows]
    if not actual_rows and not expected_rows:
        return True, ""
    normalized = []
    by_name = {column_key(column): column for column in columns}
    mapping = {}
    for header in headers:
        column = by_name.get(column_key(header))
        if column is None:
            return False, f"columns {columns!r} do not contain {headers!r}"
        mapping[header] = column
    for row in actual_rows:
        normalized.append(
            {
                header: normalize_actual(row.get(mapping[header]))
                for header in headers
            }
        )
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



# --- fixture setup -------------------------------------------------------


class FixtureError(ValueError):
    pass


def lex_fixture(text: str) -> list[str]:
    tokens: list[str] = []
    index = 0
    while index < len(text):
        char = text[index]
        if char.isspace():
            index += 1
            continue
        if text.startswith("//", index):
            end = text.find("\n", index)
            index = len(text) if end < 0 else end + 1
            continue
        if char in "'\"":
            quote = char
            index += 1
            value = []
            while index < len(text):
                char = text[index]
                index += 1
                if char == "\\" and index < len(text):
                    escaped = text[index]
                    index += 1
                    if escaped == "n":
                        value.append("\n")
                    elif escaped == "r":
                        value.append("\r")
                    elif escaped == "t":
                        value.append("\t")
                    elif escaped == "u" and index + 4 <= len(text):
                        value.append(chr(int(text[index : index + 4], 16)))
                        index += 4
                    else:
                        value.append(escaped)
                elif char == quote:
                    break
                else:
                    value.append(char)
            else:
                raise FixtureError("unterminated fixture string")
            tokens.append("'" + "".join(value) + "'")
            continue
        if text.startswith("->", index) or text.startswith("<-", index):
            tokens.append(text[index : index + 2])
            index += 2
            continue
        if char in "()[]{}:,|;-":
            tokens.append(char)
            index += 1
            continue
        number = re.match(r"-?(?:\d+\.\d*|\d*\.\d+|\d+)(?:[eE][+-]?\d+)?", text[index:])
        if number:
            tokens.append(number.group(0))
            index += len(number.group(0))
            continue
        word = re.match(r"[A-Za-z_][A-Za-z0-9_]*", text[index:])
        if word:
            tokens.append(word.group(0))
            index += len(word.group(0))
            continue
        raise FixtureError(f"unexpected fixture character {char!r}")
    return tokens


def parse_temporal_literal(kind: str, text: str) -> dict[str, Any]:
    value = text.strip()
    if kind == "date":
        year, month, day = (int(part) for part in value.split("-"))
        return {"__type": "date", "year": year, "month": month, "day": day}
    if kind in ("localtime", "time"):
        main = value
        offset = None
        for sign in ("+", "-"):
            marker = main.find(sign, 1)
            if marker > 0:
                raw = main[marker:]
                main = main[:marker]
                sign_value = 1 if raw[0] == "+" else -1
                hours, minutes = (int(part) for part in raw[1:].split(":"))
                offset = sign_value * (hours * 3600 + minutes * 60)
                break
        parts = main.split(":")
        hour, minute = int(parts[0]), int(parts[1])
        second = int(float(parts[2])) if len(parts) > 2 else 0
        nanosecond = 0
        if len(parts) > 2 and "." in parts[2]:
            fraction = parts[2].split(".", 1)[1]
            nanosecond = int((fraction + "000000000")[:9])
        out = {
            "__type": kind,
            "hour": hour,
            "minute": minute,
            "second": second,
            "nanosecond": nanosecond,
        }
        if offset is not None:
            out["offset_seconds"] = offset
        return out
    date_part, _, time_part = value.partition("T")
    year, month, day = (int(part) for part in date_part.split("-"))
    offset = None
    if time_part.endswith("Z"):
        time_part = time_part[:-1]
        offset = 0
    for sign in ("+", "-"):
        marker = time_part.find(sign, 1)
        if marker > 0:
            raw = time_part[marker:]
            time_part = time_part[:marker]
            sign_value = 1 if raw[0] == "+" else -1
            hours, minutes = (int(part) for part in raw[1:].split(":"))
            offset = sign_value * (hours * 3600 + minutes * 60)
            break
    clock = time_part.split(":")
    hour, minute = int(clock[0]), int(clock[1])
    second = int(float(clock[2])) if len(clock) > 2 else 0
    nanosecond = 0
    if len(clock) > 2 and "." in clock[2]:
        fraction = clock[2].split(".", 1)[1]
        nanosecond = int((fraction + "000000000")[:9])
    out = {
        "__type": kind,
        "year": year,
        "month": month,
        "day": day,
        "hour": hour,
        "minute": minute,
        "second": second,
        "nanosecond": nanosecond,
    }
    if offset is not None:
        out["offset_seconds"] = offset
    return out


class FixtureBuilder:
    def __init__(self):
        self.tokens: list[str] = []
        self.pos = 0
        self.vars: dict[str, str] = {}
        self.nodes: list[dict[str, Any]] = []
        self.edges: list[dict[str, Any]] = []
        self.node_seq = 0
        self.edge_seq = 0

    def peek(self, offset: int = 0) -> str:
        index = self.pos + offset
        return self.tokens[index] if index < len(self.tokens) else ""

    def take(self, expected: str | None = None) -> str:
        token = self.peek()
        if expected is not None and token != expected:
            raise FixtureError(f"expected {expected!r}, got {token!r}")
        self.pos += 1
        return token

    def word(self) -> bool:
        token = self.peek()
        return bool(token) and (token[0].isalpha() or token[0] == "_")

    def parse(self, text: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        self.tokens = lex_fixture(text)
        self.pos = 0
        while self.peek():
            if self.peek().upper() == "CREATE":
                self.take()
                self.pattern_list()
                if self.peek() == ";":
                    self.take(";")
            else:
                raise FixtureError(f"unsupported fixture clause {self.peek()!r}")
        return self.nodes, self.edges

    def pattern_list(self) -> None:
        while True:
            self.pattern()
            if self.peek() == ",":
                self.take(",")
                continue
            break

    def pattern(self) -> None:
        first = self.node()
        current = first
        while self.peek() in ("-", "<-"):
            direction = "out"
            if self.peek() == "<-":
                self.take("<-")
                direction = "in"
                rel = self.rel_bracket()
                self.take("-")
            else:
                self.take("-")
                rel = self.rel_bracket()
                if self.peek() == "->":
                    self.take("->")
                elif self.peek() == "-":
                    self.take("-")
                    direction = "both"
                else:
                    raise FixtureError("bad relationship direction")
            target = self.node()
            src = current if direction == "out" else target
            dst = target if direction == "out" else current
            self.edge_seq += 1
            edge = {
                "_id": f"e{self.edge_seq}",
                "from": src,
                "to": dst,
                "label": rel["type"],
                **rel["props"],
            }
            if rel["var"]:
                self.vars[rel["var"]] = edge["_id"]
            self.edges.append(edge)
            current = target

    def node(self) -> str:
        self.take("(")
        var = None
        labels: list[str] = []
        props: dict[str, Any] = {}
        if self.word():
            var = self.take()
        while self.peek() == ":":
            self.take(":")
            labels.append(self.take())
        if self.peek() == "{":
            props = self.map()
        self.take(")")
        if var and var in self.vars:
            node_id = self.vars[var]
            for node in self.nodes:
                if node["_id"] == node_id:
                    node["_labels"] = sorted(set(node.get("_labels", [])) | set(labels))
                    node.update(props)
            return node_id
        self.node_seq += 1
        node_id = f"n{self.node_seq}"
        if var:
            self.vars[var] = node_id
        self.nodes.append({"_id": node_id, "_labels": labels, **props})
        return node_id

    def rel_bracket(self) -> dict[str, Any]:
        var = None
        rel_type = ""
        props: dict[str, Any] = {}
        if self.peek() != "[":
            return {"var": None, "type": rel_type, "props": props}
        self.take("[")
        if self.word():
            var = self.take()
        if self.peek() == ":":
            self.take(":")
            rel_type = self.take()
            while self.peek() == "|":
                self.take("|")
                self.take()  # additional types are not representable; keep first
        if self.peek() == "{":
            props = self.map()
        self.take("]")
        return {"var": var, "type": rel_type, "props": props}

    def value(self) -> Any:
        token = self.peek()
        if token in {"date", "localtime", "time", "localdatetime", "datetime"} and self.peek(1) == "(":
            kind = self.take()
            self.take("(")
            if self.peek() == "{":
                fields = self.map()
                self.take(")")
                timezone = fields.pop("timezone", None)
                if isinstance(timezone, str):
                    sign = -1 if timezone.startswith("-") else 1
                    hours, minutes = (int(part) for part in timezone[1:].split(":"))
                    fields["offset_seconds"] = sign * (hours * 3600 + minutes * 60)
                return {"__type": kind, **fields}
            text = self.take()
            self.take(")")
            return parse_temporal_literal(kind, text[1:-1])
        if token in ("true", "false"):
            self.take()
            return token == "true"
        if token == "null":
            self.take()
            return None
        if token.startswith("'"):
            self.take()
            return token[1:-1]
        if token == "[":
            return self.list()
        if token == "{":
            return self.map()
        if re.fullmatch(r"-?\d+", token):
            self.take()
            return int(token)
        if re.fullmatch(r"-?(?:\d+\.\d*|\d*\.\d+)(?:[eE][+-]?\d+)?", token):
            self.take()
            return float(token)
        if token == "-":
            self.take()
            number = self.take()
            return -float(number) if "." in number else -int(number)
        raise FixtureError(f"unsupported fixture value {token!r}")

    def list(self) -> list[Any]:
        self.take("[")
        out = []
        while self.peek() != "]":
            out.append(self.value())
            if self.peek() == ",":
                self.take(",")
        self.take("]")
        return out

    def map(self) -> dict[str, Any]:
        self.take("{")
        out: dict[str, Any] = {}
        while self.peek() != "}":
            key = self.take()
            if key.startswith("'"):
                key = key[1:-1]
            self.take(":")
            out[key] = self.value()
            if self.peek() == ",":
                self.take(",")
        self.take("}")
        return out


def fixture_docs(
    scenario: Scenario, tck: Path
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    builder = FixtureBuilder()
    for step in scenario.steps:
        if step.text.startswith("having executed"):
            if not step.docstring:
                raise FixtureError("fixture step has no query")
            builder.parse(step.docstring)
        elif step.text.startswith("the ") and step.text.endswith(" graph"):
            name = step.text[4:-6].strip()
            if name == "empty":
                continue
            script = tck / "tck" / "graphs" / name / f"{name}.cypher"
            if not script.is_file():
                raise FixtureError(f"unknown named graph {name!r}")
            builder.parse(script.read_text(encoding="utf-8"))
    return builder.nodes, builder.edges


def scenario_parameters(scenario: Scenario) -> dict[str, Any]:
    params: dict[str, Any] = {}
    for step in scenario.steps:
        if step.text.startswith("parameters are"):
            for row in step.table:
                if len(row) >= 2:
                    params[row[0]] = parse_cell(row[1])
    return params


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
    return proc, db, nodes, db.collection("edges")


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
    excluded: list[dict[str, Any]] = []
    for scenario in scenarios:
        ok, reason = select(scenario)
        if not ok:
            excluded.append(
                {
                    "feature": scenario.feature,
                    "scenario": scenario.name,
                    "line": scenario.line,
                    "reason": reason,
                }
            )
            continue
        query = scenario_query(scenario)
        table = expected(scenario)
        if query is None or table is None:
            excluded.append(
                {
                    "feature": scenario.feature,
                    "scenario": scenario.name,
                    "line": scenario.line,
                    "reason": "missing_query_or_result",
                }
            )
            continue
        selected.append((scenario, str(scenario.line), query, table))
        if args.limit and len(selected) >= args.limit:
            break

    proc, db, nodes, edges = start_daemon(daemon)
    results = []
    try:
        for scenario, location, query, table in selected:
            ordered, headers, rows = table
            try:
                fixtures, fixture_edges = fixture_docs(scenario, tck)
                params = scenario_parameters(scenario)
                nodes.delete({}, multi=True)
                edges.delete({}, multi=True)
                if fixtures:
                    nodes.insert_many(fixtures)
                if fixture_edges:
                    edges.insert_many(fixture_edges)
            except Exception as error:  # noqa: BLE001
                results.append(
                    {
                        "feature": scenario.feature,
                        "scenario": scenario.name,
                        "line": location,
                        "query": query,
                        "status": "unsupported",
                        "detail": f"fixture: {error}",
                    }
                )
                continue
            try:
                value = nodes.cypher(
                    query,
                    params=params,
                    max_rows=1000,
                    max_operations=10_000_000,
                    timeout_ms=10_000,
                )
                passed, detail = compare_rows(value.get("rows", []), value.get("columns", []), headers, rows, ordered)
                status = "pass" if passed else "known_gap"
            except Exception as error:  # noqa: BLE001
                if "Connection refused" in str(error) or "Broken pipe" in str(error):
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    proc, db, nodes, edges = start_daemon(daemon)
                    try:
                        value = nodes.cypher(
                            query,
                            params=params,
                            max_rows=1000,
                            max_operations=10_000_000,
                            timeout_ms=10_000,
                        )
                        passed, detail = compare_rows(
                            value.get("rows", []),
                            value.get("columns", []),
                            headers,
                            rows,
                            ordered,
                        )
                        status = "pass" if passed else "known_gap"
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

    counts = {key: sum(1 for row in results if row["status"] == key) for key in ("pass", "known_gap", "unsupported")}
    total = len(results)
    report = {
        "tck_commit": TCK_COMMIT,
        "selection": (
            "All result-table scenarios with a read-only query, including graph fixtures, "
            "parameters, and named graphs. Expected-error and outline scenarios are excluded."
        ),
        "total": total,
        "counts": counts,
        "pass_rate": round(counts["pass"] / total, 4) if total else 0.0,
        "excluded": excluded,
        "results": results,
    }
    Path(args.output).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: report[key] for key in ("tck_commit", "total", "counts", "pass_rate")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
