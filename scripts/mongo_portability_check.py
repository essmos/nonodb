#!/usr/bin/env python3
"""Check a MongoDB workload against NonoDB's documented wire-compatibility subset.

This is a static portability checker. It does not connect to a server and it does
not execute application code. It validates either:

1. a JSON workload manifest describing driver operations, or
2. source files, using a heuristic scan for driver calls and MongoDB operators.

Manifest example::

    {
      "operations": [
        {"method": "find", "filter": {"age": {"$gte": 30}}, "sort": {"age": -1}},
        {"method": "aggregate", "pipeline": [
          {"$match": {"status": "active"}},
          {"$group": {"_id": "$team", "n": {"$sum": 1}}}
        ]},
        {"method": "create_index", "keys": {"email": 1}, "options": {"unique": true}}
      ]
    }

Usage::

    python3 scripts/mongo_portability_check.py workload.json
    python3 scripts/mongo_portability_check.py --source src/
    python3 scripts/mongo_portability_check.py --self-test
    python3 scripts/mongo_portability_check.py workload.json --fail-on-unsupported

Exit status is 0 by default so the report can be used interactively. Pass
``--fail-on-unsupported`` to fail on explicitly unsupported operations, or
``--strict`` to fail on unknown operators as well.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

FILTER_OPERATORS = {
    "$and",
    "$all",
    "$bitsAllClear",
    "$bitsAllSet",
    "$bitsAnyClear",
    "$bitsAnySet",
    "$elemMatch",
    "$eq",
    "$exists",
    "$expr",
    "$geoIntersects",
    "$geoWithin",
    "$gt",
    "$gte",
    "$in",
    "$jsonSchema",
    "$lt",
    "$lte",
    "$mod",
    "$near",
    "$nearSphere",
    "$ne",
    "$nin",
    "$nor",
    "$not",
    "$options",
    "$or",
    "$regex",
    "$size",
    "$text",
    "$type",
}
PROJECTION_OPERATORS = {"$elemMatch", "$meta", "$slice"}
UPDATE_OPERATORS = {
    "$addToSet",
    "$bit",
    "$currentDate",
    "$each",
    "$inc",
    "$max",
    "$min",
    "$mul",
    "$pop",
    "$position",
    "$pull",
    "$pullAll",
    "$push",
    "$rename",
    "$set",
    "$setOnInsert",
    "$slice",
    "$sort",
    "$type",
    "$unset",
}
AGGREGATION_STAGES = {
    "$addFields",
    "$bucket",
    "$bucketAuto",
    "$count",
    "$densify",
    "$facet",
    "$fill",
    "$graphLookup",
    "$group",
    "$limit",
    "$lookup",
    "$match",
    "$merge",
    "$out",
    "$project",
    "$replaceRoot",
    "$replaceWith",
    "$sample",
    "$set",
    "$setWindowFields",
    "$skip",
    "$sort",
    "$sortByCount",
    "$unionWith",
    "$unset",
    "$unwind",
}
AGGREGATION_EXPRESSIONS = {
    "$abs",
    "$add",
    "$addToSet",
    "$and",
    "$arrayElemAt",
    "$arrayToObject",
    "$avg",
    "$ceil",
    "$concat",
    "$concatArrays",
    "$cond",
    "$count",
    "$dateAdd",
    "$dateDiff",
    "$dateFromParts",
    "$dateFromString",
    "$dateSubtract",
    "$dateToParts",
    "$dateToString",
    "$dateTrunc",
    "$denseRank",
    "$derivative",
    "$divide",
    "$documentNumber",
    "$eq",
    "$expMovingAvg",
    "$filter",
    "$first",
    "$floor",
    "$gt",
    "$gte",
    "$ifNull",
    "$in",
    "$indexOfCP",
    "$integral",
    "$isArray",
    "$last",
    "$length",
    "$let",
    "$literal",
    "$lt",
    "$lte",
    "$ltrim",
    "$map",
    "$max",
    "$mergeObjects",
    "$min",
    "$mod",
    "$multiply",
    "$ne",
    "$not",
    "$objectToArray",
    "$or",
    "$pow",
    "$push",
    "$rank",
    "$reduce",
    "$regexMatch",
    "$rtrim",
    "$setDifference",
    "$shift",
    "$size",
    "$slice",
    "$split",
    "$strLenCP",
    "$substr",
    "$substrCP",
    "$subtract",
    "$sum",
    "$switch",
    "$toLower",
    "$toUpper",
    "$trim",
    "$type",
}
EXTENDED_JSON_KEYS = {
    "$binary",
    "$date",
    "$maxKey",
    "$minKey",
    "$numberDecimal",
    "$numberDouble",
    "$numberInt",
    "$numberLong",
    "$oid",
    "$regularExpression",
    "$timestamp",
    "$uuid",
}
UNSUPPORTED_OPERATORS = {
    "$accumulator",
    "$changeStream",
    "$collStats",
    "$comment",
    "$currentOp",
    "$documents",
    "$function",
    "$geoNear",
    "$getField",
    "$indexStats",
    "$listLocalSessions",
    "$listSessions",
    "$planCacheStats",
    "$rand",
    "$redact",
    "$search",
    "$searchMeta",
    "$setField",
    "$unsetField",
    "$vectorSearch",
    "$where",
}
UNSUPPORTED_OPTIONS = {
    "awaitData",
    "bypassDocumentValidation",
    "collation",
    "commitQuorum",
    "expireAfterSeconds",
    "hidden",
    "let",
    "max",
    "min",
    "noCursorTimeout",
    "partialFilterExpression",
    "returnKey",
    "showRecordId",
    "sparse",
    "tailable",
    "wildcardProjection",
}
UNSUPPORTED_METHODS = {
    "abort_transaction",
    "aggregate_raw_batches",
    "change_stream",
    "commit_transaction",
    "end_session",
    "find_raw_batches",
    "map_reduce",
    "start_session",
    "start_transaction",
    "watch",
    "with_transaction",
}
SUPPORTED_METHODS = {
    "aggregate",
    "bulk_write",
    "count",
    "create_collection",
    "create_index",
    "create_indexes",
    "delete_many",
    "delete_one",
    "distinct",
    "drop_collection",
    "drop_database",
    "drop_index",
    "drop_indexes",
    "estimated_document_count",
    "find",
    "find_one",
    "find_one_and_delete",
    "find_one_and_replace",
    "find_one_and_update",
    "insert_many",
    "insert_one",
    "list_collection_names",
    "list_collections",
    "list_database_names",
    "list_databases",
    "list_indexes",
    "replace_one",
    "update_many",
    "update_one",
}
METHOD_ALIASES = {
    "count_documents": "count",
    "estimated_document_count": "count",
    "find_one": "find",
    "insert_one": "insert_one",
    "insert_many": "insert_many",
    "update_one": "update_one",
    "update_many": "update_many",
    "replace_one": "replace_one",
    "delete_one": "delete_one",
    "delete_many": "delete_many",
    "find_one_and_update": "find_one_and_update",
    "find_one_and_replace": "find_one_and_replace",
    "find_one_and_delete": "find_one_and_delete",
    "create_index": "create_index",
    "create_indexes": "create_indexes",
    "drop_index": "drop_index",
    "drop_indexes": "drop_indexes",
    "list_indexes": "list_indexes",
    "create_collection": "create_collection",
    "drop_collection": "drop_collection",
    "drop_database": "drop_database",
    "list_collection_names": "list_collection_names",
    "list_collections": "list_collections",
    "list_database_names": "list_database_names",
    "list_databases": "list_databases",
    "aggregate": "aggregate",
    "distinct": "distinct",
    "bulk_write": "bulk_write",
}


@dataclass
class Issue:
    severity: str
    message: str


@dataclass
class OperationReport:
    index: int
    method: str
    issues: list[Issue] = field(default_factory=list)

    @property
    def unsupported(self) -> int:
        return sum(issue.severity == "unsupported" for issue in self.issues)

    @property
    def unknown(self) -> int:
        return sum(issue.severity == "unknown" for issue in self.issues)


def add_issue(issues: list[Issue], seen: set[tuple[str, str]], severity: str, message: str) -> None:
    key = (severity, message)
    if key not in seen:
        seen.add(key)
        issues.append(Issue(severity, message))


def classify_operator(operator: str, context: str) -> str:
    """Return ``supported``, ``unsupported`` or ``unknown`` for an operator."""
    if operator in UNSUPPORTED_OPERATORS:
        return "unsupported"
    if operator in EXTENDED_JSON_KEYS:
        return "supported"
    allowed = {
        "filter": FILTER_OPERATORS,
        "projection": PROJECTION_OPERATORS,
        "update": UPDATE_OPERATORS,
        "stage": AGGREGATION_STAGES,
        "expression": AGGREGATION_EXPRESSIONS,
        "value": set(),
    }.get(context, AGGREGATION_EXPRESSIONS)
    return "supported" if operator in allowed else "unknown"


def classify_any(operator: str) -> str:
    """Classify an operator without knowing its surrounding context."""
    if operator in UNSUPPORTED_OPERATORS:
        return "unsupported"
    kinds = {
        classify_operator(operator, context)
        for context in ("filter", "projection", "update", "stage", "expression")
    }
    if "supported" in kinds:
        return "supported"
    return "unknown"


def scan_value(
    value: Any,
    context: str,
    issues: list[Issue],
    seen: set[tuple[str, str]],
    path: str = "",
) -> None:
    if isinstance(value, list):
        for index, item in enumerate(value):
            scan_value(item, context, issues, seen, f"{path}[{index}]")
        return
    if not isinstance(value, dict):
        return
    if context == "stage":
        for stage, body in value.items():
            kind = classify_operator(stage, "stage")
            if kind == "unsupported":
                add_issue(
                    issues,
                    seen,
                    "unsupported",
                    f"{stage} aggregation stage is not supported",
                )
            elif kind == "unknown":
                add_issue(
                    issues,
                    seen,
                    "unknown",
                    f"{stage} is not in the supported aggregation-stage catalogue",
                )
            else:
                scan_stage_body(stage, body, issues, seen)
        return
    for key, item in value.items():
        if not isinstance(key, str) or not key.startswith("$"):
            scan_value(item, context, issues, seen, f"{path}.{key}" if path else key)
            continue
        kind = classify_operator(key, context)
        if kind == "unsupported":
            add_issue(issues, seen, "unsupported", f"{key} is not supported")
            continue
        if kind == "unknown":
            add_issue(
                issues,
                seen,
                "unknown",
                f"{key} is not in the supported {context} operator catalogue",
            )
            continue
        if context == "filter":
            if key in {"$and", "$or", "$nor"}:
                scan_value(item, "filter", issues, seen, key)
            elif key in {"$not", "$elemMatch"}:
                scan_value(item, "filter", issues, seen, key)
            elif key == "$expr":
                scan_value(item, "expression", issues, seen, key)
            elif key in {"$in", "$nin", "$all"}:
                scan_value(item, "value", issues, seen, key)
            elif key in {"$jsonSchema", "$options", "$regex"}:
                continue
            else:
                scan_value(item, "value", issues, seen, key)
        elif context == "update":
            if key == "$pull":
                scan_value(item, "filter", issues, seen, key)
            else:
                scan_value(item, "update", issues, seen, key)
        elif context == "expression":
            if key == "$literal":
                continue
            scan_value(item, "expression", issues, seen, key)
        elif context == "projection":
            if key == "$slice":
                continue
            scan_value(item, "expression", issues, seen, key)
        else:
            scan_value(item, "value", issues, seen, key)


def scan_stage_body(
    stage: str, body: Any, issues: list[Issue], seen: set[tuple[str, str]]
) -> None:
    if stage == "$match":
        scan_value(body, "filter", issues, seen, stage)
    elif stage in {
        "$project",
        "$addFields",
        "$set",
        "$group",
        "$bucket",
        "$bucketAuto",
        "$sortByCount",
        "$replaceRoot",
        "$replaceWith",
        "$setWindowFields",
        "$densify",
        "$fill",
    }:
        scan_value(body, "expression", issues, seen, stage)
    elif stage == "$lookup":
        if isinstance(body, dict):
            if "pipeline" in body:
                scan_value(body["pipeline"], "stage", issues, seen, "$lookup.pipeline")
            if "let" in body:
                scan_value(body["let"], "expression", issues, seen, "$lookup.let")
    elif stage == "$unionWith" and isinstance(body, dict) and "pipeline" in body:
        scan_value(body["pipeline"], "stage", issues, seen, "$unionWith.pipeline")
    elif stage == "$facet" and isinstance(body, dict):
        for name, pipeline in body.items():
            scan_value(pipeline, "stage", issues, seen, f"$facet.{name}")


def check_options(
    options: dict[str, Any], issues: list[Issue], seen: set[tuple[str, str]]
) -> None:
    for key, value in options.items():
        if key in UNSUPPORTED_OPTIONS:
            if key == "bypassDocumentValidation" and not value:
                continue
            add_issue(
                issues,
                seen,
                "unsupported",
                f"option `{key}` is not supported",
            )
            continue
        if key == "readConcern":
            level = value.get("level") if isinstance(value, dict) else value
            if level not in (None, "local", "available"):
                add_issue(
                    issues,
                    seen,
                    "unsupported",
                    f"readConcern `{level}` is not supported; use local or available",
                )
        elif key == "writeConcern":
            w = value.get("w") if isinstance(value, dict) else None
            if w not in (None, 1, "1"):
                add_issue(
                    issues,
                    seen,
                    "unsupported",
                    f"writeConcern w={w!r} is not supported; use w:1",
                )


def check_index_keys(
    keys: Any, issues: list[Issue], seen: set[tuple[str, str]]
) -> None:
    if isinstance(keys, list):
        items = [item for item in keys if isinstance(item, (list, tuple)) and len(item) == 2]
    elif isinstance(keys, dict):
        items = list(keys.items())
    else:
        return
    for field, direction in items:
        if direction in (1, -1):
            continue
        if direction == "2dsphere" and len(items) == 1:
            continue
        if direction == "text" and all(
            item_direction == "text" for _, item_direction in items
        ):
            continue
        add_issue(
            issues,
            seen,
            "unsupported",
            f"index direction {direction!r} for `{field}` is not supported",
        )


def operation_report(operation: Any, index: int) -> OperationReport:
    if not isinstance(operation, dict):
        report = OperationReport(index=index, method="<invalid>")
        report.issues.append(Issue("unsupported", "operation must be a JSON object"))
        return report
    method = (
        operation.get("method")
        or operation.get("command")
        or operation.get("op")
        or "<missing>"
    )
    method = str(method)
    report = OperationReport(index=index, method=method)
    issues = report.issues
    seen: set[tuple[str, str]] = set()
    normalized = METHOD_ALIASES.get(method, method)
    if method in UNSUPPORTED_METHODS or normalized in UNSUPPORTED_METHODS:
        add_issue(
            issues,
            seen,
            "unsupported",
            f"method `{method}` is not supported by the MongoDB wire listener",
        )
        return report
    if method not in SUPPORTED_METHODS and normalized not in SUPPORTED_METHODS:
        add_issue(
            issues,
            seen,
            "unknown",
            f"method `{method}` is not in the supported-operation catalogue",
        )
        return report

    options = operation.get("options")
    if isinstance(options, dict):
        check_options(options, issues, seen)
    for key in UNSUPPORTED_OPTIONS | {"readConcern", "writeConcern"}:
        if key in operation:
            check_options({key: operation[key]}, issues, seen)

    if normalized in {"find", "find_one"}:
        scan_value(operation.get("filter", operation.get("query", {})), "filter", issues, seen)
        if "projection" in operation:
            scan_value(operation["projection"], "projection", issues, seen)
    elif normalized in {"count", "distinct", "delete_one", "delete_many"}:
        scan_value(operation.get("filter", operation.get("query", {})), "filter", issues, seen)
    elif normalized in {"update_one", "update_many", "replace_one"}:
        scan_value(operation.get("filter", operation.get("query", {})), "filter", issues, seen)
        update = operation.get("update", {})
        if isinstance(update, list):
            scan_value(update, "stage", issues, seen)
        else:
            scan_value(update, "update", issues, seen)
    elif normalized in {
        "find_one_and_update",
        "find_one_and_replace",
        "find_one_and_delete",
    }:
        scan_value(operation.get("filter", operation.get("query", {})), "filter", issues, seen)
        if normalized != "find_one_and_delete" and "update" in operation:
            update = operation["update"]
            if isinstance(update, list):
                scan_value(update, "stage", issues, seen)
            else:
                scan_value(update, "update", issues, seen)
    elif normalized == "aggregate":
        scan_value(operation.get("pipeline", []), "stage", issues, seen)
    elif normalized in {"create_index", "drop_index"}:
        check_index_keys(operation.get("keys", operation.get("key", {})), issues, seen)
    elif normalized == "create_indexes":
        for item in operation.get("indexes", []):
            if isinstance(item, dict):
                check_index_keys(item.get("key", {}), issues, seen)
                item_options = item.get("options", {})
                if isinstance(item_options, dict):
                    check_options(item_options, issues, seen)
    elif normalized == "bulk_write":
        for nested in operation.get("operations", []):
            nested_report = operation_report(nested, index)
            for issue in nested_report.issues:
                add_issue(issues, seen, issue.severity, issue.message)
    return report


def check_manifest(manifest: Any) -> list[OperationReport]:
    operations = manifest.get("operations", []) if isinstance(manifest, dict) else manifest
    if not isinstance(operations, list):
        return [
            OperationReport(
                index=0,
                method="<manifest>",
                issues=[Issue("unsupported", "`operations` must be a list")],
            )
        ]
    return [operation_report(operation, index + 1) for index, operation in enumerate(operations)]


def summarize(reports: list[OperationReport]) -> dict[str, int]:
    return {
        "operations": len(reports),
        "ok": sum(not report.issues for report in reports),
        "unsupported": sum(report.unsupported for report in reports),
        "unknown": sum(report.unknown for report in reports),
    }


def print_reports(reports: list[OperationReport], source: str, as_json: bool) -> None:
    if as_json:
        print(
            json.dumps(
                {
                    "source": source,
                    "operations": [
                        {
                            "index": report.index,
                            "method": report.method,
                            "issues": [
                                {"severity": issue.severity, "message": issue.message}
                                for issue in report.issues
                            ],
                        }
                        for report in reports
                    ],
                    "summary": summarize(reports),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return
    print(f"Workload: {source}")
    for report in reports:
        marker = "ok" if not report.issues else "review"
        print(f"[{marker}] #{report.index} {report.method}")
        for issue in report.issues:
            print(f"  - {issue.severity}: {issue.message}")
    summary = summarize(reports)
    print(
        "Summary: {operations} operation(s), {ok} clean, "
        "{unsupported} unsupported finding(s), {unknown} unknown finding(s)".format(
            **summary
        )
    )


SOURCE_CALL_PATTERNS = {
    "watch": r"\.watch\s*\(",
    "start_session": r"\.start_session\s*\(",
    "start_transaction": r"\.start_transaction\s*\(",
    "with_transaction": r"\.with_transaction\s*\(",
    "map_reduce": r"\.map_reduce\s*\(",
}
SOURCE_METHOD_PATTERNS = {
    method: re.compile(rf"\.{re.escape(method)}\s*\(")
    for method in sorted(SUPPORTED_METHODS)
}
SOURCE_OPERATOR_RE = re.compile(r"""["'](\$[A-Za-z][A-Za-z0-9_]*)["']""")


def scan_source_file(path: Path) -> list[OperationReport]:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as error:
        return [
            OperationReport(
                index=0,
                method=str(path),
                issues=[Issue("unsupported", f"cannot read source file: {error}")],
            )
        ]
    reports: list[OperationReport] = []
    lines = text.splitlines()
    for method, pattern in SOURCE_CALL_PATTERNS.items():
        for line_number, line in enumerate(lines, 1):
            if re.search(pattern, line):
                reports.append(
                    OperationReport(
                        index=line_number,
                        method=method,
                        issues=[
                            Issue(
                                "unsupported",
                                f"{path}:{line_number}: `{method}` is not supported",
                            )
                        ],
                    )
                )
    for method, pattern in SOURCE_METHOD_PATTERNS.items():
        for line_number, line in enumerate(lines, 1):
            if pattern.search(line):
                reports.append(OperationReport(index=line_number, method=method))
                break
    seen: set[tuple[str, str]] = set()
    issues: list[Issue] = []
    for line_number, line in enumerate(lines, 1):
        for operator in SOURCE_OPERATOR_RE.findall(line):
            kind = classify_any(operator)
            if kind == "unsupported":
                add_issue(
                    issues,
                    seen,
                    "unsupported",
                    f"{path}:{line_number}: {operator} is not supported",
                )
    if issues:
        reports.append(OperationReport(index=0, method="<source scan>", issues=issues))
    if not reports:
        reports.append(
            OperationReport(
                index=0,
                method="<source scan>",
                issues=[
                    Issue(
                        "unknown",
                        f"{path}: no known MongoDB driver calls or operators found",
                    )
                ],
            )
        )
    return reports


def source_files(paths: list[str]) -> list[Path]:
    suffix = {".py", ".js", ".ts", ".mjs", ".cjs", ".go", ".rb", ".java"}
    files: list[Path] = []
    for raw in paths:
        path = Path(raw)
        if path.is_dir():
            files.extend(
                sorted(
                    child
                    for child in path.rglob("*")
                    if child.is_file() and child.suffix in suffix
                )
            )
        else:
            files.append(path)
    return files


def self_test() -> int:
    cases = [
        (
            {
                "operations": [
                    {
                        "method": "find",
                        "filter": {
                            "age": {"$gte": 30},
                            "name": {"$regularExpression": {"pattern": "^A", "options": ""}},
                        },
                    },
                    {
                        "method": "aggregate",
                        "pipeline": [
                            {"$match": {"status": "active"}},
                            {"$group": {"_id": "$team", "n": {"$sum": 1}}},
                        ],
                    },
                    {
                        "method": "create_index",
                        "keys": {"email": 1},
                        "options": {"name": "email_1", "unique": True},
                    },
                ]
            },
            {"unsupported": 0, "unknown": 0},
        ),
        (
            {
                "operations": [
                    {"method": "find", "filter": {"$where": "return true"}},
                    {"method": "watch"},
                    {"method": "aggregate", "pipeline": [{"$changeStream": {}}]},
                    {"method": "create_index", "keys": {"email": 1}, "options": {"sparse": True}},
                ]
            },
            {"unsupported": 4, "unknown": 0},
        ),
        (
            {
                "operations": [
                    {
                        "method": "update_one",
                        "filter": {"_id": 1},
                        "update": {
                            "$pull": {"scores": {"$gte": 80}},
                            "$currentDate": {"at": {"$type": "timestamp"}},
                        },
                    }
                ]
            },
            {"unsupported": 0, "unknown": 0},
        ),
        (
            {
                "operations": [
                    {"method": "find", "filter": {"age": {"$unknownOperator": 1}}},
                    {"method": "aggregate", "pipeline": [{"$unknownStage": {}}]},
                ]
            },
            {"unsupported": 0, "unknown": 2},
        ),
    ]
    failures = 0
    for index, (manifest, expected) in enumerate(cases, 1):
        summary = summarize(check_manifest(manifest))
        for key, value in expected.items():
            if summary[key] != value:
                print(f"self-test case {index}: {key}={summary[key]}, expected {value}")
                failures += 1
    if failures:
        print(f"portability self-test: {failures} failure(s)")
        return 1
    print("portability self-test: ok")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifests", nargs="*", help="JSON workload manifest files or -")
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        help="source file or directory to scan heuristically (repeatable)",
    )
    parser.add_argument("--json", action="store_true", help="emit JSON reports")
    parser.add_argument(
        "--fail-on-unsupported",
        action="store_true",
        help="exit 1 when an explicitly unsupported operation or option is found",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="exit 1 on unsupported or unknown findings",
    )
    parser.add_argument("--self-test", action="store_true", help="run internal checks")
    args = parser.parse_args()

    if args.self_test:
        return self_test()
    if not args.manifests and not args.source:
        parser.error("provide a manifest, --source, or --self-test")

    all_reports: list[OperationReport] = []
    for raw in args.manifests:
        if raw == "-":
            source = "<stdin>"
            try:
                manifest = json.load(sys.stdin)
            except json.JSONDecodeError as error:
                all_reports.append(
                    OperationReport(
                        index=0,
                        method="<manifest>",
                        issues=[Issue("unsupported", f"invalid JSON: {error}")],
                    )
                )
                continue
        else:
            source = raw
            try:
                manifest = json.loads(Path(raw).read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as error:
                all_reports.append(
                    OperationReport(
                        index=0,
                        method="<manifest>",
                        issues=[Issue("unsupported", f"cannot read manifest: {error}")],
                    )
                )
                continue
        reports = check_manifest(manifest)
        print_reports(reports, source, args.json)
        all_reports.extend(reports)

    for path in source_files(args.source):
        reports = scan_source_file(path)
        print_reports(reports, f"source:{path}", args.json)
        all_reports.extend(reports)

    summary = summarize(all_reports)
    if args.strict and (summary["unsupported"] or summary["unknown"]):
        return 1
    if args.fail_on_unsupported and summary["unsupported"]:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
