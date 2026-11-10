"""Thin Python client for nonodb (HTTP/JSON over a Unix socket).

Usage:
    from nonodb import NonoDB
    db = NonoDB("/run/nonodb.sock")
    users = db.collection("users")
    users.insert({"name": "Ala", "age": 30})
    adults = users.find({"age": {"$gte": 18}})
    db.close()

The client is thin and stateless apart from the HTTP connection - all logic lives
in the daemon. Reuse one instance per process (uvicorn worker).
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Any, Iterable, Optional

import httpx

try:  # optional faster JSON; falls back to stdlib
    import orjson as _orjson
except ImportError:  # pragma: no cover
    _orjson = None

__all__ = ["NonoDB", "VaultDB", "Collection", "NonoDBError"]

# Operations safe to retry after a transport error (idempotent reads).
_READ_OPS = {
    "find",
    "find_one",
    "count",
    "list_indexes",
    "list_collections",
    "list_text_indexes",
    "list_fuzzy_indexes",
    "search",
    "fuzzy",
    "batch",
}


def _dumps(obj: Any) -> bytes:
    if _orjson is not None:
        return _orjson.dumps(obj)
    return json.dumps(obj, separators=(",", ":")).encode()


def _loads(data: bytes) -> Any:
    if _orjson is not None:
        return _orjson.loads(data)
    return json.loads(data)


def _cache_key(payload: dict[str, Any]) -> bytes:
    if _orjson is not None:
        return _orjson.dumps(payload, option=_orjson.OPT_SORT_KEYS)
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


class NonoDBError(Exception):
    """Error returned by the daemon (`ok: false`)."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class NonoDB:
    def __init__(
        self,
        socket_path: Optional[str] = None,
        timeout: Optional[float] = None,
        cache_ttl: float = 0.0,
        token: Optional[str] = None,
        base_url: Optional[str] = None,
        verify: Optional[bool] = None,
    ):
        if base_url is None:
            if socket_path is None:
                socket_path = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")
            self.base_url = "http://localhost"
            transport = httpx.HTTPTransport(uds=socket_path)
        else:
            self.base_url = base_url.rstrip("/")
            transport = None
        self.socket_path = socket_path
        self.token = token if token is not None else os.environ.get("NONODB_TOKEN")
        if timeout is None:
            timeout = float(os.environ.get("NONODB_TIMEOUT", "10"))
        if verify is None:
            verify = os.environ.get("NONODB_TLS_VERIFY", "1") not in ("0", "false", "no")
        self._http = httpx.Client(
            transport=transport,
            base_url=self.base_url,
            timeout=timeout,
            verify=verify if transport is None else True,
        )
        # Optional read cache. Disabled by default (cache_ttl=0).
        # TTL bounds staleness for data written by other processes;
        # every write through this instance clears the whole cache.
        self._cache_ttl = cache_ttl
        # Values are stored as serialized JSON (no deepcopy).
        self._cache: dict[bytes, tuple[float, bytes]] = {}
        self._cache_lock = threading.Lock()

    # -- lifecycle ---------------------------------------------------
    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "NonoDB":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def clear_cache(self) -> None:
        with self._cache_lock:
            self._cache.clear()

    # -- core --------------------------------------------------------
    def exec(
        self,
        payload: dict[str, Any],
        *,
        headers: Optional[dict[str, str]] = None,
    ) -> Any:
        op = payload.get("op")
        cache_key: Optional[bytes] = None
        if headers is None and self._cache_ttl > 0 and op in _READ_OPS:
            cache_key = _cache_key(payload)
            with self._cache_lock:
                hit = self._cache.get(cache_key)
            if hit is not None:
                ts, raw = hit
                if time.monotonic() - ts < self._cache_ttl:
                    return _loads(raw)
                with self._cache_lock:
                    self._cache.pop(cache_key, None)

        result = self._exec_uncached(payload, op, headers)

        if self._cache_ttl > 0 and headers is None:
            with self._cache_lock:
                if cache_key is not None:
                    self._cache[cache_key] = (time.monotonic(), _dumps(result))
                else:
                    # A write invalidates all reads (consistency within the client).
                    self._cache.clear()
        return result

    def _exec_uncached(
        self,
        payload: dict[str, Any],
        op: Optional[str],
        extra_headers: Optional[dict[str, str]] = None,
    ) -> Any:
        # Reads are retried after a transport error; writes are not (duplicate risk).
        retries = 2 if op in _READ_OPS else 0
        attempt = 0
        while True:
            try:
                headers = {"content-type": "application/json"}
                if extra_headers:
                    headers.update(extra_headers)
                if self.token:
                    headers["authorization"] = f"Bearer {self.token}"
                resp = self._http.post(
                    self.base_url + "/v1/exec",
                    content=_dumps(payload),
                    headers=headers,
                )
                break
            except httpx.TransportError as exc:
                if attempt >= retries:
                    # Daemon unavailable / broken connection - always our own type,
                    # so callers do not need to know about httpx.
                    raise NonoDBError("connection", str(exc)) from exc
                attempt += 1
                time.sleep(0.01 * attempt)
        try:
            body = _loads(resp.content)
        except ValueError:
            raise NonoDBError(
                "invalid_response", f"HTTP {resp.status_code}: {resp.text[:200]}"
            )
        if not body.get("ok"):
            err = body.get("error") or {}
            raise NonoDBError(err.get("code", "error"), err.get("message", ""))
        return body.get("result")

    def health(self) -> bool:
        try:
            resp = self._http.get(self.base_url + "/healthz")
        except httpx.HTTPError:
            return False
        return resp.status_code == 200 and resp.json().get("ok") is True

    def collections(self) -> list[str]:
        return self.exec({"op": "list_collections"})["collections"]

    def batch(self, ops: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Executes a list of **reads** in one LMDB transaction.

        Each result element is `{"ok": true, "result": ...}` or
        `{"ok": false, "error": {"code", "message"}}`. Write operations are
        rejected (there are no multi-document transactions).
        """
        return self.exec({"op": "batch", "ops": ops})["results"]

    def write_batch(self, ops: list[dict[str, Any]]) -> list[Any]:
        """Atomic write batch: all or nothing.

        Allowed operations: `insert`, `insert_many`, `update`, `delete`.
        Each must contain `coll`. Returns results in operation order.
        """
        return self.exec({"op": "write_batch", "ops": ops})["results"]

    def create_collection(self, name: str, profile: str = "standard") -> dict[str, Any]:
        """Creates a collection explicitly. Use `profile="vault"` for encrypted storage."""
        return self.exec(
            {"op": "create_collection", "coll": name, "profile": profile}
        )

    def vault(self, tenant: str, key_id: str, key: str) -> "VaultDB":
        """Returns a handle that sends vault encryption headers on every request."""
        return VaultDB(self, tenant, key_id, key)

    def collection(self, name: str) -> "Collection":
        return Collection(self, name)


class VaultDB:
    """A tenant/key bound view of a nonodb database.

    The key is sent only in request headers (never in JSON bodies) and this
    wrapper does not use the client-side read cache.
    """

    def __init__(self, db: NonoDB, tenant: str, key_id: str, key: str):
        self._db = db
        self.tenant = tenant
        self.key_id = key_id
        self._key = key

    def __repr__(self) -> str:
        return (
            f"VaultDB(tenant={self.tenant!r}, "
            f"key_id={self.key_id!r}, key=<redacted>)"
        )

    def _headers(self) -> dict[str, str]:
        return {
            "x-nonodb-tenant": self.tenant,
            "x-nonodb-key-id": self.key_id,
            "x-nonodb-key": self._key,
        }

    def exec(self, payload: dict[str, Any]) -> Any:
        return self._db.exec(payload, headers=self._headers())

    def collection(self, name: str) -> "Collection":
        return Collection(self, name)

    def batch(self, ops: list[dict[str, Any]]) -> list[dict[str, Any]]:
        raise NonoDBError(
            "unsupported_encrypted_operation",
            "batch reads are not supported for vault collections",
        )

    def write_batch(self, ops: list[dict[str, Any]]) -> list[Any]:
        raise NonoDBError(
            "unsupported_encrypted_operation",
            "write_batch is not supported for vault collections",
        )


class Collection:
    def __init__(self, db: NonoDB, name: str):
        self._db = db
        self.name = name

    # -- write -------------------------------------------------------
    def insert(self, doc: dict[str, Any]) -> str:
        return self._db.exec({"op": "insert", "coll": self.name, "doc": doc})[
            "inserted_id"
        ]

    def insert_many(self, docs: Iterable[dict[str, Any]]) -> list[str]:
        return self._db.exec(
            {"op": "insert_many", "coll": self.name, "docs": list(docs)}
        )["inserted_ids"]

    def update(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        multi: bool = False,
        *,
        upsert: bool = False,
    ) -> int:
        """Updates documents; with `upsert=True`, inserts a new one when nothing matches."""
        return self.update_result(filter, update, multi=multi, upsert=upsert)["modified"]

    def update_result(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        multi: bool = False,
        *,
        upsert: bool = False,
    ) -> dict[str, Any]:
        """Returns `{matched, modified, upserted_id}` (useful for upserts)."""
        return self._db.exec(
            {
                "op": "update",
                "coll": self.name,
                "filter": filter,
                "update": update,
                "multi": multi,
                "upsert": upsert,
            }
        )

    def delete(self, filter: dict[str, Any], multi: bool = False) -> int:
        return self._db.exec(
            {"op": "delete", "coll": self.name, "filter": filter, "multi": multi}
        )["deleted"]

    def write_batch(self, ops: list[dict[str, Any]]) -> list[Any]:
        """Atomic write batch in this collection (`coll` is added automatically)."""
        return self._db.write_batch(
            [{**op, "coll": op.get("coll", self.name)} for op in ops]
        )

    # -- indexes -----------------------------------------------------
    def create_index(
        self,
        field: Optional[str] = None,
        unique: bool = False,
        *,
        fields: Optional[list[str]] = None,
        include: Optional[list[str]] = None,
    ) -> bool:
        """Creates a single-field (`field`) or compound (`fields`) index.

        `include` copies extra fields into the index, allowing a projection to be covered
        without reading the document (covering index).
        """
        if fields is None:
            if field is None:
                raise ValueError("provide `field` or `fields`")
            fields = [field]
        self._db.exec(
            {
                "op": "create_index",
                "coll": self.name,
                "fields": fields,
                "unique": unique,
                "include": include or [],
            }
        )
        return True

    def drop_index(self, field: str) -> bool:
        return self._db.exec(
            {"op": "drop_index", "coll": self.name, "field": field}
        )["dropped"]

    def list_indexes(self) -> list[dict[str, Any]]:
        return self._db.exec({"op": "list_indexes", "coll": self.name})["indexes"]

    def get_many(self, ids: Iterable[str]) -> list[dict[str, Any]]:
        """Fetches multiple documents by `_id` in one request (batch)."""
        ops = [
            {"op": "find_one", "coll": self.name, "filter": {"_id": i}} for i in ids
        ]
        return [
            r["result"]
            for r in self._db.batch(ops)
            if r.get("ok") and r.get("result") is not None
        ]

    # -- history -----------------------------------------------------
    def history(
        self,
        doc_id: str,
        *,
        limit: int = 50,
        include_docs: bool = False,
    ) -> list[dict[str, Any]]:
        """Returns document revisions, newest first.

        Revisions use global monotonic `rev` values. History is opt-in on the
        daemon via `NONODB_HISTORY=1`.
        """
        return self._db.exec(
            {
                "op": "history",
                "coll": self.name,
                "id": doc_id,
                "limit": limit,
                "include_docs": include_docs,
            }
        )["revisions"]

    def get_version(self, doc_id: str, version: int) -> Optional[dict[str, Any]]:
        """Returns one exact revision, or `None` if it was pruned/not found."""
        return self._db.exec(
            {
                "op": "get_version",
                "coll": self.name,
                "id": doc_id,
                "version": version,
            }
        )

    # -- full-text / fuzzy -------------------------------------------
    def create_text_index(self, field: str) -> bool:
        self._db.exec(
            {"op": "create_text_index", "coll": self.name, "field": field}
        )
        return True

    def drop_text_index(self, field: str) -> bool:
        return self._db.exec(
            {"op": "drop_text_index", "coll": self.name, "field": field}
        )["dropped"]

    def list_text_indexes(self) -> list[str]:
        return self._db.exec(
            {"op": "list_text_indexes", "coll": self.name}
        )["text_indexes"]

    def create_fuzzy_index(self, field: str) -> bool:
        self._db.exec(
            {"op": "create_fuzzy_index", "coll": self.name, "field": field}
        )
        return True

    def drop_fuzzy_index(self, field: str) -> bool:
        return self._db.exec(
            {"op": "drop_fuzzy_index", "coll": self.name, "field": field}
        )["dropped"]

    def list_fuzzy_indexes(self) -> list[str]:
        return self._db.exec(
            {"op": "list_fuzzy_indexes", "coll": self.name}
        )["fuzzy_indexes"]

    def search(
        self,
        field: str,
        query: str,
        *,
        filter: Optional[dict[str, Any]] = None,
        skip: int = 0,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Full-text search (BM25). Results include an extra `_score`, descending."""
        return self._db.exec(
            {
                "op": "search",
                "coll": self.name,
                "field": field,
                "query": query,
                "filter": filter or {},
                "skip": skip,
                "limit": limit,
            }
        )["results"]

    def fuzzy(
        self,
        field: str,
        query: str,
        *,
        max_distance: int = 2,
        filter: Optional[dict[str, Any]] = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Approximate search. Results include `_distance`, ascending."""
        return self._db.exec(
            {
                "op": "fuzzy",
                "coll": self.name,
                "field": field,
                "query": query,
                "max_distance": max_distance,
                "filter": filter or {},
                "limit": limit,
            }
        )["results"]

    # -- read --------------------------------------------------------
    def find(
        self,
        filter: Optional[dict[str, Any]] = None,
        *,
        projection: Optional[dict[str, Any]] = None,
        sort: Optional[list[tuple[str, int]]] = None,
        skip: int = 0,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "op": "find",
            "coll": self.name,
            "filter": filter or {},
            "skip": skip,
        }
        if projection is not None:
            payload["projection"] = projection
        if sort is not None:
            payload["sort"] = sort
        if limit is not None:
            payload["limit"] = limit
        return self._db.exec(payload)

    def find_one(
        self,
        filter: Optional[dict[str, Any]] = None,
        *,
        projection: Optional[dict[str, Any]] = None,
    ) -> Optional[dict[str, Any]]:
        payload: dict[str, Any] = {
            "op": "find_one",
            "coll": self.name,
            "filter": filter or {},
        }
        if projection is not None:
            payload["projection"] = projection
        result = self._db.exec(payload)
        return result if result is not None else None

    def count(self, filter: Optional[dict[str, Any]] = None) -> int:
        return self._db.exec(
            {"op": "count", "coll": self.name, "filter": filter or {}}
        )["count"]

    def aggregate(self, pipeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Runs a read-only aggregation pipeline and returns its result rows."""
        return self._db.exec(
            {"op": "aggregate", "coll": self.name, "pipeline": pipeline}
        )["results"]
