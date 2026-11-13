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
import ssl
import threading
import time
from typing import Any, Iterable, Iterator, Optional, Union

import httpx

try:  # optional faster JSON; falls back to stdlib
    import orjson as _orjson
except ImportError:  # pragma: no cover
    _orjson = None

__all__ = [
    "NonoDB",
    "VaultDB",
    "Collection",
    "Cursor",
    "NonoDBError",
    "StreamTruncatedError",
]

# Operations safe to retry after a transport error (idempotent reads).
_READ_OPS = {
    "find",
    "find_one",
    "lookup",
    "count",
    "list_indexes",
    "list_collections",
    "get_collection_ttl",
    "get_collection_history",
    "get_collection_events",
    "list_subscriptions",
    "list_text_indexes",
    "list_fuzzy_indexes",
    "list_vector_indexes",
    "search",
    "fuzzy",
    "vector_search",
    "sql",
    "explain_sql",
    "batch",
    "map_info",
}

# Reads are safe to retry. `watch` is long-polling and must not be cached.
_RETRY_OPS = _READ_OPS | {"watch"}


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


class Cursor:
    """Bounded, server-side cursor returned by `find_cursor`/`aggregate_cursor`.

    Iterating fetches further batches automatically. Call `close()` (or use the
    context manager) to release the server-side cursor when stopping early.
    """

    def __init__(
        self,
        db: "NonoDB",
        payload: dict[str, Any],
        batch_size: Optional[int] = None,
    ):
        self._db = db
        self._batch_size = batch_size
        result = db.exec(payload)
        self.cursor_id: int = result["cursor_id"]
        self._docs: list[dict[str, Any]] = result["docs"]
        self._has_more: bool = bool(result["has_more"])
        self._closed = False

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __iter__(self):
        while True:
            docs, self._docs = self._docs, []
            for doc in docs:
                yield doc
            if not self._has_more or self._closed:
                break
            self._fetch_more()

    def next_batch(self) -> list[dict[str, Any]]:
        """Returns the next batch, or `[]` when the cursor is exhausted."""
        if self._docs:
            docs, self._docs = self._docs, []
            return docs
        if not self._has_more or self._closed:
            return []
        self._fetch_more()
        docs, self._docs = self._docs, []
        return docs

    def _fetch_more(self) -> None:
        payload: dict[str, Any] = {
            "op": "get_more",
            "cursor_id": self.cursor_id,
        }
        if self._batch_size is not None:
            payload["batch_size"] = self._batch_size
        result = self._db.exec(payload)
        self._docs = result["docs"]
        self._has_more = bool(result["has_more"])
        if not self._docs and self._has_more:
            # A concurrent writer can move the keyset; never loop forever.
            self._has_more = False

    def close(self) -> bool:
        if self._closed:
            return False
        self._closed = True
        self._docs = []
        self._has_more = False
        result = self._db.exec(
            {"op": "close_cursor", "cursor_id": self.cursor_id}
        )
        return bool(result.get("closed"))


class NonoDBError(Exception):
    """Error returned by the daemon (`ok: false`)."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class StreamTruncatedError(NonoDBError):
    """The resume cursor is older than the events retained by the server."""

    def __init__(self, oldest: Optional[str], cursor: Optional[str] = None):
        self.oldest = oldest
        self.cursor = cursor
        super().__init__(
            "stream_truncated",
            f"events before {oldest or cursor or 'the oldest retained id'} "
            "are no longer retained",
        )


class NonoDB:
    def __init__(
        self,
        socket_path: Optional[str] = None,
        timeout: Optional[float] = None,
        cache_ttl: float = 0.0,
        token: Optional[str] = None,
        base_url: Optional[str] = None,
        verify: Optional[bool] = None,
        client_cert: Optional[str] = None,
        client_key: Optional[str] = None,
        ca_cert: Optional[str] = None,
        database: Optional[str] = None,
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
        self.database = (
            database
            if database is not None
            else os.environ.get("NONODB_DB", "default")
        ).strip() or "default"
        self.token = token if token is not None else os.environ.get("NONODB_TOKEN")
        if timeout is None:
            timeout = float(os.environ.get("NONODB_TIMEOUT", "10"))
        if verify is None:
            verify = os.environ.get("NONODB_TLS_VERIFY", "1") not in ("0", "false", "no")
        client_cert = client_cert or os.environ.get("NONODB_TLS_CLIENT_CERT")
        client_key = client_key or os.environ.get("NONODB_TLS_CLIENT_KEY")
        ca_cert = ca_cert or os.environ.get("NONODB_TLS_CA")
        if bool(client_cert) != bool(client_key):
            raise ValueError("client_cert and client_key must be provided together")
        tls_verify = ca_cert if ca_cert else verify
        # httpx 0.28 returns early when `verify` is a CA path and then does not
        # load `cert`. Build the SSL context explicitly so mTLS works reliably.
        if transport is None and (client_cert or ca_cert):
            if verify is False:
                tls_verify = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                tls_verify.check_hostname = False
                tls_verify.verify_mode = ssl.CERT_NONE
            elif ca_cert:
                tls_verify = ssl.create_default_context(cafile=ca_cert)
            else:
                tls_verify = ssl.create_default_context()
            if client_cert:
                tls_verify.load_cert_chain(client_cert, client_key)
        self._http = httpx.Client(
            transport=transport,
            base_url=self.base_url,
            timeout=timeout,
            verify=tls_verify if transport is None else True,
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

    def sync(self) -> dict[str, Any]:
        """Force an fsync of the selected database."""
        return self.exec({"op": "sync"})

    def _data_path(self, path: str) -> str:
        if not self.database or self.database == "default":
            return path
        return f"/{self.database}{path}"

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
        retries = 2 if op in _RETRY_OPS else 0
        attempt = 0
        while True:
            try:
                headers = {"content-type": "application/json"}
                if extra_headers:
                    headers.update(extra_headers)
                if self.token:
                    headers["authorization"] = f"Bearer {self.token}"
                resp = self._http.post(
                    self.base_url + self._data_path("/v1/exec"),
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

    def map_info(self) -> dict[str, Any]:
        """Current LMDB map size, high-water mark, and page size."""
        return self.exec({"op": "map_info"})

    def resize_map(self, size: int) -> dict[str, Any]:
        """Grow the LMDB map online. `size` is in bytes and must be page-aligned."""
        return self.exec({"op": "resize_map", "size": int(size)})

    def collections(self) -> list[str]:
        return self.exec({"op": "list_collections"})["collections"]

    def sql(
        self,
        query: str,
        params: Optional[list[Any]] = None,
    ) -> list[dict[str, Any]]:
        """Runs a read-only SQL SELECT and returns its result rows.

        Parameters are positional and may use `?` or `$1` placeholders.
        """
        return self.exec(
            {"op": "sql", "query": query, "params": list(params or [])}
        )["results"]

    def explain_sql(
        self,
        query: str,
        params: Optional[list[Any]] = None,
    ) -> dict[str, Any]:
        """Returns the plan selected for a read-only SQL SELECT."""
        return self.exec(
            {"op": "explain_sql", "query": query, "params": list(params or [])}
        )

    def batch(self, ops: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Executes a list of **reads** in one LMDB transaction.

        Each result element is `{"ok": true, "result": ...}` or
        `{"ok": false, "error": {"code", "message"}}`. Write operations are
        rejected (there are no multi-document transactions).
        """
        return self.exec({"op": "batch", "ops": ops})["results"]

    def write_batch(
        self,
        ops: list[dict[str, Any]],
        *,
        durability: Optional[str] = None,
    ) -> list[Any]:
        """Atomic write batch: all or nothing.

        Allowed operations: `insert`, `insert_many`, `update`, `delete`.
        Each must contain `coll`. Returns results in operation order.
        `durability` is `full` (default), `relaxed`, or `unsafe`.
        """
        payload: dict[str, Any] = {"op": "write_batch", "ops": ops}
        if durability is not None:
            payload["durability"] = durability
        return self.exec(payload)["results"]

    def create_collection(
        self,
        name: str,
        profile: str = "standard",
        ttl_seconds: int | None = None,
        ttl_purge_history: bool | None = None,
    ) -> dict[str, Any]:
        """Creates a collection explicitly. Use `profile="vault"` for encrypted storage.

        `ttl_seconds` sets a default document TTL; individual documents can
        override it with a `_ttl` field.
        """
        req: dict[str, Any] = {
            "op": "create_collection",
            "coll": name,
            "profile": profile,
        }
        if ttl_seconds is not None:
            req["ttl_seconds"] = ttl_seconds
        if ttl_purge_history is not None:
            req["ttl_purge_history"] = ttl_purge_history
        return self.exec(req)

    def set_collection_ttl(
        self,
        name: str,
        ttl_seconds: int | None,
        purge_history: bool | None = None,
    ) -> dict[str, Any]:
        """Sets the default document TTL, or clears it with `None`.

        `purge_history=True` removes stored revisions when TTL expires a
        document instead of appending a delete tombstone.
        """
        req: dict[str, Any] = {
            "op": "set_collection_ttl",
            "coll": name,
            "ttl_seconds": ttl_seconds,
        }
        if purge_history is not None:
            req["purge_history"] = purge_history
        return self.exec(req)

    def get_collection_ttl(self, name: str) -> int | None:
        """Returns the collection default TTL in seconds, or `None`."""
        return self.exec({"op": "get_collection_ttl", "coll": name})["ttl_seconds"]

    def set_collection_history(self, name: str, max_versions: int) -> dict[str, Any]:
        """Sets per-collection revision history retention. `0` disables it."""
        return self.exec(
            {
                "op": "set_collection_history",
                "coll": name,
                "max_versions": max_versions,
            }
        )

    def get_collection_history(self, name: str) -> int:
        """Returns the collection revision history retention. `0` means disabled."""
        return self.exec({"op": "get_collection_history", "coll": name})[
            "max_versions"
        ]

    def set_collection_events(
        self,
        name: str,
        enabled: bool,
        fields: Optional[list[str]] = None,
        *,
        ops: Optional[list[str]] = None,
        filter: Optional[dict[str, Any]] = None,
        include_before: bool = True,
        include_after: bool = True,
        projection: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Enables automatic change events for a collection.

        `fields` limits update events to changed fields. `ops` limits event
        operations. `filter` is matched against the complete event document
        before it is stored. `projection` is applied to before/after payloads.
        """
        payload: dict[str, Any] = {
            "op": "set_collection_events",
            "coll": name,
            "enabled": bool(enabled),
            "fields": list(fields or []),
            "ops": list(ops or []),
            "filter": filter or {},
            "include_before": include_before,
            "include_after": include_after,
        }
        if projection is not None:
            payload["projection"] = projection
        return self.exec(payload)

    def get_collection_events(self, name: str) -> dict[str, Any]:
        """Returns automatic change-event settings for a collection."""
        return self.exec({"op": "get_collection_events", "coll": name})

    def create_subscription(
        self,
        coll: str,
        name: str,
        *,
        filter: Optional[dict[str, Any]] = None,
        count: Optional[dict[str, Any]] = None,
        ops: Optional[list[str]] = None,
        once: bool = False,
        event_type: Optional[str] = None,
        lease_seconds: int = 600,
    ) -> dict[str, Any]:
        """Creates/replaces a conditional subscription on a collection.

        The client must renew the lease before it expires; the default is
        10 minutes. Use `renew_subscription` from a keepalive task.
        """
        payload: dict[str, Any] = {
            "op": "create_subscription",
            "coll": coll,
            "name": name,
            "ops": list(ops or []),
            "filter": filter or {},
            "once": once,
            "lease_seconds": lease_seconds,
        }
        if count is not None:
            payload["count"] = count
        if event_type is not None:
            payload["event_type"] = event_type
        return self.exec(payload)

    def renew_subscription(
        self,
        coll: str,
        name: str,
        *,
        lease_seconds: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        """Renews a persistent subscription lease. Returns ``None`` if gone."""
        payload: dict[str, Any] = {
            "op": "renew_subscription",
            "coll": coll,
            "name": name,
        }
        if lease_seconds is not None:
            payload["lease_seconds"] = lease_seconds
        return self.exec(payload).get("subscription")

    def ping_subscription(
        self,
        coll: str,
        name: str,
        *,
        lease_seconds: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        """Alias for `renew_subscription`."""
        return self.renew_subscription(coll, name, lease_seconds=lease_seconds)

    def delete_subscription(self, coll: str, name: str) -> bool:
        """Deletes a conditional subscription."""
        return self.exec(
            {"op": "delete_subscription", "coll": coll, "name": name}
        )["deleted"]

    def list_subscriptions(self, coll: str) -> list[dict[str, Any]]:
        """Lists conditional subscriptions on a collection."""
        return self.exec({"op": "list_subscriptions", "coll": coll})[
            "subscriptions"
        ]

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
    def insert(self, doc: dict[str, Any], *, durability: Optional[str] = None) -> str:
        payload: dict[str, Any] = {"op": "insert", "coll": self.name, "doc": doc}
        if durability is not None:
            payload["durability"] = durability
        return self._db.exec(payload)["inserted_id"]

    def insert_many(
        self,
        docs: Iterable[dict[str, Any]],
        *,
        durability: Optional[str] = None,
    ) -> list[str]:
        payload: dict[str, Any] = {
            "op": "insert_many",
            "coll": self.name,
            "docs": list(docs),
        }
        if durability is not None:
            payload["durability"] = durability
        return self._db.exec(payload)["inserted_ids"]

    def update(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        multi: bool = False,
        *,
        upsert: bool = False,
        array_filters: Optional[list[dict[str, Any]]] = None,
        durability: Optional[str] = None,
    ) -> int:
        """Updates documents; with `upsert=True`, inserts a new one when nothing matches."""
        return self.update_result(
            filter,
            update,
            multi=multi,
            upsert=upsert,
            array_filters=array_filters,
            durability=durability,
        )["modified"]

    def update_result(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        multi: bool = False,
        *,
        upsert: bool = False,
        array_filters: Optional[list[dict[str, Any]]] = None,
        durability: Optional[str] = None,
    ) -> dict[str, Any]:
        """Returns `{matched, modified, upserted_id}` (useful for upserts)."""
        payload: dict[str, Any] = {
            "op": "update",
            "coll": self.name,
            "filter": filter,
            "update": update,
            "multi": multi,
            "upsert": upsert,
        }
        if array_filters is not None:
            payload["array_filters"] = array_filters
        if durability is not None:
            payload["durability"] = durability
        return self._db.exec(payload)

    def find_and_modify(
        self,
        filter: dict[str, Any],
        update: Optional[dict[str, Any]] = None,
        *,
        remove: bool = False,
        new: bool = False,
        upsert: bool = False,
        sort: Optional[list[tuple[str, int]]] = None,
        array_filters: Optional[list[dict[str, Any]]] = None,
        projection: Optional[dict[str, Any]] = None,
    ) -> Optional[dict[str, Any]]:
        """Atomically updates/removes one document and returns its old or new image."""
        return self.find_and_modify_result(
            filter,
            update,
            remove=remove,
            new=new,
            upsert=upsert,
            sort=sort,
            array_filters=array_filters,
            projection=projection,
        )["value"]

    def find_and_modify_result(
        self,
        filter: dict[str, Any],
        update: Optional[dict[str, Any]] = None,
        *,
        remove: bool = False,
        new: bool = False,
        upsert: bool = False,
        sort: Optional[list[tuple[str, int]]] = None,
        array_filters: Optional[list[dict[str, Any]]] = None,
        projection: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Returns `{value, matched, modified, upserted_id}`."""
        payload: dict[str, Any] = {
            "op": "find_and_modify",
            "coll": self.name,
            "filter": filter,
            "new": new,
            "upsert": upsert,
            "remove": remove,
        }
        if update is not None:
            payload["update"] = update
        if sort is not None:
            payload["sort"] = sort
        if array_filters is not None:
            payload["array_filters"] = array_filters
        if projection is not None:
            payload["projection"] = projection
        return self._db.exec(payload)

    def delete(
        self,
        filter: dict[str, Any],
        multi: bool = False,
        *,
        durability: Optional[str] = None,
    ) -> int:
        payload: dict[str, Any] = {
            "op": "delete",
            "coll": self.name,
            "filter": filter,
            "multi": multi,
        }
        if durability is not None:
            payload["durability"] = durability
        return self._db.exec(payload)["deleted"]

    def write_batch(
        self,
        ops: list[dict[str, Any]],
        *,
        durability: Optional[str] = None,
    ) -> list[Any]:
        """Atomic write batch in this collection (`coll` is added automatically)."""
        return self._db.write_batch(
            [{**op, "coll": op.get("coll", self.name)} for op in ops],
            durability=durability,
        )

    # -- indexes -----------------------------------------------------
    def create_index(
        self,
        field: Optional[str] = None,
        unique: bool = False,
        *,
        fields: Optional[list[str]] = None,
        include: Optional[list[str]] = None,
        geo: bool = False,
    ) -> bool:
        """Creates a single-field (`field`) or compound (`fields`) index.

        `include` copies extra fields into the index, allowing a projection to be covered
        without reading the document (covering index). Set `geo=True` to create a
        single-field GeoJSON point index for `$near` and `$geoWithin`.
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
                "geo": geo,
            }
        )
        return True

    def create_indexes(self, indexes: list[dict[str, Any]]) -> list[str]:
        """Creates several indexes in one collection scan.

        Each entry accepts the same keys as :meth:`create_index`:
        `field` or `fields`, optional `unique`, optional `include`, and optional
        `geo` for a single-field GeoJSON point index.
        """
        result = self._db.exec(
            {
                "op": "create_indexes",
                "coll": self.name,
                "indexes": indexes,
            }
        )
        return result["created"]

    def drop_index(self, field: str) -> bool:
        return self._db.exec(
            {"op": "drop_index", "coll": self.name, "field": field}
        )["dropped"]

    def list_indexes(self) -> list[dict[str, Any]]:
        return self._db.exec({"op": "list_indexes", "coll": self.name})["indexes"]

    def lookup(
        self,
        by: Union[str, Iterable[str]],
        values: Iterable[Any],
        *,
        fields: Optional[Iterable[str]] = None,
        projection: Optional[dict[str, Any]] = None,
    ) -> list[Optional[dict[str, Any]]]:
        """Ordered point lookups by `_id` or a unique index.

        `by` is one field name or a list of fields for a compound unique index.
        For a compound lookup each value must be an array with the same number
        of elements as `by`. Missing or malformed values produce `None`.

        `fields` is an inclusive projection and can be served from a covering
        index. `projection` accepts a normal flat projection including `_id: 0`;
        it cannot be combined with `fields`.
        """
        if fields is not None and projection is not None:
            raise ValueError("lookup accepts either fields or projection, not both")
        payload: dict[str, Any] = {
            "op": "lookup",
            "coll": self.name,
            "by": by if isinstance(by, str) else list(by),
            "values": list(values),
        }
        if fields is not None:
            payload["fields"] = list(fields)
        if projection is not None:
            payload["projection"] = projection
        return self._db.exec(payload)["results"]

    def lookup_one(
        self,
        by: Union[str, Iterable[str]],
        value: Any,
        *,
        fields: Optional[Iterable[str]] = None,
        projection: Optional[dict[str, Any]] = None,
    ) -> Optional[dict[str, Any]]:
        """Returns one document by `_id` or a unique index."""
        return self.lookup(by, [value], fields=fields, projection=projection)[0]

    def get_many(self, ids: Iterable[str]) -> list[dict[str, Any]]:
        """Fetches multiple documents by `_id` in one lookup request."""
        return [doc for doc in self.lookup("_id", ids) if doc is not None]

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
        daemon by setting per-collection history retention.
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

    # -- vector search ----------------------------------------------
    def create_vector_index(
        self,
        field: str,
        dims: int,
        metric: str = "cosine",
    ) -> bool:
        """Creates an exact vector index.

        `metric` is `cosine` (default), `dot`, or `l2`. Vectors must be JSON
        arrays of exactly `dims` finite numbers. Missing or invalid vectors are
        not indexed.
        """
        self._db.exec(
            {
                "op": "create_vector_index",
                "coll": self.name,
                "field": field,
                "dims": dims,
                "metric": metric,
            }
        )
        return True

    def drop_vector_index(self, field: str) -> bool:
        return self._db.exec(
            {"op": "drop_vector_index", "coll": self.name, "field": field}
        )["dropped"]

    def list_vector_indexes(self) -> list[dict[str, Any]]:
        return self._db.exec(
            {"op": "list_vector_indexes", "coll": self.name}
        )["vector_indexes"]

    def vector_search(
        self,
        field: str,
        vector: list[float],
        *,
        filter: Optional[dict[str, Any]] = None,
        skip: int = 0,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Exact vector search. Results include `_distance`, lowest first."""
        return self._db.exec(
            {
                "op": "vector_search",
                "coll": self.name,
                "field": field,
                "vector": vector,
                "filter": filter or {},
                "skip": skip,
                "limit": limit,
            }
        )["results"]

    # -- read --------------------------------------------------------
    def watch_once(
        self,
        after: Optional[str] = None,
        *,
        limit: int = 100,
        timeout_ms: int = 1000,
        types: Optional[Iterable[str]] = None,
        filter: Optional[dict[str, Any]] = None,
        projection: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Fetches one long-poll batch from an append-only event collection.

        Results are ordered by `_id`. `next` is the resume token for the next
        request. `timed_out` is true when no event arrived before the timeout.
        `truncated` means `after` is older than retained events; `oldest` is the
        oldest event id still available.
        """
        payload: dict[str, Any] = {
            "op": "watch",
            "coll": self.name,
            "limit": limit,
            "timeout_ms": timeout_ms,
        }
        if after:
            payload["after"] = after
        if types is not None:
            payload["types"] = list(types)
        if filter is not None:
            payload["filter"] = filter
        if projection is not None:
            payload["projection"] = projection
        return self._db.exec(payload)

    def watch(
        self,
        after: Optional[str] = None,
        *,
        limit: int = 100,
        timeout_ms: int = 1000,
        types: Optional[Iterable[str]] = None,
        filter: Optional[dict[str, Any]] = None,
        projection: Optional[dict[str, Any]] = None,
        retry: bool = True,
        retry_delay: float = 1.0,
        max_retry_delay: float = 30.0,
    ) -> Iterator[dict[str, Any]]:
        """Yields events from an append-only collection as they arrive.

        This is a long-polling generator. It reconnects after transport errors
        with exponential backoff. It raises `StreamTruncatedError` when the
        resume cursor is older than retained events. `after` is the last
        processed event `_id`, or `None` to start from the beginning.
        """
        cursor = after
        delay = max(0.001, retry_delay)
        while True:
            try:
                batch = self.watch_once(
                    cursor,
                    limit=limit,
                    timeout_ms=timeout_ms,
                    types=types,
                    filter=filter,
                    projection=projection,
                )
                if batch.get("truncated"):
                    raise StreamTruncatedError(batch.get("oldest"), cursor)
                for event in batch.get("events", []):
                    yield event
                cursor = batch.get("next") or cursor
                delay = max(0.001, retry_delay)
            except NonoDBError as exc:
                if not retry or exc.code != "connection":
                    raise
                time.sleep(delay)
                delay = min(max_retry_delay, delay * 2)

    def watch_stream(
        self,
        after: Optional[str] = None,
        *,
        limit: int = 100,
        types: Optional[Iterable[str]] = None,
        filter: Optional[dict[str, Any]] = None,
        projection: Optional[dict[str, Any]] = None,
        heartbeat_ms: Optional[int] = None,
        timeout_ms: Optional[int] = None,
        overlap_ms: Optional[int] = None,
        threshold: Optional[dict[str, Any]] = None,
        mode: Optional[str] = None,
        ops: Optional[Iterable[str]] = None,
        count: Optional[dict[str, Any]] = None,
        once: bool = False,
        name: Optional[str] = None,
        event_type: Optional[str] = None,
        retry: bool = True,
        retry_delay: float = 1.0,
        max_retry_delay: float = 30.0,
    ) -> Iterator[dict[str, Any]]:
        """Yields events from the NDJSON `/v1/watch` stream.

        This is a true streaming connection, not long polling. It reconnects
        from the last event `_id` after transport errors and raises
        `StreamTruncatedError` when the server reports a gap. Vault collections
        are not streamable and are rejected by the daemon.
        """
        db = self._db
        http = getattr(db, "_http", None)
        if http is None:
            raise NonoDBError(
                "unsupported_encrypted_operation",
                "watch_stream is not supported for vault collections",
            )
        cursor = after
        delay = max(0.001, retry_delay)
        while True:
            live = mode == "live"
            payload: dict[str, Any] = {
                "op": "watch",
                "coll": self.name,
                "limit": limit,
            }
            if live:
                payload["mode"] = "live"
            elif cursor:
                payload["after"] = cursor
            if types is not None and not live:
                payload["types"] = list(types)
            if filter is not None:
                payload["filter"] = filter
            if projection is not None and not live:
                payload["projection"] = projection
            if heartbeat_ms is not None:
                payload["heartbeat_ms"] = heartbeat_ms
            if timeout_ms is not None:
                payload["timeout_ms"] = timeout_ms
            if overlap_ms is not None and not live:
                payload["overlap_ms"] = overlap_ms
            if threshold is not None and not live:
                payload["threshold"] = threshold
            if live:
                if ops is not None:
                    payload["ops"] = list(ops)
                if count is not None:
                    payload["count"] = count
                if once:
                    payload["once"] = True
                if name is not None:
                    payload["name"] = name
                if event_type is not None:
                    payload["event_type"] = event_type
            headers = {
                "content-type": "application/json",
                "accept": "application/x-ndjson",
            }
            token = getattr(db, "token", None)
            if token:
                headers["authorization"] = f"Bearer {token}"
            try:
                with http.stream(
                    "POST",
                    db.base_url + db._data_path("/v1/watch"),
                    content=_dumps(payload),
                    headers=headers,
                    timeout=None,
                ) as resp:
                    if resp.status_code != 200:
                        raw = resp.read()
                        try:
                            body = _loads(raw)
                            err = body.get("error") or {}
                        except ValueError:
                            err = {}
                        raise NonoDBError(
                            err.get("code", "http_error"),
                            err.get("message", f"HTTP {resp.status_code}"),
                        )
                    for raw_line in resp.iter_lines():
                        line = raw_line.strip()
                        if not line:
                            continue
                        item = _loads(line)
                        if "heartbeat" in item:
                            continue
                        if "error" in item:
                            err = item.get("error") or {}
                            raise NonoDBError(
                                err.get("code", "error"), err.get("message", "")
                            )
                        if "stream" in item:
                            info = item["stream"]
                            if info.get("truncated"):
                                raise StreamTruncatedError(
                                    info.get("oldest"), cursor
                                )
                            continue
                        event_id = item.get("_id")
                        if event_id:
                            cursor = event_id
                        yield item
                if not retry:
                    return
                delay = max(0.001, retry_delay)
                time.sleep(delay)
            except httpx.TransportError as exc:
                if not retry:
                    raise NonoDBError("connection", str(exc)) from exc
                time.sleep(delay)
                delay = min(max_retry_delay, delay * 2)

    def watch_live(
        self,
        *,
        filter: Optional[dict[str, Any]] = None,
        count: Optional[dict[str, Any]] = None,
        ops: Optional[Iterable[str]] = None,
        once: bool = False,
        heartbeat_ms: int = 10_000,
        timeout_ms: Optional[int] = None,
        name: Optional[str] = None,
        event_type: Optional[str] = None,
        retry: bool = True,
        retry_delay: float = 1.0,
        max_retry_delay: float = 30.0,
    ) -> Iterator[dict[str, Any]]:
        """Yields matching changes over a connection-scoped live SSE stream.

        The subscription exists only while the stream is connected and is
        removed immediately on disconnect (or after a short idle grace period
        for half-open connections). Live events are not written to `_events`.
        """
        return self.watch_stream(
            mode="live",
            filter=filter,
            count=count,
            ops=ops,
            once=once,
            heartbeat_ms=heartbeat_ms,
            timeout_ms=timeout_ms,
            name=name,
            event_type=event_type,
            retry=retry,
            retry_delay=retry_delay,
            max_retry_delay=max_retry_delay,
        )

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

    def explain(
        self,
        filter: Optional[dict[str, Any]] = None,
        *,
        sort: Optional[list[tuple[str, int]]] = None,
        skip: int = 0,
        limit: Optional[int] = None,
    ) -> dict[str, Any]:
        """Returns the selected access path, index/bounds, and actual counts."""
        payload: dict[str, Any] = {
            "op": "explain",
            "coll": self.name,
            "filter": filter or {},
            "skip": skip,
        }
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

    # -- cursors -----------------------------------------------------
    def find_cursor(
        self,
        filter: Optional[dict[str, Any]] = None,
        *,
        projection: Optional[dict[str, Any]] = None,
        sort: Optional[list[tuple[str, int]]] = None,
        skip: int = 0,
        limit: Optional[int] = None,
        batch_size: Optional[int] = None,
    ) -> Cursor:
        """Opens a keyset cursor. Sort must be empty or a single `_id` field."""
        payload: dict[str, Any] = {
            "op": "cursor_find",
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
        if batch_size is not None:
            payload["batch_size"] = batch_size
        return Cursor(self._db, payload, batch_size)

    def aggregate_cursor(
        self,
        pipeline: list[dict[str, Any]],
        *,
        limit: Optional[int] = None,
        batch_size: Optional[int] = None,
    ) -> Cursor:
        """Opens a cursor over an optional leading `$match` and document-local
        stages (`$project`, `$addFields`/`$set`, `$unset`)."""
        payload: dict[str, Any] = {
            "op": "cursor_aggregate",
            "coll": self.name,
            "pipeline": pipeline,
        }
        if limit is not None:
            payload["limit"] = limit
        if batch_size is not None:
            payload["batch_size"] = batch_size
        return Cursor(self._db, payload, batch_size)
