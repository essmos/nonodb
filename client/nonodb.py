"""Cienki klient nonodb dla Pythona (HTTP/JSON po gnieździe uniksowym).

Użycie:
    from nonodb import NonoDB
    db = NonoDB("/run/nonodb.sock")
    users = db.collection("users")
    users.insert({"name": "Ala", "age": 30})
    adults = users.find({"age": {"$gte": 18}})
    db.close()

Klient jest cienki i bezstanowy poza połączeniem HTTP — cała logika jest
w daemonie. Reużywaj jednej instancji na proces (worker uvicorn).
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Any, Iterable, Optional

import httpx

try:  # opcjonalnie szybszy JSON; fallback na stdlib
    import orjson as _orjson
except ImportError:  # pragma: no cover
    _orjson = None

__all__ = ["NonoDB", "Collection", "NonoDBError"]

# Operacje bezpieczne do ponowienia po błędzie transportu (idempotentne odczyty).
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
    """Błąd zwrócony przez daemon (`ok: false`)."""

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
        # Opcjonalny cache odczytów. Domyślnie wyłączony (cache_ttl=0).
        # TTL ogranicza nieświeżość danych zapisanych przez inne procesy;
        # każdy zapis przez tę instancję czyści cache w całości.
        self._cache_ttl = cache_ttl
        # Wartości trzymamy jako zserializowany JSON (bez deepcopy).
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
    def exec(self, payload: dict[str, Any]) -> Any:
        op = payload.get("op")
        cache_key: Optional[bytes] = None
        if self._cache_ttl > 0 and op in _READ_OPS:
            cache_key = _cache_key(payload)
            with self._cache_lock:
                hit = self._cache.get(cache_key)
            if hit is not None:
                ts, raw = hit
                if time.monotonic() - ts < self._cache_ttl:
                    return _loads(raw)
                with self._cache_lock:
                    self._cache.pop(cache_key, None)

        result = self._exec_uncached(payload, op)

        if self._cache_ttl > 0:
            with self._cache_lock:
                if cache_key is not None:
                    self._cache[cache_key] = (time.monotonic(), _dumps(result))
                else:
                    # Zapis unieważnia wszystkie odczyty (spójność w obrębie klienta).
                    self._cache.clear()
        return result

    def _exec_uncached(self, payload: dict[str, Any], op: Optional[str]) -> Any:
        # Odczyty ponawiamy po błędzie transportu; zapisów nie (ryzyko duplikatu).
        retries = 2 if op in _READ_OPS else 0
        attempt = 0
        while True:
            try:
                headers = {"content-type": "application/json"}
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
                    # Daemon niedostępny / zerwane połączenie — zawsze nasz typ,
                    # żeby wołający nie musiał znać httpx.
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
        """Wykonuje listę **odczytów** w jednej transakcji LMDB.

        Każdy element wyniku to `{"ok": true, "result": ...}` albo
        `{"ok": false, "error": {"code", "message"}}`. Operacje zapisu są
        odrzucane (brak transakcji wielo-dokumentowych).
        """
        return self.exec({"op": "batch", "ops": ops})["results"]

    def write_batch(self, ops: list[dict[str, Any]]) -> list[Any]:
        """Atomowy batch zapisów: wszystko albo nic.

        Dozwolone operacje: `insert`, `insert_many`, `update`, `delete`.
        Każda musi zawierać `coll`. Zwraca listę wyników w kolejności operacji.
        """
        return self.exec({"op": "write_batch", "ops": ops})["results"]

    def collection(self, name: str) -> "Collection":
        return Collection(self, name)


class Collection:
    def __init__(self, db: NonoDB, name: str):
        self._db = db
        self.name = name

    # -- zapis -------------------------------------------------------
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
        """Aktualizuje dokumenty; z `upsert=True` wstawia nowy, gdy brak trafień."""
        return self.update_result(filter, update, multi=multi, upsert=upsert)["modified"]

    def update_result(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        multi: bool = False,
        *,
        upsert: bool = False,
    ) -> dict[str, Any]:
        """Zwraca `{matched, modified, upserted_id}` (przydatne przy upsercie)."""
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
        """Atomowy batch zapisów w tej kolekcji (`coll` dodawane automatycznie)."""
        return self._db.write_batch(
            [{**op, "coll": op.get("coll", self.name)} for op in ops]
        )

    # -- indeksy -----------------------------------------------------
    def create_index(
        self,
        field: Optional[str] = None,
        unique: bool = False,
        *,
        fields: Optional[list[str]] = None,
        include: Optional[list[str]] = None,
    ) -> bool:
        """Tworzy indeks pojedynczy (`field`) albo złożony (`fields`).

        `include` kopiuje dodatkowe pola do indeksu, pozwalając pokryć projekcję
        bez odczytu dokumentu (covering index).
        """
        if fields is None:
            if field is None:
                raise ValueError("podaj `field` albo `fields`")
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
        """Pobiera wiele dokumentów po `_id` jednym żądaniem (batch)."""
        ops = [
            {"op": "find_one", "coll": self.name, "filter": {"_id": i}} for i in ids
        ]
        return [
            r["result"]
            for r in self._db.batch(ops)
            if r.get("ok") and r.get("result") is not None
        ]

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
        """Full-text (BM25). Wyniki mają dodatkowe `_score`, malejąco."""
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
        """Wyszukiwanie przybliżone. Wyniki mają `_distance`, rosnąco."""
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

    # -- odczyt ------------------------------------------------------
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
