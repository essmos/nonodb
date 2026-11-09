"""Przykład integracji nonodb z FastAPI/uvicorn (wielu workerów).

Uruchomienie (daemon musi działać):
    NONODB_SOCK=/tmp/nonodb.sock uvicorn client.example_fastapi:app --workers 12

Każdy worker tworzy własne połączenie do gniazda w `lifespan` — nie współdziel
klienta między procesami.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from nonodb import NonoDB

SOCK = os.environ.get("NONODB_SOCK", "/run/nonodb/nonodb.sock")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = NonoDB(SOCK)
    yield
    app.state.db.close()


app = FastAPI(lifespan=lifespan)


@app.get("/users")
def list_users(request: Request, min_age: int = 18):
    users = request.app.state.db.collection("users")
    return users.find({"age": {"$gte": min_age}}, sort=[("age", 1)])


@app.post("/users")
def create_user(request: Request, body: dict):
    users = request.app.state.db.collection("users")
    return {"id": users.insert(body)}


@app.post("/counters/{name}/inc")
def inc_counter(request: Request, name: str):
    counters = request.app.state.db.collection("counters")
    counters.update({"name": name}, {"$inc": {"n": 1}}, multi=False)
    doc = counters.find_one({"name": name})
    return {"n": doc["n"] if doc else None}
