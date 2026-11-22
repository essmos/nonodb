"""Example nonodb integration with FastAPI/uvicorn (multiple workers).

Run with (the daemon must be running):
    NONODB_SOCK=/tmp/nonodb.sock uvicorn client.example_fastapi:app --workers 12

Each worker creates its own socket connection in `lifespan` - do not share
the client between processes.
"""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

from nonodb import AsyncNonoDB, NonoDB

SOCK = os.environ.get("NONODB_SOCK", "/run/nonodb/nonodb.sock")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = NonoDB(SOCK)
    app.state.async_db = AsyncNonoDB(SOCK)
    yield
    app.state.db.close()
    await app.state.async_db.close()


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


@app.get("/orders/stream")
async def stream_orders(request: Request):
    async def events():
        async for event in request.app.state.async_db.collection("orders").watch_live(
            filter={"status": "paid"},
            heartbeat_ms=10_000,
        ):
            yield json.dumps(event, separators=(",", ":")) + "\n"

    return StreamingResponse(events(), media_type="application/x-ndjson")
