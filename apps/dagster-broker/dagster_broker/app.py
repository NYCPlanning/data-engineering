"""HTTP API: ``uvicorn dagster_broker.app:create_app --factory``.

404 means the queue doesn't know the item (never existed, expired, or Valkey restarted):
a producer re-enqueues with the same idempotency key, a worker abandons the attempt.
409 means the (item, attempt, worker) token no longer holds the item.
"""

import hmac
import json
import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import Any

import redis
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from dagster_broker.store import NoSuchItem, Stale, Store

logger = logging.getLogger(__name__)
MAX_EVENT_BYTES = 1_000_000


class Enqueue(BaseModel):
    required_caps: list[str] = []
    priority: int = 0
    max_attempts: int = Field(default=3, ge=1)
    payload: dict[str, Any] = {}
    idempotency_key: str | None = None
    run_id: str | None = None
    asset_key: str | None = None
    partition_key: str | None = None


class Claim(BaseModel):
    worker_id: str = Field(min_length=1)
    caps: list[str] = []
    affinity_run_ids: list[str] = []


class Token(BaseModel):
    worker_id: str
    attempt: int


class Complete(Token):
    result: dict[str, Any] = {}


class Fail(Token):
    error: str | None = None
    retryable: bool = True


class Event(BaseModel):
    kind: str = Field(min_length=1)
    data: dict[str, Any] = {}


class Events(Token):
    events: list[Event]


def create_app() -> FastAPI:
    env = os.environ.get
    url = env("DAGSTER_BROKER_VALKEY_URL", "redis://localhost:6379/0")
    key = env("DAGSTER_BROKER_SHARED_KEY")
    lease = float(env("DAGSTER_BROKER_LEASE_SECONDS", 60))
    reap_every = float(env("DAGSTER_BROKER_REAP_SECONDS", 15))
    ttl = int(env("DAGSTER_BROKER_TERMINAL_TTL_SECONDS", 24 * 3600))
    r = redis.Redis.from_url(url, decode_responses=True)
    store = Store(r, ttl)

    def reaper(stop: threading.Event) -> None:
        while not stop.wait(reap_every):
            try:
                if reaped := store.reap():
                    logger.warning("reaped expired leases: %s", reaped)
            except Exception:
                logger.exception("reap failed")

    @asynccontextmanager
    async def lifespan(app):
        stop = threading.Event()
        threading.Thread(target=reaper, args=(stop,), daemon=True).start()
        yield
        stop.set()

    def check_key(x_dagster_broker_key: str = Header("")) -> None:
        if key and not hmac.compare_digest(x_dagster_broker_key.encode(), key.encode()):
            raise HTTPException(401, "bad or missing X-Dagster-Broker-Key")

    app = FastAPI(title="dagster-broker", lifespan=lifespan)
    api = APIRouter(dependencies=[Depends(check_key)])

    def error(status: int, detail: str):
        return lambda request, exc: JSONResponse({"detail": detail}, status)

    app.add_exception_handler(NoSuchItem, error(404, "no such item"))
    app.add_exception_handler(Stale, error(409, "stale lease"))
    app.add_exception_handler(redis.RedisError, error(503, "valkey unavailable"))

    @app.get("/readyz")
    def readyz():
        r.ping()
        return {"ok": True}

    @api.post("/items", status_code=201)
    def enqueue(body: Enqueue, response: Response):
        item, inserted = store.enqueue(**body.model_dump())
        if not inserted:
            response.status_code = 200
        return item

    @api.get("/items/{item_id}")
    def get_item(item_id: str):
        if (item := store.get(item_id)) is None:
            raise NoSuchItem()
        return item

    @api.get("/items/{item_id}/events")
    def get_events(
        item_id: str,
        after: str | None = Query(None, pattern=r"^\d+-\d+$"),
        limit: int = Query(1000, ge=1, le=10_000),
    ):
        events, cursor = store.events(item_id, after, limit)
        if not events and store.get(item_id) is None:
            raise NoSuchItem()
        return {"events": events, "cursor": cursor}

    @api.post("/items/{item_id}/cancel")
    def cancel(item_id: str):
        return store.cancel(item_id)

    @api.post("/work/claim")
    def claim(body: Claim):
        item = store.claim(body.worker_id, body.caps, body.affinity_run_ids, lease)
        return item or Response(status_code=204)

    @api.post("/work/{item_id}/heartbeat")
    def heartbeat(item_id: str, body: Token):
        return store.heartbeat(item_id, body.attempt, body.worker_id, lease)

    @api.post("/work/{item_id}/events", status_code=201)
    def add_events(item_id: str, body: Events):
        events = [e.model_dump() for e in body.events]
        if (
            len(json.dumps(events)) > MAX_EVENT_BYTES
        ):  # don't hold the lock for big writes
            raise HTTPException(
                413, "event batch over 1 MB; put large data in the gateway"
            )
        return {"ids": store.add_events(item_id, body.attempt, body.worker_id, events)}

    @api.post("/work/{item_id}/complete")
    def complete(item_id: str, body: Complete):
        return store.complete(item_id, body.attempt, body.worker_id, body.result)

    @api.post("/work/{item_id}/fail")
    def fail(item_id: str, body: Fail):
        token = (item_id, body.attempt, body.worker_id)
        return store.fail(*token, body.error, body.retryable)

    @api.get("/metrics/demand")
    def demand(caps: list[str] = Query([]), min_age_seconds: float = Query(0, ge=0)):
        """For KEDA's metrics-api scaler (valueLocation: demand). caps may repeat or be
        comma-separated."""
        have = {c for v in caps for c in v.split(",") if c}
        return store.stats(have, min_age_seconds)

    app.include_router(api)
    return app
