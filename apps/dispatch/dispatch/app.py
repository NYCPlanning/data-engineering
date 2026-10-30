"""HTTP API. Run with ``uvicorn dispatch.app:create_app --factory``.

The routes are on an APIRouter, so the queue can be mounted into another ASGI app later
without changes. Endpoints are sync ``def``s: FastAPI runs them in its threadpool, each
taking a connection from the redis-py pool for the length of one script call.

404 means the queue doesn't know the item: it never existed, finished long enough ago to
expire, or Valkey restarted (it keeps nothing on disk). Clients must treat that as normal:
a producer re-enqueues with the same idempotency key; a worker abandons the attempt.
"""

from __future__ import annotations

import hmac
import logging
import threading
from contextlib import asynccontextmanager
from typing import Any

import redis
from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
)
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, model_validator

from dispatch import store
from dispatch.config import Settings

logger = logging.getLogger(__name__)


# ---- request bodies ---------------------------------------------------------------


class EnqueueRequest(BaseModel):
    run_id: str | None = None
    asset_key: str | None = None
    partition_key: str | None = None
    idempotency_key: str | None = Field(default=None, min_length=1)
    required_caps: list[str] = Field(default_factory=list)
    priority: int = Field(default=0, ge=-(10**9), le=10**9)
    max_attempts: int = Field(default=3, ge=1)
    payload: dict[str, Any] = Field(default_factory=dict)


class ClaimRequest(BaseModel):
    worker_id: str = Field(min_length=1)
    caps: list[str] = Field(default_factory=list)
    affinity_run_ids: list[str] = Field(default_factory=list)


class Fenced(BaseModel):
    """The fencing token for one claim: the item id is in the path."""

    worker_id: str = Field(min_length=1)
    attempt: int = Field(ge=1)


class CompleteRequest(Fenced):
    result: dict[str, Any] = Field(default_factory=dict)


class FailRequest(Fenced):
    error: str | None = None
    retryable: bool = True


class Event(BaseModel):
    kind: str = Field(min_length=1)
    data: dict[str, Any] = Field(default_factory=dict)


class EventRequest(Fenced):
    """One event (``kind`` + ``data``) or a batch (``events``), appended all or nothing."""

    kind: str | None = Field(default=None, min_length=1)
    data: dict[str, Any] = Field(default_factory=dict)
    events: list[Event] = Field(default_factory=list)

    @model_validator(mode="after")
    def one_form(self) -> EventRequest:
        if (self.kind is None) == (not self.events):
            raise ValueError("give either kind (+ data) or a non-empty events list")
        return self

    def batch(self) -> list[tuple[str, dict]]:
        if self.kind is not None:
            return [(self.kind, self.data)]
        return [(e.kind, e.data) for e in self.events]


# ---- dependencies -----------------------------------------------------------------


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_store(request: Request) -> store.ValkeyStore:
    return request.app.state.store


def require_key(
    request: Request, x_dispatch_key: str | None = Header(default=None)
) -> None:
    expected = request.app.state.settings.shared_key
    if expected is None:
        return
    if x_dispatch_key is None or not hmac.compare_digest(
        x_dispatch_key.encode(), expected.encode()
    ):
        raise HTTPException(status_code=401, detail="bad or missing X-Dispatch-Key")


def not_found() -> HTTPException:
    return HTTPException(
        status_code=404,
        detail="no such item (never existed, expired, or the queue restarted)",
    )


def fenced(fn, *args, **kwargs):
    """Map the store's fencing errors to HTTP."""
    try:
        return fn(*args, **kwargs)
    except store.NoSuchItem:
        raise not_found()
    except store.StaleLease:
        raise HTTPException(
            status_code=409,
            detail="stale lease: this (item, attempt, worker) no longer holds the item",
        )
    except store.EventsFull:
        raise HTTPException(status_code=429, detail="item has too many events")


# ---- routes -----------------------------------------------------------------------

router = APIRouter(dependencies=[Depends(require_key)])


@router.post("/items", status_code=201)
def enqueue_item(
    body: EnqueueRequest,
    response: Response,
    s: store.ValkeyStore = Depends(get_store),
) -> dict:
    item, inserted = s.enqueue(**body.model_dump())
    if not inserted:
        response.status_code = 200
    return item


@router.get("/items/{item_id}")
def get_item(item_id: int, s: store.ValkeyStore = Depends(get_store)) -> dict:
    item = s.get_item(item_id)
    if item is None:
        raise not_found()
    return item


@router.get("/items/{item_id}/events")
def get_events(
    item_id: int,
    after: str | None = Query(default=None, pattern=r"^\d+-\d+$"),
    attempt: int | None = Query(default=None, ge=1),
    limit: int = Query(default=1000, ge=1, le=10_000),
    s: store.ValkeyStore = Depends(get_store),
) -> dict:
    """Events oldest first, after the cursor ``after`` (exclusive). Poll with the returned
    ``cursor``. 404 if the item is gone (its events are gone with it)."""
    events, cursor = s.list_events(item_id, after=after, attempt=attempt, limit=limit)
    if not events and s.get_item(item_id) is None:
        raise not_found()
    return {"events": events, "cursor": cursor}


@router.post("/items/{item_id}/cancel")
def cancel_item(item_id: int, s: store.ValkeyStore = Depends(get_store)) -> dict:
    item, _ = fenced(s.cancel, item_id)
    return item  # an already-terminal item is returned unchanged


@router.post("/work/claim", responses={204: {"description": "nothing to claim"}})
def claim(
    body: ClaimRequest,
    s: store.ValkeyStore = Depends(get_store),
    settings: Settings = Depends(get_settings),
) -> Any:
    item = s.claim(
        worker_id=body.worker_id,
        caps=body.caps,
        affinity_run_ids=body.affinity_run_ids,
        lease_seconds=settings.lease_seconds,
    )
    if item is None:
        return Response(status_code=204)
    return item


@router.post("/work/{item_id}/heartbeat")
def heartbeat(
    item_id: int,
    body: Fenced,
    s: store.ValkeyStore = Depends(get_store),
    settings: Settings = Depends(get_settings),
) -> dict:
    hb = fenced(
        s.heartbeat,
        item_id,
        attempt=body.attempt,
        worker_id=body.worker_id,
        lease_seconds=settings.lease_seconds,
    )
    return {"lease_until": hb.lease_until, "cancel": hb.cancel}


@router.post("/work/{item_id}/events", status_code=201)
def add_events(
    item_id: int, body: EventRequest, s: store.ValkeyStore = Depends(get_store)
) -> dict:
    ids = fenced(
        s.add_events,
        item_id,
        attempt=body.attempt,
        worker_id=body.worker_id,
        events=body.batch(),
    )
    return {"ids": ids}


@router.post("/work/{item_id}/complete")
def complete(
    item_id: int, body: CompleteRequest, s: store.ValkeyStore = Depends(get_store)
) -> dict:
    return fenced(s.complete, item_id, **body.model_dump())


@router.post("/work/{item_id}/fail")
def fail(
    item_id: int, body: FailRequest, s: store.ValkeyStore = Depends(get_store)
) -> dict:
    return fenced(s.fail, item_id, **body.model_dump())


@router.get("/metrics/demand")
def demand(
    caps: list[str] | None = Query(default=None),
    min_age_seconds: float = Query(default=0, ge=0),
    s: store.ValkeyStore = Depends(get_store),
) -> dict:
    """For the KEDA metrics-api scaler (``valueLocation: demand``): how many queued items
    a worker with ``caps`` could claim, counting only items that have waited at least
    ``min_age_seconds`` (for a local-first fallback). ``caps`` may repeat or be
    comma-separated; omitted, it means a worker with no caps."""
    have = sorted({c for v in caps or [] for c in v.split(",") if c})
    st = s.stats(caps=have, min_age_seconds=min_age_seconds)
    return {
        "demand": st.demand,
        "caps": have,
        "min_age_seconds": min_age_seconds,
        "queued": st.queued,
        "claimed": st.claimed,
    }


health = APIRouter()


@health.get("/healthz")
def healthz() -> dict:
    return {"ok": True}


@health.get("/readyz")
def readyz(request: Request, response: Response) -> dict:
    ready = store.check_ready(request.app.state.client)
    if not ready.ok:
        response.status_code = 503
    return {"ok": ready.ok, **ready.detail}


# ---- reaper -----------------------------------------------------------------------


class Reaper:
    """Runs :meth:`ValkeyStore.reap` every ``interval`` seconds on a daemon thread."""

    def __init__(self, s: store.ValkeyStore, interval: float):
        self.store = s
        self.interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="reaper", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=10)

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                reaped = self.store.reap()
                if reaped:
                    logger.warning("reaped %d expired leases: %s", len(reaped), reaped)
            except Exception:
                logger.exception("reaper pass failed")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.client = store.connect(
            settings.valkey_url, max_connections=settings.pool_max_size
        )
        app.state.store = store.ValkeyStore(
            app.state.client,
            prefix=settings.key_prefix,
            terminal_ttl_seconds=settings.terminal_ttl_seconds,
            max_events_per_item=settings.max_events_per_item,
        )
        reaper = None
        if settings.reap_interval_seconds > 0:
            reaper = Reaper(app.state.store, settings.reap_interval_seconds)
            reaper.start()
        try:
            yield
        finally:
            if reaper:
                reaper.stop()
            app.state.client.close()

    app = FastAPI(title="dispatch", lifespan=lifespan)
    app.state.settings = settings

    @app.exception_handler(redis.exceptions.OutOfMemoryError)
    def _full(request: Request, exc: Exception) -> JSONResponse:
        # Valkey is at maxmemory (noeviction): new work is refused, nothing is dropped
        logger.error("valkey is full: %s", exc)
        return JSONResponse(
            status_code=507,
            content={
                "detail": "dispatch store is full (Valkey maxmemory); retry later"
            },
            headers={"Retry-After": "30"},
        )

    @app.exception_handler(redis.exceptions.ConnectionError)
    def _down(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={"detail": "dispatch store unreachable"},
            headers={"Retry-After": "5"},
        )

    app.include_router(health)
    app.include_router(router)
    return app
