"""HTTP API. Run with ``uvicorn dispatch.app:create_app --factory``.

The routes are on an APIRouter, so the queue can be mounted into another ASGI app later
without changes. Endpoints are sync ``def``s: FastAPI runs them in its threadpool, each
borrowing a connection from a psycopg pool.
"""

from __future__ import annotations

import hmac
import logging
import threading
from contextlib import asynccontextmanager
from typing import Any, Iterator

import psycopg
from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Request,
    Response,
)
from pydantic import BaseModel, Field

from dispatch import db, queue
from dispatch.config import Settings

logger = logging.getLogger(__name__)


# ---- request bodies ---------------------------------------------------------------


class EnqueueRequest(BaseModel):
    run_id: str | None = None
    asset_key: str | None = None
    partition_key: str | None = None
    idempotency_key: str | None = None
    required_caps: list[str] = Field(default_factory=list)
    priority: int = 0
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


class EventRequest(Fenced):
    kind: str = Field(min_length=1)
    data: dict[str, Any] = Field(default_factory=dict)


# ---- dependencies -----------------------------------------------------------------


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_conn(request: Request) -> Iterator[queue.Conn]:
    with request.app.state.pool.connection() as conn:
        yield conn


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


def _stale(conn: queue.Conn, item_id: int) -> HTTPException:
    if queue.get_item(conn, item_id) is None:
        return HTTPException(status_code=404, detail="no such item")
    return HTTPException(
        status_code=409,
        detail="stale lease: this (item, attempt) no longer holds the item",
    )


def _item(row: dict) -> dict:
    return {k: v for k, v in row.items() if k != "inserted"}


# ---- routes -----------------------------------------------------------------------

router = APIRouter(dependencies=[Depends(require_key)])


@router.post("/items", status_code=201)
def enqueue_item(
    body: EnqueueRequest,
    response: Response,
    conn: queue.Conn = Depends(get_conn),
) -> dict:
    row = queue.enqueue(conn, **body.model_dump())
    if not row["inserted"]:
        response.status_code = 200
    return _item(row)


@router.get("/items/{item_id}")
def get_item(item_id: int, conn: queue.Conn = Depends(get_conn)) -> dict:
    row = queue.get_item(conn, item_id)
    if row is None:
        raise HTTPException(status_code=404, detail="no such item")
    return row


@router.get("/items/{item_id}/events")
def get_events(item_id: int, conn: queue.Conn = Depends(get_conn)) -> list:
    return queue.list_events(conn, item_id)


@router.post("/items/{item_id}/cancel")
def cancel_item(item_id: int, conn: queue.Conn = Depends(get_conn)) -> dict:
    row = queue.cancel(conn, item_id)
    if row is None:
        current = queue.get_item(conn, item_id)
        if current is None:
            raise HTTPException(status_code=404, detail="no such item")
        return current  # already terminal: nothing to cancel
    return row


@router.post("/work/claim", responses={204: {"description": "nothing to claim"}})
def claim(
    body: ClaimRequest,
    conn: queue.Conn = Depends(get_conn),
    settings: Settings = Depends(get_settings),
) -> Any:
    row = queue.claim(
        conn,
        worker_id=body.worker_id,
        caps=body.caps,
        affinity_run_ids=body.affinity_run_ids,
        lease_seconds=settings.lease_seconds,
    )
    if row is None:
        return Response(status_code=204)
    return row


@router.post("/work/{item_id}/heartbeat")
def heartbeat(
    item_id: int,
    body: Fenced,
    conn: queue.Conn = Depends(get_conn),
    settings: Settings = Depends(get_settings),
) -> dict:
    try:
        hb = queue.heartbeat(
            conn,
            item_id,
            attempt=body.attempt,
            worker_id=body.worker_id,
            lease_seconds=settings.lease_seconds,
        )
    except queue.StaleLease:
        raise _stale(conn, item_id)
    return {"lease_until": hb.lease_until, "cancel": hb.cancel}


@router.post("/work/{item_id}/events", status_code=201)
def add_event(
    item_id: int, body: EventRequest, conn: queue.Conn = Depends(get_conn)
) -> dict:
    try:
        return queue.add_event(conn, item_id, **body.model_dump())
    except queue.StaleLease:
        raise _stale(conn, item_id)


@router.post("/work/{item_id}/complete")
def complete(
    item_id: int, body: CompleteRequest, conn: queue.Conn = Depends(get_conn)
) -> dict:
    try:
        return queue.complete(conn, item_id, **body.model_dump())
    except queue.StaleLease:
        raise _stale(conn, item_id)


@router.post("/work/{item_id}/fail")
def fail(item_id: int, body: FailRequest, conn: queue.Conn = Depends(get_conn)) -> dict:
    try:
        return queue.fail(conn, item_id, **body.model_dump())
    except queue.StaleLease:
        raise _stale(conn, item_id)


health = APIRouter()


@health.get("/healthz")
def healthz() -> dict:
    return {"ok": True}


@health.get("/readyz")
def readyz(request: Request, response: Response) -> dict:
    head = db.head_revision()
    try:
        with request.app.state.pool.connection(timeout=5) as conn:
            current = db.current_revision(conn)
    except psycopg.errors.UndefinedTable:
        current = None
    except Exception as e:  # db unreachable
        response.status_code = 503
        return {"ok": False, "db": False, "error": type(e).__name__}
    ok = current == head
    if not ok:
        response.status_code = 503
    return {"ok": ok, "db": True, "revision": current, "head": head}


# ---- reaper -----------------------------------------------------------------------


class Reaper:
    """Runs :func:`dispatch.queue.reap` every ``interval`` seconds on a daemon thread."""

    def __init__(self, pool, interval: float):
        self.pool = pool
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
                with self.pool.connection() as conn:
                    reaped = queue.reap(conn)
                if reaped:
                    logger.warning("reaped %d expired leases: %s", len(reaped), reaped)
            except Exception:
                logger.exception("reaper pass failed")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.pool = db.make_pool(
            settings.database_url, max_size=settings.pool_max_size
        )
        reaper = None
        if settings.reap_interval_seconds > 0:
            reaper = Reaper(app.state.pool, settings.reap_interval_seconds)
            reaper.start()
        try:
            yield
        finally:
            if reaper:
                reaper.stop()
            app.state.pool.close()

    app = FastAPI(title="dispatch", lifespan=lifespan)
    app.state.settings = settings
    app.include_router(health)
    app.include_router(router)
    return app
