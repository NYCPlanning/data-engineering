"""The queue: every state transition is ONE SQL statement.

Rules this module follows (tracker bead inv-k0d.3, "Alex's prior PG queue"):

1. A claim is a single statement: an inner ``SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1``
   picks the row, and the outer ``UPDATE`` re-checks ``status = 'queued' AND attempt =
   <seen attempt>``. Success is judged only by the rows RETURNING gives back (0 rows
   means no claim). No claim logic spans two round trips.
2. ``(id, attempt)`` is the fencing token. Heartbeat, complete, fail and events only touch
   a row that is still ``claimed`` with that attempt (and that worker); otherwise they
   change nothing and the caller gets :class:`StaleLease` (HTTP 409).
3. The reaper is the same kind of guarded single UPDATE (``status = 'claimed' AND
   lease_until < now()``), so it can't clobber a heartbeat: whichever statement takes the
   row lock first wins, and the other re-evaluates its WHERE clause and matches nothing.
4. ``work_attempts`` has PRIMARY KEY (item_id, attempt), written inside the claim
   statement, so a double claim would fail loudly instead of passing silently.
5. Time is always the database's ``now()``, never the app's clock.

Lease expiry is decided by the reaper alone. A heartbeat or complete that arrives after
``lease_until`` but before the reaper has run still succeeds: the row is still claimed with
that attempt, so nobody else can hold it, and the finished work isn't thrown away.

Functions expect an autocommit connection (see :func:`dispatch.db.connect`), so each
statement is its own transaction.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

import psycopg
from psycopg.types.json import Jsonb

TERMINAL_STATUSES = frozenset({"done", "failed", "dead", "cancelled"})
DEFAULT_LEASE_SECONDS = 60.0


class StaleLease(Exception):
    """The (id, attempt) token no longer holds the item: lease lost, reaped or finished."""


@dataclass(frozen=True)
class Heartbeat:
    lease_until: Any
    cancel: bool


ENQUEUE_SQL = """
INSERT INTO work_items (
    run_id, asset_key, partition_key, idempotency_key,
    required_caps, priority, max_attempts, payload
)
VALUES (
    %(run_id)s, %(asset_key)s, %(partition_key)s, %(idempotency_key)s,
    %(required_caps)s::text[], %(priority)s, %(max_attempts)s, %(payload)s
)
ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
RETURNING *, (xmax = 0) AS inserted
"""

CLAIM_SQL = """
WITH candidate AS (
    SELECT id, attempt
    FROM work_items
    WHERE status = 'queued'
      AND required_caps <@ %(caps)s::text[]
    ORDER BY
        coalesce(run_id = ANY(%(affinity)s::text[]), false) DESC,
        priority DESC,
        enqueued_at,
        id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
),
claimed AS (
    UPDATE work_items w
    SET status = 'claimed',
        attempt = w.attempt + 1,
        worker_id = %(worker_id)s,
        lease_until = now() + make_interval(secs => %(lease_seconds)s),
        claimed_at = now(),
        updated_at = now()
    FROM candidate c
    WHERE w.id = c.id
      AND w.status = 'queued'
      AND w.attempt = c.attempt
    RETURNING w.*
),
logged AS (
    INSERT INTO work_attempts (item_id, attempt, worker_id)
    SELECT id, attempt, worker_id FROM claimed
),
seen AS (
    INSERT INTO workers (id, caps)
    VALUES (%(worker_id)s, %(caps)s::text[])
    ON CONFLICT (id) DO UPDATE SET caps = EXCLUDED.caps, last_seen = now()
)
SELECT * FROM claimed
"""

HEARTBEAT_SQL = """
WITH hb AS (
    UPDATE work_items
    SET lease_until = now() + make_interval(secs => %(lease_seconds)s),
        updated_at = now()
    WHERE id = %(id)s
      AND attempt = %(attempt)s
      AND worker_id = %(worker_id)s
      AND status = 'claimed'
    RETURNING lease_until, cancel_requested
),
seen AS (
    UPDATE workers SET last_seen = now()
    WHERE id = %(worker_id)s AND EXISTS (SELECT 1 FROM hb)
)
SELECT * FROM hb
"""

COMPLETE_SQL = """
WITH done AS (
    UPDATE work_items
    SET status = 'done',
        result = %(result)s,
        error = NULL,
        worker_id = NULL,
        lease_until = NULL,
        finished_at = now(),
        updated_at = now()
    WHERE id = %(id)s
      AND attempt = %(attempt)s
      AND worker_id = %(worker_id)s
      AND status = 'claimed'
    RETURNING *
),
logged AS (
    UPDATE work_attempts a
    SET ended_at = now(), outcome = 'done'
    FROM done d
    WHERE a.item_id = d.id AND a.attempt = d.attempt
)
SELECT * FROM done
"""

FAIL_SQL = """
WITH failed AS (
    UPDATE work_items
    SET status = CASE
            WHEN cancel_requested THEN 'cancelled'
            WHEN NOT %(retryable)s THEN 'failed'
            WHEN attempt >= max_attempts THEN 'dead'
            ELSE 'queued'
        END,
        error = %(error)s,
        worker_id = NULL,
        lease_until = NULL,
        finished_at = CASE
            WHEN %(retryable)s AND NOT cancel_requested AND attempt < max_attempts THEN NULL
            ELSE now()
        END,
        updated_at = now()
    WHERE id = %(id)s
      AND attempt = %(attempt)s
      AND worker_id = %(worker_id)s
      AND status = 'claimed'
    RETURNING *
),
logged AS (
    UPDATE work_attempts a
    SET ended_at = now(),
        outcome = CASE WHEN f.status = 'cancelled' THEN 'cancelled' ELSE 'failed' END,
        error = %(error)s
    FROM failed f
    WHERE a.item_id = f.id AND a.attempt = f.attempt
)
SELECT * FROM failed
"""

REAP_SQL = """
WITH expired AS (
    SELECT id, attempt
    FROM work_items
    WHERE status = 'claimed'
      AND lease_until < now()
    ORDER BY lease_until
    LIMIT %(limit)s
    FOR UPDATE SKIP LOCKED
),
reaped AS (
    UPDATE work_items w
    SET status = CASE
            WHEN w.cancel_requested THEN 'cancelled'
            WHEN w.attempt >= w.max_attempts THEN 'dead'
            ELSE 'queued'
        END,
        error = format('lease expired (attempt %%s, worker %%s)', w.attempt, w.worker_id),
        worker_id = NULL,
        lease_until = NULL,
        finished_at = CASE
            WHEN w.cancel_requested OR w.attempt >= w.max_attempts THEN now()
        END,
        updated_at = now()
    FROM expired e
    WHERE w.id = e.id
      AND w.attempt = e.attempt
      AND w.status = 'claimed'
      AND w.lease_until < now()
    RETURNING w.id, w.attempt, w.status
),
logged AS (
    UPDATE work_attempts a
    SET ended_at = now(), outcome = 'expired'
    FROM reaped r
    WHERE a.item_id = r.id AND a.attempt = r.attempt
)
SELECT * FROM reaped
"""

CANCEL_SQL = """
UPDATE work_items
SET status = CASE WHEN status = 'queued' THEN 'cancelled' ELSE status END,
    cancel_requested = true,
    finished_at = CASE WHEN status = 'queued' THEN now() ELSE finished_at END,
    updated_at = now()
WHERE id = %(id)s
  AND status IN ('queued', 'claimed')
RETURNING *
"""

EVENT_SQL = """
INSERT INTO events (item_id, attempt, worker_id, kind, data)
SELECT id, attempt, worker_id, %(kind)s, %(data)s
FROM work_items
WHERE id = %(id)s
  AND attempt = %(attempt)s
  AND worker_id = %(worker_id)s
  AND status = 'claimed'
RETURNING *
"""


def enqueue(
    conn: psycopg.Connection,
    *,
    run_id: str | None = None,
    asset_key: str | None = None,
    partition_key: str | None = None,
    required_caps: Sequence[str] = (),
    priority: int = 0,
    max_attempts: int = 3,
    payload: dict | None = None,
    idempotency_key: str | None = None,
) -> dict:
    """Add an item. With an ``idempotency_key`` already in use, returns the existing item
    (``inserted`` is False) instead of adding a second one."""
    return conn.execute(
        ENQUEUE_SQL,
        {
            "run_id": run_id,
            "asset_key": asset_key,
            "partition_key": partition_key,
            "idempotency_key": idempotency_key,
            "required_caps": sorted(set(required_caps)),
            "priority": priority,
            "max_attempts": max_attempts,
            "payload": Jsonb(payload or {}),
        },
    ).fetchone()


def claim(
    conn: psycopg.Connection,
    *,
    worker_id: str,
    caps: Sequence[str],
    affinity_run_ids: Sequence[str] = (),
    lease_seconds: float = DEFAULT_LEASE_SECONDS,
) -> dict | None:
    """Claim the best queued item this worker can run, or None.

    Matching: ``required_caps`` must be a subset of ``caps``. Order: items whose run_id is
    in ``affinity_run_ids`` first, then higher priority, then oldest.
    """
    return conn.execute(
        CLAIM_SQL,
        {
            "worker_id": worker_id,
            "caps": sorted(set(caps)),
            "affinity": list(affinity_run_ids),
            "lease_seconds": float(lease_seconds),
        },
    ).fetchone()


def heartbeat(
    conn: psycopg.Connection,
    item_id: int,
    *,
    attempt: int,
    worker_id: str,
    lease_seconds: float = DEFAULT_LEASE_SECONDS,
) -> Heartbeat:
    row = conn.execute(
        HEARTBEAT_SQL,
        {
            "id": item_id,
            "attempt": attempt,
            "worker_id": worker_id,
            "lease_seconds": float(lease_seconds),
        },
    ).fetchone()
    if row is None:
        raise StaleLease(item_id, attempt)
    return Heartbeat(lease_until=row["lease_until"], cancel=row["cancel_requested"])


def complete(
    conn: psycopg.Connection,
    item_id: int,
    *,
    attempt: int,
    worker_id: str,
    result: dict | None = None,
) -> dict:
    row = conn.execute(
        COMPLETE_SQL,
        {
            "id": item_id,
            "attempt": attempt,
            "worker_id": worker_id,
            "result": Jsonb(result or {}),
        },
    ).fetchone()
    if row is None:
        raise StaleLease(item_id, attempt)
    return row


def fail(
    conn: psycopg.Connection,
    item_id: int,
    *,
    attempt: int,
    worker_id: str,
    error: str | None = None,
    retryable: bool = True,
) -> dict:
    """Report a failed attempt. Retryable failures requeue until max_attempts, then the
    item is ``dead``; non-retryable ones go straight to ``failed``."""
    row = conn.execute(
        FAIL_SQL,
        {
            "id": item_id,
            "attempt": attempt,
            "worker_id": worker_id,
            "error": error,
            "retryable": retryable,
        },
    ).fetchone()
    if row is None:
        raise StaleLease(item_id, attempt)
    return row


def reap(conn: psycopg.Connection, *, limit: int = 1000) -> list[dict]:
    """Requeue (or kill, or cancel) items whose lease has expired. Safe to run from any
    number of processes at once."""
    return conn.execute(REAP_SQL, {"limit": limit}).fetchall()


def cancel(conn: psycopg.Connection, item_id: int) -> dict | None:
    """Cancel a queued item now, or ask the worker holding a claimed one to stop (it sees
    ``cancel`` on its next heartbeat). Returns None if the item is missing or terminal."""
    return conn.execute(CANCEL_SQL, {"id": item_id}).fetchone()


def add_event(
    conn: psycopg.Connection,
    item_id: int,
    *,
    attempt: int,
    worker_id: str,
    kind: str,
    data: dict | None = None,
) -> dict:
    row = conn.execute(
        EVENT_SQL,
        {
            "id": item_id,
            "attempt": attempt,
            "worker_id": worker_id,
            "kind": kind,
            "data": Jsonb(data or {}),
        },
    ).fetchone()
    if row is None:
        raise StaleLease(item_id, attempt)
    return row


def get_item(conn: psycopg.Connection, item_id: int) -> dict | None:
    return conn.execute(
        "SELECT * FROM work_items WHERE id = %s", (item_id,)
    ).fetchone()


def list_events(conn: psycopg.Connection, item_id: int) -> list[dict]:
    return conn.execute(
        "SELECT * FROM events WHERE item_id = %s ORDER BY id", (item_id,)
    ).fetchall()
