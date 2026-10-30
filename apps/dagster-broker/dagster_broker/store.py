"""The queue, stored in an in-memory Valkey. Every state transition is ONE Lua script.

Why this is safe under concurrency (tracker bead inv-k0d.3): Valkey runs a script to
completion before serving any other command, so a claim (match caps, pick the best item,
take it, start its lease) can't interleave with another claim, a heartbeat or the reaper.
No transition is split across round trips from Python. The scripts carry a shebang, which
matters: Valkey checks maxmemory once, before a flagged script starts, whereas an
unflagged script can be aborted at its first write after earlier writes have landed.
Scripts that grow memory (enqueue, events) are refused cleanly when the store is full;
the ones that drain it (claim, heartbeat, complete, fail, reap, cancel) carry
``allow-oom`` so workers can keep finishing work.

``(id, attempt, worker_id)`` is the fencing token: heartbeat, complete, fail and events
only act on an item that is still claimed with exactly that token, otherwise they change
nothing and the caller gets :class:`StaleLease` (HTTP 409), or :class:`NoSuchItem` (404).

Lease expiry is the reaper's call. A heartbeat or complete that arrives after
``lease_until`` but before the reaper has run still succeeds: nobody else can hold the
item yet, so the finished work isn't thrown away.

Ephemeral by design: Valkey runs with no persistence, so a restart loses everything.
Durable truth lives in Dagster (runs, Pipes messages it has read) and blob storage.
Clients treat 404 as "the queue forgot this item": the producer re-enqueues with the same
idempotency key, a worker abandons the attempt.

Item ids are random (uuid4 hex), so an id never comes back after a Valkey restart.

Key layout (``P`` = key prefix, default ``dagster-broker:``):

- ``P item:{id}``: hash with the item's fields; JSON for caps, payload, result
- ``P sigs``: set of caps signatures (sorted caps as JSON) that have queued items
- ``P q:{sig}``: zset of queued items needing exactly those caps, score ``-priority``,
  member ``{enqueued_at ms, zero-padded}:{id}`` (equal scores sort by member, so oldest
  enqueue first, then id; a requeued item keeps its place)
- ``P qr:{sha1(sig, run_id)}``: same, per run_id, for affinity
- ``P qt:{sig}``: zset of queued ids scored by queued_at (ms), for demand min-age
- ``P leases``: zset of claimed ids scored by lease_until (ms), for the reaper
- ``P att:{id}``: hash ``{attempt: worker_id, "{attempt}:outcome": ...}``, the claim log
- ``P ev:{id}``: stream of events (attempt, worker_id, kind, data)
- ``P idem:{key}``: the item id for an idempotency key
- ``P worker:{id}``: hash caps / first_seen / last_seen

Live items never expire. When an item reaches a terminal status, its hash, attempts,
events and idempotency key get a TTL (``terminal_ttl_seconds``).
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import resources
from typing import Any, Protocol, Sequence

import redis

TERMINAL_STATUSES = frozenset({"done", "failed", "dead", "cancelled"})
DEFAULT_LEASE_SECONDS = 60.0
WORKER_TTL_SECONDS = 24 * 3600


def new_item_id() -> str:
    """A random item id. Never a counter: Valkey is ephemeral, and a counter would hand
    out the same ids again after a restart."""
    return uuid.uuid4().hex


class StaleLease(Exception):
    """The (id, attempt, worker) token no longer holds the item: lease lost, reaped or
    finished."""


class NoSuchItem(Exception):
    """The item doesn't exist (never did, expired, or Valkey restarted)."""


class EventsFull(Exception):
    """The item already has max_events_per_item events."""


@dataclass(frozen=True)
class Heartbeat:
    lease_until: datetime
    cancel: bool


@dataclass(frozen=True)
class Stats:
    queued: int
    claimed: int
    demand: int


class QueueStore(Protocol):
    """What the HTTP layer needs from storage."""

    def enqueue(self, **fields: Any) -> tuple[dict, bool]: ...
    def claim(
        self,
        *,
        worker_id: str,
        caps: Sequence[str],
        affinity_run_ids: Sequence[str] = (),
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
    ) -> dict | None: ...
    def heartbeat(
        self, item_id: str, *, attempt: int, worker_id: str, lease_seconds: float
    ) -> Heartbeat: ...
    def complete(
        self, item_id: str, *, attempt: int, worker_id: str, result: dict | None = None
    ) -> dict: ...
    def fail(
        self,
        item_id: str,
        *,
        attempt: int,
        worker_id: str,
        error: str | None = None,
        retryable: bool = True,
    ) -> dict: ...
    def add_events(
        self,
        item_id: str,
        *,
        attempt: int,
        worker_id: str,
        events: Sequence[tuple[str, dict]],
    ) -> list[str]: ...
    def list_events(
        self,
        item_id: str,
        *,
        after: str | None = None,
        attempt: int | None = None,
        limit: int = 1000,
    ) -> tuple[list[dict], str | None]: ...
    def cancel(self, item_id: str) -> tuple[dict, bool]: ...
    def reap(self, *, limit: int = 1000) -> list[dict]: ...
    def get_item(self, item_id: str) -> dict | None: ...
    def stats(
        self, *, caps: Sequence[str] | None = None, min_age_seconds: float = 0
    ) -> Stats: ...


def _load_script(name: str) -> str:
    lua = resources.files("dagster_broker") / "lua"
    body = (lua / f"{name}.lua").read_text()
    shebang, rest = body.split("\n", 1)
    assert shebang.startswith("#!lua"), f"{name}.lua must start with a #!lua shebang"
    return f"{shebang}\n{(lua / 'prelude.lua').read_text()}\n{rest}"


SCRIPT_NAMES = (
    "enqueue",
    "claim",
    "heartbeat",
    "complete",
    "fail",
    "reap",
    "cancel",
    "events",
    "stats",
)


def caps_signature(caps: Sequence[str]) -> str:
    return json.dumps(sorted(set(caps)), separators=(",", ":"))


def _ts(ms: str | int | None) -> datetime | None:
    if ms in (None, ""):
        return None
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


_INT_FIELDS = ("attempt", "max_attempts", "priority")
_TS_FIELDS = (
    "enqueued_at",
    "queued_at",
    "claimed_at",
    "finished_at",
    "updated_at",
    "lease_until",
)
_ALL_FIELDS = (
    "id",
    "run_id",
    "asset_key",
    "partition_key",
    "idempotency_key",
    "required_caps",
    "priority",
    "status",
    "attempt",
    "max_attempts",
    "worker_id",
    "lease_until",
    "cancel_requested",
    "enqueued_at",
    "queued_at",
    "claimed_at",
    "finished_at",
    "updated_at",
    "payload",
    "result",
    "error",
)


def _pairs(flat: list) -> dict[str, str]:
    return dict(zip(flat[::2], flat[1::2]))


def _item(raw: dict[str, str]) -> dict:
    """Hash fields (all strings) -> the API's item dict."""
    out: dict[str, Any] = {f: None for f in _ALL_FIELDS}
    for k, v in raw.items():
        if k in _INT_FIELDS:
            out[k] = int(v)
        elif k in _TS_FIELDS:
            out[k] = _ts(v)
        elif k in ("payload", "result"):
            out[k] = json.loads(v)
        elif k == "caps":
            out["required_caps"] = json.loads(v)
        elif k == "cancel_requested":
            out[k] = v == "1"
        elif k in out:
            out[k] = v
    return out


class ValkeyStore:
    """The queue on a redis-py client (``decode_responses=True``). Thread-safe if the
    client is (a client with a connection pool is)."""

    def __init__(
        self,
        client: redis.Redis,
        *,
        prefix: str = "dagster-broker:",
        terminal_ttl_seconds: int = 24 * 3600,
        max_events_per_item: int = 100_000,
    ):
        self.r = client
        self.prefix = prefix
        self.ttl = int(terminal_ttl_seconds)
        self.max_events = int(max_events_per_item)
        # register_script runs EVALSHA and falls back to SCRIPT LOAD on NOSCRIPT, so a
        # Valkey restart (which empties its script cache) is handled transparently.
        self._scripts = {
            n: client.register_script(_load_script(n)) for n in SCRIPT_NAMES
        }

    def _run(self, name: str, *args: Any) -> Any:
        return self._scripts[name](keys=[], args=[self.prefix, *args], client=self.r)

    @staticmethod
    def _check(reply: list, item_id: str) -> list:
        code = reply[0]
        if code == "noitem":
            raise NoSuchItem(item_id)
        if code == "stale":
            raise StaleLease(item_id)
        if code == "full":
            raise EventsFull(item_id)
        return reply

    def enqueue(
        self,
        *,
        run_id: str | None = None,
        asset_key: str | None = None,
        partition_key: str | None = None,
        required_caps: Sequence[str] = (),
        priority: int = 0,
        max_attempts: int = 3,
        payload: dict | None = None,
        idempotency_key: str | None = None,
    ) -> tuple[dict, bool]:
        """Add an item; returns (item, inserted). With an idempotency_key whose item still
        exists, returns that item and False instead of adding a second one."""
        sig = caps_signature(required_caps)
        fields: list[Any] = [
            "caps",
            sig,
            "sig",
            sig,
            "priority",
            int(priority),
            "max_attempts",
            int(max_attempts),
            "payload",
            json.dumps(payload or {}),
        ]
        for name, value in (
            ("run_id", run_id),
            ("asset_key", asset_key),
            ("partition_key", partition_key),
            ("idempotency_key", idempotency_key),
        ):
            if value is not None:
                fields += [name, value]
        inserted, flat = self._run(
            "enqueue",
            idempotency_key or "",
            sig,
            run_id or "",
            int(priority),
            new_item_id(),
            *fields,
        )
        return _item(_pairs(flat)), bool(inserted)

    def claim(
        self,
        *,
        worker_id: str,
        caps: Sequence[str],
        affinity_run_ids: Sequence[str] = (),
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
    ) -> dict | None:
        """Claim the best queued item this worker can run, or None.

        Matching: an item's required caps must be a subset of ``caps``. Order: items whose
        run_id is in ``affinity_run_ids`` first, then higher priority, then oldest.
        """
        flat = self._run(
            "claim",
            worker_id,
            json.dumps(sorted(set(caps))),
            json.dumps(list(affinity_run_ids)),
            int(lease_seconds * 1000),
            WORKER_TTL_SECONDS,
        )
        return _item(_pairs(flat)) if flat else None

    def heartbeat(
        self,
        item_id: str,
        *,
        attempt: int,
        worker_id: str,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
    ) -> Heartbeat:
        reply = self._check(
            self._run(
                "heartbeat",
                item_id,
                attempt,
                worker_id,
                int(lease_seconds * 1000),
                WORKER_TTL_SECONDS,
            ),
            item_id,
        )
        lease_until = _ts(reply[1])
        assert lease_until is not None
        return Heartbeat(lease_until=lease_until, cancel=reply[2] == "1")

    def complete(
        self, item_id: str, *, attempt: int, worker_id: str, result: dict | None = None
    ) -> dict:
        reply = self._check(
            self._run(
                "complete",
                item_id,
                attempt,
                worker_id,
                json.dumps(result or {}),
                self.ttl,
            ),
            item_id,
        )
        return _item(_pairs(reply[1]))

    def fail(
        self,
        item_id: str,
        *,
        attempt: int,
        worker_id: str,
        error: str | None = None,
        retryable: bool = True,
    ) -> dict:
        """Report a failed attempt. Retryable failures requeue until max_attempts, then
        the item is ``dead``; non-retryable ones go straight to ``failed``."""
        reply = self._check(
            self._run(
                "fail",
                item_id,
                attempt,
                worker_id,
                error or "",
                1 if retryable else 0,
                self.ttl,
            ),
            item_id,
        )
        return _item(_pairs(reply[1]))

    def add_events(
        self,
        item_id: str,
        *,
        attempt: int,
        worker_id: str,
        events: Sequence[tuple[str, dict]],
    ) -> list[str]:
        """Append events for the current attempt, all or nothing. Returns stream ids."""
        flat: list[str] = []
        for kind, data in events:
            flat += [kind, json.dumps(data or {})]
        reply = self._check(
            self._run("events", item_id, attempt, worker_id, self.max_events, *flat),
            item_id,
        )
        return list(reply[1])

    def list_events(
        self,
        item_id: str,
        *,
        after: str | None = None,
        attempt: int | None = None,
        limit: int = 1000,
    ) -> tuple[list[dict], str | None]:
        """Events after the stream id ``after`` (exclusive), oldest first, at most
        ``limit``. Returns (events, cursor); pass the cursor back as ``after``. With
        ``attempt``, other attempts' events are skipped (the cursor still advances)."""
        start = f"({after}" if after else "-"
        rows = self.r.xrange(f"{self.prefix}ev:{item_id}", start, "+", count=limit)
        events = []
        for stream_id, f in rows:
            if attempt is not None and int(f["attempt"]) != attempt:
                continue
            events.append(
                {
                    "id": stream_id,
                    "attempt": int(f["attempt"]),
                    "worker_id": f["worker_id"],
                    "kind": f["kind"],
                    "data": json.loads(f["data"]),
                    "ts": _ts(stream_id.split("-")[0]),
                }
            )
        cursor = rows[-1][0] if rows else after
        return events, cursor

    def cancel(self, item_id: str) -> tuple[dict, bool]:
        """Cancel a queued item now, or flag a claimed one (its worker sees ``cancel`` on
        the next heartbeat). Returns (item, changed); changed is False if it was already
        terminal."""
        reply = self._check(self._run("cancel", item_id, self.ttl), item_id)
        return _item(_pairs(reply[1])), reply[0] == "ok"

    def reap(self, *, limit: int = 1000) -> list[dict]:
        """Requeue (or kill, or cancel) items whose lease has expired."""
        return [
            {"id": i, "attempt": int(a), "status": s}
            for i, a, s in self._run("reap", limit, self.ttl)
        ]

    def get_item(self, item_id: str) -> dict | None:
        raw = self.r.hgetall(f"{self.prefix}item:{item_id}")
        return _item(raw) if raw else None

    def stats(
        self, *, caps: Sequence[str] | None = None, min_age_seconds: float = 0
    ) -> Stats:
        """Counts of queued and claimed items, and the demand: queued items that a worker
        with ``caps`` could claim (all queued items if caps is None), counting only items
        queued at least ``min_age_seconds`` ago."""
        caps_arg = "" if caps is None else json.dumps(sorted(set(caps)))
        queued, claimed, demand = self._run(
            "stats", caps_arg, int(min_age_seconds * 1000)
        )
        return Stats(queued=int(queued), claimed=int(claimed), demand=int(demand))


def connect(url: str, *, max_connections: int | None = None) -> redis.Redis:
    """A redis-py client for Valkey. With max_connections, callers block for a free
    connection instead of erroring when the pool is busy."""
    if max_connections:
        pool = redis.BlockingConnectionPool.from_url(
            url, decode_responses=True, max_connections=max_connections, timeout=10
        )
        return redis.Redis(connection_pool=pool)
    return redis.Redis.from_url(url, decode_responses=True)


@dataclass(frozen=True)
class Readiness:
    ok: bool
    detail: dict


def check_ready(client: redis.Redis) -> Readiness:
    """PING, and refuse to be ready unless Valkey can't silently drop keys: maxmemory
    must be set (else the host OOM-kills it) and the policy must be noeviction (else it
    evicts queued items)."""
    try:
        client.ping()
        conf = client.config_get("maxmemory*")
    except redis.RedisError as e:
        return Readiness(False, {"valkey": False, "error": type(e).__name__})
    maxmemory = int(conf.get("maxmemory", 0))
    policy = conf.get("maxmemory-policy")
    problems = []
    if maxmemory <= 0:
        problems.append("maxmemory is not set")
    if policy != "noeviction":
        problems.append(f"maxmemory-policy is {policy!r}, must be 'noeviction'")
    return Readiness(
        not problems,
        {
            "valkey": True,
            "maxmemory": maxmemory,
            "maxmemory_policy": policy,
            "problems": problems,
        },
    )
