"""Runs the queue's Lua scripts. Every state transition is ONE script, and Valkey runs a
script to completion before serving anything else, so transitions can't interleave.

``(id, attempt, worker_id)`` is the fencing token: heartbeat, complete, fail and events
change nothing unless the item is still claimed with exactly that token (:class:`Stale`).
Valkey keeps nothing on disk, so after a restart every item is :class:`NoSuchItem`.
Key layout: see lua/prelude.lua.
"""

import json
import uuid
from importlib import resources

import redis

PREFIX = "dagster-broker:"
WORKER_TTL_SECONDS = 24 * 3600


class Stale(Exception):
    pass


class NoSuchItem(Exception):
    pass


def _script(name: str) -> str:
    lua = resources.files("dagster_broker") / "lua"
    shebang, body = (lua / f"{name}.lua").read_text().split("\n", 1)
    return f"{shebang}\n{(lua / 'prelude.lua').read_text()}\n{body}"


def caps_json(caps) -> str:
    return json.dumps(sorted(set(caps)), separators=(",", ":"))


def item(flat) -> dict:
    """A script's HGETALL reply (or a hash) -> the API's item dict."""
    raw = dict(zip(flat[::2], flat[1::2])) if isinstance(flat, list) else flat
    out = dict(raw)
    for k in ("attempt", "max_attempts", "priority", "lease_until"):
        if k in raw:
            out[k] = int(raw[k])
    for k in ("payload", "result", "caps"):
        if k in raw:
            out[k] = json.loads(raw[k])
    out["cancel_requested"] = raw.get("cancel_requested") == "1"
    out.pop("sig", None)
    return out


class Store:
    def __init__(self, r: redis.Redis, terminal_ttl_seconds: int = 24 * 3600):
        self.r = r
        self.ttl = int(terminal_ttl_seconds)
        names = "enqueue claim heartbeat complete fail events cancel reap stats"
        self.scripts = {n: r.register_script(_script(n)) for n in names.split()}

    def run(self, name: str, *args):
        reply = self.scripts[name](args=[PREFIX, *args], client=self.r)
        if isinstance(reply, list) and reply and reply[0] == "noitem":
            raise NoSuchItem()
        if isinstance(reply, list) and reply and reply[0] == "stale":
            raise Stale()
        return reply

    def enqueue(
        self,
        *,
        required_caps=(),
        priority=0,
        max_attempts=3,
        payload=None,
        idempotency_key=None,
        run_id=None,
        asset_key=None,
        partition_key=None,
    ) -> tuple[dict, bool]:
        """(item, inserted). An idempotency key that still has an item returns that item."""
        sig = caps_json(required_caps)
        fields = ["caps", sig, "sig", sig, "priority", priority]
        fields += ["max_attempts", max_attempts, "payload", json.dumps(payload or {})]
        for k, v in (
            ("idempotency_key", idempotency_key),
            ("run_id", run_id),
            ("asset_key", asset_key),
            ("partition_key", partition_key),
        ):
            if v is not None:
                fields += [k, v]
        inserted, flat = self.run(
            "enqueue",
            idempotency_key or "",
            sig,
            run_id or "",
            priority,
            uuid.uuid4().hex,
            *fields,
        )
        return item(flat), bool(inserted)

    def claim(self, worker_id, caps, affinity_run_ids=(), lease_seconds=60.0):
        """The best item whose required caps are a subset of ``caps``: affinity run_ids
        first, then priority, then oldest. None if there is nothing to claim."""
        flat = self.run(
            "claim",
            worker_id,
            caps_json(caps),
            json.dumps(list(affinity_run_ids)),
            int(lease_seconds * 1000),
            WORKER_TTL_SECONDS,
        )
        return item(flat) if flat else None

    def heartbeat(self, item_id, attempt, worker_id, lease_seconds=60.0) -> dict:
        _, lease_until, cancel = self.run(
            "heartbeat",
            item_id,
            attempt,
            worker_id,
            int(lease_seconds * 1000),
            WORKER_TTL_SECONDS,
        )
        return {"lease_until": lease_until, "cancel": cancel == "1"}

    def complete(self, item_id, attempt, worker_id, result=None) -> dict:
        result = json.dumps(result or {})
        return item(
            self.run("complete", item_id, attempt, worker_id, result, self.ttl)[1]
        )

    def fail(self, item_id, attempt, worker_id, error=None, retryable=True) -> dict:
        """Retryable failures requeue until max_attempts (then dead), others are failed."""
        args = (item_id, attempt, worker_id, error or "", int(retryable), self.ttl)
        return item(self.run("fail", *args)[1])

    def add_events(self, item_id, attempt, worker_id, events) -> list:
        flat = [x for e in events for x in (e["kind"], json.dumps(e.get("data") or {}))]
        return self.run("events", item_id, attempt, worker_id, *flat)[1]

    def events(self, item_id, after=None, limit=1000) -> tuple[list, str | None]:
        """(events after the cursor ``after``, oldest first; the next cursor)."""
        start = f"({after}" if after else "-"
        rows = self.r.xrange(f"{PREFIX}ev:{item_id}", start, "+", count=limit)
        events = [
            {
                "id": i,
                "attempt": int(f["attempt"]),
                "worker_id": f["worker_id"],
                "kind": f["kind"],
                "data": json.loads(f["data"]),
            }
            for i, f in rows
        ]
        return events, rows[-1][0] if rows else after

    def cancel(self, item_id) -> dict:
        """Cancel a queued item; flag a claimed one (its worker sees it on heartbeat)."""
        return item(self.run("cancel", item_id, self.ttl)[1])

    def reap(self, limit=1000) -> list:
        """Requeue (or kill, or cancel) items whose lease has expired."""
        return self.run("reap", limit, self.ttl)

    def get(self, item_id) -> dict | None:
        raw = self.r.hgetall(f"{PREFIX}item:{item_id}")
        return item(raw) if raw else None

    def stats(self, caps=None, min_age_seconds=0.0) -> dict:
        """Queued and claimed counts, and the demand: queued items a worker with ``caps``
        could claim (all, if None) that have waited at least min_age_seconds."""
        caps = "" if caps is None else caps_json(caps)
        queued, claimed, demand = self.run("stats", caps, int(min_age_seconds * 1000))
        return {"queued": queued, "claimed": claimed, "demand": demand}
