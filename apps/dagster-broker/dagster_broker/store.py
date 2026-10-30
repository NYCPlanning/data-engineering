"""The queue, kept in Valkey. Every state transition runs under :data:`LOCK`, so
transitions can't interleave, and does its writes in one MULTI/EXEC, so a crash can't
leave one half done. ``(id, attempt, worker_id)`` is the fencing token:
heartbeat, complete, fail and events change nothing unless the item is still claimed
with exactly that token (:class:`Stale`). Valkey keeps nothing on disk, so after a
Valkey restart every item is :class:`NoSuchItem`.

Keys, all under ``dagster-broker:``:
    item:{id}    hash of the item's fields
    q:{caps}     queued items needing exactly these caps (sorted JSON list),
                 score -priority, member "<enqueued_at ms>:<id>" so ties go oldest first
    qr:{run_id}  the same members per run, for affinity
    sigs         set of caps signatures that have queued items
    leases       claimed item ids, scored by lease_until (ms)
    ev:{id}      stream of events
    idem:{key}   the item id for an idempotency key
Live items never expire; a finished item's keys expire after the terminal TTL.
"""

import json
import threading
import time
import uuid

import redis

P = "dagster-broker:"

# Correctness depends on this lock, so there must be exactly ONE broker process
# (uvicorn --workers 1, one replica: ACA min = max = 1).
LOCK = threading.Lock()


class Stale(Exception):
    pass


class NoSuchItem(Exception):
    pass


def now_ms() -> int:
    return int(time.time() * 1000)


def caps_json(caps) -> str:
    return json.dumps(sorted(set(caps)), separators=(",", ":"))


def item(raw: dict) -> dict:
    out = dict(raw)
    for k in ("attempt", "max_attempts", "priority", "enqueued_at", "lease_until"):
        if k in raw:
            out[k] = int(raw[k])
    for k in ("payload", "result", "caps"):
        if k in raw:
            out[k] = json.loads(raw[k])
    out["cancel_requested"] = raw.get("cancel_requested") == "1"
    return out


def member(it: dict) -> str:
    return f"{it['enqueued_at']:016d}:{it['id']}"


class Store:
    def __init__(self, r: redis.Redis, terminal_ttl_seconds: int = 24 * 3600):
        self.r = r
        self.ttl = int(terminal_ttl_seconds)

    # ---- helpers; call with LOCK held. ``p`` is the transition's MULTI pipeline ----

    def get(self, item_id) -> dict | None:
        raw = self.r.hgetall(f"{P}item:{item_id}")
        return item(raw) if raw else None

    def _set(self, p, it: dict, **fields) -> None:
        p.hset(f"{P}item:{it['id']}", mapping=fields)

    def _fenced(self, item_id, attempt, worker_id) -> dict:
        it = self.get(item_id)
        if it is None:
            raise NoSuchItem()
        token = (it["status"], it["attempt"], it.get("worker_id"))
        if token != ("claimed", attempt, worker_id):
            raise Stale()
        return it

    def _index(self, p, it: dict) -> None:
        sig = caps_json(it["caps"])
        p.zadd(f"{P}q:{sig}", {member(it): -it["priority"]})
        p.sadd(f"{P}sigs", sig)
        if it.get("run_id"):
            p.zadd(f"{P}qr:{it['run_id']}", {member(it): -it["priority"]})

    def _unindex(self, p, it: dict) -> None:
        sig = caps_json(it["caps"])
        if self.r.zcard(f"{P}q:{sig}") <= 1:  # only this item
            p.srem(f"{P}sigs", sig)
        p.zrem(f"{P}q:{sig}", member(it))
        if it.get("run_id"):
            p.zrem(f"{P}qr:{it['run_id']}", member(it))

    def _release(self, p, it: dict) -> None:
        p.zrem(f"{P}leases", it["id"])
        p.hdel(f"{P}item:{it['id']}", "worker_id", "lease_until")

    def _finish(self, p, it: dict, status: str) -> None:
        self._set(p, it, status=status, finished_at=now_ms())
        keys = [f"item:{it['id']}", f"ev:{it['id']}"]
        if it.get("idempotency_key"):
            keys.append(f"idem:{it['idempotency_key']}")
        for key in keys:
            p.expire(P + key, self.ttl)

    def _end_attempt(self, p, it: dict, error: str, retryable=True) -> str:
        """After an attempt fails or expires: requeue it, or end it dead/failed/cancelled."""
        self._release(p, it)
        self._set(p, it, error=error)
        if it["cancel_requested"]:
            status = "cancelled"
        elif it["attempt"] >= it["max_attempts"]:
            status = "dead"
        else:
            status = "queued" if retryable else "failed"
        if status == "queued":
            self._set(p, it, status="queued")
            self._index(p, it)  # same member, so it keeps its place
        else:
            self._finish(p, it, status)
        return status

    # ---- transitions ----

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
        with LOCK:
            if idempotency_key:
                existing = self.r.get(f"{P}idem:{idempotency_key}")
                if existing and (it := self.get(existing)):
                    return it, False
            fields = {
                "id": uuid.uuid4().hex,
                "status": "queued",
                "attempt": 0,
                "priority": priority,
                "max_attempts": max_attempts,
                "caps": caps_json(required_caps),
                "payload": json.dumps(payload or {}),
                "enqueued_at": now_ms(),
            }
            optional = {
                "idempotency_key": idempotency_key,
                "run_id": run_id,
                "asset_key": asset_key,
                "partition_key": partition_key,
            }
            fields.update({k: v for k, v in optional.items() if v is not None})
            it = item(fields)
            p = self.r.pipeline()
            p.hset(f"{P}item:{it['id']}", mapping=fields)
            if idempotency_key:
                p.set(f"{P}idem:{idempotency_key}", it["id"])
            self._index(p, it)
            p.execute()
            return it, True

    def claim(self, worker_id, caps, affinity_run_ids=(), lease_seconds=60.0):
        """The best item whose required caps are a subset of ``caps``: affinity run_ids
        first, then priority, then oldest. None if there is nothing to claim."""
        with LOCK:
            have = set(caps)
            sigs = {
                s for s in self.r.smembers(f"{P}sigs") if set(json.loads(s)) <= have
            }
            best = None  # (score, member)
            for run in affinity_run_ids:
                for m, score in self.r.zrange(f"{P}qr:{run}", 0, -1, withscores=True):
                    if self.r.hget(f"{P}item:{m.split(':')[1]}", "caps") in sigs:
                        best = min(best or (score, m), (score, m))
                        break
            if best is None:
                for sig in sigs:
                    for m, score in self.r.zrange(f"{P}q:{sig}", 0, 0, withscores=True):
                        best = min(best or (score, m), (score, m))
            if best is None:
                return None
            it = self.get(best[1].split(":")[1])
            p = self.r.pipeline()
            self._unindex(p, it)
            lease_until = now_ms() + int(lease_seconds * 1000)
            self._set(
                p,
                it,
                status="claimed",
                attempt=it["attempt"] + 1,
                worker_id=worker_id,
                lease_until=lease_until,
            )
            p.zadd(f"{P}leases", {it["id"]: lease_until})
            p.execute()
            return self.get(it["id"])

    def heartbeat(self, item_id, attempt, worker_id, lease_seconds=60.0) -> dict:
        """Extends the lease. A heartbeat after lease_until but before the reaper ran
        still succeeds: nobody else can hold the item yet."""
        with LOCK:
            it = self._fenced(item_id, attempt, worker_id)
            lease_until = now_ms() + int(lease_seconds * 1000)
            p = self.r.pipeline()
            self._set(p, it, lease_until=lease_until)
            p.zadd(f"{P}leases", {item_id: lease_until})
            p.execute()
            return {"lease_until": lease_until, "cancel": it["cancel_requested"]}

    def complete(self, item_id, attempt, worker_id, result=None) -> dict:
        with LOCK:
            it = self._fenced(item_id, attempt, worker_id)
            p = self.r.pipeline()
            self._release(p, it)
            self._set(p, it, result=json.dumps(result or {}))
            self._finish(p, it, "done")
            p.execute()
            return self.get(item_id)

    def fail(self, item_id, attempt, worker_id, error=None, retryable=True) -> dict:
        """Retryable failures requeue until max_attempts (then dead), others are failed."""
        with LOCK:
            it = self._fenced(item_id, attempt, worker_id)
            p = self.r.pipeline()
            self._end_attempt(p, it, error or "", retryable)
            p.execute()
            return self.get(item_id)

    def add_events(self, item_id, attempt, worker_id, events) -> list:
        with LOCK:
            self._fenced(item_id, attempt, worker_id)
            p = self.r.pipeline()
            for e in events:
                p.xadd(
                    f"{P}ev:{item_id}",
                    {
                        "attempt": attempt,
                        "worker_id": worker_id,
                        "kind": e["kind"],
                        "data": json.dumps(e.get("data") or {}),
                    },
                )
            return p.execute()

    def cancel(self, item_id) -> dict:
        """Cancel a queued item; flag a claimed one (its worker sees it on heartbeat)."""
        with LOCK:
            it = self.get(item_id)
            if it is None:
                raise NoSuchItem()
            p = self.r.pipeline()
            if it["status"] in ("queued", "claimed"):
                self._set(p, it, cancel_requested=1)
            if it["status"] == "queued":
                self._unindex(p, it)
                self._finish(p, it, "cancelled")
            p.execute()
            return self.get(item_id)

    def reap(self, limit=1000) -> list:
        """Requeue (or kill, or cancel) items whose lease has expired."""
        out = []
        with LOCK:
            expired = self.r.zrangebyscore(f"{P}leases", "-inf", now_ms(), 0, limit)
            p = self.r.pipeline()
            for item_id in expired:
                it = self.get(item_id)
                if it is None or it["status"] != "claimed":
                    p.zrem(f"{P}leases", item_id)
                    continue
                error = f"lease expired (attempt {it['attempt']}, {it['worker_id']})"
                out.append((item_id, it["attempt"], self._end_attempt(p, it, error)))
            p.execute()
        return out

    def events(self, item_id, after=None, limit=1000) -> tuple[list, str | None]:
        """(events after the cursor ``after``, oldest first; the next cursor)."""
        start = f"({after}" if after else "-"
        rows = self.r.xrange(f"{P}ev:{item_id}", start, "+", count=limit)
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

    def stats(self, caps=None, min_age_seconds=0.0) -> dict:
        """Queued and claimed counts, and the demand: queued items a worker with ``caps``
        could claim (all, if None), enqueued at least min_age_seconds ago."""
        cutoff = now_ms() - min_age_seconds * 1000
        queued = demand = 0
        with LOCK:
            for sig in self.r.smembers(f"{P}sigs"):
                members = self.r.zrange(f"{P}q:{sig}", 0, -1)
                queued += len(members)
                if caps is None or set(json.loads(sig)) <= set(caps):
                    demand += sum(int(m.split(":")[0]) <= cutoff for m in members)
            claimed = self.r.zcard(f"{P}leases")
        return {"queued": queued, "claimed": claimed, "demand": demand}
