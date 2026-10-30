"""Single-client behavior of the queue: matching, ordering, fencing, retries, TTLs."""

import pytest
import redis

from dagster_broker import store
from dagster_broker.store import NoSuchItem, StaleLease

from .conftest import PREFIX, all_items, attempts_log


def test_claim_complete_happy_path(q, r):
    item, inserted = q.enqueue(run_id="r1", asset_key="a", payload={"x": 1})
    assert inserted and item["status"] == "queued" and item["attempt"] == 0

    claimed = q.claim(worker_id="w1", caps=[])
    assert claimed["id"] == item["id"]
    assert claimed["status"] == "claimed"
    assert claimed["attempt"] == 1
    assert claimed["worker_id"] == "w1"
    assert claimed["lease_until"] > claimed["claimed_at"]
    assert claimed["payload"] == {"x": 1}

    hb = q.heartbeat(item["id"], attempt=1, worker_id="w1")
    assert hb.cancel is False and hb.lease_until >= claimed["lease_until"]

    done = q.complete(item["id"], attempt=1, worker_id="w1", result={"ok": 1})
    assert done["status"] == "done" and done["result"] == {"ok": 1}
    assert done["worker_id"] is None and done["lease_until"] is None
    assert attempts_log(r, item["id"]) == {"1": "w1", "1:outcome": "done"}
    assert q.claim(worker_id="w1", caps=[]) is None
    assert q.stats() == store.Stats(queued=0, claimed=0, demand=0)

    worker = r.hgetall(f"{PREFIX}worker:w1")
    assert worker["last_seen"] and r.ttl(f"{PREFIX}worker:w1") > 0


def test_empty_queue_claims_nothing(q):
    assert q.claim(worker_id="w1", caps=["anything"]) is None


def test_caps_subset_matching(q):
    needs_gdal_big, _ = q.enqueue(required_caps=["gdal", "big"])
    needs_nothing, _ = q.enqueue(required_caps=[], priority=-1)

    # missing "big": can only take the item with no requirements
    got = q.claim(worker_id="small", caps=["gdal"])
    assert got["id"] == needs_nothing["id"]
    assert q.claim(worker_id="small", caps=["gdal"]) is None
    assert q.claim(worker_id="none", caps=[]) is None

    # superset of required caps matches, in any order, duplicates ignored
    got = q.claim(worker_id="big", caps=["geosupport", "gdal", "big", "gdal"])
    assert got["id"] == needs_gdal_big["id"]
    assert got["required_caps"] == ["big", "gdal"]


def test_priority_then_fifo(q):
    low, _ = q.enqueue(priority=0)
    high, _ = q.enqueue(priority=5)
    low2, _ = q.enqueue(priority=0)
    neg, _ = q.enqueue(priority=-3)
    order = [q.claim(worker_id="w", caps=[])["id"] for _ in range(4)]
    assert order == [high["id"], low["id"], low2["id"], neg["id"]]


def test_priority_and_fifo_across_caps_signatures(q):
    a, _ = q.enqueue(required_caps=["a"], priority=1)
    b, _ = q.enqueue(required_caps=["b"], priority=1)
    c, _ = q.enqueue(required_caps=[], priority=2)
    d, _ = q.enqueue(required_caps=["a", "b"], priority=1)
    order = [q.claim(worker_id="w", caps=["a", "b"])["id"] for _ in range(4)]
    assert order == [c["id"], a["id"], b["id"], d["id"]]


def test_requeued_item_keeps_its_place(q):
    first, _ = q.enqueue()
    second, _ = q.enqueue()
    q.claim(worker_id="w", caps=[])
    q.fail(first["id"], attempt=1, worker_id="w")
    assert q.claim(worker_id="w", caps=[])["id"] == first["id"]
    assert q.claim(worker_id="w", caps=[])["id"] == second["id"]


def test_affinity_beats_priority(q):
    other_hi, _ = q.enqueue(run_id="other", priority=10)
    mine_lo, _ = q.enqueue(run_id="mine", priority=0)
    mine_lo2, _ = q.enqueue(run_id="mine2", priority=1)
    no_run, _ = q.enqueue(run_id=None, priority=5)

    def claim():
        return q.claim(worker_id="w", caps=[], affinity_run_ids=["mine", "mine2"])

    # among affinity matches, priority still orders
    assert claim()["id"] == mine_lo2["id"]
    assert claim()["id"] == mine_lo["id"]
    # no affinity match left: back to priority order, null run_id handled
    assert claim()["id"] == other_hi["id"]
    assert claim()["id"] == no_run["id"]


def test_affinity_does_not_bypass_caps(q):
    q.enqueue(run_id="mine", required_caps=["gpu"])
    other, _ = q.enqueue(run_id="other")
    got = q.claim(worker_id="w", caps=[], affinity_run_ids=["mine"])
    assert got["id"] == other["id"]


def test_stale_attempt_and_wrong_worker_are_rejected(q):
    item, _ = q.enqueue()
    q.claim(worker_id="w1", caps=[])
    for kwargs in (
        {"attempt": 2, "worker_id": "w1"},
        {"attempt": 0, "worker_id": "w1"},
        {"attempt": 1, "worker_id": "w2"},
    ):
        with pytest.raises(StaleLease):
            q.heartbeat(item["id"], **kwargs)
        with pytest.raises(StaleLease):
            q.complete(item["id"], **kwargs)
        with pytest.raises(StaleLease):
            q.fail(item["id"], **kwargs)
        with pytest.raises(StaleLease):
            q.add_events(item["id"], events=[("log", {})], **kwargs)
    assert q.get_item(item["id"])["status"] == "claimed"


def test_missing_item_is_no_such_item(q):
    token = {"attempt": 1, "worker_id": "w"}
    for call in (q.heartbeat, q.complete, q.fail):
        with pytest.raises(NoSuchItem):
            call(999, **token)
    with pytest.raises(NoSuchItem):
        q.add_events(999, events=[("log", {})], **token)
    with pytest.raises(NoSuchItem):
        q.cancel(999)
    assert q.get_item(999) is None


def test_double_complete_is_rejected(q):
    item, _ = q.enqueue()
    q.claim(worker_id="w1", caps=[])
    q.complete(item["id"], attempt=1, worker_id="w1")
    with pytest.raises(StaleLease):
        q.complete(item["id"], attempt=1, worker_id="w1")


def test_retryable_fail_requeues_until_dead(q, r):
    item, _ = q.enqueue(max_attempts=3)
    for n in (1, 2, 3):
        got = q.claim(worker_id=f"w{n}", caps=[])
        assert got["attempt"] == n
        row = q.fail(item["id"], attempt=n, worker_id=f"w{n}", error=f"e{n}")
        assert row["status"] == ("dead" if n == 3 else "queued")
    assert q.claim(worker_id="w", caps=[]) is None
    final = q.get_item(item["id"])
    assert final["status"] == "dead" and final["error"] == "e3"
    assert final["finished_at"] is not None
    log = attempts_log(r, item["id"])
    assert [log[f"{n}:outcome"] for n in (1, 2, 3)] == ["failed"] * 3


def test_non_retryable_fail_is_terminal(q):
    item, _ = q.enqueue(max_attempts=5)
    q.claim(worker_id="w", caps=[])
    row = q.fail(item["id"], attempt=1, worker_id="w", retryable=False)
    assert row["status"] == "failed"
    assert q.claim(worker_id="w", caps=[]) is None


def test_reaper_requeues_then_kills(q, r, expire):
    item, _ = q.enqueue(max_attempts=2)
    q.claim(worker_id="w1", caps=[])
    assert q.reap() == []  # lease still live

    expire(item["id"])
    assert q.reap() == [{"id": item["id"], "attempt": 1, "status": "queued"}]
    requeued = q.get_item(item["id"])
    assert requeued["worker_id"] is None and requeued["lease_until"] is None
    assert "lease expired" in requeued["error"]

    # the old holder is fenced out, even after reclaim by someone else
    again = q.claim(worker_id="w2", caps=[])
    assert again["attempt"] == 2
    with pytest.raises(StaleLease):
        q.heartbeat(item["id"], attempt=1, worker_id="w1")
    with pytest.raises(StaleLease):
        q.complete(item["id"], attempt=1, worker_id="w1")

    expire(item["id"])
    assert q.reap() == [{"id": item["id"], "attempt": 2, "status": "dead"}]
    log = attempts_log(r, item["id"])
    assert (log["1:outcome"], log["2:outcome"]) == ("expired", "expired")


def test_late_heartbeat_before_reap_keeps_lease(q, expire):
    """Expiry is the reaper's call: until it runs, the holder can still extend."""
    item, _ = q.enqueue()
    q.claim(worker_id="w1", caps=[], lease_seconds=60)
    expire(item["id"])
    q.heartbeat(item["id"], attempt=1, worker_id="w1")
    assert q.reap() == []
    assert q.get_item(item["id"])["status"] == "claimed"


def test_cancel_queued_and_claimed(q, expire):
    queued, _ = q.enqueue(priority=-1)
    running, _ = q.enqueue()
    q.claim(worker_id="w", caps=[])

    row, changed = q.cancel(queued["id"])
    assert changed and row["status"] == "cancelled"
    row, changed = q.cancel(running["id"])
    assert changed and row["status"] == "claimed" and row["cancel_requested"]
    assert q.claim(worker_id="w", caps=[]) is None  # cancelled one left the queue

    hb = q.heartbeat(running["id"], attempt=1, worker_id="w")
    assert hb.cancel is True
    row = q.fail(running["id"], attempt=1, worker_id="w", error="cancelled")
    assert row["status"] == "cancelled"
    row, changed = q.cancel(running["id"])  # already terminal
    assert not changed and row["status"] == "cancelled"

    # a cancelled item whose worker vanished is cancelled by the reaper, not requeued
    vanished, _ = q.enqueue()
    q.claim(worker_id="w", caps=[])
    q.cancel(vanished["id"])
    expire(vanished["id"])
    assert q.reap()[0]["status"] == "cancelled"


def test_events_are_fenced_ordered_and_paged(q):
    item, _ = q.enqueue(max_attempts=2)
    q.claim(worker_id="w", caps=[])
    q.add_events(item["id"], attempt=1, worker_id="w", events=[("log", {"m": 1})])
    q.fail(item["id"], attempt=1, worker_id="w")
    q.claim(worker_id="w2", caps=[])
    with pytest.raises(StaleLease):  # the old attempt can't write any more
        q.add_events(item["id"], attempt=1, worker_id="w", events=[("log", {})])
    ids = q.add_events(
        item["id"],
        attempt=2,
        worker_id="w2",
        events=[("pipes", {"n": i}) for i in range(5)],
    )
    assert len(ids) == 5

    events, cursor = q.list_events(item["id"])
    assert [(e["kind"], e["attempt"]) for e in events] == [("log", 1)] + [
        ("pipes", 2)
    ] * 5
    assert cursor == ids[-1]

    page, cur = q.list_events(item["id"], limit=2)
    assert len(page) == 2
    rest, cur2 = q.list_events(item["id"], after=cur)
    assert [e["id"] for e in page + rest] == [e["id"] for e in events]
    assert q.list_events(item["id"], after=cur2) == ([], cur2)

    only2, _ = q.list_events(item["id"], attempt=2)
    assert [e["data"]["n"] for e in only2] == list(range(5))


def test_events_cap_refuses_instead_of_trimming(r):
    q = store.ValkeyStore(r, prefix=PREFIX, max_events_per_item=3)
    item, _ = q.enqueue()
    q.claim(worker_id="w", caps=[])
    q.add_events(item["id"], attempt=1, worker_id="w", events=[("a", {})] * 2)
    with pytest.raises(store.EventsFull):
        q.add_events(item["id"], attempt=1, worker_id="w", events=[("b", {})] * 2)
    assert len(q.list_events(item["id"])[0]) == 2  # all or nothing


def test_idempotent_enqueue(q, r):
    a, a_new = q.enqueue(idempotency_key="run1:asset:p1", priority=1)
    b, b_new = q.enqueue(idempotency_key="run1:asset:p1", priority=9)
    assert a_new and not b_new
    assert a["id"] == b["id"] and b["priority"] == 1
    assert q.enqueue()[1] and q.enqueue()[1]  # no key never collides

    # if the item is gone (e.g. expired), the key no longer pins it: a fresh item
    r.delete(f"{PREFIX}item:{a['id']}")
    c, c_new = q.enqueue(idempotency_key="run1:asset:p1")
    assert c_new and c["id"] != a["id"]


def test_terminal_items_expire_live_items_do_not(r):
    q = store.ValkeyStore(r, prefix=PREFIX, terminal_ttl_seconds=100)
    live, _ = q.enqueue(idempotency_key="live")
    done, _ = q.enqueue(idempotency_key="done", priority=1)
    q.claim(worker_id="w", caps=[])
    q.add_events(done["id"], attempt=1, worker_id="w", events=[("log", {})])
    q.complete(done["id"], attempt=1, worker_id="w")
    for key in ("item:{}", "att:{}", "ev:{}"):
        assert 0 < r.ttl(PREFIX + key.format(done["id"])) <= 100
    assert 0 < r.ttl(f"{PREFIX}idem:done") <= 100
    assert r.ttl(f"{PREFIX}item:{live['id']}") == -1
    assert r.ttl(f"{PREFIX}idem:live") == -1


def test_claim_backstop_fails_loudly(q, r):
    """If attempt 1 of an item is already logged (as after a double claim), the claim
    script errors before writing anything."""
    item, _ = q.enqueue()
    r.hset(f"{PREFIX}att:{item['id']}", "1", "ghost")
    with pytest.raises(redis.ResponseError, match="already claimed"):
        q.claim(worker_id="w", caps=[])
    row = q.get_item(item["id"])
    assert row["status"] == "queued" and row["attempt"] == 0
    assert q.stats().queued == 1


def test_demand_counts(q, r):
    q.enqueue(required_caps=[])
    q.enqueue(required_caps=["gdal"])
    q.enqueue(required_caps=["gdal", "big"])
    q.enqueue(required_caps=["gpu"])
    assert q.stats(caps=[]).demand == 1
    assert q.stats(caps=["gdal"]).demand == 2
    assert q.stats(caps=["big", "gdal"]).demand == 3
    assert q.stats(caps=None).demand == 4
    assert q.stats(caps=["gdal"], min_age_seconds=3600).demand == 0
    # backdate one item's queued time: only it counts at min-age
    old = int(r.time()[0]) * 1000 - 120_000
    r.zadd(f"{PREFIX}qt:" + store.caps_signature(["gdal"]), {f"{2:016d}": old})
    assert q.stats(caps=["gdal"], min_age_seconds=60).demand == 1
    q.claim(worker_id="w", caps=["gdal"])
    st = q.stats(caps=["gdal"])
    assert (st.queued, st.claimed, st.demand) == (3, 1, 1)
    assert set(all_items(r)) == {1, 2, 3, 4}
