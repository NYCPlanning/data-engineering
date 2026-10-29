"""Single-connection behavior of the queue: matching, ordering, fencing, retries."""

import psycopg
import pytest

from dispatch import queue


def attempts(conn, item_id):
    return conn.execute(
        "SELECT attempt, worker_id, outcome FROM work_attempts "
        "WHERE item_id = %s ORDER BY attempt",
        (item_id,),
    ).fetchall()


def test_claim_complete_happy_path(conn):
    item = queue.enqueue(conn, run_id="r1", asset_key="a", payload={"x": 1})
    assert item["status"] == "queued" and item["attempt"] == 0

    claimed = queue.claim(conn, worker_id="w1", caps=[])
    assert claimed["id"] == item["id"]
    assert claimed["status"] == "claimed"
    assert claimed["attempt"] == 1
    assert claimed["worker_id"] == "w1"
    assert claimed["lease_until"] > claimed["claimed_at"]
    assert claimed["payload"] == {"x": 1}

    hb = queue.heartbeat(conn, item["id"], attempt=1, worker_id="w1")
    assert hb.cancel is False and hb.lease_until >= claimed["lease_until"]

    done = queue.complete(conn, item["id"], attempt=1, worker_id="w1", result={"ok": 1})
    assert done["status"] == "done" and done["result"] == {"ok": 1}
    assert done["worker_id"] is None and done["lease_until"] is None
    assert attempts(conn, item["id"]) == [
        {"attempt": 1, "worker_id": "w1", "outcome": "done"}
    ]
    assert queue.claim(conn, worker_id="w1", caps=[]) is None

    worker = conn.execute("SELECT * FROM workers WHERE id = 'w1'").fetchone()
    assert worker["last_seen"] is not None


def test_empty_queue_claims_nothing(conn):
    assert queue.claim(conn, worker_id="w1", caps=["anything"]) is None


def test_caps_subset_matching(conn):
    needs_gdal_big = queue.enqueue(conn, required_caps=["gdal", "big"])
    needs_nothing = queue.enqueue(conn, required_caps=[], priority=-1)

    # missing "big": can only take the item with no requirements
    got = queue.claim(conn, worker_id="small", caps=["gdal"])
    assert got["id"] == needs_nothing["id"]
    assert queue.claim(conn, worker_id="small", caps=["gdal"]) is None
    assert queue.claim(conn, worker_id="none", caps=[]) is None

    # superset of required caps matches
    got = queue.claim(conn, worker_id="big", caps=["big", "gdal", "geosupport"])
    assert got["id"] == needs_gdal_big["id"]


def test_priority_then_fifo(conn):
    low = queue.enqueue(conn, priority=0)
    high = queue.enqueue(conn, priority=5)
    low2 = queue.enqueue(conn, priority=0)
    order = [queue.claim(conn, worker_id="w", caps=[])["id"] for _ in range(3)]
    assert order == [high["id"], low["id"], low2["id"]]


def test_affinity_beats_priority(conn):
    other_hi = queue.enqueue(conn, run_id="other", priority=10)
    mine_lo = queue.enqueue(conn, run_id="mine", priority=0)
    no_run = queue.enqueue(conn, run_id=None, priority=5)

    got = queue.claim(conn, worker_id="w", caps=[], affinity_run_ids=["mine"])
    assert got["id"] == mine_lo["id"]
    # no affinity match left: back to priority order, null run_id handled
    got = queue.claim(conn, worker_id="w", caps=[], affinity_run_ids=["mine"])
    assert got["id"] == other_hi["id"]
    got = queue.claim(conn, worker_id="w", caps=[], affinity_run_ids=["mine"])
    assert got["id"] == no_run["id"]


def test_affinity_does_not_bypass_caps(conn):
    queue.enqueue(conn, run_id="mine", required_caps=["gpu"])
    other = queue.enqueue(conn, run_id="other")
    got = queue.claim(conn, worker_id="w", caps=[], affinity_run_ids=["mine"])
    assert got["id"] == other["id"]


def test_stale_attempt_and_wrong_worker_are_rejected(conn):
    item = queue.enqueue(conn)
    queue.claim(conn, worker_id="w1", caps=[])
    for kwargs in (
        {"attempt": 2, "worker_id": "w1"},
        {"attempt": 0, "worker_id": "w1"},
        {"attempt": 1, "worker_id": "w2"},
    ):
        with pytest.raises(queue.StaleLease):
            queue.heartbeat(conn, item["id"], **kwargs)
        with pytest.raises(queue.StaleLease):
            queue.complete(conn, item["id"], **kwargs)
        with pytest.raises(queue.StaleLease):
            queue.fail(conn, item["id"], **kwargs)
        with pytest.raises(queue.StaleLease):
            queue.add_event(conn, item["id"], kind="log", **kwargs)
    assert queue.get_item(conn, item["id"])["status"] == "claimed"


def test_double_complete_is_rejected(conn):
    item = queue.enqueue(conn)
    queue.claim(conn, worker_id="w1", caps=[])
    queue.complete(conn, item["id"], attempt=1, worker_id="w1")
    with pytest.raises(queue.StaleLease):
        queue.complete(conn, item["id"], attempt=1, worker_id="w1")


def test_retryable_fail_requeues_until_dead(conn):
    item = queue.enqueue(conn, max_attempts=3)
    for n in (1, 2, 3):
        got = queue.claim(conn, worker_id=f"w{n}", caps=[])
        assert got["attempt"] == n
        row = queue.fail(conn, item["id"], attempt=n, worker_id=f"w{n}", error=f"e{n}")
        assert row["status"] == ("dead" if n == 3 else "queued")
    assert queue.claim(conn, worker_id="w", caps=[]) is None
    final = queue.get_item(conn, item["id"])
    assert final["status"] == "dead" and final["error"] == "e3"
    assert final["finished_at"] is not None
    assert [a["outcome"] for a in attempts(conn, item["id"])] == ["failed"] * 3


def test_non_retryable_fail_is_terminal(conn):
    item = queue.enqueue(conn, max_attempts=5)
    queue.claim(conn, worker_id="w", caps=[])
    row = queue.fail(conn, item["id"], attempt=1, worker_id="w", retryable=False)
    assert row["status"] == "failed"
    assert queue.claim(conn, worker_id="w", caps=[]) is None


def test_reaper_requeues_then_kills(conn, expire):
    item = queue.enqueue(conn, max_attempts=2)
    queue.claim(conn, worker_id="w1", caps=[])
    assert queue.reap(conn) == []  # lease still live

    expire(item["id"])
    assert queue.reap(conn) == [{"id": item["id"], "attempt": 1, "status": "queued"}]
    requeued = queue.get_item(conn, item["id"])
    assert requeued["worker_id"] is None and requeued["lease_until"] is None
    assert "lease expired" in requeued["error"]

    # the old holder is fenced out, even after reclaim by someone else
    again = queue.claim(conn, worker_id="w2", caps=[])
    assert again["attempt"] == 2
    with pytest.raises(queue.StaleLease):
        queue.heartbeat(conn, item["id"], attempt=1, worker_id="w1")
    with pytest.raises(queue.StaleLease):
        queue.complete(conn, item["id"], attempt=1, worker_id="w1")

    expire(item["id"])
    assert queue.reap(conn) == [{"id": item["id"], "attempt": 2, "status": "dead"}]
    assert [a["outcome"] for a in attempts(conn, item["id"])] == ["expired"] * 2


def test_late_heartbeat_before_reap_keeps_lease(conn, expire):
    """Expiry is the reaper's call: until it runs, the holder can still extend."""
    item = queue.enqueue(conn)
    queue.claim(conn, worker_id="w1", caps=[], lease_seconds=60)
    expire(item["id"])
    queue.heartbeat(conn, item["id"], attempt=1, worker_id="w1")
    assert queue.reap(conn) == []
    assert queue.get_item(conn, item["id"])["status"] == "claimed"


def test_cancel_queued_and_claimed(conn, expire):
    queued = queue.enqueue(conn, priority=-1)
    running = queue.enqueue(conn)
    queue.claim(conn, worker_id="w", caps=[])

    assert queue.cancel(conn, queued["id"])["status"] == "cancelled"
    row = queue.cancel(conn, running["id"])
    assert row["status"] == "claimed" and row["cancel_requested"]

    hb = queue.heartbeat(conn, running["id"], attempt=1, worker_id="w")
    assert hb.cancel is True
    row = queue.fail(conn, running["id"], attempt=1, worker_id="w", error="cancelled")
    assert row["status"] == "cancelled"
    assert queue.cancel(conn, running["id"]) is None  # already terminal

    # a cancelled item whose worker vanished is cancelled by the reaper, not requeued
    vanished = queue.enqueue(conn)
    queue.claim(conn, worker_id="w", caps=[])
    queue.cancel(conn, vanished["id"])
    expire(vanished["id"])
    assert queue.reap(conn)[0]["status"] == "cancelled"


def test_events_are_fenced_and_listed(conn):
    item = queue.enqueue(conn)
    queue.claim(conn, worker_id="w", caps=[])
    queue.add_event(conn, item["id"], attempt=1, worker_id="w", kind="log", data={"m": 1})
    queue.add_event(conn, item["id"], attempt=1, worker_id="w", kind="progress")
    events = queue.list_events(conn, item["id"])
    assert [(e["kind"], e["attempt"]) for e in events] == [("log", 1), ("progress", 1)]


def test_idempotent_enqueue(conn):
    a = queue.enqueue(conn, idempotency_key="run1:asset:p1", priority=1)
    b = queue.enqueue(conn, idempotency_key="run1:asset:p1", priority=9)
    assert a["inserted"] and not b["inserted"]
    assert a["id"] == b["id"] and b["priority"] == 1
    assert queue.enqueue(conn)["inserted"]  # null keys never collide
    assert queue.enqueue(conn)["inserted"]


def test_attempts_unique_backstop_fails_loudly(conn):
    """If a work_attempts row for the next attempt already exists (as it would after a
    double claim), the claim statement errors and claims nothing."""
    item = queue.enqueue(conn)
    conn.execute(
        "INSERT INTO work_attempts (item_id, attempt, worker_id) VALUES (%s, 1, 'ghost')",
        (item["id"],),
    )
    with pytest.raises(psycopg.errors.UniqueViolation):
        queue.claim(conn, worker_id="w", caps=[])
    row = queue.get_item(conn, item["id"])
    assert row["status"] == "queued" and row["attempt"] == 0


def test_schema_rejects_inconsistent_claim_state(conn):
    item = queue.enqueue(conn)
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            "UPDATE work_items SET status = 'claimed' WHERE id = %s", (item["id"],)
        )
