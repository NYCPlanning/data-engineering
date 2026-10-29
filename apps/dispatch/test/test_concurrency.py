"""Concurrency stress tests: real threads, each with its own Postgres connection.

These are the tests for the failure Alex hit in an earlier Postgres queue (two workers
claiming the same job). See tracker bead inv-k0d.3.
"""

import random
import threading
import time
from collections import Counter, defaultdict

import pytest

from dispatch import db, queue

N_WORKERS = 64


def run_threads(n, target):
    """Start n threads behind a barrier so they really hit the DB at the same time."""
    barrier = threading.Barrier(n)
    errors = []

    def wrapped(i):
        try:
            barrier.wait()
            target(i)
        except BaseException as e:  # surface thread errors in the test
            errors.append(e)

    threads = [threading.Thread(target=wrapped, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    assert not any(t.is_alive() for t in threads), "worker threads hung"
    if errors:
        raise errors[0]


def test_pure_claim_race_each_item_claimed_once(conn, clean_db):
    n_items = 400
    for i in range(n_items):
        queue.enqueue(conn, asset_key=f"a{i}", priority=i % 3)

    claims = [[] for _ in range(N_WORKERS)]

    def worker(i):
        with db.connect(clean_db) as c:
            while (item := queue.claim(c, worker_id=f"w{i}", caps=[])) is not None:
                claims[i].append((item["id"], item["attempt"]))
                queue.complete(c, item["id"], attempt=item["attempt"], worker_id=f"w{i}")

    started = time.monotonic()
    run_threads(N_WORKERS, worker)
    elapsed = time.monotonic() - started

    flat = [c for per in claims for c in per]
    assert len(flat) == n_items
    assert len(set(flat)) == n_items
    assert {a for _, a in flat} == {1}
    assert sum(1 for per in claims if per) > N_WORKERS // 2, "claims weren't spread"

    stats = conn.execute(
        "SELECT count(*) FILTER (WHERE status = 'done') AS done, count(*) AS total "
        "FROM work_items"
    ).fetchone()
    assert stats == {"done": n_items, "total": n_items}
    assert conn.execute("SELECT count(*) AS n FROM work_attempts").fetchone()["n"] == (
        n_items
    )
    print(
        f"\npure race: {N_WORKERS} workers, {n_items} items, {len(flat)} claims, "
        f"{elapsed:.2f}s, busiest worker {max(len(p) for p in claims)}"
    )


def test_chaos_claims_fail_abandon_reap(conn, clean_db):
    """Workers complete, fail, crash (abandon) or stall past their lease while a reaper
    runs continuously. Every item must end terminal, every (item, attempt) must be
    claimed at most once, and exactly one completion may succeed per done item."""
    n_items = 300
    lease = 0.3
    for i in range(n_items):
        queue.enqueue(conn, asset_key=f"a{i}", max_attempts=3, priority=i % 5)

    claims = defaultdict(list)  # worker -> [(id, attempt)]
    completed = []  # (id, attempt) whose complete() succeeded
    outcomes = Counter()
    reaped = []
    lock = threading.Lock()
    stop_reaper = threading.Event()
    deadline = time.monotonic() + 90

    def bump(key):
        with lock:
            outcomes[key] += 1

    def reaper():
        with db.connect(clean_db) as c:
            while not stop_reaper.is_set():
                rows = queue.reap(c)
                with lock:
                    reaped.extend(rows)
                time.sleep(0.05)

    def drained(c):
        return (
            c.execute(
                "SELECT count(*) AS n FROM work_items WHERE status IN ('queued', 'claimed')"
            ).fetchone()["n"]
            == 0
        )

    def worker(i):
        rng = random.Random(i)
        wid = f"w{i}"
        with db.connect(clean_db) as c:
            while time.monotonic() < deadline:
                item = queue.claim(c, worker_id=wid, caps=[], lease_seconds=lease)
                if item is None:
                    if drained(c):
                        return
                    time.sleep(0.02)
                    continue
                token = {"attempt": item["attempt"], "worker_id": wid}
                with lock:
                    claims[wid].append((item["id"], item["attempt"]))
                roll = rng.random()
                try:
                    if roll < 0.55:
                        queue.complete(c, item["id"], **token)
                        with lock:
                            completed.append((item["id"], item["attempt"]))
                        bump("complete")
                    elif roll < 0.70:
                        queue.fail(c, item["id"], error="boom", **token)
                        bump("fail_retryable")
                    elif roll < 0.75:
                        queue.fail(c, item["id"], retryable=False, **token)
                        bump("fail_final")
                    elif roll < 0.90:
                        bump("abandon")  # crash: never heard from again
                    else:
                        time.sleep(rng.uniform(0.2, 0.6))  # stall around the lease
                        queue.heartbeat(c, item["id"], lease_seconds=lease, **token)
                        queue.complete(c, item["id"], **token)
                        with lock:
                            completed.append((item["id"], item["attempt"]))
                        bump("late_complete_ok")
                except queue.StaleLease:
                    bump("stale_409")

    reaper_thread = threading.Thread(target=reaper)
    reaper_thread.start()
    started = time.monotonic()
    try:
        run_threads(N_WORKERS, worker)
    finally:
        stop_reaper.set()
        reaper_thread.join()
    elapsed = time.monotonic() - started

    items = {r["id"]: r for r in conn.execute("SELECT * FROM work_items").fetchall()}
    assert len(items) == n_items, "items lost"
    status = Counter(r["status"] for r in items.values())
    assert set(status) <= {"done", "failed", "dead"}, f"not drained: {status}"

    flat = [c for per in claims.values() for c in per]
    dupes = [k for k, v in Counter(flat).items() if v > 1]
    assert not dupes, f"double claims: {dupes[:10]}"

    by_item = defaultdict(list)
    for item_id, attempt in flat:
        by_item[item_id].append(attempt)
    for item_id, row in items.items():
        assert sorted(by_item[item_id]) == list(range(1, row["attempt"] + 1)), item_id
        if row["status"] == "dead":
            assert row["attempt"] == row["max_attempts"]

    done_completions = Counter(item_id for item_id, _ in completed)
    assert max(done_completions.values()) == 1, "an item completed twice"
    for item_id, row in items.items():
        if row["status"] == "done":
            assert (item_id, row["attempt"]) in completed
        else:
            assert done_completions[item_id] == 0

    logged = {
        (r["item_id"], r["attempt"])
        for r in conn.execute("SELECT item_id, attempt FROM work_attempts").fetchall()
    }
    assert logged == set(flat)
    assert outcomes["abandon"] > 0 and reaped, "reaper path wasn't exercised"

    print(
        f"\nchaos: {N_WORKERS} workers, {n_items} items, {len(flat)} claims in "
        f"{elapsed:.2f}s; outcomes {dict(outcomes)}; reaped {len(reaped)}; "
        f"final {dict(status)}"
    )


RACE_ROUNDS = 150


@pytest.mark.parametrize("rival", [False, True], ids=["no-rival", "with-rival"])
def test_reaper_vs_late_heartbeat_vs_late_complete(conn, clean_db, expire, rival):
    """An expired-but-unreaped holder sends a heartbeat and a complete at the same moment
    the reaper runs (and, with a rival, while another worker tries to claim). Exactly one
    of {reaped, completed} happens, and the state agrees with whichever it was."""
    seen = Counter()
    conns = [db.connect(clean_db) for _ in range(4)]
    try:
        for _ in range(RACE_ROUNDS):
            item = queue.enqueue(conn)
            queue.claim(conn, worker_id="holder", caps=[])
            expire(item["id"])
            token = {"attempt": 1, "worker_id": "holder"}
            res = {}

            def act(i):
                c = conns[i]
                try:
                    if i == 0:
                        res["reaped"] = bool(queue.reap(c))
                    elif i == 1:
                        queue.heartbeat(c, item["id"], **token)
                        res["hb"] = True
                    elif i == 2:
                        queue.complete(c, item["id"], **token)
                        res["completed"] = True
                    else:
                        for _ in range(3):
                            got = queue.claim(c, worker_id="rival", caps=[])
                            if got:
                                res["rival"] = got["attempt"]
                                break
                except queue.StaleLease:
                    pass

            run_threads(4 if rival else 3, act)
            reaped_, hb, completed_ = (
                res.get("reaped", False),
                res.get("hb", False),
                res.get("completed", False),
            )
            row = queue.get_item(conn, item["id"])

            assert reaped_ != completed_, (res, row)
            if hb:
                assert not reaped_
            if completed_:
                assert row["status"] == "done" and row["attempt"] == 1
                assert "rival" not in res
            else:
                assert row["attempt"] == (2 if "rival" in res else 1)
                assert row["status"] == ("claimed" if "rival" in res else "queued")
                if "rival" in res:
                    assert row["worker_id"] == "rival"
            seen[
                ("reaped" if reaped_ else "completed")
                + ("+hb" if hb else "")
                + ("+rival" if "rival" in res else "")
            ] += 1
            # tidy up so the next round's reaper only sees its own item
            conn.execute(
                "UPDATE work_items SET status = 'cancelled', worker_id = NULL, "
                "lease_until = NULL WHERE id = %s",
                (item["id"],),
            )
    finally:
        for c in conns:
            c.close()

    print(f"\nrace outcomes over {RACE_ROUNDS} rounds: {dict(seen)}")
    # both orders should show up; if this flakes the race window is too narrow to matter
    assert any(k.startswith("reaped") for k in seen)
    assert any(k.startswith("completed") for k in seen)
