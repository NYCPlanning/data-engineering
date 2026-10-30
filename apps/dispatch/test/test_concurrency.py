"""Concurrency stress tests: real threads, each with its own Valkey connection.

These are the tests for the failure Alex hit in an earlier queue (two workers claiming
the same job). See tracker bead inv-k0d.3. The last test is a mutation check: it swaps
the one-script claim for a split (pick, then take) claim and shows the race test fails.
"""

import random
import threading
import time
from collections import Counter, defaultdict

import pytest

from dispatch import store
from dispatch.store import StaleLease

from .conftest import PREFIX, all_items, attempts_log, new_store

N_WORKERS = 64


def run_threads(n, target):
    """Start n threads behind a barrier so they really hit Valkey at the same time."""
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


def pure_claim_race(q, r, n_items=400, claim=None):
    """64 threads drain n_items, completing each claim. Asserts each item was claimed
    exactly once, at attempt 1, and ended done. Returns (claims, elapsed)."""
    for i in range(n_items):
        q.enqueue(asset_key=f"a{i}", priority=i % 3)

    claims = [[] for _ in range(N_WORKERS)]

    def worker(i):
        s = new_store()
        take = claim or (lambda s, wid: s.claim(worker_id=wid, caps=[]))
        try:
            while (item := take(s, f"w{i}")) is not None:
                claims[i].append((item["id"], item["attempt"]))
                try:
                    s.complete(item["id"], attempt=item["attempt"], worker_id=f"w{i}")
                except StaleLease:
                    pass  # only a broken claim can get here; the asserts below catch it
        finally:
            s.r.close()

    started = time.monotonic()
    run_threads(N_WORKERS, worker)
    elapsed = time.monotonic() - started

    flat = [c for per in claims for c in per]
    assert len(set(i for i, _ in flat)) == n_items, "items lost"
    assert len(flat) == n_items, f"{len(flat) - n_items} extra claims (double claims)"
    assert {a for _, a in flat} == {1}
    assert sum(1 for per in claims if per) > N_WORKERS // 2, "claims weren't spread"

    items = all_items(r)
    assert len(items) == n_items
    assert Counter(i["status"] for i in items.values()) == {"done": n_items}
    claimant = {c[0]: f"w{i}" for i, per in enumerate(claims) for c in per}
    for item_id in items:
        assert attempts_log(r, item_id) == {"1": claimant[item_id], "1:outcome": "done"}
    assert q.stats() == store.Stats(queued=0, claimed=0, demand=0)
    return claims, elapsed


def test_pure_claim_race_each_item_claimed_once(q, r):
    claims, elapsed = pure_claim_race(q, r)
    print(
        f"\npure race: {N_WORKERS} workers, 400 items, "
        f"{sum(map(len, claims))} claims, {elapsed:.2f}s, "
        f"busiest worker {max(len(p) for p in claims)}"
    )


def test_chaos_claims_fail_abandon_reap(q, r):
    """Workers complete, fail, crash (abandon) or stall past their lease while a reaper
    runs continuously. Every item must end terminal, every (item, attempt) must be
    claimed at most once, and exactly one completion may succeed per done item."""
    n_items = 300
    lease = 0.3
    for i in range(n_items):
        q.enqueue(asset_key=f"a{i}", max_attempts=3, priority=i % 5)

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
        s = new_store()
        while not stop_reaper.is_set():
            rows = s.reap()
            with lock:
                reaped.extend(rows)
            time.sleep(0.05)
        s.r.close()

    def worker(i):
        rng = random.Random(i)
        wid = f"w{i}"
        s = new_store()
        while time.monotonic() < deadline:
            item = s.claim(worker_id=wid, caps=[], lease_seconds=lease)
            if item is None:
                st = s.stats()
                if st.queued == 0 and st.claimed == 0:
                    break
                time.sleep(0.02)
                continue
            token = {"attempt": item["attempt"], "worker_id": wid}
            with lock:
                claims[wid].append((item["id"], item["attempt"]))
            roll = rng.random()
            try:
                if roll < 0.55:
                    s.complete(item["id"], **token)
                    with lock:
                        completed.append((item["id"], item["attempt"]))
                    bump("complete")
                elif roll < 0.70:
                    s.fail(item["id"], error="boom", **token)
                    bump("fail_retryable")
                elif roll < 0.75:
                    s.fail(item["id"], retryable=False, **token)
                    bump("fail_final")
                elif roll < 0.90:
                    bump("abandon")  # crash: never heard from again
                else:
                    time.sleep(rng.uniform(0.2, 0.6))  # stall around the lease
                    s.heartbeat(item["id"], lease_seconds=lease, **token)
                    s.complete(item["id"], **token)
                    with lock:
                        completed.append((item["id"], item["attempt"]))
                    bump("late_complete_ok")
            except StaleLease:
                bump("stale_409")
        s.r.close()

    reaper_thread = threading.Thread(target=reaper)
    reaper_thread.start()
    started = time.monotonic()
    try:
        run_threads(N_WORKERS, worker)
    finally:
        stop_reaper.set()
        reaper_thread.join()
    elapsed = time.monotonic() - started

    items = all_items(r)
    assert len(items) == n_items, "items lost"
    status = Counter(i["status"] for i in items.values())
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

    logged = set()
    for item_id in items:
        for field in attempts_log(r, item_id):
            if ":" not in field:
                logged.add((item_id, int(field)))
    assert logged == set(flat)
    assert outcomes["abandon"] > 0 and reaped, "reaper path wasn't exercised"
    assert q.stats() == store.Stats(queued=0, claimed=0, demand=0)
    assert r.zcard(f"{PREFIX}leases") == 0 and r.scard(f"{PREFIX}sigs") == 0

    print(
        f"\nchaos: {N_WORKERS} workers, {n_items} items, {len(flat)} claims in "
        f"{elapsed:.2f}s; outcomes {dict(outcomes)}; reaped {len(reaped)}; "
        f"final {dict(status)}"
    )


RACE_ROUNDS = 150


@pytest.mark.parametrize("rival", [False, True], ids=["no-rival", "with-rival"])
def test_reaper_vs_late_heartbeat_vs_late_complete(q, r, expire, rival):
    """An expired-but-unreaped holder sends a heartbeat and a complete at the same moment
    the reaper runs (and, with a rival, while another worker tries to claim). Exactly one
    of {reaped, completed} happens, and the state agrees with whichever it was."""
    seen = Counter()
    jitter = random.Random(42)
    stores = [new_store() for _ in range(4)]
    try:
        for _ in range(RACE_ROUNDS):
            item, _ = q.enqueue()
            q.claim(worker_id="holder", caps=[])
            expire(item["id"])
            token = {"attempt": 1, "worker_id": "holder"}
            res = {}

            def act(i):
                s = stores[i]
                # jitter the start so every ordering of the racers actually occurs
                time.sleep(jitter.uniform(0, 0.004))
                try:
                    if i == 0:
                        res["reaped"] = bool(s.reap())
                    elif i == 1:
                        s.heartbeat(item["id"], **token)
                        res["hb"] = True
                    elif i == 2:
                        s.complete(item["id"], **token)
                        res["completed"] = True
                    else:
                        for _ in range(3):
                            got = s.claim(worker_id="rival", caps=[])
                            if got:
                                res["rival"] = got["attempt"]
                                break
                except StaleLease:
                    pass

            run_threads(4 if rival else 3, act)
            reaped_, hb, completed_ = (
                res.get("reaped", False),
                res.get("hb", False),
                res.get("completed", False),
            )
            row = q.get_item(item["id"])

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
            # tidy up so the next round's reaper and claimers only see their own item
            q.cancel(item["id"])
            if row["status"] == "claimed":
                q.fail(item["id"], attempt=2, worker_id="rival")
    finally:
        for s in stores:
            s.r.close()

    print(f"\nrace outcomes over {RACE_ROUNDS} rounds: {dict(seen)}")
    # both orders should show up; if this flakes the race window is too narrow to matter
    assert any(k.startswith("reaped") for k in seen)
    assert any(k.startswith("completed") for k in seen)


def split_claim(s: store.ValkeyStore, worker_id: str) -> dict | None:
    """A deliberately broken claim: pick the head in one round trip, take it in another
    (the read-then-write shape of Alex's old Hasura queue). No atomicity, no re-check."""
    sig = store.caps_signature([])
    head = s.r.zrange(f"{PREFIX}q:{sig}", 0, 0)
    if not head:
        return None
    item_id = int(head[0])
    k = f"{PREFIX}item:{item_id}"
    attempt = s.r.hincrby(k, "attempt", 1)
    s.r.zrem(f"{PREFIX}q:{sig}", head[0])
    s.r.zrem(f"{PREFIX}qt:{sig}", head[0])
    s.r.hset(k, mapping={"status": "claimed", "worker_id": worker_id, "lease_until": 0})
    s.r.hset(f"{PREFIX}att:{item_id}", str(attempt), worker_id)
    return {"id": item_id, "attempt": attempt}


def test_mutation_check_split_claim_is_caught(q, r):
    """The stress test must fail if the claim stops being one atomic script."""
    with pytest.raises(AssertionError) as caught:
        pure_claim_race(q, r, claim=split_claim)
    print(f"\nsplit claim caught: {str(caught.value).splitlines()[0]}")
