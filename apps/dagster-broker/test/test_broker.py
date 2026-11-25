"""The queue against a real Valkey. Set DAGSTER_BROKER_TEST_VALKEY_URL to a throwaway
Valkey started with valkey.conf (the tests flush it), as CI does; otherwise they skip."""

import contextlib
import os
import random
import threading
import time
from collections import Counter

import pytest
import redis
from fastapi.testclient import TestClient

from dagster_broker import store
from dagster_broker.app import create_app
from dagster_broker.store import P, Stale, Store

VALKEY_URL = os.environ.get("DAGSTER_BROKER_TEST_VALKEY_URL")
N_WORKERS = 64


@pytest.fixture
def valkey_url():
    if not VALKEY_URL:
        pytest.skip("DAGSTER_BROKER_TEST_VALKEY_URL not set")
    redis.Redis.from_url(VALKEY_URL).flushdb()
    return VALKEY_URL


def new_store() -> Store:
    """A store on one connection of its own (one per stress-test thread)."""
    r = redis.Redis.from_url(
        VALKEY_URL, decode_responses=True, single_connection_client=True
    )
    return Store(r)


@pytest.fixture
def q(valkey_url):
    s = new_store()
    yield s
    s.r.close()


def all_items(r) -> dict[str, dict]:
    keys = r.scan_iter(f"{P}item:*", count=1000)
    return {k.rsplit(":", 1)[1]: store.item(r.hgetall(k)) for k in keys}


def run_threads(n, target):
    """n threads behind a barrier, so they really hit Valkey at the same time."""
    barrier = threading.Barrier(n)
    errors = []

    def wrapped(i):
        try:
            barrier.wait()
            target(i)
        except BaseException as e:
            errors.append(e)

    threads = [threading.Thread(target=wrapped, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    assert not any(t.is_alive() for t in threads), "threads hung"
    if errors:
        raise errors[0]


@pytest.fixture
def api(valkey_url, monkeypatch):
    monkeypatch.setenv("DAGSTER_BROKER_VALKEY_URL", valkey_url)
    monkeypatch.setenv("DAGSTER_BROKER_SHARED_KEY", "k")
    monkeypatch.setenv("DAGSTER_BROKER_REAP_SECONDS", "3600")
    with TestClient(create_app(), headers={"X-Dagster-Broker-Key": "k"}) as c:
        yield c


def test_http_lifecycle(api):
    assert api.get("/readyz").status_code == 200
    assert (
        api.get("/items/x", headers={"X-Dagster-Broker-Key": "no"}).status_code == 401
    )
    assert api.get("/items/nope").status_code == 404

    def enqueue(status=201, **body):
        res = api.post("/items", json=body)
        assert res.status_code == status, res.text
        return res.json()["id"]

    gpu = enqueue(required_caps=["gpu"])
    affine = enqueue(run_id="r1")
    high = enqueue(priority=5)
    idem = enqueue(idempotency_key="k1", max_attempts=1)
    assert enqueue(200, idempotency_key="k1") == idem
    demand = api.get("/metrics/demand?caps=gpu").json()
    assert demand == {"queued": 4, "claimed": 0, "demand": 4}
    assert api.get("/metrics/demand").json()["demand"] == 3

    def claim(caps=(), affinity=()):
        body = {"worker_id": "w", "caps": caps, "affinity_run_ids": affinity}
        res = api.post("/work/claim", json=body)
        return res.json()["id"] if res.status_code == 200 else None

    assert claim(affinity=["r1"]) == affine  # affinity beats priority
    assert claim() == high  # priority beats age
    assert claim() == idem  # no gpu cap: gpu is skipped
    assert claim(["gpu", "x"]) == gpu  # required caps are a subset
    assert claim(["gpu"]) is None

    token = {"worker_id": "w", "attempt": 1}
    hb = api.post(f"/work/{affine}/heartbeat", json=token)
    assert hb.json()["cancel"] is False
    stale = [{"worker_id": "w", "attempt": 2}, {"worker_id": "v", "attempt": 1}]
    for bad in stale:
        assert api.post(f"/work/{affine}/heartbeat", json=bad).status_code == 409
    assert api.post("/work/nope/heartbeat", json=token).status_code == 404

    events = [{"kind": "pipes", "data": {"n": i}} for i in range(3)]
    api.post(f"/work/{affine}/events", json={**token, "events": events})
    page = api.get(f"/items/{affine}/events").json()
    assert [e["data"]["n"] for e in page["events"]] == [0, 1, 2]
    after = api.get(f"/items/{affine}/events?after={page['cursor']}").json()
    assert after["events"] == []

    done = api.post(f"/work/{affine}/complete", json={**token, "result": {"x": 1}})
    assert done.json()["status"] == "done" and done.json()["result"] == {"x": 1}
    assert api.post(f"/work/{affine}/complete", json=token).status_code == 409
    assert redis.Redis.from_url(VALKEY_URL).ttl(f"{P}item:{affine}") > 0

    res = api.post(f"/work/{high}/fail", json={**token, "error": "boom"})
    assert res.json()["status"] == "queued"
    assert api.post(f"/work/{idem}/fail", json=token).json()["status"] == "dead"
    assert api.post(f"/items/{high}/cancel").json()["status"] == "cancelled"
    assert api.post(f"/items/{gpu}/cancel").json()["cancel_requested"] is True
    assert api.post(f"/work/{gpu}/heartbeat", json=token).json()["cancel"] is True
    assert api.post(f"/work/{gpu}/fail", json=token).json()["status"] == "cancelled"


def claim_race(q, n_items=400):
    """64 threads drain n_items, completing each claim. Every item must be claimed exactly
    once, at attempt 1, and end done."""
    for i in range(n_items):
        q.enqueue(priority=i % 3)
    claims = [[] for _ in range(N_WORKERS)]

    def worker(i):
        s, wid = new_store(), f"w{i}"
        while (item := s.claim(wid, [])) is not None:
            claims[i].append((item["id"], item["attempt"]))
            try:
                s.complete(item["id"], item["attempt"], wid)
            except Stale:
                pass  # only an unlocked claim gets here; the asserts catch it
        s.r.close()

    run_threads(N_WORKERS, worker)
    flat = [c for per in claims for c in per]
    assert len({i for i, _ in flat}) == n_items, "items lost"
    assert len(flat) == n_items, f"{len(flat) - n_items} double claims"
    assert {a for _, a in flat} == {1}
    assert sum(1 for per in claims if per) > 4, "threads didn't really race"
    items = all_items(q.r)
    assert Counter(i["status"] for i in items.values()) == {"done": n_items}
    assert q.stats() == {"queued": 0, "claimed": 0, "demand": 0}


def test_claim_race_each_item_claimed_once(q):
    claim_race(q)


def test_mutation_check_unlocked_claim_is_caught(q, monkeypatch):
    """The race test must fail if the claim's read and write aren't under the lock."""
    monkeypatch.setattr(store, "LOCK", contextlib.nullcontext())
    with pytest.raises(AssertionError, match="double claims"):
        claim_race(q)


@pytest.mark.parametrize("rival", [False, True], ids=["no-rival", "with-rival"])
def test_reaper_vs_late_heartbeat_vs_late_complete(q, rival):
    """An expired-but-unreaped holder sends a heartbeat and a complete while the reaper
    runs (and, with a rival, while another worker claims). Exactly one of {reaped,
    completed} happens, and the item's state agrees with it."""
    seen = Counter()
    jitter = random.Random(42)
    stores = [new_store() for _ in range(4)]
    for _ in range(150):
        item, _ = q.enqueue()
        q.claim("holder", [])
        past = store.now_ms() - 1000
        q.r.hset(f"{P}item:{item['id']}", "lease_until", past)
        q.r.zadd(f"{P}leases", {item["id"]: past})
        res = {}

        def act(i):
            s = stores[i]
            time.sleep(jitter.uniform(0, 0.004))  # so every ordering occurs
            try:
                if i == 0:
                    res["reaped"] = bool(s.reap())
                elif i == 1:
                    res["hb"] = bool(s.heartbeat(item["id"], 1, "holder"))
                elif i == 2:
                    res["completed"] = bool(s.complete(item["id"], 1, "holder"))
                else:
                    for _ in range(3):
                        if got := s.claim("rival", []):
                            res["rival"] = got["attempt"]
                            break
            except Stale:
                pass

        run_threads(4 if rival else 3, act)
        reaped, completed = res.get("reaped", False), res.get("completed", False)
        row = q.get(item["id"])
        assert reaped != completed, (res, row)
        if res.get("hb"):
            assert not reaped
        if completed:
            assert row["status"] == "done" and row["attempt"] == 1
            assert "rival" not in res
        elif "rival" in res:
            assert (row["status"], row["attempt"], row["worker_id"]) == (
                "claimed", 2, "rival",
            )  # fmt: skip
        else:
            assert (row["status"], row["attempt"]) == ("queued", 1)
        seen["reaped" if reaped else "completed"] += 1
        q.cancel(item["id"])
        if row["status"] == "claimed":
            q.fail(item["id"], 2, "rival")
    for s in stores:
        s.r.close()
    assert seen["reaped"] and seen["completed"], f"one order never happened: {seen}"
