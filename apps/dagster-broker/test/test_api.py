import time

import pytest
from fastapi.testclient import TestClient

from dagster_broker.app import create_app
from dagster_broker.config import Settings

from .conftest import all_items

KEY = {"X-Dagster-Broker-Key": "sekrit"}


@pytest.fixture
def client(valkey_url):
    settings = Settings(
        valkey_url=valkey_url, shared_key="sekrit", reap_interval_seconds=0
    )
    with TestClient(create_app(settings)) as c:
        yield c


def test_health_needs_no_key(client):
    assert client.get("/healthz").json() == {"ok": True}
    ready = client.get("/readyz")
    assert ready.status_code == 200, ready.json()
    assert ready.json()["maxmemory_policy"] == "noeviction"


def test_readyz_fails_on_evicting_policy_or_no_maxmemory(client, r):
    before = r.config_get("maxmemory*")
    try:
        r.config_set("maxmemory-policy", "allkeys-lru")
        res = client.get("/readyz")
        assert res.status_code == 503 and "noeviction" in res.json()["problems"][0]
        r.config_set("maxmemory-policy", "noeviction")
        r.config_set("maxmemory", 0)
        res = client.get("/readyz")
        assert res.status_code == 503
        assert res.json()["problems"] == ["maxmemory is not set"]
    finally:
        r.config_set("maxmemory", before["maxmemory"])
        r.config_set("maxmemory-policy", before["maxmemory-policy"])
    assert client.get("/readyz").status_code == 200


def test_auth_required(client):
    assert client.post("/work/claim", json={"worker_id": "w"}).status_code == 401
    bad = {"X-Dagster-Broker-Key": "nope"}
    assert client.get("/items/1", headers=bad).status_code == 401
    assert client.get("/metrics/demand", headers=bad).status_code == 401


def test_full_lifecycle_over_http(client):
    r = client.post(
        "/items",
        json={"run_id": "r", "asset_key": "a", "required_caps": ["gdal"]},
        headers=KEY,
    )
    assert r.status_code == 201
    item_id = r.json()["id"]

    nothing = client.post(
        "/work/claim", json={"worker_id": "w", "caps": []}, headers=KEY
    )
    assert nothing.status_code == 204

    r = client.post(
        "/work/claim", json={"worker_id": "w", "caps": ["gdal"]}, headers=KEY
    )
    assert r.status_code == 200 and r.json()["id"] == item_id
    token = {"worker_id": "w", "attempt": r.json()["attempt"]}

    r = client.post(f"/work/{item_id}/heartbeat", json=token, headers=KEY)
    assert r.status_code == 200 and r.json()["cancel"] is False

    r = client.post(
        f"/work/{item_id}/events",
        json={**token, "kind": "log", "data": {"m": "hi"}},
        headers=KEY,
    )
    assert r.status_code == 201 and len(r.json()["ids"]) == 1
    r = client.post(
        f"/work/{item_id}/events",
        json={**token, "events": [{"kind": "pipes", "data": {"n": 1}}] * 3},
        headers=KEY,
    )
    assert r.status_code == 201 and len(r.json()["ids"]) == 3
    page = client.get(f"/items/{item_id}/events?limit=2", headers=KEY).json()
    assert [e["kind"] for e in page["events"]] == ["log", "pipes"]
    rest = client.get(
        f"/items/{item_id}/events", params={"after": page["cursor"]}, headers=KEY
    ).json()
    assert len(rest["events"]) == 2
    empty = client.get(
        f"/items/{item_id}/events", params={"after": rest["cursor"]}, headers=KEY
    ).json()
    assert empty == {"events": [], "cursor": rest["cursor"]}

    stale = {"worker_id": "w", "attempt": 99}
    assert (
        client.post(f"/work/{item_id}/heartbeat", json=stale, headers=KEY).status_code
        == 409
    )
    assert (
        client.post("/work/12345/heartbeat", json=token, headers=KEY).status_code == 404
    )

    r = client.post(
        f"/work/{item_id}/complete", json={**token, "result": {"rows": 3}}, headers=KEY
    )
    assert r.status_code == 200 and r.json()["status"] == "done"
    assert (
        client.post(f"/work/{item_id}/complete", json=token, headers=KEY).status_code
        == 409
    )
    assert client.get(f"/items/{item_id}", headers=KEY).json()["result"] == {"rows": 3}


def test_gone_items_are_404(client):
    """Ephemeral-safe contract: anything about an unknown item is 404, never 500."""
    token = {"worker_id": "w", "attempt": 1}
    assert client.get("/items/77", headers=KEY).status_code == 404
    assert client.get("/items/77/events", headers=KEY).status_code == 404
    assert client.post("/items/77/cancel", headers=KEY).status_code == 404
    for verb in ("heartbeat", "complete", "fail"):
        assert (
            client.post(f"/work/77/{verb}", json=token, headers=KEY).status_code == 404
        )
    r = client.post("/work/77/events", json={**token, "kind": "log"}, headers=KEY)
    assert r.status_code == 404


def test_idempotent_enqueue_returns_200(client):
    body = {"idempotency_key": "k1"}
    first = client.post("/items", json=body, headers=KEY)
    second = client.post("/items", json=body, headers=KEY)
    assert (first.status_code, second.status_code) == (201, 200)
    assert first.json()["id"] == second.json()["id"]


def test_fail_and_cancel_over_http(client):
    item_id = client.post("/items", json={"max_attempts": 1}, headers=KEY).json()["id"]
    claimed = client.post("/work/claim", json={"worker_id": "w"}, headers=KEY).json()
    r = client.post(
        f"/work/{item_id}/fail",
        json={"worker_id": "w", "attempt": claimed["attempt"], "error": "x"},
        headers=KEY,
    )
    assert r.json()["status"] == "dead"

    other = client.post("/items", json={}, headers=KEY).json()["id"]
    assert (
        client.post(f"/items/{other}/cancel", headers=KEY).json()["status"]
        == "cancelled"
    )
    # cancelling a terminal item is a no-op that returns it
    assert (
        client.post(f"/items/{item_id}/cancel", headers=KEY).json()["status"] == "dead"
    )


def test_metrics_demand(client):
    for caps in ([], ["gdal"], ["gdal", "big"], ["gpu"]):
        client.post("/items", json={"required_caps": caps}, headers=KEY)

    def get(query):
        return client.get(f"/metrics/demand{query}", headers=KEY).json()

    assert get("")["demand"] == 1
    assert get("?caps=gdal")["demand"] == 2
    both = get("?caps=gdal,big")
    assert both == {
        "demand": 3,
        "caps": ["big", "gdal"],
        "min_age_seconds": 0.0,
        "queued": 4,
        "claimed": 0,
    }
    assert get("?caps=gdal&caps=big&caps=gpu")["demand"] == 4
    assert get("?caps=gdal&min_age_seconds=3600")["demand"] == 0
    assert (
        client.get("/metrics/demand?min_age_seconds=-1", headers=KEY).status_code == 422
    )


def test_full_store_refuses_new_work_cleanly(client, r):
    """Under maxmemory + noeviction, enqueue and events get a clean 507 and nothing is
    dropped or half-written; workers can still claim, heartbeat and finish (allow-oom)."""
    before = r.config_get("maxmemory")["maxmemory"]
    accepted = []
    try:
        claimed = None
        r.config_set("maxmemory", int(r.info("memory")["used_memory"]) + 400_000)
        blob = {"pad": "x" * 8000}
        for _ in range(2000):
            res = client.post("/items", json={"payload": blob}, headers=KEY)
            if res.status_code == 507:
                break
            assert res.status_code == 201, res.text
            accepted.append(res.json()["id"])
            if claimed is None:
                claimed = client.post(
                    "/work/claim", json={"worker_id": "w"}, headers=KEY
                ).json()
        else:
            pytest.fail("store never filled up")
        assert res.json()["detail"].startswith("dagster-broker store is full")
        assert res.headers["Retry-After"]

        # pin it well over the limit, so used_memory jitter can't let writes back in
        r.config_set("maxmemory", int(r.info("memory")["used_memory"]) // 2)
        assert client.post("/items", json={}, headers=KEY).status_code == 507
        token = {"worker_id": "w", "attempt": 1}
        cid = claimed["id"]
        res = client.post(
            f"/work/{cid}/events", json={**token, "kind": "log"}, headers=KEY
        )
        assert res.status_code == 507
        assert (
            client.post(f"/work/{cid}/heartbeat", json=token, headers=KEY).status_code
            == 200
        )
        assert (
            client.post(f"/work/{cid}/complete", json=token, headers=KEY).status_code
            == 200
        )
        nxt = client.post("/work/claim", json={"worker_id": "w2"}, headers=KEY)
        assert nxt.status_code == 200
        assert client.get("/metrics/demand", headers=KEY).status_code == 200
    finally:
        r.config_set("maxmemory", before)

    # every accepted item is there and intact, and no refused enqueue left anything behind
    items = all_items(r)
    assert sorted(items) == sorted(accepted) and len(accepted) > 5
    assert all(i["payload"] == blob for i in items.values())
    print(f"\nfull store: {len(accepted)} items accepted before 507")


def test_reaper_thread_requeues(valkey_url):
    settings = Settings(
        valkey_url=valkey_url, lease_seconds=0.2, reap_interval_seconds=0.1
    )
    with TestClient(create_app(settings)) as c:
        item_id = c.post("/items", json={}).json()["id"]
        c.post("/work/claim", json={"worker_id": "w"})
        for _ in range(50):
            time.sleep(0.1)
            if c.get(f"/items/{item_id}").json()["status"] == "queued":
                break
        assert c.get(f"/items/{item_id}").json()["status"] == "queued"


def test_request_validation(client):
    r = client.post("/work/claim", json={"worker_id": ""}, headers=KEY)
    assert r.status_code == 422
    r = client.post(
        "/work/1/complete", json={"worker_id": "w", "attempt": 0}, headers=KEY
    )
    assert r.status_code == 422
    both = {"worker_id": "w", "attempt": 1, "kind": "x", "events": [{"kind": "y"}]}
    assert client.post("/work/1/events", json=both, headers=KEY).status_code == 422
    neither = {"worker_id": "w", "attempt": 1}
    assert client.post("/work/1/events", json=neither, headers=KEY).status_code == 422
    assert client.get("/items/1/events?after=bogus", headers=KEY).status_code == 422
