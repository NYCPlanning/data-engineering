import os

import pytest
from fastapi.testclient import TestClient

from dispatch import db
from dispatch.app import create_app
from dispatch.config import Settings

KEY = {"X-Dispatch-Key": "sekrit"}


@pytest.fixture
def client(clean_db):
    settings = Settings(
        database_url=clean_db, shared_key="sekrit", reap_interval_seconds=0
    )
    with TestClient(create_app(settings)) as c:
        yield c


def test_health_needs_no_key(client):
    assert client.get("/healthz").json() == {"ok": True}
    ready = client.get("/readyz")
    assert ready.status_code == 200
    assert ready.json()["revision"] == db.head_revision()


def test_auth_required(client):
    assert client.post("/work/claim", json={"worker_id": "w"}).status_code == 401
    bad = {"X-Dispatch-Key": "nope"}
    assert client.get("/items/1", headers=bad).status_code == 401


def test_full_lifecycle_over_http(client):
    r = client.post(
        "/items",
        json={"run_id": "r", "asset_key": "a", "required_caps": ["gdal"]},
        headers=KEY,
    )
    assert r.status_code == 201
    item_id = r.json()["id"]
    assert "inserted" not in r.json()

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
    assert r.status_code == 201
    assert (
        client.get(f"/items/{item_id}/events", headers=KEY).json()[0]["kind"] == "log"
    )

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
    assert client.post("/items/999/cancel", headers=KEY).status_code == 404


def test_readyz_fails_when_not_migrated(db_url):
    # point at a database with no alembic_version table: the admin database
    ADMIN_URL = os.environ["DISPATCH_TEST_DATABASE_URL"]

    settings = Settings(database_url=ADMIN_URL, reap_interval_seconds=0)
    with TestClient(create_app(settings)) as c:
        r = c.get("/readyz")
    assert r.status_code == 503 and r.json()["revision"] is None


def test_reaper_thread_requeues(clean_db):
    import time

    settings = Settings(
        database_url=clean_db, lease_seconds=0.2, reap_interval_seconds=0.1
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
