"""Tests that need no Valkey, so CI always runs something even without it."""

from fastapi.testclient import TestClient

from dagster_broker import store
from dagster_broker.app import create_app
from dagster_broker.config import Settings


def test_scripts_have_shebangs_and_drain_scripts_allow_oom():
    """Flagged scripts are OOM-checked once, before they start (never half-applied).
    Only scripts that grow memory may be refused when Valkey is full."""
    flags = {n: store._load_script(n).split("\n", 1)[0] for n in store.SCRIPT_NAMES}
    assert flags["enqueue"] == flags["events"] == "#!lua"
    assert flags["stats"] == "#!lua flags=no-writes"
    for n in ("claim", "heartbeat", "complete", "fail", "reap", "cancel"):
        assert flags[n] == "#!lua flags=allow-oom", n


def test_caps_signature_is_canonical():
    assert store.caps_signature(["b", "a", "b"]) == '["a","b"]'
    assert store.caps_signature([]) == "[]"


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("DAGSTER_BROKER_VALKEY_URL", "redis://x:1/0")
    monkeypatch.setenv("DAGSTER_BROKER_SHARED_KEY", "")
    monkeypatch.setenv("DAGSTER_BROKER_LEASE_SECONDS", "30")
    monkeypatch.setenv("DAGSTER_BROKER_TERMINAL_TTL_SECONDS", "60")
    s = Settings.from_env()
    assert s.shared_key is None and s.lease_seconds == 30.0
    assert s.valkey_url == "redis://x:1/0" and s.terminal_ttl_seconds == 60


def test_auth_rejects_before_touching_valkey():
    # No lifespan (no `with`), so there is no client: a 401 proves auth runs first.
    settings = Settings(valkey_url="redis://nowhere:1/0", shared_key="k")
    client = TestClient(create_app(settings))
    assert client.post("/work/claim", json={"worker_id": "w"}).status_code == 401
    assert client.get("/healthz").status_code == 200


def test_unreachable_valkey_is_503():
    settings = Settings(valkey_url="redis://127.0.0.1:1/0", reap_interval_seconds=0)
    with TestClient(create_app(settings)) as client:
        assert client.get("/readyz").status_code == 503
        res = client.post("/work/claim", json={"worker_id": "w"})
        assert res.status_code == 503
