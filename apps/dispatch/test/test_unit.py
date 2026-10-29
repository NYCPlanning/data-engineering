"""Tests that need no database, so CI always runs something even without Postgres."""

import pytest
from fastapi.testclient import TestClient

from dispatch import db
from dispatch.app import create_app
from dispatch.config import Settings


def test_url_helpers():
    assert db.sqlalchemy_url("postgresql://u:p@h/d") == "postgresql+psycopg://u:p@h/d"
    assert db.sqlalchemy_url("postgres://u:p@h/d") == "postgresql+psycopg://u:p@h/d"
    assert db.libpq_url("postgresql+psycopg://u:p@h/d") == "postgresql://u:p@h/d"


def test_single_migration_head():
    assert db.head_revision() == "0001"


def test_settings_from_env(monkeypatch):
    monkeypatch.delenv("DISPATCH_DATABASE_URL", raising=False)
    with pytest.raises(RuntimeError):
        Settings.from_env()
    monkeypatch.setenv("DISPATCH_DATABASE_URL", "postgresql://x/y")
    monkeypatch.setenv("DISPATCH_SHARED_KEY", "")
    monkeypatch.setenv("DISPATCH_LEASE_SECONDS", "30")
    s = Settings.from_env()
    assert s.shared_key is None and s.lease_seconds == 30.0


def test_auth_rejects_before_touching_db():
    # No lifespan (no `with`), so there is no pool: a 401 proves auth runs first.
    settings = Settings(database_url="postgresql://nowhere/x", shared_key="k")
    client = TestClient(create_app(settings))
    assert client.post("/work/claim", json={"worker_id": "w"}).status_code == 401
    assert client.get("/healthz").status_code == 200
