"""Fixtures for tests against a real Postgres.

Set DISPATCH_TEST_DATABASE_URL to a server where the user may CREATE DATABASE, e.g.

    docker run --rm -d --name dispatch-test-pg -p 55432:5432 \\
        -e POSTGRES_PASSWORD=postgres postgres:16-alpine -c max_connections=200
    export DISPATCH_TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:55432/postgres

Each test session creates its own throwaway database there and drops it afterwards.
Without the variable, every test that needs the database is skipped.
"""

import os
import uuid
from urllib.parse import urlsplit, urlunsplit

import psycopg
import pytest

from dispatch import db

ADMIN_URL = os.environ.get("DISPATCH_TEST_DATABASE_URL")


def _with_dbname(url: str, dbname: str) -> str:
    parts = urlsplit(url)
    return urlunsplit(parts._replace(path="/" + dbname))


@pytest.fixture(scope="session")
def db_url():
    if not ADMIN_URL:
        pytest.skip("DISPATCH_TEST_DATABASE_URL not set")
    name = f"dispatch_test_{uuid.uuid4().hex[:10]}"
    with psycopg.connect(ADMIN_URL, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{name}"')
    url = _with_dbname(ADMIN_URL, name)
    try:
        db.migrate(url)
        yield url
    finally:
        with psycopg.connect(ADMIN_URL, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


@pytest.fixture
def clean_db(db_url):
    with db.connect(db_url) as conn:
        conn.execute(
            "TRUNCATE events, work_attempts, work_items, workers RESTART IDENTITY"
        )
    return db_url


@pytest.fixture
def conn(clean_db):
    with db.connect(clean_db) as c:
        yield c


@pytest.fixture
def expire(conn):
    """Push an item's lease into the past, as if the worker went quiet."""

    def _expire(item_id: int) -> None:
        conn.execute(
            "UPDATE work_items SET lease_until = now() - interval '1 second' "
            "WHERE id = %s",
            (item_id,),
        )

    return _expire
