"""Fixtures for tests against a real Valkey.

Set DAGSTER_BROKER_TEST_VALKEY_URL to a THROWAWAY Valkey (tests FLUSHDB it), run with the
repo's valkey.conf so it has maxmemory + noeviction like production:

    docker run --rm -d --name dagster-broker-test-valkey -p 56379:6379 \\
        -v "$PWD/apps/dagster-broker/valkey.conf:/etc/valkey/valkey.conf:ro" \\
        valkey/valkey:9.1.3-alpine valkey-server /etc/valkey/valkey.conf \\
        --bind 0.0.0.0 --protected-mode no --maxmemory 64mb
    export DAGSTER_BROKER_TEST_VALKEY_URL=redis://localhost:56379/0

Without the variable, every test that needs Valkey is skipped.
"""

import os
import time

import pytest
import redis

from dagster_broker import store

VALKEY_URL = os.environ.get("DAGSTER_BROKER_TEST_VALKEY_URL")
PREFIX = "dagster-broker:"


def new_client() -> redis.Redis:
    """A client with exactly one connection of its own (one per stress-test thread)."""
    assert VALKEY_URL
    return redis.Redis.from_url(
        VALKEY_URL, decode_responses=True, single_connection_client=True
    )


def new_store(**kwargs) -> store.ValkeyStore:
    return store.ValkeyStore(new_client(), prefix=PREFIX, **kwargs)


@pytest.fixture
def valkey_url():
    if not VALKEY_URL:
        pytest.skip("DAGSTER_BROKER_TEST_VALKEY_URL not set")
    r = redis.Redis.from_url(VALKEY_URL, decode_responses=True)
    r.flushdb()
    r.close()
    return VALKEY_URL


@pytest.fixture
def r(valkey_url):
    c = new_client()
    yield c
    c.close()


@pytest.fixture
def q(r):
    return store.ValkeyStore(r, prefix=PREFIX)


def all_items(r: redis.Redis) -> dict[str, dict]:
    out = {}
    for key in r.scan_iter(f"{PREFIX}item:*", count=1000):
        out[key.rsplit(":", 1)[1]] = store._item(r.hgetall(key))
    return out


def next_ms() -> None:
    """Sleep past the current millisecond, so the next enqueue has a later enqueue time.
    Equal-priority items are ordered by enqueue time, then by their (random) id."""
    time.sleep(0.002)


def attempts_log(r: redis.Redis, item_id: str) -> dict[str, str]:
    return r.hgetall(f"{PREFIX}att:{item_id}")


@pytest.fixture
def expire(r):
    """Push an item's lease into the past, as if the worker went quiet."""

    def _expire(item_id: str) -> None:
        past = int(r.time()[0]) * 1000 - 1000
        r.hset(f"{PREFIX}item:{item_id}", "lease_until", past)
        r.zadd(f"{PREFIX}leases", {item_id: past})

    return _expire
