"""The version catalog against a live Postgres.

Uses DCPY_CATALOG_TEST_PG_URL, by default the dev container's postgis service. Drops and
recreates the `catalog` schema in that database, so never point it at a real catalog.
"""

import os
import threading
from datetime import datetime

import psycopg2
import pytest

from dcpy.services import catalog
from dcpy.services.catalog import VersionRow

URL = os.environ.get(
    "DCPY_CATALOG_TEST_PG_URL", "postgresql://postgres:postgres@postgis:5432/postgres"
)
STORE = "edm.recipes.datasets"
WHEN = datetime.fromisoformat("2026-11-01T22:30:00-04:00")


def _drop():
    conn = psycopg2.connect(URL)
    with conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS catalog CASCADE")
    conn.close()


@pytest.fixture
def db():
    _drop()
    catalog.migrate(URL)
    yield URL
    _drop()


def _row(key, version, **kwargs) -> VersionRow:
    return VersionRow(
        domain="ingest",
        store=kwargs.pop("store", STORE),
        key=key,
        version=version,
        location=f"s3://edm-recipes/datasets/{key}/{version}/",
        file_types=["parquet"],
        archived_at=WHEN,
        **kwargs,
    )


def test_migrate_is_idempotent(db):
    assert catalog.migrate(db) == []
    conn = psycopg2.connect(db)
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_name IN ('versions', 'schema_migrations') ORDER BY 2"
        )
        assert cur.fetchall() == [
            ("catalog", "schema_migrations"),
            ("catalog", "versions"),
        ]
    conn.close()


def test_migrate_concurrently():
    _drop()
    results: list = []
    errors: list = []

    def run():
        try:
            results.append(catalog.migrate(URL))
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=run) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert sorted(len(r) for r in results) == [0, 0, 0, len(catalog.migration_files())]
    _drop()


def test_upsert_and_query(db):
    counts = catalog.upsert(
        [_row("a", "v1"), _row("a", "v2", is_latest=True)], conn_url=db
    )
    assert (counts.added, counts.updated) == (2, 0)
    counts = catalog.upsert(
        [_row("a", "v1"), _row("a", "v2", is_latest=True, details={"name": "A"})],
        conn_url=db,
    )
    assert (counts.added, counts.updated, counts.unchanged) == (0, 1, 1)

    rows = catalog.query("ingest", keys=["a"], conn_url=db)
    assert [(r.version, r.is_latest) for r in rows] == [("v1", False), ("v2", True)]
    assert rows[1].details == {"name": "A"}
    assert rows[0].archived_at == WHEN
    assert rows[0].first_seen_at is not None
    assert [
        r.version for r in catalog.query("ingest", latest_only=True, conn_url=db)
    ] == ["v2"]
    assert catalog.query("builds", conn_url=db) == []


def test_upsert_can_keep_is_latest(db):
    catalog.upsert([_row("a", "v1", is_latest=True)], conn_url=db)
    catalog.upsert([_row("a", "v1")], update_is_latest=False, conn_url=db)
    assert catalog.query("ingest", conn_url=db)[0].is_latest


def test_set_latest(db):
    catalog.upsert([_row("a", "v1", is_latest=True), _row("a", "v2")], conn_url=db)
    assert catalog.set_latest("ingest", STORE, "a", "v2", conn_url=db)
    rows = catalog.query("ingest", conn_url=db)
    assert [(r.version, r.is_latest) for r in rows] == [("v1", False), ("v2", True)]
    # no row for it: nothing changes
    assert not catalog.set_latest("ingest", STORE, "a", "v9", conn_url=db)
    assert [r.is_latest for r in catalog.query("ingest", conn_url=db)] == [False, True]


def test_sync_store_deletes_missing_in_that_store_only(db):
    catalog.upsert(
        [
            _row("a", "v1"),
            _row("a", "v2"),
            _row("a", "v1", store="az.edm.recipes.datasets"),
        ],
        conn_url=db,
    )
    counts = catalog.sync_store(
        "ingest", STORE, [_row("a", "v2"), _row("b", "v1")], conn_url=db
    )
    assert (counts.added, counts.removed, counts.unchanged) == (1, 1, 1)
    assert sorted(
        (r.store, r.key, r.version) for r in catalog.query("ingest", conn_url=db)
    ) == [
        ("az.edm.recipes.datasets", "a", "v1"),
        (STORE, "a", "v2"),
        (STORE, "b", "v1"),
    ]


def test_sync_store_refuses_empty_listing(db):
    catalog.upsert([_row("a", "v1")], conn_url=db)
    with pytest.raises(ValueError, match="found nothing"):
        catalog.sync_store("ingest", STORE, [], conn_url=db)
    assert (
        catalog.sync_store("ingest", STORE, [], allow_empty=True, conn_url=db).removed
        == 1
    )
