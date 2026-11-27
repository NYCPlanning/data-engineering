"""The catalog's lifecycle event handler: writes through archived datasets when configured."""

import json
from importlib.metadata import entry_points
from unittest import mock

import pytest

from dcpy.connectors.hybrid_pathed_storage import PathedStorageConnector, StorageType
from dcpy.connectors.ingest_datastore import Connector as IngestDatastore
from dcpy.lifecycle import events
from dcpy.services import catalog
from dcpy.services.catalog import ingest_versions, lifecycle_events

STORE = "edm.recipes.datasets"
EVENT = events.DatasetsArchived(
    store=STORE,
    datasets=(events.ArchivedVersion("a", "v2"),),
    latest=True,
)


@pytest.fixture
def datastore(tmp_path):
    root = tmp_path / "datasets"
    config = {
        "id": "a",
        "version": "v2",
        "attributes": {"name": "a name"},
        "transformation": {"run_details": {"timestamp": "2026-11-01T22:30:00-04:00"}},
    }
    for folder in ["v2", "latest"]:
        (root / "a" / folder).mkdir(parents=True)
        (root / "a" / folder / "a.parquet").write_bytes(b"")
        (root / "a" / folder / "config.json").write_text(json.dumps(config))
    store = IngestDatastore(
        conn_type=STORE,
        storage=PathedStorageConnector.from_storage_kwargs(
            conn_type=STORE, storage_backend=StorageType.LOCAL, local_dir=root
        ),
    )
    with mock.patch.object(
        lifecycle_events, "connectors", {(STORE, IngestDatastore): store}
    ):
        yield store


@pytest.fixture
def fake_db(monkeypatch):
    """The catalog's table, as a list."""
    rows: list = []
    monkeypatch.setattr(ingest_versions, "query", lambda domain, **kw: [])
    monkeypatch.setattr(
        ingest_versions,
        "upsert",
        lambda new, **kw: rows.extend(new) or catalog.WriteCounts(added=len(new)),
    )
    return rows


def test_skips_when_unconfigured(datastore, fake_db, monkeypatch):
    monkeypatch.delenv(catalog.CATALOG_PG_URL_ENV_VAR, raising=False)
    lifecycle_events.handle(EVENT)
    assert fake_db == []


def test_upserts_when_configured(datastore, fake_db, monkeypatch):
    monkeypatch.setenv(catalog.CATALOG_PG_URL_ENV_VAR, "postgresql://catalog")
    lifecycle_events.handle(EVENT)
    [row] = fake_db
    assert (row.store, row.key, row.version, row.is_latest) == (STORE, "a", "v2", True)
    assert row.details[ingest_versions.NAME] == "a name"


def test_ignores_other_events(fake_db, monkeypatch):
    monkeypatch.setenv(catalog.CATALOG_PG_URL_ENV_VAR, "postgresql://catalog")
    lifecycle_events.handle(events.Event())
    assert fake_db == []


def test_registered_as_a_lifecycle_event_handler():
    [ep] = [
        ep
        for ep in entry_points(group=events.ENTRY_POINT_GROUP)
        if ep.name == "catalog"
    ]
    assert ep.load() is lifecycle_events.handle
