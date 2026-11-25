"""Catalog row building from an ingest datastore listing. No database: the writes and queries
are covered in apps/dcpy_integration_tests/connectors/edm/test_catalog.py."""

import json
from datetime import datetime
from pathlib import Path

import pytest

from dcpy.connectors.edm import catalog
from dcpy.connectors.edm.catalog import ingest_versions as iv
from dcpy.connectors.hybrid_pathed_storage import PathedStorageConnector, StorageType
from dcpy.connectors.ingest_datastore import Connector as IngestDatastore

STORE = "edm.recipes.datasets"
INGEST_TS = "2026-11-01T22:30:00-04:00"


def _ingest_config(key: str, version: str, ts: str = INGEST_TS) -> dict:
    return {
        "id": key,
        "version": version,
        "attributes": {"name": f"{key} name", "url": f"https://example.com/{key}"},
        "transformation": {"run_details": {"timestamp": ts}},
    }


def _write(root: Path, path: str, body: bytes | dict = b"") -> None:
    p = root / path
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(json.dumps(body).encode() if isinstance(body, dict) else body)


@pytest.fixture
def store(tmp_path) -> tuple[IngestDatastore, Path]:
    root = tmp_path / "datasets"
    _write(root, "a/v1/a.parquet")
    _write(root, "a/v1/config.json", _ingest_config("a", "v1"))
    _write(root, "a/v2/a.parquet")
    _write(root, "a/v2/a.csv")
    _write(root, "a/v2/config.json", _ingest_config("a", "v2"))
    _write(root, "a/latest/a.parquet")
    _write(root, "a/latest/config.json", _ingest_config("a", "v2"))
    # set aside by an overwrite: not a version
    _write(root, "a/v1_library/a.sql")
    _write(
        root, "a/v1_library/config.json", {"dataset": {"name": "a", "version": "v1"}}
    )
    # library-era archive
    _write(root, "lib/2020/lib.sql")
    _write(
        root,
        "lib/2020/config.json",
        {
            "dataset": {"name": "lib", "version": "2020"},
            "execution_details": {"timestamp": "2020-01-02T03:04:05-05:00"},
        },
    )
    _write(
        root, "lib/latest/config.json", {"dataset": {"name": "lib", "version": "2020"}}
    )
    # data without a config.json: not a version
    _write(root, "partial/v1/partial.parquet")
    conn = IngestDatastore(
        conn_type=STORE,
        storage=PathedStorageConnector.from_storage_kwargs(
            conn_type=STORE, storage_backend=StorageType.LOCAL, local_dir=root
        ),
    )
    return conn, root


def test_group_listing():
    grouped = iv.group_listing(
        [("a/v1/a.parquet", None), ("a/v1/sub/x.csv", None), ("stray.txt", None)]
    )
    assert grouped == {"a": {"v1": {"a.parquet": None, "sub/x.csv": None}}}


def test_file_types_match_recipes():
    assert iv.file_types(["a.sql", "a.parquet", "config.json", "a.gdb.zip"]) == [
        "parquet",
        "pg_dump",
    ]


def test_archived_at_order():
    when = datetime.fromisoformat("2021-01-01T00:00:00+00:00")
    assert iv.archived_at(_ingest_config("a", "v1"), lambda: None, when) == (
        datetime.fromisoformat(INGEST_TS)
    )
    assert iv.archived_at(
        {"dataset": {}}, lambda: "2020-05-05T00:00:00+00:00", when
    ) == datetime.fromisoformat("2020-05-05T00:00:00+00:00")
    assert iv.archived_at(None, lambda: None, when) == when


def test_sweep(store):
    conn, root = store
    rows, stats = iv.sweep(conn, STORE)
    by_id = {(r.key, r.version): r for r in rows}
    assert set(by_id) == {("a", "v1"), ("a", "v2"), ("lib", "2020")}
    assert stats.versions == 3 and stats.configs_read == 3 and not stats.errors

    v2 = by_id[("a", "v2")]
    assert v2.is_latest and not by_id[("a", "v1")].is_latest
    assert v2.file_types == ["csv", "parquet"]
    assert v2.archived_at == datetime.fromisoformat(INGEST_TS)
    # the offset as written, so the archive date matches the bucket's
    assert v2.details[iv.ARCHIVED_AT_ISO] == INGEST_TS
    assert v2.details[iv.NAME] == "a name"
    assert v2.details[iv.URL] == "https://example.com/a"
    assert v2.location == f"{root}/a/v2/"
    assert all(r.domain == "ingest" and r.store == STORE for r in rows)

    lib = by_id[("lib", "2020")]
    assert lib.is_latest
    assert lib.file_types == ["pg_dump"]
    assert lib.archived_at == datetime.fromisoformat("2020-01-02T03:04:05-05:00")
    assert lib.details[iv.NAME] is None and lib.details[iv.URL] is None


def test_sweep_reuses_unchanged_configs(store, monkeypatch):
    conn, root = store
    first, _ = iv.sweep(conn, STORE)
    rows, stats = iv.sweep(conn, STORE, previous=first)
    assert stats.configs_read == 0 and stats.latest_read == 0
    assert [(r.key, r.version, r.is_latest, r.details) for r in rows] == [
        (r.key, r.version, r.is_latest, r.details) for r in first
    ]

    # a new version, latest moved to it
    _write(root, "a/v3/a.parquet")
    _write(root, "a/v3/config.json", _ingest_config("a", "v3"))
    _write(root, "a/latest/config.json", _ingest_config("a", "v3"))
    rows, stats = iv.sweep(conn, STORE, previous=first)
    assert stats.configs_read == 1 and stats.latest_read == 1
    assert {r.version for r in rows if r.key == "a" and r.is_latest} == {"v3"}


def test_sweep_records_unreadable_config(store):
    conn, root = store
    _write(root, "bad/v1/config.json", b"{not json")
    rows, stats = iv.sweep(conn, STORE)
    bad = next(r for r in rows if r.key == "bad")
    assert iv.CONFIG_ERROR in bad.details
    assert stats.errors
    # retried next time, not reused
    _, stats = iv.sweep(conn, STORE, previous=rows)
    assert stats.configs_read == 1


def test_not_configured(monkeypatch):
    monkeypatch.delenv(catalog.CATALOG_PG_URL_ENV_VAR, raising=False)
    assert not catalog.is_configured()
    with pytest.raises(catalog.CatalogNotConfigured):
        catalog.query("ingest")
    monkeypatch.setenv(catalog.CATALOG_PG_URL_ENV_VAR, " ")
    assert not catalog.is_configured()


def test_migration_files_are_numbered():
    names = [p.name for p in catalog.migration_files()]
    assert names[0] == "0001_create_versions.sql"
    assert len({n.split("_", 1)[0] for n in names}) == len(names)
