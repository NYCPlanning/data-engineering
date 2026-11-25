"""Run from the repo root: pytest apps/dagster/tests"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from dagster import DagsterInstance, SensorResult, build_sensor_context

from dcpy.connectors.hybrid_pathed_storage import PathedStorageConnector, StorageType
from dcpy.connectors.ingest_datastore import Connector as IngestDatastoreConnector
from dcpy.lifecycle.ingest.version_schedules import VersionSchedule

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingest import sensors  # noqa: E402
from ingest.assets import TemplateInfo, ingest_asset_key  # noqa: E402
from ingest.partitions import dynamic_partitions_def_name  # noqa: E402

WHEN = datetime(2026, 11, 1, tzinfo=timezone.utc)
DYNAMIC = TemplateInfo("bpl_libraries", [], None, ("bpl_libraries",))
QUARTERLY = TemplateInfo(
    "dcp_ct2020", [], VersionSchedule.geosupport_quarterly, ("dcp_ct2020",)
)
SOURCE = TemplateInfo("gtfs", [], None, ("gtfs_stops", "gtfs_routes"))
TEMPLATES = {t.id: t for t in [DYNAMIC, QUARTERLY, SOURCE]}


@pytest.fixture
def instance():
    with DagsterInstance.ephemeral() as instance:
        yield instance


def _apply(instance: DagsterInstance, result: SensorResult) -> None:
    """What the sensor daemon does with a result."""
    for event in result.asset_events:
        instance.report_runless_asset_event(event)
    for request in result.dynamic_partitions_requests or []:
        instance.add_dynamic_partitions(
            request.partitions_def_name, list(request.partition_keys)
        )


def _sync(instance, archived) -> SensorResult:
    """`archived` as {template id: {version: archive date}}."""
    context = build_sensor_context(instance=instance)
    return sensors.sync_archives(
        context,
        list(TEMPLATES.values()),
        {
            t: {
                v: sensors.ArchivedVersion(f"s3://b/datasets/{t}/{v}", when)
                for v, when in versions.items()
            }
            for t, versions in archived.items()
        },
    )


def _reported(result: SensorResult) -> set[tuple[str, str]]:
    return {(e.asset_key.path[-1], e.partition) for e in result.asset_events}


def test_reports_new_versions(instance):
    archived = {
        "bpl_libraries": {"20260101": WHEN, "20260201": None},
        "dcp_ct2020": {"26c": WHEN, "26C": WHEN, "20261124": WHEN, "99a": WHEN},
        "gtfs": {"20260301": WHEN},
    }
    result = _sync(instance, archived)

    assert _reported(result) == {
        ("ingest_bpl_libraries", "20260101"),
        ("ingest_bpl_libraries", "20260201"),
        ("ingest_dcp_ct2020", "26c"),  # 26C, 20261124 and 99a aren't quarterly keys
        ("ingest_gtfs", "20260301"),
    }
    requests = {
        r.partitions_def_name: sorted(r.partition_keys)
        for r in result.dynamic_partitions_requests
    }
    assert requests == {
        dynamic_partitions_def_name("bpl_libraries"): ["20260101", "20260201"],
        dynamic_partitions_def_name("gtfs"): ["20260301"],
    }
    ct = next(e for e in result.asset_events if e.partition == "26c")
    assert ct.metadata["source"].value == "archive_sync"
    assert ct.metadata["archive_path"].value == "s3://b/datasets/dcp_ct2020/26c"
    assert ct.metadata["archived_at"].value == WHEN.isoformat()


def test_does_not_rereport(instance):
    archived = {"bpl_libraries": {"20260101": WHEN}, "dcp_ct2020": {"26c": WHEN}}
    _apply(instance, _sync(instance, archived))
    assert instance.get_materialized_partitions(ingest_asset_key("dcp_ct2020")) == {
        "26c"
    }

    again = _sync(instance, archived)
    assert not again.asset_events
    assert not again.dynamic_partitions_requests


def test_adds_key_for_a_version_materialized_under_the_old_def(instance):
    """A run recorded under the old shared def has a materialization but no key in the
    template's own def: the key is added, the materialization isn't repeated."""
    archived = {"bpl_libraries": {"20260101": WHEN}}
    first = _sync(instance, archived)
    for event in first.asset_events:
        instance.report_runless_asset_event(event)  # no partition requests applied

    again = _sync(instance, archived)
    assert not again.asset_events
    assert [list(r.partition_keys) for r in again.dynamic_partitions_requests] == [
        ["20260101"]
    ]


@pytest.mark.parametrize(
    ("cursor", "batch", "next_index"),
    [
        (None, ["a", "b"], 2),
        ("2", ["c", "d"], 4),
        ("4", ["e"], 0),  # last, short batch wraps the cursor
        ("9", ["a", "b"], 2),  # past the end (templates removed): start over
        ("junk", ["a", "b"], 2),
    ],
)
def test_next_batch(cursor, batch, next_index):
    assert sensors.next_batch(list("abcde"), cursor, 2) == (batch, next_index)


def test_sensor_round_robin(instance, tmp_path, monkeypatch):
    for dataset_id, version in [
        ("bpl_libraries", "20260101"),
        ("dcp_ct2020", "26c"),
        ("gtfs_routes", "20260301"),
        ("gtfs_stops", "20260301"),
        ("gtfs_stops", "20260401"),
    ]:
        folder = tmp_path / dataset_id / version
        folder.mkdir(parents=True)
        (folder / "config.json").write_text("{}")
    datastore = IngestDatastoreConnector(
        storage=PathedStorageConnector.from_storage_kwargs(
            conn_type="test",
            storage_backend=StorageType.LOCAL,
            local_dir=tmp_path,
            _validate_root_path=True,
        )
    )
    monkeypatch.setattr(
        "dcpy.lifecycle.ingest.connectors.get_processed_datastore_connector",
        lambda: datastore,
    )
    monkeypatch.setattr(sensors, "templates", TEMPLATES)
    monkeypatch.setenv(sensors.BATCH_SIZE_ENV_VAR, "2")

    cursor = None
    reported = set()
    for expected_cursor in ["2", "0"]:  # sorted: bpl_libraries, dcp_ct2020 | gtfs
        context = build_sensor_context(instance=instance, cursor=cursor)
        result = sensors.ingest_archive_sync(context)
        assert result.cursor == expected_cursor
        reported |= _reported(result)
        cursor = result.cursor
    assert reported == {
        ("ingest_bpl_libraries", "20260101"),
        ("ingest_dcp_ct2020", "26c"),
        ("ingest_gtfs", "20260301"),  # once, though two of its datasets have it
        ("ingest_gtfs", "20260401"),
    }


def test_sensor_tick_fails_without_store(instance, monkeypatch):
    def broken():
        raise RuntimeError("no S3 configured")

    monkeypatch.setattr(
        "dcpy.lifecycle.ingest.connectors.get_processed_datastore_connector", broken
    )
    with pytest.raises(RuntimeError, match="no S3"):
        sensors.ingest_archive_sync(build_sensor_context(instance=instance))


def test_list_archived_versions(tmp_path, monkeypatch):
    for dataset_id, version in [("gtfs_stops", "1"), ("gtfs_routes", "2")]:
        (tmp_path / dataset_id / version).mkdir(parents=True)
        (tmp_path / dataset_id / version / "config.json").write_text("{}")
    datastore = IngestDatastoreConnector(
        storage=PathedStorageConnector.from_storage_kwargs(
            conn_type="test",
            storage_backend=StorageType.LOCAL,
            local_dir=tmp_path,
            _validate_root_path=True,
        )
    )
    monkeypatch.setattr(
        "dcpy.lifecycle.ingest.connectors.get_processed_datastore_connector",
        lambda: datastore,
    )
    found = sensors.list_archived_versions([SOURCE, DYNAMIC])
    assert {t: sorted(v) for t, v in found.items()} == {
        "gtfs": ["1", "2"],
        "bpl_libraries": [],
    }
    assert found["gtfs"]["2"].path == f"{tmp_path}/gtfs_routes/2"


@pytest.fixture(autouse=True)
def no_catalog(monkeypatch):
    monkeypatch.delenv("DCPY_CATALOG_PG_URL", raising=False)


def _catalog_rows(pairs):
    from dcpy.services.catalog import VersionRow

    return [
        VersionRow(
            domain="ingest",
            store="edm.recipes.datasets",
            key=key,
            version=version,
            location=f"s3://edm-recipes/ingest/datasets/{key}/{version}/",
            archived_at=WHEN,
        )
        for key, version in pairs
    ]


def test_list_archived_versions_from_catalog(monkeypatch):
    from dcpy.services import catalog

    queries = []

    def query(domain, *, store=None, keys=None, **kwargs):
        queries.append((domain, store, set(keys)))
        return _catalog_rows(
            [("gtfs_stops", "1"), ("gtfs_routes", "1"), ("gtfs_routes", "2")]
        )

    monkeypatch.setattr(catalog, "query", query)
    found = sensors.list_archived_versions_from_catalog([SOURCE, DYNAMIC])
    assert queries == [
        (
            "ingest",
            "edm.recipes.datasets",
            {"gtfs_stops", "gtfs_routes", "bpl_libraries"},
        )
    ]
    assert {t: sorted(v) for t, v in found.items()} == {
        "gtfs": ["1", "2"],
        "bpl_libraries": [],
    }
    assert found["gtfs"]["2"].path == "s3://edm-recipes/ingest/datasets/gtfs_routes/2"
    assert found["gtfs"]["2"].archived_at == WHEN


def test_sensor_reads_catalog_for_all_templates(instance, monkeypatch):
    from dcpy.services import catalog

    monkeypatch.setenv("DCPY_CATALOG_PG_URL", "postgresql://catalog")
    monkeypatch.setattr(sensors, "templates", TEMPLATES)
    monkeypatch.setattr(
        catalog,
        "query",
        lambda *a, **k: _catalog_rows(
            [("bpl_libraries", "20260101"), ("gtfs_stops", "3")]
        ),
    )

    def no_store(*a):
        raise AssertionError("listed the store")

    monkeypatch.setattr(sensors, "list_archived_versions", no_store)
    # a cursor left by a store-listing tick doesn't matter
    result = sensors.ingest_archive_sync(
        build_sensor_context(instance=instance, cursor="2")
    )
    assert result.cursor == "0"  # every template in one tick
    assert _reported(result) == {
        ("ingest_bpl_libraries", "20260101"),
        ("ingest_gtfs", "3"),
    }


def test_sensor_falls_back_to_store_when_catalog_fails(instance, monkeypatch):
    from dcpy.services import catalog

    monkeypatch.setenv("DCPY_CATALOG_PG_URL", "postgresql://catalog")
    monkeypatch.setenv(sensors.BATCH_SIZE_ENV_VAR, "2")
    monkeypatch.setattr(sensors, "templates", TEMPLATES)

    def down(*a, **k):
        raise OSError("connection refused")

    monkeypatch.setattr(catalog, "query", down)
    listed = []
    monkeypatch.setattr(
        sensors,
        "list_archived_versions",
        lambda ts: listed.append([t.id for t in ts]) or {},
    )
    result = sensors.ingest_archive_sync(build_sensor_context(instance=instance))
    assert listed == [["bpl_libraries", "dcp_ct2020"]]
    assert result.cursor == "2"
