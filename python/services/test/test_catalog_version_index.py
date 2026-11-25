"""Planning with the version catalog resolves inputs the same as planning from the bucket.

The catalog's rows come from a real sweep of the (moto) bucket; only the database is faked.
"""

import json
from unittest import mock

import pytest

from dcpy.connectors.edm import recipes
from dcpy.lifecycle import connector_registry
from dcpy.lifecycle.builds import plan
from dcpy.lifecycle.builds.version_index import InputRequest
from dcpy.services import catalog
from dcpy.services.catalog import db, ingest_versions
from dcpy.services.catalog.version_index import CatalogVersionIndex
from dcpy.utils import s3

from .conftest import RECIPES_BUCKET

STORE = "edm.recipes.datasets"

RECIPE = """
name: Tester
version: 23v1
product: db-test
env:
  VERSION_PREV: 22v1
inputs:
  missing_versions_strategy: find_latest
  datasets:
  - name: ingested
  - name: ingested
    version: v1
    import_as: ingested_v1
  - name: library_era
    version: "2020"
  - name: not_in_catalog
  - name: ingested_csv
    file_type: csv
"""


def _put(key: str, body: bytes | dict = b"", metadata: dict | None = None):
    s3.client().put_object(
        Bucket=RECIPES_BUCKET,
        Key=f"datasets/{key}",
        Body=json.dumps(body).encode() if isinstance(body, dict) else body,
        Metadata=metadata or {},
    )


def _ingest_config(key, version, ts):
    return {
        "id": key,
        "version": version,
        "attributes": {"name": f"{key} name", "url": f"https://example.com/{key}"},
        "transformation": {"run_details": {"timestamp": ts}},
    }


def _archive(key, version, ts, *, latest=False, extra_files=()):
    config = _ingest_config(key, version, ts)
    for folder in [version, "latest"] if latest else [version]:
        _put(f"{key}/{folder}/{key}.parquet")
        for f in extra_files:
            _put(f"{key}/{folder}/{f}")
        _put(f"{key}/{folder}/config.json", config)


@pytest.fixture
def archives(create_buckets, monkeypatch):
    monkeypatch.delenv("RECIPES_BUCKET_ROOT_FOLDER", raising=False)
    # late in the evening, New York time: the next day in UTC
    _archive("ingested", "v1", "2026-10-01T23:30:00-04:00")
    _archive("ingested", "v2", "2026-11-01T23:30:00-04:00", latest=True)
    _archive(
        "ingested_csv",
        "v1",
        "2026-11-02T10:00:00-05:00",
        latest=True,
        extra_files=["ingested_csv.csv"],
    )
    _archive("not_in_catalog", "v3", "2026-11-03T10:00:00-05:00", latest=True)
    # library-era: a pg_dump and a csv, no name or url in its config
    for folder in ["2020", "latest"]:
        _put(f"library_era/{folder}/library_era.sql")
        _put(f"library_era/{folder}/library_era.csv")
        _put(
            f"library_era/{folder}/config.json",
            {
                "dataset": {"name": "library_era", "version": "2020"},
                "execution_details": {"timestamp": "2020-01-02T03:04:05-05:00"},
            },
        )

    connector_registry._set_default_connectors()
    yield
    connector_registry._set_default_connectors()


@pytest.fixture
def recipe_path(tmp_path):
    path = tmp_path / "recipe.yml"
    path.write_text(RECIPE)
    return path


def _catalog_rows():
    datastore = connector_registry.connectors[STORE]
    rows, stats = ingest_versions.sweep(datastore, STORE)
    assert not stats.errors
    return [r for r in rows if r.key != "not_in_catalog"]


def _lock(recipe_path, tmp_path, name, index=None):
    planned = plan.plan_recipe(recipe_path, version_index=index)
    return plan.generate_lock_file(recipe_path, planned, tmp_path / name).read_text()


@pytest.mark.usefixtures("archives")
def test_catalog_plan_matches_bucket_plan(recipe_path, tmp_path, monkeypatch):
    monkeypatch.delenv(catalog.CATALOG_PG_URL_ENV_VAR, raising=False)
    from_bucket = _lock(recipe_path, tmp_path, "bucket.lock.yml")

    rows = _catalog_rows()
    queries = []

    def query(domain, *, store=None, keys=None, **kwargs):
        queries.append((domain, store, sorted(keys)))
        return rows

    monkeypatch.setenv(catalog.CATALOG_PG_URL_ENV_VAR, "postgresql://catalog")
    monkeypatch.setattr(db, "query", query)
    with (
        mock.patch.object(
            recipes, "get_file_types", wraps=recipes.get_file_types
        ) as ft,
        mock.patch.object(
            recipes, "get_archive_date", wraps=recipes.get_archive_date
        ) as ad,
    ):
        from_catalog = _lock(
            recipe_path, tmp_path, "catalog.lock.yml", CatalogVersionIndex()
        )

    assert from_catalog == from_bucket
    # one query for every input
    assert queries == [
        ("ingest", STORE, ["ingested", "ingested_csv", "library_era", "not_in_catalog"])
    ]
    # only the dataset the catalog doesn't have went to the bucket
    assert {c.args[0].id for c in ft.call_args_list} == {"not_in_catalog"}
    assert {c.args[0].id for c in ad.call_args_list} == {"not_in_catalog"}

    # what the lock says, to be sure the comparison means something
    planned = plan.recipe_from_yaml(tmp_path / "catalog.lock.yml")
    by_name = {(d.id, d.import_as): d for d in planned.inputs.datasets}
    latest = by_name[("ingested", None)]
    assert (latest.version, latest.file_type, str(latest.archive_date)) == (
        "v2",
        "parquet",
        "2026-11-01",
    )
    assert latest.name == "ingested name"
    assert by_name[("library_era", None)].file_type == "pg_dump"
    assert by_name[("ingested_csv", None)].file_type == "csv"


@pytest.mark.usefixtures("archives")
def test_catalog_error_falls_back_to_bucket(recipe_path, tmp_path, monkeypatch):
    monkeypatch.delenv(catalog.CATALOG_PG_URL_ENV_VAR, raising=False)
    from_bucket = _lock(recipe_path, tmp_path, "bucket.lock.yml")

    def down(*args, **kwargs):
        raise OSError("connection refused")

    monkeypatch.setenv(catalog.CATALOG_PG_URL_ENV_VAR, "postgresql://catalog")
    monkeypatch.setattr(db, "query", down)
    index = CatalogVersionIndex()
    assert _lock(recipe_path, tmp_path, "catalog.lock.yml", index) == from_bucket


@pytest.mark.usefixtures("archives")
def test_catalog_off_reads_nothing(monkeypatch):
    monkeypatch.delenv(catalog.CATALOG_PG_URL_ENV_VAR, raising=False)
    monkeypatch.setattr(db, "query", mock.Mock(side_effect=AssertionError))
    inputs = [InputRequest("ingested", "latest")]
    assert CatalogVersionIndex().resolve(STORE, inputs, []) == [None]


@pytest.mark.usefixtures("archives")
def test_rows_for_another_root_folder_are_ignored(recipe_path, tmp_path, monkeypatch):
    """A catalog swept under a different RECIPES_BUCKET_ROOT_FOLDER doesn't answer."""
    rows = _catalog_rows()
    monkeypatch.setenv("RECIPES_BUCKET_ROOT_FOLDER", "ingest")
    inputs = [InputRequest("ingested", "latest"), InputRequest("ingested", "v2")]
    assert CatalogVersionIndex.from_rows(
        rows, inputs, plan.RECIPE_FILE_TYPE_PREFERENCE
    ) == [None, None]
