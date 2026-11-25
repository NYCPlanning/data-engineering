"""Run from the repo root: pytest apps/dagster/tests"""

import sys
from pathlib import Path

import pytest
from dagster import materialize

from dcpy.connectors.edm import catalog
from dcpy.connectors.edm.catalog import WriteCounts, ingest_versions

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingest import catalog as catalog_asset  # noqa: E402


def test_stores_default_to_ingest_connector(monkeypatch):
    monkeypatch.delenv("DCPY_INGEST_DATASETS_CONNECTOR", raising=False)
    assert catalog_asset.catalog_stores()[0] == "edm.recipes.datasets"


def test_version_catalog(monkeypatch):
    monkeypatch.setenv("DCPY_CATALOG_PG_URL", "postgresql://catalog")
    monkeypatch.setattr(
        catalog_asset, "catalog_stores", lambda: ["edm.recipes.datasets"]
    )
    monkeypatch.setattr(catalog, "migrate", lambda: ["0001_create_versions.sql"])
    refreshed = []

    def refresh(datastore, store):
        refreshed.append(store)
        return ingest_versions.RefreshResult(
            store=store,
            counts=WriteCounts(added=3, updated=1, removed=2),
            stats=ingest_versions.SweepStats(versions=4, configs_read=4),
            seconds=0.5,
        )

    monkeypatch.setattr(ingest_versions, "refresh_store", refresh)
    result = materialize([catalog_asset.version_catalog])
    assert result.success and refreshed == ["edm.recipes.datasets"]
    [mat] = result.asset_materializations_for_node("version_catalog")
    assert mat.metadata["edm.recipes.datasets"].value["added"] == 3
    assert mat.metadata["edm.recipes.datasets"].value["removed"] == 2


def test_version_catalog_needs_the_catalog(monkeypatch):
    monkeypatch.delenv("DCPY_CATALOG_PG_URL", raising=False)
    with pytest.raises(Exception, match="DCPY_CATALOG_PG_URL"):
        materialize([catalog_asset.version_catalog])


def test_schedule():
    schedule = catalog_asset.version_catalog_daily
    assert schedule.cron_schedule == "0 6 * * *"
    assert schedule.execution_timezone == "America/New_York"
