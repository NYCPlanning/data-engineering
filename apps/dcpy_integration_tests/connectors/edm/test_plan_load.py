import shutil
from pathlib import Path

import pandas as pd
import pytest

from dcpy.lifecycle.builds import load, plan
from dcpy.lifecycle.builds.models import InputDatasetDestination
from dcpy.utils import postgres

RESOURCES = Path(__file__).parent / "resources"
RECIPE_PATH = RESOURCES / "recipe.yml"
RECIPE_WITH_JSON_PATH = RESOURCES / "recipe_with_json_input.yml"

assert RESOURCES.exists()


def test_resolving_and_loading_recipes(tmp_path, pg_client: postgres.PostgresClient):
    temp_dir = Path(tmp_path)
    temp_recipe_path = temp_dir / "recipe.yml"

    shutil.copyfile(RECIPE_PATH, temp_recipe_path)
    lock_path = plan.plan(temp_recipe_path)
    recipe = plan.recipe_from_yaml(lock_path)
    load_result = load.load_source_data_from_resolved_recipe(lock_path)

    recipe_pg_dataset_table_names = {
        ds.import_as or ds.id
        for ds in recipe.inputs.datasets
        if ds.destination == InputDatasetDestination.postgres
    }
    # Assuming a single version of each
    loaded_datasets = [
        load_result.get_latest_version(ds_name)
        for ds_name in load_result.datasets.keys()
    ]
    load_result_pg_table_names = {
        str(ds.destination).split(".")[-1]
        for ds in loaded_datasets
        if ds.destination_type == InputDatasetDestination.postgres
    }

    source_data_versions = pd.read_csv(temp_dir / "source_data_versions.csv", dtype=str)
    assert source_data_versions.columns.to_list() == [
        "schema_name",
        "dataset_name",
        "v",
        "file_type",
        "archive_date",
        "url",
    ]

    assert recipe_pg_dataset_table_names == load_result_pg_table_names

    schema_tables = pg_client.get_schema_tables()
    assert recipe_pg_dataset_table_names.issubset(set(schema_tables)), (
        "The PG sources from the recipe should have been imported"
    )

    assert 1 == pg_client.get_table_row_count(table_name="test_bpl_libraries"), (
        "`test_bpl_libraries` should have been truncated to a single row by the preprocessor"
    )


def test_loading_json(tmp_path, pg_client: postgres.PostgresClient, clean_build_engine):
    """Loads a JSON dataset from the test.edm.publishing.

    The dataset has a list of records with keys = {'a', 'b'}. This tests that the JSON has
    been preprocessed (upcasing the keys) and inserted into PG correctly.
    """

    temp_dir = Path(tmp_path)
    temp_recipe_path = temp_dir / "recipe.yml"

    shutil.copyfile(RECIPE_WITH_JSON_PATH, temp_recipe_path)
    lock_path = plan.plan(temp_recipe_path)
    recipe = plan.recipe_from_yaml(lock_path)
    load.load_source_data_from_resolved_recipe(lock_path)

    table_name = recipe.inputs.datasets[0].import_as
    assert table_name in pg_client.get_schema_tables(), (
        "The table should hav been created."
    )

    table_data = pg_client.read_table_df(table_name)

    # TODO: This should really use the raw pulled JSON file. Blocked from doing that atm
    # by the bug around duplicated recipe dataset ids.
    assert {"A", "B"}.issubset(table_data.to_dict()), (
        "The column names should have been upcased by the preprocessor."
    )


def test_get_source_data_versions_integration(pg_client: postgres.PostgresClient):
    """Test get_source_data_versions with real published connector.

    This test requires:
    - Real published connector configured to test storage
    - A published version with source_data_versions.csv
    """
    from dcpy.connectors.edm.models import PublishKey
    from dcpy.lifecycle.builds import utils
    from dcpy.lifecycle.builds.connector import get_published_default_connector

    # Find a product with source_data_versions.csv
    connector = get_published_default_connector()
    product = "db-pluto"  # Use a product we know exists

    versions_list = connector.list_versions(product, exclude_latest=True)
    if not versions_list:
        pytest.skip("Need published versions for this test")

    product_key = PublishKey(product, versions_list[0])

    # Check if file exists (using new interface method)
    if not connector.resource_exists(
        product, versions_list[0], "source_data_versions.csv"
    ):
        pytest.skip(f"Version {versions_list[0]} doesn't have source_data_versions.csv")

    # Test: Read and transform CSV
    result = utils.get_source_data_versions(product_key)

    # Verify transformations
    assert isinstance(result, pd.DataFrame)
    assert "datalibrary_name" in result.columns, "Column should be renamed"
    assert "version" in result.columns, "Column should be renamed"
    assert "schema_name" not in result.columns, "Old column name should be gone"
    assert result.index.name == "datalibrary_name", "Should be indexed"
    assert len(result) > 0, "Should have some source data"


def test_get_file_contents_integration(pg_client: postgres.PostgresClient):
    """Test get_file_contents with real published connector."""
    import json

    from dcpy.connectors.edm.models import PublishKey
    from dcpy.lifecycle.builds import utils
    from dcpy.lifecycle.builds.connector import get_published_default_connector

    connector = get_published_default_connector()
    product = "db-pluto"

    versions_list = connector.list_versions(product, exclude_latest=True)
    if not versions_list:
        pytest.skip("Need published versions for this test")

    product_key = PublishKey(product, versions_list[0])

    # Check if build_metadata.json exists
    if not connector.resource_exists(product, versions_list[0], "build_metadata.json"):
        pytest.skip(f"Version {versions_list[0]} doesn't have build_metadata.json")

    # Test: Get file contents
    contents = utils.get_file_contents(product_key, "build_metadata.json")

    assert isinstance(contents, bytes)
    assert len(contents) > 0

    # Verify it's valid JSON
    data = json.loads(contents)
    assert "recipe" in data, "build_metadata.json should have recipe key"


def test_resource_exists_connector_method(pg_client: postgres.PostgresClient):
    """Test the new resource_exists() method on VersionedConnector."""
    from dcpy.lifecycle.builds.connector import get_published_default_connector

    connector = get_published_default_connector()
    product = "db-pluto"

    versions_list = connector.list_versions(product, exclude_latest=True)
    if not versions_list:
        pytest.skip("Need published versions for this test")

    version = versions_list[0]

    # Test: File that should exist
    assert connector.resource_exists(product, version, "source_data_versions.csv"), (
        "source_data_versions.csv should exist in most published versions"
    )

    # Test: File that shouldn't exist
    assert not connector.resource_exists(
        product, version, "nonexistent_file_xyz.txt"
    ), "Random file should not exist"
