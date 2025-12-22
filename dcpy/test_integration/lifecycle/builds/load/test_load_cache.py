from pathlib import Path

import pandas as pd
import pytest
import yaml

from dcpy.lifecycle.builds.load import (
    CachedEntityType,
    load_source_data_from_resolved_recipe,
)
from dcpy.utils import postgres

RECIPE_PATH = Path(__file__).parent / "resources" / "recipe.yml.lock"
RECIPE_CACHE_SCHEMA = "test_cache_schema"
BUILD_SCHEMA = "test_build_schema"
build_pg_client = postgres.PostgresClient(schema=BUILD_SCHEMA)

# Load expected data from test CSV
TEST_DATA = pd.read_csv(Path(__file__).parent / "resources" / "test_dataset.csv")


@pytest.fixture
def setup_cache_schema(pg_client, setup_mock_connector):
    """Create cache schema using target_schema parameter"""

    # Load data into the cache
    load_source_data_from_resolved_recipe(
        RECIPE_PATH,
        clear_pg_schema=True,
        target_schema=RECIPE_CACHE_SCHEMA,
        _write_metadata_file=False,
    )

    setup_mock_connector.reset_mock()
    yield

    # Cleanup cache schema
    cache_pg_client = postgres.PostgresClient(schema=RECIPE_CACHE_SCHEMA)
    cache_pg_client.drop_schema()


@pytest.mark.parametrize(
    "cached_entity_type",
    [CachedEntityType.view, CachedEntityType.copy],
)
def test_builds_load_source_data_caching(
    pg_client, setup_mock_connector, setup_cache_schema, cached_entity_type
):
    mock_connector = setup_mock_connector

    load_source_data_from_resolved_recipe(
        RECIPE_PATH,
        cache_schema=RECIPE_CACHE_SCHEMA,
        cached_entity_type=cached_entity_type,
        clear_pg_schema=True,
        target_schema=BUILD_SCHEMA,
        _write_metadata_file=False,
    )

    assert mock_connector.pull_call_count == 0, (
        "The mock connector's pull method should NOT have been called (cache hit)"
    )

    # Assert data was created in build schema
    data = build_pg_client.execute_select_query(
        'SELECT name, value FROM "test_dataset" ORDER BY name'
    )
    pd.testing.assert_frame_equal(
        data.reset_index(drop=True),
        TEST_DATA.sort_values("name").reset_index(drop=True),
    )

    expected_is_view = cached_entity_type == CachedEntityType.view
    assert build_pg_client.is_view("test_dataset") is expected_is_view, (
        "The created entity type should match expectations (table or view)"
    )


def test_builds_load_source_data_caching__cache_miss_wrong_version(
    pg_client, setup_mock_connector, setup_cache_schema, tmp_path
):
    """Test that loading proceeds as normal when the cached data version is different."""
    mock_connector = setup_mock_connector

    # Create a temporary recipe with different version (different from cache) and load it.
    with open(RECIPE_PATH, "r") as f:
        recipe_data = yaml.safe_load(f)
    recipe_data["inputs"]["datasets"][0]["version"] = "1999v1"
    temp_recipe_path = tmp_path / "temp_recipe.yml.lock"
    with open(temp_recipe_path, "w") as f:
        yaml.dump(recipe_data, f)

    load_source_data_from_resolved_recipe(
        temp_recipe_path,
        cache_schema=RECIPE_CACHE_SCHEMA,
        cached_entity_type=CachedEntityType.view,
        clear_pg_schema=True,
        target_schema=BUILD_SCHEMA,
        _write_metadata_file=False,
    )

    assert mock_connector.pull_call_count == 1, (
        "The mock connector should have been called (cache miss due to version mismatch)"
    )

    table_data = build_pg_client.execute_select_query(
        'SELECT name, value FROM "test_dataset" ORDER BY name'
    )
    pd.testing.assert_frame_equal(
        table_data.reset_index(drop=True),
        TEST_DATA.sort_values("name").reset_index(drop=True),
    )
    assert build_pg_client.is_view("test_dataset") is False


@pytest.mark.parametrize(
    "cached_entity_type",
    [CachedEntityType.view, CachedEntityType.copy],
)
def test_builds_load_source_data_caching__different_import_as_name(
    pg_client, setup_mock_connector, setup_cache_schema, tmp_path, cached_entity_type
):
    """Test that loading from cache works when the import_as name is different."""
    target_entity_name = "renamed_dataset"
    # Rename the cache dataset to match import_as
    cache_pg_client = postgres.PostgresClient(schema=RECIPE_CACHE_SCHEMA)
    cache_pg_client.rename_table(old_name="test_dataset", new_name=target_entity_name)

    # Create a temporary recipe with an overridden import_as name
    with open(RECIPE_PATH, "r") as f:
        recipe_data = yaml.safe_load(f)
    recipe_data["inputs"]["datasets"][0]["import_as"] = target_entity_name
    temp_recipe_path = tmp_path / "temp_recipe.yml.lock"
    with open(temp_recipe_path, "w") as f:
        yaml.dump(recipe_data, f)

    load_source_data_from_resolved_recipe(
        temp_recipe_path,
        cache_schema=RECIPE_CACHE_SCHEMA,
        cached_entity_type=cached_entity_type,
        clear_pg_schema=True,
        target_schema=BUILD_SCHEMA,
        _write_metadata_file=False,
    )

    expected_is_view = cached_entity_type == CachedEntityType.view
    assert build_pg_client.is_view(target_entity_name) is expected_is_view, (
        "The created entity type should match expectations (table or view)"
    )
    assert not build_pg_client.table_or_view_exists("test_dataset"), (
        "The original dataset name should not exist in the build schema"
    )
