import shutil
from pathlib import Path

import pandas as pd

from dcpy.lifecycle.builds import load, plan
from dcpy.models.lifecycle.builds import InputDatasetDestination
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
