from pathlib import Path

from dcpy.utils import postgres
import shutil

from dcpy.lifecycle.builds import plan, load
from dcpy.models.lifecycle.builds import InputDatasetDestination

RESOURCES = Path(__file__).parent / "resources"
RECIPE_PATH = RESOURCES / "recipe.yml"

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
    load_result_pg_table_names = {
        str(ds.destination).split(".")[-1]
        for ds in load_result.datasets.values()
        if ds.destination_type == InputDatasetDestination.postgres
    }

    assert recipe_pg_dataset_table_names == load_result_pg_table_names

    schema_tables = pg_client.get_schema_tables()
    assert recipe_pg_dataset_table_names.issubset(set(schema_tables)), (
        "The PG sources from the recipe should have been imported"
    )

    assert 1 == pg_client.get_table_row_count(table_name="test_bpl_libraries"), (
        "`test_bpl_libraries` should have been truncated to a single row by the preprocessor"
    )
