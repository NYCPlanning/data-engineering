from pathlib import Path
from dcpy.utils import postgres
from dcpy.connectors.edm import recipes


def load_source_data(
    pg_client: postgres.PostgresClient,
    recipe_path: Path,
):
    recipe_lock_path = recipe_path.parent / "recipe.lock.yml"
    recipes.plan(
        recipe_path,
        recipe_lock_path,
    )
    recipe = recipes.recipe_from_yaml(Path(recipe_lock_path))
    recipes.write_source_data_versions(recipe_file=Path(recipe_lock_path))

    [recipes.import_dataset(dataset, pg_client) for dataset in recipe.inputs.datasets]

    pg_client.create_table_from_csv(
        "source_data_versions",
        recipe_lock_path.parent / "source_data_versions.csv",
    )
