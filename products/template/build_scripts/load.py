from pathlib import Path
from dcpy.connectors.edm import recipes

from . import RECIPE_PATH, RECIPE_LOCK_PATH, PG_CLIENT


def setup_build_db():
    PG_CLIENT.drop_schema()
    PG_CLIENT.create_schema()


def load_source_data():
    recipes.plan(
        Path(RECIPE_PATH),
        Path(RECIPE_LOCK_PATH),
    )
    recipe = recipes.recipe_from_yaml(Path(RECIPE_LOCK_PATH))
    recipes.write_source_data_versions(recipe_file=Path(RECIPE_LOCK_PATH))

    [recipes.import_dataset(dataset, PG_CLIENT) for dataset in recipe.inputs.datasets]

    PG_CLIENT.create_table_from_csv(
        "source_data_versions",
        RECIPE_LOCK_PATH.parent / "source_data_versions.csv",
    )


if __name__ == "__main__":
    setup_build_db()
    load_source_data()
