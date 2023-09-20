import os
from pathlib import Path
from dcpy.utils import git, postgres
from dcpy.connectors.edm import recipes

from . import RECIPE_PATH, RECIPE_LOCK_PATH

BUILD_SCHEMA = git.run_name()

pg_client = postgres.PostgresClient(
    schema=BUILD_SCHEMA,
)


def setup_build_db():
    pg_client.drop_schema(BUILD_SCHEMA)
    pg_client.create_schema(BUILD_SCHEMA)


def load_source_data():
    recipes.plan(
        Path(RECIPE_PATH),
        Path(RECIPE_LOCK_PATH),
    )
    recipe = recipes.recipe_from_yaml(Path(RECIPE_LOCK_PATH))
    [recipes.import_dataset(ds, pg_client) for ds in recipe.inputs.datasets]


if __name__ == "__main__":
    setup_build_db()
    load_source_data()
