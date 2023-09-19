import os
from pathlib import Path
from dcpy.utils import git, postgres
from dcpy.connectors.edm import recipes

from . import RECIPE_PATH, RECIPE_LOCK_PATH

build_schema = git.run_name()
build_engine_server = os.environ["BUILD_ENGINE_SERVER"]
build_engine_db = os.environ["BUILD_ENGINE_DB"]

pg_client = postgres.PostgresClient(
    server_url=build_engine_server,
    database=build_engine_db,
    schema=build_schema,
)


def setup_build_db():
    pg_client.drop_schema(build_schema)
    pg_client.create_schema(build_schema)


def load_source_data():
    recipes.plan(
        Path(RECIPE_PATH),
        Path(RECIPE_LOCK_PATH),
    )
    recipes.import_datasets(Path(RECIPE_LOCK_PATH))


if __name__ == "__main__":
    setup_build_db()
    load_source_data()
