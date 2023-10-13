from pathlib import Path
import typer

from dcpy import REPO_ROOT_PATH
from dcpy.utils import postgres, s3
from dcpy.utils.logging import logger
from dcpy.connectors.edm import build_metadata, recipes, publishing


def setup_build_environments(pg_client: postgres.PostgresClient):
    pg_client.drop_schema()
    pg_client.create_schema()
    s3.delete(
        bucket=publishing.BUCKET,
        path=f"{pg_client.database}/draft/{pg_client.schema}",
    )


def load_source_data(recipe_path: Path):
    recipe_lock_path = recipes.plan(recipe_path)
    recipes.write_source_data_versions(recipe_file=Path(recipe_lock_path))
    recipe = recipes.recipe_from_yaml(Path(recipe_lock_path))

    build_name = build_metadata.build_name()
    logger.info(f"Loading source data for {recipe.name} build named {build_name}")

    pg_client = postgres.PostgresClient(schema=build_name)
    setup_build_environments(pg_client)

    pg_client.create_table_from_csv(
        "source_data_versions",
        recipe_lock_path.parent / "source_data_versions.csv",
    )

    [recipes.import_dataset(dataset, pg_client) for dataset in recipe.inputs.datasets]

    recipes.purge_recipe_cache()


app = typer.Typer(add_completion=False)


@app.command("load")
def _cli_wrapper_load(
    recipe_path: Path = typer.Option(
        "./recipe.yml",
        "-r",
        "--recipe_path",
        help="Path of recipe file to use",
    ),
):
    load_source_data(recipe_path)


if __name__ == "__main__":
    app()
