from pathlib import Path
import typer

from dcpy import REPO_ROOT_PATH
from dcpy.utils import git, postgres
from dcpy.utils.logging import logger
from dcpy.connectors.edm import recipes


def setup_environment(pg_client: postgres.PostgresClient):
    pg_client.drop_schema()
    pg_client.create_schema()
    # TODO delete draft S3 folder


def load_source_data(product: str):
    product_path = REPO_ROOT_PATH / f"products/{product}"
    build_name = git.run_name()
    pg_client = postgres.PostgresClient(
        schema=build_name,
        database=f"db-{product}",
    )

    recipe_path = product_path / "recipe.yml"
    recipe_lock_path = recipe_path.parent / "recipe.lock.yml"

    recipes.plan(
        recipe_path,
        recipe_lock_path,
    )
    recipe = recipes.recipe_from_yaml(Path(recipe_lock_path))
    recipes.write_source_data_versions(recipe_file=Path(recipe_lock_path))

    setup_environment(pg_client)

    [recipes.import_dataset(dataset, pg_client) for dataset in recipe.inputs.datasets]

    pg_client.create_table_from_csv(
        "source_data_versions",
        recipe_lock_path.parent / "source_data_versions.csv",
    )


app = typer.Typer(add_completion=False)


@app.command("load")
def _cli_wrapper_load(
    product: str = typer.Option(
        None,
        "-p",
        "--product",
        help="Name of data product (products subfolder in repo)",
    ),
):
    logger.info(f"Loading {product} source data")
    load_source_data(product)


if __name__ == "__main__":
    app()
