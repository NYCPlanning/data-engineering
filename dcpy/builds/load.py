import importlib
from pathlib import Path
import typer

from dcpy.utils import postgres, s3
from dcpy.utils.logging import logger
from dcpy.connectors.edm import recipes, publishing
from dcpy.builds import metadata, plan


def setup_build_environments(pg_client: postgres.PostgresClient):
    if pg_client.schema != "public":
        pg_client.drop_schema()
        pg_client.create_schema()
    s3.delete(
        bucket=publishing.BUCKET,
        path=f"{pg_client.database}/draft/{pg_client.schema}",
    )


def import_dataset(
    ds: plan.InputDataset,
    pg_client: postgres.PostgresClient,
):
    """Import a recipe to local data library folder and build engine."""
    ds_table_name = ds.import_as or ds.name
    logger.info(
        f"Importing {ds.name} into {pg_client.database}.{pg_client.schema}.{ds_table_name}"
    )

    if ds.version == "latest" or ds.version is None:
        raise Exception(f"Cannot import a dataset without a resolved version: {ds}")
    if ds.preprocessor is not None:
        preproc_mod = importlib.import_module(ds.preprocessor.module)
        preproc_func = getattr(preproc_mod, ds.preprocessor.function)
        if ds.file_type == recipes.DatasetType.pg_dump:
            logger.warning(
                f"Preprocessor {ds.preprocessor.module} cannot be applied to pg_dump dataset {ds.name}."
            )
    else:
        preproc_func = None

    recipes.import_dataset(
        ds.dataset, pg_client, preprocessor=preproc_func, import_as=ds.import_as
    )


def load_source_data(
    recipe_path: Path, version: str | None = None, repeat: bool = False
):
    recipe_lock_path = plan.plan(recipe_path, version, repeat)
    plan.write_source_data_versions(recipe_file=Path(recipe_lock_path))
    recipe = plan.recipe_from_yaml(Path(recipe_lock_path))

    plan.write_source_data_versions(recipe_file=Path(recipe_lock_path))
    metadata.write_build_metadata(recipe, recipe_path.parent)

    build_name = metadata.build_name()
    logger.info(f"Loading source data for {recipe.name} build named {build_name}")

    pg_client = postgres.PostgresClient(schema=build_name)
    setup_build_environments(pg_client)

    pg_client.create_table_from_csv(
        recipe_lock_path.parent / "source_data_versions.csv"
    )

    [import_dataset(dataset, pg_client) for dataset in recipe.inputs.datasets]

    recipes.purge_recipe_cache()


app = typer.Typer(add_completion=False)


@app.command("recipe")
def _cli_wrapper_load(
    recipe_path: Path = typer.Option(
        Path(plan.DEFAULT_RECIPE),
        "--recipe-path",
        "-r",
        help="Path of recipe file to use",
    ),
    version=typer.Option(
        None,
        "--version",
        "-v",
        help="Version of dataset being built",
    ),
    repeat: bool = typer.Option(
        False, "--repeat", help="Repeat specific published build"
    ),
):
    load_source_data(recipe_path, version, repeat)


@app.command("dataset")
def _import_dataset(
    dataset_name: str = typer.Option(
        None,
        "-n",
        "--dataset_name",
        help="Name of the dataset",
    ),
    version: str = typer.Option(
        None,
        "-v",
        "--version",
        help="Dataset version to import",
    ),
    dataset_type: recipes.DatasetType = typer.Option(
        recipes.DatasetType.pg_dump,
        "-t",
        "--type",
        help="Dataset type",
    ),
    database_schema: str = typer.Option(
        "postgres",
        "-s",
        "--schema",
        help="Database Schema",
    ),
):
    import_dataset(
        plan.InputDataset(name=dataset_name, version=version, file_type=dataset_type),
        postgres.PostgresClient(schema=database_schema),
    )


if __name__ == "__main__":
    app()
