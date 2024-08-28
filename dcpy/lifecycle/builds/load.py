from __future__ import annotations
import importlib
import os
import pandas as pd
from pathlib import Path
import typer

from dcpy.utils import postgres
from dcpy.utils.logging import logger
from dcpy.connectors.edm import recipes
from dcpy.models.lifecycle.builds import (
    ImportedDataset,
    LoadResult,
    InputDatasetDestination,
    InputDataset,
)
from dcpy.lifecycle.builds import metadata, plan


def setup_build_pg_schema(pg_client: postgres.PostgresClient):
    if pg_client.schema != "public":
        pg_client.drop_schema()
        pg_client.drop_schema(pg_client.schema_tests)
        pg_client.create_schema()


def import_dataset(
    ds: InputDataset,
    pg_client: postgres.PostgresClient | None,
) -> ImportedDataset:
    """Import a recipe to local data library folder and build engine."""

    if ds.version == "latest" or ds.version is None:
        raise Exception(f"Cannot import a dataset without a resolved version: {ds}")
    if ds.file_type is None:
        raise Exception(f"Cannot import a dataset without a resolved file type: {ds}")
    if ds.preprocessor is not None:
        preproc_mod = importlib.import_module(ds.preprocessor.module)
        preproc_func = getattr(preproc_mod, ds.preprocessor.function)
        if ds.file_type == recipes.DatasetType.pg_dump:
            logger.warning(
                f"Preprocessor {ds.preprocessor.module} cannot be applied to pg_dump dataset {ds.id}."
            )
    else:
        preproc_func = None

    if pg_client and (not ds.destination):
        ds.destination = InputDatasetDestination.postgres
    assert ds.destination, f"Dataset destination not resolved for dataset {ds.id}"
    match ds.destination:
        case InputDatasetDestination.postgres:
            assert pg_client, "pg_client must be defined for postgres import"
            table = recipes.import_dataset(
                ds.dataset,
                pg_client,
                preprocessor=preproc_func,
                import_as=ds.import_as,
            )
            return ImportedDataset.from_input(ds, table)
        case InputDatasetDestination.df:
            df = recipes.read_df(ds.dataset)
            return ImportedDataset.from_input(ds, df)
        case InputDatasetDestination.file:
            file_path = recipes.fetch_dataset(ds.dataset)
            return ImportedDataset.from_input(ds, file_path)
        case _ as d:
            raise Exception(f"Unsupported dataset import destination: {d}")


def load_source_data(
    recipe_lock_path: Path,
    keep_files: bool = False,
) -> LoadResult:
    recipe = plan.recipe_from_yaml(Path(recipe_lock_path))

    plan.write_source_data_versions(recipe_file=Path(recipe_lock_path))
    metadata.write_build_metadata(recipe, recipe_lock_path.parent)

    build_name = metadata.build_name()
    logger.info(f"Loading source data for {recipe.name} build named {build_name}")
    if InputDatasetDestination.postgres in [
        dataset.destination for dataset in recipe.inputs.datasets
    ]:
        pg_client = postgres.PostgresClient(schema=build_name)
        setup_build_pg_schema(pg_client)

        pg_client.create_table_from_csv(
            recipe_lock_path.parent / "source_data_versions.csv"
        )
    else:
        pg_client = None

    results = {ds.id: import_dataset(ds, pg_client) for ds in recipe.inputs.datasets}
    if not keep_files:
        recipes.purge_recipe_cache()
    return LoadResult(name=recipe.name, build_name=build_name, datasets=results)


def get_imported_df(load_result: LoadResult, ds_id: str) -> pd.DataFrame:
    assert (
        ds_id in load_result.datasets
    ), f"No dataset of name {ds_id} imported in build {load_result.build_name} of {load_result.name}"
    match load_result.datasets[ds_id].destination:
        case pd.DataFrame() as df:
            return df
        case str() as table_name:
            pg_client = postgres.PostgresClient(schema=load_result.build_name)
            return pg_client.read_table_df(table_name)
        case Path() as file_path:
            if file_path.suffix == ".csv":
                return pd.read_csv(file_path, dtype=str)
            elif file_path.suffix == ".parquet":
                return pd.read_parquet(file_path)
            else:
                raise Exception(
                    f"File {file_path} cannot be simply read by pandas, use 'load.get_imported_file' instead."
                )
        case _ as d:
            raise Exception(f"Unknown type of imported dataset {ds_id}: {type(d)}")


def get_imported_filepath(load_result: LoadResult, ds_id: str) -> Path:
    assert (
        ds_id in load_result.datasets
    ), f"No dataset of name {ds_id} imported in build {load_result.build_name} of {load_result.name}"
    match load_result.datasets[ds_id].destination:
        case Path() as file_path:
            return file_path
        case pd.DataFrame():
            format = "DataFrame"
        case str() as table_name:
            format = f"Postgres table {table_name}"
        case _:
            format = "unknown"
    raise Exception(
        f"Cannot get imported file of dataset '{ds_id}' because it is of format '{format}'"
    )


def get_imported_file(load_result: LoadResult, ds_id: str):  # -> TextIOWrapper:
    return open(get_imported_filepath(load_result, ds_id))


app = typer.Typer(add_completion=False)


@app.command("recipe")
def _cli_wrapper_recipe(
    recipe_path: Path = typer.Option(
        Path(plan.DEFAULT_RECIPE),
        "--recipe-path",
        "-r",
        help="Path of recipe file to use",
    ),
    version: str = typer.Option(
        None,
        "--version",
        "-v",
        help="Version of dataset being built",
    ),
):
    recipe_lock_path = plan.plan(recipe_path, version)
    load_source_data(recipe_lock_path)


@app.command("load")
def _cli_wrapper_load(
    recipe_lock_path: Path = typer.Option(
        None,
        "--recipe-path",
        "-r",
        help="Path of recipe lock file to use",
    ),
):
    recipe_lock_path = recipe_lock_path or (
        Path(plan.DEFAULT_RECIPE).parent / "recipe.lock.yml"
    )
    load_source_data(recipe_lock_path)


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
        None,
        "-s",
        "--schema",
        help="Database Schema",
    ),
    database: str = typer.Option(
        None,
        "-d",
        "--database",
    ),
    import_as: str = typer.Option(
        None, "--import-as", help="Renames the imported table"
    ),
):
    database_schema = database_schema or os.environ["BUILD_SCHEMA"]
    import_dataset(
        InputDataset(
            id=dataset_name,
            version=version,
            file_type=dataset_type,
            import_as=import_as,
        ),
        postgres.PostgresClient(schema=database_schema, database=database),
    )


if __name__ == "__main__":
    app()
