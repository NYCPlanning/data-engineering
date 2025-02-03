import pandas as pd
from pathlib import Path
import typer

from dcpy.utils import postgres
from dcpy.utils.logging import logger
from dcpy.connectors.edm import recipes
from dcpy.models.lifecycle.builds import (
    ImportedDataset,
    InputDatasetDestination,
    InputDataset,
    LoadResult,
)
from dcpy.lifecycle.builds import metadata, plan
from dcpy.lifecycle import data_loader

LIFECYCLE_STAGE = "builds.load"


def setup_build_pg_schema(pg_client: postgres.PostgresClient):
    if pg_client.schema != "public":
        pg_client.drop_schema()
        pg_client.drop_schema(pg_client.schema_tests)
        pg_client.create_schema()


ERROR_UNRESOLVED_DATASET_VERSION = "Cannot import a dataset without a resolved version"
ERROR_UNRESOLVED_FILE_TYPE = "Cannot import a dataset without a resolved file type"
ERROR_UNRESOLVED_DESTINATION = "Dataset destination not resolved for dataset"


def import_dataset(
    ds: InputDataset,
    file_path: Path,
    pg_client: postgres.PostgresClient | None,
) -> ImportedDataset:
    """Import a recipe to local data library folder and build engine."""
    assert ds.version and not ds.version == "latest", ERROR_UNRESOLVED_DATASET_VERSION
    assert ds.file_type, ERROR_UNRESOLVED_FILE_TYPE

    if pg_client and (not ds.destination):
        ds.destination = InputDatasetDestination.postgres
    assert ds.destination, f"ERROR_UNRESOLVED_DESTINATION {ds.id}"

    match ds.destination:
        case InputDatasetDestination.postgres:
            assert pg_client, "pg_client must be defined for postgres import"
            logger.info(f"Inserting {ds.dataset} into postgres")
            table = data_loader.load_dataset_into_pg(
                ds, pg_client, local_dataset_path=file_path
            )
            logger.info(f"Finished inserting {ds.dataset} into postgres")
            return ImportedDataset.from_input(ds, table)
        case InputDatasetDestination.df:
            # TODO: remove reference to recipes
            df = recipes.read_df(ds.dataset)
            return ImportedDataset.from_input(ds, df)
        case InputDatasetDestination.file:
            return ImportedDataset.from_input(ds, file_path)
        case _ as d:
            raise Exception(f"Unsupported dataset import destination: {d}")


def load_source_data_from_resolved_recipe(
    recipe_lock_path: Path, clear_pg_schema: bool = True
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
        if clear_pg_schema:
            setup_build_pg_schema(pg_client)

        pg_client.create_table_from_csv(
            recipe_lock_path.parent / "source_data_versions.csv"
        )
    else:
        pg_client = None

    imported_datasets = {}
    for ds in recipe.inputs.datasets:
        file_path = data_loader.pull_dataset(ds, stage=LIFECYCLE_STAGE)
        imported_ds = import_dataset(ds, file_path, pg_client)
        imported_datasets[ds.id] = imported_ds

    return LoadResult(
        name=recipe.name, build_name=build_name, datasets=imported_datasets
    )


def get_imported_df(load_result: LoadResult, ds_id: str) -> pd.DataFrame:
    assert ds_id in load_result.datasets, (
        f"No dataset of name {ds_id} imported in build {load_result.build_name} of {load_result.name}"
    )
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
    assert ds_id in load_result.datasets, (
        f"No dataset of name {ds_id} imported in build {load_result.build_name} of {load_result.name}"
    )
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
    recipe_path: Path,
    version: str = typer.Option(
        None,
        "--version",
        "-v",
        help="Version of dataset being built",
    ),
    clear_pg_schema: bool = typer.Option(
        False,
        "--clear-schema",
        "-x",
        help="Clear the build schema?",
    ),
):
    recipe_lock_path = plan.plan(recipe_path, version)
    load_source_data_from_resolved_recipe(
        recipe_lock_path, clear_pg_schema=clear_pg_schema
    )


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
    load_source_data_from_resolved_recipe(recipe_lock_path)


if __name__ == "__main__":
    app()
