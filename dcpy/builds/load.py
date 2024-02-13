from __future__ import annotations
import importlib
import pandas as pd
from pathlib import Path
from pydantic import BaseModel
import typer

from dcpy.utils import postgres
from dcpy.utils.logging import logger
from dcpy.connectors.edm import recipes, publishing
from dcpy.builds import metadata, plan


class ImportedDataset(
    BaseModel, use_enum_values=True, extra="forbid", arbitrary_types_allowed=True
):
    name: str
    version: str
    file_type: recipes.DatasetType
    destination: str | pd.DataFrame | Path

    @staticmethod
    def from_input(
        ds: plan.InputDataset, result: str | pd.DataFrame | Path
    ) -> ImportedDataset:
        assert ds.version, f"Version of {ds.name} not resolved"
        assert ds.file_type, f"File type of {ds.name} not resolved"
        return ImportedDataset(
            name=ds.name, version=ds.version, file_type=ds.file_type, destination=result
        )


class LoadResult(BaseModel, use_enum_values=True, extra="forbid"):
    name: str
    build_name: str
    datasets: dict[str, ImportedDataset]


def setup_build_pg_schema(pg_client: postgres.PostgresClient):
    if pg_client.schema != "public":
        pg_client.drop_schema()
        pg_client.drop_schema(pg_client.schema_tests)
        pg_client.create_schema()


def import_dataset(
    ds: plan.InputDataset,
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
                f"Preprocessor {ds.preprocessor.module} cannot be applied to pg_dump dataset {ds.name}."
            )
    else:
        preproc_func = None

    assert ds.destination, f"Dataset destination not resolved for dataset {ds.name}"
    match ds.destination:
        case plan.InputDatasetDestination.postgres:
            assert pg_client, "pg_client must be defined for postgres import"
            table = recipes.import_dataset(
                ds.dataset,
                pg_client,
                preprocessor=preproc_func,
                import_as=ds.import_as,
            )
            return ImportedDataset.from_input(ds, table)
        case plan.InputDatasetDestination.df:
            df = recipes.read_df(ds.dataset)
            return ImportedDataset.from_input(ds, df)
        case plan.InputDatasetDestination.file:
            file_path = recipes.fetch_dataset(ds.dataset)
            return ImportedDataset.from_input(ds, file_path)
        case _ as d:
            raise Exception(f"Unsupported dataset import destination: {d}")


def load_source_data(
    recipe_path: Path,
    version: str | None = None,
    repeat: bool = False,
    keep_files: bool = False,
) -> LoadResult:
    recipe_lock_path = plan.plan(recipe_path, version, repeat)
    recipe = plan.recipe_from_yaml(Path(recipe_lock_path))

    plan.write_source_data_versions(recipe_file=Path(recipe_lock_path))
    metadata.write_build_metadata(recipe, recipe_path.parent)

    build_name = metadata.build_name()
    logger.info(f"Loading source data for {recipe.name} build named {build_name}")
    if plan.InputDatasetDestination.postgres.value in [
        dataset.destination for dataset in recipe.inputs.datasets
    ]:
        pg_client = postgres.PostgresClient(schema=build_name)
        setup_build_pg_schema(pg_client)

        pg_client.create_table_from_csv(
            recipe_lock_path.parent / "source_data_versions.csv"
        )
    else:
        pg_client = None

    results = {ds.name: import_dataset(ds, pg_client) for ds in recipe.inputs.datasets}
    if not keep_files:
        recipes.purge_recipe_cache()
    return LoadResult(name=recipe.name, build_name=build_name, datasets=results)


def get_imported_df(load_result: LoadResult, ds_name: str) -> pd.DataFrame:
    assert (
        ds_name in load_result.datasets
    ), f"No dataset of name {ds_name} imported in build {load_result.build_name} of {load_result.name}"
    match load_result.datasets[ds_name].destination:
        case pd.DataFrame() as df:
            return df
        case str() as table_name:
            pg_client = postgres.PostgresClient(schema=load_result.build_name)
            return pg_client.read_sql_table(table_name)
        case Path() as file_path:
            if file_path.suffix == "csv":
                return pd.read_csv(file_path)
            elif file_path.suffix == "parquet":
                return pd.read_parquet(file_path)
            else:
                raise Exception(
                    f"File {file_path} cannot be simply read by pandas, use 'load.get_imported_file' instead."
                )
        case _ as d:
            raise Exception(f"Unknown type of imported dataset {ds_name}: {type(d)}")


def get_imported_filepath(load_result: LoadResult, ds_name: str) -> Path:
    assert (
        ds_name in load_result.datasets
    ), f"No dataset of name {ds_name} imported in build {load_result.build_name} of {load_result.name}"
    match load_result.datasets[ds_name].destination:
        case Path() as file_path:
            return file_path
        case pd.DataFrame():
            format = "DataFrame"
        case str() as table_name:
            format = f"Postgres table '{table_name}'"
    raise Exception(
        f"Cannot get imported file of dataset {ds_name} because it is of format {format}"
    )


def get_imported_file(load_result: LoadResult, ds_name: str):  # -> TextIOWrapper:
    return open(get_imported_filepath(load_result, ds_name))


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
