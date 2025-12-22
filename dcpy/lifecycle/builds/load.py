from collections import defaultdict
from enum import StrEnum
from pathlib import Path

import pandas as pd
import typer
import yaml

from dcpy.connectors.edm import recipes
from dcpy.lifecycle import data_loader
from dcpy.lifecycle.builds import metadata, plan
from dcpy.models.lifecycle.builds import (
    BuildMetadata,
    ImportedDataset,
    InputDataset,
    InputDatasetDestination,
    LoadResult,
)
from dcpy.utils import postgres
from dcpy.utils.logging import logger

LIFECYCLE_STAGE = "builds.load"


class CachedEntityType(StrEnum):
    view = "view"
    copy = "copy"


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
    assert ds.destination, f"{ERROR_UNRESOLVED_DESTINATION} {ds.id}"

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
    recipe_lock_path: Path,
    clear_pg_schema: bool = True,
    cache_schema: str | None = None,
    cached_entity_type: CachedEntityType | None = None,
    target_schema: str | None = None,
    _write_metadata_file: bool = True,
) -> LoadResult:
    recipe = plan.recipe_from_yaml(Path(recipe_lock_path))

    plan.write_source_data_versions(recipe_file=Path(recipe_lock_path))

    target_schema = target_schema or metadata.build_name()
    logger.info(f"Loading source data for {recipe.name} build named {target_schema}")
    has_postgres = InputDatasetDestination.postgres in [
        dataset.destination for dataset in recipe.inputs.datasets
    ]
    if has_postgres:
        pg_client = postgres.PostgresClient(schema=target_schema)
        if clear_pg_schema:
            setup_build_pg_schema(pg_client)
    else:
        pg_client = None

    # Fetch source data versions if caching is enabled
    cache_source_versions_df = None
    if cache_schema and cached_entity_type and pg_client:
        try:
            cache_source_versions_df = get_source_data_versions(cache_schema)
        except Exception:
            logger.warning(
                f"Could not fetch source data versions from cache schema {cache_schema}"
            )

    imported_datasets: dict[str, dict[str, ImportedDataset]] = defaultdict(dict)
    for ds in recipe.inputs.datasets:
        use_cached = (
            cache_schema
            and cached_entity_type
            and ds.destination == InputDatasetDestination.postgres
            and cache_source_versions_df is not None
            and dataset_exists_in_schema(ds, cache_source_versions_df)
        )
        if not use_cached:  # business as usual. Pull data and load it.
            file_path = data_loader.pull_dataset(ds, stage=LIFECYCLE_STAGE)
            imported_datasets[ds.id][str(ds.version)] = import_dataset(
                ds, file_path, pg_client
            )
        else:
            assert pg_client and cache_schema
            logger.info(
                f"Dataset {ds.dataset} version {ds.version} exists in schema {cache_schema}"
            )
            table_name = ds.import_as or ds.id
            imported_datasets[ds.id][str(ds.version)] = ImportedDataset.from_input(
                ds, table_name
            )
            if cached_entity_type == CachedEntityType.view:
                pg_client.create_view(table_name, cache_schema)
                logger.info(
                    f"Created a view of {cache_schema}.{table_name} -> {target_schema}.{table_name}"
                )
            else:
                pg_client.copy_table(table_name, cache_schema)
                logger.info(
                    f"Copied {cache_schema}.{table_name} -> {target_schema}.{table_name}"
                )

    if has_postgres:
        pg_client.create_table_from_csv(  # type: ignore
            recipe_lock_path.parent / "source_data_versions.csv"
        )
    load_result = LoadResult(
        name=recipe.name, build_name=target_schema, datasets=imported_datasets
    )
    if _write_metadata_file:  # mostly an override for test cases, so we don't have build files lying around afterward
        metadata.write_build_metadata(
            recipe, recipe_lock_path.parent, load_result=load_result
        )

    return load_result


def dataset_exists_in_schema(
    ds: InputDataset, source_versions_df: pd.DataFrame
) -> bool:
    """Check if a dataset with specific version exists in the source_data_versions DataFrame."""
    try:
        logger.info(
            f"Checking if dataset {ds.id} version {ds.version} file-type {ds.file_type} exists in cache"
        )
        # Check if this dataset id and version combination exists
        matching_datasets = source_versions_df[
            (source_versions_df["schema_name"] == ds.id)
            & (source_versions_df["v"] == ds.version)
            & (source_versions_df["file_type"] == ds.file_type)
        ]
        return len(matching_datasets) > 0
    except Exception:
        return False


def get_source_data_versions(schema_name: str) -> pd.DataFrame:
    """Get source data versions from the specified schema.

    Returns DataFrame with columns: schema_name, v, file_type
    """
    return postgres.PostgresClient(schema=schema_name).execute_select_query(
        "SELECT * FROM source_data_versions"
    )


def get_imported_df(
    load_result: LoadResult, ds_id: str, version: str = ""
) -> pd.DataFrame:
    assert ds_id in load_result.datasets, (
        f"No dataset of name {ds_id} imported in build {load_result.build_name} of {load_result.name}"
    )
    dataset = load_result.datasets[ds_id][
        version or load_result.get_latest_version_str(ds_id)
    ]

    match dataset.destination_type:
        case "df":
            return dataset.destination  # type: ignore
        case "postgres":
            pg_client = postgres.PostgresClient(schema=load_result.build_name)
            return pg_client.read_table_df(dataset.destination)  # type: ignore
        case "file":
            path = Path(dataset.destination)  # type: ignore
            if path.suffix == ".csv":
                return pd.read_csv(path)
            elif path.suffix == ".parquet":
                return pd.read_parquet(path)
            else:
                raise Exception(
                    f"File {path} cannot be simply read by pandas, use 'load.get_imported_file' instead."
                )
        case _ as d:
            raise Exception(f"Unknown type of imported dataset {ds_id}: {type(d)}")


def get_imported_filepath(
    load_result: LoadResult, ds_id: str, version: str = ""
) -> Path | str:
    assert ds_id in load_result.datasets, (
        f"No dataset of name {ds_id} imported in build {load_result.build_name} of {load_result.name}"
    )
    version = version or list(load_result.datasets[ds_id].keys())[-1]
    ds = load_result.datasets[ds_id][version]
    if ds.destination_type == "file":
        return Path(str(ds.destination))
    elif ds.destination_type == "pg":
        return f"Postgres table {str(ds.destination)}"
    else:
        raise Exception(
            f"Cannot get imported file of dataset '{ds_id}' because it is of format '{format}'"
        )


def get_imported_file(load_result: LoadResult, ds_id: str):  # -> TextIOWrapper:
    return open(get_imported_filepath(load_result, ds_id))


def get_build_metadata(project_path: Path) -> BuildMetadata:
    with open(project_path / "build_metadata.json") as f:
        return BuildMetadata(**yaml.safe_load(f))


app = typer.Typer(add_completion=False)


@app.command("recipe")
def _cli_wrapper_recipe(
    recipe_path: Path = typer.Option(
        None,
        "--recipe-path",
        "-r",
        help="Recipe Path",
    ),
    version: str = typer.Option(
        None,
        "--version",
        "-v",
        help="Version of dataset being built",
    ),
    clear_pg_schema: bool = typer.Option(
        True,
        "--clear-schema",
        "-x",
        help="Clear the build schema?",
    ),
    cache_schema: str = typer.Option(
        None,
        "--cache-schema",
        "-c",
        help="Schema name to use for caching existing datasets",
    ),
    cached_entity_type: CachedEntityType = typer.Option(
        None,
        "--cached-entity-type",
        "-t",
        help="How to cache datasets: 'view' creates views (read-only), 'copy' creates table copies (modifiable)",
    ),
):
    recipe_lock_path = plan.plan(recipe_path or Path("./recipe.yml"), version)
    load_source_data_from_resolved_recipe(
        recipe_lock_path,
        clear_pg_schema=clear_pg_schema,
        cache_schema=cache_schema,
        cached_entity_type=cached_entity_type,
    )


@app.command("load")
def _cli_wrapper_load(
    recipe_lock_path: Path = typer.Option(
        None,
        "--recipe-path",
        "-r",
        help="Path of recipe lock file to use",
    ),
    clear_pg_schema: bool = typer.Option(
        True,
        "--clear-schema",
        "-x",
        help="Clear the build schema?",
    ),
    cache_schema: str = typer.Option(
        None,
        "--cache-schema",
        "-c",
        help="Schema name to use for caching existing datasets",
    ),
    cached_entity_type: CachedEntityType = typer.Option(
        None,
        "--cached-entity-type",
        "-t",
        help="How to cache datasets: 'view' creates views (read-only), 'copy' creates table copies (modifiable)",
    ),
):
    recipe_lock_path = recipe_lock_path or (
        Path(plan.DEFAULT_RECIPE).parent / "recipe.lock.yml"
    )
    load_source_data_from_resolved_recipe(
        recipe_lock_path,
        clear_pg_schema=clear_pg_schema,
        cache_schema=cache_schema,
        cached_entity_type=cached_entity_type,
    )


if __name__ == "__main__":
    app()
