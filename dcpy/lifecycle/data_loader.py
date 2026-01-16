import importlib
import json
import os
import pandas as pd
from pathlib import Path
import subprocess
import typer

from dcpy.lifecycle import config
from dcpy.lifecycle.connector_registry import connectors
from dcpy.models.connectors.edm.recipes import DatasetType
from dcpy.models.lifecycle.builds import InputDataset
from dcpy.utils import postgres
from dcpy.utils.geospatial import parquet as geoparquet
from dcpy.utils.logging import logger


def pull_dataset(
    ds: InputDataset, stage: str, *, dest_dir_override: Path | None = None
) -> Path:
    logger.info(f"Pulling {ds.id} to destination: {ds.destination}")

    if ds.version == "latest" or ds.version is None:
        raise Exception(f"Cannot import a dataset without a resolved version: {ds}")
    if ds.file_type is None:
        raise Exception(f"Cannot import a dataset without a resolved file type: {ds}")
    if ds.source is None:
        raise Exception(f"Cannot import a dataset with no source: {ds}")

    ds.custom["file_type"] = ds.file_type or "Other"
    connector = connectors.pull[ds.source]

    stage_path = config.local_data_path_for_stage(stage)
    conn_sub_path = connector.get_pull_local_sub_path(
        key=ds.id, version=ds.version, pull_conf=ds.custom
    )
    pull_res = connector.pull(
        key=ds.id,
        version=ds.version,
        destination_path=dest_dir_override or stage_path / conn_sub_path,
        **ds.custom,
    )
    logger.info(f"Pulled {ds.id} to {pull_res['path']}.")
    return pull_res["path"].absolute()


def _get_preprocessor(ds: InputDataset):
    def identity(_: str, df: pd.DataFrame):
        return df

    if ds.preprocessor and ds.file_type == DatasetType.pg_dump:
        raise Exception(
            f"Preprocessor for {ds.id} cannot be applied to pg_dump dataset {ds.id}."
        )
    elif ds.preprocessor:
        preproc_mod = importlib.import_module(ds.preprocessor.module)
        return getattr(preproc_mod, ds.preprocessor.function)
    else:
        return identity


def _load_df(
    df: pd.DataFrame,
    ds_table_name: str,
    pg_client: postgres.PostgresClient,
    include_ogc_fid_col: bool = True,
):
    # make column names more sql-friendly
    columns = {
        column: column.strip().replace("-", "_").replace("'", "_").replace(" ", "_")
        for column in df.columns
    }
    df.rename(columns=columns, inplace=True)
    pg_client.insert_dataframe(df, ds_table_name)

    if include_ogc_fid_col:
        # This maybe should be applicable to pg_dumps, but they tend to have this column already
        pg_client.add_pk(ds_table_name, "ogc_fid")


def load_dataset_into_pg(
    ds: InputDataset,
    pg_client: postgres.PostgresClient,
    local_dataset_path: Path,
    *,
    include_version_col: bool = True,
    include_ogc_fid_col: bool = True,
):
    has_preprocessor = ds.preprocessor is not None
    preprocessor = _get_preprocessor(ds)
    ds_table_name = ds.import_as or ds.id
    engine = ds.load_engine or "pandas"
    match engine, ds.file_type:
        case _, DatasetType.pg_dump:
            logger.info(f"DatasetType pg_dump specified, ignoring engine '{engine}'")
            if has_preprocessor:
                logger.warning(
                    "A preprocessor is defined for a pg_dump type. However, preprocessors cannot be applied to pg_dump datasets."
                )
            pg_client.import_pg_dump(
                local_dataset_path,
                pg_dump_table_name=ds.id,
                target_table_name=ds_table_name,
            )
        case "gdal", _:
            command = " ".join(
                [
                    "ogr2ogr",
                    "-f",
                    "PostgreSQL",
                    'PG:"host=$BUILD_ENGINE_HOST port=$BUILD_ENGINE_PORT user=$BUILD_ENGINE_USER password=$BUILD_ENGINE_PASSWORD dbname=$BUILD_ENGINE_DB"',
                    f'"{local_dataset_path}"',
                    ds.custom["layer_name"],
                    "-nln",
                    f'"{pg_client.schema}"."{ds_table_name.lower()}"',
                    "-nlt",
                    "geometry",
                    "-lco",
                    "GEOMETRY_NAME=geom",
                    "-lco",
                    "FID=ogc_fid",
                    "-overwrite",
                ]
            )

            # process = subprocess.list2cmdline(command)
            # print(f"Command to be executed (list): {process}")
            subprocess.check_call(command, shell=True)
        case "pandas", DatasetType.csv:
            raw_df = pd.read_csv(local_dataset_path, dtype=str)
            df = preprocessor(ds.id, raw_df) if has_preprocessor else raw_df
            _load_df(df, ds_table_name, pg_client, include_ogc_fid_col)
        case "pandas", DatasetType.parquet:
            raw_df = geoparquet.read_df(local_dataset_path)
            df = preprocessor(ds.id, raw_df) if has_preprocessor else raw_df
            _load_df(df, ds_table_name, pg_client, include_ogc_fid_col)
        case "pandas", DatasetType.json:
            with open(local_dataset_path, "r") as json_file:
                records = json.load(json_file)

            if not has_preprocessor:
                logger.warning(
                    "Coverting JSON to a dataframe without a preprocessor. This could have unintended results."
                )
                df = pd.DataFrame(records)
            else:
                df = preprocessor(ds.id, records)
            _load_df(df, ds_table_name, pg_client, include_ogc_fid_col)
        case "pandas", _:
            raise Exception(f"Invalid file_type for {ds.id}: {ds.file_type}")

    if include_version_col:
        pg_client.add_table_column(
            ds_table_name,
            col_name="data_library_version",
            col_type="text",
            default_value=str(ds.version),
        )

    return f"{pg_client.schema}.{ds_table_name}"


def import_dataset(
    ds: InputDataset,
    stage: str,
    pg_client: postgres.PostgresClient,
    *,
    dest_dir_override: Path | None = None,
    include_version_col: bool = True,
    include_ogc_fid_col: bool = True,
):
    local_dataset_path = pull_dataset(ds, stage, dest_dir_override=dest_dir_override)

    # TODO: this should probably just be a registered connector, instead of pg
    load_dataset_into_pg(
        ds=ds,
        pg_client=pg_client,
        local_dataset_path=local_dataset_path,
        include_ogc_fid_col=include_ogc_fid_col,
        include_version_col=include_version_col,
    )


app = typer.Typer(add_completion=False)


@app.command("import")
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
    lifecycle_stage: str = typer.Option(
        "builds.load",
        "-l",
        "--stage",
        help="Lifecycle Stage",
    ),
    source: str = typer.Option(
        "edm.recipes.datasets",
        "-f",
        "--source",
        help="Registered source to import from",
    ),
    dataset_type: DatasetType = typer.Option(
        DatasetType.parquet,
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
    ds = InputDataset(
        id=dataset_name,
        version=version,
        source=source,
        file_type=dataset_type,
        import_as=import_as,
        custom={"file_type": dataset_type},
    )
    client = postgres.PostgresClient(schema=database_schema, database=database)
    import_dataset(ds=ds, stage=lifecycle_stage, pg_client=client)


@app.command("get_versions")
def _cli_wrapper_get_versions(
    connector: str = typer.Argument(
        help="Connector Name",
    ),
    key: str = typer.Argument(
        help="Resource Key",
    ),
    # TODO: support for kwargs / opts
):
    conn = connectors.versioned[connector]
    print(conn.list_versions(key))
