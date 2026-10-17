import importlib
import json
import os
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

import duckdb
import pandas as pd
import pyogrio
import typer
from pyogrio.raw import read_arrow

from dcpy.connectors.edm.models import DatasetType
from dcpy.geospatial import parquet as geoparquet
from dcpy.geospatial.curves.layer import layer_to_parquet
from dcpy.lifecycle import config
from dcpy.lifecycle.builds.models import InputDataset
from dcpy.lifecycle.connector_registry import connectors
from dcpy.utils import duckdb as duckdb_utils
from dcpy.utils import postgres
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


# Rows per batch when streaming a parquet file into Postgres. Sized to keep peak
# memory well under a standard CI runner (~7GB) while limiting round-trips.
PARQUET_LOAD_BATCH_SIZE = 100_000


def _sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to be more sql-friendly."""
    return df.rename(
        columns={
            column: column.strip().replace("-", "_").replace("'", "_").replace(" ", "_")
            for column in df.columns
        }
    )


# The loader owns these two columns: it appends ogc_fid as the surrogate primary key
# and data_library_version as the version loaded. A product that re-imports its own
# published output (see the edm.publishing.published sources in products/*/recipe.yml)
# gets them back as data, and re-adding them then fails on a duplicate column. Drop
# them on the way in so the loader's own values are the ones that land.
LOADER_OWNED_COLUMNS = ["ogc_fid", "data_library_version"]


def _drop_loader_owned_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(columns=LOADER_OWNED_COLUMNS, errors="ignore")


def _load_df(
    df: pd.DataFrame,
    ds_table_name: str,
    pg_client: postgres.PostgresClient,
    include_ogc_fid_col: bool = True,
):
    pg_client.insert_dataframe(
        _drop_loader_owned_columns(_sanitize_columns(df)), ds_table_name
    )

    if include_ogc_fid_col:
        # This maybe should be applicable to pg_dumps, but they tend to have this column already
        pg_client.add_pk(ds_table_name, "ogc_fid")


def _load_parquet_chunked(
    local_dataset_path: Path,
    ds_table_name: str,
    pg_client: postgres.PostgresClient,
    include_ogc_fid_col: bool = True,
):
    """Stream a parquet file into Postgres in batches, never holding it all in memory.

    Only safe for plain (non-geo, un-preprocessed) parquet — see load_dataset_into_pg
    for why the preprocessor / geoparquet cases still read the whole frame.
    """
    for i, batch in enumerate(
        geoparquet.iter_batches_df(local_dataset_path, PARQUET_LOAD_BATCH_SIZE)
    ):
        pg_client.insert_dataframe(
            _drop_loader_owned_columns(_sanitize_columns(batch)),
            ds_table_name,
            if_exists="replace" if i == 0 else "append",
        )

    if include_ogc_fid_col:
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

    # If the path is a directory and custom.filename is specified, append the filename -
    # same fallback load_dataset_into_duckdb already has. A connector's pull() can return
    # a directory rather than a single file (e.g. edm.private, which downloads the whole
    # version folder when no single-file filepath/source_path kwarg is set - custom.filename
    # only narrows which file *this* loader reads, same recipe.yml convention as duckdb).
    if local_dataset_path.is_dir() and ds.custom and "filename" in ds.custom:
        local_dataset_path = local_dataset_path / ds.custom["filename"]
        logger.info(
            f"Using specific file from directory: {local_dataset_path.name} for dataset {ds.id}"
        )

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
            # A preprocessor may need the whole frame, and geoparquet needs geometry
            # reconstructed into a GeoDataFrame — neither is safe to stream in batches.
            # The plain path streams to keep large files off the runner's memory.
            if has_preprocessor or geoparquet.is_geoparquet(local_dataset_path):
                raw_df = geoparquet.read_df(local_dataset_path)
                df = preprocessor(ds.id, raw_df) if has_preprocessor else raw_df
                _load_df(df, ds_table_name, pg_client, include_ogc_fid_col)
            else:
                _load_parquet_chunked(
                    local_dataset_path, ds_table_name, pg_client, include_ogc_fid_col
                )
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


def _unzipped_gdb(path: Path) -> Path:
    """Extract a zipped GDB once, next to the zip, and return the .gdb directory.

    GDAL reads a GDB inside a zip far slower than an extracted one (Centerline: ~100s
    vs ~2s), and every layer of a multi-layer GDB would pay that again.
    """
    if path.suffix.lower() != ".zip":
        return path
    out = path.with_name(f"{path.name}.unzipped")
    if not out.exists():
        partial = out.with_name(f"{out.name}.partial")
        shutil.rmtree(partial, ignore_errors=True)
        with zipfile.ZipFile(path) as zf:
            zf.extractall(partial)
        partial.replace(out)
    gdbs = [p for p in out.rglob("*.gdb") if p.is_dir() and "__MACOSX" not in p.parts]
    if len(gdbs) != 1:
        raise Exception(f"Expected one .gdb in {path}, found {gdbs}")
    return gdbs[0]


def _load_gdb_into_duckdb(
    ds: InputDataset,
    duckdb_client: duckdb_utils.DuckDBClient,
    gdb_path: Path,
    table_name: str,
) -> None:
    """Load one GDB layer. ``load_engine: curves`` goes through the curve library
    (linear table plus ``<table>_arcs``); ``gdal`` uses DuckDB's ST_Read."""
    layer_name = (ds.custom or {}).get("layer_name")
    if not layer_name:
        raise Exception(f"GDB dataset {ds.id} ({table_name}) needs custom.layer_name")

    def load_via_curves():
        with tempfile.TemporaryDirectory() as tmp:
            linear, arcs = Path(tmp) / "linear.parquet", Path(tmp) / "arcs.parquet"
            n_rows, _ = layer_to_parquet(gdb_path, layer_name, linear, arcs)
            if n_rows:
                duckdb_client.load_geoparquet(linear, table_name)
            else:  # no GeoParquet is written for an empty layer; ST_Read keeps its schema
                duckdb_client.load_gdb_layer(gdb_path, table_name, layer_name)
            duckdb_client.load_parquet(
                arcs, f"{table_name}_arcs", include_ogc_fid_col=False
            )

    match ds.load_engine:
        case "curves":
            load_via_curves()
        case "gdal":
            if pyogrio.read_info(gdb_path, layer=layer_name)["geometry_type"] is None:
                # attribute-only table: pyogrio reads these reliably (ST_Read can stall)
                # arrow (not pandas) keeps nullable integers as integers
                _, table = read_arrow(gdb_path, layer=layer_name, read_geometry=False)
                duckdb_client.load_arrow(table, table_name)
                return
            try:
                duckdb_client.load_gdb_layer(gdb_path, table_name, layer_name)
            except duckdb.InvalidInputException as e:
                # ST_Read can't parse curved geometry (CompoundCurve, CurvePolygon, ...)
                if "Unsupported geometry type" not in str(e):
                    raise
                logger.warning(f"{table_name}: curved geometry, using curve library")
                load_via_curves()
        case _:
            raise Exception(
                f"Unsupported load_engine for GDB into DuckDB: {ds.load_engine}"
            )


def load_dataset_into_duckdb(
    ds: InputDataset,
    duckdb_client: duckdb_utils.DuckDBClient,
    local_dataset_path: Path,
    *,
    include_version_col: bool = True,
    include_ogc_fid_col: bool = True,
):
    """Load a dataset into DuckDB.

    Args:
        ds: Input dataset configuration
        duckdb_client: DuckDB client instance
        local_dataset_path: Path to local dataset file or directory
        include_version_col: Whether to add data_library_version column
        include_ogc_fid_col: Whether to add ogc_fid primary key column
    """
    ds_table_name = ds.import_as or ds.id

    # If the path is a directory and custom.filename is specified, append the filename
    if local_dataset_path.is_dir() and ds.custom and "filename" in ds.custom:
        local_dataset_path = local_dataset_path / ds.custom["filename"]
        logger.info(
            f"Using specific file from directory: {local_dataset_path.name} for dataset {ds.id}"
        )

    match ds.file_type:
        case DatasetType.csv:
            duckdb_client.load_csv(
                local_dataset_path,
                ds_table_name,
                include_ogc_fid_col,
                all_varchar=(ds.custom or {}).get("all_varchar", False),
            )
        case DatasetType.parquet:
            duckdb_client.load_parquet(
                local_dataset_path, ds_table_name, include_ogc_fid_col
            )
        case DatasetType.shapefile:
            duckdb_client.load_spatial(
                local_dataset_path,
                ds_table_name,
                include_ogc_fid_col,
                layer_name=ds.custom.get("layer_name"),
            )
        case DatasetType.geodatabase:
            _load_gdb_into_duckdb(
                ds, duckdb_client, _unzipped_gdb(local_dataset_path), ds_table_name
            )
        case _:
            raise Exception(
                f"Unsupported file type for DuckDB: {ds.file_type}. Only CSV, Parquet, shapefile, and gdb are currently supported."
            )

    if include_version_col:
        duckdb_client.add_table_column(
            ds_table_name,
            col_name="data_library_version",
            col_type="VARCHAR",
            default_value=str(ds.version),
        )

    return f"{duckdb_client.schema}.{duckdb_utils.sanitize_name(ds_table_name)}"


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
    database_schema = database_schema or os.environ["BUILD_ENGINE_SCHEMA"]
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
