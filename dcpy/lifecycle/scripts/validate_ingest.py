import importlib
import os
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

import typer
import yaml

from dcpy import configuration
from dcpy.connectors.edm import recipes
from dcpy.data import compare
from dcpy.lifecycle import data_loader
from dcpy.lifecycle.ingest.run import INGEST_DIR
from dcpy.lifecycle.ingest.run import ingest as run_ingest
from dcpy.models.base import SortedSerializedBase, YamlWriter
from dcpy.models.data import comparison
from dcpy.models.lifecycle.builds import InputDataset
from dcpy.models.lifecycle.ingest import DatasetAttributes
from dcpy.utils import postgres
from dcpy.utils.collections import indented_report

DATABASE = "sandbox"
LIBRARY_DEFAULT_PATH = (
    Path(os.environ.get("PROJECT_ROOT_PATH") or os.getcwd()) / ".library"
)
LIBRARY_PATH = LIBRARY_DEFAULT_PATH / "datasets"
SCHEMA = "ingest_validation"


def _dataset_label(dataset: str) -> str:
    """Ensure dataset label will not result in too long geom index name (<63 chars)."""
    return dataset[:39]


def _input_dataset(
    dataset_id: str,
    version: str,
    file_type: recipes.DatasetType,
    table_name: str | None,
) -> InputDataset:
    table_name = table_name or f"{dataset_id}_{version}"
    return InputDataset(
        id=dataset_id,
        version=version,
        source="edm.recipes.datasets",
        file_type=file_type,
        custom={"file_type": file_type},
        import_as=table_name,
    )


class Converter(SortedSerializedBase, YamlWriter):
    _exclude_falsey_values: bool = False
    _exclude_none: bool = False
    _head_sort_order: list[str] = [
        "id",
        "acl",
        "attributes",
        "ingestion",
        "columns",
        "library_dataset",
    ]

    id: str
    acl: str
    attributes: DatasetAttributes
    ingestion: dict
    columns: list = []
    library_dataset: dict


def convert_template(dataset: str):
    library_path = (
        Path(__file__).parent.parent.parent / "library" / "templates" / f"{dataset}.yml"
    )
    ingest_path = (
        Path(__file__).parent.parent.parent
        / "lifecycle"
        / "ingest"
        / "templates"
        / f"{dataset}.yml"
    )
    with open(library_path) as library_file:
        library_template = yaml.safe_load(library_file)
    converter = Converter(
        id=library_template["dataset"]["name"],
        acl=library_template["dataset"]["acl"],
        attributes=DatasetAttributes(
            name="<CHANGE ME>",
            description=library_template["dataset"].get("info", {}).get("description"),
            url=library_template["dataset"].get("info", {}).get("url"),
        ),
        ingestion={
            "source": {
                "one_of": [
                    {"type": "local_file", "path": ""},
                    {"type": "file_download", "url": ""},
                    {"type": "api", "endpoint": "", "format": ""},
                    {"type": "s3", "bucket": "", "key": ""},
                    {
                        "type": "socrata",
                        "org": "",
                        "uid": "",
                        "format": "",
                    },
                    {"type": "edm_publishing_gis_dataset", "name": ""},
                ]
            },
            "file_format": {
                "type": "csv, json, xlsx, shapefile, geojson, geodatabase",
                "crs": "EPSG:2263",
            },
            "processing_steps": [],
        },
        library_dataset=library_template["dataset"],
    )
    converter.write_to_yaml(ingest_path)


def library_archive(
    dataset: str, version: str | None = None, file_type: str = "pgdump"
):
    # BEWARE: once you import library, parquet file writing fails
    # Something to do with gdal's interaction with parquet file driver
    from dcpy.library.archive import Archive

    a = Archive()
    config = a(name=dataset, output_format=file_type, version=version)
    # We're running ingest too, so change version after the fact
    # Can't just feed this version to archive call because of datasets that template in the version
    target_dir = LIBRARY_PATH / dataset / "library"
    if target_dir.is_dir():
        shutil.rmtree(target_dir)
    os.rename(LIBRARY_PATH / dataset / config.version, target_dir)


def ingest(
    dataset: str,
    version: str | None = None,
) -> None:
    ## separate from "standard" ingest folders
    dataset_staging_dir = INGEST_DIR / "migrate_from_library"
    if dataset_staging_dir.is_dir():
        shutil.rmtree(dataset_staging_dir)

    config = run_ingest(
        dataset,
        version=version,
        staging_dir=dataset_staging_dir,
        push=False,
    )
    assert len(config) == 1, (
        "Validate ingest not set up to compare one-to-many definitions"
    )

    ## copy so that it's in the "library" file system for easy import
    output_path = dataset_staging_dir / dataset / f"{dataset}.parquet"
    ingest_path = LIBRARY_PATH / dataset / "ingest" / f"{dataset}.parquet"
    ingest_path.parent.mkdir(exist_ok=True, parents=True)
    shutil.copy(output_path, ingest_path)


def load_recipe(
    dataset: str,
    version: Literal["library", "ingest"],
    file_type: recipes.DatasetType | None = None,
) -> None:
    if not file_type:
        if version == "library":
            file_type = recipes.DatasetType.pg_dump
        else:
            file_type = recipes.DatasetType.parquet

    target_table = f"{_dataset_label(dataset)}_{version}"

    client = postgres.PostgresClient(schema=SCHEMA, database=DATABASE)
    client.drop_table(dataset)
    client.drop_table(target_table)

    data_loader.load_dataset_into_pg(
        _input_dataset(dataset, version, file_type, target_table),
        client,
        LIBRARY_PATH / dataset / version / f"{dataset}.{file_type.to_extension()}",
    )


def load_recipe_from_s3(
    ds_id: str,
    s3_version: str,
    validation_version: Literal["library", "ingest"],
    bucket: str | None = None,
    library_ds_type: recipes.DatasetType = recipes.DatasetType.pg_dump,
):
    if bucket:
        os.environ["RECIPES_BUCKET"] = bucket
        importlib.reload(configuration)
    target_table = f"{ds_id}_{validation_version}"

    if validation_version == "library":
        file_type = library_ds_type
    else:
        file_type = recipes.DatasetType.parquet

    client = postgres.PostgresClient(schema=SCHEMA, database=DATABASE)
    client.drop_table(ds_id)
    client.drop_table(target_table)

    # often use case would be archiving to dev bucket multiple times
    # just ensure that local copy is not re-used
    with TemporaryDirectory() as _dir:
        data_loader.import_dataset(
            _input_dataset(ds_id, s3_version, file_type, target_table),
            pg_client=client,
            stage="builds.qa",
            dest_dir_override=Path(_dir),
        )


def compare_recipes_in_postgres(
    dataset: str,
    left_version: str = "library",
    right_version: str = "ingest",
    *,
    key_columns: list[str] | None = None,
    ignore_columns: list[str] | None = None,
    columns_only_comparison: bool = False,
    cast_to_numeric: list[str] | None = None,
) -> comparison.SqlReport:
    dataset = _dataset_label(dataset)
    ignore_columns = ignore_columns or []
    ignore_columns.append("ogc_fid")
    ignore_columns.append("data_library_version")

    client = postgres.PostgresClient(schema=SCHEMA, database="sandbox")
    left_table = dataset + "_" + left_version
    right_table = dataset + "_" + right_version

    return compare.get_sql_report(
        left_table,
        right_table,
        client,
        key_columns=key_columns,
        ignore_columns=ignore_columns,
        columns_only_comparison=columns_only_comparison,
        cast_to_numeric=cast_to_numeric,
    )


def _library_format_to_recipe_file_type(library_format: str) -> recipes.DatasetType:
    match library_format:
        case "pgdump":
            return recipes.DatasetType.pg_dump
        case "parquet":
            return recipes.DatasetType.parquet
        case "csv":
            return recipes.DatasetType.csv
        case _:
            raise ValueError(f"Unsupport library format '{library_format}'")


app = typer.Typer()


@app.command("convert")
def _convert(dataset: str = typer.Argument()):
    convert_template(dataset)


@app.command("run_single")
def run_single(
    tool: str = typer.Argument(),
    dataset: str = typer.Argument(),
    version: str | None = typer.Option(None, "--version", "-v"),
    library_format: str = typer.Option(
        "pgdump", "--library-format", "--lf", help="one of 'pgdump', 'csv'`, 'parquet'"
    ),
):
    if tool == "library":
        library_archive(dataset, version, file_type=library_format)
        load_recipe(dataset, tool, _library_format_to_recipe_file_type(library_format))  # type: ignore
    elif tool == "ingest":
        ingest(dataset, version)
        load_recipe(dataset, tool)  # type: ignore
    else:
        raise NotImplementedError("'tool' must be either 'library' or 'ingest'")


@app.command("run")
def _run_both(
    dataset: str = typer.Argument(),
    version: str = typer.Option(None, "--version", "-v"),
    library_format: str = typer.Option(
        "pgdump", "--library-format", "--lf", help="one of 'pgdump', 'csv'`, 'parquet'"
    ),
):
    ingest(dataset, version)
    library_archive(dataset, version, file_type=library_format)

    load_recipe(dataset, "library", _library_format_to_recipe_file_type(library_format))
    load_recipe(dataset, "ingest")


@app.command("compare")
def _compare(
    dataset: str = typer.Argument(),
    key_columns: list[str] = typer.Option(None, "-k", "--key"),
    ignore_columns: list[str] = typer.Option(None, "-i", "--ignore"),
    columns_only_comparison: bool = typer.Option(False, "--columns-only", "-c"),
    cast_to_numeric_columns: list[str] = typer.Option(
        None, "--cast-to-numeric", "--c2n"
    ),
):
    report = compare_recipes_in_postgres(
        dataset,
        key_columns=key_columns,
        ignore_columns=ignore_columns,
        columns_only_comparison=columns_only_comparison,
        cast_to_numeric=cast_to_numeric_columns,
    )
    print(indented_report(report.model_dump(), include_line_breaks=True))


@app.command("run_and_compare")
def _run_and_compare(
    dataset: str = typer.Argument(),
    version: str | None = typer.Option(None, "--version", "-v"),
    key_columns: list[str] = typer.Option(None, "-k", "--key"),
    ignore_columns: list[str] = typer.Option(None, "-i", "--ignore"),
    columns_only_comparison: bool = typer.Option(False, "--columns-only", "-c"),
    cast_to_numeric_columns: list[str] = typer.Option(
        None, "--cast-to-numeric", "--c2n"
    ),
    library_format: str = typer.Option(
        "pgdump", "--library-format", "--lf", help="one of 'pgdump', 'csv'`, 'parquet'"
    ),
):
    ingest(dataset, version)
    library_archive(dataset, version, file_type=library_format)

    load_recipe(dataset, "library", _library_format_to_recipe_file_type(library_format))
    load_recipe(dataset, "ingest")
    report = compare_recipes_in_postgres(
        dataset,
        key_columns=key_columns,
        ignore_columns=ignore_columns,
        columns_only_comparison=columns_only_comparison,
        cast_to_numeric=cast_to_numeric_columns,
    )
    print(indented_report(report.model_dump(), include_line_breaks=True))


@app.command("get_columns")
def _get_columns(
    dataset: str = typer.Argument(),
):
    client = postgres.PostgresClient(schema=SCHEMA, database="sandbox")

    res = client.execute_select_query(f"""
        with setup(schema, table_name) as (select '{SCHEMA}', '{dataset}'),
        all_rows as (
        select
            '- id: ' || column_name as t,
            ordinal_position as n
        from information_schema."columns" c
        inner join setup s
            on c.table_name = s.table_name || '_ingest'
            and c.table_schema = s.schema
        where column_name not in ('ogc_fid', 'data_library_version')
        union all
        select
            '  data_type: ' || (
                CASE
                    WHEN data_type = 'USER-DEFINED' THEN udt_name
                    when data_type = 'bigint' then 'integer'
                    when data_type = 'smallint' then 'integer'
                    when data_type = 'double precision' then 'decimal'
                    when data_type = 'timestamp with time zone' then 'datetime'
                    when data_type = 'timestamp without time zone' then 'datetime'
                    ELSE data_type
                END
            ) as t,
            ordinal_position + 0.5 as n
        from information_schema."columns" c
        inner join setup s
            on c.table_name = s.table_name || '_ingest'
            and c.table_schema = s.schema
        where column_name not in ('ogc_fid', 'data_library_version')
        )
        select t from all_rows order by n
    """)

    for _, row in res.iterrows():
        print(row["t"])


@app.command("load_from_s3")
def _load_from_s3(
    ds_id: str = typer.Argument(),
    s3_version: str = typer.Argument(help="the version in S3 to load"),
    validation_version: str = typer.Argument(
        help="which tool the ds was archived with"
    ),
    bucket: str = typer.Option(None, "--bucket", "-b", help="S3 bucket to use"),
):
    """Load a dataset version from S3 into the validation schema

    Note that this does not run ingest or library - it just loads the specified version from S3
    into the validation schema as either the 'library' or 'ingest' version.

    This is useful if you have a dataset version in S3 that you want to validate against a new
    ingest or library run.

    Example:

        python -m dcpy lifecycle scripts validate_ingest load_from_s3 \
            nyc_parking_permits 20250101 library --bucket de-dev-f
    """
    load_recipe_from_s3(ds_id, s3_version, validation_version, bucket)  # type: ignore
