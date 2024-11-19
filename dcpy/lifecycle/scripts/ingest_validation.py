import os
from pathlib import Path
import shutil
import typer
from typing import Literal

from dcpy.utils import postgres
from dcpy.utils.collections import indented_report
from dcpy.models.data import comparison
from dcpy.data import compare
from dcpy.connectors.edm import recipes
from dcpy.lifecycle.ingest.run import TMP_DIR, run as run_ingest
from dcpy.lifecycle.builds import metadata as build_metadata

DATABASE = "sandbox"
LIBRARY_PATH = recipes.LIBRARY_DEFAULT_PATH / "datasets"
SCHEMA = build_metadata.build_name(os.environ.get("BUILD_NAME"))


def library_archive(dataset: str, version: str | None = None, file_type="pgdump"):
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
    ingest_parent_dir: Path = TMP_DIR,
) -> None:
    ingest_dir = ingest_parent_dir / dataset / "staging"
    if ingest_dir.is_dir():
        shutil.rmtree(ingest_dir)
    run_ingest(dataset, version=version, staging_dir=ingest_dir, skip_archival=True)

    ingest_output_path = ingest_dir / f"{dataset}.parquet"
    ingest_path = LIBRARY_PATH / dataset / "ingest" / f"{dataset}.parquet"

    ingest_path.parent.mkdir(exist_ok=True, parents=True)
    shutil.copy(ingest_output_path, ingest_path)


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

    target_table = f"{dataset}_{version}"

    client = postgres.PostgresClient(schema=SCHEMA, database=DATABASE)
    client.drop_table(dataset)
    client.drop_table(target_table)

    left_ds = recipes.Dataset(id=dataset, version=version, file_type=file_type)
    recipes.import_dataset(
        left_ds,
        client,
        import_as=target_table,
    )


def compare_recipes_in_postgres(
    dataset: str,
    left_version: str = "library",
    right_version: str = "ingest",
    *,
    key_columns: list[str] | None = None,
    ignore_columns: list[str] | None = None,
    columns_only_comparison: bool = False,
) -> comparison.SqlReport:
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
    )


app = typer.Typer()


@app.command("run_single")
def run_single(
    tool: str = typer.Argument(),
    dataset: str = typer.Argument(),
    version: str | None = typer.Option(None, "--version", "-v"),
):
    if tool == "library":
        library_archive(dataset, version)
    elif tool == "ingest":
        ingest(dataset, version)
    else:
        raise NotImplementedError("'tool' must be either 'library' or 'ingest'")

    load_recipe(dataset, tool)  # type: ignore


@app.command("run")
def _run_both(
    dataset: str = typer.Argument(),
    version: str | None = typer.Option(None, "--version", "-v"),
):
    ingest(dataset, version)
    library_archive(dataset, version)

    load_recipe(dataset, "library")
    load_recipe(dataset, "ingest")


@app.command("compare")
def _compare(
    dataset: str = typer.Argument(),
    key_columns: list[str] = typer.Option(None, "-k", "--key"),
    ignore_columns: list[str] = typer.Option(None, "-i", "--ignore"),
    columns_only_comparison: bool = typer.Option(False, "--columns-only", "-c"),
):
    report = compare_recipes_in_postgres(
        dataset,
        key_columns=key_columns,
        ignore_columns=ignore_columns,
        columns_only_comparison=columns_only_comparison,
    )
    print(
        indented_report(
            report.model_dump(), pretty_print_fields=True, include_line_breaks=True
        )
    )


@app.command("run_and_compare")
def _run_and_compare(
    dataset: str = typer.Argument(),
    version: str | None = typer.Option(None, "--version", "-v"),
    key_columns: list[str] = typer.Option(None, "-k", "--key"),
    ignore_columns: list[str] = typer.Option(None, "-i", "--ignore"),
    columns_only_comparison: bool = typer.Option(False, "--columns-only", "-c"),
):
    ingest(dataset, version)
    library_archive(dataset, version)

    load_recipe(dataset, "library")
    load_recipe(dataset, "ingest")
    report = compare_recipes_in_postgres(
        dataset,
        key_columns=key_columns,
        ignore_columns=ignore_columns,
        columns_only_comparison=columns_only_comparison,
    )
    print(
        indented_report(
            report.model_dump(), pretty_print_fields=True, include_line_breaks=True
        )
    )
