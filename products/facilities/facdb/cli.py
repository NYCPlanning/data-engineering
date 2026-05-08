import concurrent.futures
import importlib
import shutil
import subprocess
import zipfile
from pathlib import Path

import typer

from dcpy.utils import postgres
from facdb import (
    BUILD_ENGINE,
    BUILD_NAME,
    CACHE_PATH,
    SQL_PATH,
)

from .utility.metadata import dump_metadata
from .utility.prepare import read_datasets_yml

if not CACHE_PATH.exists():
    CACHE_PATH.mkdir()

app = typer.Typer(add_completion=False)


def _autocomplete_dataset_name(incomplete: str) -> list:
    pipelines = importlib.import_module("facdb.pipelines")
    completion = []
    for name in dir(pipelines):
        if name.startswith(incomplete):
            completion.append(name)
    return completion


_CSV_EXPORTS = [
    ("facdb_export_csv", "facilities"),
    ("qc_operator", "qc_operator"),
    ("qc_oversight", "qc_oversight"),
    ("qc_classification", "qc_classification"),
    ("qc_captype", "qc_captype"),
    ("qc_mapped", "qc_mapped"),
    ("qc_diff", "qc_diff"),
    ("qc_recordcounts", "qc_recordcounts"),
    ("qc_subgrpbins", "qc_subgrpbins"),
]


def _export_csv(
    pg: postgres.PostgresClient, table: str, filename: str, output_dir: Path
) -> None:
    pg.export_to_csv(table, output_dir / f"{filename}.csv")


def _export_shp(pg: postgres.PostgresClient, output_dir: Path) -> None:
    shp_dir = output_dir / "facilities_shp"
    shp_dir.mkdir(exist_ok=True)
    subprocess.check_call(
        [
            "ogr2ogr",
            "-progress",
            "-f",
            "ESRI Shapefile",
            str(shp_dir / "facilities.shp"),
            f"PG:{BUILD_ENGINE}",
            f"{pg.schema}.facdb_export",
            "-nlt",
            "POINT",
            "-t_srs",
            "EPSG:2263",
        ]
    )
    with zipfile.ZipFile(
        output_dir / "facilities.shp.zip", "w", zipfile.ZIP_DEFLATED, compresslevel=9
    ) as zf:
        for f in shp_dir.iterdir():
            zf.write(f, f.name)
    shutil.rmtree(shp_dir)


def _export_fgdb(pg: postgres.PostgresClient, output_dir: Path) -> None:
    gdb_outer = output_dir / "facilities_fgdb"
    gdb_outer.mkdir(exist_ok=True)
    gdb_path = gdb_outer / "facilities.gdb"
    subprocess.check_call(
        [
            "ogr2ogr",
            "-progress",
            "-f",
            "OpenFileGDB",
            str(gdb_path),
            f"PG:{BUILD_ENGINE}",
            f"{pg.schema}.facdb_export",
            "-mapFieldType",
            "Integer64=Real",
            "-lco",
            "GEOMETRY_NAME=Shape",
            "-nln",
            "facdb",
            "-nlt",
            "POINT",
            "-t_srs",
            "EPSG:2263",
        ]
    )
    with zipfile.ZipFile(
        output_dir / "facilities.gdb.zip", "w", zipfile.ZIP_DEFLATED
    ) as zf:
        for f in gdb_path.rglob("*"):
            zf.write(f, f.relative_to(gdb_outer))
    shutil.rmtree(gdb_outer)


@app.command("init")
def _cli_init():
    """
    Initialize empty facdb_base table and create procedures and functions
    """
    postgres.execute_file_via_shell(BUILD_ENGINE, SQL_PATH / "_create_facdb_base.sql")
    postgres.execute_file_via_shell(
        BUILD_ENGINE, SQL_PATH / "_create_reference_tables.sql"
    )
    postgres.execute_file_via_shell(BUILD_ENGINE, SQL_PATH / "_procedures.sql")


@app.command("build")
def _cli_build():
    """
    Building facdb based on facdb_base
    """
    postgres.execute_file_via_shell(BUILD_ENGINE, SQL_PATH / "_create_facdb_geom.sql")
    postgres.execute_file_via_shell(
        BUILD_ENGINE, SQL_PATH / "_create_facdb_address.sql"
    )
    postgres.execute_file_via_shell(
        BUILD_ENGINE, SQL_PATH / "_create_facdb_spatial.sql"
    )
    postgres.execute_file_via_shell(BUILD_ENGINE, SQL_PATH / "_create_facdb_boro.sql")
    postgres.execute_file_via_shell(
        BUILD_ENGINE, SQL_PATH / "_create_facdb_classification.sql"
    )
    postgres.execute_file_via_shell(BUILD_ENGINE, SQL_PATH / "_create_facdb_agency.sql")
    postgres.execute_file_via_shell(
        BUILD_ENGINE,
        SQL_PATH / "_create_facdb.sql",
        build_schema=BUILD_NAME,
    )
    postgres.execute_file_via_shell(BUILD_ENGINE, SQL_PATH / "_deduplication.sql")


@app.command("qaqc")
def _cli_qaqc():
    """
    Running QAQC commands
    """
    postgres.execute_file_via_shell(BUILD_ENGINE, SQL_PATH / "_qaqc.sql")


@app.command("run_pipelines")
def _cli_run_pipelines(
    name: str = typer.Option(
        None,
        "--name",
        "-n",
        help="Name of the dataset",
        autocompletion=_autocomplete_dataset_name,
    ),
):
    """ """
    datasets = read_datasets_yml()
    dataset_names = [d["name"] for d in datasets]

    target_names = [name] if name else dataset_names
    if name and name not in dataset_names:
        typer.echo(
            typer.style(
                f"ERROR: dataset name {name} not found in datasets yml",
                fg=typer.colors.RED,
            )
        )
        raise typer.Exit(code=1)

    for dataset_name in target_names:
        dataset = next((d for d in datasets if d["name"] == dataset_name), None)
        scripts = dataset.get("scripts", []) if dataset is not None else []

        for script in scripts:
            postgres.execute_file_via_shell(
                BUILD_ENGINE, SQL_PATH / "pipelines" / script
            )

        typer.echo(typer.style(f"SUCCESS: {dataset_name}", fg=typer.colors.GREEN))
    dump_metadata()


@app.command("reformat_facdb")
def _cli_reformat_facdb():
    """Update columns and data types in facdb table."""
    postgres.execute_file_via_shell(
        BUILD_ENGINE,
        SQL_PATH / "_reformat_facdb.sql",
        build_schema=BUILD_NAME,
    )


@app.command("export")
def _cli_export():
    """Export facdb tables and geospatial files to the output directory."""
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    shutil.copy("facdb/metadata.yml", output_dir / "metadata.yml")
    shutil.copy("build_metadata.json", output_dir / "build_metadata.json")
    shutil.copy("source_data_versions.csv", output_dir / "source_data_versions.csv")

    pg = postgres.PostgresClient()

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(_export_csv, pg, table, filename, output_dir)
            for table, filename in _CSV_EXPORTS
        ]
        futures.append(executor.submit(_export_shp, pg, output_dir))
        futures.append(executor.submit(_export_fgdb, pg, output_dir))
        for future in concurrent.futures.as_completed(futures):
            future.result()

    typer.echo(typer.style("SUCCESS: export complete", fg=typer.colors.GREEN))


if __name__ == "__main__":
    app()
