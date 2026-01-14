import importlib

import typer

from dcpy.utils import postgres
from .utility.prepare import read_datasets_yml
from .utility.metadata import dump_metadata

from facdb import (
    SQL_PATH,
    BUILD_NAME,
    BUILD_ENGINE,
    CACHE_PATH,
)

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


if __name__ == "__main__":
    app()
