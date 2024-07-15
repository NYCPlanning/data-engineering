import importlib
from pathlib import Path

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


def complete_dataset_name(incomplete: str) -> list:
    pipelines = importlib.import_module("facdb.pipelines")
    completion = []
    for name in dir(pipelines):
        if name.startswith(incomplete):
            completion.append(name)
    return completion


@app.command()
def init():
    """
    Initialize empty facdb_base table and create procedures and functions
    """
    postgres.execute_file_via_shell(BUILD_ENGINE, SQL_PATH / "_create_facdb_base.sql")
    postgres.execute_file_via_shell(
        BUILD_ENGINE, SQL_PATH / "_create_reference_tables.sql"
    )
    postgres.execute_file_via_shell(BUILD_ENGINE, SQL_PATH / "_procedures.sql")
    postgres.execute_file_via_shell(BUILD_ENGINE, SQL_PATH / "_functions.sql")


@app.command()
def build():
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


@app.command()
def qaqc():
    """
    Running QAQC commands
    """
    postgres.execute_file_via_shell(BUILD_ENGINE, SQL_PATH / "_qaqc.sql")


@app.command()
def run(
    name: str = typer.Option(
        None,
        "--name",
        "-n",
        help="Name of the dataset",
        autocompletion=complete_dataset_name,
    ),
):
    """ """
    datasets = read_datasets_yml()
    for name in [dataset["name"] for dataset in datasets]:
        dataset = next(filter(lambda x: x["name"] == name, datasets), None)
        scripts = dataset.get("scripts") if dataset is not None else []

        if scripts:
            for script in scripts:
                postgres.execute_file_via_shell(
                    BUILD_ENGINE, Path(__file__).parent / "sql" / script
                )

        typer.echo(typer.style(f"SUCCESS: {name}", fg=typer.colors.GREEN))
    dump_metadata()


@app.command()
def reformat_facdb():
    """Update columns and data types in facdb table."""
    postgres.execute_file_via_shell(
        BUILD_ENGINE,
        SQL_PATH / "_reformat_facdb.sql",
        build_schema=BUILD_NAME,
    )


if __name__ == "__main__":
    app()
