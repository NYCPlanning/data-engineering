import importlib
import os
from pathlib import Path
from typing import List, Optional

import typer

from dcpy.connectors import postgres
from .utility.prepare import read_datasets_yml
from .utility.metadata import dump_metadata

from facdb import SQL_PATH, BASH_PATH, BUILD_ENGINE, CACHE_PATH, VERSION_PREV

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
    postgres.exec_file_via_shell(BUILD_ENGINE, SQL_PATH / "_create_facdb_base.sql")
    postgres.exec_file_via_shell(BUILD_ENGINE, SQL_PATH / "_create_reference_tables.sql")
    postgres.exec_file_via_shell(BUILD_ENGINE, SQL_PATH / "_procedures.sql")
    postgres.exec_file_via_shell(BUILD_ENGINE, SQL_PATH / "_functions.sql")


@app.command()
def dataloading():
    """
    Load SQL dump datasets from data library e.g. dcp_mappluto_wi, doitt_buildingcentroids
    """
    os.system(BASH_PATH / f"dataloading.sh {VERSION_PREV}")


@app.command()
def build():
    """
    Building facdb based on facdb_base
    """
    postgres.exec_file_via_shell(BUILD_ENGINE, SQL_PATH / "_create_facdb_geom.sql")
    postgres.exec_file_via_shell(BUILD_ENGINE, SQL_PATH / "_create_facdb_address.sql")
    postgres.exec_file_via_shell(BUILD_ENGINE, SQL_PATH / "_create_facdb_spatial.sql")
    postgres.exec_file_via_shell(BUILD_ENGINE, SQL_PATH / "_create_facdb_boro.sql")
    postgres.exec_file_via_shell(BUILD_ENGINE, SQL_PATH / "_create_facdb_classification.sql")
    postgres.exec_file_via_shell(BUILD_ENGINE, SQL_PATH / "_create_facdb_agency.sql")
    postgres.exec_file_via_shell(BUILD_ENGINE, SQL_PATH / "_create_facdb.sql")
    postgres.exec_file_via_shell(BUILD_ENGINE, SQL_PATH / "_deduplication.sql")


@app.command()
def qaqc():
    """
    Running QAQC commands
    """
    postgres.exec_file_via_shell(BUILD_ENGINE, SQL_PATH / "_qaqc.sql")


@app.command()
def run(
    name: str = typer.Option(
        None,
        "--name",
        "-n",
        help="Name of the dataset",
        autocompletion=complete_dataset_name,
    ),
    python: bool = typer.Option(False, "--python", help="Execute python part only"),
    sql: bool = typer.Option(False, "--sql", help="Execute sql part only"),
    all_datasets: bool = typer.Option(
        None, "--all", help="Execute all datasets, both python and sql"
    ),
):
    """
    This function is used to execute the python portion of a pipeline,
    if there's a scripts section in datasets.yml under this dataset, the
    sql script will also be executed.
    facdb run -n {{ name }} to run both python and sql part\n
    facdb run -n {{ name }} --python to run the python part only\n
    facdb run -n {{ name }} --sql to run the sql part only\n
    facdb run --all\n
    """
    datasets = read_datasets_yml()
    dataset_names = (
        [name] if not all_datasets else [dataset["name"] for dataset in datasets]
    )
    if not python and not sql:
        # if both unspecified, we should
        # execute both python and sql
        python = True
        sql = True
    for name in dataset_names:
        dataset = next(filter(lambda x: x["name"] == name, datasets), None)
        scripts = dataset.get("scripts", None)
        if python:
            pipelines = importlib.import_module("facdb.pipelines")
            pipeline = getattr(pipelines, name)
            pipeline()

        if scripts and sql:
            for script in scripts:
                postgres.exec_file_via_shell(BUILD_ENGINE, Path(__file__).parent / "sql" / script)

        typer.echo(typer.style(f"SUCCESS: {name}", fg=typer.colors.GREEN))
    dump_metadata()


@app.command()
def sql(
    scripts: Optional[List[Path]] = typer.Option(
        None, "-f", help="SQL Scripts to execute"
    )
):
    """
    this command will execute any given sql script against the facdb database\n
    facdb sql -f path/to/file.sql\n
    facdb sql -f path/to/file1.sql -f path/to/file2.sql\n
    """
    if scripts:
        for script in scripts:
            postgres.exec_file_via_shell(script)


@app.command()
def clear(
    name: str = typer.Option(
        None,
        "--name",
        "-n",
        help="Name of the dataset",
        autocompletion=complete_dataset_name,
    ),
    all_datasets: bool = typer.Option(None, "--all", help="Execute all datasets"),
):
    """
    clear will clear the cached dataset created while reading a csv\n
    facdb clear -n {{ name }}\n
    facdb clear --all\n
    """
    files_for_removal = (
        [f"{name}.pkl"]
        if name and not all_datasets
        else [f for f in os.listdir(CACHE_PATH) if ".pkl" in f]
    )
    for f in files_for_removal:
        os.remove(CACHE_PATH / f)


if __name__ == "__main__":
    app()
