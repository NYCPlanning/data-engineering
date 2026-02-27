"""
This script is for parsing a lion.dat file into its individual fields
and loading the result into a postgres table

This is done ad-hoc and not on an operational basis
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd
import typer

from dcpy.lifecycle.builds import plan
from dcpy.utils import postgres, s3

CLIENT = postgres.PostgresClient(database="db-cscl", schema="production_outputs")
LOAD_FOLDER = Path("prod")

version: str | None = None
datasets_by_name = {}
datasets_by_filename = {}


@dataclass
class OutputDataset:
    name: str
    file_name: str
    file_format: Literal["dat", "csv"]
    formatting_path: Path | None = None


try:
    recipe = plan.recipe_from_yaml(Path("./recipe.yml"))
    version = recipe.version
    assert recipe.exports

    for export in recipe.exports.datasets:
        formatting = (export.custom or {}).get("formatting")
        assert export.filename, "filename is required for export datasets"
        assert export.format.value in ["dat", "csv"], "unsupported file format"
        f_path = (
            Path(f"seeds/text_formatting/text_formatting__{formatting}.csv")
            if formatting
            else None
        )
        dataset = OutputDataset(
            name=export.name,
            file_name=export.filename,
            file_format=export.format.value,  # type: ignore
            formatting_path=f_path,
        )
        datasets_by_filename[export.filename] = dataset
        datasets_by_name[export.name] = dataset
except Exception:
    raise Exception("Error reading recipe.yml")


def parse_file(
    file_path: Path, formatting_path: Path, max_records: int | None = None
) -> pd.DataFrame:
    formatting = pd.read_csv(formatting_path)

    records = []
    with open(file_path) as f:
        i = 0
        for row in f:
            if max_records and i > max_records:
                break
            record = {}
            for _, field in formatting.iterrows():
                label = field["field_name"]
                start_index = field["start_index"] - 1
                end_index = field["end_index"]
                record[label] = row[start_index:end_index]
            records.append(record)
            i += 1

    return pd.DataFrame(records)


def load_datasets(datasets: list[str], folder: Path):
    for ds_name in datasets:
        dataset = datasets_by_name[ds_name]
        match dataset.file_format, dataset.formatting_path:
            case "csv", _:
                dat_df = pd.read_csv(folder / dataset.file_name, dtype=str, header=None)
            case "dat", None:
                dat_df = pd.read_csv(
                    folder / dataset.file_name, header=None, names=["dat_column"]
                )
            case "dat", formatting_path:
                dat_df = parse_file(
                    folder / dataset.file_name, formatting_path=formatting_path
                )
            case _:
                raise Exception(f"unsupported file format {dataset.file_format}")

        print(f"loading {folder / dataset.file_name} to {CLIENT.schema}.{dataset.name}")
        CLIENT.insert_dataframe(dat_df, dataset.name)


def create_citywide_table(file: str):
    CLIENT.execute_query(
        f"""
            DROP TABLE IF EXISTS citywide_{file};
            CREATE TABLE citywide_{file} AS
            SELECT * FROM bronx_{file}
            UNION ALL
            SELECT * FROM brooklyn_{file}
            UNION ALL
            SELECT * FROM manhattan_{file}
            UNION ALL
            SELECT * FROM queens_{file}
            UNION ALL
            SELECT * FROM staten_island_{file};
        """
    )


app = typer.Typer()


@app.command()
def head(
    file_name: str = typer.Argument(),
    save_to_csv: bool = typer.Option(False, "-c"),
    folder: Path = typer.Option(LOAD_FOLDER, "--folder", "-f"),
):
    """Just useful to make sure that the formatting is being parsed correctly"""
    df = parse_file(folder / file_name, 10)
    if save_to_csv:
        df.to_csv(Path(file_name).stem + ".csv")
    typer.echo(df.head())


@app.command("load_single")
def load_single(
    file_name: str = typer.Argument(),
    folder: Path = typer.Option(LOAD_FOLDER, "--folder", "-f"),
    table_name: str | None = typer.Option(None, "--table", "-t"),
):
    dataset = datasets_by_filename[file_name]
    dat_df = parse_file(folder / file_name, dataset["formatting_path"])
    CLIENT.insert_dataframe(dat_df, table_name or dataset["name"])


@app.command("load")
def _load(
    datasets: list[str] = typer.Option(datasets_by_name.keys(), "--datasets", "-d"),
    version: str | None = typer.Option(version, "--version", "-v"),
    local: bool = typer.Option(False, "--local", "-l"),
    local_folder: Path = typer.Option(LOAD_FOLDER, "--folder", "-f"),
):
    """
    Primary purpose is to load production outputs for comparison to outputs of this pipeline
    """
    if not (version or local):
        raise Exception(
            "Either specify loading locally with '-l' flag or specify version to pull from s3 with '-v' flag"
        )

    if not local:
        for dataset in datasets:
            file_name = datasets_by_name[dataset].file_name
            s3.download_file(
                "edm-private", f"cscl_etl/{version}/{file_name}", local_folder / file_name
            )

    load_datasets(datasets, local_folder)

    boro_level_files = {"lion", "face_code"}
    for file in boro_level_files:
        if any(f"_{file}" in dataset for dataset in datasets):
            create_citywide_table(file)


@app.command("pull")
def _pull(
    datasets: list[str] = typer.Option(datasets_by_name.keys(), "--datasets", "-d"),
    version: str | None = typer.Option(version, "--version", "-v"),
    local_folder: Path = typer.Option(LOAD_FOLDER, "--folder", "-f"),
):
    """
    Primary purpose is to load production outputs for comparison to outputs of this pipeline
    """
    if not version:
        raise Exception(
            "Specify version to pull from s3 with '-v' flag. "
            "If running in CI, this defaults to hardcoded version in recipe"
        )

    for dataset in datasets:
        file_name = datasets_by_name[dataset].file_name
        s3.download_file(
            "edm-private", f"cscl_etl/{version}/{file_name}", local_folder / file_name
        )


if __name__ == "__main__":
    app()
