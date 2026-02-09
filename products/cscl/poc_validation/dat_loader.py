"""
This script is for parsing a lion.dat file into its individual fields
and loading the result into a postgres table

This is done ad-hoc and not on an operational basis
"""

from pathlib import Path

import pandas as pd
import typer

from dcpy.lifecycle.builds import plan
from dcpy.utils import postgres, s3

CLIENT = postgres.PostgresClient(database="db-cscl", schema="production_outputs")
LOAD_FOLDER = Path("prod")
DATASETS_BY_NAME = {}
DATASETS_BY_FILENAME = {}

try:
    recipe = plan.recipe_from_yaml(Path("./recipe.yml"))
    assert recipe.exports

    for export in recipe.exports.datasets:
        formatting = (export.custom or {"formatting": export.name})["formatting"]
        f_path = Path(f"seeds/text_formatting/text_formatting__{formatting}.csv")
        dataset = {
            "name": export.name,
            "file_name": export.filename,
            "formatting_path": f_path,
        }
        DATASETS_BY_FILENAME[export.filename] = dataset
        DATASETS_BY_NAME[export.name] = dataset
except Exception:
    raise Exception("Error reading recipe.yml")


def parse_file(
    file_path: Path, max_records: int | None = None, formatting_path: Path | None = None
) -> pd.DataFrame:
    formatting_path = (
        formatting_path or DATASETS_BY_FILENAME[file_path.name]["formatting_path"]
    )
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
        dataset = DATASETS_BY_NAME[ds_name]
        dat_df = parse_file(
            folder / dataset["file_name"], formatting_path=dataset["formatting_path"]
        )
        print(
            f"loading {folder / dataset['file_name']} to {CLIENT.schema}.{dataset['name']}"
        )
        CLIENT.insert_dataframe(dat_df, dataset["name"])


def create_citywide_lion():
    CLIENT.execute_query(
        """
            DROP TABLE IF EXISTS citywide_lion;
            CREATE TABLE citywide_lion AS
            SELECT * FROM bronx_lion
            UNION ALL
            SELECT * FROM brooklyn_lion
            UNION ALL
            SELECT * FROM manhattan_lion
            UNION ALL
            SELECT * FROM queens_lion
            UNION ALL
            SELECT * FROM staten_island_lion;
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
    dataset = DATASETS_BY_FILENAME[file_name]
    dat_df = parse_file(folder / file_name, dataset["formatting_path"])
    CLIENT.insert_dataframe(dat_df, table_name or dataset["name"])


@app.command("load")
def _load(
    datasets: list[str] = typer.Option(DATASETS_BY_NAME.keys(), "--datasets", "-d"),
    version: str | None = typer.Option(None, "--version", "-v"),
    local: bool = typer.Option(False, "--local", "-l"),
    local_folder: Path = typer.Option(LOAD_FOLDER, "--folder", "-f"),
):
    """
    Primary purpose is to load production outputs for comparison to outputs of this pipeline
    """
    if not version or local:
        raise Exception(
            "Either specify loading locally with '-l' flag or specify version to pull from s3 with '-v' flag"
        )

    for dataset in datasets:
        file_name = DATASETS_BY_NAME[dataset]["file_name"]
        s3.download_file(
            "edm-private", f"cscl_etl/{version}/{file_name}", local_folder / file_name
        )

    load_datasets(datasets, local_folder)

    boros = {
        "bronx_lion",
        "brooklyn_lion",
        "manhattan_lion",
        "queens_lion",
        "staten_island_island",
    }
    if boros.issubset(set(datasets)):
        create_citywide_lion()


if __name__ == "__main__":
    app()
