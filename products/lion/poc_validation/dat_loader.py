"""
This script is for parsing a lion.dat file into its individual fields
and loading the result into a postgres table

This is done ad-hoc and not on an operational basis
"""

import pandas as pd
from pathlib import Path
import typer

from dcpy.utils import postgres

DAT_FORMATTING_FILE = Path("seeds/lion_dat_formatting.csv")


def parse_dat(dat_file: Path, max_records: int | None = None) -> pd.DataFrame:
    formatting = pd.read_csv(DAT_FORMATTING_FILE)

    records = []
    with open(dat_file) as f:
        i = 0
        for row in f:
            if max_records and i > max_records:
                break
            record = {}
            for j, field in formatting.iterrows():
                label = field["field_label"]
                start_index = field["start_index"] - 1
                end_index = field["end_index"]
                record[label] = row[start_index:end_index]
            records.append(record)
            i += 1

    return pd.DataFrame(records)


def load_dat(dat_file: Path, table_name: str | None, schema: str | None = None) -> None:
    dat_df = parse_dat(dat_file)
    table_name = table_name or dat_file.stem
    client = postgres.PostgresClient(database="db-lion", schema=schema)
    client.insert_dataframe(dat_df, table_name)


app = typer.Typer()


@app.command()
def head(
    filepath: Path = typer.Argument(),
    save_to_csv: bool = typer.Option(False, "-c"),
):
    df = parse_dat(filepath, 10)
    if save_to_csv:
        df.to_csv(filepath.stem + ".csv")
    typer.echo(df.head())


@app.command("load")
def _load_dat(
    filepath: Path = typer.Argument(),
    table_name: str | None = typer.Option(None, "--table", "-t"),
    schema: str = typer.Option("production_outputs", "--schema", "-s"),
):
    load_dat(filepath, table_name, schema)


@app.command("load_all")
def load_all(
    folderpath: Path = typer.Argument(),
    schema: str = typer.Option("production_outputs", "--schema", "-s"),
    suffix: str = typer.Option("", "--suffix"),
):
    boros = ["Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"]
    for boro in boros:
        file = boro.replace(" ", "") + "LION.dat"
        table = boro.lower().replace(" ", "_") + "_lion" + suffix
        load_dat(folderpath / file, table, "production_outputs", schema=schema)


if __name__ == "__main__":
    app()
