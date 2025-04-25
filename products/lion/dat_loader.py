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
    postgres.PostgresClient(schema=schema).insert_dataframe(dat_df, table_name)


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
    schema: str | None = typer.Option(None, "--schema", "-s"),
):
    load_dat(filepath, table_name, schema)


if __name__ == "__main__":
    app()
