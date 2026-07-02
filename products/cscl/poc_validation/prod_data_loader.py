"""
This script is for parsing a lion.dat file into its individual fields
and loading the result into a postgres table

This is done ad-hoc and not on an operational basis

It assumes that production outputs are specified as exports in recipe.yml
"""

import subprocess
import tempfile
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd
import typer

from dcpy.lifecycle.builds import plan
from dcpy.utils import postgres, s3

CLIENT = postgres.PostgresClient(database="db-cscl", schema="production_outputs")
LOAD_FOLDER = Path(".data/prod")

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
        if export.name == "log":
            continue
        if export.format.value not in ["dat", "csv"]:
            continue
        formatting = (export.custom or {}).get("formatting")
        assert export.filename, "filename is required for export datasets"
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


def load_production_lion_fgdb_layers(version: str):
    """
    Downloads LION FGDB from DCP website and loads all layers into production_outputs schema.

    Args:
        version: Version string (e.g., "26b") to construct the download URL

    The function will:
    1. Download nyclion_{version}.zip from DCP website
    2. Extract the FileGDB
    3. Load all layers into db-cscl.production_outputs schema with "fgdb_" prefix
    4. Does NOT drop the schema (appends/replaces tables only)
    """
    url = f"https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/lion/nyclion_{version}.zip"

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        zip_path = tmpdir_path / f"nyclion_{version}.zip"

        # Download the zip file
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, zip_path)

        # Extract the zip file
        print(f"Extracting {zip_path}...")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(tmpdir_path)

        # Find the .gdb directory
        gdb_dirs = list(tmpdir_path.glob("**/*.gdb"))
        if not gdb_dirs:
            raise Exception(f"No .gdb directory found in {zip_path}")

        gdb_path = gdb_dirs[0]
        print(f"Found FileGDB at {gdb_path}")

        # List all layers in the FGDB
        result = subprocess.run(
            ["ogrinfo", "-q", str(gdb_path)], capture_output=True, text=True, check=True
        )

        # Parse layer names from ogrinfo output
        layers = []
        for line in result.stdout.split("\n"):
            if line.startswith("1:") or line.startswith(" "):
                continue
            if ":" in line and not line.startswith("INFO"):
                # Format is typically "N: layer_name (geometry_type)"
                layer_name = line.split(":")[1].strip().split(" ")[0]
                layers.append(layer_name)

        print(f"Found {len(layers)} layers: {layers}")

        # Verify we have the expected layers
        expected_layers = ["node", "node_stname", "altnames", "lion"]
        assert set(layers) == set(expected_layers), (
            f"Expected layers {expected_layers} but found {layers}"
        )

        # Get connection info from CLIENT
        # Build the PG connection string for ogr2ogr
        engine_url = CLIENT.engine.url
        host = engine_url.host
        port = engine_url.port or 5432
        db_name = engine_url.database
        user = engine_url.username
        password = engine_url.password

        pg_connection = f"PG:host={host} port={port} dbname={db_name} user={user} password={password} active_schema={CLIENT.schema}"

        # Load each layer with fgdb_ prefix
        for layer in layers:
            table_name = f"fgdb_{layer.lower()}"
            print(f"Loading {layer} -> {CLIENT.schema}.{table_name}")

            # Use ogr2ogr to load the layer
            # -overwrite: replace table if it exists
            # -nln: set the new layer name (table name)
            # -nlt GEOMETRY: Use generic geometry type to handle all geometry types
            # --config PG_USE_COPY NO: Disable COPY mode to allow geometry conversions
            subprocess.run(
                [
                    "ogr2ogr",
                    "-f",
                    "PostgreSQL",
                    pg_connection,
                    str(gdb_path),
                    layer,
                    "-nln",
                    table_name,
                    "-nlt",
                    "GEOMETRY",
                    "-overwrite",
                    "-progress",
                    "--config",
                    "PG_USE_COPY",
                    "NO",
                ],
                check=True,
            )

        print(f"Successfully loaded {len(layers)} layers from LION {version}")


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
    Primary purpose is to load production outputs for comparison to outputs of this pipeline.
    """
    if not (version or local):
        raise Exception(
            "Either specify loading locally with '-l' flag or specify version to pull from s3 with '-v' flag"
        )

    if not local:
        for dataset in datasets:
            file_name = datasets_by_name[dataset].file_name
            s3.download_file(
                "edm-private",
                f"cscl_etl/{version}/{file_name}",
                local_folder / file_name,
            )

    load_datasets(datasets, local_folder)

    boro_level_files = {"lion", "face_code"}
    assert False, (
        "Hello, this is Alex from the past with a message for you. You NEED to fix the citywide table code for the flatfiles next time you load."
    )
    for file in boro_level_files:
        if any(dataset.endswith(f"_{file}") for dataset in datasets):
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
