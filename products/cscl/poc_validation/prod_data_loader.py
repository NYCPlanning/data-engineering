"""
The legacy pipeline's production outputs: pulling them from edm-private (`pull`, used by
the QA stage's file comparisons) and parsing them into tables, which
build_prod_snapshot.py writes into the production_outputs parquet bundle.

It assumes that production outputs are specified as exports in recipe.yml.
"""

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

import exports
import pandas as pd
import typer
from paths import prod_files_dir

from dcpy.lifecycle.builds import plan
from dcpy.utils import s3

LOAD_FOLDER = prod_files_dir()

version: str | None = None
datasets_by_name = {}
datasets_by_filename = {}


class Sink(Protocol):
    """Where parsed tables go (build_prod_snapshot.py's ParquetSink)."""

    def insert_dataframe(self, df: pd.DataFrame, table_name: str) -> None: ...


@dataclass
class OutputDataset:
    name: str
    file_name: str
    file_format: Literal["dat", "csv"]
    formatting_path: Path | None = None
    compare_file: bool = True


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
            compare_file=exports.compares_to_prod(export),
        )
        datasets_by_filename[export.filename] = dataset
        datasets_by_name[export.name] = dataset
except Exception:
    raise Exception("Error reading recipe.yml")


def parse_file(
    file_path: Path, formatting_path: Path, max_records: int | None = None
) -> pd.DataFrame:
    formatting = pd.read_csv(formatting_path)
    # (label, start, end) computed once: iterating the layout per row is far too slow
    # for the citywide LION (214k rows x 114 fields)
    slices = [
        (field["field_name"], field["start_index"] - 1, field["end_index"])
        for _, field in formatting.iterrows()
    ]

    records = []
    with open(file_path) as f:
        i = 0
        for row in f:
            # GR's tooling terminates at least one file (nyc.thinlion) with a trailing
            # DOS-era Ctrl-Z (0x1A) EOF marker and no newline after it - plain line
            # iteration yields that as one more "row", producing a spurious all-blank
            # record with only the marker byte sliced into the first field (confirmed:
            # qa__diffs_thinlion_summary's thinlion_all "only_in_legacy" comparison_id
            # \x1A000000). Skip
            # any line that's empty once stripped of whitespace and that marker.
            if row.strip("\x1a\r\n ") == "":
                continue
            if max_records and i > max_records:
                break
            records.append({label: row[start:end] for label, start, end in slices})
            i += 1

    return pd.DataFrame(records)


def load_datasets(sink: Sink, datasets: list[str], folder: Path):
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

        print(f"loading {folder / dataset.file_name} to {dataset.name}")
        sink.insert_dataframe(dat_df, dataset.name)


def load_flat_files(sink: Sink, version: str, local_folder: Path) -> list[str]:
    """Download this release's flat files from edm-private and load each one; returns
    the datasets loaded."""
    # compare_file: false datasets (e.g. ldf_base/ldf_header) have no prod file to load -
    # their `filename` is dev's own export name, not something GR archives. `_pull`
    # skips these too.
    datasets = [d for d in datasets_by_name if datasets_by_name[d].compare_file]
    for dataset in set(datasets_by_name) - set(datasets):
        print(
            f"skipping {datasets_by_name[dataset].file_name}: compare_file is off in recipe.yml"
        )

    for dataset in datasets:
        file_name = datasets_by_name[dataset].file_name
        s3.download_file(
            "edm-private",
            f"cscl_etl/{version}/{file_name}",
            local_folder / file_name,
        )

    load_datasets(sink, datasets, local_folder)
    return datasets


def load_citywide_lion(sink: Sink, version: str, table_name: str, local_folder: Path):
    """
    Load one release's five borough LION .dat files as a single citywide table.

    The .dat files are parsed and concatenated here rather than unioned from the
    per-borough tables, so this works without `load_flat_files` having run first.
    """
    lion_datasets = [
        d for d in datasets_by_name.values() if d.name.endswith("_lion_dat")
    ]
    assert len(lion_datasets) == 5, (
        f"expected 5 borough LION exports in recipe.yml, found {len(lion_datasets)}"
    )

    frames = []
    for dataset in lion_datasets:
        local_path = local_folder / version / dataset.file_name
        s3.download_file(
            "edm-private",
            f"cscl_etl/{version}/{dataset.file_name}",
            local_path,
        )
        assert dataset.formatting_path, f"{dataset.name} has no formatting csv"
        frames.append(parse_file(local_path, formatting_path=dataset.formatting_path))

    df = pd.concat(frames, ignore_index=True)
    print(f"loading {len(df)} rows from LION {version} to {table_name}")
    sink.insert_dataframe(df, table_name)


def load_prod_ldf(sink: Sink, version: str, local_folder: Path):
    """
    Load the production LDF for this release, for dev/prod comparison.

    Records are kept as raw 100-character lines rather than parsed into fields: the LDF
    mixes record layouts in one file, and the QA models compare whole records anyway.
    """
    for file_name, table_name in [
        ("LDF.dat", "ldf_base"),
        ("LDF.header", "ldf_header"),
    ]:
        local_path = local_folder / version / file_name
        s3.download_file("edm-private", f"cscl_etl/{version}/{file_name}", local_path)
        with open(local_path) as f:
            rows = [line.rstrip("\n") for line in f if line.strip()]
        bad = {len(r) for r in rows} - {100}
        assert not bad, f"{file_name} has records of length {sorted(bad)}, expected 100"

        df = pd.DataFrame({"dat_column": rows})
        print(f"loading {len(df)} rows from {file_name} to {table_name}")
        sink.insert_dataframe(df, table_name)


def _district_gdb_filename(version: str) -> str:
    """Prod's district gdb filename, e.g. "v26C_Districts.gdb.zip" for version "26c".

    Capitalization is inconsistent with everywhere else this script deals with
    `version`: the S3 folder segment stays lowercase (cscl_etl/26c/...) but the file
    itself only uppercases the trailing release letter - confirmed against the real
    object at cscl_etl/26c/v26C_Districts.gdb.zip, not a documented convention.
    """
    return f"v{version[:-1]}{version[-1].upper()}_Districts.gdb.zip"


def _frozen_district_layers() -> set[str]:
    """District gdb layers flagged `status: blocked` in seeds/lion_outputs.csv.

    These haven't been genuinely rebuilt in prod since 2009-2015, only re-exported
    (per their FileGDB item metadata) - still under separate
    investigation with another team as of this writing. Skipped entirely rather than
    loaded-and-ignored: comparing our output against a FileGDB object prod hasn't
    touched in over a decade wouldn't tell us anything about our own pipeline.
    """
    with Path("seeds/lion_outputs.csv").open(newline="") as f:
        return {
            row["filename"]
            for row in csv.DictReader(f)
            if row["output_group"] == "district_gdb" and row["status"] == "blocked"
        }


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
        output = datasets_by_name[dataset]
        if not output.compare_file:
            print(f"skipping {output.file_name}: compare_file is off in recipe.yml")
            continue
        s3.download_file(
            "edm-private",
            f"cscl_etl/{version}/{output.file_name}",
            local_folder / output.file_name,
        )


if __name__ == "__main__":
    app()
