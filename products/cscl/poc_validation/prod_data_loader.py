"""
The legacy pipeline's production outputs: pulling them from edm-private (`pull`, used by
the QA stage's file comparisons) and parsing them into tables, which
build_prod_snapshot.py writes into the production_outputs parquet bundle.

It assumes that production outputs are specified as exports in recipe.yml.
"""

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import exports
import pandas as pd
import typer
from paths import prod_files_dir

from dcpy.lifecycle.builds import plan
from dcpy.utils import s3

# Where parsed tables go: set by poc_validation/build_prod_snapshot.py (a parquet sink).
# Anything with insert_dataframe(df, table_name) and a .schema works.
CLIENT: Any = None
LOAD_FOLDER = prod_files_dir()
CONFIG_PATH = Path("seeds/config.csv")

version: str | None = None
datasets_by_name = {}
datasets_by_filename = {}


def read_config(path: Path = CONFIG_PATH) -> dict[str, str]:
    """seeds/config.csv's key/value rows, as a dict - see that seed's
    description in seeds.yml for what belongs there and why."""
    with path.open(newline="") as f:
        return {row["key"]: row["value"] for row in csv.DictReader(f)}


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
    previous_version = read_config().get("ldf_previous_version")
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
            # \x1A000000 - see Bug 015 in data_issues.md). Skip
            # any line that's empty once stripped of whitespace and that marker.
            if row.strip("\x1a\r\n ") == "":
                continue
            if max_records and i > max_records:
                break
            records.append({label: row[start:end] for label, start, end in slices})
            i += 1

    return pd.DataFrame(records)


def load_datasets(
    datasets: list[str],
    folder: Path,
    version: str | None = None,
    force: bool = False,
):
    """
    version is optional (a `-l/--local` load has none) - staleness tracking only
    applies when it's given, same as load_citywide_lion/load_previous_ldf_header/
    load_prod_ldf already do. Before this, `load` was the one loader with no
    already_loaded/record_load tracking at all, so a table loaded once could sit
    stale in the shared production_outputs schema indefinitely with no record of
    what version it actually holds - root-caused for SAF in
    Bug 007 in data_issues.md (item 5&6),
    and the likely explanation for a similar pattern seen on ThinFire.
    """
    for ds_name in datasets:
        dataset = datasets_by_name[ds_name]

        if version and not force and already_loaded(dataset.name, version):
            print(f"{dataset.name} already holds {version}, skipping")
            continue

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

        if version:
            record_load(dataset.name, version)


def already_loaded(table_name: str, version: str) -> bool:
    """Every snapshot is built from scratch; nothing is ever already loaded."""
    return False


def record_load(table_name: str, version: str) -> None:
    pass


def load_citywide_lion(
    version: str, table_name: str, local_folder: Path, force: bool = False
):
    """
    Load one release's five borough LION .dat files as a single citywide table.

    The .dat files are parsed and concatenated here rather than unioned from the
    per-borough tables, so this works without `load` having run first.
    """
    if not force and already_loaded(table_name, version):
        print(f"{table_name} already holds LION {version}, skipping")
        return

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
    print(f"loading {len(df)} rows from LION {version} to {CLIENT.schema}.{table_name}")
    CLIENT.insert_dataframe(df, table_name)
    record_load(table_name, version)


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
    (Bug 013 in data_issues.md) - still under separate
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


@app.command("load")
def _load(
    datasets: list[str] = typer.Option(datasets_by_name.keys(), "--datasets", "-d"),
    version: str | None = typer.Option(version, "--version", "-v"),
    local: bool = typer.Option(False, "--local", "-l"),
    local_folder: Path = typer.Option(LOAD_FOLDER, "--folder", "-f"),
    force: bool = typer.Option(
        False,
        "--force",
        help="Reload even if this version is already logged as loaded",
    ),
):
    """
    Primary purpose is to load production outputs for comparison to outputs of this pipeline.
    """
    if not (version or local):
        raise Exception(
            "Either specify loading locally with '-l' flag or specify version to pull from s3 with '-v' flag"
        )

    # compare_file: false datasets (e.g. ldf_base/ldf_header) have no prod file to load -
    # their `filename` is dev's own export name, not something GR archives. `_pull` already
    # skips these; mirror that here rather than 404ing on s3.
    comparable_datasets = [d for d in datasets if datasets_by_name[d].compare_file]
    for dataset in set(datasets) - set(comparable_datasets):
        print(
            f"skipping {datasets_by_name[dataset].file_name}: compare_file is off in recipe.yml"
        )

    if not local:
        for dataset in comparable_datasets:
            file_name = datasets_by_name[dataset].file_name
            s3.download_file(
                "edm-private",
                f"cscl_etl/{version}/{file_name}",
                local_folder / file_name,
            )

    load_datasets(comparable_datasets, local_folder, version=version, force=force)

    # Citywide LION is `load_prod_lion`, which parses the .dat files directly. A union of
    # the per-borough tables written above can't build it: they're named for their recipe
    # exports (bronx_lion_dat), not the bronx_lion that create_citywide_table expects.
    if any(dataset.endswith("_face_code") for dataset in comparable_datasets):
        create_citywide_table("face_code")


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


@app.command("load_previous_lion")
def _load_previous_lion(
    previous_version: str | None = typer.Option(
        previous_version, "--previous-version", "-p"
    ),
    local_folder: Path = typer.Option(LOAD_FOLDER, "--folder", "-f"),
    table_name: str = typer.Option("previous_citywide_lion_dat", "--table", "-t"),
    force: bool = typer.Option(False, "--force", "-F"),
):
    """
    Load the *prior* release's production LION into the build db.

    The LDF documents changes between two LION releases, so unlike the rest of this
    script - which loads the current release for dev/prod comparison - this pulls the
    release before it. Boroughs are concatenated because the LDF is citywide.
    """
    if not previous_version:
        raise Exception(
            "Specify the previous release with '-p'. "
            "If running in CI, this defaults to seeds/config.csv's ldf_previous_version"
        )

    load_citywide_lion(previous_version, table_name, local_folder, force)


@app.command("load_previous_ldf_header")
def _load_previous_ldf_header(
    previous_version: str | None = typer.Option(
        previous_version, "--previous-version", "-p"
    ),
    local_folder: Path = typer.Option(LOAD_FOLDER, "--folder", "-f"),
    table_name: str = typer.Option("previous_ldf_header", "--table", "-t"),
    force: bool = typer.Option(False, "--force", "-F"),
):
    """
    Load the *prior* LDF edition's header record.

    LDF record numbers are cumulative across editions forever, so a new edition has to
    start where the last one stopped. The prior header carries both numbers we need:
    its own cumulative number and its record count.
    """
    if not previous_version:
        raise Exception(
            "Specify the previous release with '-p'. "
            "If running in CI, this defaults to seeds/config.csv's ldf_previous_version"
        )

    if not force and already_loaded(table_name, previous_version):
        print(f"{table_name} already holds LDF {previous_version}, skipping")
        return

    file_name = "LDF.header"
    local_path = local_folder / previous_version / file_name
    s3.download_file(
        "edm-private", f"cscl_etl/{previous_version}/{file_name}", local_path
    )
    df = parse_file(
        local_path,
        formatting_path=Path("seeds/text_formatting/text_formatting__ldf_h.csv"),
    )
    assert len(df) == 1, f"expected exactly 1 LDF header record, found {len(df)}"

    print(f"loading LDF {previous_version} header to {CLIENT.schema}.{table_name}")
    CLIENT.insert_dataframe(df, table_name)
    record_load(table_name, previous_version)


@app.command("load_prod_ldf")
def _load_prod_ldf(
    version: str | None = typer.Option(version, "--version", "-v"),
    local_folder: Path = typer.Option(LOAD_FOLDER, "--folder", "-f"),
    force: bool = typer.Option(False, "--force", "-F"),
):
    """
    Load the production LDF for this release, for dev/prod comparison.

    Records are kept as raw 100-character lines rather than parsed into fields: the LDF
    mixes record layouts in one file, and the QA models compare whole records anyway.
    """
    if not version:
        raise Exception("Specify version with '-v'")

    for file_name, table_name in [
        ("LDF.dat", "ldf_base"),
        ("LDF.header", "ldf_header"),
    ]:
        if not force and already_loaded(table_name, version):
            print(f"{table_name} already holds LDF {version}, skipping")
            continue

        local_path = local_folder / version / file_name
        s3.download_file("edm-private", f"cscl_etl/{version}/{file_name}", local_path)
        with open(local_path) as f:
            rows = [line.rstrip("\n") for line in f if line.strip()]
        bad = {len(r) for r in rows} - {100}
        assert not bad, f"{file_name} has records of length {sorted(bad)}, expected 100"

        df = pd.DataFrame({"dat_column": rows})
        print(
            f"loading {len(df)} rows from {file_name} to {CLIENT.schema}.{table_name}"
        )
        CLIENT.insert_dataframe(df, table_name)
        record_load(table_name, version)


if __name__ == "__main__":
    app()
