"""
Streams CSCL build outputs and production files from S3 and computes diffs — without
downloading input files to disk or re-running the full build.

Run from the products/cscl directory:
    python poc_validation/run_validation.py --build <build_name>

Diff results are written to output/validation_output/ (one file per CSCL output file)
along with a validation_summary.csv. No input files are written to disk.

The prod version is read from the build metadata by default; override with --prod-version.
"""

import csv
from pathlib import Path

import typer

from dcpy.connectors.edm import publishing
from dcpy.connectors.edm.models import BuildKey
from dcpy.utils import s3

PRODUCT = "db-cscl"
PROD_BUCKET = "edm-private"
VALIDATION_OUTPUT_DIR = Path("output/validation_output")

# Files present in the build S3 folder that are not CSCL output files
BUILD_METADATA_FILES = {
    "build_metadata.json",
    "log.csv",
    "output.zip",
    "recipe.lock.yml",
    "source_data_versions.csv",
}

app = typer.Typer(add_completion=False)


def _compute_file_diff(
    build_key: BuildKey, filename: str, prod_version: str
) -> tuple[list[str], int]:
    """Stream both files from S3 and return (diff_rows, prod_row_count).

    diff_rows contains lines present in the dev build but not in prod,
    equivalent to comm -23 <(sort dev) <(sort prod).
    """
    dev_buffer = publishing.get_file(build_key, filename)
    dev_lines = {
        line
        for line in dev_buffer.read().decode("latin-1").splitlines()
        if line.strip()
    }

    prod_buffer = s3.get_file_as_stream(
        PROD_BUCKET, f"cscl_etl/{prod_version}/{filename}"
    )
    prod_lines = {
        line
        for line in prod_buffer.read().decode("latin-1").splitlines()
        if line.strip()
    }

    return sorted(dev_lines - prod_lines), len(prod_lines)


@app.command()
def run(
    build: str = typer.Option(..., "--build", "-b", help="Build name, e.g. nightly_qa"),
    prod_version: str | None = typer.Option(
        None,
        "--prod-version",
        "-p",
        help="Prod CSCL version to compare against, e.g. 25a. Defaults to version in build metadata.",
    ),
) -> None:
    """Stream build outputs and prod files from S3 and compute diffs without using disk."""
    build_key = BuildKey(PRODUCT, build)

    if prod_version is None:
        print(f"Reading prod version from build metadata for '{build}'...")
        prod_version = publishing.get_build_metadata(build_key).version
    print(f"Prod version: {prod_version}")

    all_filenames = publishing.get_filenames(build_key)
    cscl_output_filenames = sorted(
        f for f in all_filenames if "/" not in f and f not in BUILD_METADATA_FILES
    )
    print(f"\nValidating {len(cscl_output_filenames)} files...")

    VALIDATION_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary_rows = []
    total_records = 0
    total_mismatched = 0

    for filename in cscl_output_filenames:
        print(f"Validating {filename}")
        diff_rows, prod_row_count = _compute_file_diff(
            build_key, filename, prod_version
        )
        n_mismatched = len(diff_rows)
        print(f"  Total records:      {prod_row_count}")
        print(f"  Mismatched records: {n_mismatched}")

        total_records += prod_row_count
        total_mismatched += n_mismatched

        (VALIDATION_OUTPUT_DIR / filename).write_text(
            "\n".join(diff_rows), encoding="latin-1"
        )
        summary_rows.append((filename, prod_row_count, n_mismatched))

    summary_path = VALIDATION_OUTPUT_DIR / "validation_summary.csv"
    with summary_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["filename", "prod_row_count", "mismatched_rows"])
        writer.writerows(summary_rows)

    print("\nComparison complete!")
    print(f"Total records:      {total_records}")
    print(f"Mismatched records: {total_mismatched}")
    print(f"Summary written to {summary_path}")


if __name__ == "__main__":
    app()
