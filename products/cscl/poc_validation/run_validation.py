"""
Downloads a CSCL build's output files and the corresponding prod files from S3,
then runs diff validation locally — without needing to re-run the full build.

Run from the products/cscl directory:
    python poc_validation/run_validation.py --build <build_name>

The build's output files are downloaded to output/ and the prod files to .data/prod/.
Then validate_outputs.sh is run to generate the diffs in output/validation_output/.

The prod version is read from the build metadata by default; override with --prod-version.
"""

import subprocess
from pathlib import Path

import typer

from dcpy.configuration import PUBLISHING_BUCKET
from dcpy.connectors.edm import publishing
from dcpy.models.connectors.edm.publishing import BuildKey
from dcpy.utils import s3

PRODUCT = "db-cscl"
PROD_BUCKET = "edm-private"
OUTPUT_DIR = Path("output")
PROD_DIR = Path(".data/prod")

app = typer.Typer(add_completion=False)


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
    """Download build outputs and prod files from S3, then run diff validation locally."""
    build_key = BuildKey(PRODUCT, build)

    if prod_version is None:
        print(f"Reading prod version from build metadata for '{build}'...")
        prod_version = publishing.get_build_metadata(build_key).version
    print(f"Prod version: {prod_version}")

    # Files present in the build S3 folder that are not CSCL output files
    BUILD_METADATA_FILES = {
        "build_metadata.json",
        "log.csv",
        "output.zip",
        "recipe.lock.yml",
        "source_data_versions.csv",
    }

    bucket = PUBLISHING_BUCKET
    all_filenames = publishing.get_filenames(build_key)
    # Only top-level files (no subdirectory separator) are the build outputs
    output_filenames = {f for f in all_filenames if "/" not in f}
    cscl_output_filenames = output_filenames - BUILD_METADATA_FILES

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\nDownloading {len(cscl_output_filenames)} build output files to {OUTPUT_DIR}/...")
    for filename in sorted(cscl_output_filenames):
        s3.download_file(bucket, f"{build_key.path}/{filename}", OUTPUT_DIR / filename)

    PROD_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\nDownloading prod files (version {prod_version}) to {PROD_DIR}/...")
    for filename in sorted(cscl_output_filenames):
        s3.download_file(
            PROD_BUCKET,
            f"cscl_etl/{prod_version}/{filename}",
            PROD_DIR / filename,
        )

    print("\nRunning validation...")
    subprocess.run(
        ["bash", "poc_validation/validate_outputs.sh"],
        check=True,
    )


if __name__ == "__main__":
    app()
