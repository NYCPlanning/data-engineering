"""Download the pinned source files into data/sources/.

Reads S3 credentials from the same env vars as dcpy (AWS_S3_ENDPOINT, AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY), which the repo's root .env provides through direnv.

    python scripts/download.py          # skip files already downloaded
    python scripts/download.py --force  # download everything again
"""

import argparse
import os
from pathlib import Path

import boto3

SOURCES_DIR = Path(__file__).parent.parent / "data" / "sources"

# local filename -> (bucket, key). Bump a version here to move the analysis to a new release.
SOURCES = {
    "pff_decennial_2010.csv": (
        "edm-publishing",
        "db-factfinder/publish/decennial/2010/2026/decennial.csv",
    ),
    "pff_decennial_2020.csv": (
        "edm-publishing",
        "db-factfinder/publish/decennial/2020/2026/decennial.csv",
    ),
    "pff_acs_2024.csv": (
        "edm-publishing",
        "db-factfinder/publish/acs/2024/2026/acs.csv",
    ),
    "kpdb_cpp_housing_growth_cd.csv": (
        "edm-publishing",
        "db-kpdb/publish/2026-10-01/cpp_housing_growth/cpp_housing_growth_cd.csv",
    ),
    "kpdb_longform_cd_output.csv": (
        "edm-publishing",
        "db-kpdb/publish/2026-10-01/aggregation/longform_cd_output.csv",
    ),
    # the same Housing Database version KPDB 2026-10-01 was built on. Move them together.
    "dcp_housing.parquet": (
        "edm-recipes",
        "datasets/dcp_housing/24Q4/dcp_housing.parquet",
    ),
    "dcp_cdboundaries.parquet": (
        "edm-recipes",
        "datasets/dcp_cdboundaries/26c/dcp_cdboundaries.parquet",
    ),
    "dcp_cdta2020.parquet": (
        "edm-recipes",
        "datasets/dcp_cdta2020/26c/dcp_cdta2020.parquet",
    ),
    "dcp_housing_ahft.csv": (
        "edm-private",
        "datasets/dcp_housing_ahft/20261002/dcp_housing_ahft.csv",
    ),
}


def main(force: bool) -> None:
    client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_S3_ENDPOINT"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    SOURCES_DIR.mkdir(parents=True, exist_ok=True)
    for filename, (bucket, key) in SOURCES.items():
        path = SOURCES_DIR / filename
        if path.exists() and not force:
            print(f"exists  {filename}")
            continue
        print(f"get     {bucket}/{key}")
        client.download_file(bucket, key, str(path))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    main(parser.parse_args().force)
