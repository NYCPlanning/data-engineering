"""Download the pinned source files into data/sources/.

Most come from S3, and the HPD Housing and Vacancy Survey public use files from nyc.gov. Reads S3 credentials from the same env vars as dcpy (AWS_S3_ENDPOINT, AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY), which the repo's root .env provides through direnv.

    python scripts/download.py          # skip files already downloaded
    python scripts/download.py --force  # download everything again
"""

import argparse
import os
import urllib.request
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
    "kpdb_cpp_housing_growth_cd.csv": (
        "edm-publishing",
        "db-kpdb/publish/2026-10-01/cpp_housing_growth/cpp_housing_growth_cd.csv",
    ),
    "kpdb_longform_cd_output.csv": (
        "edm-publishing",
        "db-kpdb/publish/2026-10-01/aggregation/longform_cd_output.csv",
    ),
    "dcp_housing.parquet": (
        "edm-recipes",
        "datasets/dcp_housing/26Q2/dcp_housing.parquet",
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

# local filename -> url, for files published outside S3
HVS_URL = "https://www.nyc.gov/assets/hpd/data"
URL_SOURCES = {
    f"hvs_{dataset}_{year}.csv": f"{HVS_URL}/{dataset}_puf_{year % 100}.csv"
    for dataset in ["allunits", "occupied"]
    for year in [2021, 2023]
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
    for filename, url in URL_SOURCES.items():
        path = SOURCES_DIR / filename
        if path.exists() and not force:
            print(f"exists  {filename}")
            continue
        print(f"get     {url}")
        # nyc.gov rejects urllib's default user agent
        request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(request) as response:
            path.write_bytes(response.read())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    main(parser.parse_args().force)
