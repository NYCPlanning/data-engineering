import os
from pathlib import Path

import boto3
import pandas as pd

from dcpy.utils.s3 import download_file

from . import LIB_DIR, OUTPUT_DIR

BASE_BUCKET = "edm-recipes"
BASE_URL = "https://edm-recipes.nyc3.cdn.digitaloceanspaces.com"


def download_s3_parks(version="latest") -> None:
    """download parks properties dataset as csv from edm-recipes"""
    s3_object_key = f"datasets/dpr_parksproperties/{version}/dpr_parksproperties.csv"
    download_filepath = Path(LIB_DIR / "dpr_parksproperties.csv")
    download_file(BASE_BUCKET, s3_object_key, download_filepath)
    return


def download_s3_edm_recipes_cpdb() -> None:
    """read EDM data: using S3 connectors
    example: datasets/dcp_cpdb/2018_adopted_polygons/
    version: 2017_adopted, 2018_adopted, 2019_adopted, 2020_adopted, 2021_adopted, 2022_adopted, 2023_executive
    type_geom: _polygons, _points
    """
    prefix = "datasets/dcp_cpdb/"
    s3_resource = boto3.resource(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        endpoint_url=os.environ["AWS_S3_ENDPOINT"],
    )

    bucket = s3_resource.Bucket(BASE_BUCKET)
    for obj in bucket.objects.filter(Prefix=prefix):
        key = obj.key.replace(prefix, "")
        if key and (key[-1] != "/"):
            if not os.path.exists(LIB_DIR / os.path.dirname(key)):
                os.makedirs(LIB_DIR / os.path.dirname(key))
            bucket.download_file(obj.key, LIB_DIR / key)
    return


def read_edm_recipes_nyc_checkbook(version="latest") -> None:
    """filepath: datasets/nycoc_checkbook/latest/nycoc_checkbook.csv"""
    s3_object_key = f"datasets/nycoc_checkbook/{version}/nycoc_checkbook.csv"
    download_filepath = Path(LIB_DIR / "nycoc_checkbook.csv")
    download_file(BASE_BUCKET, s3_object_key, download_filepath)
    return


def create_source_data_version_csv() -> None:
    """makes a csv with a schema (dataset name) and version (CPDB year)"""

    files = Path(LIB_DIR).glob("*")
    schema = []
    version = []
    for file in files:
        file_name = file.stem
        schema.append(file_name)
        version_name = file_name.split("_")[0]
        version.append(version_name)
    pd.DataFrame({"Schema": schema, "v": version}).to_csv(
        OUTPUT_DIR / "source_data_versions.csv"
    )
    return


def run_dataloading() -> None:
    download_s3_edm_recipes_cpdb()
    download_s3_parks()
    read_edm_recipes_nyc_checkbook()
    create_source_data_version_csv()

    return


if __name__ == "__main__":
    print("Started dataloading ...")
    run_dataloading()
    print("Finished dataloading ...")
