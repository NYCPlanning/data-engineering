from dcpy.connectors import s3
import json
from functools import wraps
from pathlib import Path

import pandas as pd

BASE_BUCKET = 'edm-recipes'
BASE_URL = "https://edm-recipes.nyc3.cdn.digitaloceanspaces.com"


import os
import boto3



def read_s3(digital_ocean_filepath, save_file_path):

    #digital_ocean_filepath example "datasets/bpl_libraries/20210208/bpl_libraries.csv"

    s3_client = s3.client()
    file = s3_client.download_file('edm-recipes', digital_ocean_filepath, save_file_path)
    return file

# reading in the checkboob daa
def read_from_csv(name: str) -> pd.DataFrame:
    df = pd.read_csv(
        "https://edm-recipes.nyc3.cdn.digitaloceanspaces.com/datasets/test_nypl_libraries/20210122/test_nypl_libraries.csv", dtype=str, index_col=False
    )
    return df


if __name__ == "__main__":
    print("started dataloading ...")
