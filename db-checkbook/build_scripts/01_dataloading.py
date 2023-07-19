from dcpy.connectors import s3
import json
from functools import wraps
from pathlib import Path

import pandas as pd
import yaml
import geopandas as gpd


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

def read_from_gpd(path):
    df = gpd.read_file('https://edm-recipes.nyc3.cdn.digitaloceanspaces.com/datasets/dcp_cpdb/2018_adopted/dcp_cpdb.sql', geometry = 'geometry')
    return df




if __name__ == "__main__":
    print("started dataloading ...")
    #do_stuff()
