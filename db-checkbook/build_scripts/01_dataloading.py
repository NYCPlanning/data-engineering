#from dcpy.connectors.s3 import client
import json
import os
from functools import wraps
from pathlib import Path

import pandas as pd
import yaml
import geopandas as gpd



#def do_stuff() -> None:
 #   print("doing stuff")
  #  s3_client = client()
   # available_buckets = [
    #    bucket["Name"] for bucket in s3_client.list_buckets()["Buckets"]
    #]
   # print(f"This S3 client is a {type(s3_client)}")
   # print(f"This S3 client has access to buckets: {available_buckets}")


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
