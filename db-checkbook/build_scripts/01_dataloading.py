from dcpy.connectors import s3
from functools import wraps
from pathlib import Path
import pandas as pd
import geopandas as gpd

base_bucket = 'edm-recipes'
base_url = "https://edm-recipes.nyc3.cdn.digitaloceanspaces.com"



# reading in the data: using S3 connectors

# example: datasets/dcp_cpdb/2018_adopted_polygons/
# dataset name: datasets/dcp_cpdb
# version: 2018_adopted, 2019_adopted, 2020_adopted, 2021_adopted, 2022_adopted, 2023_executive
# type geom: _polygons, _points

def read_s3_edm_recipes_cpdb(dataset_name, version, type_geom, save_file_path):
    digital_ocean_filepath = f'{dataset_name}/{version}{type_geom}/dcp_cpdb.shp'
    s3_client = s3.client()
    file = s3_client.download_file(base_bucket, digital_ocean_filepath, save_file_path)
    return file


def read_edm_recipes_nyc_checkbook(name):
    file_name = f'{base_url}/{name}'
    df = pd.read_csv(file_name, dtype=str, index_col=False)
    return df


if __name__ == "__main__":
    print("started dataloading ...")
