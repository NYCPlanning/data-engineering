import pandas as pd
import geopandas as gpd
import boto3
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_BUCKET = 'edm-recipes'
BASE_URL = "https://edm-recipes.nyc3.cdn.digitaloceanspaces.com"

_curr_file_path = Path(__file__).resolve()
LIB_DIR = _curr_file_path.parent.parent / '.library'
load_dotenv(_curr_file_path.parent.parent.parent / '.env') 



def download_s3_edm_recipes_cpdb():
    """read EDM data: using S3 connectors
    example: datasets/dcp_cpdb/2018_adopted_polygons/
    version: 2017_adopted, 2018_adopted, 2019_adopted, 2020_adopted, 2021_adopted, 2022_adopted, 2023_executive
    type_geom: _polygons, _points 
    """

    s3_resource = boto3.resource('s3', 
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        endpoint_url=os.environ["AWS_S3_ENDPOINT"] 
        )

    prefix = 'datasets/dcp_cpdb/'

    bucket = s3_resource.Bucket(BASE_BUCKET)     
    for obj in bucket.objects.filter(Prefix = prefix):
        key = obj.key.replace(prefix, '')
        if key and (key[-1] != '/'):
            if not os.path.exists(LIB_DIR / os.path.dirname(key)):
                os.makedirs(LIB_DIR / os.path.dirname(key))
            bucket.download_file(obj.key, LIB_DIR / key)
    return


def read_edm_recipes_nyc_checkbook(version = "latest"):
    """filepath: datasets/nycoc_checkbook/latest/nycoc_checkbook.csv 
    """
    file_name = f'{BASE_URL}/datasets/nycoc_checkbook/{version}/nycoc_checkbook.csv'
    df = pd.read_csv(file_name, dtype=str, index_col=False)
    pd.to_csv(LIB_DIR / 'nycoc_checkbook.csv')
    return

def run_dataloading() -> None:

    download_s3_edm_recipes_cpdb()
    read_edm_recipes_nyc_checkbook()

    return 



if __name__ == "__main__":
    print("Started dataloading ...")
    run_dataloading()
    print("Finished dataloading ...")
    

