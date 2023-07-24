from dcpy.connectors import s3
import pandas as pd

BASE_BUCKET = 'edm-recipes'
BASE_URL = "https://edm-recipes.nyc3.cdn.digitaloceanspaces.com"



def read_s3_edm_recipes_cpdb(version, type_geom, save_file_path):
    """read EDM data: using S3 connectors
    example: datasets/dcp_cpdb/2018_adopted_polygons/
    version: 2018_adopted, 2019_adopted, 2020_adopted, 2021_adopted, 2022_adopted, 2023_executive
    type_geom: _polygons, _points 
    """
    digital_ocean_filepath = f'datasets/dcp_cpdb/{version}{type_geom}/dcp_cpdb.shp'
    s3.client().download_file(BASE_BUCKET, digital_ocean_filepath, save_file_path)
    return save_file_path


def read_edm_recipes_nyc_checkbook(version = "latest"):
    """filepath: datasets/nycoc_checkbook/latest/nycoc_checkbook.csv 
    """
    file_name = f'{BASE_URL}/datasets/nycoc_checkbook/{version}/nycoc_checkbook.csv'
    df = pd.read_csv(file_name, dtype=str, index_col=False)
    return df


if __name__ == "__main__":
    print("started dataloading ...")
