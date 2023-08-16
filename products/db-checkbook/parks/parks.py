import pandas as pd
import geopandas as gpd
import re
import os
from pathlib import Path
from sqlalchemy import create_engine, text
from dcpy.utils.s3 import download_file
from dcpy.utils.postgres import execute_file_via_shell

from . import LIB_DIR, OUTPUT_DIR, SQL_DIR, BUILD_OUTPUT_FILENAME

DB_URL = "sqlite:///checkbook.db"
ENGINE = create_engine(DB_URL)

BASE_BUCKET = "edm-recipes"
BASE_URL = "https://edm-recipes.nyc3.cdn.digitaloceanspaces.com"

def download_s3_parks(v="latest") -> None:
    """download parks properties dataset as csv from edm-recipes 
    """
    s3_object_key = f"datasets/dpr_parksproperties/{version}/dpr_parksproperties.csv"
    download_filepath = Path(LIB_DIR / "dpr_parksproperties.csv")
    download_file(BASE_BUCKET, s3_object_key, download_filepath)
    return

def read_csv_to_df(dir: Path = LIB_DIR, fn: str = "dpr_parksproperties.csv") -> pd.DataFrame:
    """read csv to df
    """
    df = pd.read_csv(dir / fn)
    return df

def layer_parks(csdb: pd.DataFrame, parks: pd.DataFrame) -> pd.DataFrame:
    """execute sql query on csdb to layer in geometries from parks properties
    """
    with ENGINE.connect() as conn:
        csdb.to_sql("csdb", ENGINE, if_exists="replace", index=False)
        parks.to_sql("parks", ENGINE, if_exists="replace", index=False)
        execute_file_via_shell(conn, SQL_DIR / "parks.sql")
    
    csdb_with_parks = pd.read_sql_table("csdb", conn)
    return csdb_with_parks

def run() -> None:
    download_s3_parks()
    csdb = read_csv_to_df(OUTPUT_DIR, BUILD_OUTPUT_FILENAME) 
    parks = read_csv_to_df()

    print('% projects mapped to geoms before adding parks: {}'.format(csdb[csdb['has_geometry']==True].shape[0]/csdb.shape[0]))
    csdb_with_parjs = layer_parks(csdb, parks)
    print('% projects mapped to geoms AFTER adding parks: {}'.format(csdb_with_parks[csdb_with_parks['has_geometry']==True].shape[0]/csdb_with_parks.shape[0]))
    return

if __name__=="__main__":
    print("layering in parks properties...")
    run()
    print("...done!")