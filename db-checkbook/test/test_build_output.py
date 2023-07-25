import pandas as pd
import geopandas as gpd
from build_scripts.02_build import run_build

def check_df_size(gdf: gpd.GeoDataFrame) -> bool:
    if gdf.shape[0] == 16687:
        return True
    return False 

def check_join_size(gdf: gpd.GeoDataFrame) -> bool:
    if gdf.shape[0] == 3880:
        return True
    return False

if __name__ =="__main__":
    df = run_build()
    print(check_join_size(df))
    print(check_df_size(df))

