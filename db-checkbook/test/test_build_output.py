import os
import sys
import pandas as pd
import geopandas as gpd

_current_dir = os.path.dirname(os.path.abspath(__file__))
_parent_dir = os.path.dirname(_current_dir)
sys.path.insert(0, _parent_dir)

from build_scripts.build import _group_checkbook, _clean_checkbook

def check_df_size(gdf: gpd.GeoDataFrame) -> bool:
    if gdf.shape[0] == 16687:
        return True
    return False 

def check_join_size(gdf: gpd.GeoDataFrame) -> bool:
    if gdf.shape[0] == 3880:
        return True
    return False

def test_num_capital_projects() -> bool:
    print('testing number of capital projects...')
    df = _clean_checkbook()

def test_checkbook_grouping() -> bool:
    print('testing checkbook size...')
    df = _group_checkbook()
    print(df.shape)
    assert df.shape[0] == 16687

if __name__ =="__main__":
    test_checkbook_grouping()