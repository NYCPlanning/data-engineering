import os
import sys
import pandas as pd
import geopandas as gpd
from pathlib import Path

_current_dir = os.path.dirname(os.path.abspath(__file__))
_parent_dir = os.path.dirname(_current_dir)
sys.path.insert(0, _parent_dir)

_curr_file_path = Path(__file__).resolve()
LIB_DIR = _curr_file_path.parent.parent / '.library'

from build_scripts.build import _group_checkbook, _clean_checkbook, run_build

def test_num_capital_projects() -> bool:
    print('testing number of capital projects...')
    df = _clean_checkbook()
    print(df['fms_id'].nunique())
    return df['fms_id'].nunique() == 16687

def test_size_raw_checkbook() -> bool:
    print('testing size of raw checkbook data...')
    df = _clean_checkbook()
    print(df.shape)
    return df.shape[0] > 1900000

def test_checkbook_grouping() -> bool:
    print('testing checkbook size...')
    df = _group_checkbook()
    print(df.shape)
    return df.shape[0] == 16687

def test_join_size(gdf: gpd.GeoDataFrame) -> bool:
    print('testing number of projects with geometries...')
    print(gdf[gdf['has_geometry']==True].shape[0])
    return gdf[gdf['has_geometry']==True].shape[0] == 3880

# TODO: check category assignment numbers

def test_all() -> bool:
    test_size_raw_checkbook()
    test_num_capital_projects()
    test_checkbook_grouping()
    test_join_size(run_build())

if __name__ =="__main__":
    print('testing build...')
    test_all()
    print('...complete!')