import os
import sys
import pandas as pd
import geopandas as gpd
import numpy as np
import pytest
from pathlib import Path

_current_dir = os.path.dirname(os.path.abspath(__file__))
_parent_dir = os.path.dirname(_current_dir)
sys.path.insert(0, _parent_dir)

_curr_file_path = Path(__file__).resolve()
LIB_DIR = _curr_file_path.parent.parent / '.library'

from build_scripts.build import _group_checkbook, _clean_checkbook, _merge_cpdb_geoms, _join_checkbook_geoms

raw_checkbook_df = pd.read_csv(LIB_DIR / 'nycoc_checkbook.csv')
grouped_checkbook_df = _group_checkbook()
cpdb_df = _merge_cpdb_geoms()
joined_df - _join

# --- unit tests

# --- checkbook data
def test_fms_id_exists():
    assert 'fms_id' in grouped_checkbook_df.columns

def test_null_fms_id():
    assert np.where(grouped_checkbook_df['fms_id'].isnull())

def test_unique_fms_id():
    assert pd.Series(grouped_checkbook_df['fms_id']).isunique()

def test_check_nonneg():
    assert pd.Series(grouped_checkbook_df['check_amount']) > 0

# --- cpdb data
def test_null_maprojid():
    assert np.where(cpdb_df['maprojid'].isnull())

def test_unique_maprojid(): 
    assert pd.Series(cpdb_df['maprojid']).isunique()

def test_null_geometry():
    assert np.where(cpdb_df['geometry'].isnull())

# --- results of join

def test_join_results():
    return

# --- below are endpoint tests

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
