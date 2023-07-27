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

# --- checkbook data
def test_fms_id_exists():
    assert 'fms_id' in grouped_checkbook_df.columns

def test_null_fms_id():
    assert np.where(grouped_checkbook_df['fms_id'].isnull())

@pytest.mark.skip(reason='not done')
def test_unique_fms_id():
    assert pd.Series(grouped_checkbook_df['fms_id']).is_unique()

@pytest.mark.skip(reason='not done')
def test_check_nonneg():
    assert pd.Series(grouped_checkbook_df['check_amount']) > 0

@pytest.mark.skip(reason='clean checkbook needs to be updated')
def test_clean_checkbook():
    df = pd.DataFrame({
        'test_case' : ['normal', 'neg', 'big'],
        'check_amount': [1000, -500, 200000000],
        'capital_project': ['998CAP2024  005', '123CAP2025  005', '987CAP2023  005']
    })
    expected_result = pd.DataFrame({
        'check_amount': [1000],
        'fms_id': ['998CAP2024']
    })
    result = _clean_checkbook(df)
    assert result.equals(expected_result)


# --- cpdb data
def test_null_maprojid():
    assert np.where(cpdb_df['maprojid'].isnull())

@pytest.mark.skip(reason='not done')
def test_unique_maprojid(): 
    assert pd.Series(cpdb_df['maprojid']).is_unique()

def test_null_geometry():
    assert np.where(cpdb_df['geometry'].isnull())

# --- results of join

@pytest.mark.skip(reason='not done')
def test_merge_cpdb_geoms():
    gdf1 = gpd.GeoDataFrame({'maprojid': [1, 2, 3], 'geometry': [None, 'POINT (1 1)', 'POINT (2 2)']})
    gdf2 = gpd.GeoDataFrame({'maprojid': [2, 4], 'geometry': ['POINT (3 3)', 'POINT (4 4)']})
    expected_result = gpd.GeoDataFrame({'maprojid': [1, 2, 3, 4], 'geometry': [None, 'POINT (1 1)', 'POINT (2 2)', 'POINT (4 4)']})
    result = _merge_cpdb_geoms(gdf_list=[gdf1, gdf2])
    assert result.equals(expected_result)

# --- below are endpoint tests

# def test_num_capital_projects() -> bool:
#     print('testing number of capital projects...')
#     df = _clean_checkbook()
#     print(df['fms_id'].nunique())
#     return df['fms_id'].nunique() == 16687

# def test_size_raw_checkbook() -> bool:
#     print('testing size of raw checkbook data...')
#     df = _clean_checkbook()
#     print(df.shape)
#     return df.shape[0] > 1900000

# def test_checkbook_grouping() -> bool:
#     print('testing checkbook size...')
#     df = _group_checkbook()
#     print(df.shape)
#     return df.shape[0] == 16687

# def test_join_size(gdf: gpd.GeoDataFrame) -> bool:
#     print('testing number of projects with geometries...')
#     print(gdf[gdf['has_geometry']==True].shape[0])
#     return gdf[gdf['has_geometry']==True].shape[0] == 3880

if __name__ == '__main__':
    pytest.main()
