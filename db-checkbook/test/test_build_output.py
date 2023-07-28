import os
import sys
import pandas as pd
import geopandas as gpd
import numpy as np
import pytest
from pathlib import Path

from build_scripts.build import _group_checkbook, _clean_checkbook, _merge_cpdb_geoms, _join_checkbook_geoms
from test.generate_test_data import generate_cpdb_test_data, generate_checkbook_test_data, generate_expected_cpdb_join, generate_expected_grouped_checkbook

# TODO: use raw checkbook data in future
cpdb_gpd_list_test = generate_cpdb_test_data()
checkbook_test = generate_checkbook_test_data()

# --- checkbook data
def test_fms_id_exists():
    clean_checkbook = _clean_checkbook(checkbook_test)
    grouped_checkbook_df = _group_checkbook(clean_checkbook)
    assert 'fms_id' in grouped_checkbook_df.columns

def test_null_fms_id():
    clean_checkbook = _clean_checkbook(checkbook_test)
    grouped_checkbook_df = _group_checkbook(clean_checkbook)
    assert np.where(grouped_checkbook_df['fms_id'].isnull())

def test_unique_fms_id():
    clean_checkbook = _clean_checkbook(checkbook_test)
    grouped_checkbook_df = _group_checkbook(clean_checkbook)
    assert not grouped_checkbook_df['fms_id'].duplicated().any()

def test_check_nonneg():
    cleaned_checkbook_df = _clean_checkbook(checkbook_test)
    assert (cleaned_checkbook_df['check_amount'] >= 0).all()

@pytest.mark.skip(reason='clean checkbook needs to be updated')
def test_group_checkbook():
    expected_result = generate_expected_grouped_checkbook()
    result = _clean_checkbook(checkbook_test)
    assert result.equals(expected_result)

# --- cpdb data
def test_null_maprojid():
    cpdb_gpd_list_test = generate_cpdb_test_data()
    cpdb_df = _merge_cpdb_geoms(cpdb_gpd_list_test)
    assert np.where(cpdb_df['maprojid'].isnull())

# @pytest.mark.skip(reason='TODO after major refactor')
def test_unique_maprojid(): 
    cpdb_gpd_list_test = generate_cpdb_test_data()
    cpdb_df = _merge_cpdb_geoms(cpdb_gpd_list_test)
    assert not cpdb_df['maprojid'].duplicated().any()

def test_null_geometry():
    cpdb_gpd_list_test = generate_cpdb_test_data()
    cpdb_df = _merge_cpdb_geoms(cpdb_gpd_list_test)
    assert np.where(cpdb_df['geometry'].isnull())

# --- results of join

# @pytest.mark.skip(reason='TODO after major refactor')
def test_merge_cpdb_geoms():
    cpdb_gpd_list_test = generate_cpdb_test_data()
    expected_result = generate_expected_cpdb_join()
    result = _merge_cpdb_geoms(cpdb_gpd_list_test)
    assert result.equals(expected_result)

# --- high-sensitivity fixed asset CATEGORY ASSIGNMENT
