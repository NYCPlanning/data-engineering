import numpy as np
import pytest

from pandas.testing import assert_frame_equal, assert_series_equal
from build_scripts.build import _group_checkbook, _clean_checkbook, _merge_cpdb_geoms, _join_checkbook_geoms, _assign_checkbook_category, _clean_joined_checkbook_cpdb, _assign_final_category 
from test.generate_test_data import generate_cpdb_test_data, generate_checkbook_test_data, generate_expected_cpdb_join, generate_expected_grouped_checkbook, generate_expected_final_data

CPDB_GDF_LIST = generate_cpdb_test_data()
CHECKBOOK_TEST = generate_checkbook_test_data()

def test_fms_id_exists():
    clean_checkbook = _clean_checkbook(CHECKBOOK_TEST)
    grouped_checkbook_df = _group_checkbook(clean_checkbook)
    assert 'fms_id' in grouped_checkbook_df.columns

def test_null_fms_id():
    clean_checkbook = _clean_checkbook(CHECKBOOK_TEST)
    grouped_checkbook_df = _group_checkbook(clean_checkbook)
    assert np.where(grouped_checkbook_df['fms_id'].isnull())

def test_unique_fms_id():
    clean_checkbook = _clean_checkbook(CHECKBOOK_TEST)
    grouped_checkbook_df = _group_checkbook(clean_checkbook)
    assert not grouped_checkbook_df['fms_id'].duplicated().any()

def test_check_nonneg():
    cleaned_checkbook_df = _clean_checkbook(CHECKBOOK_TEST)
    assert (cleaned_checkbook_df['check_amount'] >= 0).all()

def test_group_checkbook():
    expected_result = generate_expected_grouped_checkbook().set_index('fms_id').sort_index()
    result = _group_checkbook(_clean_checkbook(CHECKBOOK_TEST)).set_index('fms_id').sort_index()
    assert result.equals(expected_result)

@pytest.mark.skip(reason="TODO define a more useful test")
def test_null_maprojid():
    CPDB_GDF_LIST = generate_cpdb_test_data()
    cpdb_df = _merge_cpdb_geoms(CPDB_GDF_LIST)
    assert np.where(cpdb_df['maprojid'].isnull())

def test_unique_maprojid(): 
    CPDB_GDF_LIST = generate_cpdb_test_data()
    cpdb_df = _merge_cpdb_geoms(CPDB_GDF_LIST)
    assert not cpdb_df['maprojid'].duplicated().any()

def test_null_geometry():
    CPDB_GDF_LIST = generate_cpdb_test_data()
    cpdb_df = _merge_cpdb_geoms(CPDB_GDF_LIST)
    assert np.where(cpdb_df['geometry'].isnull())

def test_merge_cpdb_geoms():
    CPDB_GDF_LIST = generate_cpdb_test_data()
    expected_result = generate_expected_cpdb_join()
    result = _merge_cpdb_geoms(CPDB_GDF_LIST)
    assert result.equals(expected_result)

@pytest.mark.skip(reason='TODO QA output of category assignment on budget code and contract purpose')
def test_high_sensitivity_fixed_asset():
    cpdb = _merge_cpdb_geoms(CPDB_GDF_LIST)
    checkbook = _group_checkbook(_clean_checkbook(CHECKBOOK_TEST))
    cat_checkbook = _assign_checkbook_category(checkbook)
    join = _join_checkbook_geoms(cat_checkbook, cpdb)
    clean_join = _clean_joined_checkbook_cpdb(join)
    clean_join = clean_join[[
        'fms_id', 
        'contract_purpose', 
        'agency', 
        'budget_code',
        'check_amount',
        'bc_category',
        'cp_category',
        'maprojid',
        'cpdb_category',
        'geometry',
        'has_geometry'
    ]]
    
    result = _assign_final_category(clean_join).set_index('fms_id').sort_index()
    expected_result = generate_expected_final_data().set_index('fms_id').sort_index()
    assert result.equals(expected_result)