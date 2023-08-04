import numpy as np
import pandas as pd
import pytest

from build_scripts.build import _group_checkbook, _clean_checkbook, _merge_cpdb_geoms, _join_checkbook_geoms, _assign_checkbook_category, _clean_joined_checkbook_cpdb, _assign_final_category 
from test.generate_test_data import generate_cpdb_test_data, generate_checkbook_test_data, generate_expected_cpdb_join, generate_expected_grouped_checkbook, generate_expected_final_data

CPDB_GDF_LIST = generate_cpdb_test_data()
CHECKBOOK_TEST = generate_checkbook_test_data()

@pytest.fixture
def df_columns(df: pd.DataFrame) -> List[str]:
    return 

class TestCheckbook:
    """
    tests that validate Checkbook NYC input data,
    cleaning and groupby transformations before joining 
    to CPDB geoms
    """
    cleaned_checkbook_df = _clean_checkbook(CHECKBOOK_TEST)
    grouped_checkbook_df = _group_checkbook(cleaned_checkbook_df)
    expected_grouped_checkbook = generate_expected_grouped_checkbook()
    
    def test_check_nonneg(self):
        assert (self.cleaned_checkbook_df['check_amount'] >= 0).all(), \
        "nonnegative checks exist in cleaned checkbook data"

    def test_fms_id_exists(self):
        assert 'fms_id' in self.grouped_checkbook_df.columns, \
        "`fms_id` column missing from grouped checkbook data"

    def test_null_fms_id(self):
        assert np.where(self.grouped_checkbook_df['fms_id'].isnull()), \
        "there are null values in `fms_id` column in grouped checkbook data"

    def test_unique_fms_id(self):
        assert not self.grouped_checkbook_df['fms_id'].duplicated().any(), \
        "duplicate `fms_id`s exist in grouped checkbook data"

    def test_group_checkbook_cols(self):
        assert

    def test_group_checkbook(self):
        expected_result = self.expected_grouped_checkbook.set_index('fms_id').sort_index()
        result = self.grouped_checkbook_df.set_index('fms_id').sort_index()
        assert result.equals(expected_result), \
        "results of grouping checkbook data do not match expectations"

class TestCPDB:
    """
    tests that validate CPDB geoms and transformations 
    before joining to Checkbook NYC capital projects 
    """
    cpdb_df = _merge_cpdb_geoms(CPDB_GDF_LIST)
    expected_result = generate_expected_cpdb_join()

    @pytest.mark.skip(reason="TODO define a more useful test")
    def test_null_maprojid(self):
        assert np.where(self.cpdb_df['maprojid'].isnull())

    def test_unique_maprojid(self): 
        assert not self.cpdb_df['maprojid'].duplicated().any()

    def test_null_geometry(self):
        assert np.where(self.cpdb_df['geometry'].isnull())

    def test_merge_cpdb_geoms(self):
        assert self.cpdb_df.equals(self.expected_result)

class TestHistoricalLiquidations:
    """
    tests that validate the build output, i.e. the final 
    Historical Liquidations dataset
    """
    cpdb = _merge_cpdb_geoms(CPDB_GDF_LIST)
    checkbook = _group_checkbook(_clean_checkbook(CHECKBOOK_TEST))
    cat_checkbook = _assign_checkbook_category(checkbook)
    join = _join_checkbook_geoms(cat_checkbook, cpdb)
    clean_join = _clean_joined_checkbook_cpdb(join)[[
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
    historical_liquidations = _assign_final_category(clean_join).set_index('fms_id').sort_index()
    expected_historical_liquidations = generate_expected_final_data().set_index('fms_id').sort_index()
        
    @pytest.mark.skip(reason='TODO QA output of category assignment on budget code and contract purpose')
    def test_high_sensitivity_fixed_asset(self):
        assert self.historical_liquidations.equals(self.expected_historical_liquidations)


