import pytest
from tests.PUMS.local_loader import LocalLoader
from tests.util import race_counts

local_loader = LocalLoader()


@pytest.mark.test_aggregation
def test_local_loader(all_data):
    """This code to take all_data arg from command line and get the corresponding data has to be put in test because of how pytest works.
    This test exists for the sake of passing all_data arg from command line to local loader, it DOESN'T test anything
    """
    local_loader.load_aggregated_counts(all_data, type="economics")


@pytest.mark.test_aggregation
def test_all_race_sum_to_total_within_labor_force():
    """Can be folded into test_that_all_races_sum_to_total_within_indicator in demographics aggregation tests"""
    lf_race_cols = [f"lf_{r}" for r in race_counts]
    assert (
        local_loader.aggregated[lf_race_cols].sum(axis=1)
        == local_loader.aggregated["lf-count"]
    ).all()


@pytest.mark.test_aggregation
def test_all_occupations_and_industry_sum_to_total():
    """This is harder, have to reflect on how to do this"""
    pass


@pytest.mark.test_aggregation
def test_industry_assigned_correctly():
    """Can parameterize this to include other industries"""
    assert (
        local_loader.by_person[local_loader.by_person["INDP"] == "Wholesale Trade"][
            "industry"
        ]
        == "Whlsl"
    ).all()


@pytest.mark.test_aggregation
def test_occupation_assigned_correctly():
    """Can parameterize this to include other occupations"""
    assert (
        local_loader.by_person[
            local_loader.by_person["OCCP"] == "Sales and Office Occupations"
        ]["occupation"]
        == "slsoff"
    ).all()
