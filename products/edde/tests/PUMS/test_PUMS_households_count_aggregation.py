"""
Note that changes from fraction to pct will break these tests
"""

import pytest
from tests.PUMS.local_loader import LocalLoader

local_loader = LocalLoader()


@pytest.mark.test_aggregation
def test_local_loader(all_data):
    """This code to take all_data arg from command line and get the corresponding data has to be put in test because of how pytest works.
    This test exists for the sake of passing all_data arg from command line to local loader, it DOESN'T test anything
    """
    local_loader.load_aggregated_counts(all_data, type="households")


@pytest.mark.test_aggregation
def test_industry_assigned_correctly_LI():
    """Can parameterize this to include other industries"""
    assert (
        local_loader.by_person[
            (local_loader.by_person["HINCP"] == "50000")
            & (local_loader.by_person["NPF"] == "5")
        ]["household_income_bands"]
        == "LI"
    ).all()


@pytest.mark.test_aggregation
def test_industry_assigned_correctly_MIDI():
    """Can parameterize this to include other industries"""
    assert (
        local_loader.by_person[
            (local_loader.by_person["HINCP"] == "100000")
            & (local_loader.by_person["NPF"] == "2")
        ]["household_income_bands"]
        == "MIDI"
    ).all()


@pytest.mark.test_aggregation
def test_income_bands_fractions_add_up_to_one():
    """does the income band categories fractions in each puma adds up to one"""
    ib = ["ELI", "VLI", "LI", "MI", "MIDI", "HI"]
    ib_frac_cols = [f"{x}-pct" for x in ib]
    assert (local_loader.aggregated[ib_frac_cols].sum(axis=1) == 1).all()
