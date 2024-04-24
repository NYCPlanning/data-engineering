"""Coefficient of variation should always be included and always be less than one
Note that changes from fraction to pct will break these tests
"""

import pytest
from tests.PUMS.local_loader import LocalLoader

count_dem_loader = LocalLoader()
count_eco_loader = LocalLoader()
median_dem_loader = LocalLoader()


def test_local_loader(all_data):
    count_dem_loader.load_aggregated_counts(all_data=all_data, type="demographics")
    count_eco_loader.load_aggregated_counts(all_data=all_data, type="economics")
    median_dem_loader.load_aggregated_medians(all_data=all_data, type="demographics")


count_loaders = [count_dem_loader, count_eco_loader]


@pytest.mark.parametrize("loader", count_loaders)
def test_counts_have_coefficient_of_variation(loader):
    df = loader.aggregated
    for c in df.columns:
        if c[-6:] == "-count":
            assert c + "-cv" in df.columns


@pytest.mark.parametrize("loader", count_loaders)
def test_fractions_have_coefficient_of_variation(loader):
    df = loader.aggregated
    for c in df.columns:
        if c[-9:] == "-fraction":
            assert c + "-cv" in df.columns


median_loaders = [median_dem_loader]


@pytest.mark.parametrize("loader", median_loaders)
def test_medians_have_coefficient_of_variation(loader):
    df = loader.aggregated
    for c in df.columns:
        if c[-7:] == "-median":
            assert c + "-cv" in df.columns
