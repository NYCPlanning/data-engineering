"""Margin of error is standard error times z-score associated with a given probability.
For this project our probability is 90%"""

import numpy as np
import pytest
from scipy import stats
from tests.PUMS.local_loader import LocalLoader

z_score = stats.norm.ppf(0.9)

counts_fractions_dem_loader_SE = LocalLoader()
counts_fractions_dem_loader_MOE = LocalLoader()
counts_fractions_dem_loader_SE_and_MOE = LocalLoader()


def test_local_counts_fractions_demo_loader(all_data):
    """This code to take all_data arg from command line and get the corresponding data has to be put in test because of how pytest works.
    This test exists for the sake of passing all_data arg from command line to local loader, it DOESN'T test anything
    """
    counts_fractions_dem_loader_SE.load_count_aggregator(
        all_data, keep_SE=True, add_MOE=False
    )
    counts_fractions_dem_loader_MOE.load_count_aggregator(
        all_data, keep_SE=False, add_MOE=True
    )
    counts_fractions_dem_loader_SE_and_MOE.load_count_aggregator(
        all_data, keep_SE=True, add_MOE=True
    )


demographic_indicators = ["lep", "fb-anh"]
calculation_types = ["count", "fraction"]


@pytest.mark.parametrize("ind", demographic_indicators)
@pytest.mark.parametrize("calculation_type", calculation_types)
def test_counts_fractions_demo_add_MOE_and_SE_flags(ind, calculation_type):
    assert (
        f"{ind}-{calculation_type}-se"
        in counts_fractions_dem_loader_SE.aggregated.columns
    )
    assert (
        f"{ind}-{calculation_type}-moe"
        not in counts_fractions_dem_loader_SE.aggregated.columns
    )

    assert (
        f"{ind}-{calculation_type}-se"
        not in counts_fractions_dem_loader_MOE.aggregated.columns
    )
    assert (
        f"{ind}-{calculation_type}-moe"
        in counts_fractions_dem_loader_MOE.aggregated.columns
    )


@pytest.mark.parametrize("ind", demographic_indicators)
@pytest.mark.parametrize("calculation_type", calculation_types)
def test_SE_to_MOE_demographic_indicators_counts_fractions_dem(ind, calculation_type):
    """Don't have to test each indicator. Test one crosstabbed by race and one not crosstabbed by race"""
    aggregated = counts_fractions_dem_loader_SE_and_MOE.aggregated
    assert np.allclose(
        aggregated[f"{ind}-{calculation_type}-se"].values * z_score,
        aggregated[f"{ind}-{calculation_type}-moe"].values,
    )


median_demographics_loader_only_SE = LocalLoader()
median_demographics_loader_only_MOE = LocalLoader()
median_loader_demographics_with_ME_and_SE = LocalLoader()


def test_local_median_loader(all_data):
    median_demographics_loader_only_SE.load_aggregated_medians(
        all_data=all_data, type="demographics", add_MOE=False, keep_SE=True
    )
    median_demographics_loader_only_MOE.load_aggregated_medians(
        all_data, type="demographics", add_MOE=True, keep_SE=False
    )
    median_loader_demographics_with_ME_and_SE.load_aggregated_medians(
        all_data=all_data, type="demographics", add_MOE=True, keep_SE=True
    )


median_indicators = ["age-median", "hsp-age-median"]


@pytest.mark.parametrize("ind", median_indicators)
def test_demo_median_add_MOE_and_SE_flags(ind):
    assert f"{ind}-se" in median_demographics_loader_only_SE.aggregated.columns
    assert f"{ind}-moe" not in median_demographics_loader_only_SE.aggregated.columns

    assert f"{ind}-se" not in median_demographics_loader_only_MOE.aggregated.columns
    assert f"{ind}-moe" in median_demographics_loader_only_MOE.aggregated.columns


@pytest.mark.parametrize("ind", median_indicators)
def test_demo_median_MOE_calculation_correct(ind):
    aggregated = median_loader_demographics_with_ME_and_SE.aggregated
    assert np.allclose(
        aggregated[f"{ind}-se"].values * z_score,
        aggregated[f"{ind}-moe"].values,
    )


counts_fraction_eco_loader_only_SE = LocalLoader()
counts_fraction_eco_loader_only_MOE = LocalLoader()
counts_fraction_eco_loader_with_ME_and_SE = LocalLoader()


@pytest.mark.current_dev
def test_local_counts_fractions_eco_loader(all_data):
    """This code to take all_data arg from command line and get the corresponding data has to be put in test because of how pytest works.
    This test exists for the sake of passing all_data arg from command line to local loader, it DOESN'T test anything
    """
    counts_fraction_eco_loader_only_SE.load_aggregated_counts(
        all_data, type="economics", keep_SE=True, add_MOE=False
    )
    counts_fraction_eco_loader_only_MOE.load_aggregated_counts(
        all_data, type="economics", keep_SE=False, add_MOE=True
    )
    counts_fraction_eco_loader_with_ME_and_SE.load_aggregated_counts(
        all_data, type="economics", keep_SE=True, add_MOE=True
    )


economic_indicators = ["occupation-srvc", "lf-wnh"]


@pytest.mark.current_dev
@pytest.mark.parametrize("ind", economic_indicators)
@pytest.mark.parametrize("calculation_type", calculation_types)
def test_eco_counts_fractions_MOE_and_SE_flags(ind, calculation_type):
    assert (
        f"{ind}-{calculation_type}-se"
        in counts_fraction_eco_loader_only_SE.aggregated.columns
    )
    assert (
        f"{ind}-{calculation_type}-moe"
        not in counts_fraction_eco_loader_only_SE.aggregated.columns
    )

    assert (
        f"{ind}-{calculation_type}-se"
        not in counts_fraction_eco_loader_only_MOE.aggregated.columns
    )
    assert (
        f"{ind}-{calculation_type}-moe"
        in counts_fraction_eco_loader_only_MOE.aggregated.columns
    )


@pytest.mark.current_dev
@pytest.mark.parametrize("ind", economic_indicators)
@pytest.mark.parametrize("calculation_type", calculation_types)
def test_SE_to_MOE_demographic_indicators_counts_fractions_econ(ind, calculation_type):
    """Don't have to test each indicator. Test one crosstabbed by race and one not crosstabbed by race"""
    aggregated = counts_fraction_eco_loader_with_ME_and_SE.aggregated
    assert np.allclose(
        aggregated[f"{ind}-{calculation_type}-se"].values * z_score,
        aggregated[f"{ind}-{calculation_type}-moe"].values,
    )
