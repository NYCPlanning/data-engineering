import pytest
import numpy as np
from aggregate.housing_production.area_within_historic_district import (
    _load_historic_districts_gdf,
    fraction_historic,
)

by_puma = fraction_historic("puma")
by_borough = fraction_historic("borough")
by_citywide = fraction_historic("citywide")
ind_by_geom = [by_puma, by_borough, by_citywide]
hd = _load_historic_districts_gdf()


def test_that_zero_fraction_historic_means_zero_total_historic():
    zero_fraction_historic = by_puma[by_puma["area_historic_pct"] == 0]
    assert zero_fraction_historic["area_historic_sqmiles_count"].sum() == 0


@pytest.mark.parametrize("indicator", [by_puma, by_borough])
def test_all_historic_area_assigned_to_PUMA(indicator):
    """Check that total is equal to within a foot tolerance"""
    assert np.isclose(
        hd.area.sum() / (5280**2),
        indicator["area_historic_sqmiles_count"].sum(),
        atol=1,
    )


@pytest.mark.parametrize("ind", ind_by_geom)
def test_that_denom_correct(ind):
    assert np.isclose(
        ind["area_historic_sqmiles_count"] / ind["total_sqmiles_count"],
        ind["area_historic_pct"] / 100,
        atol=0.01,
    ).all()
