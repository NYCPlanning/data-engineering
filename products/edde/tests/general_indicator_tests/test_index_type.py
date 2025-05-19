import pandas as pd
import pytest
from tests.general_indicator_tests.general_indicator_test_helpers import get_by_geo
from utils.geo_helpers import get_all_NYC_PUMAs, get_all_boroughs

all_PUMAs = get_all_NYC_PUMAs()
all_boroughs = get_all_boroughs()

by_puma, by_borough, by_citywide = get_by_geo()


@pytest.mark.parametrize("data, ind_name", by_puma)
def test_all_PUMAs_present(data, ind_name):
    assert data.index.values.sort() == all_PUMAs.sort(), (
        f"not all PUMAs present for {ind_name}"
    )


@pytest.mark.parametrize("data, ind_name", by_borough)
def test_all_boroughs_present(data, ind_name):
    assert data.index.values.sort() == all_boroughs.sort(), (
        f"not all boroughs present for {ind_name}"
    )


@pytest.mark.parametrize("data, ind_name", by_citywide)
def test_citywide_single_index(data, ind_name):
    assert data.index.values == ["citywide"], (
        f"citywide index incorrect  for {ind_name}"
    )


@pytest.mark.parametrize("data, ind_name", by_puma + by_borough + by_citywide)
def test_rv_dataframe(data, ind_name):
    assert isinstance(data, pd.DataFrame), f"{ind_name} returns incorrect type"
