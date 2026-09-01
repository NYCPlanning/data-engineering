import pandas as pd
import pytest
from utils.geo_helpers import get_all_boroughs, get_all_NYC_PUMAs

from tests.general_indicator_tests.general_indicator_test_helpers import get_by_geo

all_PUMAs = get_all_NYC_PUMAs()
all_boroughs = get_all_boroughs()

by_puma, by_borough, by_citywide = get_by_geo()

# Known PUMA-index bugs, tracked here rather than silently skipped so a fix
# causes an xfail_strict failure that prompts removing the entry.
KNOWN_BROKEN_PUMA_INDEX = {
    "nycha_tenants": (
        "A footer string from the source Excel ('Data Source: ...') leaks "
        "through as a fake PUMA row - aggregate/housing_security/"
        "nycha_tenants.py's puma.notna() filter doesn't catch it since it's "
        "non-null text, not NaN."
    ),
    "count_residential_evictions": (
        "Indexed by a different PUMA vintage (e.g. '03701') than the 2020 "
        "PUMAs (e.g. '04103') everything else uses."
    ),
}


@pytest.mark.parametrize("data, ind_name", by_puma)
def test_all_PUMAs_present(data, ind_name):
    if ind_name in KNOWN_BROKEN_PUMA_INDEX:
        pytest.xfail(KNOWN_BROKEN_PUMA_INDEX[ind_name])
    assert sorted(data.index.tolist()) == sorted(all_PUMAs), (
        f"not all PUMAs present for {ind_name}"
    )


@pytest.mark.parametrize("data, ind_name", by_borough)
def test_all_boroughs_present(data, ind_name):
    assert sorted(data.index.tolist()) == sorted(all_boroughs), (
        f"not all boroughs present for {ind_name}"
    )


@pytest.mark.parametrize("data, ind_name", by_citywide)
def test_citywide_single_index(data, ind_name):
    assert data.index.tolist() == ["citywide"], (
        f"citywide index incorrect  for {ind_name}"
    )


@pytest.mark.parametrize("data, ind_name", by_puma + by_borough + by_citywide)
def test_rv_dataframe(data, ind_name):
    assert isinstance(data, pd.DataFrame), f"{ind_name} returns incorrect type"
