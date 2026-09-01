import pytest
from tests.general_indicator_tests.general_indicator_test_helpers import get_by_geo

by_puma, by_borough, by_citywide = get_by_geo()


@pytest.mark.parametrize("data, ind_name", by_puma + by_borough + by_citywide)
def test_measure_token_present(data, ind_name):
    for c in data.columns:
        assert any(
            token in c for token in ["count", "pct", "rate", "index", "median"]
        ), f"{ind_name} returns column {c} with no count or pct token"
