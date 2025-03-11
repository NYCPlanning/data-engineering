# test geocoding logic
import pytest
import pandas as pd


def assert_value_equality(actual_value, expected_value):
    if isinstance(expected_value, float):  # testing np.nan equality
        assert actual_value is expected_value
    else:
        assert actual_value == expected_value


@pytest.fixture
def example_cbbr_data():
    return pd.DataFrame.from_dict(
        {
            "test_id": [
                "empty_location",
                "invalid_location",
                "site_name_only",
                "no_site_name",
            ],
            "location": [
                None,
                ";",
                "Site Name: Linden Park Comfort Station",
                "Street Name: Atlantic Avenue;    Cross Street 1: Crescent Street;",
            ],
        }
    )


@pytest.fixture
def example_cbbr_data_parsed():
    return pd.DataFrame.from_dict(
        {
            "test_id": [
                "empty_location",
                "invalid_location",
                "site_name_only",
                "no_site_name",
            ],
            "location": [
                None,
                ";",
                "Site Name: Linden Park Comfort Station",
                "Street Name: Atlantic Avenue;    Cross Street 1: Crescent Street;",
            ],
            "cross_street_2": [
                None,
                None,
                None,
                None,
            ],
            "cross_street_1": [
                None,
                None,
                None,
                "Crescent Street",
            ],
            "address": [
                None,
                None,
                None,
                "Atlantic Avenue",
            ],
            "site_or_facility_name": [
                None,
                None,
                "Linden Park Comfort Station",
                None,
            ],
        }
    )


def test_validate_example_data(example_cbbr_data, example_cbbr_data_parsed):
    assert isinstance(example_cbbr_data, pd.DataFrame)
    assert (
        example_cbbr_data["test_id"]
        .drop_duplicates()
        .equals(example_cbbr_data["test_id"])
    )
    assert isinstance(example_cbbr_data_parsed, pd.DataFrame)
    assert (
        example_cbbr_data_parsed["test_id"]
        .drop_duplicates()
        .equals(example_cbbr_data_parsed["test_id"])
    )
