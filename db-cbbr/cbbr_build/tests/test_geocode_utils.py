# test geocoding logic
import pytest
import pandas as pd
import numpy as np

from library.helper.geocode_utils import (
    get_location_value_from_end,
    remove_location_value_from_end,
    parse_location,
)


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
            "and_cross_street_2": [
                None,
                None,
                None,
                None,
            ],
            "between_cross_street_1": [
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
            "facility_or_park_name": [
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


@pytest.mark.parametrize(
    "location_value, expected_value",
    [
        (None, None),
        (None, None),
        ("", None),
        ("a", None),
        ("Site Name: A", "A"),
        ("Site Name: Park A ", "Park A"),
        ("Site Name: Park A;", "Park A"),
        ("Site Name: Park A Cross Street 1: B Drive", "Park A Cross Street 1: B Drive"),
        ("Cross Street 1: B Drive", None),
    ],
)
def test_get_site_name(location_value, expected_value):
    location_value_prefix = "Site Name:"
    assert_value_equality(
        get_location_value_from_end(location_value, location_value_prefix),
        expected_value,
    )


@pytest.mark.parametrize(
    "location_value, expected_value",
    [
        (None, None),
        (None, None),
        ("", None),
        ("a", None),
        ("Site Name: Park A ", None),
        ("Cross Street 1: B Drive", "B Drive"),
        (
            "Cross Street 1: B Drive Cross Street 2: C Drive",
            "B Drive Cross Street 2: C Drive",
        ),
        ("Site Name: Park A Cross Street 1: B Drive", "B Drive"),
    ],
)
def test_get_cross_street_1(location_value, expected_value):
    location_value_prefix = "Cross Street 1:"
    assert_value_equality(
        get_location_value_from_end(location_value, location_value_prefix),
        expected_value,
    )


@pytest.mark.parametrize(
    "location_value, expected_value",
    [
        (None, None),
        (None, None),
        ("", ""),
        ("a", "a"),
        ("Site Name: Park A ", "Site Name: Park A"),
        ("Site Name: Park A Cross Street 1: B Drive", "Site Name: Park A"),
        (" Cross Street 1: B Drive", ""),
    ],
)
def test_remove_cross_street_1_name(location_value, expected_value):
    location_value_prefix = "Cross Street 1:"
    assert_value_equality(
        remove_location_value_from_end(location_value, location_value_prefix),
        expected_value,
    )


def test_parse_location_site(example_cbbr_data, example_cbbr_data_parsed):
    parsed_data = parse_location(example_cbbr_data)

    pd.testing.assert_frame_equal(parsed_data, example_cbbr_data_parsed)
