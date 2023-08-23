import pytest
import pandas as pd
import numpy as np
import copy

from library.helper.geocode_utils import GEOCODE_COLUMNS, parse_location
from library.geocode import geocode_records


def assert_dataframe_rows_equality(
    actual_dataframe: pd.DataFrame, expected_dataframe: pd.DataFrame
):
    assert len(actual_dataframe) == len(expected_dataframe)
    for row_index in actual_dataframe.index:
        pd.testing.assert_series_equal(
            actual_dataframe.loc[row_index],
            expected_dataframe.loc[row_index],
        )


@pytest.fixture
def example_cbbr_data_geocoded() -> pd.DataFrame:
    return pd.read_csv(
        "tests/test_data/geocoded_cbbr_data.csv",
        dtype=str,
    ).replace(np.nan, None)


@pytest.fixture
def example_cbbr_data() -> pd.DataFrame:
    geocoded = pd.read_csv(
        "tests/test_data/geocoded_cbbr_data.csv",
        dtype=str,
    ).replace(np.nan, None)
    return geocoded.drop(GEOCODE_COLUMNS, axis=1)


def test_validate_example_data(example_cbbr_data, example_cbbr_data_geocoded):
    assert isinstance(example_cbbr_data_geocoded, pd.DataFrame)
    assert (
        example_cbbr_data_geocoded["test_id"]
        .drop_duplicates()
        .equals(example_cbbr_data_geocoded["test_id"])
    )
    assert len(example_cbbr_data_geocoded.columns) - len(
        example_cbbr_data.columns
    ) == len(GEOCODE_COLUMNS)


# @pytest.mark.skip(reason="dev in progress")
def test_geocode(example_cbbr_data, example_cbbr_data_geocoded):
    parsed_example_cbbr_data = parse_location(example_cbbr_data)
    geocoded_data = geocode_records(parsed_example_cbbr_data)
    assert_dataframe_rows_equality(geocoded_data, example_cbbr_data_geocoded)
    pd.testing.assert_frame_equal(
        geocoded_data,
        example_cbbr_data_geocoded,
    )
