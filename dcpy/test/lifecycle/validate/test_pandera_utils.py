from inspect import (
    signature,
)  # used for checking expected attributes in a class signuture

import pandas as pd
import pandera as pa
import pytest
import yaml
from pydantic import TypeAdapter

from dcpy.lifecycle.validate import pandera_utils
from dcpy.models.dataset import CheckAttributes, Column

from . import RESOURCES


def get_valid_checks():
    """A list of test models.dataset.Column.checks objects."""
    with open(RESOURCES / "valid_data_checks.yml") as f:
        data_checks = TypeAdapter(
            list[str | dict[str, CheckAttributes]]
        ).validate_python(yaml.safe_load(f))
    return data_checks


def get_invalid_checks():
    """A list of test models.dataset.Column.checks objects."""
    with open(RESOURCES / "invalid_data_checks.yml") as f:
        data_checks = TypeAdapter(
            list[str | dict[str, CheckAttributes]]
        ).validate_python(yaml.safe_load(f))
    return data_checks


valid_data_checks = get_valid_checks()
invalid_data_checks = get_invalid_checks()


@pytest.mark.parametrize(
    "input_data, expected_pa_check",
    [
        (
            valid_data_checks[0],
            pa.Check.in_range(
                min_value=100,
                max_value=200,
                raise_warning=True,
                description="My custom check description.",
            ),
        ),
        (
            valid_data_checks[1],
            pa.Check.greater_than(min_value=5, raise_warning=False),
        ),
        (
            valid_data_checks[2],
            pa.Check.greater_than(min_value="abc", raise_warning=False),
        ),
        (
            valid_data_checks[3],
            pa.Check.greater_than(
                min_value=1,
                raise_warning=True,
                name="greater than",
                title="My greater than check",
                n_failure_cases=1,
                ignore_na=False,
                groups="col_a",
                groupby=["col_a", "col_b"],
            ),
        ),
        # TODO: add custom registered check
    ],
)
def test_create_check_success(input_data, expected_pa_check):
    actual_check = pandera_utils.create_check(input_data)
    assert actual_check == expected_pa_check


@pytest.mark.parametrize(
    "input_data, error_message_substring",
    [
        (
            invalid_data_checks[0],
            "Unregistered check name",
        ),
        (
            invalid_data_checks[1],
            "Unregistered check name",
        ),
        (
            invalid_data_checks[2],
            "couldn't be created",
        ),
        (
            invalid_data_checks[3],
            "Invalid argument keys found for check",
        ),
    ],
)
def test_create_check_failure(input_data, error_message_substring):
    with pytest.raises(ValueError, match=error_message_substring):
        pandera_utils.create_check(input_data)


def test_check_attributes_consistency_with_pa_check():
    """
    Ensures that all attributes in CheckAttributes.model_fields (excluding "args")
    are valid parameters for the pa.Check class.

    This test is necessary because feeding invalid attributes to pa.Check() does
    not raise an error, which could lead to silent failures. It is particularly
    important for long-term stability, as Pandera's API may change before it
    reaches version 1.0, including potential renaming of pa.Check parameters.
    """
    check_attributes = pandera_utils.CheckAttributes.model_fields

    # rename check_attributes keys to match with pandera keys
    check_attributes["check_kwargs"] = check_attributes.pop("args")
    check_attributes["raise_warning"] = check_attributes.pop("warn_only")

    check_expected_params = signature(pa.Check).parameters

    invalid_check_keys = set(check_attributes.keys()) - set(
        check_expected_params.keys()
    )
    assert len(invalid_check_keys) == 0


def get_valid_columns():
    """A list of test models.dataset.Column objects."""
    with open(RESOURCES / "valid_columns_with_checks.yml") as f:
        columns = TypeAdapter(list[Column]).validate_python(yaml.safe_load(f))
    return columns


@pytest.mark.parametrize(
    "test_df",
    [
        pd.DataFrame(
            {
                "bbl": ["1000157502", "1000157501", None],
                "custom_value": [1, 5, 10],
            }
        ),
        pd.DataFrame(
            {
                "bbl": ["1000157502", "1000157501", None],
                "custom_value": [1, 5, 10],
                "extra_column": ["a", "b", None],
            }
        ),  # df with a column that doesn't have data checks
        pd.DataFrame(
            {
                "bbl": ["1000157502", "1000157501", None, None],
                "custom_value": [None, 1, 5, 10],
            }
        ),  # df with warning only data check
    ],
)
def test_run_data_checks_success(test_df):
    """
    Test the `run_data_checks` function for passing, warning, and expected failing scenarios.

    Verifies:
        - Valid data passes checks.
        - Data with extra columns passes checks.
        - Data with warnings still passes checks.
    """
    columns = get_valid_columns()
    pandera_utils.run_data_checks(df=test_df, columns=columns)


def test_run_data_checks_fail():
    """Test that data fails data checks as expected."""
    columns = get_valid_columns()
    data_checks_fail = pd.DataFrame(
        {"bbl": ["1000150002", "1000157501"], "custom_value": [0, 1]}
    )
    with pytest.raises(
        pa.errors.SchemaError, match="failed element-wise validator number 0"
    ):
        pandera_utils.run_data_checks(df=data_checks_fail, columns=columns)


def test_run_data_checks_duplicate_columns_error():
    """Test the `run_data_checks` function for handling duplicate column names."""

    columns = get_valid_columns()
    df = pd.DataFrame(
        {"bbl": ["1000157502", "1000157501", None], "custom_value": [1, 5, 10]}
    )
    # create a fail test case with same column names
    for col in columns:
        col.id = "duplicate_name"

    with pytest.raises(AssertionError, match="Columns should have unique names"):
        pandera_utils.run_data_checks(df=df, columns=columns)
