import pytest
import pandera as pa
import pandas as pd
import yaml
from pydantic import TypeAdapter


from dcpy.models.dataset import Column, CheckAttributes
from dcpy.lifecycle.validate import pandera_utils

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
            pa.Check.greater_than(min_value="abc"),
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
