import pandera as pa
import pandas as pd
import geopandas as gpd
from inspect import (
    signature,
)  # used for checking expected attributes in a class signuture

from dcpy.models.dataset import Column, CheckAttributes


def create_check(check: str | dict[str, CheckAttributes]) -> pa.Check:
    """
    Creates a Pandera `Check` object from a given check definition.

    Args:
        check:
            A string representing the name of the check or a dictionary with the
            check name as the key and a dictionary of attributes as the value.
    Returns:
        pa.Check:
            A Pandera `Check` object constructed with the specified parameters.
    Raises:
        AssertionError:
            If the `check` dictionary does not contain exactly one key-value pair.
        ValueError:
            If the check name is not registered or if attributes cannot be parsed
            or used to create a valid `Check`.
    """
    allowed_check_names = {
        **pa.Check.CHECK_FUNCTION_REGISTRY,
        **pa.Check.REGISTERED_CUSTOM_CHECKS,
    }

    if isinstance(check, str):
        check_name = check
        check_args = None
    elif isinstance(check, dict):
        assert (
            len(check) == 1
        ), "`utils.create_pa_check` expects exactly 1 key-value pair in `check` param."
        check_name, check_args = next(iter(check.items()))

    if check_name not in allowed_check_names:
        raise ValueError(f"Unregistered check name: '{check_name}'.")

    pandera_registered_check = getattr(pa.Check, check_name)

    if check_args:
        check_expected_params = signature(pandera_registered_check).parameters
        invalid_check_keys = set(check_args.args.keys()) - set(
            check_expected_params.keys()
        )
        if invalid_check_keys:
            raise ValueError(
                f"Invalid argument keys found for check '{check_name}': {invalid_check_keys}. "
                f"Valid argument keys are: {sorted(check_expected_params.keys())}."
            )

    try:
        check_obj = (
            pandera_registered_check(
                **check_args.args,
                raise_warning=check_args.warn_only,
                description=check_args.description,
            )
            if check_args
            else pandera_registered_check()
        )
    except Exception as e:
        raise ValueError(
            f"Check '{check_name}' couldn't be created. Error message: {e}"
        )

    return check_obj


def create_checks(checks: list[str | dict[str, CheckAttributes]]) -> list[pa.Check]:
    """Create Pandera checks."""
    checks = [create_check(check) for check in checks]
    return checks


def create_column_with_checks(column: Column) -> pa.Column:
    """Create Pandera column validator object."""
    data_checks = create_checks(column.checks) if column.checks else None
    return pa.Column(
        # TODO: implement `dtype` param
        coerce=True,  # coerce column to defined data type. This decision is up for debate
        checks=data_checks,
        required=column.is_required,
        description=column.description,
        nullable=True,  # TODO: temp solution. Need to figure out what to do with this (equivalent to can be null)
    )


def run_data_checks(
    df: pd.DataFrame | gpd.GeoDataFrame, columns: list[Column]
) -> pd.DataFrame | gpd.GeoDataFrame:
    """
    Validate a DataFrame or GeoDataFrame against a schema defined by a list of columns with Pandera.

    Args:
        df (pd.DataFrame | gpd.GeoDataFrame): The input DataFrame to validate.
        columns (list[Column]): List of column definitions specifying validation rules.

    Raises:
        AssertionError: If column names in `columns` are not unique.
    """

    column_names = [column.id for column in columns]
    assert len(column_names) == len(
        set(column_names)
    ), "Columns should have unique names"

    dataframe_checks = {}
    for column in columns:
        dataframe_checks[column.id] = create_column_with_checks(column)

    return pa.DataFrameSchema(dataframe_checks).validate(df)
