from datetime import date
import geopandas as gpd
import pandas as pd
import pytest
from typing import cast
import yaml

from dcpy.models import file
from dcpy.models.lifecycle.ingest import LocalFileSource, Config, FunctionCall
from dcpy.lifecycle.ingest import (
    metadata,
    transform,
    PARQUET_PATH,
)
from . import RESOURCES, TEST_DATA_DIR, FAKE_VERSION


def get_fake_data_configs():
    """
    Returns a list of Config objects
    that represent data files in resources/test_data directory
    """
    with open(RESOURCES / "transform_to_parquet_template.yml") as f:
        configs = yaml.safe_load(f)

    test_files = []

    for config in configs:
        match config["format"]:
            case "csv":
                file_name = "test.csv"
            case "shapefile":
                file_name = "test/test.shp"
            case "geodatabase":
                file_name = "test.gdb"
            case _:
                raise ValueError(f"Unknown data format {config['format']}")

        local_file_path = RESOURCES / TEST_DATA_DIR / file_name

        source = LocalFileSource(type="local_file", path=local_file_path)

        template = metadata.Template(
            name="test",
            acl="public-read",
            source=source,
            file_format=config,
        )

        ingest_config = metadata.get_config(
            template, version=FAKE_VERSION, timestamp=date.today(), file_name=file_name
        )
        test_files.append(ingest_config)

    return test_files


# TODO: implement tests for zip, json, and geojson format
@pytest.mark.parametrize("config", get_fake_data_configs())
def test_transform_to_parquet(config: Config):
    """
    Test the transform_to_parquet function.

    Checks:
        - Checks if the function creates expected parquet file.
        - Checks if the saved Parquet file contains the expected data.
    """

    source = cast(LocalFileSource, config.source)
    file_path = source.path

    transform.to_parquet(config=config, local_data_path=file_path)

    assert PARQUET_PATH.is_file()

    to_parquet_config = config.file_format

    match to_parquet_config:
        case file.Shapefile():
            raw_df = gpd.read_file(file_path)

        case file.Geodatabase():
            raw_df = gpd.read_file(file_path)

        case file.Csv() as csv:
            raw_df = pd.read_csv(file_path)

            # case when csv contains geospatial data. Convert to gpd dataframe
            if csv.geometry:
                geom_column = csv.geometry.geom_column
                crs = csv.geometry.crs

                if raw_df[geom_column].isnull().any():
                    raw_df[geom_column] = raw_df[geom_column].astype(object)
                    raw_df[geom_column] = raw_df[geom_column].where(
                        raw_df[geom_column].notnull(), None
                    )

                raw_df[geom_column] = gpd.GeoSeries.from_wkt(raw_df[geom_column])

                raw_df = gpd.GeoDataFrame(
                    raw_df,
                    geometry=geom_column,
                    crs=crs,
                )

    # translate geometry to wkb format to match parquet geom column
    if getattr(to_parquet_config, "geometry", None) or getattr(
        to_parquet_config, "crs", None
    ):
        raw_df = raw_df.to_wkb()

    output_df = pd.read_parquet(PARQUET_PATH)

    print(f"raw_df:\n{raw_df.head()}")
    print(f"\noutput_df:\n{output_df.head()}")

    pd.testing.assert_frame_equal(
        left=raw_df,
        right=output_df,
        check_dtype=False,
        check_like=True,  # ignores order of rows and columns
    )


def test_validate_function_args():
    """
    Test the validate_function_args function.

    Ensures that a variety of cases - missing args, unexpected args, mistyped args, etc are caught
    """
    # Variety of dummy data. All roughly follows same signature
    a = {"a": "a"}
    ab = {"a": "a", "b": 1}
    ab_wrong_type = {"a": 1, "b": "b", "c": True}
    abc = {"a": "a", "b": 1, "c": True}
    abcd = {"a": "a", "b": 1, "c": True, "d": "goodbye"}

    # Function with no arguments
    def noargs():
        return

    # No arguments passed
    assert not transform.validate_function_args(
        noargs, {}
    ), "No arguments supplied to noargs"
    # Unexpected argument returns value (error message not validated, just presence of it in returned dict)
    assert "a" in transform.validate_function_args(
        noargs, a
    ), "Unexpected argument to noargs not caught"

    # Function with positional arguments
    def args(a: str, b: int):
        return

    # Two valid arguments
    assert not transform.validate_function_args(
        args, ab
    ), "Both arguments supplied to args"
    # Missing argument is returned
    assert "b" in transform.validate_function_args(
        args, a
    ), "Missing argument to args not caught"
    # Type mismatch of arguments and additonal argument. All 3 errors should be present
    multiple_errors = transform.validate_function_args(args, ab_wrong_type)
    assert (
        "a" in multiple_errors and "b" in multiple_errors and "c" in multiple_errors
    ), "Type mismatch and unexpected argument to args not caught"

    # Function with positional arguments and one default value
    def args_default(a: str, b: int = 0):
        return

    # Default not supplied
    assert not transform.validate_function_args(
        args_default, a
    ), "Valid missing arg with default to args_default"
    # Default supplied
    assert not transform.validate_function_args(
        args_default, ab
    ), "Both args supplied to args_default"

    # Function with **kwargs
    def star_kwargs(a: str, b: int, **kwargs):
        return

    # No error with "unexpected" argument
    assert not transform.validate_function_args(
        star_kwargs, abc
    ), "Valid extra kwarg supplied to star_kwargs"

    # Function with kwargs
    def kwargs(a: str, b: int, *, c: bool):
        return

    multiple_errors = transform.validate_function_args(kwargs, a)
    assert (
        "b" in multiple_errors and "c" in multiple_errors
    ), "Missing positional and kwargs in kwargs not caught"
    assert not transform.validate_function_args(
        kwargs, abc
    ), "All arguments supplied to kwargs"

    # Function with *args. Not supported
    def star_args(a: str, *args):
        return

    with pytest.raises(TypeError, match="Positional args not supported"):
        transform.validate_function_args(star_args, abc)

    # Function with everything
    # For this, test both presence and absence of variables in returned dict
    def kwargs_default(a: str, b: int = 1, *, c: bool, d: str = "Hello!"):
        return

    empty = transform.validate_function_args(kwargs_default, {})
    assert "a" in empty and ("b" not in empty) and ("c" in empty) and (not "d" in empty)
    only_a = transform.validate_function_args(kwargs_default, a)
    assert (
        ("a" not in only_a)
        and ("b" not in only_a)
        and ("c" in only_a)
        and ("d" not in only_a)
    ), "Supplying only 'a' to kwargs_default should error"
    type_e = transform.validate_function_args(kwargs_default, ab_wrong_type)
    assert (
        ("a" in type_e)
        and ("b" in type_e)
        and ("c" not in type_e)
        and ("d" not in type_e)
    ), "Supplying wrong types to kwargs_default should error"
    assert not transform.validate_function_args(
        kwargs_default, abc
    ), "Supplying abc to kwargs_default should not error"
    assert not transform.validate_function_args(
        kwargs_default, abcd
    ), "Supplying abcd to kwargs_default should not error"


def test_validate_processing_steps():
    """
    Test the validate_processing_steps function.

    Currently tests using two defined preprocessin functions
    `no_arg_function` expects no args other than the transformation df
    `drop_columns` expects a single arg `columns`, a list of columns to drop
    """

    steps = [
        FunctionCall(name="no_arg_function"),
        FunctionCall(name="drop_columns", args={"columns": ["col1", "col2"]}),
    ]
    compiled_steps = transform.validate_processing_steps(steps)
    assert len(compiled_steps) == 2

    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6], "col3": [7, 8, 9]})
    for step in compiled_steps:
        df = step(df)
    assert len(df.columns) == 1

    # test invalid steps
    error_steps = [
        # Non-existent function
        FunctionCall(name="fake_function_name"),
        # Missing arg
        FunctionCall(name="drop_columns", args={}),
        # Unexpected arg
        FunctionCall(name="drop_columns", args={"columns": [0], "fake_arg": 0}),
    ]
    # validate each separately for ease of making sure that each throws an error
    for step in error_steps:
        with pytest.raises(Exception, match="Invalid preprocessing steps"):
            transform.validate_processing_steps([step])
