import geopandas as gpd
import pandas as pd
import pytest
import yaml
from pydantic import TypeAdapter, BaseModel

from dcpy.models.file import Format
from dcpy.models.lifecycle.ingest import FunctionCall
from dcpy.lifecycle.ingest import (
    transform,
    PARQUET_PATH,
)

from dcpy.utils import data
from . import RESOURCES, TEST_DATA_DIR


class TestConfig(BaseModel):
    """
    Test pydentic class used to validate input yaml file
    """

    file_name: str
    to_parquet_config: Format


def get_fake_data_configs():
    """
    Returns a list of dicts that represent data files in resources/test_data directory.
    Each dict contains a file.Format object and a path to the test data.
    """
    with open(RESOURCES / "transform_to_parquet_template.yml") as f:
        configs = TypeAdapter(list[TestConfig]).validate_python(yaml.safe_load(f))

    test_files = []

    for config in configs:
        format = config.to_parquet_config
        file_name = config.file_name
        local_file_path = RESOURCES / TEST_DATA_DIR / file_name

        test_files.append({"format": format, "local_file_path": local_file_path})

    return test_files


# TODO: implement tests for json, and geojson format
@pytest.mark.parametrize("file", get_fake_data_configs())
def test_to_parquet(file: dict):
    """
    Test the to_parquet function.

    Checks:
        - Checks if the function creates expected parquet file.
        - Checks if the saved Parquet file contains the expected data.
    """

    transform.to_parquet(
        file_format_config=file["format"], local_data_path=file["local_file_path"]
    )

    assert PARQUET_PATH.is_file()

    output_df = pd.read_parquet(PARQUET_PATH)
    raw_df = data.read_data_to_df(
        data_format=file["format"], local_data_path=file["local_file_path"]
    )

    # rename geom column & translate geometry to wkb format to match parquet geom column
    if isinstance(raw_df, gpd.GeoDataFrame):
        raw_df = raw_df.rename_geometry(transform.OUTPUT_GEOM_COLUMN)
        raw_df = raw_df.to_wkb()

    print(f"raw_df:\n{raw_df.head()}")
    print(f"\noutput_df:\n{output_df.head()}")

    pd.testing.assert_frame_equal(
        left=raw_df,
        right=output_df,
        check_dtype=False,
        check_like=True,  # ignores order of rows and columns
    )


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
