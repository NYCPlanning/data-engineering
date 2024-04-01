from datetime import date
import geopandas as gpd
import pandas as pd
import pytest
from typing import cast
import yaml

from dcpy.models import file
from dcpy.models.lifecycle.ingest import Template
from dcpy.models.lifecycle.ingest import LocalFileSource, Config, FunctionCall
from dcpy.lifecycle.ingest import (
    configure,
    transform,
    PARQUET_PATH,
)

from dcpy.utils import data
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

        template = Template(
            name="test",
            acl="public-read",
            source=source,
            file_format=config,
        )

        ingest_config = configure.get_config(
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

    output_df = pd.read_parquet(PARQUET_PATH)
    raw_df = data.read_data_to_df(config=config, local_data_path=file_path)

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
