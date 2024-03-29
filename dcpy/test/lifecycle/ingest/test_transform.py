from datetime import date
import geopandas as gpd
import pandas as pd
import pytest
from typing import cast
import yaml

from dcpy.models import file
from dcpy.models.lifecycle.ingest import (
    LocalFileSource,
    Config,
)
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
