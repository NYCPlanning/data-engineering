import pytest
from pathlib import Path
import yaml
from datetime import date
import pandas as pd
import geopandas as gpd
from typing import cast

from dcpy.utils import s3
from dcpy.connectors.edm import publishing, recipes
from dcpy.extract import metadata, ingest, PARQUET_PATH

RESOURCES = Path(__file__).parent / "resources"
TEST_DATA_DIR = "test_data"
FAKE_VERSION = "v001"


def test_download_file(create_buckets, create_temp_filesystem: Path):
    with open(RESOURCES / "sources.yml") as f:
        sources = yaml.safe_load(f)
    s3.client().put_object(
        Bucket=publishing.BUCKET,
        Key="datasets/dcp_borough_boundary/24A/dcp_borough_boundary.zip",
    )
    for source in sources:
        if source["type"] == "local_file":
            tmp_file = create_temp_filesystem / "tmp.txt"
            tmp_file.touch()
            source["path"] = tmp_file
        template = metadata.Template(
            name="test",
            acl="public-read",
            source=source,
            transform_to_parquet_metadata=recipes.ExtractConfig.ToParquetMeta.Csv(
                format="csv"  # easiest to mock
            ),
        )

        file = ingest.download_file_from_source(
            template, "24a", dir=create_temp_filesystem
        )
        assert file.exists()


def get_fake_data_configs():
    """
    Returns a list of recipes.ExtractConfig objects
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

        source = recipes.ExtractConfig.Source.LocalFile(
            type="local_file", path=local_file_path
        )

        template = metadata.Template(
            name="test",
            acl="public-read",
            source=source,
            transform_to_parquet_metadata=config,
        )

        extract_config = metadata.get_config(
            template, version=FAKE_VERSION, timestamp=date.today(), file_name=file_name
        )
        test_files.append(extract_config)

    return test_files


# TODO: implement tests for zip, json, and geojson format
@pytest.mark.parametrize("config", get_fake_data_configs())
def test_transform_to_parquet(config: recipes.ExtractConfig):
    """
    Test the transform_to_parquet function.

    Checks:
        - Checks if the function creates expected parquet file.
        - Checks if the saved Parquet file contains the expected data.
    """

    source = cast(recipes.ExtractConfig.Source.LocalFile, config.source)
    file_path = source.path

    ingest.transform_to_parquet(config=config, local_data_path=file_path)

    assert PARQUET_PATH.is_file()

    to_parquet_config = config.transform_to_parquet_metadata

    match to_parquet_config:
        case recipes.ExtractConfig.ToParquetMeta.Shapefile() as shapefile:
            raw_df = gpd.read_file(file_path)

        case recipes.ExtractConfig.ToParquetMeta.Geodatabase() as geodatabase:
            raw_df = gpd.read_file(file_path)

        case recipes.ExtractConfig.ToParquetMeta.Csv() as csv:
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
