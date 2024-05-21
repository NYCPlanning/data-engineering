import pytest
import tempfile
import zipfile
from pathlib import Path
import pandas as pd
from geopandas import GeoDataFrame

from dcpy.models import file
from dcpy.utils import data

RESOURCES_DIR = Path(__file__).parent / "resources"


@pytest.fixture
def temp_zip_file():
    """
    Creates a temporary directory with text file and zipped text file.
    """

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        zip_filepath = temp_dir / "test.zip"
        unzipped_filename = "test.txt"
        unzipped_filepath = temp_dir / unzipped_filename

        with open(unzipped_filepath, "w") as f:
            f.write("Hello, world!")

        with zipfile.ZipFile(zip_filepath, "w") as zip:
            zip.write(unzipped_filepath, unzipped_filename)

        yield zip_filepath, unzipped_filename


def test_unzip_file(temp_zip_file):
    """
    Test the unzip_file function.

    Checks:
        - Checks if the function returns a list of expected file names.
        - Checks if the expected file exists in the filesystem.
    """

    zip_filepath, unzipped_filename = temp_zip_file

    with tempfile.TemporaryDirectory() as output_dir:
        output_dir = Path(output_dir)
        extracted_files = data.unzip_file(
            zipped_filename=zip_filepath, output_dir=output_dir
        )

        expected_file_path = output_dir / unzipped_filename

        assert extracted_files == [unzipped_filename]
        assert expected_file_path.exists()


def test_serialize_nested_objects():
    test_data = [
        {
            "boro_code": 4,
            "location": {"bbl": 4469310598},
            "details": {"text": "GsifrlkxmckyxrKHjGsr", "wkt": None},
        },
        {
            "boro_code": None,
            "location": {"bbl": 5192630318},
            "details": {"text": None, "wkt": "POINT (10.3894635 -175.008089)"},
        },
    ]
    df = pd.DataFrame(test_data)
    serialized_df = data.serialize_nested_objects(df)

    # Check if nested structures are serialized as JSON strings
    for col in ["location", "details"]:
        for value in serialized_df[col]:
            assert isinstance(value, str)
            assert value.startswith("{")


def test_read_parquet():
    df = data.read_parquet(RESOURCES_DIR / "simple.parquet")
    assert not isinstance(df, GeoDataFrame)

    gdf = data.read_parquet(RESOURCES_DIR / "geo.parquet")
    assert isinstance(gdf, GeoDataFrame)
