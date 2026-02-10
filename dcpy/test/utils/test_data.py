import tempfile
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from dcpy.utils import data


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

        assert extracted_files == {unzipped_filename}
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


def test_upsert_df_columns():
    on = ["key1", "key2"]
    df = pd.DataFrame(
        {
            "key1": ["foo", "bar", "bar"],
            "key2": [1, 1, 2],
            "other": [True, False, True],
            "value": [1, 2, 3],
        }
    )
    df_upsert = pd.DataFrame(
        {
            "key1": ["bar", "foo", "bar"],
            "key2": [2, 1, 1],
            "value": [6, 4, 5],
            "value2": [7, 8, 9],
        }
    )

    with pytest.raises(ValueError):
        data.upsert_df_columns(df, df_upsert, key=on, insert_behavior="error")

    updated = data.upsert_df_columns(df, df_upsert, key=on, insert_behavior="ignore")
    assert updated.equals(
        pd.DataFrame(
            {
                "key1": ["foo", "bar", "bar"],
                "key2": [1, 1, 2],
                "other": [True, False, True],
                "value": [4, 5, 6],
            }
        )
    )

    inserted = data.upsert_df_columns(df, df_upsert, key=on)
    assert inserted.equals(
        pd.DataFrame(
            {
                "key1": ["foo", "bar", "bar"],
                "key2": [1, 1, 2],
                "other": [True, False, True],
                "value": [4, 5, 6],
                "value2": [8, 9, 7],
            }
        )
    )

    df_missing_key_column = pd.DataFrame({"key1": ["bar", "foo"], "value": [6, 5]})
    with pytest.raises(ValueError):
        data.upsert_df_columns(df, df_missing_key_column, key=on)

    df_multiple = pd.DataFrame(
        {
            "key1": ["bar", "foo", "bar", "foo"],
            "key2": [2, 1, 1, 1],
            "value": [6, 4, 5, 3],
            "value2": [7, 8, 9, 10],
        }
    )
    with pytest.raises(ValueError):
        data.upsert_df_columns(df, df_multiple, key=on)
    upserted_multiple = data.upsert_df_columns(
        df, df_multiple, key=on, allow_duplicate_keys=True
    )
    assert upserted_multiple.equals(
        pd.DataFrame(
            {
                "key1": ["foo", "foo", "bar", "bar"],
                "key2": [1, 1, 1, 2],
                "other": [True, True, False, True],
                "value": [4, 3, 5, 6],
                "value2": [8, 10, 9, 7],
            }
        )
    )

    df_missing_key = pd.DataFrame(
        {"key1": ["bar", "foo"], "key2": [2, 1], "value": [6, 4]}
    )
    with pytest.raises(ValueError):
        data.upsert_df_columns(df, df_missing_key, key=on)
    updated_with_missing = data.upsert_df_columns(
        df, df_missing_key, key=on, missing_key_behavior="null"
    )
    assert updated_with_missing.equals(
        pd.DataFrame(
            {
                "key1": ["foo", "bar", "bar"],
                "key2": [1, 1, 2],
                "other": [True, False, True],
                "value": [4, None, 6],
            }
        )
    )

    updated_with_missing_coalesced = data.upsert_df_columns(
        df, df_missing_key, key=on, missing_key_behavior="coalesce"
    )
    assert updated_with_missing_coalesced.equals(
        pd.DataFrame(
            {
                "key1": ["foo", "bar", "bar"],
                "key2": [1, 1, 2],
                "other": [True, False, True],
                "value": [4, 2, 6],
            }
        )
    )
