import pytest
from pathlib import Path
import uuid  # for generating a unique temp directory name
import os
import shutil
import csv
from dcpy.connectors.edm import publishing


@pytest.fixture(scope="module")
def mock_data_constants():
    constants = {
        "TEST_DATA_DIR": Path(__file__).resolve().parent / str(uuid.uuid4()),
        "TEST_VERSION": "v001",
        "TEST_VERSION_FILE": "version.txt",
        "TEST_FILE": "file.csv",
        "TEST_DATA_FIELDS": ["drink", "like"],
        "TEST_DATA": [
            {"drink": "coffee", "like": "yes"},
            {"drink": "tea", "like": "maybe"},
        ],
    }
    yield constants


@pytest.fixture(scope="module")
def create_temp_filesystem(mock_data_constants):
    """Creates a new directory with files and removes it upon test completion.
    The directory is created and removed once per script ('module' scope)."""

    data_path = mock_data_constants["TEST_DATA_DIR"]
    version = mock_data_constants["TEST_VERSION"]
    version_file = mock_data_constants["TEST_VERSION_FILE"]
    test_file = mock_data_constants["TEST_FILE"]
    file_columns = mock_data_constants["TEST_DATA_FIELDS"]
    file_data = mock_data_constants["TEST_DATA"]

    try:
        data_path.mkdir(parents=False, exist_ok=False)
    except Exception as err:
        print("❌ Unable to create test dir.")
        raise err

    try:
        txt_file_path = os.path.join(data_path, version_file)
        csv_file_path = os.path.join(data_path, test_file)
        with open(txt_file_path, "w") as txt_file:
            txt_file.write(version)
        with open(csv_file_path, "w", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=file_columns)
            writer.writeheader()
            writer.writerows(file_data)
        print("Created test filesystem ✅")

    except Exception as exc:
        print("❌ Exception occured while creating test files. Deleting test dir...")
        shutil.rmtree(data_path)
        raise exc

    yield

    try:
        shutil.rmtree(data_path)
        print("Removed test filesystem ✅")
    except Exception as e:
        f"❌ Unable to remove {data_path} after running tests"
        raise e
