import csv
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pytest
import yaml

# Must be set before dcpy.configuration is imported anywhere in this test run: it
# otherwise defaults BUILD_ENGINE_SCHEMA to the sanitized git branch name, which makes
# any test that reaches the build-schema fallback pass or fail depending on the branch
# it runs on.
os.environ["RECIPES_BUCKET"] = "test-recipes"
os.environ["PUBLISHING_BUCKET"] = "test-publishing"
os.environ["BUILD_ENGINE_SCHEMA"] = "unit_tests"

from dcpy.lifecycle.builds import plan
from dcpy.lifecycle.builds.models import BuildMetadata
from dcpy.utils import s3, versions

RESOURCES = Path(__file__).parent / "resources"
UTILS_RESOURCES = RESOURCES

TEST_BUCKET = "test-bucket"
RECIPES_BUCKET = "test-recipes"
PUBLISHING_BUCKET = "test-publishing"
TEST_BUCKETS = [TEST_BUCKET, RECIPES_BUCKET, PUBLISHING_BUCKET]

# package_and_distribute is a test-support module (path constants), not part of the
# dcpy namespace - it lives alongside this conftest rather than being importable as a
# proper dependency, so it needs its own directory on sys.path.
sys.path.insert(0, str(RESOURCES))
import package_and_distribute  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def ensure_no_callouts():
    import socket

    def guard(*args, **kwargs):
        raise Exception("No internet allowed in the unit tests!")

    socket.socket = guard


@pytest.fixture(scope="function")
def resources_path():
    return RESOURCES


@pytest.fixture(scope="function")
def utils_resources_path():
    return UTILS_RESOURCES


@pytest.fixture(scope="function")
def package_and_dist_test_resources():
    return package_and_distribute


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    if "AWS_S3_ENDPOINT" in os.environ:
        os.environ.pop("AWS_S3_ENDPOINT")
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"


@pytest.fixture(scope="function")
def create_buckets(aws_credentials):
    """Creates a test S3 bucket."""
    from moto import mock_aws

    with mock_aws():
        for bucket in TEST_BUCKETS:
            s3.client().create_bucket(Bucket=bucket)
        yield  ## the yield within the mock_aws() is key to persisting the mocked session


@pytest.fixture(scope="module")
def mock_data_constants():
    test_product_name = "test-product"
    test_version = versions.MajorMinor(year=24, major=2).label
    constants = {
        "TEST_PRODUCT_NAME": test_product_name,
        "TEST_PACKAGE_DATASET": "test_package_dataset",
        "TEST_PACKAGE_NAME": "dcp_test_product",
        "TEST_DATA_DIR": Path(__file__).resolve().parent / "test_data",
        "TEST_BUILD": "build-branch",
        "TEST_VERSION": test_version,
        "TEST_VERSION_FILE": "version.txt",
        "TEST_BUILD_METADATA": "build_metadata.json",
        "TEST_FILE": "file.csv",
        "TEST_PACKAGED_FILE": f"file_{test_version}.csv",
        "TEST_DATA_FIELDS": ["drink", "like"],
        "TEST_DATA": [
            {"drink": "coffee", "like": "yes"},
            {"drink": "tea", "like": "maybe"},
        ],
    }
    yield constants


@pytest.fixture(scope="function")
def create_temp_filesystem(mock_data_constants):
    """Creates a new directory with files and removes it upon test completion.
    The directory is created and removed once per script ('module' scope)."""

    data_path = mock_data_constants["TEST_DATA_DIR"]
    version = mock_data_constants["TEST_VERSION"]
    version_file = mock_data_constants["TEST_VERSION_FILE"]
    build_metadata_file = mock_data_constants["TEST_BUILD_METADATA"]
    test_file = mock_data_constants["TEST_FILE"]
    file_columns = mock_data_constants["TEST_DATA_FIELDS"]
    file_data = mock_data_constants["TEST_DATA"]

    if data_path.exists():
        shutil.rmtree(data_path)

    try:
        data_path.mkdir(parents=False, exist_ok=False)
    except Exception as err:
        print("❌ Unable to create test dir.")
        raise err

    test_recipe = plan.recipe_from_yaml(
        Path(__file__).parent / "builds" / "resources" / "recipe.yml"
    )
    build_metadata = BuildMetadata(
        timestamp=datetime.now(), version=version, recipe=test_recipe
    )

    try:
        txt_file_path = data_path / version_file
        csv_file_path = data_path / test_file
        with open(txt_file_path, "w") as txt_file:
            txt_file.write(version)
        with open(csv_file_path, "w", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=file_columns)
            writer.writeheader()
            writer.writerows(file_data)
        with open(data_path / build_metadata_file, "w") as f:
            yaml.dump(build_metadata.model_dump(mode="json"), f)
        print("Created test filesystem ✅")

    except Exception as exc:
        print("❌ Exception occured while creating test files. Deleting test dir...")
        shutil.rmtree(data_path)
        raise exc

    yield data_path

    try:
        shutil.rmtree(data_path)
        print("Removed test filesystem ✅")
    except Exception as e:
        f"❌ Unable to remove {data_path} after running tests"
        raise e


@pytest.fixture(scope="class")
def create_temp_filesystem_class(request, mock_data_constants, create_temp_filesystem):
    request.cls.temp_filesystem = create_temp_filesystem
