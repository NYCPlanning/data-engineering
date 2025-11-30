import csv
import importlib
import json
import os
import shutil
from datetime import datetime
from pathlib import Path

import pytest
import yaml
from moto import mock_aws

TEST_RESOURCES_PATH = Path(__file__).parent / "resources"

TEST_BUCKET = "test-bucket"
RECIPES_BUCKET = "test-recipes"
PUBLISHING_BUCKET = "test-publishing"
TEST_BUCKETS = [
    TEST_BUCKET,
    RECIPES_BUCKET,
    PUBLISHING_BUCKET,
]
os.environ["RECIPES_BUCKET"] = RECIPES_BUCKET
os.environ["PUBLISHING_BUCKET"] = PUBLISHING_BUCKET
os.environ["PRODUCT_METADATA_REPO_PATH"] = str(
    TEST_RESOURCES_PATH / "package_and_distribute" / "metadata_repo"
)

from dcpy import configuration
from dcpy.connectors.edm import packaging, publishing
from dcpy.lifecycle.builds import plan
from dcpy.models.lifecycle.builds import BuildMetadata
from dcpy.test.resources import package_and_distribute
from dcpy.utils import s3, versions

RESOURCES = Path(__file__).parent / "resources"
UTILS_RESOURCES = Path(__file__).parent / "utils" / "resources"


@pytest.fixture(scope="session", autouse=True)
def ensure_no_callouts():
    import socket

    def guard(*args, **kwargs):
        raise Exception("No internet allowed in the unit tests!")

    socket.socket = guard


@pytest.fixture(scope="function")
def resources_path():
    return TEST_RESOURCES_PATH


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
    with mock_aws():
        for bucket in TEST_BUCKETS:
            s3.client().create_bucket(Bucket=bucket)
        yield  ## the yield within the mock_aws() is key to persisting the mocked session


@pytest.fixture(scope="module")
def mock_data_constants():
    def _test_product(
        publish_key: publishing.PublishKey, package_key: packaging.DatasetPackageKey
    ) -> None:
        output_path = packaging.OUTPUT_ROOT_PATH / package_key.path
        shutil.copytree(
            packaging.DOWNLOAD_ROOT_PATH / publish_key.path,
            output_path,
            dirs_exist_ok=True,
        )
        os.rename(
            output_path / "file.csv",
            output_path / f"file_{package_key.version}.csv",
        )

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
        Path(__file__).parent / "lifecycle" / "builds" / "resources" / "recipe.yml"
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


class MockResponse:
    def __init__(self, content: bytes):
        self.content = content

    def json(self):
        return json.loads(self.content)

    def raise_for_status(self):
        pass


def mock_request_get(
    url: str, headers=None, auth=None, params: dict | None = None
) -> MockResponse:
    """
    Mocks calls to request.get

    To use, annotate test with
    @mock.patch("requests.get", side_effect=mock_request_get)
    If you do this, the first argument of the test MUST be a sort of "dummy" variable. This is
    meant more for if the mocked variable is a Class than a function. But if you attempt to use
    a fixture as the first argument to a function with this annotation, it will not actually be accessed.

    To use, add a new path and filename to the `test_files` dictionary and add the appropriate file
    (containing a plain string response) to resources/mocked_responses
    """

    test_files = {
        "https://www.bklynlibrary.org/locations/json": "bpl_libraries.json",
        "https://data.cityofnewyork.us/api/views/w7w3-xahh/rows.csv": "dca_operatingbusinesses.csv",
        "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/pad_24a.zip": "pad_24a.zip",
        "https://health.data.ny.gov/api/views/izta-vnpq/rows.csv": "nysdoh_nursinghomes.csv",
        "https://data.cityofnewyork.us/api/views/w7w3-xahh.json": "dca_operatingbusinesses_metadata.json",
        "https://services.arcgis.com/1xFZPtKn1wKC6POA/ArcGIS/rest/services/National_Register_Building_Listings/FeatureServer": "arcfs_metadata.json",
        "https://services.arcgis.com/1xFZPtKn1wKC6POA/ArcGIS/rest/services/National_Register_Building_Listings_Zero/FeatureServer": "arcfs_metadata_no_layers.json",
        "https://services.arcgis.com/1xFZPtKn1wKC6POA/ArcGIS/rest/services/National_Register_Building_Listings_Multiple/FeatureServer": "arcfs_metadata_multiple_layers.json",
        "https://services.arcgis.com/1xFZPtKn1wKC6POA/ArcGIS/rest/services/National_Register_Building_Listings/FeatureServer/13": "arcfs_layer_metadata.json",
    }

    error_urls = [
        "https://services.arcgis.com/1xFZPtKn1wKC6POA/ArcGIS/rest/services/error/FeatureServer",
        "https://services.arcgis.com/1xFZPtKn1wKC6POA/ArcGIS/rest/services/error/FeatureServer/13",
    ]

    if url in test_files:
        with open(
            TEST_RESOURCES_PATH / "mocked_responses" / test_files[url], "rb"
        ) as file:
            return MockResponse(file.read())
    elif url in error_urls:
        return MockResponse(b'{"error": "fake api error"}')

    raise Exception(f"Url {url} has not been configured with test data")


@pytest.fixture
def dev_flag():
    os.environ["DEV_FLAG"] = "true"
    importlib.reload(configuration)
    yield
    os.environ.pop("DEV_FLAG")
    importlib.reload(configuration)
