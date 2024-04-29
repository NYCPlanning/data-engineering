import csv
from datetime import datetime
import json
from moto import mock_aws
import os
from pathlib import Path
import pytest
import shutil
import yaml

from dcpy.utils import s3
from dcpy.models.lifecycle.builds import BuildMetadata
from dcpy.connectors.edm import recipes, publishing, packaging
from dcpy.lifecycle.builds import plan


TEST_BUCKET = "test-bucket"
TEST_BUCKETS = [TEST_BUCKET, publishing.BUCKET, packaging.BUCKET, recipes.BUCKET]
RESOURCES = Path(__file__).parent / "resources"


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
        publish_key: publishing.PublishKey, package_key: packaging.PackageKey
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
    test_version = "v001"
    constants = {
        "TEST_PRODUCT_NAME": test_product_name,
        "TEST_PACKAGE_METADATA": packaging.PackageMetadata(
            "dcp_test_product",
            _test_product,
        ),
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


@pytest.fixture(scope="module")
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
            yaml.dump(build_metadata.model_dump(), f)
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


def mock_request_get(url):
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

    class MockResponse:
        def __init__(self, content: bytes):
            self.content = content

        def json(self):
            return json.loads(self.content)

        def raise_for_status(self):
            pass

    test_files = {
        "https://www.bklynlibrary.org/locations/json": "bpl_libraries.json",
        "https://data.cityofnewyork.us/api/views/w7w3-xahh/rows.csv": "dca_operatingbusinesses.csv",
        "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/pad_24a.zip": "pad_24a.zip",
        "https://health.data.ny.gov/api/views/izta-vnpq/rows.csv": "nysdoh_nursinghomes.csv",
        "https://data.cityofnewyork.us/api/views/w7w3-xahh.json": "dca_operatingbusinesses_metadata.json",
    }

    if url not in test_files:
        raise Exception(f"Url {url} has not been configured with test data")

    with open(RESOURCES / "mocked_responses" / test_files[url], "rb") as file:
        return MockResponse(file.read())
