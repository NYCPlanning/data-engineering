import os
import pytest
from pathlib import Path
import shutil
import csv
from moto import mock_s3

from dcpy.utils import s3
from dcpy.connectors.edm import publishing, packaging


TEST_BUCKET = "test-bucket"
TEST_BUCKETS = [
    TEST_BUCKET,
    publishing.BUCKET,
    packaging.BUCKET,
]


@pytest.fixture(scope="session")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    if "AWS_S3_ENDPOINT" in os.environ:
        os.environ.pop("AWS_S3_ENDPOINT")


@pytest.fixture(scope="module")
def create_buckets(aws_credentials):
    """Creates a test S3 bucket."""
    with mock_s3():
        for bucket in TEST_BUCKETS:
            s3.client().create_bucket(Bucket=bucket)
        yield  ## the yield within the mock_s3() is key to persisting the mocked session


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
            test_product_name,
            "dcp_test_product",
            _test_product,
        ),
        "TEST_DATA_DIR": Path(__file__).resolve().parent / "test_data",
        "TEST_BUILD": "build-branch",
        "TEST_VERSION": test_version,
        "TEST_VERSION_FILE": "version.txt",
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

    try:
        txt_file_path = data_path / version_file
        csv_file_path = data_path / test_file
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
