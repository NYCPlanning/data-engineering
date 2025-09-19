import os
from pathlib import Path
import pytest
import time

from dcpy.connectors.hybrid_pathed_storage import (
    StorageType,
    PathedStorageConnector,
)

EXISTING_TEST_ROOT = "integration_tests"
CONNECTOR_TEST_PATH = "edm/connectors"


def test_path():
    return Path(EXISTING_TEST_ROOT) / "edm" / "connectors" / str(int(time.time()))


@pytest.fixture
def azure_storage_connector():
    # make this one just to create the test dir that
    # we'll hand over for the test.
    conn = PathedStorageConnector(
        conn_type="az_test",
        storage_backend=StorageType.AZURE,
        az_container_name="test",
        root_folder=EXISTING_TEST_ROOT,
    )
    # Create a unique directory name using epoch time
    temp_test_dir = test_path()
    (conn.storage.root_path / temp_test_dir).mkdir(parents=True)

    test_connector = PathedStorageConnector(
        conn_type="az_test",
        storage_backend=StorageType.AZURE,
        az_container_name="test",
        root_folder=temp_test_dir,
    )
    try:
        yield test_connector
    finally:
        test_connector.storage.root_path.rmtree()


@pytest.fixture
def s3_storage_connector():
    # make this one just to create the test dir that
    # we'll hand over for the test.

    s3_bucket = os.getenv("TEST_S3_BUCKET")
    assert s3_bucket, "Integration tests require a s3 bucket to be specified"
    # temp_test_dir = test_path()
    conn = PathedStorageConnector(
        conn_type="s3_test",
        storage_backend=StorageType.S3,
        s3_bucket=s3_bucket,  # TODO: TEMP. Change
        root_folder=test_path(),
        _validate_root_path=False,
    )
    (conn.storage.root_path / ".keep").touch()

    try:
        yield conn
    finally:
        pass
        # conn.storage.root_path.rmtree()
