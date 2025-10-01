from pathlib import Path
import pytest
import time
import uuid

from dcpy import configuration
from dcpy.connectors.hybrid_pathed_storage import (
    StorageType,
    PathedStorageConnector,
)

EXISTING_TEST_ROOT = "integration_tests"


def test_path():
    return (
        Path(EXISTING_TEST_ROOT)
        / "edm"
        / "connectors"
        / f"{int(time.time())}_{str(uuid.uuid4())[:8]}"
    )


@pytest.fixture
def azure_storage_connector():
    conn = PathedStorageConnector.from_storage_kwargs(
        conn_type="az_test",
        storage_backend=StorageType.AZURE,
        az_container_name="test",
        root_folder=test_path(),
        _validate_root_path=False,
    )
    try:
        (conn.storage.root_path / ".keep").touch()
        assert conn.storage.root_path.exists()
        yield conn
    finally:
        conn.storage.root_path.rmtree()


@pytest.fixture
def s3_storage_connector():
    assert configuration.RECIPES_BUCKET, "Integration tests require an s3 bucket"
    assert configuration.RECIPES_BUCKET != configuration._DEFAULT_RECIPES_BUCKET, (
        "The Integration Recipe bucket should not be the production bucket"
    )
    conn = PathedStorageConnector.from_storage_kwargs(
        conn_type="s3_test",
        storage_backend=StorageType.S3,
        s3_bucket=configuration.RECIPES_BUCKET,
        root_folder=test_path(),
        _validate_root_path=False,
    )
    (conn.storage.root_path / ".keep").touch()

    try:
        yield conn
    finally:
        conn.storage.root_path.rmtree()
