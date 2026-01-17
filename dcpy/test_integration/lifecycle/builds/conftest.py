import pytest

from dcpy.connectors.edm.builds import BuildsConnector
from dcpy.connectors.edm.drafts import DraftsConnector
from dcpy.connectors.edm.published import PublishedConnector
from dcpy.connectors.hybrid_pathed_storage import PathedStorageConnector, StorageType
from dcpy.lifecycle import connector_registry
from dcpy.lifecycle.connector_registry import connectors as lifecycle_connectors


@pytest.fixture(scope="module")
def setup_local_connectors(tmp_path_factory):
    """Setup local storage connectors for the test module."""
    # Create a temporary directory for the entire test module
    tmp_path = tmp_path_factory.mktemp("lifecycle_test")

    # Clear and setup connectors with local storage
    lifecycle_connectors.clear()

    # Create and register local storage connectors
    local_storage = PathedStorageConnector.from_storage_kwargs(
        conn_type="local_path_conn",
        storage_backend=StorageType.LOCAL,
        local_dir=tmp_path,
        _validate_root_path=False,
    )

    [
        lifecycle_connectors.register(c)
        for c in [
            BuildsConnector(storage=local_storage),
            DraftsConnector(storage=local_storage),
            PublishedConnector(storage=local_storage),
        ]
    ]

    yield tmp_path

    # Reset connectors after test module completes
    connector_registry._set_default_connectors()
