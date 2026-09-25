import shutil
from pathlib import Path

import pytest

from dcpy.connectors.registry import VersionedConnector
from dcpy.lifecycle import connector_registry
from dcpy.utils import postgres


class MockVersionedConnector(VersionedConnector):
    conn_type: str = "mock_recipes_source"
    pull_call_count: int = 0

    def reset_mock(self):
        self.pull_call_count = 0

    def pull_versioned(
        self, key: str, version: str, destination_path: Path, **kwargs
    ) -> dict:
        self.pull_call_count += 1
        # Copy the mock CSV file to the destination
        source_file = Path(__file__).parent / "resources" / "test_dataset.csv"
        destination_file = destination_path / f"{key}.csv"
        destination_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(source_file, destination_file)
        return {"path": destination_file}

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        return {"status": "success"}

    def get_latest_version(self, key: str, **kwargs) -> str:
        return "2025v2"

    def get_name(self, key: str, version: str) -> str:
        return "DCP Test Dataset"

    def list_versions(self, key: str, **kwargs) -> list[str]:
        return ["2025v2"]

    def assert_pull_not_called(self):
        assert self.pull_call_count == 0, (
            f"Expected pull_versioned not to be called, but it was called {self.pull_call_count} times"
        )

    def assert_pull_called_once(self):
        assert self.pull_call_count == 1, (
            f"Expected pull_versioned to be called once, but it was called {self.pull_call_count} times"
        )


@pytest.fixture
def setup_mock_connector():
    mock_connector = MockVersionedConnector()
    connector_registry.connectors.clear()
    connector_registry.connectors.register(
        mock_connector, conn_type="mock_recipes_source"
    )
    yield mock_connector


@pytest.fixture
def pg_client():
    return postgres.PostgresClient(schema="lifecycle_builds_tests")
