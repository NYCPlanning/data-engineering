import os
import shutil
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

from dcpy.connectors.registry import VersionedConnector
from dcpy.lifecycle import connector_registry

RESOURCES_DIR = Path(__file__).parent / "resources"
TEMP_DATA_PATH = RESOURCES_DIR.parent / "temp"
RECIPE_NO_DEFAULTS_LOCK_PATH = RESOURCES_DIR / "recipe_no_defaults.lock.yml"

REQUIRED_VERSION_ENV_VAR = "VERSION_PREV"


@pytest.fixture
def file_setup_teardown():
    RECIPE_NO_DEFAULTS_LOCK_PATH.unlink(missing_ok=True)
    TEMP_DATA_PATH.mkdir(exist_ok=True)
    yield
    RECIPE_NO_DEFAULTS_LOCK_PATH.unlink(missing_ok=True)
    shutil.rmtree(TEMP_DATA_PATH)


def setup():
    # TEMP_DATA_PATH.mkdir(exist_ok=True)
    os.environ[REQUIRED_VERSION_ENV_VAR] = "v123"


def set_mock_published_connector(method_responses: dict):
    """Register a mock published connector with specified method responses.

    Args:
        method_responses: Dict mapping method names to return values or Mock objects
            e.g., {"get_latest_version": "24v1", "list_versions": ["24v1", "24v2"]}
    """
    mock_connector = MagicMock(spec=VersionedConnector)
    mock_connector.conn_type = "edm.publishing.published"

    for method, response in method_responses.items():
        if isinstance(response, Mock):
            setattr(mock_connector, method, response)
        else:
            setattr(mock_connector, method, Mock(return_value=response))

    connector_registry.connectors.register(
        mock_connector, conn_type="edm.publishing.published"
    )
