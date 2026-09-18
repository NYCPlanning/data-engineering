from pathlib import Path

import pytest

RESOURCES = Path(__file__).parent / "resources"


@pytest.fixture(scope="function")
def utils_resources_path():
    return RESOURCES
