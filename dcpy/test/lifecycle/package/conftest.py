import shutil
from pathlib import Path

import pytest

from dcpy.lifecycle import product_metadata
from dcpy.test.resources import package_and_distribute

TEST_ASSEMBLED_PACKAGE_AND_METADATA_PATH = package_and_distribute.PACKAGE_PATH_ASSEMBLED

PACKAGE_RESOURCES_PATH = Path(__file__).parent.resolve() / "resources"
TEST_PACKAGE_PATH = PACKAGE_RESOURCES_PATH / "test_package"

TEST_METADATA_YAML_PATH = TEST_PACKAGE_PATH / "metadata.yml"

TEMP_DATA_PATH = TEST_PACKAGE_PATH / "output"


@pytest.fixture
def file_setup_teardown():
    TEMP_DATA_PATH.mkdir(exist_ok=True)
    yield
    shutil.rmtree(TEMP_DATA_PATH)


@pytest.fixture
def test_dcpy_org_metadata():
    """Loads test_dcpy, a synthetic dcpy-only product."""
    return product_metadata.load()
