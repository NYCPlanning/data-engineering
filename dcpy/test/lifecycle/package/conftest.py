from pathlib import Path
import pytest
import shutil

RESOURCES_PATH = Path(__file__).parent.resolve() / "resources"

TEST_PACKAGE_PATH = RESOURCES_PATH / "test_package"

TEST_METADATA_YAML_PATH = TEST_PACKAGE_PATH / "metadata.yml"

TEMP_DATA_PATH = TEST_PACKAGE_PATH / "output"


@pytest.fixture
def file_setup_teardown():
    TEMP_DATA_PATH.mkdir(exist_ok=True)
    yield
    shutil.rmtree(TEMP_DATA_PATH)
