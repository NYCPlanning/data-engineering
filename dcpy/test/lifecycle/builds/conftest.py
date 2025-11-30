import os
import shutil
from pathlib import Path

import pytest

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
