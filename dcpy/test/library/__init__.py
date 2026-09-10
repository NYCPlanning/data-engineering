import os
from pathlib import Path

import dcpy.library
from dotenv import load_dotenv
from rich.console import Console
from sqlalchemy import create_engine

# Load environmental variables
load_dotenv()

recipe_engine = os.environ["RECIPE_ENGINE"]
pg = create_engine(recipe_engine)

console = Console()
test_root_path = Path(__file__).parent
# dcpy.library is its own workspace package now (a different directory tree than this
# test file), so resolve its templates dir via the installed module.
template_path = f"{Path(dcpy.library.__file__).parent}/templates"

TEST_DATASET_NAME = "test_nypl_libraries"
TEST_DATASET_VERSION = "20210122"
TEST_DATASET_CONFIG_FILE = f"{test_root_path}/data/{TEST_DATASET_NAME}.yml"
TEST_DATASET_OUTPUT_DIRECTORY = (
    f".library/datasets/{TEST_DATASET_NAME}/{TEST_DATASET_VERSION}"
)
TEST_DATASET_OUTPUT_PATH = f"{TEST_DATASET_OUTPUT_DIRECTORY}/{TEST_DATASET_NAME}"
TEST_DATASET_OUTPUT_PATH_S3 = f"datasets/{TEST_DATASET_NAME}/{TEST_DATASET_VERSION}"


def get_config_file(filename: str) -> str:
    return f"{test_root_path}/data/{filename}.yml"
