import os
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console
from sqlalchemy import create_engine

# Load environmental variables
load_dotenv()

recipe_engine = os.environ["RECIPE_ENGINE"]
pg = create_engine(recipe_engine)

console = Console()
test_root_path = Path(__file__).parent
template_path = f"{Path(__file__).parent.parent}/library/templates"

TEST_DATASET_NAME = "test_nypl_libraries"
TEST_DATASET_VERSION = "20210122"
TEST_DATASET_CONFIG_FILE = f"{test_root_path}/data/{TEST_DATASET_NAME}.yml"
TEST_DATASET_OUTPUT_DIRECTORY = (
    f".library/datasets/{TEST_DATASET_NAME}/{TEST_DATASET_VERSION}"
)
TEST_DATASET_OUTPUT_PATH = f"{TEST_DATASET_OUTPUT_DIRECTORY}/{TEST_DATASET_NAME}"
TEST_DATASET_OUTPUT_PATH_S3 = f"datasets/{TEST_DATASET_NAME}/{TEST_DATASET_VERSION}"

get_config_file = lambda filename: f"{test_root_path}/data/{filename}.yml"
