from pathlib import Path

from dcpy.models.connectors.edm.publishing import BuildKey
from dcpy.lifecycle.builds import metadata

PRODUCT_PATH = Path(__file__).resolve().parent.parent

BASH_UTILS_PATH = PRODUCT_PATH.parent.parent / "bash" / "utils.sh"
DATA_DIR = PRODUCT_PATH / "data"
OUTPUT_DIR = PRODUCT_PATH / "output"

PRODUCT_S3_NAME = "db-ceqr"
BUILD_NAME = metadata.build_name()
BUILD_KEY = BuildKey(product=PRODUCT_S3_NAME, build=BUILD_NAME)
