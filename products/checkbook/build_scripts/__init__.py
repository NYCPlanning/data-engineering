import os
import sys
from pathlib import Path
from dotenv import load_dotenv

from dcpy.utils import postgres
from dcpy.builds import metadata

load_dotenv()

_product_path = Path(__file__).resolve().parent.parent
_proj_root = _product_path.parent.parent

# Make `dcpy` available
sys.path.append(str(_proj_root))

LIB_DIR = _product_path / ".library"
OUTPUT_DIR = _product_path / ".output"
SQL_QUERY_DIR = _product_path / "sql"

SOURCE_DATA_VERSIONS_FILENAME = "source_data_versions.csv"
BUILD_OUTPUT_FILENAME = "historical_spend.csv"
SUMMARY_STATS_DESCRIBE_FILENAME = "historical_spend_stats.csv"
SUMMARY_STATS_LOG_FILENAME = "build_summarization.log"

BUILD_NAME = metadata.build_name()

PG_CLIENT = postgres.PostgresClient(
    schema=BUILD_NAME,
)

LIB_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
