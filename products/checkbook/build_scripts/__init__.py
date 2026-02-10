import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from dcpy.lifecycle.builds import metadata
from dcpy.utils import postgres

load_dotenv()

_product_path = Path(__file__).resolve().parent.parent

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
