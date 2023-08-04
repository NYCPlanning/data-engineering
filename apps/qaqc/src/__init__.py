import sys
import os
from pathlib import Path

from dotenv import load_dotenv

APP_PATH = Path(__file__).parent.parent
ROOT_PATH = APP_PATH.parent.parent

load_dotenv(ROOT_PATH / ".env")

sys.path.append(str(ROOT_PATH))

QAQC_DB_SCHEMA_SOURCE_DATA = "source_data"
