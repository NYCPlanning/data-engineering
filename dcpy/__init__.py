from pathlib import Path

from dotenv import dotenv_values
from sqlalchemy import create_engine

DCPY_ROOT_PATH = Path(__file__).resolve().parent

_config = dotenv_values(DCPY_ROOT_PATH.parent / ".env")

BUILD_ENGINE_RAW = _config["BUILD_ENGINE"]
build_engine = create_engine(BUILD_ENGINE_RAW)
