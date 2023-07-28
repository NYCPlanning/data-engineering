import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine

DCPY_ROOT_PATH = Path(__file__).resolve().parent

load_dotenv(DCPY_ROOT_PATH.parent / ".env")

BUILD_ENGINE_RAW = os.environ["BUILD_ENGINE"]
build_engine = create_engine(BUILD_ENGINE_RAW)
