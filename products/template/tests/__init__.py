import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

TOY_SECRET_GITHUB = os.environ["TOY_SECRET_GITHUB"]
TOY_SECRET_1PASSWORD = os.environ["TOY_SECRET_1PASSWORD"]

TEST_DATA_DIR = Path(__file__).resolve().parent / "test_data"
