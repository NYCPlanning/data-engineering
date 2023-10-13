import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

TOY_SECRET = os.environ["TOY_SECRET"]

TEST_DATA_PATH = Path(__file__).resolve().parent / "test_data"
