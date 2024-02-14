import os
from pathlib import Path

PRODUCT_PATH = Path(__file__).parent.parent
DATA_PATH = PRODUCT_PATH / "factfinder" / "data"
OUTPUT_FOLDER = DATA_PATH / ".output"

API_KEY = os.environ["API_KEY"]
