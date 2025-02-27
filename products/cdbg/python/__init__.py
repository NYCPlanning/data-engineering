import os
from pathlib import Path

PRODUCT_PATH = Path(__file__).parent.parent
OUTPUT_PATH = PRODUCT_PATH / "output"

CENSUS_TRACTS_YEAR = os.environ["CENSUS_TRACTS_YEAR"]
