import os
from pathlib import Path
from dcpy.lifecycle import config

PRODUCT_PATH = Path(__file__).parent.parent
DATA_PATH = PRODUCT_PATH / "factfinder" / "data"
OUTPUT_FOLDER = config.local_data_path_for_stage("builds.build") / "factfinder"

API_KEY = os.environ["API_KEY"]
