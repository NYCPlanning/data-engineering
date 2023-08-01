import os
import sys
from pathlib import Path

from dotenv import load_dotenv

PRODUCT_PATH = Path(__file__).parent.parent
ROOT_PATH = PRODUCT_PATH.parent.parent
BASE_PATH = PRODUCT_PATH / ".output"

if not os.path.isdir(BASE_PATH):
    os.makedirs(BASE_PATH, exist_ok=True)

load_dotenv(ROOT_PATH / ".env")

sys.path.append(str(ROOT_PATH))

API_KEY = os.environ["API_KEY"]
