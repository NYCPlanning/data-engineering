from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://nyc3.digitaloceanspaces.com/edm-recipes/datasets"
BASE_PATH = Path(__file__).parent / ".cache"
