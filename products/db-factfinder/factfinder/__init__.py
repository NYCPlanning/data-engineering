import logging
import os

from dotenv import load_dotenv

load_dotenv()

# Create a local .library directory to store temporary files
base_path = ".cache"

if not os.path.isdir(base_path):
    os.makedirs(base_path, exist_ok=True)

api_key = os.environ.get("API_KEY")
logging.basicConfig(level=os.environ.get("LOGLEVEL", "WARNING").upper())
