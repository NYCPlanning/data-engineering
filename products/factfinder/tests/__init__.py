import os

from dotenv import load_dotenv

# Load environmental variables
load_dotenv()
api_key = os.environ.get("API_KEY")
