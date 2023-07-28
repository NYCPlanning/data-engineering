from pathlib import Path
from dotenv import load_dotenv

DCPY_ROOT_PATH = Path(__file__).resolve().parent

load_dotenv(DCPY_ROOT_PATH.parent / ".env")
