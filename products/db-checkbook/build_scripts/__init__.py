import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

_product_path = Path(__file__).resolve().parent.parent
_proj_root = _product_path.parent.parent

# Make `dcpy` available
sys.path.append(str(_proj_root))

LIB_DIR = _product_path / ".library"
OUTPUT_DIR = _product_path / ".output"
SQL_QUERY_DIR = _product_path / "sql"

if not os.path.isdir(LIB_DIR):
    os.makedirs(LIB_DIR)

if not os.path.isdir(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
