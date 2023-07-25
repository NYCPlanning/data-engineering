import sys
from pathlib import Path


_product_path = Path(__file__).resolve().parent.parent
_proj_root = _product_path.parent

print(_proj_root)

# Make `dcpy` available
sys.path.append(str(_proj_root))