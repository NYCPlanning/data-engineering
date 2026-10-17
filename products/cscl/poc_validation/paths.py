"""Where the validation scripts read dev and prod files and write their results.

Inside the lifecycle QA stage (`dcpy lifecycle builds qa run`, which sets PACKAGE_DIR and
QA_DIR), dev files come from the package dir and everything else lives in the QA dir.
Otherwise the original local layout (output/, .data/prod) is used. DATASET_FILES_DIR,
PROD_FILES_DIR and VALIDATION_DIR override either. validate_outputs.sh mirrors this.
"""

import os
from pathlib import Path


def _stage_dir(name: str) -> Path | None:
    value = os.environ.get(name)
    return Path(value) if value else None


def dataset_files_dir() -> Path:
    if os.environ.get("DATASET_FILES_DIR"):
        return Path(os.environ["DATASET_FILES_DIR"])
    package = _stage_dir("PACKAGE_DIR")
    return package / "dataset_files" if package else Path("output") / "dataset_files"


def prod_files_dir() -> Path:
    if os.environ.get("PROD_FILES_DIR"):
        return Path(os.environ["PROD_FILES_DIR"])
    qa = _stage_dir("QA_DIR")
    return qa / "prod" if qa else Path(".data") / "prod"


def validation_dir() -> Path:
    if os.environ.get("VALIDATION_DIR"):
        return Path(os.environ["VALIDATION_DIR"])
    qa = _stage_dir("QA_DIR")
    return qa / "validation_output" if qa else Path("output") / "validation_output"


def find_dataset_file(filename: str) -> Path:
    """dataset_files may group files into subfolders (e.g. district_gdb/); find by name."""
    root = dataset_files_dir()
    if (root / filename).exists():
        return root / filename
    matches = sorted(root.rglob(filename))
    return matches[0] if matches else root / filename
