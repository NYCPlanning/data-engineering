"""Where the validation scripts read dev and prod files and write their results.

They run as the build's QA stage (`dcpy lifecycle builds qa run`), which sets PACKAGE_DIR
and QA_DIR: dev files come from the package dir, prod files and results live in the QA
dir. DATASET_FILES_DIR, PROD_FILES_DIR and VALIDATION_DIR override them.
validate_outputs.sh mirrors this.
"""

import os
from pathlib import Path


def _stage_dir(name: str) -> Path:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"{name} is not set: run this as the QA stage (dcpy lifecycle builds qa run)"
        )
    return Path(value)


def dataset_files_dir() -> Path:
    if os.environ.get("DATASET_FILES_DIR"):
        return Path(os.environ["DATASET_FILES_DIR"])
    return _stage_dir("PACKAGE_DIR") / "dataset_files"


def prod_files_dir() -> Path:
    if os.environ.get("PROD_FILES_DIR"):
        return Path(os.environ["PROD_FILES_DIR"])
    return _stage_dir("QA_DIR") / "prod"


def validation_dir() -> Path:
    if os.environ.get("VALIDATION_DIR"):
        return Path(os.environ["VALIDATION_DIR"])
    return _stage_dir("QA_DIR") / "validation_output"


def find_dataset_file(filename: str) -> Path:
    """dataset_files groups files into subfolders (e.g. district_gdb/); find by name."""
    root = dataset_files_dir()
    if (root / filename).exists():
        return root / filename
    matches = sorted(root.rglob(filename))
    return matches[0] if matches else root / filename
