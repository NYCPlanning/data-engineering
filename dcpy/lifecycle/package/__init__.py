from pathlib import Path

RESOURCES_PATH = Path(__file__).parent / "resources"

from .assemble import assemble_package, pull_destination_package_files
from .validate import validate as validate_package
from dcpy.lifecycle import WORKING_DIRECTORIES

ASSEMBLY_DIR = WORKING_DIRECTORIES.packaging / "assembly"

__all__ = [
    "assemble_package",
    "ASSEMBLY_DIR",
    "pull_destination_package_files",
    "validate_package",
]
