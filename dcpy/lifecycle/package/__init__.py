from pathlib import Path

RESOURCES_PATH = Path(__file__).parent / "resources"

from .assemble import assemble_dataset_package
from .validate import validate as validate_package
from dcpy.lifecycle import config

ASSEMBLY_DIR = config.local_data_path_for_stage("package.assemble")

__all__ = [
    "assemble_dataset_package",
    "ASSEMBLY_DIR",
    "pull_destination_package_files",
    "validate_package",
]
