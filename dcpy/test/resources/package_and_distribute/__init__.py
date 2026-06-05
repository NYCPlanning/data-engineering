from pathlib import Path

MODULE_PATH = Path(__file__).parent

# Package paths for lifecycle tests
PACKAGES_PATH = MODULE_PATH / "packages"
PACKAGE_PATH_COLP_SINGLE_FEATURE = PACKAGES_PATH / "colp_single_feature"
PACKAGE_PATH_ASSEMBLED = PACKAGES_PATH / "assembled_package_and_metadata"
