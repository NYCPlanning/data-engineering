from pathlib import Path

from dcpy.lifecycle import product_metadata

MODULE_PATH = Path(__file__).parent
REPO_PATH = MODULE_PATH / "metadata_repo"

# Package paths for lifecycle tests
PACKAGES_PATH = MODULE_PATH / "packages"
PACKAGE_PATH_COLP_SINGLE_FEATURE = PACKAGES_PATH / "colp_single_feature"
PACKAGE_PATH_ASSEMBLED = PACKAGES_PATH / "assembled_package_and_metadata"

AGENCY = "DCP"
DEFAULT_TEMPLATE_VARS = {
    "version": "24c",
    "agency": AGENCY,
}

# Loaded from the local mock repo below (not PRODUCT_METADATA_REPO_PATH / the real
# product-metadata repo): this PR's pluto GDB overrides haven't been ported to the
# real repo yet (planned as a separate follow-up PR), so tests that need them read
# from this local fixture in the meantime.
org_md = product_metadata.load(org_md_path_override=REPO_PATH, **DEFAULT_TEMPLATE_VARS)
