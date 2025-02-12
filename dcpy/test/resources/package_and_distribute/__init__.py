from pathlib import Path

from dcpy.models.product.metadata import OrgMetadata

MODULE_PATH = Path(__file__).parent
REPO_PATH = MODULE_PATH / "metadata_repo"

PACKAGES_PATH = MODULE_PATH / "packages"
PACKAGE_PATH_COLP_SINGLE_FEATURE = PACKAGES_PATH / "colp_single_feature"
PACKAGE_PATH_ASSEMBLED = PACKAGES_PATH / "assembled_package_and_metadata"

LION_PRODUCT_LEVEL_PUB_FREQ = "lion_product_freq"
PSEUDO_LOTS_PUB_FREQ = "pseudo_lots-freq"
AGENCY = "DCP"
DEFAULT_TEMPLATE_VARS = {
    "version": "24c",
    "lion_prod_level_pub_freq": LION_PRODUCT_LEVEL_PUB_FREQ,
    "pseudo_lots_pub_freq": PSEUDO_LOTS_PUB_FREQ,
    "agency": AGENCY,
}
PRODUCT_WITH_ERRORS = "mock_product_with_errors"


def org_md(template_vars: dict | None = None) -> OrgMetadata:
    return OrgMetadata.from_path(
        REPO_PATH, template_vars=template_vars or DEFAULT_TEMPLATE_VARS
    )
