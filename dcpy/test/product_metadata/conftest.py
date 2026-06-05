import os
from pathlib import Path

import pytest

from dcpy.lifecycle import product_metadata

# Assert that PRODUCT_METADATA_REPO_PATH is set and points to real repo
_repo_path_str = os.environ.get("PRODUCT_METADATA_REPO_PATH")
if not _repo_path_str:
    raise EnvironmentError(
        "PRODUCT_METADATA_REPO_PATH environment variable must be set. "
        "Point it to your local product-metadata repo clone. "
        "Example: export PRODUCT_METADATA_REPO_PATH=~/product-metadata"
    )

PRODUCT_METADATA_REPO_PATH: Path = Path(_repo_path_str)

if not PRODUCT_METADATA_REPO_PATH.exists():
    raise FileNotFoundError(
        f"PRODUCT_METADATA_REPO_PATH points to non-existent directory: {PRODUCT_METADATA_REPO_PATH}"
    )


@pytest.fixture(scope="session")
def org_md():
    """
    Load org metadata from product-metadata repo.

    NOTE: No template variables are passed. All Jinja2 templates in the
    product-metadata repo MUST have corresponding entries in snippets/strings.yml.
    """
    return product_metadata.load()


@pytest.fixture
def test_dcpy_product(org_md):
    """Get test_dcpy product for testing."""
    return org_md.product("test_dcpy")


@pytest.fixture
def test_overrides_dataset(test_dcpy_product):
    """Get test_overrides dataset for override hierarchy testing."""
    return test_dcpy_product.dataset("test_overrides")
