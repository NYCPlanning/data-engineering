import pytest
from dcpy.connectors.edm import publishing

TEST_PRODUCT_NAME = "test_product"
TEST_VERSION = "version_001"

@pytest.fixture
def get_draft_key(scope="function"):
    return publishing.DraftKey(product=TEST_PRODUCT_NAME, build=TEST_VERSION)
