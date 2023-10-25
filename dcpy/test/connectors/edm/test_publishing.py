import pytest
import tempfile
from dcpy.connectors.edm import publishing


TEST_PRODUCT_NAME = "test_product"
TEST_BUILD = "build_branch"
TEST_VERSION = "v001"


@pytest.fixture(scope="module")
def get_draft_key():
    return publishing.DraftKey(product=TEST_PRODUCT_NAME, build=TEST_BUILD)


@pytest.fixture(scope="module")
def get_publish_key():
    return publishing.DraftKey(product=TEST_PRODUCT_NAME, version=TEST_VERSION)


@pytest.fixture(scope="module")
def func():
    pass


def test_upload():
    pass
