import pytest
import pandas as pd
from dcpy.connectors.edm import publishing

TEST_PRODUCT_NAME = "test-product"
TEST_BUILD = "build-branch"
TEST_VERSION = "v001"
TEST_ACL = "bucket-owner-read"

draft_key = publishing.DraftKey(product=TEST_PRODUCT_NAME, build=TEST_BUILD)
publish_key = publishing.PublishKey(product=TEST_PRODUCT_NAME, version=TEST_VERSION)


def test_bucket_empty(create_buckets, mock_data_constants):
    """Sanity check there are no draft or publish versions from previous tests
    or actual data."""

    assert publishing.get_draft_builds(product=TEST_PRODUCT_NAME) == []
    assert publishing.get_published_versions(product=TEST_PRODUCT_NAME) == []


def test_upload(create_buckets, create_temp_filesystem, mock_data_constants):
    """Checks build directory is found in draft builds.
    Tests version from version.txt file matches actual version."""
    data_path = mock_data_constants["TEST_DATA_DIR"]

    publishing.upload(output_path=data_path, draft_key=draft_key, acl=TEST_ACL)
    assert TEST_BUILD in publishing.get_draft_builds(product=TEST_PRODUCT_NAME)
    assert publishing.get_version(product_key=draft_key) == TEST_VERSION


def test_publish(create_buckets, create_temp_filesystem, mock_data_constants):
    publishing.publish(
        draft_key=draft_key, acl=TEST_ACL, version=None, keep_draft=False
    )

    assert publishing.get_draft_builds(product=draft_key.product) == []
    assert [TEST_VERSION] == publishing.get_published_versions(
        product=TEST_PRODUCT_NAME
    )

    test_data = pd.DataFrame(
        data=mock_data_constants["TEST_DATA"],
        columns=mock_data_constants["TEST_DATA_FIELDS"],
    )
    published_data = publishing.read_csv(
        product_key=publish_key, filepath=mock_data_constants["TEST_FILE"]
    )
    assert published_data.equals(test_data)
