import pytest
import pandas as pd
import boto3
from dcpy.utils import versions, s3
from dcpy.connectors.edm import publishing

TEST_PRODUCT_NAME = "test-product"
TEST_BUILD = "build-branch"
TEST_VERSION = "v001"
TEST_ACL = "bucket-owner-read"
TEST_BUCKET_NAME = "edm-publishing"

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


@pytest.fixture()
def mock_versions_data():
    versions_by_product = {
        "test_product_1": ["23v2.2", "23v3", "23v3.1"],
        "test_product_2": ["22Q2", "22Q4", "23Q2"],
        "test_product_3": ["2022-12-01", "2023-01-01", "2023-02-01"],
    }
    for product in versions_by_product:
        for version in versions_by_product[product]:
            s3.client().put_object(
                Bucket=publishing.BUCKET, Key=f"{product}/publish/{version}/dummy.txt"
            )


@pytest.mark.parametrize(
    "product, version, expected",
    [
        ("test_product_1", "23v3.1", "23v3"),
        ("test_product_1", "23v3", "23v2.2"),
        ("test_product_1", "24v3", "23v3.1"),
        pytest.param("test_product_1", "23v2", None, marks=pytest.mark.xfail),
        ("test_product_2", "25Q4", "23Q2"),
        ("test_product_2", "23Q2", "22Q4"),
        ("test_product_2", "22Q4", "22Q2"),
        pytest.param("test_product_2", "20Q4", None, marks=pytest.mark.xfail),
        ("test_product_3", "2024-01-01", "2023-02-01"),
        ("test_product_3", "2023-02-01", "2023-01-01"),
        ("test_product_3", "2023-01-01", "2022-12-01"),
        pytest.param("test_product_3", "2022-12-01", None, marks=pytest.mark.xfail),
    ],
)
def test_previous(
    create_buckets,
    mock_versions_data,
    product,
    version,
    expected,
):
    previous = publishing.previous(product, version).label
    if expected is not None:
        assert previous == expected
