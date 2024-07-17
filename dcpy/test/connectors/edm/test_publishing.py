from botocore.exceptions import ClientError
from datetime import datetime
import pandas as pd
from pathlib import Path
import pytest

from dcpy.utils import s3, versions
from dcpy.connectors.edm import publishing

TEST_PRODUCT_NAME = "test-product"
TEST_BUILD = "build-branch"
TEST_VERSION_OBJ = versions.MajorMinor(year=24, major=2)  # matches conftest.py
TEST_VERSION = TEST_VERSION_OBJ.label
TEST_ACL = "bucket-owner-read"
TEST_BUCKET_NAME = "edm-publishing"
TEST_GIS_DATASET = "test_gis_dataset"
DATE_CONSTANT = "20240101"
DATE_TODAY = datetime.now().strftime("%Y%m%d")

draft_key = publishing.DraftKey(product=TEST_PRODUCT_NAME, build=TEST_BUILD)
publish_key = publishing.PublishKey(product=TEST_PRODUCT_NAME, version=TEST_VERSION)


@pytest.fixture(scope="function")
def add_gis_datasets(create_buckets):
    test_objects = [
        f"datasets/{TEST_GIS_DATASET}/staging/{TEST_GIS_DATASET}.zip",
        f"datasets/{TEST_GIS_DATASET}/{DATE_TODAY}/{TEST_GIS_DATASET}.zip",
        f"datasets/{TEST_GIS_DATASET}/{DATE_CONSTANT}/{TEST_GIS_DATASET}.zip",
    ]
    for object in test_objects:
        s3.client().put_object(Bucket=publishing.BUCKET, Key=object)
    yield


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


def test_publish_patch(create_buckets, create_temp_filesystem, mock_data_constants):
    """
    Tests publish function when a version already exists.
    When is_path = True, publish a patched version. Otherwise throw an error."""
    data_path = mock_data_constants["TEST_DATA_DIR"]
    publishing.upload(output_path=data_path, draft_key=draft_key, acl=TEST_ACL)
    publishing.publish(
        draft_key=draft_key,
        acl=TEST_ACL,
        version=TEST_VERSION,
        keep_draft=True,
        latest=False,
        is_patch=False,
    )
    # re-publish and assert an error is thrown when is_patch = False
    with pytest.raises(ValueError) as e_info:
        publishing.publish(
            draft_key=draft_key,
            acl=TEST_ACL,
            version=TEST_VERSION,
            keep_draft=True,
            latest=False,
            is_patch=False,
        )

    # re-publish and assert a bumped version is created
    publishing.publish(
        draft_key=draft_key,
        acl=TEST_ACL,
        version=TEST_VERSION,
        keep_draft=True,
        latest=False,
        is_patch=True,
    )
    bumped_version = versions.bump(
        previous_version=TEST_VERSION,
        bump_type=versions.VersionSubType.patch,
        bump_by=1,
    ).label
    assert set(publishing.get_published_versions(product=draft_key.product)) == set(
        [TEST_VERSION, bumped_version]
    )


def test_publish_draft_folder(
    create_buckets, create_temp_filesystem, mock_data_constants
):
    """
    Test to confirm files are retained or deleted based on input argument.
    """
    data_path = mock_data_constants["TEST_DATA_DIR"]
    publishing.upload(output_path=data_path, draft_key=draft_key, acl=TEST_ACL)
    publishing.publish(
        draft_key=draft_key,
        acl=TEST_ACL,
        version=TEST_VERSION,
        keep_draft=False,
    )

    assert publishing.get_draft_builds(product=draft_key.product) == []

    publishing.upload(output_path=data_path, draft_key=draft_key, acl=TEST_ACL)
    publishing.publish(
        draft_key=draft_key,
        acl=TEST_ACL,
        version=TEST_VERSION,
        keep_draft=True,
        is_patch=True,
    )
    assert publishing.get_draft_builds(product=draft_key.product) == [TEST_BUILD]


def test_publish_expected_data(
    create_buckets, create_temp_filesystem, mock_data_constants
):
    """Tests whether published data matches original data."""
    data_path = mock_data_constants["TEST_DATA_DIR"]
    publishing.upload(output_path=data_path, draft_key=draft_key, acl=TEST_ACL)
    publishing.publish(draft_key=draft_key, acl=TEST_ACL, version=TEST_VERSION)

    test_data = pd.DataFrame(
        data=mock_data_constants["TEST_DATA"],
        columns=mock_data_constants["TEST_DATA_FIELDS"],
    )
    publish_key = publishing.PublishKey(product=draft_key.product, version=TEST_VERSION)
    published_data = publishing.read_csv(
        product_key=publish_key, filepath=mock_data_constants["TEST_FILE"]
    )
    assert published_data.equals(test_data)


def test_publish_excludes_latest_when_flag_false(
    create_buckets, create_temp_filesystem, mock_data_constants
):
    """
    Test to confirm files are not published to "latest" folder when input arg `latest` = False.
    """
    data_path = mock_data_constants["TEST_DATA_DIR"]
    publishing.upload(output_path=data_path, draft_key=draft_key, acl=TEST_ACL)

    publishing.publish(
        draft_key=draft_key, acl=TEST_ACL, version=TEST_VERSION, latest=False
    )
    # assert that latest folder was not populated
    with pytest.raises(ClientError) as e_info:
        publishing.get_latest_version(TEST_PRODUCT_NAME)
    # assert a file is published to "latest"
    publishing.publish(
        draft_key=draft_key,
        acl=TEST_ACL,
        version=TEST_VERSION,
        latest=True,
        is_patch=True,
    )
    assert publishing.get_latest_version(TEST_PRODUCT_NAME) == TEST_VERSION


@pytest.fixture(scope="function")
def test_publish_latest_ignores_older_versions(
    create_buckets, create_temp_filesystem, mock_data_constants
):
    """
    Test to confirm 'latest' folder doesn't get updated with older data version.
    """
    data_path = mock_data_constants["TEST_DATA_DIR"]
    publishing.upload(output_path=data_path, draft_key=draft_key, acl=TEST_ACL)

    publishing.publish(
        draft_key=draft_key, acl=TEST_ACL, version=TEST_VERSION, latest=True
    )
    # sanity check
    assert publishing.get_latest_version(TEST_PRODUCT_NAME) == TEST_VERSION

    # republish an older version. "latest" should NOT be updated
    TEST_VERSION_OBJ = versions.parse(TEST_VERSION)
    older_version = versions.MajorMinor(
        year=TEST_VERSION_OBJ.year, major=TEST_VERSION_OBJ.major - 1
    ).label
    with pytest.raises(ValueError) as e_info:
        publishing.publish(
            draft_key=draft_key,
            acl=TEST_ACL,
            version=older_version,
            latest=True,
        )


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
    previous = publishing.get_previous_version(product, version).label
    if expected is not None:
        assert previous == expected


def test_assert_gis_dataset_exists(create_buckets, add_gis_datasets):
    publishing._assert_gis_dataset_exists(TEST_GIS_DATASET, DATE_CONSTANT)

    v_fail = "fake_version"
    with pytest.raises(FileNotFoundError):
        publishing._assert_gis_dataset_exists(TEST_GIS_DATASET, v_fail)


def test_get_latest_gis_dataset_version(create_buckets, add_gis_datasets):
    assert DATE_TODAY == publishing.get_latest_gis_dataset_version(TEST_GIS_DATASET)

    s3.client().put_object(
        Bucket=publishing.BUCKET,
        Key=f"datasets/{TEST_GIS_DATASET}/24A/{TEST_GIS_DATASET}.zip",
    )
    with pytest.raises(ValueError, match="Multiple"):
        publishing.get_latest_gis_dataset_version(TEST_GIS_DATASET)


def test_download_gis_dataset(
    create_buckets, add_gis_datasets, create_temp_filesystem: Path
):
    file_path = publishing.download_gis_dataset(
        TEST_GIS_DATASET, DATE_CONSTANT, create_temp_filesystem
    )
    assert file_path.is_file()
