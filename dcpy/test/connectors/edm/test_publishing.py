from datetime import datetime
import pandas as pd
from pathlib import Path
import os
import pytest
from unittest.mock import patch

from dcpy.utils import s3, versions
from dcpy.connectors.edm import publishing
from dcpy.test.conftest import PUBLISHING_BUCKET

TEST_PRODUCT_NAME = "test-product"
TEST_BUILD = "build-branch"
TEST_VERSION_OBJ = versions.MajorMinor(year=24, major=2)  # matches conftest.py
TEST_VERSION = TEST_VERSION_OBJ.label
TEST_ACL = "bucket-owner-read"
TEST_BUCKET_NAME = PUBLISHING_BUCKET
TEST_GIS_DATASET = "test_gis_dataset"
DATE_CONSTANT = "20240101"
DATE_TODAY = datetime.now().strftime("%Y%m%d")

build_key = publishing.BuildKey(product=TEST_PRODUCT_NAME, build=TEST_BUILD)
publish_key = publishing.PublishKey(product=TEST_PRODUCT_NAME, version=TEST_VERSION)


@pytest.fixture(scope="function")
def add_gis_datasets(create_buckets):
    test_objects = [
        f"datasets/{TEST_GIS_DATASET}/staging/{TEST_GIS_DATASET}.zip",
        f"datasets/{TEST_GIS_DATASET}/{DATE_TODAY}/{TEST_GIS_DATASET}.zip",
        f"datasets/{TEST_GIS_DATASET}/{DATE_CONSTANT}/{TEST_GIS_DATASET}.zip",
    ]
    for object in test_objects:
        s3.client().put_object(Bucket=TEST_BUCKET_NAME, Key=object)
    yield


def test_bucket_empty(create_buckets, mock_data_constants):
    """Sanity check there are no draft or publish versions from previous tests
    or actual data."""

    assert publishing.get_builds(product=TEST_PRODUCT_NAME) == []
    assert publishing.get_draft_versions(product=TEST_PRODUCT_NAME) == []
    assert publishing.get_published_versions(product=TEST_PRODUCT_NAME) == []


def test_upload(create_buckets, create_temp_filesystem, mock_data_constants):
    """Checks build directory is found in s3 "build" directory.
    Tests version from version.txt file matches actual version."""
    data_path = mock_data_constants["TEST_DATA_DIR"]

    publishing.upload(output_path=data_path, build_key=build_key, acl=TEST_ACL)
    assert TEST_BUILD in publishing.get_builds(product=TEST_PRODUCT_NAME)
    assert publishing.get_version(product_key=build_key) == TEST_VERSION


def test_upload_file_fails(create_buckets, create_temp_filesystem, mock_data_constants):
    data_path = mock_data_constants["TEST_DATA_DIR"] / mock_data_constants["TEST_FILE"]
    with pytest.raises(
        Exception, match="'upload' expects output_path to be a directory, not a file"
    ):
        publishing.upload(output_path=data_path, build_key=build_key, acl=TEST_ACL)


def test_get_filenames(create_buckets, create_temp_filesystem, mock_data_constants):
    """Checks build directory is found in s3 "build" directory.
    Tests version from version.txt file matches actual version."""
    data_path = mock_data_constants["TEST_DATA_DIR"]

    publishing.upload(output_path=data_path, build_key=build_key, acl=TEST_ACL)
    expected_filenames = {"file.csv", "version.txt", "build_metadata.json"}
    assert publishing.get_filenames(build_key) == expected_filenames


# @pytest.mark.parametrize(
#     "latest, contents_only, expected_path, not_expected_paths",
#     [
#         (
#             False,
#             False,
#             "24v2/test_data/file.csv",
#             ["latest/test_data/file.csv", "24v2/file.csv"],
#         ),
#         (True, False, "latest/test_data/file.csv", []),
#         (False, True, "24v2/file.csv", ["24v2/test_data/file.csv"]),
#     ],
# )
# def test_legacy_upload(
#     latest: bool,
#     contents_only: bool,
#     expected_path: str,
#     not_expected_paths: list[str],
#     create_buckets,
#     create_temp_filesystem,
#     mock_data_constants,
# ):
#     data_path = mock_data_constants["TEST_DATA_DIR"]
#     prefix = "prefix"
#     publishing.legacy_upload(
#         output=data_path,
#         publishing_folder=TEST_PRODUCT_NAME,
#         version=TEST_VERSION,
#         acl=TEST_ACL,  # type: ignore
#         s3_subpath=prefix,
#         contents_only=contents_only,
#         latest=latest,
#     )
#     assert s3.object_exists(
#         TEST_BUCKET_NAME,
#         f"{TEST_PRODUCT_NAME}/{prefix}/{expected_path}",
#     )
#     for path in not_expected_paths:
#         assert not s3.object_exists(
#             TEST_BUCKET_NAME, f"{TEST_PRODUCT_NAME}/{prefix}/{path}"
#         )


@patch("dcpy.connectors.edm.publishing.BUILD_NAME", TEST_BUILD)
def test_upload_build_success(
    create_buckets,
    create_temp_filesystem,
    mock_data_constants,
):
    """Tests successful upload of a build, using env variable for build name."""
    data_path = mock_data_constants["TEST_DATA_DIR"]
    publishing.upload_build(
        data_path,
        product=TEST_PRODUCT_NAME,
        build=None,
        acl=TEST_ACL,
    )
    build_key = publishing.BuildKey(product=TEST_PRODUCT_NAME, build=TEST_BUILD)
    assert TEST_BUILD in publishing.get_builds(product=TEST_PRODUCT_NAME)
    assert publishing.get_version(product_key=build_key) == TEST_VERSION


def test_upload_build_missing_build_name(
    create_buckets,
    create_temp_filesystem,
    mock_data_constants,
):
    """Tests failure when build name is not provided and env variable is missing."""
    data_path = mock_data_constants["TEST_DATA_DIR"]

    assert os.getenv("BUILD_NAME") is None  # sanity check
    with pytest.raises(ValueError, match="'BUILD_NAME' cannot be"):
        publishing.upload_build(
            data_path,
            product=TEST_PRODUCT_NAME,
            build=None,
            acl=TEST_ACL,
        )


def test_upload_build_file_not_found(create_buckets, mock_data_constants):
    """Tests failure when the provided output path does not exist."""
    data_path = mock_data_constants["TEST_DATA_DIR"]

    assert not data_path.exists()  # sanity check

    with pytest.raises(FileNotFoundError):
        publishing.upload_build(
            data_path,
            product=TEST_PRODUCT_NAME,
            build=TEST_BUILD,
            acl=TEST_ACL,
        )


@patch("dcpy.connectors.edm.publishing.log_event_in_db")
def test_upload_build_validate_logging_logic(
    mock_log_event,
    create_buckets,
    create_temp_filesystem,
    mock_data_constants,
):
    data_path = mock_data_constants["TEST_DATA_DIR"]
    build_to_ignore = publishing.IGNORED_LOGGING_BUILDS[0]
    publishing.upload_build(
        data_path,
        product=TEST_PRODUCT_NAME,
        build=build_to_ignore,
        acl=TEST_ACL,
    )
    # Ensure log_event_in_db is not called since the build is ignored
    mock_log_event.assert_not_called()

    mock_log_event.reset_mock()

    non_ignored_build = TEST_BUILD
    non_ignored_product = "db-template"
    assert non_ignored_product in publishing.PRODUCTS_TO_LOG  # sanity check
    assert non_ignored_build not in publishing.IGNORED_LOGGING_BUILDS  # sanity check
    publishing.upload_build(
        data_path,
        product=non_ignored_product,
        build=non_ignored_build,
        acl=TEST_ACL,
    )
    # Ensure log_event_in_db is called
    mock_log_event.assert_called_once()


def test_publish_patch(create_buckets, create_temp_filesystem, mock_data_constants):
    """
    Tests publish function when a version already exists. When is_path = True,
    publish a patched version and update build_metadata.json.
    Otherwise throw an error."""
    data_path = mock_data_constants["TEST_DATA_DIR"]

    publishing.upload(output_path=data_path, build_key=build_key, acl=TEST_ACL)
    publishing.promote_to_draft(
        build_key=build_key,
        acl=TEST_ACL,
        keep_build=True,
    )
    draft_revision = publishing.get_draft_version_revisions(
        build_key.product, TEST_VERSION
    )[0]
    draft_key = publishing.DraftKey(build_key.product, TEST_VERSION, draft_revision)

    publishing.publish(
        draft_key=draft_key,
        acl=TEST_ACL,
        latest=False,
        is_patch=False,
    )
    # re-publish and assert an error is thrown when is_patch = False
    with pytest.raises(ValueError):
        publishing.publish(
            draft_key=draft_key,
            acl=TEST_ACL,
            latest=False,
            is_patch=False,
        )
    # re-publish and assert a bumped version is created and metadata file is updated
    publishing.publish(
        draft_key=draft_key,
        acl=TEST_ACL,
        latest=True,
        is_patch=True,
    )
    bumped_version = versions.bump(
        previous_version=TEST_VERSION,
        bump_type=versions.VersionSubType.patch,
        bump_by=1,
    ).label
    bumped_publish_key = publishing.PublishKey(publish_key.product, bumped_version)
    assert set(publishing.get_published_versions(product=draft_key.product)) == set(
        [TEST_VERSION, bumped_version]
    )
    # tests version in metadata was updated to patched version
    assert publishing.get_version(bumped_publish_key) == bumped_version
    assert publishing.get_latest_version(TEST_PRODUCT_NAME) == bumped_version


def test_publish_expected_data(
    create_buckets, create_temp_filesystem, mock_data_constants
):
    """Tests whether published data matches original data."""
    data_path = mock_data_constants["TEST_DATA_DIR"]
    publishing.upload(output_path=data_path, build_key=build_key, acl=TEST_ACL)
    publishing.promote_to_draft(
        build_key=build_key,
        acl=TEST_ACL,
    )
    draft_revision = publishing.get_draft_version_revisions(
        build_key.product, TEST_VERSION
    )[0]
    draft_key = publishing.DraftKey(build_key.product, TEST_VERSION, draft_revision)

    publishing.publish(draft_key=draft_key, acl=TEST_ACL)

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
    publishing.upload(output_path=data_path, build_key=build_key, acl=TEST_ACL)
    publishing.promote_to_draft(
        build_key=build_key,
        acl=TEST_ACL,
    )
    draft_revision = publishing.get_draft_version_revisions(
        build_key.product, TEST_VERSION
    )[0]
    draft_key = publishing.DraftKey(build_key.product, TEST_VERSION, draft_revision)
    publishing.publish(draft_key=draft_key, acl=TEST_ACL, latest=False)

    # assert that latest folder was not populated
    publishing.get_latest_version(TEST_PRODUCT_NAME) is None

    s3.delete(TEST_BUCKET_NAME, publish_key.path + "/")

    # assert a file is published to "latest"
    publishing.publish(
        draft_key=draft_key,
        acl=TEST_ACL,
        latest=True,
    )
    assert publishing.get_latest_version(TEST_PRODUCT_NAME) == TEST_VERSION


# TODO: need to fix setup test code to produce different versions
# in build_metadata.yml and test this function

# @pytest.fixture(scope="function")
# def test_publish_latest_ignores_older_versions(
#     create_buckets, create_temp_filesystem, mock_data_constants
# ):
#     """
#     Test to confirm 'latest' folder doesn't get updated with older data version.
#     """
#     data_path = mock_data_constants["TEST_DATA_DIR"]
#     publishing.upload(output_path=data_path, build_key=draft_key, acl=TEST_ACL)

#     publishing.publish(draft_key=draft_key, acl=TEST_ACL, latest=True)
#     # sanity check
#     assert publishing.get_latest_version(TEST_PRODUCT_NAME) == TEST_VERSION

#     # republish an older version. "latest" should NOT be updated
#     TEST_VERSION_OBJ = versions.parse(TEST_VERSION)
#     older_version = versions.MajorMinor(
#         year=TEST_VERSION_OBJ.year, major=TEST_VERSION_OBJ.major - 1
#     ).label

#     with pytest.raises(ValueError) as e_info:
#         publishing.publish(
#             draft_key=publishing.DraftKey(
#                 draft_key.product, version=older_version, revision="1-init"
#             ),
#             acl=TEST_ACL,
#             latest=True,
#         )


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
                Bucket=TEST_BUCKET_NAME, Key=f"{product}/publish/{version}/dummy.txt"
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
        Bucket=TEST_BUCKET_NAME,
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


def test_get_builds(create_buckets, create_temp_filesystem, mock_data_constants):
    data_path = mock_data_constants["TEST_DATA_DIR"]

    publishing.get_builds(TEST_PRODUCT_NAME) == []
    publishing.upload(output_path=data_path, build_key=build_key, acl=TEST_ACL)

    publishing.get_builds(TEST_PRODUCT_NAME) == [TEST_BUILD]


def test_get_draft_versions(
    create_buckets, create_temp_filesystem, mock_data_constants
):
    data_path = mock_data_constants["TEST_DATA_DIR"]
    publishing.upload(output_path=data_path, build_key=build_key, acl=TEST_ACL)

    publishing.get_draft_versions(TEST_PRODUCT_NAME) == []
    publishing.promote_to_draft(build_key=build_key, acl=TEST_ACL)
    publishing.get_draft_versions(TEST_PRODUCT_NAME) == [TEST_VERSION]


def test_get_draft_version_revisions(
    create_buckets, create_temp_filesystem, mock_data_constants
):
    """Validates a list of given draft version revisions is returned correctly."""
    data_path = mock_data_constants["TEST_DATA_DIR"]
    publishing.upload(output_path=data_path, build_key=build_key, acl=TEST_ACL)

    publishing.get_draft_version_revisions(TEST_PRODUCT_NAME, TEST_VERSION) == []
    publishing.promote_to_draft(build_key=build_key, acl=TEST_ACL)

    publishing.get_draft_version_revisions(TEST_PRODUCT_NAME, TEST_VERSION) == ["1"]

    publishing.promote_to_draft(build_key=build_key, acl=TEST_ACL)

    publishing.get_draft_version_revisions(TEST_PRODUCT_NAME, TEST_VERSION) == [
        "1",
        "2",
    ]


def test_get_draft_revision_label(
    create_buckets, create_temp_filesystem, mock_data_constants
):
    data_path = mock_data_constants["TEST_DATA_DIR"]
    publishing.upload(output_path=data_path, build_key=build_key, acl=TEST_ACL)
    publishing.promote_to_draft(
        build_key=build_key, draft_revision_summary="", acl=TEST_ACL
    )
    publishing.promote_to_draft(
        build_key=build_key, draft_revision_summary="second-draft", acl=TEST_ACL
    )
    assert (
        publishing.get_draft_revision_label(build_key.product, TEST_VERSION, 1) == "1"
    )
    assert (
        publishing.get_draft_revision_label(build_key.product, TEST_VERSION, 2)
        == "2-second-draft"
    )


def test_read_csv_error(create_buckets):
    with pytest.raises(FileNotFoundError, match="publishing file"):
        publishing.read_csv(publishing.PublishKey("test", "test"), "file.csv")


def test_missing_build_metadata_error(create_buckets):
    product_key = publishing.PublishKey("test", "test")
    s3.client().put_object(
        Bucket=PUBLISHING_BUCKET, Key=product_key.path + "/fake_file.txt"
    )
    with pytest.raises(
        FileNotFoundError, match=f"Build metadata not found for product {product_key}."
    ):
        publishing.get_build_metadata(product_key)


def test_promote_to_draft_build_folder(
    create_buckets, create_temp_filesystem, mock_data_constants
):
    """
    Test to confirm files are retained or deleted based on input argument.
    """
    data_path = mock_data_constants["TEST_DATA_DIR"]
    publishing.upload(output_path=data_path, build_key=build_key, acl=TEST_ACL)
    publishing.promote_to_draft(
        build_key=build_key,
        acl=TEST_ACL,
        keep_build=False,
    )

    assert publishing.get_builds(product=build_key.product) == []

    publishing.upload(output_path=data_path, build_key=build_key, acl=TEST_ACL)
    publishing.promote_to_draft(
        build_key=build_key,
        acl=TEST_ACL,
        keep_build=True,
    )
    assert publishing.get_builds(product=build_key.product) == [TEST_BUILD]


def test_promote_to_draft_updates_metadata(
    create_buckets, create_temp_filesystem, mock_data_constants
):
    """
    Tests to confirm metadata file in draft contains draft revision name.
    """
    data_path = mock_data_constants["TEST_DATA_DIR"]
    publishing.upload(output_path=data_path, build_key=build_key, acl=TEST_ACL)
    publishing.promote_to_draft(
        build_key=build_key,
        acl=TEST_ACL,
        keep_build=True,
    )
    draft_version = publishing.get_draft_versions(build_key.product)[0]
    draft_revisions = publishing.get_draft_version_revisions(
        build_key.product, draft_version
    )
    # sanity check
    assert len(draft_revisions) == 1

    draft_revision_label = draft_revisions[0]
    draft_key = publishing.DraftKey(
        build_key.product, TEST_VERSION, draft_revision_label
    )
    build_metadata = publishing.get_build_metadata(product_key=draft_key)

    assert build_metadata.draft_revision_name == draft_revision_label


def test_promote_to_draft_revison_versioning(
    create_buckets, create_temp_filesystem, mock_data_constants
):
    """Ensures correct versioning of draft revisions."""
    data_path = mock_data_constants["TEST_DATA_DIR"]
    publishing.upload(output_path=data_path, build_key=build_key, acl=TEST_ACL)
    publishing.promote_to_draft(
        build_key=build_key,
        acl=TEST_ACL,
        keep_build=True,
    )
    publishing.promote_to_draft(
        build_key=build_key,
        acl=TEST_ACL,
        keep_build=True,
        draft_revision_summary="correct-zoning",
    )

    assert publishing.get_draft_versions(build_key.product) == [TEST_VERSION]
    assert publishing.get_draft_version_revisions(build_key.product, TEST_VERSION) == [
        "2-correct-zoning",
        "1",
    ]


@patch("dcpy.connectors.edm.publishing.get_published_versions")
def test_validate_or_patch_version_patch_version(get_published_versions):
    get_published_versions.return_value = [
        "24v3",
        "24v3.0.1",
        "23v3",
        "24v3.1",
        "24v3.1.1",
    ]
    version_to_patch = "24v3"
    assert (
        publishing.validate_or_patch_version(
            product=TEST_PRODUCT_NAME,
            version=version_to_patch,
            is_patch=True,
        )
        == "24v3.0.2"
    )


@patch("dcpy.connectors.edm.publishing.get_published_versions")
def test_validate_or_patch_version_version_already_exists(get_published_versions):
    get_published_versions.return_value = [
        "24v3",
        "24v3.0.1",
        "23v3",
        "24v3.1",
        "24v3.1.1",
    ]
    version_to_patch = "24v3"
    with pytest.raises(
        ValueError,
        match="already exists in published",
    ):
        publishing.validate_or_patch_version(
            product=TEST_PRODUCT_NAME,
            version=version_to_patch,
            is_patch=False,
        )
