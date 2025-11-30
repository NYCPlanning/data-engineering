import os
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dcpy.test.conftest import (
    PUBLISHING_BUCKET,
    RECIPES_BUCKET,
    TEST_BUCKET,
    TEST_BUCKETS,
)
from dcpy.utils import s3

TEST_DIR_NAME_1 = "test-dir-1"
TEST_DIR_NAME_2 = "test-dir-2"
TEST_FILE_NAME = "test-file"

TEST_OBJECTS = [
    f"{TEST_DIR_NAME_1}/{TEST_FILE_NAME}",
    f"{TEST_DIR_NAME_1}/{TEST_DIR_NAME_2}/{TEST_FILE_NAME}",
    f"{TEST_DIR_NAME_2}/{TEST_FILE_NAME}",
]


@pytest.fixture()
def put_test_objects(create_buckets):
    for obj in TEST_OBJECTS:
        s3.client().put_object(Bucket=TEST_BUCKET, Key=obj)


# assert that the default mock credentials work
def test_client_create(aws_credentials):
    s3.client()


# assert client creation results for invalid client endpoint_url options
# shows approaches that don't work for a mock endpoint in aws_credentials
def test_client_create_errors(aws_credentials):
    with pytest.raises(TypeError, match="str expected, not NoneType"):
        os.environ["AWS_S3_ENDPOINT"] = None

    with pytest.raises(ValueError, match="Invalid endpoint: testing"):
        os.environ["AWS_S3_ENDPOINT"] = "testing"
        s3.client()

    with pytest.raises(ValueError, match="Invalid endpoint: "):
        os.environ["AWS_S3_ENDPOINT"] = ""
        s3.client()


def test_list_buckets(create_buckets):
    """Tests listing s3 buckets."""
    buckets = s3.list_buckets()
    assert buckets == TEST_BUCKETS


@pytest.mark.parametrize(
    "prefix, expected_num_objects",
    [
        ("", 3),
        (TEST_DIR_NAME_1, 2),
        (TEST_DIR_NAME_2, 1),
    ],
)
def test_list_objects(create_buckets, put_test_objects, prefix, expected_num_objects):
    """Tests total number of objects with given prefix."""
    actual_objects = s3.list_objects(bucket=TEST_BUCKET, prefix=prefix)
    assert len(actual_objects) == expected_num_objects


@pytest.mark.parametrize(
    "prefix, expected_files",
    [
        ("", set(TEST_OBJECTS)),
        (TEST_DIR_NAME_1, {TEST_FILE_NAME, f"{TEST_DIR_NAME_2}/{TEST_FILE_NAME}"}),
    ],
)
def test_get_filenames(create_buckets, put_test_objects, prefix, expected_files):
    """Tests total number of objects with given prefix."""
    actual_objects = s3.get_filenames(bucket=TEST_BUCKET, prefix=prefix)
    assert actual_objects == expected_files


@pytest.mark.parametrize(
    "prefix, index, expected_folders",
    [
        ("", 1, [TEST_DIR_NAME_1, TEST_DIR_NAME_2]),
        ("", 2, [f"{TEST_DIR_NAME_1}/{TEST_DIR_NAME_2}"]),
        (TEST_DIR_NAME_1, 1, [TEST_DIR_NAME_2]),
        (TEST_DIR_NAME_1, 2, []),
        (TEST_DIR_NAME_2, 1, []),
    ],
)
def test_get_subfolders(
    create_buckets, put_test_objects, prefix, index, expected_folders
):
    """Tests total number of folders with given prefix and depth (index)."""
    actual_objects = s3.get_subfolders(bucket=TEST_BUCKET, prefix=prefix, index=index)
    assert actual_objects == expected_folders


def test_upload_file(create_buckets):
    RESOURCES_DIR = Path(__file__).parent / "resources"
    test_file_key = TEST_DIR_NAME_1 + "/" + TEST_FILE_NAME
    test_file_path = Path(RESOURCES_DIR / "data_wkb.csv")
    s3.upload_file(
        bucket=TEST_BUCKET, path=test_file_path, key=test_file_key, acl="public-read"
    )
    assert s3.object_exists(TEST_BUCKET, test_file_key)


def test_upload_file_obj(create_buckets):
    test_file_key = TEST_DIR_NAME_1 + "/" + TEST_FILE_NAME
    RESOURCES_DIR = Path(__file__).parent / "resources"
    with open(RESOURCES_DIR / "data_wkb.csv", "rb") as f:
        file_obj = BytesIO(f.read())
    s3.upload_file_obj(
        file_obj=file_obj,
        bucket=TEST_BUCKET,
        key=test_file_key,
        acl="public-read",
    )
    assert s3.object_exists(TEST_BUCKET, test_file_key)


def test_copy_folder_via_download(create_buckets, put_test_objects):
    s3.copy_folder_via_download(
        TEST_BUCKET, RECIPES_BUCKET, TEST_DIR_NAME_1, TEST_DIR_NAME_1, acl="public-read"
    )
    assert s3.get_filenames(RECIPES_BUCKET, "") == set(TEST_OBJECTS[:2])


@patch("dcpy.utils.s3.copy_folder_via_download")
def test_copy_folder_within_bucket(copy: MagicMock, create_buckets, put_test_objects):
    new_folder = "new_folder"
    s3.copy_folder(TEST_BUCKET, TEST_DIR_NAME_1, new_folder, acl="public-read")
    assert not copy.called
    assert s3.get_filenames(TEST_BUCKET, new_folder) == s3.get_filenames(
        TEST_BUCKET, TEST_DIR_NAME_1
    )


mock_regions = {
    TEST_BUCKET: "nyc3b",
    RECIPES_BUCKET: "nyc3b",
    PUBLISHING_BUCKET: "nyc3d",
}


@patch("dcpy.utils.s3.copy_folder_via_download")
@patch("dcpy.utils.s3.get_bucket_region", side_effect=lambda b: mock_regions[b])
def test_copy_folder_within_region(
    bucket_region, copy, create_buckets, put_test_objects
):
    s3.copy_folder(
        TEST_BUCKET,
        TEST_DIR_NAME_1,
        TEST_DIR_NAME_1,
        acl="public-read",
        target_bucket=RECIPES_BUCKET,
    )
    assert not copy.called
    assert s3.get_filenames(RECIPES_BUCKET, "") == set(TEST_OBJECTS[:2])


@patch("dcpy.utils.s3.copy_folder_via_download")
@patch("dcpy.utils.s3.get_bucket_region", side_effect=lambda b: mock_regions[b])
def test_copy_folder_across_regions_calls_manual_copy(
    bucket_region, copy, create_buckets, put_test_objects
):
    s3.copy_folder(
        TEST_BUCKET,
        TEST_DIR_NAME_1,
        TEST_DIR_NAME_1,
        acl="public-read",
        target_bucket=PUBLISHING_BUCKET,
    )
    assert copy.called
    assert len(s3.get_filenames(PUBLISHING_BUCKET, "")) == 0


@patch("dcpy.utils.s3.get_bucket_region", side_effect=lambda b: mock_regions[b])
def test_copy_folder_across_regions(bucket_region, create_buckets, put_test_objects):
    s3.copy_folder(
        TEST_BUCKET,
        TEST_DIR_NAME_1,
        TEST_DIR_NAME_1,
        acl="public-read",
        target_bucket=PUBLISHING_BUCKET,
    )
    assert s3.get_filenames(PUBLISHING_BUCKET, "") == set(TEST_OBJECTS[:2])
