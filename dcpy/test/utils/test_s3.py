import pytest
import os
from botocore.exceptions import EndpointConnectionError
from dcpy.utils import s3
from dcpy.test.conftest import TEST_BUCKET, TEST_BUCKETS
from pathlib import Path
from io import BytesIO

TEST_DIR_NAME_1 = "test-dir-1"
TEST_DIR_NAME_2 = "test-dir-2"
TEST_FILE_NAME = "test-file"


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
    buckets = [bucket["Name"] for bucket in s3.client().list_buckets()["Buckets"]]
    assert buckets == TEST_BUCKETS


@pytest.mark.parametrize(
    "prefix, expected_num_objects",
    [
        ("", 2),
        (TEST_DIR_NAME_1, 1),
        (TEST_DIR_NAME_2, 1),
    ],
)
def test_list_objects(create_buckets, prefix, expected_num_objects):
    """Tests total number of objects with given prefix."""
    objects_to_add = [TEST_DIR_NAME_1 + "/" + TEST_FILE_NAME, TEST_DIR_NAME_2]
    for obj in objects_to_add:
        s3.client().put_object(Bucket=TEST_BUCKET, Key=obj)
    actual_objects = s3.list_objects(bucket=TEST_BUCKET, prefix=prefix)
    assert len(actual_objects) == expected_num_objects


@pytest.mark.parametrize(
    "prefix, expected_num_objects",
    [("", 2), (TEST_DIR_NAME_1, 1)],
)
def test_get_filenames(create_buckets, prefix, expected_num_objects):
    """Tests total number of objects with given prefix."""
    objects_to_add = [TEST_DIR_NAME_1 + "/" + TEST_FILE_NAME, TEST_DIR_NAME_2]
    for obj in objects_to_add:
        s3.client().put_object(Bucket=TEST_BUCKET, Key=obj)
    actual_objects = s3.get_filenames(bucket=TEST_BUCKET, prefix=prefix)
    assert len(actual_objects) == expected_num_objects


@pytest.mark.parametrize(
    "prefix, index, expected_num_folders",
    [
        ("", 1, 2),
        ("", 2, 1),
        (TEST_DIR_NAME_1, 1, 1),
        (TEST_DIR_NAME_1, 2, 0),
        (TEST_DIR_NAME_2, 1, 0),
    ],
)
def test_get_subfolders(create_buckets, prefix, index, expected_num_folders):
    """Tests total number of folders with given prefix and depth (index)."""
    objects_to_add = [
        TEST_DIR_NAME_1 + "/" + TEST_FILE_NAME,
        TEST_DIR_NAME_1 + "/" + TEST_DIR_NAME_2 + "/",
        TEST_DIR_NAME_2 + "/",
    ]
    for obj in objects_to_add:
        s3.client().put_object(Bucket=TEST_BUCKET, Key=obj)
    actual_objects = s3.get_subfolders(bucket=TEST_BUCKET, prefix=prefix, index=index)

    assert len(actual_objects) == expected_num_folders


def test_upload_file(create_buckets):
    RESOURCES_DIR = Path(__file__).parent / "resources"
    test_file_key = TEST_DIR_NAME_1 + "/" + TEST_FILE_NAME
    test_file_path = Path(RESOURCES_DIR / "data_wkb.csv")
    s3.upload_file(
        bucket=TEST_BUCKET, path=test_file_path, key=test_file_key, acl="public-read"
    )
    assert s3.exists(TEST_BUCKET, test_file_key) == True


def test_upload_file_obj(create_buckets):
    test_file_key = TEST_DIR_NAME_1 + "/" + TEST_FILE_NAME
    RESOURCES_DIR = Path(__file__).parent / "resources"
    with open(RESOURCES_DIR / "data_wkb.csv", "rb") as f:
        file_obj = BytesIO(f.read())
    s3.upload_file_obj(
        finallyile_obj=file_obj,
        bucket=TEST_BUCKET,
        key=test_file_key,
        acl="public-read",
    )
    assert s3.exists(TEST_BUCKET, test_file_key) == True
