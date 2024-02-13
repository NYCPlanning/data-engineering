import pytest
import os
from botocore.exceptions import EndpointConnectionError
from dcpy.utils import s3
from dcpy.test.conftest import TEST_BUCKET, TEST_BUCKETS

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
    [
        ("", 2),
        (TEST_DIR_NAME_1, 1),
        (TEST_DIR_NAME_2, 0),
    ],
)
def test_get_filenames(create_buckets, prefix, expected_num_objects):
    """Tests total number of objects with given prefix."""
    objects_to_add = [TEST_DIR_NAME_1 + "/" + TEST_FILE_NAME, TEST_DIR_NAME_2]
    for obj in objects_to_add:
        s3.client().put_object(Bucket=TEST_BUCKET, Key=obj)
    actual_objects = s3.get_filenames(bucket=TEST_BUCKET, prefix=prefix)
    assert len(actual_objects) == expected_num_objects
