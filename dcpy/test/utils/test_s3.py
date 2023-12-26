import pytest
import os
from dcpy.utils import s3
from dcpy.test.conftest import TEST_BUCKET, TEST_BUCKETS

TEST_DIR_NAME_1 = "test-dir-1"
TEST_DIR_NAME_2 = "test-dir-2"
TEST_FILE_NAME = "test-file"


def test_list_buckets(create_buckets):
    """Tests listing s3 buckets."""
    # buckets = s3.client().list_buckets()["Buckets"][0]["Name"]
    buckets = [bucket["Name"] for bucket in s3.client().list_buckets()["Buckets"]]
    assert buckets == TEST_BUCKETS


@pytest.mark.parametrize(
    "prefix, expected_num_objects",
    [
        ("", 2),
        (TEST_DIR_NAME_1, 1),
        (TEST_DIR_NAME_2, 0),
    ],
)
def test_list_objects(create_buckets, prefix, expected_num_objects):
    """Tests total number of objects with given prefix."""
    objects_to_add = [TEST_DIR_NAME_1 + "/" + TEST_FILE_NAME, TEST_DIR_NAME_2]
    for obj in objects_to_add:
        s3.client().put_object(Bucket=TEST_BUCKET, Key=obj)
    actual_objects = s3.list_objects(bucket=TEST_BUCKET, prefix=prefix)
    assert len(actual_objects) == expected_num_objects
