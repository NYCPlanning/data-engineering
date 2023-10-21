import pytest
import os
from dcpy.utils.s3 import list_objects


TEST_BUCKET_NAME = "test-bucket"
TEST_DIR_NAME_1 = "test-dir-1"
TEST_DIR_NAME_2 = "test-dir-2"
TEST_FILE_NAME = "test-file"


def test_list_buckets(s3_client, create_bucket):
    """Tests listing s3 buckets."""
    buckets = s3_client.list_buckets()["Buckets"][0]["Name"]
    assert buckets == TEST_BUCKET_NAME


@pytest.mark.parametrize(
    "prefix, expected_num_objects",
    [
        ("", 2),
        (TEST_DIR_NAME_1, 1),
        (TEST_DIR_NAME_2, 0),
    ],
)
def test_list_objects(s3_client, create_bucket, prefix, expected_num_objects):
    """Tests total number of objects with given prefix."""
    objects_to_add = [TEST_DIR_NAME_1 + "/" + TEST_FILE_NAME, TEST_DIR_NAME_2]

    for obj in objects_to_add:
        s3_client.put_object(Bucket=TEST_BUCKET_NAME, Key=obj)
    actual_objects = list_objects(bucket=TEST_BUCKET_NAME, prefix=prefix)
    assert len(actual_objects) == expected_num_objects
