import pytest
import os
from conftest import TEST_BUCKET_NAME


@pytest.fixture
def s3_test(s3_client):
    s3_client.create_bucket(Bucket=TEST_BUCKET_NAME)
    yield


def test_list_buckets(s3_client, s3_test):
    buckets = s3_client.list_buckets()['Buckets'][0]['Name']
    assert buckets == TEST_BUCKET_NAME



# def test_create_bucket(s3_client):
#     # s3_client is a fixture defined above that yields a boto3 s3 client.
#     # Feel free to instantiate another boto3 S3 client -- Keep note of the region though.
#     s3_client.create_bucket(Bucket="somebucket")

#     result = s3_client.list_buckets()
#     assert len(result["Buckets"]) == 1
#     assert result["Buckets"][0]["Name"] == "somebucket"
