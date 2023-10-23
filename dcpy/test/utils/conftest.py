import boto3
import os
import pytest
from moto import mock_s3
from dcpy.utils.s3 import client


TEST_BUCKET_NAME = "test-bucket"


@pytest.fixture(scope="module")
def s3_client():
    """Mocked S3 client. Note, mock_s3() takes care of updating
    env variables for fake AWS credentials."""
    with mock_s3():
        yield client()


@pytest.fixture(scope="module")
def create_bucket(s3_client):
    """Creates a test S3 bucket."""
    s3_client.create_bucket(Bucket=TEST_BUCKET_NAME)
    yield
