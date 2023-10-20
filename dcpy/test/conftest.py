import boto3
import os
import pytest

from moto import mock_s3
from dcpy.utils.s3 import client

TEST_BUCKET_NAME = "test-bucket"
TEST_DIR_NAME_1 = "test-dir-1"
TEST_DIR_NAME_2 = "test-dir-2"
TEST_FILE_NAME = "test-file"


@pytest.fixture(scope="module")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    """Mocked S3 client."""
    with mock_s3():
        yield client()


@pytest.fixture
def create_bucket(s3_client):
    """Creates a test S3 bucket."""
    s3_client.create_bucket(Bucket=TEST_BUCKET_NAME)
    yield
