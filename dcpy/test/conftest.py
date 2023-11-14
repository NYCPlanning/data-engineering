import boto3
import os
import pytest
from moto import mock_s3
from dcpy.utils import s3


TEST_BUCKET_NAME = "edm-publishing"


@pytest.fixture(scope="session")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    if "AWS_S3_ENDPOINT" in os.environ:
        os.environ.pop("AWS_S3_ENDPOINT")


@pytest.fixture(scope="module")
def create_bucket(aws_credentials):
    """Creates a test S3 bucket."""
    with mock_s3():
        s3.client().create_bucket(Bucket=TEST_BUCKET_NAME)
        yield  ## the yield within the mock_s3() is key to persisting the mocked session
