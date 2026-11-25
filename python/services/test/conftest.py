import os

import pytest

# Must be set before dcpy.configuration is imported anywhere in this test run: it
# otherwise defaults BUILD_ENGINE_SCHEMA to the sanitized git branch name.
os.environ["RECIPES_BUCKET"] = "test-recipes"
os.environ["PUBLISHING_BUCKET"] = "test-publishing"
os.environ["BUILD_ENGINE_SCHEMA"] = "unit_tests"

RECIPES_BUCKET = "test-recipes"
PUBLISHING_BUCKET = "test-publishing"
TEST_BUCKETS = [RECIPES_BUCKET, PUBLISHING_BUCKET]


@pytest.fixture(scope="session", autouse=True)
def ensure_no_callouts():
    import socket

    # botocore lazily imports urllib3.contrib.socks (-> PySocks) the first time a boto3
    # client is constructed; pre-import it while socket.socket is still the real class.
    # See python/lifecycle/test/conftest.py.
    try:
        import socks  # noqa: F401
    except ImportError:
        pass

    def guard(*args, **kwargs):
        raise Exception("No internet allowed in the unit tests!")

    socket.socket = guard


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_S3_ENDPOINT"] = "https://s3.amazonaws.com"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ.pop("DCPY_S3_ENDPOINT_DENYLIST", None)
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"


@pytest.fixture(scope="function")
def create_buckets(aws_credentials):
    from moto import mock_aws

    from dcpy.utils import s3

    with mock_aws():
        for bucket in TEST_BUCKETS:
            s3.client().create_bucket(Bucket=bucket)
        yield
