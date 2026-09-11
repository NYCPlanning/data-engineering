import os
from pathlib import Path

import pytest

# Must be set before dcpy.configuration is imported anywhere in this test run: it
# otherwise defaults BUILD_ENGINE_SCHEMA to the sanitized git branch name, which makes
# any test that reaches the build-schema fallback pass or fail depending on the branch
# it runs on.
os.environ["RECIPES_BUCKET"] = "test-recipes"
os.environ["PUBLISHING_BUCKET"] = "test-publishing"
os.environ["BUILD_ENGINE_SCHEMA"] = "unit_tests"

UTILS_RESOURCES = Path(__file__).parent / "resources"

TEST_BUCKET = "test-bucket"
RECIPES_BUCKET = "test-recipes"
PUBLISHING_BUCKET = "test-publishing"
TEST_BUCKETS = [TEST_BUCKET, RECIPES_BUCKET, PUBLISHING_BUCKET]


@pytest.fixture(scope="session", autouse=True)
def ensure_no_callouts():
    import socket

    def guard(*args, **kwargs):
        raise Exception("No internet allowed in the unit tests!")

    socket.socket = guard


@pytest.fixture(scope="function")
def utils_resources_path():
    return UTILS_RESOURCES


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    if "AWS_S3_ENDPOINT" in os.environ:
        os.environ.pop("AWS_S3_ENDPOINT")
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"


@pytest.fixture(scope="function")
def create_buckets(aws_credentials):
    """Creates a test S3 bucket."""
    from dcpy.utils import s3
    from moto import mock_aws

    with mock_aws():
        for bucket in TEST_BUCKETS:
            s3.client().create_bucket(Bucket=bucket)
        yield  ## the yield within the mock_aws() is key to persisting the mocked session
