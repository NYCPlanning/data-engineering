import pytest

from dcpy.utils import postgres
from dcpy.utils.logging import logger

BUILD_ENGINE_SCHEMA = "lifecycle_builds_tests"


@pytest.fixture
def pg_client():
    """Create a PostgresClient for testing with an isolated schema."""
    return postgres.PostgresClient(schema=BUILD_ENGINE_SCHEMA)


@pytest.fixture
def clean_build_engine(pg_client):
    """Clean the build engine schema before and after each test."""
    logger.info("Fixture - pre-test - cleaning build engine schema")
    pg_client.drop_schema()
    yield pg_client
    logger.info("Fixture - post-test - cleaned build engine schema")
    pg_client.drop_schema()
