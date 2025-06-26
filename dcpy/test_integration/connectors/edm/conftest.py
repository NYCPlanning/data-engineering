import os
import pytest

from dcpy.test_integration.conftest import DOCKER_FLAG

# Connector buckets
TEST_EDM_BUCKET = "test-recipes"

os.environ["RECIPES_BUCKET"] = TEST_EDM_BUCKET
os.environ["PUBLISHING_BUCKET"] = TEST_EDM_BUCKET

# Build Engine setup
BUILD_ENGINE_SCHEMA = "connectors_edm_tests"
os.environ["BUILD_ENGINE_SCHEMA"] = BUILD_ENGINE_SCHEMA
os.environ["BUILD_ENGINE_DB"] = "postgres"
host = "postgis:5432" if DOCKER_FLAG else "localhost:5434"
os.environ["BUILD_ENGINE_SERVER"] = f"postgresql://{host}"

os.environ["PGUSER"] = "postgres"
os.environ["PGPASSWORD"] = "postgres"

TEST_LIFECYCLE_DATA_PATH = ".lifecycle_test"

# NOTE: dcpy imports must be after set the env vars above
from dcpy.utils import postgres
from dcpy.utils.logging import logger
from dcpy.lifecycle import config
from dcpy import configuration


@pytest.fixture(scope="function", autouse=True)
def edm_connector_guards():
    assert configuration.RECIPES_BUCKET == TEST_EDM_BUCKET
    assert configuration.PUBLISHING_BUCKET == TEST_EDM_BUCKET


@pytest.fixture(scope="function", autouse=True)
def set_lifecycle_default_conf(tmp_path):
    config.CONF["local_data_path"] = str(tmp_path / TEST_LIFECYCLE_DATA_PATH)
    yield
    config.CONF = config._set_default_conf()


@pytest.fixture
def pg_client():
    return postgres.PostgresClient(schema=BUILD_ENGINE_SCHEMA)


@pytest.fixture
def clean_build_engine(pg_client):
    logger.info("Fixture - pre-test - cleaning build engine schema")
    pg_client.drop_schema()
    yield
    pg_client.drop_schema()
    logger.info("Fixture - post-test - cleaned build engine schema")
