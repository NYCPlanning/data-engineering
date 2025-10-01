from pathlib import Path
import os


RESOURCES_DIR = Path(__file__).parent / "resources"

# Connector buckets
TEST_EDM_BUCKET = "test-recipes"

os.environ["RECIPES_BUCKET"] = TEST_EDM_BUCKET
os.environ["PUBLISHING_BUCKET"] = TEST_EDM_BUCKET

# Build Engine setup
BUILD_ENGINE_SCHEMA = "connectors_edm_tests"
os.environ["BUILD_ENGINE_SCHEMA"] = BUILD_ENGINE_SCHEMA
os.environ["BUILD_ENGINE_DB"] = "postgres"
os.environ["BUILD_ENGINE_SERVER"] = "postgresql://postgis"

os.environ["PGUSER"] = "postgres"
os.environ["PGPASSWORD"] = "postgres"
