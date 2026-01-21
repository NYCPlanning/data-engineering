import os
from pathlib import Path

import pytest

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


# Add incremental pytest. Docs here: https://docs.pytest.org/en/latest/example/simple.html#incremental-testing-test-steps
# store history of failures per test class name and per index in parametrize (if parametrize used)
_test_failed_incremental: dict[str, dict[tuple[int, ...], str]] = {}


def pytest_configure(config):
    """Register custom marks."""
    config.addinivalue_line(
        "markers",
        "incremental: mark test to run incrementally, stopping on first failure",
    )


def pytest_runtest_makereport(item, call):
    if "incremental" in item.keywords:
        # incremental marker is used
        if call.excinfo is not None:
            # the test has failed
            # retrieve the class name of the test
            cls_name = str(item.cls)
            # retrieve the index of the test (if parametrize is used in combination with incremental)
            parametrize_index = (
                tuple(item.callspec.indices.values())
                if hasattr(item, "callspec")
                else ()
            )
            # retrieve the name of the test function
            test_name = item.originalname or item.name
            # store in _test_failed_incremental the original name of the failed test
            _test_failed_incremental.setdefault(cls_name, {}).setdefault(
                parametrize_index, test_name
            )


def pytest_runtest_setup(item):
    if "incremental" in item.keywords:
        # retrieve the class name of the test
        cls_name = str(item.cls)
        # check if a previous test has failed for this class
        if cls_name in _test_failed_incremental:
            # retrieve the index of the test (if parametrize is used in combination with incremental)
            parametrize_index = (
                tuple(item.callspec.indices.values())
                if hasattr(item, "callspec")
                else ()
            )
            # retrieve the name of the first test function to fail for this class name and index
            test_name = _test_failed_incremental[cls_name].get(parametrize_index, None)
            # if name found, test has failed for the combination of class name & test name
            if test_name is not None:
                pytest.xfail(f"previous test failed ({test_name})")
