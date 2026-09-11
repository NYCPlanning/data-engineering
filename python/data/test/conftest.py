import os

import pytest

# Must be set before dcpy.configuration is imported anywhere in this test run: it
# otherwise defaults BUILD_ENGINE_SCHEMA to the sanitized git branch name, which makes
# any test that reaches the build-schema fallback pass or fail depending on the branch
# it runs on.
os.environ["RECIPES_BUCKET"] = "test-recipes"
os.environ["PUBLISHING_BUCKET"] = "test-publishing"
os.environ["BUILD_ENGINE_SCHEMA"] = "unit_tests"


@pytest.fixture(scope="session", autouse=True)
def ensure_no_callouts():
    import socket

    def guard(*args, **kwargs):
        raise Exception("No internet allowed in the unit tests!")

    socket.socket = guard
