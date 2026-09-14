import os
from pathlib import Path

import pytest

# Must be set before dcpy.configuration is imported below: it otherwise defaults
# BUILD_ENGINE_SCHEMA to the sanitized git branch name, which makes any test that
# reaches the build-schema fallback pass or fail depending on the branch it runs on.
os.environ["RECIPES_BUCKET"] = "test-recipes"
os.environ["PUBLISHING_BUCKET"] = "test-publishing"
os.environ["BUILD_ENGINE_SCHEMA"] = "unit_tests"

from dcpy import configuration  # noqa: E402
from dcpy.lifecycle import product_metadata  # noqa: E402

PRODUCT_METADATA_REPO_PATH: Path = Path(configuration.PRODUCT_METADATA_REPO_PATH)


@pytest.fixture(scope="session", autouse=True)
def ensure_no_callouts():
    import socket

    # botocore lazily imports urllib3.contrib.socks (-> PySocks) the first time a
    # boto3 client is constructed, to register SOCKS proxy support - if that first
    # happens after socket.socket is replaced below, PySocks' `class
    # _BaseSocket(socket.socket)` tries to subclass a plain function and crashes.
    # Pre-importing here (while socket.socket is still the real class) caches it in
    # sys.modules, so any later import elsewhere is a no-op. Only surfaces when this
    # package's tests run in isolation (own pytest process, nothing else has already
    # triggered the import first).
    try:
        import socks  # noqa: F401
    except ImportError:
        pass

    def guard(*args, **kwargs):
        raise Exception("No internet allowed in the unit tests!")

    socket.socket = guard


if not PRODUCT_METADATA_REPO_PATH.exists():
    raise FileNotFoundError(
        f"PRODUCT_METADATA_REPO_PATH points to non-existent directory: {PRODUCT_METADATA_REPO_PATH}"
    )


@pytest.fixture(scope="session")
def org_md():
    """
    Load org metadata from product-metadata repo.

    NOTE: No template variables are passed. All Jinja2 templates in the
    product-metadata repo MUST have corresponding entries in snippets/strings.yml.
    """
    return product_metadata.load()


@pytest.fixture
def test_dcpy_product(org_md):
    """Get test_dcpy product for testing."""
    return org_md.product("test_dcpy")


@pytest.fixture
def test_overrides_dataset(test_dcpy_product):
    """Get test_overrides dataset for override hierarchy testing."""
    return test_dcpy_product.dataset("test_overrides")
