import os
import sys
from pathlib import Path

import pytest

# Hosts moto answers for: one stands in for DigitalOcean, the other for the gateway. Must be
# set before moto is imported.
SOURCE_ENDPOINT = "https://source.test"
DEST_ENDPOINT = "https://dest.test"
os.environ["MOTO_S3_CUSTOM_ENDPOINTS"] = f"{SOURCE_ENDPOINT},{DEST_ENDPOINT}"

sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture(scope="session", autouse=True)
def ensure_no_callouts():
    import socket

    import boto3  # noqa: F401
    import moto  # noqa: F401

    try:
        import socks  # noqa: F401
    except ImportError:
        pass

    def guard(*args, **kwargs):
        raise Exception("No internet allowed in the unit tests!")

    socket.socket = guard


@pytest.fixture
def endpoints(monkeypatch):
    """Source and destination credentials, under the names the script reads."""
    for name in (
        "DCPY_S3_ENDPOINT_DENYLIST",
        "AZ_S3_CA_BUNDLE",
        "AZ_RECIPES_BUCKET",
        "AZ_PUBLISHING_BUCKET",
        "AZ_RECIPES_BUCKET_ROOT_FOLDER",
        "RECIPES_BUCKET_ROOT_FOLDER",
        "INBOX_ROOT_FOLDER",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_ENDPOINT", SOURCE_ENDPOINT)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "do-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "do-secret")
    monkeypatch.setenv("AZ_S3_ENDPOINT", DEST_ENDPOINT)
    monkeypatch.setenv("AZ_S3_ACCESS_KEY_ID", "az-key")
    monkeypatch.setenv("AZ_S3_SECRET_ACCESS_KEY", "az-secret")
