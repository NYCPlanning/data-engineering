from pathlib import Path
from unittest.mock import patch

import pytest

from dcpy.connectors.edm.connectors import PrivateConnector
from dcpy.connectors.hybrid_pathed_storage import PathedStorageConnector, StorageType
from dcpy.utils import s3

from ..conftest import TEST_BUCKET

DATASET = "private_ds"
VERSION = "26B"
FILES = {"a.csv": "a\n1\n", "b.csv": "b\n2\n"}


@pytest.fixture
def connector(create_buckets):
    for name, contents in FILES.items():
        s3.client().put_object(
            Bucket=TEST_BUCKET,
            Key=f"datasets/{DATASET}/{VERSION}/{name}",
            Body=contents.encode(),
        )
    return PrivateConnector.create(
        storage=PathedStorageConnector.from_storage_kwargs(
            conn_type="edm.private",
            storage_backend=StorageType.S3,
            s3_bucket=TEST_BUCKET,
            s3_endpoint_url="https://s3.amazonaws.com",  # where moto intercepts
            root_folder="datasets",
            _validate_root_path=False,
        )
    )


def _spy_pull():
    return patch.object(
        PathedStorageConnector,
        "pull",
        autospec=True,
        side_effect=PathedStorageConnector.pull,
    )


def test_directory_pull_reuses_complete_local_copy(connector, tmp_path: Path):
    dest = tmp_path / DATASET / VERSION
    with _spy_pull() as pull:
        first = connector.pull(DATASET, version=VERSION, destination_path=dest)
        second = connector.pull(DATASET, version=VERSION, destination_path=dest)

    assert pull.call_count == 1, "second pull should come from the local copy"
    assert first["path"] == second["path"] == dest
    assert {p.name: p.read_text() for p in dest.iterdir()} == FILES
    assert (tmp_path / DATASET / f"{VERSION}.pull_complete").exists()


def test_directory_pull_redownloads_incomplete_copy(connector, tmp_path: Path):
    """A directory left by an interrupted pull has no marker, so it isn't trusted."""
    dest = tmp_path / DATASET / VERSION
    dest.mkdir(parents=True)
    (dest / "a.csv").write_text("a\n")  # truncated

    with _spy_pull() as pull:
        connector.pull(DATASET, version=VERSION, destination_path=dest)

    assert pull.call_count == 1
    assert {p.name: p.read_text() for p in dest.iterdir()} == FILES


def test_failed_directory_pull_is_not_marked_complete(connector, tmp_path: Path):
    dest = tmp_path / DATASET / VERSION
    with patch.object(PathedStorageConnector, "pull", side_effect=ConnectionError):
        with pytest.raises(ConnectionError):
            connector.pull(DATASET, version=VERSION, destination_path=dest)

    assert not (tmp_path / DATASET / f"{VERSION}.pull_complete").exists()


def test_file_pull_is_not_cached(connector, tmp_path: Path):
    with _spy_pull() as pull:
        for _ in range(2):
            connector.pull(
                DATASET, version=VERSION, destination_path=tmp_path, filepath="a.csv"
            )
    assert pull.call_count == 2
