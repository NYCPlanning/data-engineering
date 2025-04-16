"""This feels both trivial and awkward, maybe a sign that this connector is a bit clunky as written
(or belongs outside of connectors as it's a bit of a special entity)"""

import pytest
from unittest.mock import MagicMock

from dcpy.connectors.filesystem import Connector as DriveConnector
from dcpy.connectors.ingest_datastore import (
    Connector as IngestStorageConnector,
    config_filename,
)

CONFIG = MagicMock()
CONFIG.archival.acl = ""
CONFIG.model_dump.return_value = {"key": "value"}

DATASET_ID = "test_dataset"
VERSION = "20240101"
FILENAME = f"{DATASET_ID}.parquet"


@pytest.fixture()
def local_path(tmp_path):
    path = tmp_path / "local"
    path.mkdir()
    return path


@pytest.fixture()
def ingest_path(tmp_path):
    path = tmp_path / "ingest"
    path.mkdir()
    return path


@pytest.fixture
def connector(ingest_path):
    drive = DriveConnector(path=ingest_path)
    return IngestStorageConnector(storage=drive)


def test_push(connector: IngestStorageConnector, local_path, ingest_path):
    file = local_path / FILENAME
    file.touch()
    connector.push(DATASET_ID, version=VERSION, filepath=file, config=CONFIG)
    assert (ingest_path / DATASET_ID / VERSION / FILENAME).exists()


def test_push_latest(connector: IngestStorageConnector, local_path, ingest_path):
    file = local_path / FILENAME
    file.touch()
    connector.push(
        DATASET_ID, version=VERSION, filepath=file, config=CONFIG, latest=True
    )
    assert (ingest_path / DATASET_ID / "latest" / FILENAME).exists()


def test_get_latest(connector: IngestStorageConnector, ingest_path):
    latest_config = ingest_path / DATASET_ID / "latest" / config_filename
    latest_config.parent.mkdir(parents=True)
    with open(latest_config, "w") as file:
        file.write(f"{{'version': '{VERSION}'}}")
    assert connector.get_latest_version(DATASET_ID) == VERSION


def test_list_versions(connector: IngestStorageConnector, local_path):
    file = local_path / FILENAME
    file.touch()
    connector.push(DATASET_ID, version=VERSION, filepath=file, config=CONFIG)
    assert connector.list_versions(DATASET_ID) == [VERSION]
