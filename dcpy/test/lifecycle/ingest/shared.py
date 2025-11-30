import json
from pathlib import Path

from dcpy.models.connectors.edm.recipes import Dataset
from dcpy.models.lifecycle.ingest import (
    ArchivedDataSource,
    IngestedDataset,
    ResolvedDataSource,
    Source,
)
from dcpy.utils.metadata import RunDetails

RESOURCES = Path(__file__).parent / "resources"
INGEST_DEF_DIR = RESOURCES / "definitions"
TEST_DATA_DIR = RESOURCES / "test_data"
TEST_DATASET_NAME = "test_dataset"
TEST_OUTPUT = TEST_DATA_DIR / f"{TEST_DATASET_NAME}.parquet"
FAKE_VERSION = "20240101"
TEST_DATASET = Dataset(id=TEST_DATASET_NAME, version=FAKE_VERSION)
RUN_DETAILS = RunDetails(
    **{
        "type": "manual",
        "runner": {"username": "user"},
        "timestamp": "2025-09-26T12:38:22.544455-04:00",
    }
)


def get_yaml(path: Path) -> dict:
    with open(path, "r") as f:
        return json.load(f)


SOURCE = Source(type="local_file", key=str(TEST_DATA_DIR / "test.csv"))

RESOLVED = ResolvedDataSource(
    **get_yaml(RESOURCES / "configs" / "definition.lock.json")
)
ARCHIVED = ARCHIVED = ArchivedDataSource(
    **get_yaml(RESOURCES / "configs" / "datasource.json")
)
DOWNSTREAM_DATASET_1 = IngestedDataset(
    **get_yaml(RESOURCES / "configs" / f"{TEST_DATASET_NAME}.json")
)
