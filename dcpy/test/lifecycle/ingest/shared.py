from datetime import datetime
from pathlib import Path

from dcpy.models.connectors import socrata, esri
from dcpy.models.connectors.edm.recipes import Dataset
from dcpy.models import file, library
from dcpy.models.lifecycle.ingest import (
    LocalFileSource,
    S3Source,
    GisDataset,
    SocrataSource,
    GenericApiSource,
    FileDownloadSource,
    DEPublished,
    Config,
    ESRIFeatureServer,
)
from dcpy.models.lifecycle.ingest_inputs import (
    DatasetAttributes,
    ArchivalMetadata,
    Ingestion,
)
from dcpy.utils.metadata import get_run_details
from dcpy.test.conftest import RECIPES_BUCKET

RESOURCES = Path(__file__).parent / "resources"
TEMPLATE_DIR = RESOURCES / "templates"
TEST_DATA_DIR = "test_data"
TEST_DATASET_NAME = "test_dataset"
TEST_OUTPUT = RESOURCES / TEST_DATA_DIR / f"{TEST_DATASET_NAME}.parquet"
FAKE_VERSION = "20240101"
TEST_DATASET = Dataset(id=TEST_DATASET_NAME, version=FAKE_VERSION)
PROD_TEMPLATE_DIR = (
    Path(__file__).parent.parent.parent.parent.parent / "ingest_templates"
)
