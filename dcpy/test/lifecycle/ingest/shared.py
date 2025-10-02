from datetime import datetime
from pathlib import Path

from dcpy.models.connectors.edm.recipes import Dataset
from dcpy.models import file, library
from dcpy.models.lifecycle.ingest import (
    Source,
    DatasetAttributes,
    ArchivalMetadata,
    Ingestion,
    Config,
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


class Sources:
    local_file = Source(**{"type": "local_file", "path": "subfolder/dummy.txt"})
    gis = Source(**{"type": "edm.publishing.gis", "name": TEST_DATASET_NAME})
    file_download = Source(
        **{
            "type": "file_download",
            "url": "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/pad_24a.zip",
        }
    )
    api = Source(
        **{
            "type": "api",
            "endpoint": "https://www.bklynlibrary.org/locations/json",
            "format": "json",
        },
        _ds_id=TEST_DATASET_NAME,
    )
    socrata = Source(
        **{
            "type": "socrata",
            "org": "nyc",
            "uid": "w7w3-xahh",
            "format": "csv",
        }
    )
    s3 = Source(
        **{
            "type": "s3",
            "bucket": RECIPES_BUCKET,
            "key": "inbox/test/test.txt",
        }
    )
    de_publish = Source(
        **{
            "type": "edm.publishing.published",
            "product": TEST_DATASET_NAME,
            "filepath": "file.csv",
        }
    )
    esri = Source(
        **{
            "type": "arcgis_feature_server",
            "server": "nys_parks",
            "dataset": "National_Register_Building_Listings",
            "layer_id": "13",
            "layer_name": "National Register Building Listings",
        }
    )


BASIC_CONFIG = Config(
    id=TEST_DATASET_NAME,
    version=FAKE_VERSION,
    attributes=DatasetAttributes(name=TEST_DATASET_NAME),
    archival=ArchivalMetadata(
        archival_timestamp=datetime(2024, 1, 1),
        raw_filename="dummy.txt",
        acl="private",
    ),
    ingestion=Ingestion(source=Sources.local_file, file_format=file.Csv(type="csv")),
    run_details=get_run_details(),
)

BASIC_LIBRARY_CONFIG = library.Config(
    dataset=library.DatasetDefinition(
        name=TEST_DATASET_NAME,
        version=FAKE_VERSION,
        acl="public-read",
        source=library.DatasetDefinition.SourceSection(),
        destination=library.DatasetDefinition.DestinationSection(
            geometry=library.GeometryType(SRS="NONE", type="NONE")
        ),
    ),
    execution_details=get_run_details(),
)


SOURCE_FILENAMES = [
    (Sources.local_file, "dummy.txt"),
    (Sources.gis, f"{TEST_DATASET_NAME}.zip"),
    (Sources.file_download, "pad_24a.zip"),
    (Sources.api, f"{TEST_DATASET_NAME}.json"),
    (Sources.socrata, f"{Sources.socrata.key}.csv"),
    (Sources.s3, "test.txt"),
    (Sources.esri, f"{Sources.esri.model_dump()['layer_name']}.geojson"),
]
