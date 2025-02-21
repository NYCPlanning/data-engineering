from datetime import datetime
from pathlib import Path

from dcpy.models.connectors.edm.publishing import GisDataset
from dcpy.models.connectors.edm.recipes import Dataset
from dcpy.models import file, library
from dcpy.models.connectors import socrata, web
from dcpy.models.lifecycle.ingest import (
    LocalFileSource,
    ScriptSource,
    S3Source,
    DEPublished,
    DatasetAttributes,
    ArchivalMetadata,
    Ingestion,
    Config,
    ESRIFeatureServer,
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


class Sources:
    local_file = LocalFileSource(type="local_file", path=Path("subfolder/dummy.txt"))
    gis = GisDataset(type="edm_publishing_gis_dataset", name=TEST_DATASET_NAME)
    script = ScriptSource(type="script", connector="web", function="get_df")
    file_download = web.FileDownloadSource(
        type="file_download",
        url="https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/pad_24a.zip",
    )
    api = web.GenericApiSource(
        type="api",
        endpoint="https://www.bklynlibrary.org/locations/json",
        format="json",
    )
    socrata = socrata.Source(
        type="socrata", org=socrata.Org.nyc, uid="w7w3-xahh", format="csv"
    )
    s3 = S3Source(type="s3", bucket=RECIPES_BUCKET, key="inbox/test/test.txt")
    de_publish = DEPublished(
        type="de-published", product=TEST_DATASET_NAME, filename="file.csv"
    )
    esri = ESRIFeatureServer(
        type="esri",
        server="nys_parks",
        dataset="National_Register_Building_Listings",
        layer_id=13,
    )


BASIC_CONFIG = Config(
    id=TEST_DATASET_NAME,
    version=FAKE_VERSION,
    attributes=DatasetAttributes(name=TEST_DATASET_NAME),
    archival=ArchivalMetadata(
        archival_timestamp=datetime(2024, 1, 1),
        raw_filename="dummy.txt",
        acl="public-read",
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
    (Sources.socrata, f"{TEST_DATASET_NAME}.csv"),
    (Sources.s3, "test.txt"),
    (Sources.esri, f"{TEST_DATASET_NAME}.json"),
]
