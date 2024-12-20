from pathlib import Path

from dcpy.models.connectors.edm.publishing import GisDataset
from dcpy.models.connectors import socrata, web
from dcpy.models.lifecycle.ingest import (
    LocalFileSource,
    ScriptSource,
    S3Source,
    DEPublished,
)
from dcpy.test.conftest import RECIPES_BUCKET

RESOURCES = Path(__file__).parent / "resources"
TEMPLATE_DIR = RESOURCES / "templates"
TEST_DATA_DIR = "test_data"
TEST_DATASET_NAME = "test_dataset"
FAKE_VERSION = "20240101"


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


SOURCE_FILENAMES = [
    (Sources.local_file, "dummy.txt"),
    (Sources.gis, f"{TEST_DATASET_NAME}.zip"),
    (Sources.file_download, "pad_24a.zip"),
    (Sources.api, f"{TEST_DATASET_NAME}.json"),
    (Sources.socrata, f"{TEST_DATASET_NAME}.csv"),
    (Sources.s3, "test.txt"),
]
