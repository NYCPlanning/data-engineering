from unittest import mock
import shutil

from dcpy.utils import s3
from dcpy.connectors.edm.recipes import BUCKET
from dcpy.lifecycle.ingest.run import run, TMP_DIR

from dcpy.test.conftest import mock_request_get
from . import FAKE_VERSION

DATASET = "bpl_libraries"
S3_PATH = f"ingest_datasets/{DATASET}/{FAKE_VERSION}/{DATASET}.parquet"
RAW_FOLDER = f"raw_datasets/{DATASET}"


@mock.patch("requests.get", side_effect=mock_request_get)
def test_run(mock_request_get, create_buckets, create_temp_filesystem):
    """Mainly an integration test to make sure code runs without error"""
    run(dataset=DATASET, version=FAKE_VERSION, staging_dir=create_temp_filesystem)
    assert len(s3.get_subfolders(BUCKET, RAW_FOLDER)) == 1
    assert s3.exists(BUCKET, S3_PATH)


@mock.patch("requests.get", side_effect=mock_request_get)
def test_run_default_folder(mock_request_get, create_buckets, create_temp_filesystem):
    run(dataset=DATASET, version=FAKE_VERSION)
    assert s3.exists(BUCKET, S3_PATH)
    assert (TMP_DIR / DATASET).exists()
    shutil.rmtree(TMP_DIR)


@mock.patch("requests.get", side_effect=mock_request_get)
def test_skip_archival(mock_request_get, create_buckets, create_temp_filesystem):
    run(
        dataset=DATASET,
        version=FAKE_VERSION,
        staging_dir=create_temp_filesystem,
        skip_archival=True,
    )
    assert not s3.exists(BUCKET, S3_PATH)
