import geopandas as gpd
import pytest
from unittest import mock
import shutil

from dcpy.configuration import RECIPES_BUCKET
from dcpy.utils import s3
from dcpy.connectors.edm import recipes
from dcpy.lifecycle.ingest.run import ingest as run_ingest, TMP_DIR

from dcpy.test.conftest import mock_request_get
from .shared import FAKE_VERSION, TEMPLATE_DIR

DATASET = "bpl_libraries"
S3_PATH = f"datasets/{DATASET}/{FAKE_VERSION}/{DATASET}.parquet"
RAW_FOLDER = f"raw_datasets/{DATASET}"


@pytest.fixture
@mock.patch("requests.get", side_effect=mock_request_get)
def run_basic(mock_request_get, create_buckets, create_temp_filesystem):
    return run_ingest(
        dataset_id=DATASET,
        version=FAKE_VERSION,
        staging_dir=create_temp_filesystem,
        template_dir=TEMPLATE_DIR,
    )


def test_run(run_basic):
    """Mainly an integration test to make sure code runs without error"""
    assert True


def test_run_raw_output_exists(run_basic):
    """Copy of run, but asserts that raw file in s3 properly archived"""
    assert s3.object_exists(
        RECIPES_BUCKET, recipes.s3_raw_file_path(run_basic.raw_dataset_key)
    )


def test_run_output_exists(run_basic):
    """Copy of run, but asserts that output in s3 properly generated"""
    assert s3.object_exists(RECIPES_BUCKET, S3_PATH)


@mock.patch("requests.get", side_effect=mock_request_get)
def test_run_default_folder(mock_request_get, create_buckets, create_temp_filesystem):
    run_ingest(dataset_id=DATASET, version=FAKE_VERSION, template_dir=TEMPLATE_DIR)
    assert s3.object_exists(RECIPES_BUCKET, S3_PATH)
    assert (TMP_DIR / DATASET).exists()
    shutil.rmtree(TMP_DIR)


@mock.patch("requests.get", side_effect=mock_request_get)
def test_skip_archival(mock_request_get, create_buckets, create_temp_filesystem):
    run_ingest(
        dataset_id=DATASET,
        version=FAKE_VERSION,
        staging_dir=create_temp_filesystem,
        skip_archival=True,
        template_dir=TEMPLATE_DIR,
    )
    assert not s3.object_exists(RECIPES_BUCKET, S3_PATH)


@mock.patch("requests.get", side_effect=mock_request_get)
def test_run_update_freshness(mock_request_get, create_buckets, create_temp_filesystem):
    run_ingest(
        dataset_id=DATASET,
        version=FAKE_VERSION,
        staging_dir=create_temp_filesystem,
        template_dir=TEMPLATE_DIR,
    )
    config = recipes.get_config(DATASET, FAKE_VERSION)
    assert config.archival.check_timestamps == []
    run_ingest(
        dataset_id=DATASET,
        version=FAKE_VERSION,
        staging_dir=create_temp_filesystem,
        latest=True,
        template_dir=TEMPLATE_DIR,
    )
    config2 = recipes.get_config(DATASET, FAKE_VERSION)

    assert len(config2.archival.check_timestamps) == 1
    assert config2.freshness > config.freshness

    latest = recipes.get_config(DATASET)
    assert latest == config2


@mock.patch("requests.get", side_effect=mock_request_get)
def test_run_update_freshness_fails_if_data_diff(
    mock_request_get, create_buckets, create_temp_filesystem
):
    """Mainly an integration test to make sure code runs without error"""
    run_ingest(
        dataset_id=DATASET,
        version=FAKE_VERSION,
        staging_dir=create_temp_filesystem,
        template_dir=TEMPLATE_DIR,
    )

    # this time, replace the dataframe with a different one in the middle of the ingest process
    with mock.patch("dcpy.utils.geospatial.parquet.read_df") as patch_read_df:
        patch_read_df.return_value = gpd.GeoDataFrame({"a": [None]}).set_geometry("a")
        with pytest.raises(
            FileExistsError,
            match=f"Archived dataset 'id='{DATASET}' version='{FAKE_VERSION}'' already exists and has different data.",
        ):
            run_ingest(
                dataset_id=DATASET,
                version=FAKE_VERSION,
                staging_dir=create_temp_filesystem,
                template_dir=TEMPLATE_DIR,
            )
