import geopandas as gpd
import pytest
from unittest import mock
import shutil

from dcpy.configuration import RECIPES_BUCKET
from dcpy.utils import s3
from dcpy.connectors.edm import recipes
from dcpy.lifecycle.ingest.run import ingest as run_ingest, INGEST_STAGING_DIR

from dcpy.test.conftest import mock_request_get
from .shared import FAKE_VERSION, TEMPLATE_DIR

DATASET = "bpl_libraries"
S3_PATH = f"datasets/{DATASET}/{FAKE_VERSION}/{DATASET}.parquet"
RAW_FOLDER = f"raw_datasets/{DATASET}"


@pytest.fixture
@mock.patch("requests.get", side_effect=mock_request_get)
def run_basic(mock_request_get, create_buckets, tmp_path):
    return run_ingest(
        dataset_id=DATASET,
        version=FAKE_VERSION,
        push=True,
        dataset_staging_dir=tmp_path,
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
def test_run_default_folder(mock_request_get, create_buckets):
    run_ingest(
        dataset_id=DATASET,
        version=FAKE_VERSION,
        push=True,
        template_dir=TEMPLATE_DIR,
    )
    assert s3.object_exists(RECIPES_BUCKET, S3_PATH)
    assert (INGEST_STAGING_DIR / DATASET).exists()
    shutil.rmtree(INGEST_STAGING_DIR)


@mock.patch("requests.get", side_effect=mock_request_get)
def test_skip_archival(mock_request_get, create_buckets, tmp_path):
    run_ingest(
        dataset_id=DATASET,
        version=FAKE_VERSION,
        dataset_staging_dir=tmp_path,
        push=False,
        template_dir=TEMPLATE_DIR,
    )
    assert not s3.object_exists(RECIPES_BUCKET, S3_PATH)


@mock.patch("requests.get", side_effect=mock_request_get)
def test_run_repeat_version(mock_request_get, create_buckets, tmp_path):
    """Essentially just a sanity check - attempting to archive should not push the second time"""
    run_ingest(
        dataset_id=DATASET,
        version=FAKE_VERSION,
        dataset_staging_dir=tmp_path,
        push=True,
        template_dir=TEMPLATE_DIR,
        latest=True,
    )
    config = recipes.get_config(DATASET, FAKE_VERSION)
    run_ingest(
        dataset_id=DATASET,
        version=FAKE_VERSION,
        dataset_staging_dir=tmp_path,
        push=True,
        latest=True,
        template_dir=TEMPLATE_DIR,
    )
    config2 = recipes.get_config(DATASET, FAKE_VERSION)
    assert (
        config == config2
    )  # would be different if second run with new timestamp had pushed


@mock.patch("requests.get", side_effect=mock_request_get)
def test_run_repeat_version_fails_if_data_diff(
    mock_request_get, create_buckets, tmp_path
):
    """Mainly an integration test to make sure code runs without error"""
    run_ingest(
        dataset_id=DATASET,
        version=FAKE_VERSION,
        dataset_staging_dir=tmp_path,
        push=True,
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
                dataset_staging_dir=tmp_path,
                push=True,
                template_dir=TEMPLATE_DIR,
            )


def test_run_missing_template_dir():
    with pytest.raises(KeyError, match="Missing required env variable: 'TEMPLATE_DIR'"):
        run_ingest(
            dataset_id=DATASET,
            version=FAKE_VERSION,
            template_dir=None,
        )
