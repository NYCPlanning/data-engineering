import geopandas as gpd
import json
import pytest
from unittest import mock
import shutil

from dcpy.configuration import RECIPES_BUCKET
from dcpy.models.lifecycle.ingest.configuration import ArchivedDataSource
from dcpy.utils import s3
from dcpy.connectors.edm import recipes
from dcpy.lifecycle.ingest.run import (
    ingest,
    extract_and_archive_raw_dataset,
    INGEST_STAGING_DIR,
)

from .shared import FAKE_VERSION, INGEST_DEF_DIR, TEST_DATASET_NAME, RESOLVED

DATASOURCE = "one_to_many"
DATASET = TEST_DATASET_NAME
S3_PATH = f"datasets/{DATASET}/{FAKE_VERSION}/{DATASET}.parquet"
RAW_FOLDER = f"raw_datasets/{DATASET}"


@pytest.fixture
def run_extract_and_archive_raw_dataset(create_buckets, tmp_path):
    return extract_and_archive_raw_dataset(
        RESOLVED,
        staging_dir=tmp_path,
        push=True,
    )


@pytest.fixture
def run_ingest(create_buckets, tmp_path):
    return ingest(
        dataset_id=DATASOURCE,
        version=FAKE_VERSION,
        push=True,
        staging_dir=tmp_path,
        definition_dir=INGEST_DEF_DIR,
    )


@pytest.mark.usefixtures("run_extract_and_archive_raw_dataset")
class TestExtractAndArchiveRawDataset:
    def test_run(self):
        """Mainly an integration test to make sure code runs without error"""
        assert True

    def test_run_raw_output_exists(self, run_extract_and_archive_raw_dataset):
        """Makes sure raw file in s3 properly archived"""
        assert s3.object_exists(
            RECIPES_BUCKET,
            recipes.s3_raw_file_path(
                run_extract_and_archive_raw_dataset.raw_dataset_key
            ),
        )

    def test_config_generated(self, run_extract_and_archive_raw_dataset, tmp_path):
        """
        Feels like a bit of a tautology, but makes sure config file is generated, saved and equals the returned obj
        """
        assert (tmp_path / "datasource.json").exists()
        with open(tmp_path / "datasource.json", "r") as f:
            config = ArchivedDataSource(**json.loads(f.read()))
            assert config == run_extract_and_archive_raw_dataset


class TestTransform:
    pass


class TestIngest:
    def test_run(self, run_ingest):
        assert True

    def test_run_raw_output_exists(self, run_ingest):
        assert s3.object_exists(
            RECIPES_BUCKET, recipes.s3_raw_file_path(run_ingest.raw_dataset_key)
        )

    def test_run_downstream_datasets(self, run_ingest):
        assert len(run_ingest.datasets) == 2

    def test_run_output_exists(self, run_ingest):
        assert s3.object_exists(RECIPES_BUCKET, S3_PATH)

    def test_run_default_folder(self, create_buckets):
        ingest(
            dataset_id=DATASOURCE,
            version=FAKE_VERSION,
            push=True,
            definition_dir=INGEST_DEF_DIR,
        )
        assert s3.object_exists(RECIPES_BUCKET, S3_PATH)
        assert (INGEST_STAGING_DIR / DATASET).exists()
        shutil.rmtree(INGEST_STAGING_DIR)

    def test_skip_archival(self, create_buckets, tmp_path):
        ingest(
            dataset_id=DATASOURCE,
            version=FAKE_VERSION,
            staging_dir=tmp_path,
            push=False,
            definition_dir=INGEST_DEF_DIR,
        )
        assert not s3.object_exists(RECIPES_BUCKET, S3_PATH)

    def test_run_repeat_version(self, create_buckets, tmp_path):
        ingest(
            dataset_id=DATASOURCE,
            version=FAKE_VERSION,
            staging_dir=tmp_path,
            push=True,
            definition_dir=INGEST_DEF_DIR,
            latest=True,
        )
        config = recipes.get_config(DATASET, FAKE_VERSION)
        ingest(
            dataset_id=DATASOURCE,
            version=FAKE_VERSION,
            staging_dir=tmp_path,
            push=True,
            latest=True,
            definition_dir=INGEST_DEF_DIR,
        )
        config2 = recipes.get_config(DATASET, FAKE_VERSION)
        assert (
            config == config2
        )  # would be different if second run with new timestamp had pushed

    def test_run_repeat_version_fails_if_data_diff(self, create_buckets, tmp_path):
        ingest(
            dataset_id=DATASOURCE,
            version=FAKE_VERSION,
            staging_dir=tmp_path,
            push=True,
            definition_dir=INGEST_DEF_DIR,
        )

        # this time, replace the dataframe with a different one in the middle of the ingest process
        with mock.patch("dcpy.utils.geospatial.parquet.read_df") as patch_read_df:
            patch_read_df.return_value = gpd.GeoDataFrame({"a": [None]}).set_geometry(
                "a"
            )
            with pytest.raises(
                FileExistsError,
                match=f"Archived dataset id='{DATASET}' version='{FAKE_VERSION}' already exists and has different data.",
            ):
                ingest(
                    dataset_id=DATASOURCE,
                    version=FAKE_VERSION,
                    staging_dir=tmp_path,
                    push=True,
                    definition_dir=INGEST_DEF_DIR,
                )
