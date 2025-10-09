import geopandas as gpd
import json
import pytest
from unittest import mock

from dcpy.models.lifecycle.ingest import ArchivedDataSource, IngestedDataset
from dcpy.lifecycle.ingest import connectors
from dcpy.lifecycle.ingest.run import (
    ingest,
    extract_and_archive_raw_dataset,
)

from .shared import FAKE_VERSION, INGEST_DEF_DIR, TEST_DATASET_NAME, RESOLVED

DATASOURCE = "one_to_many"
DATASET = TEST_DATASET_NAME


def raw_storage():
    return connectors.get_raw_datastore_connector().storage.storage.root_path


def storage():
    return connectors.get_processed_datastore_connector().storage.storage.root_path


@pytest.fixture
def run_extract_and_archive_raw_dataset(create_buckets, tmp_path):
    return extract_and_archive_raw_dataset(
        RESOLVED,
        staging_dir=tmp_path / ".staging",
        push=True,
    )


@pytest.fixture
def run_ingest(create_buckets, tmp_path):
    return ingest(
        dataset_id=DATASOURCE,
        version=FAKE_VERSION,
        push=True,
        staging_dir=tmp_path / ".staging",
        definition_dir=INGEST_DEF_DIR,
    )


@pytest.mark.usefixtures("run_extract_and_archive_raw_dataset")
class TestExtractAndArchiveRawDataset:
    def test_run(self):
        """Mainly an integration test to make sure code runs without error"""
        assert True

    def test_run_raw_output_exists(
        self, run_extract_and_archive_raw_dataset: ArchivedDataSource
    ):
        """Makes sure raw file is properly archived"""
        path = raw_storage() / run_extract_and_archive_raw_dataset.raw_dataset_path
        assert path.exists()

    def test_config_generated(
        self, run_extract_and_archive_raw_dataset: ArchivedDataSource, tmp_path
    ):
        """
        Feels like a bit of a tautology, but makes sure config file is generated, saved and equals the returned obj
        """
        ## tmp_path is used as the staging dir in the fixture to run_extract_and_archive
        assert (tmp_path / ".staging" / "datasource.json").exists()
        with open(tmp_path / ".staging" / "datasource.json", "r") as f:
            config = ArchivedDataSource(**json.loads(f.read()))
            assert config == run_extract_and_archive_raw_dataset


class TestTransform:
    pass


class TestIngest:
    def test_run(self, run_ingest):
        assert True

    def test_run_all_downstream_datasets_generated(
        self, run_ingest: list[IngestedDataset]
    ):
        assert len(run_ingest) == 2

    def test_run_raw_output_exists(self, run_ingest: list[IngestedDataset]):
        assert (raw_storage() / run_ingest[0].raw_dataset_path).exists()

    def test_run_output_exists(self, run_ingest: list[IngestedDataset]):
        assert (storage() / run_ingest[0].dataset_path).exists()
        assert (storage() / run_ingest[1].dataset_path).exists()

    def test_skip_archival(self, tmp_path):
        run_ingest = ingest(
            dataset_id=DATASOURCE,
            version=FAKE_VERSION,
            staging_dir=tmp_path / ".staging",
            push=False,
            definition_dir=INGEST_DEF_DIR,
        )
        assert not (
            (raw_storage() / run_ingest[0].raw_dataset_path).exists()
            or (storage() / run_ingest[0].dataset_path).exists()
            or (storage() / run_ingest[1].dataset_path).exists()
        )

    def test_run_repeat_version(self, tmp_path):
        ingest(
            dataset_id=DATASOURCE,
            version=FAKE_VERSION,
            staging_dir=tmp_path / ".staging",
            push=True,
            definition_dir=INGEST_DEF_DIR,
            latest=True,
        )
        config = connectors.get_processed_datastore_connector().get_config(
            DATASET, FAKE_VERSION
        )
        ingest(
            dataset_id=DATASOURCE,
            version=FAKE_VERSION,
            staging_dir=tmp_path / ".staging",
            push=True,
            latest=True,
            definition_dir=INGEST_DEF_DIR,
        )
        config2 = connectors.get_processed_datastore_connector().get_config(
            DATASET, FAKE_VERSION
        )
        assert (
            config == config2
        )  # would be different if second run with new timestamp had pushed

    def test_run_repeat_version_fails_if_data_diff(self, create_buckets, tmp_path):
        ingest(
            dataset_id=DATASOURCE,
            version=FAKE_VERSION,
            staging_dir=tmp_path / ".staging",
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
                    staging_dir=tmp_path / ".staging",
                    push=True,
                    definition_dir=INGEST_DEF_DIR,
                )
