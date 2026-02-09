import json
from unittest import mock

import geopandas as gpd
import pytest

from dcpy.connectors.ingest_datastore import Connector as IngestDatastoreConnector
from dcpy.lifecycle.ingest import connectors, run
from dcpy.models.lifecycle.ingest import ArchivedDataSource, IngestedDataset
from dcpy.utils import metadata

from .shared import FAKE_VERSION, INGEST_DEF_DIR, RESOLVED, TEST_DATASET_NAME

DATASOURCE = "one_to_many"
DATASET = TEST_DATASET_NAME


def raw_storage():
    return connectors.get_raw_datastore_connector().storage.storage.root_path


def storage():
    return connectors.get_processed_datastore_connector().storage.storage.root_path


@pytest.fixture
def run_extract_and_archive_raw_dataset(tmp_path):
    return run.extract_and_archive_raw_dataset(
        resolved_definition=RESOLVED,
        staging_dir=tmp_path / ".staging",
        push=True,
        setup_staging_dir=True,
    )


@pytest.fixture
def run_process_datasets(tmp_path, run_extract_and_archive_raw_dataset):
    return run.process_datasets(staging_dir=tmp_path / ".staging")


@pytest.fixture
def run_ingest(tmp_path):
    return run.ingest(
        dataset_id=DATASOURCE,
        version=FAKE_VERSION,
        push=True,
        staging_dir=tmp_path / ".staging",
        definition_dir=INGEST_DEF_DIR,
    )


@pytest.mark.usefixtures("tmp_path")
class TestExtractAndArchiveRawDataset:
    kwargs = {
        "resolved_definition": RESOLVED,
        "push": True,
        "setup_staging_dir": True,
    }

    def test_run(self, tmp_path):
        """Mainly an integration test to make sure code runs without error"""
        run.extract_and_archive_raw_dataset(
            staging_dir=tmp_path / ".staging", **self.kwargs
        )
        assert True

    def test_run_raw_output_exists(self, tmp_path):
        """Makes sure raw file is properly archived"""
        details = run.extract_and_archive_raw_dataset(
            staging_dir=tmp_path / ".staging", **self.kwargs
        )
        path = raw_storage() / details.raw_dataset_path
        assert path.exists()

    def test_config_generated(self, tmp_path):
        """
        Feels like a bit of a tautology, but makes sure config file is generated, saved and equals the returned obj
        """
        staging_dir = tmp_path / ".staging"
        datasource = run.extract_and_archive_raw_dataset(
            staging_dir=staging_dir, **self.kwargs
        )
        assert (staging_dir / "datasource.json").exists()
        with open(staging_dir / "datasource.json", "r") as f:
            dumped_datasource = ArchivedDataSource(**json.loads(f.read()))
            assert dumped_datasource == datasource


@pytest.mark.usefixtures("run_extract_and_archive_raw_dataset", "tmp_path")
class TestProcessDatasets:
    def test_runs(self, tmp_path, run_extract_and_archive_raw_dataset):
        run.process_datasets(
            datasource=run_extract_and_archive_raw_dataset,
            staging_dir=tmp_path / ".staging",
        )
        assert True

    def test_runs_from_staging_dir(self, tmp_path):
        run.process_datasets(staging_dir=tmp_path / ".staging")
        assert True

    def test_resolves_version_from_run_details(self, tmp_path):
        run_details = metadata.get_run_details()
        datasets = run.process_datasets(
            staging_dir=tmp_path / ".staging", run_details=run_details
        )
        assert datasets[0].version == run_details.timestamp.strftime("%Y%m%d")

    def test_outputs(self, tmp_path, run_extract_and_archive_raw_dataset):
        staging_dir = tmp_path / ".staging"
        datasets = run.process_datasets(staging_dir=staging_dir)
        assert len(datasets) == 2
        assert {ds.id for ds in datasets} == {
            ds.id for ds in run_extract_and_archive_raw_dataset.datasets
        }


@pytest.mark.usefixtures("run_process_datasets", "tmp_path")
class TestArchiveTransformedDatasets:
    def test_runs(self, tmp_path, run_process_datasets):
        run.archive_transformed_datasets(
            datasets=run_process_datasets,
            staging_dir=tmp_path / ".staging",
            latest=True,
        )
        assert True

    def test_runs_from_staging_dir(self, tmp_path):
        run.archive_transformed_datasets(staging_dir=tmp_path / ".staging", latest=True)
        assert True

    def test_outputs_in_datastore(self, tmp_path, run_process_datasets):
        datastore = connectors.get_processed_datastore_connector()
        for dataset in run_process_datasets:
            assert not datastore.version_exists(dataset.id, dataset.version), (
                "Error in test setup - version should not exist yet"
            )
        run.archive_transformed_datasets(staging_dir=tmp_path / ".staging", latest=True)
        for dataset in run_process_datasets:
            assert datastore.version_exists(dataset.id, dataset.version)
            assert datastore.get_latest_version(dataset.id) == dataset.version

    def test_repeat_version(self, tmp_path, run_process_datasets):
        run.archive_transformed_datasets(staging_dir=tmp_path / ".staging", latest=True)

        with mock.patch(
            "dcpy.lifecycle.ingest.run.get_processed_datastore_connector"
        ) as patch_connector:
            connector = mock.MagicMock()
            patch_connector.return_value = connector
            run.archive_transformed_datasets(
                staging_dir=tmp_path / ".staging", latest=True
            )
            print(connector.mock_calls)
            assert connector.version_exists.call_count == 2  # two downstream datasets
            assert not connector.push.called

    def test_repeat_version_fails_if_data_diff(self, tmp_path, run_process_datasets):
        def _patch_pull(self, key, version, path):
            output_path = path / f"{key}.parquet"
            gdf = gpd.GeoDataFrame({"a": [None]}).set_geometry("a")
            gdf.to_parquet(output_path)
            return {"path": output_path}

        run.archive_transformed_datasets(staging_dir=tmp_path / ".staging", latest=True)

        # patch the pull_versioned method to dump a different parquet file so that
        # when the archive function tries to pull the existing version, it will get different data
        with mock.patch.object(IngestDatastoreConnector, "pull_versioned", _patch_pull):
            with pytest.raises(
                FileExistsError,
                match="already exists and has different data.",
            ):
                run.archive_transformed_datasets(
                    staging_dir=tmp_path / ".staging", latest=True
                )


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
        run_ingest = run.ingest(
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


@pytest.mark.usefixtures("run_extract_and_archive_raw_dataset", "tmp_path")
class TestProcessArchivedDatasource:
    def test_standard(self, tmp_path, run_extract_and_archive_raw_dataset):
        staging_dir = tmp_path / ".staging"
        # make sure staging dir is empty after fixture is run
        run._setup_staging_dir(staging_dir)
        datasets = run.process_archived_datasource(
            ds_id=DATASOURCE, staging_dir=staging_dir, latest=True, push=True
        )
        datasource = ArchivedDataSource.from_path(
            staging_dir / run.DATASOURCE_DETAILS_FILENAME
        )
        # assert details match what was produced in fixture
        assert datasource == run_extract_and_archive_raw_dataset
        # assert raw file pulled
        assert (staging_dir / run_extract_and_archive_raw_dataset.raw_filename).exists()
        assert len(datasets) == 2
        assert (storage() / datasets[0].dataset_path).exists()
        assert (storage() / datasets[1].dataset_path).exists()

    def test_dataset_filter(self, tmp_path, run_extract_and_archive_raw_dataset):
        staging_dir = tmp_path / ".staging"
        defined_datasets = run_extract_and_archive_raw_dataset.datasets
        ds1_id = defined_datasets[0].id
        processed_datasets = run.process_archived_datasource(
            ds_id=DATASOURCE,
            staging_dir=staging_dir,
            latest=True,
            datasets_filter=ds1_id,
        )
        assert len(defined_datasets) == 2
        assert len(processed_datasets) == 1
        assert processed_datasets[0].id == ds1_id

    def test_invalid_filter(self, tmp_path, run_extract_and_archive_raw_dataset):
        staging_dir = tmp_path / ".staging"
        with pytest.raises(
            ValueError,
            match="Declared dataset\\(s\\) \\{'invalid_dataset'\\} not found in datasource details.",
        ):
            run.process_archived_datasource(
                ds_id=DATASOURCE,
                staging_dir=staging_dir,
                latest=True,
                datasets_filter=["invalid_dataset"],
            )
