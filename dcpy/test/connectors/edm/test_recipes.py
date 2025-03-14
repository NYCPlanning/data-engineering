from datetime import datetime
import json
import pandas as pd
from pathlib import Path
import pytest
from typing import Literal
from unittest import mock
import yaml

from dcpy.models import library, file
from dcpy.models.connectors.edm.recipes import DatasetType
from dcpy.models.lifecycle import ingest
from dcpy.utils import s3, metadata
from dcpy.connectors.edm import recipes

from dcpy.test.conftest import RECIPES_BUCKET

TEST_DATASET = "test"
LIBRARY_VERSION = "library"
INGEST_VERSION = "ingest"
TEST_METADATA = library.ArchivalMetadata(
    name=TEST_DATASET, version="parquet", timestamp=datetime.now()
)
INGEST_TEMPLATE = "ingest.yml"
PARQUET_FILEPATH = (
    f"{recipes.DATASET_FOLDER}/{TEST_DATASET}/{INGEST_VERSION}/{TEST_DATASET}.parquet"
)

RESOURCE_DIR = Path(__file__).parent / "resources"


def _load_folder(mode):
    version_folder = f"datasets/{TEST_DATASET}/{mode}/"
    s3.upload_folder(
        RECIPES_BUCKET,
        RESOURCE_DIR / mode,
        Path(version_folder),
        "private",
        contents_only=True,
    )
    s3.copy_folder(
        RECIPES_BUCKET, version_folder, f"datasets/{TEST_DATASET}/latest", "private"
    )
    with open(RESOURCE_DIR / mode / "config.json") as f:
        return json.load(f)


@pytest.fixture(scope="function")
def load_library(create_buckets):
    """loads pg_dump and csv"""
    config_json = _load_folder("library")
    yield library.Config(**config_json)


@pytest.fixture(scope="function")
def load_ingest(create_buckets):
    config_json = _load_folder("ingest")
    yield ingest.Config(**config_json)


def test_dataset_type_from_extension():
    assert recipes._dataset_type_from_extension("sql") == DatasetType.pg_dump
    assert recipes._dataset_type_from_extension("csv") == DatasetType.csv
    assert recipes._dataset_type_from_extension("parquet") == DatasetType.parquet
    assert recipes._dataset_type_from_extension("xlsx") == DatasetType.xlsx
    assert recipes._dataset_type_from_extension("other") is None


class TestArchiveDataset:
    dataset = "bpl_libraries"  # doesn't actually get queried, just to fill out config
    raw_file_name = "tmp.txt"
    acl: Literal["private"] = "private"
    config = ingest.Config(
        id=dataset,
        version="dummy",
        attributes=ingest.DatasetAttributes(name=dataset),
        archival=ingest.ArchivalMetadata(
            archival_timestamp=datetime.now(),
            acl="private",
            raw_filename=raw_file_name,
        ),
        ingestion=ingest.Ingestion(
            source=ingest.ScriptSource(
                type="script", connector="dummy", function="dummy"
            ),  # easiest to mock
            file_format=file.Csv(type="csv"),  # easiest to mock
        ),
        run_details=metadata.get_run_details(),
    )

    def test_archive_raw_dataset(self, create_buckets, create_temp_filesystem: Path):
        tmp_file = create_temp_filesystem / self.raw_file_name
        tmp_file.touch()
        recipes.archive_dataset(self.config, tmp_file, acl=self.acl, raw=True)
        assert s3.folder_exists(
            RECIPES_BUCKET, recipes.s3_raw_folder_path(self.config.raw_dataset_key)
        )

    def test_archive_dataset(self, create_buckets, create_temp_filesystem: Path):
        tmp_parquet = create_temp_filesystem / self.config.filename
        tmp_parquet.touch()
        recipes.archive_dataset(self.config, tmp_parquet, acl=self.acl)
        assert s3.object_exists(
            RECIPES_BUCKET, recipes.s3_file_path(self.config.dataset)
        )

    def test_archive_dataset_latest(self, create_buckets, create_temp_filesystem: Path):
        tmp_parquet = create_temp_filesystem / self.config.filename
        tmp_parquet.touch()
        recipes.archive_dataset(self.config, tmp_parquet, acl=self.acl, latest=True)
        assert s3.object_exists(
            RECIPES_BUCKET,
            f"{recipes.DATASET_FOLDER}/{self.dataset}/latest/{self.config.filename}",
        )

    def test_fails_when_exists(self, create_buckets):
        folder_path = recipes.s3_folder_path(self.config.dataset)
        s3.client().put_object(Bucket=RECIPES_BUCKET, Key=folder_path + "/config.json")
        with pytest.raises(
            Exception,
            match=f"Archived dataset at {folder_path} already exists, cannot overwrite",
        ):
            recipes.archive_dataset(self.config, Path("."), acl=self.acl)


def test_get_preferred_file_type(load_library):
    dataset = recipes.Dataset(id=TEST_DATASET, version=LIBRARY_VERSION)
    file_type = recipes.get_preferred_file_type(
        dataset,
        [
            recipes.DatasetType.parquet,
            recipes.DatasetType.pg_dump,
            recipes.DatasetType.csv,
        ],
    )
    assert file_type == recipes.DatasetType.pg_dump


def test_parse_library_config():
    # for backwards compatibility
    with open(RESOURCE_DIR / "library" / "config.json", "r", encoding="utf-8") as f:
        yml = yaml.safe_load(f)
        config = library.Config(**yml)
    assert config.execution_details


def test_get_library_config(load_library: library.Config):
    config = recipes.get_config(TEST_DATASET, LIBRARY_VERSION)
    assert config == load_library


def test_get_ingest_config(load_ingest: ingest.Config):
    config = recipes.get_config(TEST_DATASET, load_ingest.version)
    assert config == load_ingest


def test_get_latest_version(load_ingest):
    assert recipes.get_latest_version(TEST_DATASET) == INGEST_VERSION


def test_get_all_versions(load_library, load_ingest):
    assert set(recipes.get_all_versions(TEST_DATASET)) == {
        LIBRARY_VERSION,
        INGEST_VERSION,
    }


def test_fetch_dataset(load_ingest: ingest.Config, create_temp_filesystem: Path):
    ds = load_ingest.dataset
    ds.file_type = DatasetType.parquet
    folder_path = create_temp_filesystem / recipes.DATASET_FOLDER / ds.id / ds.version
    folder_path.mkdir(parents=True)  # mainly for coverage
    path = recipes.fetch_dataset(ds, target_dir=create_temp_filesystem)
    print(path)
    assert path.exists()


def test_fetch_dataset_cache(load_ingest: ingest.Config, create_temp_filesystem: Path):
    ds = load_ingest.dataset
    ds.file_type = DatasetType.parquet
    recipes.fetch_dataset(ds, target_dir=create_temp_filesystem)

    with mock.patch("dcpy.utils.s3.download_file") as download_file:
        recipes.fetch_dataset(ds, target_dir=create_temp_filesystem)
        assert download_file.call_count == 0, (
            "file did not cache properly, fetch_dataset attempted s3 call"
        )

        recipes.fetch_dataset(ds, target_dir=create_temp_filesystem / "dummy_folder")
        assert download_file.call_count == 1, (
            "fetch_dataset was refactored, testing of cache functionality no longer valid"
        )


def test_invalid_pd_reader():
    with pytest.raises(Exception, match="Cannot read pandas dataframe"):
        recipes._pd_reader(DatasetType.pg_dump)


def test_get_dataset_metadata(load_ingest: ingest.Config):
    assert load_ingest.run_details
    metadata = recipes.get_archival_metadata(TEST_DATASET)
    assert metadata.name == TEST_DATASET
    assert metadata.version == INGEST_VERSION
    assert metadata.timestamp == load_ingest.run_details.timestamp


def test_read_df(load_ingest: ingest.Config):
    preferred_file_types = [recipes.DatasetType.parquet, recipes.DatasetType.csv]
    dataset = load_ingest.dataset
    dataset.file_type = None
    file_type = recipes.get_preferred_file_type(dataset, preferred_file_types)
    assert file_type == recipes.DatasetType.parquet
    recipe_df = recipes.read_df(dataset, preferred_file_types=preferred_file_types)
    input_df = pd.read_parquet(RESOURCE_DIR / "ingest" / "test.parquet")
    assert recipe_df.equals(input_df)


def test_read_df_library_csv(load_library: library.Config):
    preferred_file_types = [recipes.DatasetType.parquet, recipes.DatasetType.csv]
    dataset = load_library.sparse_dataset
    file_type = recipes.get_preferred_file_type(dataset, preferred_file_types)
    assert file_type == recipes.DatasetType.csv
    recipe_df = recipes.read_df(dataset)
    input_df = pd.read_csv(RESOURCE_DIR / "library" / "test.csv")
    assert recipe_df.equals(input_df)


def test_read_df_missing_filetype(load_library: library.Config):
    with pytest.raises(FileNotFoundError):
        recipes.read_df(
            load_library.sparse_dataset,
            preferred_file_types=[recipes.DatasetType.parquet],
        )


def test_read_df_cache(load_ingest: ingest.Config, create_temp_filesystem: Path):
    preferred_file_types = [recipes.DatasetType.parquet, recipes.DatasetType.csv]
    dataset = load_ingest.dataset
    dataset.file_type = recipes.get_preferred_file_type(dataset, preferred_file_types)
    _df = recipes.read_df(
        dataset,
        preferred_file_types=preferred_file_types,
        local_cache_dir=create_temp_filesystem,
    )
    print(create_temp_filesystem / PARQUET_FILEPATH)
    assert (create_temp_filesystem / PARQUET_FILEPATH).exists()


def test_update_freshness(load_ingest: ingest.Config):
    def get_config():
        config = recipes.get_config(load_ingest.id, load_ingest.version)
        assert isinstance(config, ingest.Config)
        return config

    config = get_config()
    assert config.archival.check_timestamps == []
    assert config.freshness == config.archival.archival_timestamp

    timestamp = datetime.now()
    recipes.update_freshness(load_ingest.dataset_key, timestamp)
    config2 = get_config()
    assert config2.archival.check_timestamps == [timestamp]
    assert config2.freshness == timestamp

    timestamp2 = datetime.now()
    recipes.update_freshness(load_ingest.dataset_key, timestamp2)
    config3 = get_config()
    assert config3.archival.check_timestamps == [timestamp, timestamp2]
    assert config3.freshness == timestamp2


def test_update_freshness_library_fails(load_library: library.Config):
    with pytest.raises(
        TypeError,
        match=f"Cannot update freshness of dataset {load_library.dataset_key} as it was archived by library, not ingest",
    ):
        recipes.update_freshness(load_library.dataset_key, datetime.now())


def test_log_metadata(dev_flag):
    with mock.patch("dcpy.utils.postgres.PostgresClient") as pg_client:
        recipes.log_metadata(None)
        # when dev_flag present, returns without invoking anything
        pg_client.assert_not_called()
