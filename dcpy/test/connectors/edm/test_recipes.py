from datetime import datetime
from pathlib import Path
import pytest
from unittest import mock
import yaml

from dcpy.utils import s3, metadata
from dcpy.models.connectors.edm.recipes import DatasetType
from dcpy.connectors.edm import recipes
from dcpy.models import library, file
from dcpy.models.lifecycle import ingest
from dcpy.library import archive
from dcpy.lifecycle.ingest import run, configure
from dcpy.lifecycle.ingest.configure import read_template
from dcpy.test.conftest import RECIPES_BUCKET

TEST_DATASET = "test"
TEST_METADATA = library.ArchivalMetadata(
    name=TEST_DATASET, version="parquet", timestamp=datetime.now()
)
INGEST_TEMPLATE = "ingest.yml"
INGEST_VERSION = "ingest_version"
PARQUET_FILEPATH = (
    f"{recipes.DATASET_FOLDER}/{TEST_DATASET}/parquet/{TEST_DATASET}.parquet"
)

RESOURCE_DIR = Path(__file__).parent / "resources"


def library_archive(output_format: str, version: str) -> library.Config:
    a = archive.Archive()
    return a(
        path=str(RESOURCE_DIR / "library.yml"),
        output_format=output_format,
        version=version,
        push=True,
        clean=True,
        latest=True,
    )


@pytest.fixture(scope="function")
def load_library_parquet(create_buckets):
    yield library_archive(output_format="parquet", version="parquet")


@pytest.fixture(scope="function")
def load_library_pgdump(create_buckets):
    yield library_archive(output_format="pgdump", version="pg_dump")


@pytest.fixture(scope="function")
def load_library_all(create_buckets):
    version = "all"
    for output_format in ["parquet", "pgdump", "csv"]:
        library_config = library_archive(output_format=output_format, version=version)
    yield library_config


def _mock_template(name, version, path=None):
    configure.read_template(name, version, RESOURCE_DIR)


@pytest.fixture(scope="function")
def load_ingest(create_buckets):
    with mock.patch.object(read_template, "__defaults__", (None, None, RESOURCE_DIR)):
        config = run.run(TEST_DATASET, version=INGEST_VERSION)
        yield config


def test_dataset_type_from_extension():
    assert recipes._dataset_type_from_extension("sql") == DatasetType.pg_dump
    assert recipes._dataset_type_from_extension("csv") == DatasetType.csv
    assert recipes._dataset_type_from_extension("parquet") == DatasetType.parquet
    assert recipes._dataset_type_from_extension("xlsx") == DatasetType.xlsx
    assert recipes._dataset_type_from_extension("other") == None


class TestArchiveDataset:
    dataset = "bpl_libraries"  # doesn't actually get queried, just to fill out config
    raw_file_name = "tmp.txt"
    config = ingest.Config(
        name=dataset,
        version="dummy",
        archival_timestamp=datetime.now(),
        acl="private",
        raw_filename=raw_file_name,
        source=ingest.ScriptSource(
            type="script", connector="dummy", function="dummy"
        ),  # easiest to mock
        file_format=file.Csv(type="csv"),  # easiest to mock
        run_details=metadata.get_run_details(),
    )

    def test_archive_raw_dataset(self, create_buckets, create_temp_filesystem: Path):
        tmp_file = create_temp_filesystem / self.raw_file_name
        tmp_file.touch()
        recipes.archive_raw_dataset(self.config, tmp_file)
        assert s3.exists(
            RECIPES_BUCKET,
            str(self.config.raw_s3_key(recipes.RAW_FOLDER)),
        )

    def test_archive_dataset(self, create_buckets, create_temp_filesystem: Path):
        tmp_parquet = create_temp_filesystem / self.config.filename
        tmp_parquet.touch()
        recipes.archive_dataset(self.config, tmp_parquet)
        assert s3.exists(
            RECIPES_BUCKET,
            str(
                self.config.s3_file_key(recipes.DATASET_FOLDER)
            ),  ## TODO don't want to point the code at our "production" folder at the moment
        )

    def test_archive_dataset_latest(self, create_buckets, create_temp_filesystem: Path):
        tmp_parquet = create_temp_filesystem / self.config.filename
        tmp_parquet.touch()
        recipes.archive_dataset(self.config, tmp_parquet, latest=True)
        assert s3.exists(
            RECIPES_BUCKET,
            f"{recipes.DATASET_FOLDER}/{self.dataset}/latest/{self.config.filename}",
        )


def test_get_preferred_file_type(
    load_library_parquet: library.Config,
    load_library_pgdump: library.Config,
    load_library_all: library.Config,
):
    expected_file_types = {
        load_library_parquet.version: recipes.DatasetType.parquet,
        load_library_pgdump.version: recipes.DatasetType.pg_dump,
        load_library_all.version: recipes.DatasetType.parquet,
    }
    for version in expected_file_types:
        dataset = recipes.Dataset(name=TEST_DATASET, version=version)
        file_type = recipes.get_preferred_file_type(
            dataset,
            [
                recipes.DatasetType.parquet,
                recipes.DatasetType.pg_dump,
                recipes.DatasetType.csv,
            ],
        )
        assert file_type == expected_file_types[version]


def test_parse_config():
    # for backwards compatibility
    with open(RESOURCE_DIR / "config.json", "r", encoding="utf-8") as f:
        yml = yaml.safe_load(f)
        config = library.Config(**yml)
    assert config.execution_details


def test_get_library_config(load_library_parquet: library.Config):
    config = recipes.get_config(TEST_DATASET, load_library_parquet.version)
    assert config == load_library_parquet


def test_get_ingest_config(load_ingest: library.Config):
    config = recipes.get_config(TEST_DATASET, load_ingest.version)
    assert config == load_ingest


def test_get_latest_version(load_library_parquet: library.Config):
    assert recipes.get_latest_version(TEST_DATASET) == load_library_parquet.version


def test_get_all_versions(load_library_parquet: library.Config):
    assert recipes.get_all_versions(TEST_DATASET) == [load_library_parquet.version]


def test_fetch_dataset(
    load_library_parquet: library.Config, create_temp_filesystem: Path
):
    ds = load_library_parquet.sparse_dataset
    ds.file_type = DatasetType.parquet
    recipes.fetch_dataset(ds, create_temp_filesystem)
    assert (create_temp_filesystem / PARQUET_FILEPATH).exists()


def test_fetch_dataset_cache(
    load_library_parquet: library.Config, create_temp_filesystem: Path
):
    ds = load_library_parquet.sparse_dataset
    ds.file_type = DatasetType.parquet
    recipes.fetch_dataset(ds, create_temp_filesystem)

    with mock.patch("dcpy.utils.s3.download_file") as download_file:
        recipes.fetch_dataset(ds, create_temp_filesystem)
        assert (
            download_file.call_count == 0
        ), "file did not cache properly, fetch_dataset attempted s3 call"

        recipes.fetch_dataset(ds, create_temp_filesystem / "dummy_folder")
        assert (
            download_file.call_count == 1
        ), "fetch_dataset was refactored, testing of cache functionality no longer valid"


def test_invalid_pd_reader():
    with pytest.raises(Exception, match="Cannot read pandas dataframe"):
        recipes._pd_reader(DatasetType.pg_dump)


def test_get_dataset_metadata(load_library_parquet: library.Config):
    assert load_library_parquet.execution_details
    metadata = recipes.get_archival_metadata(TEST_DATASET)
    assert metadata.name == TEST_DATASET
    assert metadata.version == load_library_parquet.version
    assert metadata.timestamp == load_library_parquet.execution_details.timestamp


def test_read_df(load_library_all, load_library_pgdump):
    preferred_file_types = [recipes.DatasetType.parquet, recipes.DatasetType.csv]
    dataset = load_library_all.sparse_dataset
    dataset.file_type = None
    file_type = recipes.get_preferred_file_type(dataset, preferred_file_types)
    assert file_type == recipes.DatasetType.parquet
    parquet = recipes.read_df(dataset, preferred_file_types=preferred_file_types)
    dataset.file_type = recipes.DatasetType.csv
    csv = recipes.read_df(dataset, dtype="object")
    assert csv.equals(parquet)

    with pytest.raises(FileNotFoundError):
        recipes.read_df(load_library_pgdump.sparse_dataset)


def test_read_df_cache(load_library_all, create_temp_filesystem: Path):
    preferred_file_types = [recipes.DatasetType.parquet, recipes.DatasetType.csv]
    dataset = load_library_all.sparse_dataset
    dataset.file_type = recipes.get_preferred_file_type(dataset, preferred_file_types)
    _df = recipes.read_df(
        dataset,
        preferred_file_types=preferred_file_types,
        local_cache_dir=create_temp_filesystem,
    )
    assert (create_temp_filesystem / PARQUET_FILEPATH).exists()


def test_log_metadata(dev_flag):
    with mock.patch("dcpy.utils.postgres.PostgresClient") as pg_client:
        recipes.log_metadata(None)
        # when dev_flag present, returns without invoking anything
        pg_client.assert_not_called()
