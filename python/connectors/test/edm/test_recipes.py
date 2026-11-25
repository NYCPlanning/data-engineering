import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
import yaml

from dcpy.connectors.edm import recipes
from dcpy.connectors.edm.models import Dataset, DatasetKey, DatasetType, SparseConfig
from dcpy.utils import s3

from ..conftest import RECIPES_BUCKET

TEST_DATASET = "test"
LIBRARY_VERSION = "library"
INGEST_VERSION = "ingest"
PARQUET_FILEPATH = (
    f"{recipes.DATASET_FOLDER}/{TEST_DATASET}/{INGEST_VERSION}/{TEST_DATASET}.parquet"
)

RESOURCE_DIR = Path(__file__).parent / "resources"


def _load_folder(mode, dataset_folder="datasets"):
    version_folder = f"{dataset_folder}/{TEST_DATASET}/{mode}/"
    s3.upload_folder(
        RECIPES_BUCKET,
        RESOURCE_DIR / mode,
        Path(version_folder),
        "private",
        contents_only=True,
    )
    s3.copy_folder(
        RECIPES_BUCKET,
        version_folder,
        f"{dataset_folder}/{TEST_DATASET}/latest",
        "private",
    )
    with open(RESOURCE_DIR / mode / "config.json") as f:
        return json.load(f)


@pytest.fixture(scope="function")
def load_library(create_buckets):
    """loads pg_dump and csv"""
    config_json = _load_folder("library")
    yield SparseConfig(**config_json)


@pytest.fixture(scope="function")
def load_ingest(create_buckets):
    config_json = _load_folder("ingest")
    yield SparseConfig(**config_json)


def test_dataset_type_from_extension():
    assert recipes.DatasetType.from_extension("sql") == DatasetType.pg_dump
    assert recipes.DatasetType.from_extension("csv") == DatasetType.csv
    assert recipes.DatasetType.from_extension("parquet") == DatasetType.parquet
    assert recipes.DatasetType.from_extension("xlsx") == DatasetType.xlsx
    assert recipes.DatasetType.from_extension("other") is None


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
        config = SparseConfig(**yml)
    assert (config.id, config.version) == (TEST_DATASET, LIBRARY_VERSION)


def test_get_all_versions(load_library, load_ingest):
    assert set(recipes.get_all_versions(TEST_DATASET)) == {
        LIBRARY_VERSION,
        INGEST_VERSION,
    }


def test_invalid_pd_reader():
    with pytest.raises(Exception, match="Cannot read pandas dataframe"):
        recipes._pd_reader(DatasetType.pg_dump)


def test_read_df(load_ingest: SparseConfig):
    preferred_file_types = [recipes.DatasetType.parquet, recipes.DatasetType.csv]
    dataset = Dataset(id=load_ingest.id, version=load_ingest.version)
    dataset.file_type = None
    file_type = recipes.get_preferred_file_type(dataset, preferred_file_types)
    assert file_type == recipes.DatasetType.parquet
    recipe_df = recipes.read_df(dataset, preferred_file_types=preferred_file_types)
    input_df = pd.read_parquet(RESOURCE_DIR / "ingest" / "test.parquet")
    assert recipe_df.equals(input_df)


def test_read_df_library_csv(load_library: SparseConfig):
    preferred_file_types = [recipes.DatasetType.parquet, recipes.DatasetType.csv]
    dataset = Dataset(id=load_library.id, version=load_library.version)
    file_type = recipes.get_preferred_file_type(dataset, preferred_file_types)
    assert file_type == recipes.DatasetType.csv
    recipe_df = recipes.read_df(dataset)
    input_df = pd.read_csv(RESOURCE_DIR / "library" / "test.csv")
    assert recipe_df.equals(input_df)


def test_read_df_missing_filetype(load_library: SparseConfig):
    with pytest.raises(FileNotFoundError):
        recipes.read_df(
            Dataset(id=load_library.id, version=load_library.version),
            preferred_file_types=[recipes.DatasetType.parquet],
        )


def test_get_archive_date(load_ingest: SparseConfig):
    ds = DatasetKey(id=load_ingest.id, version=load_ingest.version)
    result = recipes.get_archive_date(ds)
    assert isinstance(result, datetime)


def test_get_archive_date_prefers_ingest_run_timestamp(load_ingest: SparseConfig):
    ds = DatasetKey(id=load_ingest.id, version=load_ingest.version)
    assert recipes.get_archive_date(ds) == datetime.fromisoformat(
        "2025-09-26T12:38:22.544455-04:00"
    )


def test_get_archive_date_library_execution_details(load_library: SparseConfig):
    ds = DatasetKey(id=load_library.id, version=load_library.version)
    assert recipes.get_archive_date(ds) == datetime.fromisoformat(
        "2024-09-05T11:36:31.636395-04:00"
    )


def _put(folder: str, name: str, body: bytes, metadata: dict | None = None):
    s3.client().put_object(
        Bucket=RECIPES_BUCKET,
        Key=f"{folder}/{name}",
        Body=body,
        Metadata=metadata or {},
    )


def test_get_archive_date_config_without_timestamp_uses_date_created(
    create_buckets,
):
    """A library-era config.json without execution_details: date-created metadata."""
    folder = "datasets/old/v1"
    _put(folder, "old.sql", b"", {"date-created": "2020-01-02T03:04:05+00:00"})
    _put(
        folder,
        "config.json",
        json.dumps({"dataset": {"name": "old", "version": "v1"}}).encode(),
        {"date-created": "2020-01-02T03:04:05+00:00"},
    )
    ds = DatasetKey(id="old", version="v1")
    assert recipes.get_archive_date(ds) == datetime.fromisoformat(
        "2020-01-02T03:04:05+00:00"
    )


def test_get_archive_date_no_config_uses_last_modified(create_buckets):
    folder = "datasets/bare/v1"
    _put(folder, "bare.sql", b"")
    ds = DatasetKey(id="bare", version="v1")
    archived = recipes.get_archive_date(ds)
    assert (
        archived == s3.get_metadata(RECIPES_BUCKET, f"{folder}/bare.sql").last_modified
    )


def test_get_archive_date_missing_dataset(create_buckets):
    ds = DatasetKey(id="nonexistent", version="v1")
    result = recipes.get_archive_date(ds)
    assert result is None


def test_get_url(load_ingest: SparseConfig):
    ds = DatasetKey(id=load_ingest.id, version=load_ingest.version)
    result = recipes.get_url(ds)
    assert result == "https://example.com/data"


def test_get_url_missing_dataset(create_buckets):
    ds = DatasetKey(id="nonexistent", version="v1")
    result = recipes.get_url(ds)
    assert result is None


def test_read_df_cache(load_ingest: SparseConfig, create_temp_filesystem: Path):
    preferred_file_types = [recipes.DatasetType.parquet, recipes.DatasetType.csv]
    dataset = Dataset(id=load_ingest.id, version=load_ingest.version)
    dataset.file_type = recipes.get_preferred_file_type(dataset, preferred_file_types)
    _df = recipes.read_df(
        dataset,
        preferred_file_types=preferred_file_types,
        local_cache_dir=create_temp_filesystem,
    )
    print(create_temp_filesystem / PARQUET_FILEPATH)
    assert (create_temp_filesystem / PARQUET_FILEPATH).exists()


@pytest.fixture
def no_root_folder(monkeypatch):
    monkeypatch.delenv("RECIPES_BUCKET_ROOT_FOLDER", raising=False)


def test_default_keys_are_at_bucket_root(no_root_folder):
    ds = DatasetKey(id="x", version="v1")
    assert recipes.s3_folder_path(ds) == "datasets/x/v1"


@pytest.mark.parametrize("root", ["ingest", "/ingest/", "ingest/"])
def test_root_folder_prefixes_keys(monkeypatch, root):
    monkeypatch.setenv("RECIPES_BUCKET_ROOT_FOLDER", root)
    ds = Dataset(id="x", version="v1", file_type=DatasetType.parquet)
    assert recipes.s3_folder_path(ds) == "ingest/datasets/x/v1"
    assert recipes.s3_file_path(ds) == "ingest/datasets/x/v1/x.parquet"


def test_lookups_under_root_folder(create_buckets, monkeypatch):
    """Archives under ingest/datasets/ are found; ones at the root are not."""
    _load_folder("library")  # at datasets/, should be invisible
    _load_folder(INGEST_VERSION, dataset_folder="ingest/datasets")
    monkeypatch.setenv("RECIPES_BUCKET_ROOT_FOLDER", "ingest")

    assert set(recipes.get_all_versions(TEST_DATASET)) == {INGEST_VERSION}
    ds = Dataset(id=TEST_DATASET, version=INGEST_VERSION)
    assert recipes.get_file_types(ds) == {DatasetType.parquet}
    assert recipes.get_archive_date(ds) is not None
    assert recipes.get_url(ds) == "https://example.com/data"
    assert recipes.get_file_types(Dataset(id=TEST_DATASET, version="library")) == set()


def test_local_cache_layout_ignores_root_folder(
    load_ingest: SparseConfig, create_temp_filesystem: Path, monkeypatch
):
    monkeypatch.setenv("RECIPES_BUCKET_ROOT_FOLDER", "ingest")
    _load_folder(INGEST_VERSION, dataset_folder="ingest/datasets")
    ds = Dataset(
        id=load_ingest.id, version=load_ingest.version, file_type=DatasetType.parquet
    )
    recipes.read_df(ds, local_cache_dir=create_temp_filesystem)
    assert (create_temp_filesystem / PARQUET_FILEPATH).exists()
