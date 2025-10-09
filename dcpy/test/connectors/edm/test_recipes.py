from datetime import datetime
import json
import pandas as pd
from pathlib import Path
import pytest
import yaml

from dcpy.models import library
from dcpy.models.connectors.edm.recipes import DatasetType
from dcpy.models.lifecycle import ingest
from dcpy.utils import s3
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
        config = library.Config(**yml)
    assert config.execution_details


def test_get_library_config(load_library: library.Config):
    config = recipes.get_config(TEST_DATASET, LIBRARY_VERSION)
    assert config == load_library


def test_get_ingest_config(load_ingest: ingest.Config):
    config = recipes.get_config(TEST_DATASET, load_ingest.version)
    assert config == load_ingest


def test_get_all_versions(load_library, load_ingest):
    assert set(recipes.get_all_versions(TEST_DATASET)) == {
        LIBRARY_VERSION,
        INGEST_VERSION,
    }


def test_invalid_pd_reader():
    with pytest.raises(Exception, match="Cannot read pandas dataframe"):
        recipes._pd_reader(DatasetType.pg_dump)


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
