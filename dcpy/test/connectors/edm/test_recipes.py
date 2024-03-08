from datetime import datetime
from pathlib import Path
import pytest
import yaml

from dcpy.utils import s3
from dcpy.connectors.edm import recipes
from dcpy.library.archive import Archive

TEST_LIBRARY_DATASET = "test"
TEST_METADATA = recipes.ArchivalMetadata(
    name=TEST_LIBRARY_DATASET, version="parquet", timestamp=datetime.now()
)

RESOURCE_DIR = Path(__file__).parent / "resources"


def library_archive(output_format: str, version: str) -> recipes.Config:
    a = Archive()
    return a(
        path=str(RESOURCE_DIR / "test.yml"),
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


def test_archive_raw_dataset(create_buckets, create_temp_filesystem: Path):
    dataset = "bpl_libraries"  # doesn't actually get queried, just to fill out config
    file_name = "tmp.txt"
    tmp_file = create_temp_filesystem / file_name
    tmp_file.touch()
    config = recipes.ExtractConfig(
        name=dataset,
        version="dummy",
        archival_timestamp=datetime.now(),
        acl="private",
        raw_filename=file_name,
        source=recipes.ExtractConfig.Source.Script(type="script"),  # easiest to mock
    )
    recipes.archive_raw_dataset(config, tmp_file)
    assert s3.exists(
        recipes.BUCKET,
        config.raw_dataset_s3_filepath,
    )


def test_get_preferred_file_type(
    load_library_parquet: recipes.Config,
    load_library_pgdump: recipes.Config,
    load_library_all: recipes.Config,
):
    expected_file_types = {
        load_library_parquet.version: recipes.DatasetType.parquet,
        load_library_pgdump.version: recipes.DatasetType.pg_dump,
        load_library_all.version: recipes.DatasetType.parquet,
    }
    for version in expected_file_types:
        dataset = recipes.Dataset(name=TEST_LIBRARY_DATASET, version=version)
        file_type = dataset.get_preferred_file_type(
            [
                recipes.DatasetType.parquet,
                recipes.DatasetType.pg_dump,
                recipes.DatasetType.csv,
            ]
        )
        assert file_type == expected_file_types[version]


def test_parse_config():
    # for backwards compatibility
    with open(RESOURCE_DIR / "config.json", "r", encoding="utf-8") as f:
        yml = yaml.safe_load(f)
        config = recipes.Config(**yml)
    assert config.execution_details


def test_get_config(load_library_parquet: recipes.Config):
    config = recipes.get_config(TEST_LIBRARY_DATASET, load_library_parquet.version)
    assert config == load_library_parquet


def test_get_latest_version(load_library_parquet: recipes.Config):
    assert (
        recipes.get_latest_version(TEST_LIBRARY_DATASET) == load_library_parquet.version
    )


def test_get_dataset_metadata(load_library_parquet: recipes.Config):
    assert load_library_parquet.execution_details
    metadata = recipes.get_archival_metadata(TEST_LIBRARY_DATASET)
    assert metadata.name == TEST_LIBRARY_DATASET
    assert metadata.version == load_library_parquet.version
    assert metadata.timestamp == load_library_parquet.execution_details.timestamp


def test_read_df(load_library_all, load_library_pgdump):
    preferred_file_types = [recipes.DatasetType.parquet, recipes.DatasetType.csv]
    dataset = load_library_all.sparse_dataset
    dataset.file_type = None
    file_type = dataset.get_preferred_file_type(preferred_file_types)
    assert file_type == recipes.DatasetType.parquet
    parquet = recipes.read_df(dataset, preferred_file_types=preferred_file_types)
    dataset.file_type = recipes.DatasetType.csv
    csv = recipes.read_df(dataset, dtype="object")
    assert csv.equals(parquet)

    with pytest.raises(FileNotFoundError):
        recipes.read_df(load_library_pgdump.sparse_dataset)
