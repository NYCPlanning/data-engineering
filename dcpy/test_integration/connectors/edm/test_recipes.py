import pytest
from pathlib import Path
import time
import uuid

from dcpy.connectors.ingest_datastore import Connector
from dcpy.connectors.hybrid_pathed_storage import (
    PathedStorageConnector,
    StorageType,
)
from dcpy.models.connectors.edm.recipes import DatasetType


def calculate_test_path():
    return (
        Path("integration_tests")
        / "edm"
        / "connectors"
        / "recipes"
        / f"{int(time.time())}_{str(uuid.uuid4())[:8]}"
    )


def _make_fake_datasets():
    datasets = ["dob_cofos", "dcp_pluto"]
    versions = ["v1", "v2"]
    file_types = [DatasetType.csv, DatasetType.parquet, DatasetType.pg_dump]
    combos = []
    for ds in datasets:
        for v in versions:
            for ft in file_types:
                combos.append((ds, v, ft))
    return combos


def _create_files(r: Connector):
    base_dir = r.storage.storage.root_path
    base_dir.mkdir(parents=True, exist_ok=True)
    for ds, v, ft in _make_fake_datasets():
        file_dir = base_dir / ds / v
        file_dir.mkdir(parents=True, exist_ok=True)
        (file_dir / f"{ds}.{ft.to_extension()}").touch()
    return base_dir


@pytest.fixture
def az_recipes_repo(azure_storage_connector):
    repo = Connector(storage=azure_storage_connector)
    try:
        _create_files(repo)
        yield repo
    finally:
        repo.storage.storage.root_path.rmtree()


@pytest.fixture
def s3_recipes_repo(s3_storage_connector):
    repo = Connector(storage=s3_storage_connector)
    try:
        _create_files(repo)
        yield repo
    finally:
        repo.storage.storage.root_path.rmtree()


@pytest.fixture
def local_recipes_repo(tmp_path):
    repo = Connector(
        storage=PathedStorageConnector.from_storage_kwargs(
            conn_type="local_test",
            storage_backend=StorageType.LOCAL,
            local_dir=tmp_path,
        )
    )
    _create_files(repo)
    return repo


def _test_connector_interface(conn: Connector, tmp_path):
    expected_dataset_files = _make_fake_datasets()
    # e.g. [('dob_cofos', 'v1', 'parquet),...]

    expected_unique_datasets = {ds for ds, _, _ in expected_dataset_files}
    expected_unique_ds_versions = {(ds, v) for ds, v, _ in expected_dataset_files}

    # Check that all versions exist
    for ds, v in expected_unique_ds_versions:
        assert conn.version_exists(ds, version=v)

    # Test listing versions
    found_versions: set[tuple[str, str]] = set()
    for d in expected_unique_datasets:
        versions = conn.list_versions(d)
        found_versions = found_versions | {(d, v) for v in versions}
    assert found_versions == expected_unique_ds_versions, (
        "All the dataset versions should be listed."
    )

    found_paths = set()
    expected_paths = set()
    for ds, v, ft in expected_dataset_files:
        expected_path = tmp_path / "other_folder" / ds / v / f"{ds}.{ft}"
        expected_paths.add(expected_path)

        actual_path = conn.pull_versioned(
            ds, version=v, file_type=ft, destination_path=expected_path
        ).get("path")
        found_paths.add(actual_path)
    assert expected_paths == found_paths, "The pulled paths should be correct"


def test_azure_repo(az_recipes_repo: Connector, tmp_path):
    _test_connector_interface(az_recipes_repo, tmp_path)


def test_s3_repo(s3_recipes_repo: Connector, tmp_path):
    _test_connector_interface(s3_recipes_repo, tmp_path)


def test_local_repo(local_recipes_repo: Connector, tmp_path):
    _test_connector_interface(local_recipes_repo, tmp_path)
