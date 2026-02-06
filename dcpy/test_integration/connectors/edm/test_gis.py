import time
import uuid
from pathlib import Path

import pytest

from dcpy.connectors.edm.gis import GisDatasetsConnector
from dcpy.connectors.hybrid_pathed_storage import (
    PathedStorageConnector,
    StorageType,
)


def calculate_test_path():
    return (
        Path("integration_tests")
        / "edm"
        / "connectors"
        / "gis"
        / f"{int(time.time())}_{str(uuid.uuid4())[:8]}"
    )


def _make_fake_gis_datasets():
    """Create test dataset combinations."""
    datasets = ["dcp_mandatory_inclusionary_housing", "dof_dtm_condos"]
    versions = ["20241201", "20241215"]  # Date format versions
    combos = []
    for ds in datasets:
        for v in versions:
            combos.append((ds, v))
    return combos


def _create_gis_files(conn: GisDatasetsConnector, tmp_path: Path):
    """Create fake GIS dataset files in the storage."""
    # Create temporary files locally then push them to storage
    local_staging = tmp_path / "staging"
    local_staging.mkdir(parents=True, exist_ok=True)

    for dataset, version in _make_fake_gis_datasets():
        # Create local zip file
        local_zip = local_staging / f"{dataset}.zip"
        local_zip.write_text(f"Mock GIS data for {dataset} version {version}")

        # Push to storage using the connector's interface
        storage_key = f"{dataset}/{version}/{dataset}.zip"
        conn.storage.push(key=storage_key, filepath=str(local_zip))


@pytest.fixture
def local_gis_conn(tmp_path):
    """Create a GIS connector with local storage for testing."""
    # Create the datasets directory first
    datasets_dir = tmp_path / "datasets"
    datasets_dir.mkdir(parents=True, exist_ok=True)

    # Create connector with local storage instead of S3
    storage = PathedStorageConnector.from_storage_kwargs(
        conn_type="edm.publishing.gis.test",
        storage_backend=StorageType.LOCAL,
        local_dir=datasets_dir,  # Note: GIS uses 'datasets' folder
    )
    conn = GisDatasetsConnector(storage=storage)
    _create_gis_files(conn, tmp_path)
    return conn


def _test_gis_connector_interface(conn: GisDatasetsConnector, tmp_path):
    """Test the basic GIS connector interface methods."""
    expected_datasets_versions = _make_fake_gis_datasets()
    # e.g. [('dcp_mandatory_inclusionary_housing', '20241201'), ...]

    expected_unique_datasets = {ds for ds, _ in expected_datasets_versions}
    expected_versions_by_dataset: dict[str, list[str]] = {}
    for ds, v in expected_datasets_versions:
        if ds not in expected_versions_by_dataset:
            expected_versions_by_dataset[ds] = []
        expected_versions_by_dataset[ds].append(v)

    # Check that all versions exist
    for ds, v in expected_datasets_versions:
        assert conn.version_exists(ds, version=v), (
            f"Version {v} should exist for dataset {ds}"
        )

    # Test listing versions
    for dataset in expected_unique_datasets:
        versions = conn.list_versions(dataset)
        expected_versions = sorted(expected_versions_by_dataset[dataset], reverse=True)
        assert versions == expected_versions, (
            f"Versions for {dataset} should match expected"
        )

    # Test get_latest_version
    for dataset in expected_unique_datasets:
        latest = conn.get_latest_version(dataset)
        expected_latest = max(expected_versions_by_dataset[dataset])
        assert latest == expected_latest, (
            f"Latest version for {dataset} should be {expected_latest}"
        )

    # Test pull_versioned
    for dataset, version in expected_datasets_versions:
        destination_dir = tmp_path / "downloads" / dataset / version
        destination_dir.mkdir(parents=True, exist_ok=True)

        result = conn.pull_versioned(
            key=dataset, version=version, destination_path=destination_dir
        )

        pulled_path = result.get("path")
        assert pulled_path is not None, f"Should return a path for {dataset}:{version}"
        assert Path(pulled_path).exists(), (
            f"Downloaded file should exist: {pulled_path}"
        )
        assert Path(pulled_path).name == f"{dataset}.zip", (
            f"Downloaded file should be named {dataset}.zip"
        )


def _test_gis_connector_push_restriction(conn: GisDatasetsConnector, tmp_path):
    """Test that pushing is restricted (only GIS team can push)."""
    test_file = tmp_path / "test.zip"
    test_file.touch()

    with pytest.raises(PermissionError, match="Currently, only GIS team pushes"):
        conn.push_versioned(key="test_dataset", version="20241201", filepath=test_file)


def _test_gis_connector_error_cases(conn: GisDatasetsConnector, tmp_path):
    """Test error cases and edge conditions."""
    # Test non-existent dataset - version_exists also throws due to the assertion
    with pytest.raises(
        AssertionError, match="No Dataset named nonexistent_dataset found"
    ):
        conn.version_exists("nonexistent_dataset", "20241201")

    with pytest.raises(
        AssertionError, match="No Dataset named nonexistent_dataset found"
    ):
        conn.list_versions("nonexistent_dataset")

    with pytest.raises(
        AssertionError, match="No Dataset named nonexistent_dataset found"
    ):
        conn.get_latest_version("nonexistent_dataset")

    # Test non-existent version for existing dataset
    existing_dataset = "dcp_mandatory_inclusionary_housing"
    assert not conn.version_exists(
        existing_dataset, "20990101"
    )  # Future date that doesn't exist


def test_local_gis_conn(local_gis_conn: GisDatasetsConnector, tmp_path):
    """Test the GIS connector with local storage."""
    _test_gis_connector_interface(local_gis_conn, tmp_path)
    _test_gis_connector_push_restriction(local_gis_conn, tmp_path)
    _test_gis_connector_error_cases(local_gis_conn, tmp_path)
