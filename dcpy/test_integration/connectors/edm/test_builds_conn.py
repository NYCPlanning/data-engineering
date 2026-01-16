import time
import uuid
from pathlib import Path

import pytest

from dcpy.connectors.edm.builds import BuildsConnector
from dcpy.connectors.hybrid_pathed_storage import (
    PathedStorageConnector,
    StorageType,
)


def calculate_test_path():
    return (
        Path("integration_tests")
        / "edm"
        / "connectors"
        / "builds"
        / f"{int(time.time())}_{str(uuid.uuid4())[:8]}"
    )


def _make_fake_builds():
    """Create test product and build combinations."""
    products = ["product1", "product2"]
    builds = ["ar-distribution", "dm-dbt", "fvk-cscl"]
    combos = []
    for product in products:
        for build in builds:
            combos.append((product, build))
    return combos


def _create_build_files(conn: BuildsConnector, tmp_path: Path):
    """Create fake build files in the storage."""
    # Create temporary files locally then push them to storage
    local_staging = tmp_path / "staging"
    local_staging.mkdir(parents=True, exist_ok=True)

    for product, build in _make_fake_builds():
        # Create local build folder with some files
        build_folder = local_staging / f"{product}_{build}"
        build_folder.mkdir(parents=True, exist_ok=True)

        # Create some mock build artifacts
        (build_folder / "build.zip").write_text(
            f"Mock build data for {product} build {build}"
        )
        (build_folder / "metadata.json").write_text(
            f'{{"product": "{product}", "build": "{build}"}}'
        )

        # Push to storage using the connector's interface
        # For builds, the structure is: {product}/build/{build}/
        storage_key = f"{product}/build/{build}"
        conn.storage.push(key=storage_key, filepath=str(build_folder))


@pytest.fixture
def local_builds_conn(tmp_path):
    """Create a Builds connector with local storage for testing."""
    # Create the root directory first
    root_dir = tmp_path / "builds"
    root_dir.mkdir(parents=True, exist_ok=True)

    # Create connector with local storage instead of S3
    storage = PathedStorageConnector.from_storage_kwargs(
        conn_type="edm.publishing.builds.test",
        storage_backend=StorageType.LOCAL,
        local_dir=root_dir,
    )
    conn = BuildsConnector(storage=storage)
    _create_build_files(conn, tmp_path)
    return conn


def _test_builds_connector_interface(conn: BuildsConnector, tmp_path):
    """Test the basic builds connector interface methods."""
    expected_builds = _make_fake_builds()
    # e.g. [('product1', 'ar-distribution'), ('product1', 'dm-dbt'), ...]

    expected_products = {product for product, _ in expected_builds}
    expected_builds_by_product: dict[str, list[str]] = {}
    for product, build in expected_builds:
        if product not in expected_builds_by_product:
            expected_builds_by_product[product] = []
        expected_builds_by_product[product].append(build)

    # Check that all builds exist
    for product, build in expected_builds:
        assert conn.version_exists(product, version=build), (
            f"Build {build} should exist for product {product}"
        )

    # Test listing versions (builds)
    for product in expected_products:
        builds = conn.list_versions(product)
        expected_builds_list = sorted(expected_builds_by_product[product], reverse=True)
        assert builds == expected_builds_list, (
            f"Builds for {product} should match expected: {builds} vs {expected_builds_list}"
        )

    # Test pull_versioned
    for product, build in expected_builds:
        destination_dir = tmp_path / "downloads" / product / build
        destination_dir.mkdir(parents=True, exist_ok=True)

        # Pull a specific file from the build
        result = conn.pull_versioned(
            key=product,
            version=build,
            destination_path=destination_dir,
            filepath="build.zip",
        )

        pulled_path = result.get("path")
        assert pulled_path is not None, f"Should return a path for {product}:{build}"
        assert Path(pulled_path).exists(), (
            f"Downloaded file should exist: {pulled_path}"
        )
        assert Path(pulled_path).name == "build.zip", (
            "Downloaded file should be named build.zip"
        )


def _test_builds_connector_error_cases(conn: BuildsConnector, tmp_path):
    """Test error cases and edge conditions."""
    # Test non-existent product
    assert conn.list_versions("nonexistent_product") == [], (
        "Non-existent product should return empty list"
    )

    # Test non-existent build for existing product
    existing_product = "product1"
    assert not conn.version_exists(existing_product, "nonexistent-build"), (
        "Non-existent build should return False"
    )

    # Test get_latest_version should raise NotImplementedError
    with pytest.raises(
        NotImplementedError, match="Builds don't have a meaningful 'latest' version"
    ):
        conn.get_latest_version(existing_product)

    # Test pulling non-existent file
    destination_dir = tmp_path / "downloads" / "test_error"
    destination_dir.mkdir(parents=True, exist_ok=True)

    with pytest.raises(FileNotFoundError, match="File .* not found"):
        conn.pull_versioned(
            key="product1",
            version="ar-distribution",
            destination_path=destination_dir,
            filepath="nonexistent_file.txt",
        )


def test_local_builds_conn(local_builds_conn: BuildsConnector, tmp_path):
    """Test the Builds connector with local storage."""
    _test_builds_connector_interface(local_builds_conn, tmp_path)
    _test_builds_connector_error_cases(local_builds_conn, tmp_path)
