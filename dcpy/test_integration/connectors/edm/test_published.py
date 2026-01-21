import time
import uuid
from pathlib import Path

import pytest

from dcpy.connectors.edm.published import PublishedConnector
from dcpy.connectors.hybrid_pathed_storage import (
    PathedStorageConnector,
    StorageType,
)


def calculate_test_path():
    return (
        Path("integration_tests")
        / "edm"
        / "connectors"
        / "published"
        / f"{int(time.time())}_{str(uuid.uuid4())[:8]}"
    )


def _make_fake_published_datasets():
    """Create test product and version combinations."""
    products = ["nycha_developments", "pluto"]
    versions = ["24v1", "24v2", "25v1"]  # Changed to match expected version format
    combos = []
    for product in products:
        for version in versions:
            combos.append((product, version))
    return combos


def _create_published_files(conn: PublishedConnector, tmp_path: Path):
    """Create fake published dataset files in the storage."""
    # Create temporary files locally then push them to storage
    local_staging = tmp_path / "staging"
    local_staging.mkdir(parents=True, exist_ok=True)

    for product, version in _make_fake_published_datasets():
        # Create some mock published files
        files_to_create = ["data.csv", "metadata.json", "readme.txt"]

        for filename in files_to_create:
            # Create local file
            local_file = local_staging / f"{product}_{version}_{filename}"
            if filename == "data.csv":
                local_file.write_text(
                    f"id,name,value\\n1,{product},test\\n2,{product},data"
                )
            elif filename == "metadata.json":
                local_file.write_text(
                    f'{{"product": "{product}", "version": "{version}", "created": "2024-01-01"}}'
                )
            else:
                local_file.write_text(f"README for {product} version {version}")

            # Push to storage using the connector's interface
            # For published, the structure is: {product}/publish/{version}/{filename}
            storage_key = f"{product}/publish/{version}/{filename}"
            conn.storage.push(key=storage_key, filepath=str(local_file))


@pytest.fixture
def local_published_conn(tmp_path):
    """Create a Published connector with local storage for testing."""
    # Create the root directory first
    root_dir = tmp_path / "published"
    root_dir.mkdir(parents=True, exist_ok=True)

    # Create connector with local storage instead of S3
    storage = PathedStorageConnector.from_storage_kwargs(
        conn_type="edm.publishing.published.test",
        storage_backend=StorageType.LOCAL,
        local_dir=root_dir,
    )
    conn = PublishedConnector(storage=storage)
    _create_published_files(conn, tmp_path)
    return conn


def _test_published_connector_interface(conn: PublishedConnector, tmp_path):
    """Test the basic published connector interface methods."""
    expected_published = _make_fake_published_datasets()
    # e.g. [('nycha_developments', '2024v1'), ('pluto', '2024v1'), ...]

    expected_products = {product for product, _ in expected_published}
    expected_versions_by_product: dict[str, list[str]] = {}
    for product, version in expected_published:
        if product not in expected_versions_by_product:
            expected_versions_by_product[product] = []
        expected_versions_by_product[product].append(version)

    # Check that all versions exist
    for product, version in expected_published:
        assert conn.version_exists(product, version=version), (
            f"Version {version} should exist for product {product}"
        )

    # Test listing versions
    for product in expected_products:
        versions = conn.list_versions(product)
        expected_versions_list = sorted(
            expected_versions_by_product[product], reverse=True
        )
        assert versions == expected_versions_list, (
            f"Versions for {product} should match expected: {versions} vs {expected_versions_list}"
        )

    # Test get_latest_version
    for product in expected_products:
        latest = conn.get_latest_version(product)
        expected_latest = max(expected_versions_by_product[product])
        assert latest == expected_latest, (
            f"Latest version for {product} should be {expected_latest}"
        )

    # Test pull_versioned
    for product, version in expected_published:
        destination_dir = tmp_path / "downloads" / product / version
        destination_dir.mkdir(parents=True, exist_ok=True)

        # Pull a specific file from the published dataset
        result = conn.pull_versioned(
            key=product,
            version=version,
            destination_path=destination_dir,
            filepath="data.csv",
        )

        pulled_path = result.get("path")
        assert pulled_path is not None, f"Should return a path for {product}:{version}"
        assert Path(pulled_path).exists(), (
            f"Downloaded file should exist: {pulled_path}"
        )
        assert Path(pulled_path).name == "data.csv", (
            "Downloaded file should be named data.csv"
        )

        # Verify content
        content = Path(pulled_path).read_text()
        assert product in content, (
            f"Downloaded content should contain product name {product}"
        )


def _test_published_connector_error_cases(conn: PublishedConnector, tmp_path):
    """Test error cases and edge conditions."""
    # Test non-existent product
    assert conn.list_versions("nonexistent_product") == [], (
        "Non-existent product should return empty list"
    )

    # Test non-existent version for existing product
    existing_product = "nycha_developments"
    assert not conn.version_exists(existing_product, "nonexistent_version"), (
        "Non-existent version should return False"
    )

    # Test pulling non-existent file
    destination_dir = tmp_path / "downloads" / "test_error"
    destination_dir.mkdir(parents=True, exist_ok=True)

    with pytest.raises(FileNotFoundError, match="File .* not found"):
        conn.pull_versioned(
            key="nycha_developments",
            version="2024v1",
            destination_path=destination_dir,
            filepath="nonexistent_file.txt",
        )

    # Test get_latest_version on empty product list
    with pytest.raises(IndexError):
        conn.get_latest_version("nonexistent_product")

    # Test push_versioned functionality
    test_dir = tmp_path / "test_version_dir"
    test_dir.mkdir(exist_ok=True)
    test_file = test_dir / "test_file.txt"
    test_file.write_text("test content")

    # Push a directory to a new version
    result = conn.push_versioned(
        "test_product", "test_version", source_path=str(test_dir)
    )

    # Verify the push was successful
    assert result is not None, "push_versioned should return a result"

    # Verify the version now exists
    assert conn.version_exists("test_product", "test_version"), (
        "Version should exist after pushing"
    )

    # Verify the pushed content can be retrieved
    pull_destination = tmp_path / "pulled_test"
    pull_destination.mkdir(exist_ok=True)

    pull_result = conn.pull_versioned(
        key="test_product",
        version="test_version",
        destination_path=pull_destination,
        filepath="test_file.txt",
    )

    pulled_path = pull_result.get("path")
    assert pulled_path is not None, "Should return a path for pulled file"
    assert Path(pulled_path).exists(), "Pulled file should exist"

    # Verify content matches
    pulled_content = Path(pulled_path).read_text()
    assert pulled_content == "test content", "Pulled content should match original"

    # Verify the pushed content can be retrieved
    pull_destination = tmp_path / "pulled_test"
    pull_destination.mkdir(exist_ok=True)

    pull_result = conn.pull_versioned(
        key="test_product",
        version="test_version",
        destination_path=pull_destination,
        filepath="test_file.txt",
    )

    pulled_path = pull_result.get("path")
    assert pulled_path is not None, "Should return a path for pulled file"
    assert Path(pulled_path).exists(), "Pulled file should exist"

    # Verify content matches
    pulled_content = Path(pulled_path).read_text()
    assert pulled_content == "test content", "Pulled content should match original"


def test_local_published_conn(local_published_conn: PublishedConnector, tmp_path):
    """Test the Published connector with local storage."""
    _test_published_connector_interface(local_published_conn, tmp_path)
    _test_published_connector_error_cases(local_published_conn, tmp_path)
