import time
import uuid
from pathlib import Path

import pytest

from dcpy.connectors.edm.drafts import DraftsConnector
from dcpy.connectors.hybrid_pathed_storage import (
    PathedStorageConnector,
    StorageType,
)


def calculate_test_path():
    return (
        Path("integration_tests")
        / "edm"
        / "connectors"
        / "drafts"
        / f"{int(time.time())}_{str(uuid.uuid4())[:8]}"
    )


def _make_fake_draft_datasets():
    """Create test product, version, and revision combinations."""
    products = ["housing_db", "pluto_changes"]
    versions = ["2024", "2025"]
    revisions = ["1", "2", "3"]
    combos = []
    for product in products:
        for version in versions:
            for revision in revisions:
                combos.append((product, version, revision))
    return combos


def _create_draft_files(conn: DraftsConnector, tmp_path: Path):
    """Create fake draft files in the storage."""
    # Create temporary files locally then push them to storage
    local_staging = tmp_path / "staging"
    local_staging.mkdir(parents=True, exist_ok=True)

    for product, version, revision in _make_fake_draft_datasets():
        # Create some mock draft files
        files_to_create = ["draft_data.csv", "draft_metadata.json", "notes.txt"]

        for filename in files_to_create:
            # Create local file
            local_file = local_staging / f"{product}_{version}_{revision}_{filename}"
            if filename == "draft_data.csv":
                local_file.write_text(
                    f"id,name,status\\n1,{product},draft\\n2,{product},reviewing"
                )
            elif filename == "draft_metadata.json":
                local_file.write_text(
                    f'{{"product": "{product}", "version": "{version}", "revision": "{revision}", "status": "draft"}}'
                )
            else:
                local_file.write_text(
                    f"Draft notes for {product} version {version} revision {revision}"
                )

            # Push to storage using the connector's interface
            # For drafts, the structure is: {product}/draft/{version}/{revision}/{filename}
            storage_key = f"{product}/draft/{version}/{revision}/{filename}"
            conn.storage.push(key=storage_key, filepath=str(local_file))


@pytest.fixture
def local_drafts_conn(tmp_path):
    """Create a Drafts connector with local storage for testing."""
    # Create the root directory first
    root_dir = tmp_path / "drafts"
    root_dir.mkdir(parents=True, exist_ok=True)

    # Create connector with local storage instead of S3
    storage = PathedStorageConnector.from_storage_kwargs(
        conn_type="edm.publishing.drafts.test",
        storage_backend=StorageType.LOCAL,
        local_dir=root_dir,
    )
    conn = DraftsConnector(storage=storage)
    _create_draft_files(conn, tmp_path)
    return conn


def _test_drafts_connector_interface(conn: DraftsConnector, tmp_path):
    """Test the basic drafts connector interface methods."""
    expected_drafts = _make_fake_draft_datasets()
    # e.g. [('housing_db', '2024', '1'), ('housing_db', '2024', '2'), ...]

    expected_products = {product for product, _, _ in expected_drafts}
    expected_versions_by_product: dict[str, list[str]] = {}
    for product, version, revision in expected_drafts:
        if product not in expected_versions_by_product:
            expected_versions_by_product[product] = []
        # For drafts, we expect version.revision format
        version_revision = f"{version}.{revision}"
        expected_versions_by_product[product].append(version_revision)

    # Check that all versions exist
    for product, version, revision in expected_drafts:
        version_revision = f"{version}.{revision}"
        assert conn.version_exists(product, version=version_revision), (
            f"Version {version_revision} should exist for product {product}"
        )

    # Test listing versions (should return version.revision format)
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
    for product, version, revision in expected_drafts:
        destination_dir = tmp_path / "downloads" / product / version / revision
        destination_dir.mkdir(parents=True, exist_ok=True)

        version_revision = f"{version}.{revision}"

        # Pull a specific file from the draft
        result = conn.pull_versioned(
            key=product,
            version=version_revision,
            destination_path=destination_dir,
            source_path="draft_data.csv",
        )

        pulled_path = result.get("path")
        assert pulled_path is not None, (
            f"Should return a path for {product}:{version_revision}"
        )
        assert Path(pulled_path).exists(), (
            f"Downloaded file should exist: {pulled_path}"
        )
        assert Path(pulled_path).name == "draft_data.csv", (
            "Downloaded file should be named draft_data.csv"
        )

        # Verify content
        content = Path(pulled_path).read_text()
        assert product in content, (
            f"Downloaded content should contain product name {product}"
        )


def _test_drafts_connector_error_cases(conn: DraftsConnector, tmp_path):
    """Test error cases and edge conditions."""
    # Test non-existent product
    assert conn.list_versions("nonexistent_product") == [], (
        "Non-existent product should return empty list"
    )

    # Test non-existent version for existing product
    existing_product = "housing_db"
    assert not conn.version_exists(existing_product, "2099.99"), (
        "Non-existent version should return False"
    )

    # Test version parsing - invalid format should raise ValueError
    with pytest.raises(
        ValueError, match="Version .* should be in format 'version.revision'"
    ):
        conn._parse_version("invalid_format")

    # Test pulling non-existent file
    destination_dir = tmp_path / "downloads" / "test_error"
    destination_dir.mkdir(parents=True, exist_ok=True)

    with pytest.raises(FileNotFoundError, match="File .* not found"):
        conn.pull_versioned(
            key="housing_db",
            version="2024.1",
            destination_path=destination_dir,
            source_path="nonexistent_file.txt",
        )

    # Test get_latest_version on empty product list
    with pytest.raises(IndexError):
        conn.get_latest_version("nonexistent_product")


def _test_drafts_connector_version_parsing(conn: DraftsConnector):
    """Test version parsing functionality."""
    # Test valid version.revision format
    version, revision = conn._parse_version("2024.1")
    assert version == "2024"
    assert revision == "1"

    # Test complex version.revision format
    version, revision = conn._parse_version("2024-beta.5")
    assert version == "2024-beta"
    assert revision == "5"

    # Test version with multiple dots (should split on last dot)
    version, revision = conn._parse_version("v1.2.3.final")
    assert version == "v1.2.3"
    assert revision == "final"


def test_local_drafts_conn(local_drafts_conn: DraftsConnector, tmp_path):
    """Test the Drafts connector with local storage."""
    _test_drafts_connector_interface(local_drafts_conn, tmp_path)
    _test_drafts_connector_error_cases(local_drafts_conn, tmp_path)
    _test_drafts_connector_version_parsing(local_drafts_conn)
