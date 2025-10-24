import json
import pytest
from pathlib import Path
import requests
import time
import uuid

from dcpy.connectors.ingest_datastore import Connector
from dcpy.connectors.hybrid_pathed_storage import (
    PathedStorageConnector,
    StorageType,
)
from dcpy.models.connectors.edm.recipes import DatasetType
from dcpy.models.lifecycle import ingest

RESOURCE_DIR = Path(__file__).parent / "resources"


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
def az_recipes_conn(azure_storage_connector):
    conn = Connector(storage=azure_storage_connector)
    try:
        _create_files(conn)
        yield conn
    finally:
        conn.storage.storage.root_path.rmtree()


@pytest.fixture
def s3_recipes_conn(s3_storage_connector):
    conn = Connector(storage=s3_storage_connector)
    try:
        _create_files(conn)
        yield conn
    finally:
        conn.storage.storage.root_path.rmtree()


@pytest.fixture
def local_recipes_conn(tmp_path):
    conn = Connector(
        storage=PathedStorageConnector.from_storage_kwargs(
            conn_type="local_test",
            storage_backend=StorageType.LOCAL,
            local_dir=tmp_path,
        )
    )
    _create_files(conn)
    return conn


# @pytest.fixture
def recipes_config():
    with open(RESOURCE_DIR / "config.json") as f:
        return ingest.IngestedDataset(**json.load(f))


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


def _test_connector_interface_pushing_private_files(conn: Connector, tmp_path):
    # Test Pushing (private ACL)
    config = recipes_config()
    file_path = tmp_path / f"{config.id}.parquet"
    file_path.touch()
    conn.push(
        key=config.id,
        version=config.version,
        filepath=file_path,
        config=config,
        overwrite=False,
        latest=True,
    )
    assert conn.version_exists(key=config.id, version=config.version), (
        "The pushed version should exist"
    )
    assert conn.version_exists(key=config.id, version="latest"), (
        "The latest version should exist"
    )
    pulled_config = conn.get_sparse_config(key=config.id, version=config.version)
    assert config.model_dump(
        mode="json", exclude_none=True
    ) == pulled_config.model_dump(mode="json", exclude_none=True), (
        "The pushed config should match the pulled config"
    )

    # Test Overwrite Failing
    with pytest.raises(Exception):
        conn.push(
            key=config.id,
            version=config.version,
            filepath=file_path,
            config=config,
            overwrite=False,
            latest=True,
        )
    # Test Overwrite Succeeding
    conn.push(
        key=config.id,
        version=config.version,
        filepath=file_path,
        config=config,
        overwrite=True,
        latest=True,
    )


def _test_connector_interface_pushing_public_files_success(conn: Connector, tmp_path):
    public_config = recipes_config()
    public_config.id = "test_public_push_success"
    public_config.acl = "public-read"
    public_config.version = "testing_acls"
    file_path = tmp_path / f"{public_config.id}.csv"
    file_path.touch()

    conn.push(
        key=public_config.id,
        version=public_config.version,
        acl="public-read",
        filepath=file_path,
        config=public_config,
        overwrite=False,
        latest=True,
    )
    assert conn.version_exists(key=public_config.id, version=public_config.version), (
        "The pushed version should exist"
    )
    assert conn.version_exists(key=public_config.id, version="latest"), (
        "The latest version should exist"
    )
    # Check that the file is actually public
    public_url = (
        conn.storage.storage.root_path
        / public_config.id
        / public_config.version
        / f"{public_config.id}.csv"
    ).as_url()
    r = requests.get(public_url)
    assert r.status_code == 200, f"The public URL should be accessible: {public_url}"

    latest_public_url = (
        conn.storage.storage.root_path
        / public_config.id
        / "latest"
        / f"{public_config.id}.csv"
    ).as_url()
    r = requests.get(latest_public_url)
    assert r.status_code == 200, (
        f"The latest public URL should be accessible: {latest_public_url}"
    )


def _test_connector_interface_pushing_public_files_failure(conn: Connector, tmp_path):
    public_config = recipes_config()
    public_config.id = "test_public_push_failure"
    public_config.acl = "public-read"
    public_config.version = "testing_acls"
    file_path = tmp_path / f"{public_config.id}.csv"
    file_path.touch()

    with pytest.raises(Exception) as exc_info:
        conn.push(
            key=public_config.id,
            version=public_config.version,
            filepath=file_path,
            config=public_config,
            overwrite=True,
            latest=True,
        )
        assert exc_info.value is not None, (
            "Expected an exception when pushing with public ACL to non-S3 storage."
        )


def test_azure_recipes_conn(az_recipes_conn: Connector, tmp_path):
    _test_connector_interface(az_recipes_conn, tmp_path)
    _test_connector_interface_pushing_private_files(az_recipes_conn, tmp_path)

    # Public ACLs should NOT work
    _test_connector_interface_pushing_public_files_failure(az_recipes_conn, tmp_path)


def test_s3_recipes_conn(s3_recipes_conn: Connector, tmp_path):
    _test_connector_interface(s3_recipes_conn, tmp_path)
    _test_connector_interface_pushing_private_files(s3_recipes_conn, tmp_path)

    # Public ACLs SHOULD work
    _test_connector_interface_pushing_public_files_success(s3_recipes_conn, tmp_path)


def test_local_recipes_conn(local_recipes_conn: Connector, tmp_path):
    _test_connector_interface(local_recipes_conn, tmp_path)
    _test_connector_interface_pushing_private_files(local_recipes_conn, tmp_path)

    # Public ACLs should NOT work
    _test_connector_interface_pushing_public_files_failure(local_recipes_conn, tmp_path)
