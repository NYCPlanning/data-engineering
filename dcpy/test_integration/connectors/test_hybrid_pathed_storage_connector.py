from pathlib import Path

from dcpy.connectors.hybrid_pathed_storage import (
    StorageType,
    PathedStorageConnector,
)


def _test_paths(conn: PathedStorageConnector, local_dir: Path):
    """Reusable test flow for any PathedStorageConnector (Azure, S3, Local"""

    # Create a test file to push
    local_file = local_dir / "test.txt"
    local_file.write_text("hello world")

    file_name = "test.txt"
    remote_path = Path("remote") / file_name

    # Push the file
    conn.push(key=str(remote_path), filepath=local_file)
    assert conn.exists(key=str(remote_path))

    # Pull the file back to a new location
    pulled_file = local_dir / "pulled.txt"
    conn.pull(key=str(remote_path), destination_path=pulled_file)
    assert pulled_file.read_text() == "hello world"

    # Create a subfolder and file
    local_subfolder = local_dir / "sub"
    local_subfolder.mkdir()
    (local_subfolder / "file2.txt").write_text("another file")

    conn.push("remote/sub", filepath=str(local_subfolder))
    assert conn.exists(key=str("remote/sub/file2.txt"))

    conn.pull("remote/sub", destination_path=local_dir / "sub2")
    assert (local_dir / "sub2").exists()
    assert (local_dir / "sub2" / "file2.txt").exists()

    # Get subfolders
    subfolders = conn.get_subfolders("remote")
    assert "sub" in subfolders


def test_local_paths(tmp_path):
    conn = PathedStorageConnector.from_storage_kwargs(
        conn_type="local_test",
        storage_backend=StorageType.LOCAL,
        local_dir=tmp_path,
    )
    _test_paths(conn, tmp_path)


def test_az_paths(tmp_path, azure_storage_connector: PathedStorageConnector):
    _test_paths(azure_storage_connector, tmp_path)


def test_s3_paths(tmp_path, s3_storage_connector: PathedStorageConnector):
    _test_paths(s3_storage_connector, tmp_path)


def test_az_metadata(tmp_path, azure_storage_connector: PathedStorageConnector):
    filename = "test.txt"
    (tmp_path / filename).write_text("hello world")

    azure_storage_connector.push(
        key="no_metadata.txt", filepath=str(tmp_path / filename)
    )
    assert not azure_storage_connector.get_metadata("no_metadata.txt"), (
        "no custom metdata was pushed, so none should be retrieved"
    )

    metadata = {"my_key": "my_value"}
    azure_storage_connector.push(
        key="has_metadata.txt", filepath=str(tmp_path / filename), metadata=metadata
    )
    assert metadata == azure_storage_connector.get_metadata("has_metadata.txt")


def test_s3_metadata(tmp_path, s3_storage_connector: PathedStorageConnector):
    filename = "test.txt"
    (tmp_path / filename).write_text("hello world")

    s3_storage_connector.push(key="no_metadata.txt", filepath=str(tmp_path / filename))
    assert not s3_storage_connector.get_metadata("no_metadata.txt"), (
        "no custom metdata was pushed, so none should be retrieved"
    )

    metadata = {
        "my-key": "my_value"
    }  # Note the "-" vs "_". S3 metadata keys are converted from "_" to "-"
    s3_storage_connector.push(
        key="has_metadata.txt", filepath=str(tmp_path / filename), metadata=metadata
    )
    assert metadata == s3_storage_connector.get_metadata("has_metadata.txt")
