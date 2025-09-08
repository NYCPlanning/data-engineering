from pathlib import Path

from dcpy.connectors.hybrid_pathed_storage import (
    StorageType,
    PathedStorageConnector,
)


def test_local_paths(tmp_path):
    # storage_backend = HybridPathedStorage.Factory.local(Path(tmp_dir))
    local_dir = Path(tmp_path)
    conn = PathedStorageConnector(
        conn_type="local_test",
        storage_backend=StorageType.LOCAL,
        local_dir=local_dir,
    )

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
    subfolder = local_dir / "sub"
    subfolder.mkdir()
    (subfolder / "file2.txt").write_text("another file")
    conn.push("remote/sub/file2.txt", filepath=str(subfolder / "file2.txt"))

    # Get subfolders
    subfolders = conn.get_subfolders("remote")
    assert "sub" in subfolders


def test_az_paths(tmp_path, azure_storage_connector: PathedStorageConnector):
    # Create a test file to push
    local_file = tmp_path / "test.txt"
    local_file.write_text("hello world")

    file_name = "test.txt"
    remote_path = Path("test_dir") / file_name

    # Push the file
    azure_storage_connector.push(key=str(remote_path), filepath=local_file)
    assert azure_storage_connector.exists(key=str(remote_path))

    # Pull the file back to a new location
    pulled_file = tmp_path / "pulled.txt"
    azure_storage_connector.pull(key=str(remote_path), destination_path=pulled_file)
    assert pulled_file.read_text() == "hello world"

    # Create a subfolder and file
    subfolder = tmp_path / "sub"
    subfolder.mkdir()
    (subfolder / "file2.txt").write_text("another file")
    azure_storage_connector.push(
        str(Path("sub") / "file2.txt"),
        filepath=str(subfolder / "file2.txt"),
    )

    # Get subfolders
    subfolders = azure_storage_connector.get_subfolders("")
    assert "sub" in subfolders


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
    # assert not s3_storage_connector.get_metadata("no_metadata.txt"), (
    #     "no custom metdata was pushed, so none should be retrieved"
    # )

    metadata = {"my-key": "my_value"}
    s3_storage_connector.push(
        key="has_metadata.txt", filepath=str(tmp_path / filename), metadata=metadata
    )
    assert metadata == s3_storage_connector.get_metadata("has_metadata.txt")
