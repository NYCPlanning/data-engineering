import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from datetime import datetime
import pytz
from typing import TypedDict

from dcpy.utils.sftp import SFTPConnector
from dcpy.connectors.sftp import SFTPConnectorAdapter


class SFTPDefaults(TypedDict):
    hostname: str
    port: int
    username: str
    known_hosts_path: Path
    private_key_path: Path


SFTP_DEFAULTS: SFTPDefaults = {
    "hostname": "sftp-server",
    "port": 22,
    "username": "dedev",
    "known_hosts_path": Path("./.devcontainer/sftp/known_hosts_integration_test"),
    "private_key_path": Path("./.devcontainer/sftp/id_rsa_key_integration_test"),
}
SFTP_REMOTE_FILES_DIR = Path("remote_files")
SFTP_REMOTE_FILE = SFTP_REMOTE_FILES_DIR / "a_file.txt"


class TestSFTPConnector:
    connector = SFTPConnector(**SFTP_DEFAULTS)

    def test_list_directory(self):
        entries = self.connector.list_directory()
        assert entries == [".ssh", "remote_files"]

    def test_list_directory_specific_path(self):
        entries = self.connector.list_directory(
            path=Path("/.ssh/"),
        )
        assert entries == ["authorized_keys", "keys"]

    def test_get_file(self, tmp_path: Path):
        local_filepath = tmp_path / "test.txt"
        self.connector.get_file(
            server_file_path=SFTP_REMOTE_FILE,
            local_file_path=local_filepath,
        )
        assert local_filepath.exists()

    def test_put_file(self):
        filename = f"{datetime.now(pytz.timezone('America/New_York')).isoformat()}.txt"

        with TemporaryDirectory() as temp_dir:
            tmp_dir_path = Path(temp_dir)
            local_file_path = tmp_dir_path / filename
            server_file_path = SFTP_REMOTE_FILES_DIR / filename

            with open(local_file_path, "w") as f:
                f.write("Some local test text. File size should be 51 bytes.")

            _ = self.connector.put_file(
                local_file_path=local_file_path,
                server_file_path=server_file_path,
            )

        remote_filenames = self.connector.list_directory(SFTP_REMOTE_FILES_DIR)
        assert filename in remote_filenames

    def test_object_exists(self):
        filename = f"{datetime.now(pytz.timezone('America/New_York')).isoformat()}.txt"
        server_file_path = SFTP_REMOTE_FILES_DIR / filename

        exists = self.connector.object_exists(server_file_path)
        assert not exists  # sanity check

        with TemporaryDirectory() as temp_dir:
            local_file_path = Path(temp_dir) / filename

            with open(local_file_path, "w") as f:
                f.write("Some local test text. File size should be 51 bytes.")

            _ = self.connector.put_file(
                local_file_path=local_file_path,
                server_file_path=server_file_path,
            )

        exists = self.connector.object_exists(server_file_path)
        assert exists

    def test_get_subfolders(self):
        entries = self.connector.get_subfolders(
            prefix="/.ssh/",
        )
        assert entries == ["keys"]

    def test_get_subfolders_nonexistent_folder(self):
        with pytest.raises(FileNotFoundError):
            self.connector.get_subfolders(
                prefix="/my_nonexistent_folder_xyz",
            )


class TestSFTPConnectorAdapter:
    connector = SFTPConnectorAdapter(**SFTP_DEFAULTS)

    def test_push(self):
        filename = f"{datetime.now(pytz.timezone('America/New_York')).isoformat()}.txt"

        with TemporaryDirectory() as temp_dir:
            tmp_dir_path = Path(temp_dir)
            local_file_path = tmp_dir_path / filename
            server_file_path = SFTP_REMOTE_FILES_DIR / filename

            with open(local_file_path, "w") as f:
                f.write("Some local test text. File size should be 51 bytes.")

            _ = self.connector.push(key=str(server_file_path), filepath=local_file_path)

        assert self.connector.exists(key=str(server_file_path))

    def test_pull(self, tmp_path: Path):
        local_filepath = tmp_path / "testtt.txt"
        self.connector.pull(key=str(SFTP_REMOTE_FILE), destination_path=local_filepath)

        assert local_filepath.exists()
