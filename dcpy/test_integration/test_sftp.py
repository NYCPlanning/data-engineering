import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from datetime import datetime
import pytz

import dcpy.utils.sftp as sftp_utils
from dcpy.connectors.sftp import SFTPConnector


SFTP_DEFAULTS = {
    "hostname": "sftp-server",
    "port": 22,
    "username": "dedev",
    "known_hosts_path": Path("./.devcontainer/sftp/known_hosts_integration_test"),
    "private_key_path": Path("./.devcontainer/sftp/id_rsa_key_integration_test"),
}
SFTP_REMOTE_FILES_DIR = Path("remote_files")
SFTP_REMOTE_FILE = SFTP_REMOTE_FILES_DIR / "a_file.txt"


def test_list_directory():
    entries = sftp_utils.list_directory(**SFTP_DEFAULTS)
    assert entries == [".ssh", "remote_files"]


def test_list_directory_specific_path():
    entries = sftp_utils.list_directory(
        **SFTP_DEFAULTS,
        path=Path("/.ssh/"),
    )
    assert entries == ["authorized_keys", "keys"]


def test_get_file(tmp_path: Path):
    local_filepath = tmp_path / "test.txt"
    sftp_utils.get_file(
        **SFTP_DEFAULTS,
        server_file_path=SFTP_REMOTE_FILE,
        local_file_path=local_filepath,
    )
    assert local_filepath.exists()


def test_put_file():
    filename = f"{datetime.now(pytz.timezone('America/New_York')).isoformat()}.txt"

    with TemporaryDirectory() as temp_dir:
        tmp_dir_path = Path(temp_dir)
        local_file_path = tmp_dir_path / filename
        server_file_path = SFTP_REMOTE_FILES_DIR / filename

        with open(local_file_path, "w") as f:
            f.write("Some local test text. File size should be 51 bytes.")

        _ = sftp_utils.put_file(
            **SFTP_DEFAULTS,
            local_file_path=local_file_path,
            server_file_path=server_file_path,
        )

    remote_filenames = sftp_utils.list_directory(
        **SFTP_DEFAULTS,
        path=SFTP_REMOTE_FILES_DIR,
    )
    assert filename in remote_filenames


def test_object_exists():
    filename = f"{datetime.now(pytz.timezone('America/New_York')).isoformat()}.txt"
    server_file_path = SFTP_REMOTE_FILES_DIR / filename

    exists = sftp_utils.object_exists(**SFTP_DEFAULTS, path=server_file_path)
    assert not exists  # sanity check

    with TemporaryDirectory() as temp_dir:
        local_file_path = Path(temp_dir) / filename

        with open(local_file_path, "w") as f:
            f.write("Some local test text. File size should be 51 bytes.")

        _ = sftp_utils.put_file(
            **SFTP_DEFAULTS,
            local_file_path=local_file_path,
            server_file_path=server_file_path,
        )

    exists = sftp_utils.object_exists(**SFTP_DEFAULTS, path=server_file_path)
    assert exists


def test_get_subfolders():
    entries = sftp_utils.get_subfolders(
        **SFTP_DEFAULTS,
        prefix="/.ssh/",
    )
    assert entries == ["keys"]


def test_get_subfolders_nonexistent_folder():
    with pytest.raises(FileNotFoundError):
        sftp_utils.get_subfolders(
            **SFTP_DEFAULTS,
            prefix="/my_nonexistent_folder_xyz",
        )


class TestConnector:
    connector: SFTPConnector = SFTPConnector(**SFTP_DEFAULTS)

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
