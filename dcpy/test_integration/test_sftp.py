import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from datetime import datetime
import pytz

import dcpy.utils.sftp as sftp_utils


SFTP_DEFAULTS = {
    "hostname": "sftp-server",
    "port": 22,
    "username": "dedev",
    "known_hosts_path": "./.devcontainer/sftp/known_hosts_integration_test",
    "private_key_path": "./.devcontainer/sftp/id_rsa_key_integration_test",
}
SFTP_REMOTE_FILES_DIR = Path("remote_files")
SFTP_REMOTE_FILE = SFTP_REMOTE_FILES_DIR / "a_file.txt"


def test_list_directory():
    entries = sftp_utils.list_directory(**SFTP_DEFAULTS)
    assert entries == [".ssh", "remote_files"]


def test_list_directory_specific_path():
    entries = sftp_utils.list_directory(
        **SFTP_DEFAULTS,
        path="/.ssh/",
    )
    assert entries == ["authorized_keys", "keys"]


def test_get_file(tmp_path: Path):
    local_filepath = tmp_path / "test.txt"
    sftp_utils.get_file(
        **SFTP_DEFAULTS,
        server_file_path=str(SFTP_REMOTE_FILE),
        local_file_path=str(local_filepath),
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
            local_file_path=str(local_file_path),
            server_file_path=str(server_file_path),
        )

    remote_filenames = sftp_utils.list_directory(
        **SFTP_DEFAULTS,
        path=str(SFTP_REMOTE_FILES_DIR),
    )
    assert filename in remote_filenames
