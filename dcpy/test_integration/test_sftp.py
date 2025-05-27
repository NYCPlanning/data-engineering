import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from datetime import datetime
import pytz
from dcpy.models.connectors.sftp import SFTPServer, SFTPUser
from dcpy.connectors import sftp


@pytest.fixture
def default_sftp_kwargs(tmp_path):
    return {
        "server": SFTPServer(hostname="sftp-server", port=22),
        "user": SFTPUser(
            username="dedev",
            private_key_path="./.devcontainer/sftp/id_rsa_key_integration_test",
            known_hosts_path="./.devcontainer/sftp/known_hosts_integration_test",
        ),
        "server_file_path": "remote_files/a_file.txt",
        "local_file_path": tmp_path / "a_file.txt",
    }


def test_list_directory(default_sftp_kwargs: dict):
    print(f"{default_sftp_kwargs['user']}")
    entries = sftp.list_directory(
        server=default_sftp_kwargs["server"], user=default_sftp_kwargs["user"]
    )
    assert entries == [".ssh", "remote_files"]


def test_list_directory_specific_path(default_sftp_kwargs: dict):
    entries = sftp.list_directory(
        server=default_sftp_kwargs["server"],
        user=default_sftp_kwargs["user"],
        path="/.ssh/",
    )
    assert entries == ["authorized_keys", "keys"]


def test_get_file(default_sftp_kwargs: dict):
    sftp.get_file(**default_sftp_kwargs)
    assert default_sftp_kwargs["local_file_path"].exists()


def test_put_file(default_sftp_kwargs: dict):
    sftp_put_kwargs = default_sftp_kwargs
    filename = f"{datetime.now(pytz.timezone('America/New_York')).isoformat()}.txt"

    with TemporaryDirectory() as temp_dir:
        tmp_dir_path = Path(temp_dir)
        local_file_path = tmp_dir_path / f"{filename}.txt"
        sftp_put_kwargs["local_file_path"] = local_file_path
        sftp_put_kwargs["server_file_path"] = f"remote_files/{filename}"

        with open(local_file_path, "w") as f:
            f.write("Some local test text. File size should be 51 bytes.")

        _ = sftp.put_file(**sftp_put_kwargs)

    remote_filenames = sftp.list_directory(
        server=default_sftp_kwargs["server"],
        user=default_sftp_kwargs["user"],
        path="/remote_files/",
    )
    assert filename in remote_filenames
