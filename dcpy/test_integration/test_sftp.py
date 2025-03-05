import pytest
from dcpy.models.connectors.sftp import SFTPServer, SFTPUser
from dcpy.connectors import sftp


@pytest.fixture
def default_sftp_server():
    return SFTPServer(hostname="sftp-server", port=22)


@pytest.fixture
def default_sftp_user():
    return SFTPUser(username="dedev")


def test_get_file(tmp_path, default_sftp_server, default_sftp_user):
    sftp.get_file(
        default_sftp_server,
        default_sftp_user,
        "remote_files/a_file.txt",
        tmp_path / "a_file.txt",
    )
    assert (tmp_path / "a_file.txt").exists()
