import pytest
from datetime import datetime
import pytz
from dcpy.models.connectors.sftp import SFTPServer, SFTPUser
from dcpy.connectors import sftp

from dcpy.test_integration.conftest import DOCKER_FLAG


SERVER = (
    SFTPServer(hostname="sftp-server", port=22)
    if DOCKER_FLAG
    else SFTPServer(hostname="localhost", port=2222)
)

USER = SFTPUser(
    username="dedev",
    private_key_path="./dcpy/test_integration/docker/sftp/id_rsa_key_integration_test",
    known_hosts_path="./dcpy/test_integration/docker/sftp/known_hosts_integration_test",
)

REMOTE_FILE_PATH = "remote_files/a_file.txt"


@pytest.fixture
def local_file_path(tmp_path):
    return tmp_path / "a_file.txt"


def test_list_directory():
    entries = sftp.list_directory(server=SERVER, user=USER)
    assert entries == [".ssh", "remote_files"]


def test_list_directory_specific_path():
    entries = sftp.list_directory(server=SERVER, user=USER, path="/.ssh/")
    assert entries == ["authorized_keys", "keys"]


def test_get_file(local_file_path):
    assert not local_file_path.exists(), "output file existed before test ran"
    sftp.get_file(
        server=SERVER,
        user=USER,
        server_file_path=REMOTE_FILE_PATH,
        local_file_path=local_file_path,
    )
    assert local_file_path.exists()


def test_put_file(local_file_path):
    filename = f"{datetime.now(pytz.timezone('America/New_York')).isoformat()}.txt"

    with open(local_file_path, "w") as f:
        f.write("Some local test text. File size should be 51 bytes.")

    _ = sftp.put_file(
        server=SERVER,
        user=USER,
        local_file_path=local_file_path,
        server_file_path=f"remote_files/{filename}",
    )

    remote_filenames = sftp.list_directory(
        server=SERVER, user=USER, path="/remote_files/"
    )
    assert filename in remote_filenames
