from contextlib import contextmanager
import paramiko
import stat
from pathlib import Path

from dcpy.utils.logging import logger


@contextmanager
def _connection(
    hostname: str,
    username: str,
    port: int,
    private_key_path: Path,
    known_hosts_path: Path,
):
    """
    Establishes a secure SFTP connection using Paramiko.

    The connection succeeds only if the server's host key is present in known_hosts and matches
    what the server presents during the handshake. Unknown or mismatched keys are rejected.

    Note: Unlike OpenSSH, Paramiko does not negotiate host key algorithms. It accepts only the
    first host key the server offers. If that key type isn't in known_hosts, the connection fails
    even if another valid key is listed (https://github.com/paramiko/paramiko/issues/2411).
    """
    logger.info(f"Connecting to SFTP server {hostname}")

    client = paramiko.SSHClient()
    client.load_host_keys(str(known_hosts_path))
    client.set_missing_host_key_policy(
        paramiko.RejectPolicy()
    )  # if server presents unknown host key, client won't connect to the server
    client.connect(
        hostname=hostname,
        port=port,
        username=username,
        key_filename=str(private_key_path),
        look_for_keys=False,
        allow_agent=False,
    )

    try:
        sftp = client.open_sftp()
        yield sftp
    finally:
        sftp.close()
        client.close()


def list_directory(
    hostname: str,
    username: str,
    *,
    known_hosts_path: Path,
    private_key_path: Path,
    path: Path = Path("."),
    port: int = 22,
) -> list[str]:
    with _connection(
        hostname=hostname,
        known_hosts_path=known_hosts_path,
        port=port,
        username=username,
        private_key_path=private_key_path,
    ) as connection:
        logger.info(f"Listing files/directories for remote path '{path}' ...")
        entries = connection.listdir(path=str(path))
    return entries


def get_subfolders(
    hostname: str,
    username: str,
    prefix: str,
    *,
    known_hosts_path: Path,
    private_key_path: Path,
    port: int = 22,
) -> list:
    with _connection(
        hostname=hostname,
        known_hosts_path=known_hosts_path,
        port=port,
        username=username,
        private_key_path=private_key_path,
    ) as connection:
        logger.info(f"Listing subfolders for remote path '{prefix}' ...")
        folder_objects = connection.listdir_attr(prefix)
        subfolders = [
            obj.filename for obj in folder_objects if stat.S_ISDIR(obj.st_mode)
        ]
        return sorted(subfolders)


def get_file(
    hostname: str,
    username: str,
    *,
    server_file_path: Path,
    local_file_path: Path,
    known_hosts_path: Path,
    private_key_path: Path,
    port: int = 22,
):
    with _connection(
        hostname=hostname,
        known_hosts_path=known_hosts_path,
        port=port,
        username=username,
        private_key_path=private_key_path,
    ) as connection:
        logger.info(
            f"Copying file from remote path '{server_file_path}' to '{local_file_path}' ..."
        )
        connection.get(remotepath=str(server_file_path), localpath=str(local_file_path))


def put_file(
    hostname: str,
    username: str,
    *,
    local_file_path: Path,
    server_file_path: Path,
    known_hosts_path: Path,
    private_key_path: Path,
    port: int = 22,
) -> paramiko.SFTPAttributes:
    with _connection(
        hostname=hostname,
        known_hosts_path=known_hosts_path,
        port=port,
        username=username,
        private_key_path=private_key_path,
    ) as connection:
        logger.info(
            f"Copying file to remote path '{server_file_path}' from '{local_file_path}' ..."
        )
        response = connection.put(
            localpath=str(local_file_path),
            remotepath=str(server_file_path),
            confirm=True,
        )
    return response


def object_exists(
    hostname: str,
    username: str,
    path: Path,
    *,
    known_hosts_path: Path,
    private_key_path: Path,
    port: int = 22,
) -> bool:
    try:
        with _connection(
            hostname=hostname,
            known_hosts_path=known_hosts_path,
            port=port,
            username=username,
            private_key_path=private_key_path,
        ) as connection:
            connection.stat(str(path))
        return True
    except FileNotFoundError:
        return False
