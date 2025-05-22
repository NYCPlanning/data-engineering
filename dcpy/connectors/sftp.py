from contextlib import contextmanager
import paramiko

from dcpy.utils.logging import logger
from dcpy.models.connectors.sftp import SFTPServer, SFTPUser


@contextmanager
def _connection(server: SFTPServer, user: SFTPUser):
    logger.info(f"Connecting to SFTP server {server.hostname}")

    client = paramiko.SSHClient()
    client.load_host_keys(user.known_hosts_path)
    client.set_missing_host_key_policy(
        paramiko.RejectPolicy()
    )  # if server presents unknown host key, client won't connect to the server
    client.connect(
        hostname=server.hostname,
        port=server.port,
        username=user.username,
        key_filename=user.private_key_path,
        look_for_keys=False,
        allow_agent=False,
    )

    try:
        sftp = client.open_sftp()
        yield sftp
    finally:
        sftp.close()
        client.close()


def list_directory(server: SFTPServer, user: SFTPUser, path: str = ".") -> list[str]:
    with _connection(server, user) as connection:
        logger.info(f"Listing files/directories for remote path '{path}' ...")
        entries = connection.listdir(path=path)
    return entries


def get_file(
    server: SFTPServer, user: SFTPUser, server_file_path: str, local_file_path: str
):
    with _connection(server, user) as connection:
        logger.info(
            f"Copying file from remote path '{server_file_path}' to '{local_file_path}' ..."
        )
        connection.get(remotepath=server_file_path, localpath=local_file_path)


def put_file(
    server: SFTPServer, user: SFTPUser, local_file_path: str, server_file_path: str
) -> paramiko.SFTPAttributes:
    with _connection(server, user) as connection:
        logger.info(
            f"Copying file to remote path '{server_file_path}' from '{local_file_path}' ..."
        )
        response = connection.put(
            localpath=local_file_path,
            remotepath=server_file_path,
            confirm=True,
        )
    return response


class FTPConnector:
    def push(self, dest_path: str, ftp_profile: str):
        raise Exception("Push not implemented for FTP")

    def pull(self, **kwargs):
        raise Exception("Pull not implemented for FTP")
