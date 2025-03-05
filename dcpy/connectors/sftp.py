import os
import pysftp
import paramiko

from dcpy.utils.logging import logger
from dcpy.models.connectors.sftp import SFTPServer, SFTPUser


def _private_key_from_env() -> str:
    key_path = ".ssh/private_key"
    with open(key_path, "wb") as f:
        f.write(os.environ["SFTP_PRIVATE_KEY"].encode("utf-8"))
    logger.info(
        f"Wrote sftp private key from environemnt variable SFTP_PRIVATE_KEY to {key_path} ..."
    )
    return key_path


def _connection(server: SFTPServer, user: SFTPUser):
    # disable host key checking
    connection_options = pysftp.CnOpts()
    connection_options.hostkeys = None
    logger.info(f"Connecting to SFTP server {server.hostname}")

    private_key_path = (
        user.private_key_path if user.private_key_path else _private_key_from_env()
    )

    return pysftp.Connection(
        host=server.hostname,
        port=server.port,
        username=user.username,
        private_key=private_key_path,
        cnopts=connection_options,
    )


def list_directory(server: SFTPServer, user: SFTPUser, path: str = ".") -> list[str]:
    with _connection(server, user) as connection:
        logger.info(f"Listing files/directories for remote path '{path}' ...")
        entries = connection.listdir(remotepath=path)
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
