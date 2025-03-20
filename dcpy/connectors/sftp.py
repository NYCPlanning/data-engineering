import pysftp
import paramiko

from dcpy.utils.logging import logger
from dcpy.models.connectors.sftp import SFTPServer, SFTPUser

PRIVATE_KEY_DIRECTORY = "~/.ssh"


def _connection(server: SFTPServer, user: SFTPUser):
    # disable host key checking
    connection_options = pysftp.CnOpts()
    connection_options.hostkeys = None
    logger.info(f"Connecting to SFTP server {server.hostname}")

    private_key_path = f"{PRIVATE_KEY_DIRECTORY}/{user.private_key_name}"

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
