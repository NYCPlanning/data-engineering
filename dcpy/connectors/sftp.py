import os
import tempfile
import pysftp  # type: ignore

from dcpy.utils.logging import logger
from dcpy.models.connectors.sftp import SFTPServer, SFTPUser

SFTP_PRIVATE_KEY = os.environ["SFTP_PRIVATE_KEY"]


def _temp_private_key_path() -> str:
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_file.write(SFTP_PRIVATE_KEY.encode("utf-8"))
        tmp_file_path = tmp_file.name
    return tmp_file_path


def get_file(
    server: SFTPServer, user: SFTPUser, server_filepath: str, output_filepath: str
):
    logger.info(f"Connecting to SFTP sever {server.hostname} ...")
    with pysftp.Connection(
        host=server.hostname,
        port=server.port,
        username=user.username,
        private_key=_temp_private_key_path(),
    ) as connection:
        logger.info(
            f"Copying file from sever '{server_filepath}' to local '{output_filepath}' ..."
        )
        connection.get(server_filepath, output_filepath)


def put_file(
    server: SFTPServer, user: SFTPUser, server_filepath: str, output_filepath: str
):
    raise NotImplementedError


class FTPConnector:
    def push(self, dest_path: str, ftp_profile: str):
        raise Exception("Push not implemented for FTP")

    def pull(self, **kwargs):
        raise Exception("Pull not implemented for FTP")
