from pathlib import Path

from dcpy.connectors.registry import StorageConnector
from dcpy.utils.sftp import SFTPConnector


class FTPConnector:
    def push(self, dest_path: str, ftp_profile: str):
        raise Exception("Push not implemented for FTP")

    def pull(self, **kwargs):
        raise Exception("Pull not implemented for FTP")


class SFTPConnectorAdapter(StorageConnector, SFTPConnector):
    conn_type: str = "sftp"

    def push(self, key: str, **kwargs) -> dict:
        return self._push(key, **kwargs)

    def pull(
        self,
        key: str,
        destination_path: Path,
        **kwargs,
    ) -> dict:
        return self.get_file(
            server_file_path=Path(key),
            local_file_path=destination_path,
        )

    def exists(self, key: str) -> bool:
        return self.object_exists(Path(key))

    def get_subfolders(self, prefix: str) -> list[str]:
        return self.get_subfolders(prefix)
