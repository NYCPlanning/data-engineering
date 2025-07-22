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

    def _push(
        self,
        key: str,
        *,
        filepath: Path,
        **kwargs,
    ) -> dict:
        self.put_file(
            local_file_path=filepath,
            server_file_path=Path(key),
        )
        return {"key": key}

    def push(self, key: str, **kwargs) -> dict:
        return self._push(key, **kwargs)

    def pull(
        self,
        key: str,
        destination_path: Path,
        **kwargs,
    ) -> dict:
        if destination_path.is_dir():
            filepath = destination_path / Path(key).name
        else:
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            filepath = destination_path

        self.get_file(
            server_file_path=Path(key),
            local_file_path=filepath,
        )
        return {"path": filepath}

    def exists(self, key: str) -> bool:
        return self.object_exists(Path(key))

    def get_subfolders(self, prefix: str) -> list[str]:
        return self.get_subfolders(prefix)
