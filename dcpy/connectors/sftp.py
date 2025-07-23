from pathlib import Path
from functools import cached_property
from typing import cast

from dcpy.connectors.registry import StorageConnector
from dcpy.utils.sftp import SFTPConnector


class FTPConnector:
    def push(self, dest_path: str, ftp_profile: str):
        raise Exception("Push not implemented for FTP")

    def pull(self, **kwargs):
        raise Exception("Pull not implemented for FTP")


class SFTPConnectorAdapter(StorageConnector):
    conn_type: str = "sftp"
    hostname: str | None
    username: str | None
    port: int
    private_key_path: Path | None
    known_hosts_path: Path | None

    _connector_cache: SFTPConnector | None = None

    @cached_property
    def _connector(self) -> SFTPConnector:
        if not all(
            [
                self.hostname,
                self.username,
                self.private_key_path,
                self.known_hosts_path,
            ]
        ):
            raise Exception(
                f"Connector '{self.conn_type}' is missing required config fields"
            )
        return SFTPConnector(
            hostname=cast(str, self.hostname),
            username=cast(str, self.username),
            private_key_path=cast(Path, self.private_key_path),
            known_hosts_path=cast(Path, self.known_hosts_path),
            port=self.port,
        )

    def push(self, key: str, **kwargs) -> dict:
        return self._connector._push(key, **kwargs)

    def pull(
        self,
        key: str,
        destination_path: Path,
        **kwargs,
    ) -> dict:
        return self._connector.get_file(
            server_file_path=Path(key),
            local_file_path=destination_path,
        )

    def exists(self, key: str) -> bool:
        return self._connector.object_exists(Path(key))

    def get_subfolders(self, prefix: str) -> list[str]:
        return self._connector.get_subfolders(prefix)
