from pathlib import Path
from functools import cached_property
from typing import cast

from dcpy.connectors.registry import StorageConnector
from dcpy.utils.sftp import SFTPServer


class SFTPConnector(StorageConnector):
    conn_type: str = "sftp"
    hostname: str | None
    username: str | None
    port: int
    private_key_path: Path | None
    known_hosts_path: Path | None

    @cached_property
    def _server(self) -> SFTPServer:
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
        return SFTPServer(
            hostname=cast(str, self.hostname),
            username=cast(str, self.username),
            private_key_path=cast(Path, self.private_key_path),
            known_hosts_path=cast(Path, self.known_hosts_path),
            port=self.port,
        )

    def _push(self, key: str, filepath: Path, **kwargs) -> dict:
        self._server.put_file(
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
        return self._server.get_file(
            server_file_path=Path(key),
            local_file_path=destination_path,
        )

    def exists(self, key: str) -> bool:
        return self._server.object_exists(Path(key))

    def get_subfolders(self, prefix: str) -> list[str]:
        return self._server.get_subfolders(prefix)
