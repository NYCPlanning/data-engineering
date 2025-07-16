from pathlib import Path

from dcpy.connectors.registry import StorageConnector
import dcpy.utils.sftp as sftp_utils


class FTPConnector:
    def push(self, dest_path: str, ftp_profile: str):
        raise Exception("Push not implemented for FTP")

    def pull(self, **kwargs):
        raise Exception("Pull not implemented for FTP")


class SFTPConnector(StorageConnector):
    conn_type: str = "sftp"
    hostname: str
    username: str
    private_key_path: Path
    known_hosts_path: Path = Path("~/.ssh/known_hosts")
    port: int = 22

    def _push(
        self,
        key: str,
        *,
        filepath: Path,
        **kwargs,
    ) -> dict:
        sftp_utils.put_file(
            self.hostname,
            self.username,
            local_file_path=filepath,
            server_file_path=Path(key),
            known_hosts_path=self.known_hosts_path,
            private_key_path=self.private_key_path,
            port=self.port,
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

        sftp_utils.get_file(
            self.hostname,
            self.username,
            server_file_path=Path(key),
            local_file_path=filepath,
            known_hosts_path=self.known_hosts_path,
            private_key_path=self.private_key_path,
            port=self.port,
        )
        return {"path": filepath}

    def exists(self, key: str) -> bool:
        return sftp_utils.object_exists(
            self.hostname,
            self.username,
            Path(key),
            known_hosts_path=self.known_hosts_path,
            private_key_path=self.private_key_path,
            port=self.port,
        )

    def get_subfolders(self, prefix: str) -> list[str]:
        return sftp_utils.get_subfolders(
            self.hostname,
            self.username,
            prefix,
            known_hosts_path=self.known_hosts_path,
            private_key_path=self.private_key_path,
            port=self.port,
        )
