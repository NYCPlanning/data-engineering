from contextlib import contextmanager
import paramiko
import stat
from pathlib import Path
from pydantic import BaseModel

from dcpy.utils.logging import logger


class SFTPConnector(BaseModel):
    hostname: str
    username: str
    private_key_path: Path
    known_hosts_path: Path = Path("~/.ssh/known_hosts")
    port: int = 22

    @contextmanager
    def _connection(self):
        """
        Establishes a secure SFTP connection using Paramiko.

        The connection succeeds only if the server's host key is present in known_hosts and matches
        what the server presents during the handshake. Unknown or mismatched keys are rejected.

        Note: Unlike OpenSSH, Paramiko does not negotiate host key algorithms. It accepts only the
        first host key the server offers. If that key type isn't in known_hosts, the connection fails
        even if another valid key is listed (https://github.com/paramiko/paramiko/issues/2411).
        """
        logger.info(f"Connecting to SFTP server {self.hostname}")

        client = paramiko.SSHClient()
        client.load_host_keys(str(self.known_hosts_path))
        client.set_missing_host_key_policy(
            paramiko.RejectPolicy()
        )  # if server presents unknown host key, client won't connect to the server
        client.connect(
            hostname=self.hostname,
            port=self.port,
            username=self.username,
            key_filename=str(self.private_key_path),
            look_for_keys=False,
            allow_agent=False,
        )

        try:
            sftp = client.open_sftp()
            yield sftp
        finally:
            sftp.close()
            client.close()

    def list_directory(self, path: Path = Path(".")) -> list[str]:
        with self._connection() as connection:
            logger.info(f"Listing files/directories for remote path '{path}' ...")
            entries = connection.listdir(path=str(path))
        return entries

    def get_subfolders(self, prefix: str) -> list:
        with self._connection() as connection:
            logger.info(f"Listing subfolders for remote path '{prefix}' ...")
            folder_objects = connection.listdir_attr(prefix)
            subfolders = [
                obj.filename for obj in folder_objects if stat.S_ISDIR(obj.st_mode)
            ]
            return sorted(subfolders)

    def get_file(
        self,
        *,
        server_file_path: Path,
        local_file_path: Path,
    ) -> dict:
        if local_file_path.is_dir():
            filepath = local_file_path / server_file_path.name
        else:
            local_file_path.parent.mkdir(parents=True, exist_ok=True)
            filepath = local_file_path
        with self._connection() as connection:
            logger.info(
                f"Copying file from remote path '{server_file_path}' to '{local_file_path}' ..."
            )
            connection.get(remotepath=str(server_file_path), localpath=str(filepath))
        return {"path": filepath}

    def put_file(
        self,
        *,
        local_file_path: Path,
        server_file_path: Path,
    ) -> paramiko.SFTPAttributes:
        with self._connection() as connection:
            logger.info(
                f"Copying file to remote path '{server_file_path}' from '{local_file_path}' ..."
            )
            response = connection.put(
                localpath=str(local_file_path),
                remotepath=str(server_file_path),
                confirm=True,
            )
        return response

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

    def object_exists(self, path: Path) -> bool:
        try:
            with self._connection() as connection:
                connection.stat(str(path))
            return True
        except FileNotFoundError:
            return False
