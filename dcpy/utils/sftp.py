import socket
import stat
import time
from contextlib import contextmanager
from pathlib import Path

import paramiko
from pydantic import BaseModel

from dcpy.utils.logging import logger

KNOWN_HOSTS_DEFAULT_PATH = Path.home() / ".ssh/known_hosts"

# The DOF SFTP server intermittently drops the SFTP channel right after a
# successful auth (paramiko raises a bare EOFError) for a window of a minute or
# two before recovering. Retry connection setup across that window before giving
# up. Worst-case added wait on persistent failure is the sum of the backoffs.
CONNECT_MAX_ATTEMPTS = 4
CONNECT_BACKOFF_SECONDS = 15
# Auth / host-key failures are deterministic, so they must surface immediately
# rather than be retried (these are paramiko.SSHException subclasses, so they're
# checked before the broader retryable set below).
_NON_RETRYABLE_CONNECT_ERRORS = (
    paramiko.AuthenticationException,
    paramiko.BadHostKeyException,
)
_RETRYABLE_CONNECT_ERRORS = (EOFError, paramiko.SSHException, socket.error)


class SFTPServer(BaseModel):
    hostname: str
    username: str
    private_key_path: Path
    known_hosts_path: Path = KNOWN_HOSTS_DEFAULT_PATH
    port: int = 22

    def _open_client(self) -> tuple[paramiko.SSHClient, paramiko.SFTPClient]:
        """
        Open an authenticated SSH client and SFTP channel, retrying transient
        connection failures with backoff.

        The connection succeeds only if the server's host key is present in known_hosts and matches
        what the server presents during the handshake. Unknown or mismatched keys are rejected.

        Note: Unlike OpenSSH, Paramiko does not negotiate host key algorithms. It accepts only the
        first host key the server offers. If that key type isn't in known_hosts, the connection fails
        even if another valid key is listed (https://github.com/paramiko/paramiko/issues/2411).

        Both connect() and open_sftp() are retried: the server's intermittent drop happens at SFTP
        channel open, *after* a successful auth, so retrying connect() alone wouldn't help.
        """
        last_error: Exception | None = None
        for attempt in range(1, CONNECT_MAX_ATTEMPTS + 1):
            logger.info(
                f"Connecting to SFTP server {self.hostname} "
                f"(attempt {attempt}/{CONNECT_MAX_ATTEMPTS})"
            )
            client = paramiko.SSHClient()
            client.load_host_keys(str(self.known_hosts_path))
            client.set_missing_host_key_policy(
                paramiko.RejectPolicy()
            )  # if server presents unknown host key, client won't connect to the server
            try:
                client.connect(
                    hostname=self.hostname,
                    port=self.port,
                    username=self.username,
                    key_filename=str(self.private_key_path),
                    look_for_keys=False,
                    allow_agent=False,
                )
                sftp = client.open_sftp()
                return client, sftp
            except _NON_RETRYABLE_CONNECT_ERRORS:
                client.close()
                raise
            except _RETRYABLE_CONNECT_ERRORS as e:
                client.close()
                last_error = e
                if attempt < CONNECT_MAX_ATTEMPTS:
                    delay = CONNECT_BACKOFF_SECONDS * attempt
                    logger.warning(
                        f"SFTP connection to {self.hostname} failed "
                        f"(attempt {attempt}/{CONNECT_MAX_ATTEMPTS}): {e!r}. "
                        f"Retrying in {delay}s ..."
                    )
                    time.sleep(delay)

        # Re-raise as a ConnectionError so the real failure stays visible: paramiko
        # raises a bare EOFError, which Typer's CLI wrapper would otherwise swallow
        # into "Aborted." with no traceback (its EOFError handler is meant for
        # interactive prompts hitting end-of-input).
        raise ConnectionError(
            f"SFTP connection to {self.hostname} failed after "
            f"{CONNECT_MAX_ATTEMPTS} attempts"
        ) from last_error

    @contextmanager
    def _connection(self):
        client, sftp = self._open_client()
        try:
            yield sftp
        finally:
            # close() tears down the underlying transport and the SFTP channel.
            client.close()

    def list_directory(self, path: Path = Path(".")) -> list[str]:
        with self._connection() as connection:
            logger.info(f"Listing files/directories for remote path '{path}' ...")
            entries = connection.listdir(path=str(path))
        return sorted(entries)

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
            connection.get(
                remotepath=str(server_file_path),
                localpath=str(filepath),
                max_concurrent_prefetch_requests=64,
            )
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

    def object_exists(self, path: Path) -> bool:
        try:
            with self._connection() as connection:
                connection.stat(str(path))
            return True
        except FileNotFoundError:
            return False
