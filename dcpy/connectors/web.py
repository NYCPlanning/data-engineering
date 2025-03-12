from pathlib import Path
import requests

from dcpy.utils.logging import logger
from dcpy.connectors.registry import NonVersionedConnector


def download_file(url: str, path: Path) -> None:
    """Simple wrapper to download a file using requests.get."""
    default_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    }
    logger.info(f"downloading {url} to {path}")
    response = requests.get(url, headers=default_headers)
    response.raise_for_status()
    with open(path, "wb") as f:
        f.write(response.content)


class Connector(NonVersionedConnector):
    conn_type = "file_download"

    def push(self, key: str, version, push_conf: dict | None = {}) -> dict:
        raise NotImplementedError("Sorry :)")

    def pull(
        self,
        key: str,
        destination_path: Path,
        pull_conf: dict | None = None,
    ) -> dict:
        download_file(key, destination_path)
        return {"path": destination_path}

    def get_current_version(self, key: str, conf=None) -> str | None:
        return None
