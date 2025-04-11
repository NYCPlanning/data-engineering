from pathlib import Path
import requests

from dcpy.utils.logging import logger
from dcpy.connectors.registry import Connector


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


class WebConnector(Connector):
    conn_type: str = "file_download"

    def push(self, key: str, **kwargs) -> dict:
        # TODO should probably not be a fully-fledged "connector"
        raise NotImplementedError(f"{self.conn_type} does not support push")

    def _filename(self, key: str, filename: str | None, format: str | None) -> str:
        if filename:
            return filename
        else:
            key_name = Path(key).name
            if "." in key_name:
                if format:
                    assert format == Path(key).suffix
                filename = key_name
            elif format:
                filename = f"{key_name}.{format}"

    def pull(
        self,
        key: str,
        destination_path: Path,
        *,
        filename: str | None = None,
        format: str | None = None,
        **kwargs,
    ) -> dict:
        filename = self._filename(key=key, filename=filename, format=format)
        download_file(key, destination_path / filename)
        return {"path": destination_path}
