from pathlib import Path

import requests

from dcpy.connectors.registry import Pull
from dcpy.utils.logging import logger


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


class WebConnector(Pull):
    conn_type: str = "file_download"

    def pull(
        self,
        key: str,
        destination_path: Path,
        *,
        filename: str | None = None,
        _ds_id: str | None = None,  # TODO hack for ingest
        format: str | None = None,  # TODO hack for ingest
        **kwargs,
    ) -> dict:
        if not filename:
            if _ds_id and format:
                filename = f"{_ds_id}.{format}"
            else:
                filename = Path(key).name
        download_file(key, destination_path / filename)
        return {"path": destination_path / filename}
