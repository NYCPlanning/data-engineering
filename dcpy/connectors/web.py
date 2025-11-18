from pathlib import Path
import requests

from dcpy.utils.logging import logger
from dcpy.connectors.registry import Pull


def download_file(url: str, path: Path) -> None:
    """Simple wrapper to download a file using requests.get."""
    # browser-like headers since some servers block non-browser requests
    default_headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": "https://www.nycgovparks.org/",
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
