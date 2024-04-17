from pathlib import Path
import requests

from dcpy.utils.logging import logger


def download_file(url: str, path: Path) -> None:
    """Simple wrapper to download a file using requests.get."""
    logger.info(f"downloading {url} to {path}")
    response = requests.get(url)
    response.raise_for_status()
    with open(path, "wb") as f:
        f.write(response.content)
