from pathlib import Path
import requests

from dcpy.utils.logging import logger


def download_file(url: str, file_name: str, dir: Path) -> Path:
    """Simple wrapper to download a file using requests.get.
    Returns filepath."""
    file = dir / file_name
    logger.info(f"downloading {url} to {file}")
    response = requests.get(url)
    response.raise_for_status()
    with open(file, "wb") as f:
        f.write(response.content)
    return file
