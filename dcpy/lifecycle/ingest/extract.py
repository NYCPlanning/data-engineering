from pathlib import Path

from dcpy.utils.logging import logger
from dcpy.models.lifecycle.ingest import Source
from dcpy.lifecycle.ingest.connectors import get_source_connectors


def download_file_from_source(source: Source, version: str, dir: Path) -> Path:
    """
    From parsed config template and version, download raw data from source to provided path
    """
    logger.info(f"Extracting {source} from source to staging folder")

    connector = get_source_connectors()[source.type]
    res = connector.pull(destination_path=dir, version=version, **source.model_dump())
    return res["path"]
