from pathlib import Path

from dcpy.utils.logging import logger
from dcpy.models.lifecycle.ingest import Source
from dcpy.lifecycle.connector_registry import connectors


def download_file_from_source(
    source: Source, version: str, filename: str, dir: Path
) -> None:
    """
    From parsed config template and version, download raw data from source to provided path
    """
    path = dir / filename
    logger.info(f"Extracting {path.name} from source to staging folder")
    connector = connectors[source.type]
    connector.pull(
        key=source.key, version=version, destination_path=path, **source.model_dump()
    )
