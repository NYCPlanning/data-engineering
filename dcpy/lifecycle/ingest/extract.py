from pathlib import Path

from dcpy.utils.logging import logger
from dcpy.models.lifecycle.ingest import Source
from dcpy.lifecycle.ingest.connectors import source_connectors


def download_file_from_source(
    source: Source, filename: str, version: str, dir: Path
) -> None:
    """
    From parsed config template and version, download raw data from source to provided path
    """
    logger.info(f"Extracting {filename} from source to staging folder")
    path = dir / filename
    pull_conf = source.model_dump()
    pull_conf["filename"] = filename
    pull_conf.pop("key", None)

    connector = source_connectors[source.type]
    connector.pull(
        key=source.key,
        version=version,
        destination_path=path,
        **pull_conf,
    )
