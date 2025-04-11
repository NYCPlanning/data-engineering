from pathlib import Path

from dcpy.utils.logging import logger
from dcpy.models.lifecycle.ingest import Source
from dcpy.lifecycle.ingest.connectors import source_connectors


def download_file_from_source(source: Source, version: str, dir: Path) -> None:
    """
    From parsed config template and version, download raw data from source to provided path
    """
    logger.info(f"Extracting {source} from source to staging folder")
    pull_conf = source.model_dump()
    pull_conf.pop("key", None)

    connector = source_connectors[source.type]
    res = connector.pull(
        key=source.key,
        version=version,
        destination_path=dir,
        **pull_conf,
    )
    return res["path"]
