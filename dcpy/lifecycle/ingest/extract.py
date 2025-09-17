from pathlib import Path

from dcpy.models.lifecycle.ingest.definitions import Source
from dcpy.models.lifecycle.ingest.configuration import (
    Archival,
    ResolvedConfig,
    DatasourceConfig,
)
from dcpy.utils.logging import logger
from dcpy.utils.metadata import RunDetails
from dcpy.lifecycle.ingest.connectors import source_connectors


def _pull(source: Source, version: str | None, dir: Path) -> Path:
    """
    From parsed config template and version, download raw data from source to provided path
    """
    logger.info(f"Extracting {source} from source to staging folder")

    connector = source_connectors[source.type]
    res = connector.pull(destination_path=dir, version=version, **source.model_dump())
    return res["path"]


def extract_source(
    config: ResolvedConfig, run_details: RunDetails, dir: Path
) -> DatasourceConfig:
    file = _pull(config.source, config.version, dir)
    archival = Archival(
        id=config.id,
        source=config.source,
        raw_filename=file.name,
        acl=config.acl,
        run_details=run_details,
    )
    args = config.model_dump()
    args.pop("source")
    return DatasourceConfig(
        timestamp=run_details.timestamp,
        archival=archival,
        **config.model_dump(),
    )
