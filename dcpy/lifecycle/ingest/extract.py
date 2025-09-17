from pathlib import Path

from dcpy.models.lifecycle.ingest.configuration import (
    Archival,
    ResolvedDataSource,
    ArchivedDataSource,
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
    config: ResolvedDataSource, run_details: RunDetails, dir: Path
) -> ArchivedDataSource:
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
    return ArchivedDataSource(
        id=config.id,
        timestamp=run_details.timestamp,
        version=config.version,
        acl=config.acl,
        attributes=config.attributes,
        archival=archival,
        datasets=config.datasets,
    )


def determine_version_from_extracted_file(file: Path, datasource: ArchivedDataSource):
    """
    TODO
    It's imaginable that some datasets could have version determined
    by the file pulled itself, like a zipped file with only one subfile
    which has the version in the filename (CAMA) or a parquet file
    (or shapefile, etc) which contains version in its metadata

    This could be done in `extract_source`... however I personally
    would be very annoyed if a large raw datasource was supposed to
    be archived but an error was thrown during trying to read the version
    from the file. So given that, I think it makes sense to call this
    separately.
    """
    return datasource.timestamp.strftime("%Y%m%d")
