from pathlib import Path

from dcpy.models.lifecycle.ingest import Source
from dcpy.lifecycle.connector_registry import connectors


def download_file_from_source(
    source: Source, version: str, filename: str, dir: Path
) -> None:
    """
    From parsed config template and version, download raw data from source to provided path
    """
    path = dir / filename
    connector = connectors[source.type]
    connector.pull(
        key=source.key, version=version, destination_path=path, **source.model_dump()
    )
