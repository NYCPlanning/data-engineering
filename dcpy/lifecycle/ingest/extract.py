from pathlib import Path

from dcpy.models.lifecycle.ingest import Source
from dcpy.lifecycle.connector_registry import connectors


def download_file_from_source(source: Source, filename: str, dir: Path) -> None:
    """
    From parsed config template and version, download raw data from source to provided path
    """
    path = dir / filename
    connector = connectors.nonversioned[source.type]
    connector.pull(key=source.key, destination_path=path, pull_conf=source.model_dump())
