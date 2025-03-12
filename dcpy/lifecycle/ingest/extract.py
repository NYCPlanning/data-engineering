from pathlib import Path
import shutil

from dcpy.models.lifecycle.ingest import Source
from dcpy.lifecycle.connector_registry import nonversioned_connectors


def download_file_from_source(source: Source, filename: str, dir: Path) -> None:
    """
    From parsed config template and version, download raw data from source to provided path
    """
    path = dir / filename
    if source.type == "local_file":
        if source.path != path:
            shutil.copy(source.path, path)
    else:
        connector = nonversioned_connectors[source.type]
        print(source.model_dump())
        connector.pull(
            key=source.key, destination_path=path, pull_conf=source.model_dump()
        )
