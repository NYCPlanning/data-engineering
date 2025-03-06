from pathlib import Path
import shutil

from dcpy.models.lifecycle.ingest import Source
from dcpy.lifecycle.connector_registry import connectors


def download_file_from_source(source: Source, filename: str, dir: Path) -> None:
    """
    From parsed config template and version, download raw data from source to provided path
    """
    path = dir / filename
    if source.type == "local_file":
        if source.path != path:
            shutil.copy(source.path, path)
    else:
        connector = connectors[source.type]
        connector.pull(**source.model_dump(), destination_path=path)
