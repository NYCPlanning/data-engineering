from pathlib import Path

from dcpy.utils.geospatial.metadata import generate_metadata
from dcpy.models.data.shapefile_metadata import Metadata


def read_metadata() -> Metadata:
    return Metadata()


def write_metadata() -> None: ...
def metadata_exists() -> None: ...
def remove_metadata() -> None: ...
