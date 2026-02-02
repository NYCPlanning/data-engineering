from dcpy.utils.geospatial.metadata import generate_metadata
from dcpy.utils.geospatial.geodatabase import (
    read_metadata,
    write_metadata,
    metadata_exists,
    remove_metadata,
)
from pytest import fixture
from pathlib import Path


def test_read_metadata():
    gdb = Path("path/to/test_db.gdb")
    layer = "test"
    print(f"{read_metadata(gdb, layer)=}")
    assert False
