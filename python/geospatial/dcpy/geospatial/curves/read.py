"""Streamed layer reader. The only module in this package that uses GDAL."""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pyarrow as pa
from pyogrio.raw import open_arrow, read_arrow


@contextmanager
def read_curved_batches(
    path: str | Path, layer: str, batch_size: int = 10_000
) -> Iterator[tuple[dict, Iterator[pa.RecordBatch]]]:
    """Yield (pyogrio meta, batch iterator) with curves left intact in ISO WKB.

    ``meta`` carries ``crs`` and ``geometry_name``.
    """
    with open_arrow(
        str(path), layer=layer, batch_size=batch_size, use_pyarrow=True
    ) as (meta, reader):
        yield meta, iter(reader)


_GRID_TAGS = {"XOrigin": "XORIGIN", "YOrigin": "YORIGIN", "XYScale": "XYSCALE",
              "XYTolerance": "XYTOLERANCE"}  # fmt: skip


def gdb_xy_grid(path: str | Path, layer: str) -> dict[str, str]:
    """A layer's xy origin/scale/tolerance as OpenFileGDB layer creation options.

    Writing with the source grid makes coordinates round-trip exactly.
    """
    _, table = read_arrow(
        str(path),
        layer="GDB_Items",
        columns=["Name", "Definition"],
        read_geometry=False,
    )
    for name, xml in zip(table["Name"].to_pylist(), table["Definition"].to_pylist()):
        if name == layer and xml:
            found = dict(
                re.findall(r"<(XOrigin|YOrigin|XYScale|XYTolerance)>([^<]+)<", xml)
            )
            return {_GRID_TAGS[k]: v for k, v in found.items()}
    return {}
