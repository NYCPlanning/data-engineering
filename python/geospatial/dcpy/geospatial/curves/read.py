"""Streamed layer reader. The only module in this package that uses GDAL."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pyarrow as pa
from pyogrio.raw import open_arrow


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
