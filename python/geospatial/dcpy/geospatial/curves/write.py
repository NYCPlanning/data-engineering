"""Streaming GeoParquet 1.1 writer for linearized batches."""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS
from shapely import from_wkb, get_type_id, is_missing, total_bounds

_TYPE_NAMES = {
    0: "Point",
    1: "LineString",
    3: "Polygon",
    4: "MultiPoint",
    5: "MultiLineString",
    6: "MultiPolygon",
    7: "GeometryCollection",
}


def _geo_metadata(
    geom_col: str, crs: str | None, types: set[str], bbox: list[float] | None
) -> bytes:
    col: dict = {"encoding": "WKB", "geometry_types": sorted(types)}
    if crs:
        col["crs"] = CRS.from_user_input(crs).to_json_dict()
    if bbox:
        col["bbox"] = bbox
    meta = {"version": "1.1.0", "primary_column": geom_col, "columns": {geom_col: col}}
    return json.dumps(meta).encode()


def write_geoparquet(
    batches: Iterable[pa.RecordBatch],
    out_path: str | Path,
    crs: str | None,
    geom_col: str = "wkb_geometry",
) -> int:
    """Write linearized batches one at a time; returns the row count.

    ``geometry_types`` and ``bbox`` are computed from the linear output and
    added to the footer metadata after the last batch.
    """
    types: set[str] = set()
    bbox: list[float] | None = None
    rows = 0
    writer: pq.ParquetWriter | None = None
    for batch in batches:
        geoms = from_wkb(batch.column(geom_col).to_pylist())
        present = geoms[~is_missing(geoms)]
        types.update(_TYPE_NAMES[int(t)] for t in set(get_type_id(present).tolist()))
        if len(present):
            b = total_bounds(present).tolist()
            if bbox is not None:
                b = [min(bbox[0], b[0]), min(bbox[1], b[1]),
                     max(bbox[2], b[2]), max(bbox[3], b[3])]  # fmt: skip
            bbox = b
        if writer is None:
            writer = pq.ParquetWriter(str(out_path), batch.schema)
        writer.write_batch(batch)
        rows += batch.num_rows
    if writer is None:
        raise ValueError("no batches to write")
    writer.add_key_value_metadata(
        {"geo": _geo_metadata(geom_col, crs, types, bbox).decode()}
    )
    writer.close()
    return rows
