"""Linear WKB plus arcs back to ISO curve WKB, and a streaming FGDB writer.

Inverse of ``split_curve_wkb``: each arc's vertex run ``pts[v0..v1]`` is replaced
by a 3-point CircularString built from the stored control points.
"""

from __future__ import annotations

import json
import struct
from collections import defaultdict
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
from pyogrio.raw import write_arrow
from pyproj import CRS

from dcpy.geospatial.curves.convert import ARCS_COLUMN
from dcpy.geospatial.curves.linearize import DEFAULT_MIN_SEGMENTS, DEFAULT_TOLERANCE
from dcpy.geospatial.curves.wkb import (
    CIRCULARSTRING,
    COMPOUNDCURVE,
    CURVEPOLYGON,
    LINESTRING,
    MULTICURVE,
    MULTISURFACE,
    Arc,
    CurveStats,
    _coords,
    _emit,
    _Node,
    _Reader,
)

_ArcMap = dict[tuple[int, int], list[Arc]]


def _head(code: int, n: int) -> bytes:
    return struct.pack("<BII", 1, code, n)


def _curve(pts: list, arcs: list[Arc] | None) -> bytes:
    """A line or ring: plain LineString, or CompoundCurve when it has arcs."""
    if not arcs:
        return struct.pack("<BI", 1, LINESTRING) + _coords(pts)
    members, prev = [], 0
    for a in arcs:
        if a.v0 > prev:
            members.append(
                struct.pack("<BI", 1, LINESTRING) + _coords(pts[prev : a.v0 + 1])
            )
        ctrl = [(a.x0, a.y0), (a.x1, a.y1), (a.x2, a.y2)]
        members.append(struct.pack("<BI", 1, CIRCULARSTRING) + _coords(ctrl))
        prev = a.v1
    if prev < len(pts) - 1:
        members.append(struct.pack("<BI", 1, LINESTRING) + _coords(pts[prev:]))
    return _head(COMPOUNDCURVE, len(members)) + b"".join(members)


def _has_arcs(node: _Node, arcs: _ArcMap, part: int) -> bool:
    kind, body = node
    if kind == "LineString":
        return (part, -1) in arcs
    if kind == "Polygon":
        return any((part, r) in arcs for r in range(len(body)))
    if kind in ("MultiLineString", "MultiPolygon"):
        return any(_has_arcs(k, arcs, i) for i, k in enumerate(body))
    return False


def _emit_curved(node: _Node, arcs: _ArcMap, part: int = 0) -> bytes:
    """Mirror of ``_Reader.geometry``: multi members carry their index as ``part``."""
    if not _has_arcs(node, arcs, part):
        return _emit(node)
    kind, body = node
    if kind == "LineString":
        return _curve(body, arcs[(part, -1)])
    if kind == "Polygon":
        rings = [_curve(r, arcs.get((part, i))) for i, r in enumerate(body)]
        return _head(CURVEPOLYGON, len(rings)) + b"".join(rings)
    code = MULTICURVE if kind == "MultiLineString" else MULTISURFACE
    kids = [_emit_curved(k, arcs, i) for i, k in enumerate(body)]
    return _head(code, len(kids)) + b"".join(kids)


def curve_wkb(linear_wkb: bytes, arcs: Sequence[Arc]) -> bytes:
    """ISO curve WKB from linear WKB and its arcs; no arcs returns the input as is.

    Lines become CompoundCurve, polygons CurvePolygon, multi-lines MultiCurve and
    multi-polygons MultiSurface. Rings without arcs stay plain LineStrings.
    Beziers aren't arcs: GDAL hands them over linearized, and they stay linear.
    """
    if not arcs:
        return linear_wkb
    by_ring: _ArcMap = defaultdict(list)
    for a in sorted(arcs, key=lambda a: (a.part, a.ring, a.seq)):
        by_ring[(a.part, a.ring)].append(a)
    reader = _Reader(linear_wkb, CurveStats(), DEFAULT_TOLERANCE, DEFAULT_MIN_SEGMENTS)
    return _emit_curved(reader.geometry(), dict(by_ring))


def recurve_batch(
    batch: pa.RecordBatch, geom_col: str = "wkb_geometry", arcs_col: str = ARCS_COLUMN
) -> pa.RecordBatch:
    """Inverse of ``linearize_batch``: curve ``geom_col`` and drop ``arcs_col``."""
    geoms = [
        None if wkb is None else curve_wkb(wkb, [Arc(**a) for a in arcs or []])
        for wkb, arcs in zip(
            batch.column(geom_col).to_pylist(), batch.column(arcs_col).to_pylist()
        )
    ]
    idx = batch.schema.get_field_index(geom_col)
    out = batch.set_column(idx, geom_col, pa.array(geoms, type=pa.binary()))
    return out.drop_columns([arcs_col])


def _arcs_by_fid(path: str | Path, batch_size: int) -> Iterator[tuple[int, list[Arc]]]:
    """Arcs grouped by ``ogc_fid``; the arcs parquet is written in fid order."""
    fid, group = -1, list[Arc]()
    for b in pq.ParquetFile(str(path)).iter_batches(batch_size):
        for row in b.to_pylist():
            f = row.pop("ogc_fid")
            if f != fid and group:
                yield fid, group
                group = []
            fid = f
            group.append(Arc(**row))
    if group:
        yield fid, group


def _recurved_batches(
    linear: pq.ParquetFile,
    arcs: Iterator[tuple[int, list[Arc]]],
    geom_col: str,
    schema: pa.Schema,
    batch_size: int,
    stats: CurveStats,
) -> Iterable[pa.RecordBatch]:
    pending = next(arcs, None)
    for batch in linear.iter_batches(batch_size):
        geoms = []
        fids = cast(list[int], batch.column("ogc_fid").to_pylist())
        for fid, wkb in zip(fids, batch.column(geom_col).to_pylist()):
            found: list[Arc] = []
            while pending is not None and pending[0] <= fid:
                if pending[0] == fid:
                    found = pending[1]
                pending = next(arcs, None)
            stats.geometries += wkb is not None
            stats.arcs += len(found)
            geoms.append(None if wkb is None else curve_wkb(wkb, found))
        cols = [
            pa.array(geoms, pa.binary()) if f.name == geom_col else batch.column(f.name)
            for f in schema
        ]
        yield pa.RecordBatch.from_arrays(cols, schema=schema)


def parquet_to_gdb(
    linear_parquet: str | Path,
    arcs_parquet: str | Path,
    out_gdb: str | Path,
    layer: str,
    geometry_type: str | None = None,
    layer_options: dict[str, str] | None = None,
    batch_size: int = 10_000,
) -> CurveStats:
    """Stream ``layer_to_parquet`` output back into an FGDB layer with its curves.

    The CRS comes from the GeoParquet metadata. ``geometry_type`` defaults to the
    linear Multi* type in that metadata: FGDB has no separate curved layer types,
    so a MultiPolygon layer holds MultiSurface features (pyogrio rejects the
    curved type names). Pass ``gdb_xy_grid(source, layer)`` as ``layer_options``
    to keep the source's coordinate grid, so coordinates round-trip exactly.
    """
    linear = pq.ParquetFile(str(linear_parquet))
    geo = json.loads((linear.metadata.metadata or {})[b"geo"])
    geom_col = geo["primary_column"]
    col = geo["columns"][geom_col]
    if geometry_type is None:
        types = col["geometry_types"]
        multi = [t for t in types if t.startswith("Multi")]
        geometry_type = (multi or types or ["Unknown"])[0]
    crs = CRS.from_json_dict(col["crs"]).to_wkt() if col.get("crs") else None
    schema = pa.schema(
        pa.field(f.name, pa.binary()) if f.name == geom_col else f.remove_metadata()
        for f in linear.schema_arrow
        if f.name != "ogc_fid"
    )
    stats = CurveStats()
    batches = _recurved_batches(
        linear,
        _arcs_by_fid(arcs_parquet, batch_size),
        geom_col,
        schema,
        batch_size,
        stats,
    )
    write_arrow(
        pa.RecordBatchReader.from_batches(schema, batches),
        str(out_gdb),
        layer=layer,
        driver="OpenFileGDB",
        geometry_name=geom_col,
        geometry_type=geometry_type,
        crs=crs,
        layer_options=layer_options,
    )
    return stats
