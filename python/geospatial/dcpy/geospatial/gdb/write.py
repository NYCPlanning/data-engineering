"""Fold (Geo)Parquet layers into a File Geodatabase.

Parquet is the build's output format; an FGDB is packaged from it. Each layer is one
parquet file. Its geometry column and CRS come from the file's GeoParquet metadata, and
geometry is written as WKB as-is, so curved (ISO curve) WKB from ``recurve_parquet``
keeps its curves. A file without GeoParquet metadata is written as an attribute table.
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyogrio.raw import write_arrow
from pyproj import CRS

from dcpy.geospatial.gdb.fgdb import zip_gdb


@dataclass
class ParquetLayer:
    name: str
    path: Path
    # GDAL geometry type for the layer, e.g. "MultiPolygon". Defaults to the Multi* type
    # in the GeoParquet metadata. FGDB has no separate curved types: a MultiPolygon
    # layer holds MultiSurface features too.
    geometry_type: str | None = None
    # e.g. the source GDB's xy grid (curves.read.gdb_xy_grid), so coordinates
    # round-trip exactly
    layer_options: dict[str, str] = field(default_factory=dict)


def _crs_wkt(crs) -> str | None:
    """GeoParquet CRS: PROJJSON (a dict), or a string such as "EPSG:2263"."""
    if not crs:
        return None
    return (
        CRS.from_json_dict(crs) if isinstance(crs, dict) else CRS.from_user_input(crs)
    ).to_wkt()


def _geo_metadata(parquet: pq.ParquetFile) -> dict | None:
    raw = (parquet.metadata.metadata or {}).get(b"geo")
    return json.loads(raw) if raw else None


def write_parquet_layer(
    layer: ParquetLayer, gdb: Path, *, append: bool, batch_size: int = 10_000
) -> None:
    """Write one parquet file as a layer of ``gdb`` (created unless ``append``)."""
    parquet = pq.ParquetFile(str(layer.path))
    geo = _geo_metadata(parquet)
    # An all-null column has Arrow's null type, which GDAL can't make a field from;
    # write it as a string field (what the old GeoDataFrame writer did).
    schema = pa.schema(
        pa.field(f.name, pa.string())
        if pa.types.is_null(f.type)
        else f.remove_metadata()
        for f in parquet.schema_arrow
    )
    kwargs: dict = {}
    if geo is not None:
        geom_col = geo["primary_column"]
        col = geo["columns"][geom_col]
        geometry_type = layer.geometry_type
        if geometry_type is None:
            types = col.get("geometry_types") or []
            multi = [t for t in types if t.startswith("Multi")]
            geometry_type = (multi or types or ["Unknown"])[0]
        crs = col.get("crs")
        kwargs = {
            "geometry_name": geom_col,
            "geometry_type": geometry_type,
            "crs": _crs_wkt(crs),
        }
        schema = pa.schema(
            pa.field(f.name, pa.binary()) if f.name == geom_col else f for f in schema
        )
    batches = (
        b.cast(schema) if b.schema != schema else b
        for b in parquet.iter_batches(batch_size)
    )
    write_arrow(
        pa.RecordBatchReader.from_batches(schema, batches),
        str(gdb),
        layer=layer.name,
        driver="OpenFileGDB",
        append=append,
        layer_options=layer.layer_options or None,
        **kwargs,
    )


def write_gdb_from_parquet(layers: list[ParquetLayer], gdb_zip: Path) -> Path:
    """Write ``layers`` (in order) to one FGDB, zipped at ``gdb_zip``.

    The archive's top-level entry is ``<name>.gdb``, where ``name`` is ``gdb_zip``'s
    name without ``.zip`` (and without a second ``.gdb``).
    """
    stem = gdb_zip.name.removesuffix(".zip").removesuffix(".gdb")
    with tempfile.TemporaryDirectory() as tmp:
        gdb = Path(tmp) / f"{stem}.gdb"
        for i, layer in enumerate(layers):
            write_parquet_layer(layer, gdb, append=i > 0)
        gdb_zip.parent.mkdir(parents=True, exist_ok=True)
        gdb_zip.unlink(missing_ok=True)
        zip_gdb(gdb, gdb_zip)
    return gdb_zip


__all__ = ["ParquetLayer", "write_gdb_from_parquet", "write_parquet_layer"]
