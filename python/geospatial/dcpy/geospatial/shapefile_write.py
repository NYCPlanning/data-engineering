"""Write a (Geo)Parquet layer as a zipped shapefile.

The parquet is the build's output; a shapefile is packaged from it, as an FGDB is
(``gdb.write``). Shapefiles can't hold curves, so curved (ISO curve) WKB is linearized
on the way. Field names are at most 10 characters: by default each column is cut the
way ArcGIS and GDAL cut it (first 10 characters; a collision gets the first 8 plus
``_1``, ``_2``, ...), so ``CENSUSBLOCK_2000`` and ``CENSUSBLOCK_2000_SUFFIX`` become
``CENSUSBLOC`` and ``CENSUSBL_1``.

Like prod's shapefiles, ours carry no ``.cpg`` and a zero DBF language driver ID, so
readers fall back to their default codepage (Latin-1 / the system's for ArcGIS). Text
is therefore limited to ASCII, which reads the same either way; anything else is
refused rather than written as UTF-8 that would be misread.
"""

from __future__ import annotations

import tempfile
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyogrio.raw import write_arrow

from dcpy.geospatial.convert import SHAPEFILE_NAME_LIMIT, SHAPEFILE_PARTS
from dcpy.geospatial.curves.wkb import CurveStats, split_curve_wkb
from dcpy.geospatial.gdb.write import _crs_wkt, _geo_metadata, _layer_type

# ISO WKB curve type codes (2D, Z, M and ZM variants share the last two digits)
_CURVE_CODES = {8, 9, 10, 11, 12}


def esri_field_names(columns: list[str]) -> dict[str, str]:
    """Map each column to a shapefile field name, as ArcGIS/GDAL truncate them."""
    names: dict[str, str] = {}
    used: set[str] = set()
    for column in columns:
        candidate = column[:SHAPEFILE_NAME_LIMIT]
        n = 0
        # DBF field names are matched case-insensitively
        while candidate.lower() in used:
            n += 1
            suffix = f"_{n}"
            candidate = column[: SHAPEFILE_NAME_LIMIT - len(suffix)] + suffix
        names[column] = candidate
        used.add(candidate.lower())
    return names


# FGDB's geometry-derived fields, and what ArcGIS names them in a shapefile (last, in
# this order)
_FGDB_SHAPE_FIELDS = {"shape_length": "Shape_Leng", "shape_area": "Shape_Area"}


def fgdb_field_names(columns: list[str]) -> dict[str, str]:
    """Shapefile field names for an FGDB layer's columns, as ArcGIS exports one:
    ``esri_field_names``, but SHAPE_Length / SHAPE_Area become Shape_Leng /
    Shape_Area and go last."""
    shape = sorted(
        (c for c in columns if c.lower() in _FGDB_SHAPE_FIELDS),
        key=lambda c: list(_FGDB_SHAPE_FIELDS).index(c.lower()),
    )
    names = esri_field_names([c for c in columns if c not in shape])
    for c in shape:
        names[c] = _FGDB_SHAPE_FIELDS[c.lower()]
    if len({n.lower() for n in names.values()}) < len(names):
        raise ValueError(f"Duplicate shapefile field names: {names}")
    return names


def _validate_field_names(names: dict[str, str]) -> None:
    too_long = [n for n in names.values() if len(n) > SHAPEFILE_NAME_LIMIT]
    if too_long:
        raise ValueError(
            f"Shapefile field names longer than {SHAPEFILE_NAME_LIMIT}: {too_long}"
        )
    lowered = [n.lower() for n in names.values()]
    dupes = sorted({n for n in lowered if lowered.count(n) > 1})
    if dupes:
        raise ValueError(f"Duplicate shapefile field names: {dupes}")


def _is_text(t: pa.DataType) -> bool:
    if pa.types.is_dictionary(t):
        t = t.value_type
    return (
        pa.types.is_string(t)
        or pa.types.is_large_string(t)
        or pa.types.is_string_view(t)
    )


def _validate_ascii(parquet: pq.ParquetFile, name: str, columns: list[str]) -> None:
    """Raise if any text value in ``columns`` isn't ASCII."""
    text = [c for c in columns if _is_text(parquet.schema_arrow.field(c).type)]
    if not text:
        return
    counts = dict.fromkeys(text, 0)
    samples: dict[str, str] = {}
    for batch in parquet.iter_batches(columns=text):
        for c in text:
            values = batch.column(c)
            if pa.types.is_dictionary(values.type):
                values = values.cast(values.type.value_type)
            # nulls are dropped by the filter
            non_ascii = pc.filter(values, pc.invert(pc.string_is_ascii(values)))
            if len(non_ascii):
                counts[c] += len(non_ascii)
                samples.setdefault(c, non_ascii[0].as_py())
    bad = {c: n for c, n in counts.items() if n}
    if bad:
        detail = "; ".join(
            f"{c}: {n} row(s), e.g. {samples[c]!r}" for c, n in bad.items()
        )
        raise ValueError(
            f"Shapefile {name} would have non-ASCII text, which it can't declare "
            f"(no .cpg): {detail}"
        )


def _is_curved(wkb: bytes) -> bool:
    """Whether ISO WKB is a curve type. A curve anywhere in a (multi)polygon or
    (multi)linestring makes the top-level type one (CurvePolygon, MultiSurface, ...)."""
    code = int.from_bytes(wkb[1:5], "little" if wkb[0] == 1 else "big") % 1000
    return code in _CURVE_CODES


def _linearize(column: pa.Array, stats: CurveStats) -> pa.Array:
    out: list[bytes | None] = []
    for wkb in column.to_pylist():
        if wkb is None:
            out.append(None)
        else:
            linear, _ = split_curve_wkb(wkb, stats=stats)
            out.append(linear)
    return pa.array(out, pa.binary())


def write_shapefile_from_parquet(
    parquet_path: Path,
    shp_zip: Path,
    *,
    name: str | None = None,
    field_names: dict[str, str] | None = None,
    zip_folder: str | None = None,
    metadata_xml: str | None = None,
    batch_size: int = 10_000,
) -> int:
    """Write the GeoParquet at ``parquet_path`` as shapefile ``name`` (default: the
    zip's stem), zipped at ``shp_zip``. Returns the feature count.

    Args:
        field_names: Source column -> shapefile field name, in output order; columns
            not in it are dropped. Defaults to every attribute column, named by
            ``esri_field_names``.
        zip_folder: Folder inside the zip holding the shapefile parts (default: none).
        metadata_xml: Esri metadata, written as ``<name>.shp.xml``.
    """
    name = name or shp_zip.name.removesuffix(".zip")
    parquet = pq.ParquetFile(str(parquet_path))
    geo = _geo_metadata(parquet)
    if geo is None:
        raise ValueError(f"{parquet_path} has no GeoParquet metadata")
    geom_col = geo["primary_column"]
    attributes = [f.name for f in parquet.schema_arrow if f.name != geom_col]
    if field_names is None:
        field_names = esri_field_names(attributes)
    missing = sorted(set(field_names) - set(attributes))
    if missing:
        raise ValueError(f"Columns not in {parquet_path.name}: {missing}")
    _validate_field_names(field_names)
    non_ascii_names = [n for n in field_names.values() if not n.isascii()]
    if non_ascii_names:
        raise ValueError(f"Shapefile {name}: non-ASCII field names {non_ascii_names}")
    _validate_ascii(parquet, name, list(field_names))

    source = parquet.schema_arrow
    # An all-null column has Arrow's null type, which GDAL can't make a field from
    schema = pa.schema(
        [
            pa.field(
                field_names[c],
                pa.string()
                if pa.types.is_null(source.field(c).type)
                else source.field(c).type,
            )
            for c in field_names
        ]
        + [pa.field(geom_col, pa.binary())]
    )
    geometry_type = _layer_type(geo)
    curved = geo["columns"][geom_col].get("curved", False)
    stats = CurveStats()

    def batches():
        for batch in parquet.iter_batches(batch_size, columns=[*field_names, geom_col]):
            geoms = batch.column(geom_col).cast(pa.binary())
            if curved or any(
                w is not None and _is_curved(w) for w in geoms.to_pylist()
            ):
                geoms = _linearize(geoms, stats)
            yield pa.RecordBatch.from_arrays(
                [
                    batch.column(c).cast(schema.field(field_names[c]).type)
                    for c in field_names
                ]
                + [geoms],
                schema=schema,
            )

    with tempfile.TemporaryDirectory() as tmp:
        shp = Path(tmp) / f"{name}.shp"
        write_arrow(
            pa.RecordBatchReader.from_batches(schema, batches()),
            str(shp),
            driver="ESRI Shapefile",
            geometry_name=geom_col,
            # a re-curved file declares none: the driver takes the first feature's
            geometry_type=geometry_type,
            crs=_crs_wkt(geo["columns"][geom_col].get("crs")),
        )
        # pyogrio always sets ENCODING=UTF-8 for shapefiles (an empty ENCODING is
        # replaced), so the .cpg can't be suppressed at write time; leave it out of the
        # zip instead. With ASCII-only text the bytes are the same in any encoding.
        if metadata_xml is not None:
            shp.with_name(f"{name}.shp.xml").write_text(metadata_xml, encoding="utf-8")
        shp_zip.parent.mkdir(parents=True, exist_ok=True)
        shp_zip.unlink(missing_ok=True)
        prefix = f"{zip_folder}/" if zip_folder else ""
        with zipfile.ZipFile(shp_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            for suffix in (*SHAPEFILE_PARTS, ".shp.xml"):
                if suffix == ".cpg":
                    continue
                part = shp.with_name(f"{name}{suffix}")
                if part.exists():
                    zf.write(part, f"{prefix}{part.name}")
    return parquet.metadata.num_rows


__all__ = ["esri_field_names", "fgdb_field_names", "write_shapefile_from_parquet"]
