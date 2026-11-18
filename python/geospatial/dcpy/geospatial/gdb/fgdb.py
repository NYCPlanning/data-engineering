import re
import shutil
import struct
import tempfile
import zipfile
from pathlib import Path

import pyogrio

from dcpy.geospatial.shapefile_metadata import Metadata


def resolve_gdb_path(gdb: Path | str) -> str:
    """A path string pyogrio/GDAL can actually open for this .gdb or .zip.

    pyogrio's own native zip-GDB handling only kicks in when the *filename*
    itself ends in ".gdb.zip" - confirmed empirically: identical zip bytes
    open fine as "x.gdb.zip" and fail as plain "x.zip", regardless of
    whether the .gdb inside is at the archive root or nested in a folder
    (real prod deliveries nest it, e.g. a LION zip's .gdb lives at
    "lion/lion.gdb/...", not the zip root - our own zip_gdb() always writes
    it at the root, but under whatever filename the recipe declares, which
    for LION is "nyclion_*.zip", not "*.gdb.zip"). Returns the path as-is
    when pyogrio can already open it; otherwise scans the zip for a *.gdb
    entry and returns an explicit /vsizip/ path to it.
    """
    path_str = str(gdb)
    try:
        pyogrio.list_layers(path_str)
    except Exception:
        inner = _find_inner_gdb(Path(gdb))
        if inner is None:
            raise
        return inner
    return path_str


def layer_geometry_types(gdb: Path | str) -> dict[str, str | None]:
    """Layer name -> geometry type name (None for a non-spatial table).

    Accepts anything resolve_gdb_path does: a .gdb directory, or a .zip
    containing one - at its top level or nested, and regardless of whether
    the zip's own filename ends in ".gdb.zip".
    """
    rows = pyogrio.list_layers(resolve_gdb_path(gdb))
    return {str(row[0]): (str(row[1]) if row[1] else None) for row in rows}


def _find_inner_gdb(zip_path: Path) -> str | None:
    """The /vsizip/<abs zip path>/<inner .gdb path> VSI path for the first
    *.gdb directory found anywhere in the zip (not just at its top level), or
    None if zip_path isn't a zip or contains no .gdb."""
    if not zipfile.is_zipfile(zip_path):
        return None
    with zipfile.ZipFile(zip_path) as z:
        gdb_dirs = sorted(
            {
                "/".join(parts[: i + 1])
                for name in z.namelist()
                for parts in [name.split("/")]
                for i, part in enumerate(parts)
                if part.endswith(".gdb")
            }
        )
    if not gdb_dirs:
        return None
    return f"/vsizip/{zip_path.resolve()}/{gdb_dirs[0]}"


def zip_gdb(gdb_dir: Path, zip_path: Path) -> None:
    """Zip a .gdb directory into zip_path, with the .gdb directory itself as
    the archive's top-level entry - so it unpacks the same way a normal GDB
    export ships (<name>.gdb.zip containing <name>.gdb/...), and downstream
    tools that read a zipped GDB natively (pyogrio, GDAL) can open it as-is.
    """
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for file in gdb_dir.rglob("*"):
            z.write(file, arcname=file.relative_to(gdb_dir.parent))


# possible TODO: replace this fn with an Info() object, from which methods like .get_layers() can be called
def get_layers(gdb: Path) -> list[str]:
    # Delegates to layer_geometry_types (pyogrio-based, already proven against
    # both classic and modern FileGDB formats via compare_gdb.py) rather than
    # GDAL's vector.info() algorithm directly - that API's Output()["rootGroup"]
    # only exists for geodatabases with a hierarchical group structure (modern/
    # 10.x+ FileGDBs). A "classic" pre-10.x FileGDB (e.g. CSCL's district gdb)
    # has no rootGroup at all;
    # vector.info() returns a flat "layers" list instead, so an
    # info["rootGroup"]["layerNames"] lookup raises KeyError('rootGroup') for
    # every such file (e.g. v26C_Districts.gdb.zip).
    return list(layer_geometry_types(gdb).keys())


def resolve_layer(gdb: Path, layer: str | None = None) -> str:
    layers = get_layers(gdb)
    if layer is not None:
        if layer not in layers:
            raise LookupError(
                f"Layer '{layer}' not found in {gdb}. Found layers: {layers}."
            )
        return layer
    if len(layers) != 1:
        raise ValueError(
            f"{gdb} has {len(layers)} layers ({layers}); layer must be specified to disambiguate."
        )
    return layers[0]


def _layer_metadata_xml(gdb: Path, layer: str) -> str:
    frame = pyogrio.read_dataframe(
        resolve_gdb_path(gdb), sql=f"GetLayerMetadata {layer}"
    )
    return frame.iloc[0, 0] or ""


def read_metadata(gdb: Path, layer: str) -> Metadata | None:
    metadata_info = _layer_metadata_xml(gdb, layer)
    return Metadata.from_xml(metadata_info) if metadata_info else None


def write_metadata(
    gdb: Path, layer: str, metadata: Metadata, overwrite: bool = False
) -> None:
    if metadata_exists(gdb=gdb, layer=layer) and not overwrite:
        raise FileExistsError(
            "Metadata already exists, and overwrite is False. Nothing will be written"
        )

    xml_data = metadata.to_xml()
    md = xml_data.decode("utf-8") if isinstance(xml_data, bytes) else xml_data

    _set_layer_documentation(gdb=gdb, layer=layer, metadata=md)


def metadata_exists(gdb: Path, layer: str) -> bool:
    return bool(_layer_metadata_xml(gdb, layer))


def remove_metadata(gdb: Path, layer: str) -> None:
    _set_layer_documentation(gdb=gdb, layer=layer, metadata="")


def _set_layer_documentation(gdb: Path, layer: str, metadata: str) -> None:
    if zipfile.is_zipfile(gdb):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with zipfile.ZipFile(gdb, "r") as z:
                z.extractall(tmp_dir)
            uncompressed_gdb = Path(tmp_dir) / gdb.stem
            _rewrite_with_documentation(uncompressed_gdb, {layer: metadata})
            zip_gdb(uncompressed_gdb, gdb)
    else:
        _rewrite_with_documentation(Path(gdb), {layer: metadata})


# ISO WKB curve types: CircularString, CompoundCurve, CurvePolygon, MultiCurve,
# MultiSurface (plus 1000/2000/3000 for Z/M/ZM)
_CURVE_WKB_TYPES = {8, 9, 10, 11, 12}


def _has_curves(gdb: Path, layer: str) -> bool:
    from pyogrio.raw import open_arrow

    with open_arrow(str(gdb), layer=layer, columns=[], use_pyarrow=True) as (
        meta,
        reader,
    ):
        if not meta["geometry_type"]:
            return False
        geometry = meta["geometry_name"] or "wkb_geometry"
        for batch in reader:
            for wkb in batch.column(geometry).to_pylist():
                if wkb:
                    order = "<I" if wkb[0] == 1 else ">I"
                    if struct.unpack(order, wkb[1:5])[0] % 1000 in _CURVE_WKB_TYPES:
                        return True
    return False


def _client_version(gdb: Path, layer: str) -> tuple[int, ...]:
    """The oldest ArcGIS client the layer declares it needs, e.g. (13, 2) for
    ArcGIS Pro 3.2. (0,) when it doesn't say."""
    from pyogrio.raw import read_arrow

    _, table = read_arrow(
        str(gdb),
        layer="GDB_Items",
        columns=["Name", "Definition"],
        read_geometry=False,
    )
    for name, xml in zip(table["Name"].to_pylist(), table["Definition"].to_pylist()):
        if name == layer and xml:
            found = re.search(r"<RequiredGeodatabaseClientVersion>([\d.]+)<", xml)
            if found:
                return tuple(int(part) for part in found.group(1).split("."))
    return (0,)


def _rewrite_with_documentation(gdb: Path, documentation: dict[str, str]) -> None:
    """Rewrite every layer of ``gdb`` with its DOCUMENTATION set from ``documentation``,
    keeping the existing value for layers not in it.

    OpenFileGDB only takes DOCUMENTATION when a layer is created, so changing it means
    writing the layer again. Each layer keeps its xy grid so coordinates round-trip
    exactly. Writing to a new gdb rather than replacing layers in place keeps the
    original layer order.
    """
    from pyogrio.raw import open_arrow, write_arrow

    from dcpy.geospatial.curves.read import gdb_xy_grid

    layers = get_layers(gdb)
    unknown = set(documentation) - set(layers)
    if unknown:
        raise LookupError(f"Layers {sorted(unknown)} not found in {gdb}: {layers}")
    # GDAL's FGDB reader can return a hole of a curved polygon as a separate,
    # overlapping part, and writing that back fills the hole. Every layer is
    # rewritten, so any curved layer blocks the stamp.
    curved = [layer for layer in layers if _has_curves(gdb, layer)]
    if curved:
        raise ValueError(
            f"Can't stamp metadata after the fact on {gdb}: layers {curved} have "
            "curved geometry, which doesn't survive a rewrite. Stamp at build time "
            "instead (custom.product_metadata on the recipe's gdb export)."
        )
    with tempfile.TemporaryDirectory() as tmp_dir:
        out = Path(tmp_dir) / gdb.name
        for i, layer in enumerate(layers):
            xml = (
                documentation[layer]
                if layer in documentation
                else _layer_metadata_xml(gdb, layer)
            )
            options = gdb_xy_grid(gdb, layer)
            # keeps BigInteger and DateOnly fields, which otherwise come back as Double
            # and Date. Only where the source needs it: it makes the layer unreadable
            # before ArcGIS Pro 3.2.
            if _client_version(gdb, layer) >= (13, 2):
                options["TARGET_ARCGIS_VERSION"] = "ARCGIS_PRO_3_2_OR_LATER"
            if xml:
                options["DOCUMENTATION"] = xml
            with open_arrow(str(gdb), layer=layer, use_pyarrow=True) as (meta, reader):
                if meta["geometry_name"]:
                    options["GEOMETRY_NAME"] = meta["geometry_name"]
                geometry = (
                    {
                        "geometry_name": meta["geometry_name"] or "wkb_geometry",
                        "geometry_type": meta["geometry_type"],
                        "crs": meta["crs"],
                    }
                    if meta["geometry_type"]
                    else {}
                )
                write_arrow(
                    reader,
                    str(out),
                    layer=layer,
                    driver="OpenFileGDB",
                    append=i > 0,
                    layer_options=options or None,
                    **geometry,
                )
        shutil.rmtree(gdb)
        shutil.move(out, gdb)
