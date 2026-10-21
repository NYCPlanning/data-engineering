import tempfile
import uuid
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
    # Imported lazily: eagerly importing osgeo.gdal at module load time (this
    # module is imported as part of the dcpy CLI bootstrap) triggers GDAL's
    # driver registration, which collides with pyarrow's Arrow filesystem
    # registration ("Attempted to register factory for scheme 'file' but that
    # scheme is already registered") if pyarrow is used anywhere else in the
    # same process.
    from osgeo import gdal

    with gdal.ExceptionMgr():
        info = gdal.alg.vector.info(
            input=gdb,
        ).Output()
    return info["rootGroup"]["layerNames"]


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


def read_metadata(gdb: Path, layer: str, as_string: bool = False) -> Metadata | None:
    from osgeo import gdal  # see get_layers() for why this is a local import

    with gdal.ExceptionMgr():
        layer_info = gdal.alg.vector.info(
            input=gdb,
            sql=f"GetLayerMetadata {layer}",
            features=True,
        ).Output()
    metadata_info = layer_info["layers"][0]["features"][0]["properties"][
        "FIELD_1"
    ].strip("'")

    if not metadata_info:
        return None
    if as_string is True:
        return metadata_info
    else:
        return Metadata.from_xml(metadata_info)


def write_metadata(
    gdb: Path, layer: str, metadata: Metadata, overwrite: bool = False
) -> None:
    if metadata_exists(gdb=gdb, layer=layer) and not overwrite:
        raise FileExistsError(
            "Metadata already exists, and overwrite is False. Nothing will be written"
        )

    xml_data = metadata.to_xml()
    md = xml_data.decode("utf-8") if isinstance(xml_data, bytes) else xml_data

    from osgeo import gdal  # see get_layers() for why this is a local import

    with gdal.ExceptionMgr():
        _edit_layer_metadata_inplace(
            gdb=gdb,
            layer=layer,
            metadata=md,
        )


def metadata_exists(gdb: Path, layer: str) -> bool:
    from osgeo import gdal  # see get_layers() for why this is a local import

    with gdal.ExceptionMgr():
        layer_info = gdal.alg.vector.info(
            input=gdb,
            sql=f"GetLayerMetadata {layer}",
            features=True,
        ).Output()
    metadata_info = layer_info["layers"][0]["features"][0]["properties"][
        "FIELD_1"
    ].strip("'")

    if metadata_info:
        return True
    return False


def remove_metadata(gdb: Path, layer: str) -> None:
    from osgeo import gdal  # see get_layers() for why this is a local import

    with gdal.ExceptionMgr():
        _edit_layer_metadata_inplace(gdb=gdb, layer=layer, metadata="")


# TODO: refactor this for speed. Requirement to rezip is slow for large GDBs.
# With a 76MB zipped gdb, performance: unzip: 0.92s, gdal: 0.67s, rezip: 7.36ss
def _edit_layer_metadata_inplace(
    gdb: Path,
    layer: str,
    metadata: str,
) -> None:
    from osgeo import gdal  # see get_layers() for why this is a local import

    intermediate_layer = f"fc_{uuid.uuid4().hex}"

    def _edit_md(
        gdb: Path,
        layer: str,
        metadata: str,
    ) -> None:
        # GDAL 3.13 requires active_layer alongside output_layer (to disambiguate which
        # input layer to copy when the FGDB has several), and now rejects output_layer
        # naming an already-existing layer even with overwrite_layer=True ("--output-layer
        # name must be ... different from an existing layer"). So: copy `layer` to a new
        # name, drop the original to free up the name, then copy back to the original name
        # (safe now that it's free) with the metadata attached - layer_creation_option has
        # to be on this final copy, since an intermediate copy's metadata doesn't survive
        # a subsequent rename-via-edit.
        gdal.alg.vector.edit(
            input_format="OpenFileGDB",
            input=gdb,
            output=gdb,
            input_layer=layer,
            output_layer=intermediate_layer,
            active_layer=layer,
            update=True,
        )
        gdal.alg.vector.sql(
            input_format="OpenFileGDB",
            input=gdb,
            sql=f"DROP TABLE {layer}",
            update=True,
        )
        gdal.alg.vector.edit(
            input_format="OpenFileGDB",
            input=gdb,
            output=gdb,
            input_layer=intermediate_layer,
            output_layer=layer,
            active_layer=intermediate_layer,
            layer_creation_option=f"DOCUMENTATION={metadata}",
            update=True,
        )
        # delete intermediate layer
        gdal.alg.vector.sql(
            input_format="OpenFileGDB",
            input=gdb,
            sql=f"DROP TABLE {intermediate_layer}",
            update=True,
        )

    if zipfile.is_zipfile(gdb):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with zipfile.ZipFile(gdb, "r") as z:
                z.extractall(tmp_dir)
                uncompressed_gdb = tmp_dir / Path(gdb.stem)

            _edit_md(gdb=uncompressed_gdb, layer=layer, metadata=metadata)

            zip_gdb(uncompressed_gdb, gdb)
    else:
        _edit_md(gdb=gdb, layer=layer, metadata=metadata)
