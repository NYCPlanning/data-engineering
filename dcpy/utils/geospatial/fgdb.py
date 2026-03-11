from pathlib import Path
import tempfile
import uuid
import zipfile

from osgeo import gdal

from dcpy.models.data.shapefile_metadata import Metadata


# possible TODO: replace this fn with an Info() object, from which methods like .get_layers() can be called
def get_layers(gdb: Path) -> list[str]:
    with gdal.ExceptionMgr():
        info = gdal.alg.vector.info(
            input=gdb,
        ).Output()
    return info["rootGroup"]["layerNames"]


def read_metadata(gdb: Path, layer: str, as_string: bool = False) -> Metadata | None:
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

    with gdal.ExceptionMgr():
        _edit_layer_metadata_inplace(
            gdb=gdb,
            layer=layer,
            metadata=md,
        )


def metadata_exists(gdb: Path, layer: str) -> bool:
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
    with gdal.ExceptionMgr():
        _edit_layer_metadata_inplace(gdb=gdb, layer=layer, metadata="")


# TODO: refactor this for speed. Requirement to rezip is slow for large GDBs.
# With a 76MB zipped gdb, performance: unzip: 0.92s, gdal: 0.67s, rezip: 7.36ss
def _edit_layer_metadata_inplace(
    gdb: Path,
    layer: str,
    metadata: str,
) -> None:
    intermediate_layer = f"fc_{uuid.uuid4().hex}"

    def _edit_md(
        gdb: Path,
        layer: str,
        metadata: str,
    ) -> None:
        # create intermediate layer
        gdal.alg.vector.edit(
            input_format="OpenFileGDB",
            input=gdb,
            output=gdb,
            input_layer=layer,
            output_layer=intermediate_layer,
            update=True,
        )
        # add metadata
        gdal.alg.vector.edit(
            input_format="OpenFileGDB",
            input=gdb,
            output=gdb,
            input_layer=intermediate_layer,
            output_layer=layer,
            layer_creation_option=f"DOCUMENTATION={metadata}",
            overwrite_layer=True,
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

            with zipfile.ZipFile(gdb, "w", zipfile.ZIP_DEFLATED) as z:
                for file in uncompressed_gdb.rglob("*"):
                    z.write(
                        filename=file, arcname=file.relative_to(uncompressed_gdb.parent)
                    )
    else:
        _edit_md(gdb=gdb, layer=layer, metadata=metadata)
