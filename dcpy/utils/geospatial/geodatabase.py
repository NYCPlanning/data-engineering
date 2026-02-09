from pathlib import Path

from osgeo import gdal

# TODO rename below to "esri_metadata"
from dcpy.models.data.shapefile_metadata import Metadata


def read_metadata(gdb: Path, layer: str) -> Metadata | None:
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
    metadata = Metadata.from_xml(metadata_info)

    return metadata


def write_metadata(
    gdb: Path, layer: str, metadata: Metadata, overwrite: bool = False
) -> None:
    if metadata_exists(gdb=gdb, layer=layer) and not overwrite:
        raise FileExistsError(
            "Metadata already exists, and overwrite is False. Nothing will be written"
        )
    if overwrite:
        remove_metadata(gdb=gdb, layer=layer)

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


def _edit_layer_metadata_inplace(
    gdb: Path,
    layer: str,
    metadata: str,
) -> None:
    intermediate_layer = "intermediate_layer"
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
        layer_creation_option=f"DOCUMENTATION='{metadata}'",
        overwrite_layer=True,
    )
    # delete intermediate layer
    gdal.alg.vector.sql(
        input_format="OpenFileGDB",
        input=gdb,
        sql=f"DROP TABLE {intermediate_layer}",
        update=True,
    )
