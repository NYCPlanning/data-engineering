from pathlib import Path

from osgeo import gdal

# TODO rename below to "esri_metadata"
from dcpy.utils.geospatial.metadata import generate_metadata

from dcpy.models.data.shapefile_metadata import Metadata


def read_metadata(gdb: Path, layer: str) -> Metadata:
    with gdal.ExceptionMgr():
        layer_info = gdal.alg.vector.info(
            input=gdb,
            sql=f"GetLayerMetadata {layer}",
            features=True,
        ).Output()
    metadata_info = layer_info["layers"][0]["features"][0]["properties"]["FIELD_1"]
    metadata = Metadata.from_xml(metadata_info)

    return metadata


# gdal.UseExceptions()  # call this in actual code
# gdal.alg.raster.convert(input="in.tif", output="out.tif")


def write_metadata(
    gdb: Path | str, layer: str, metadata: Metadata, overwrite: bool = False
) -> None:
    if metadata_exists(gdb=gdb, layer=layer) and not overwrite:
        raise FileExistsError(
            "Metadata XML already exists, and overwrite is False. Nothing will be written"
        )
    if overwrite:
        remove_metadata(gdb=gdb, layer=layer)

    xml_data = metadata.to_xml()
    md = xml_data.decode("utf-8") if isinstance(xml_data, bytes) else xml_data

    with gdal.ExceptionMgr():
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
            layer_creation_option=f"DOCUMENTATION='{md}'",
            overwrite_layer=True,
        )
        # delete intermediate layer
        gdal.vector.sql(
            input_format="OpenFileGDB",
            input=gdb,
            sql=f"DROP TABLE {intermediate_layer}",
            udpate=True,
        )


def metadata_exists(gdb: Path | str, layer: str) -> bool: ...


def remove_metadata(gdb: Path | str, layer: str) -> None: ...


def _edit_metadata_inplace(gdb: Path, layer: str) -> None: ...
