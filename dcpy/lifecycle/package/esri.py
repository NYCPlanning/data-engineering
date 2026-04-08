import re
from pathlib import Path

import typer
import yaml

import dcpy.models.product.dataset.metadata as models
from dcpy.lifecycle import product_metadata
from dcpy.models.data.shapefile_metadata import Attr, Edom
from dcpy.models.product.dataset.metadata import (
    ColumnValue,
    DatasetColumn,
)
from dcpy.models.product.metadata import OrgMetadata
from dcpy.utils.geospatial import esri_metadata, fgdb
from dcpy.utils.geospatial.shapefile import Shapefile
from dcpy.utils.logging import logger


def _parse_raw_column(field_raw) -> models.Column:
    """
    Parse a column value from ESRI metadata pdfs
    example value: 'Field AssemDist,Alias ASSEMDIST,Data type String,Width 2,,,Field description\nState Assembly District Number'
    """
    parsed_field_info = {
        "description": "CHANGE_ME",
        "name": "CHANGE_ME",
        "display_name": "CHANGE_ME",
        "data_type": "CHANGE_ME",
    }

    field_token = "Field "
    alias_token = "Alias "
    type_token = "Data type"
    desc_token = "Field description\n"
    iters = 0
    while True:
        next_comma = field_raw.find(",")
        next_chunk = field_raw[0:next_comma]

        PROBABLY_AN_INFINITE_LOOP_COUNTER = 100
        if next_comma == -1 or iters > PROBABLY_AN_INFINITE_LOOP_COUNTER:
            if next_chunk.startswith(desc_token):
                # Description always comes last in the metadata
                parsed_field_info["description"] = field_raw[len(desc_token) :]
            break

        if next_chunk.startswith(field_token) and not next_chunk.startswith(
            field_token + "description"
        ):
            parsed_field_info["name"] = next_chunk[len(field_token) :]
        elif next_chunk.startswith(alias_token):
            parsed_field_info["display_name"] = next_chunk[len(alias_token) :]
        elif next_chunk.startswith(type_token):
            parsed_field_info["data_type"] = next_chunk[len(type_token) :]

        field_raw = field_raw[next_comma + 1 :]
        iters = iters + 1

    return models.Column(**parsed_field_info)  # type: ignore


app = typer.Typer()


@app.command("parse_pdf_text")
def parse_pdf_text(
    esri_pdf_path: Path,
    output_path: Path = typer.Option(
        None,
        "--output-path",
        "-o",
        help="Path to export the metadata to",
    ),
):
    text = open(esri_pdf_path, "r").read()
    text = (
        text.replace("\n_\n", "_")
        .replace("\n►\n*\n", ",")
        .replace("\n*\n", ",")
        .replace("Scale 0\n", ",")
        .replace("Precision 0,", ",")
    )
    fields_split_messy = re.split("\nHide Field .* ▲\n", text)[1:]
    fields = []
    for f in fields_split_messy:
        try:
            fields.append(_parse_raw_column(f).model_dump(exclude_none=True))
        except Exception as e:
            logger.error(str(e))

    output_path = output_path or Path("columns.yml")
    with open(output_path, "w") as outfile:
        yaml.dump(fields, outfile, sort_keys=False)


@app.command("write_metadata")
def _write_metadata(
    product_name: str,
    dataset_name: str,
    path: Path,
    layer: str,
    org_md_path: Path | None = typer.Option(
        None,
        "--org-md-path",
        help="Path to organizational metadata",
    ),
    zip_subdir: str | None = typer.Option(
        None,
        "--zip-subdir",
        help="Directory structure within zip file, if relevant",
    ),
):
    write_metadata(
        product_name=product_name,
        dataset_name=dataset_name,
        path=path,
        layer=layer,
        zip_subdir=zip_subdir,
        org_md=org_md_path,
    )
    logger.info(f"Wrote metadata to {layer} in {path}")


def write_metadata(
    product_name: str,
    dataset_name: str,
    path: Path,
    layer: str,
    zip_subdir: str | None,
    org_md: Path | OrgMetadata | None,  # Allow passing OrgMetadata for testing purposes
):
    """Write product metadata to the shapefile metadata XML. Generates a new XML with defaults,
    and applies additional product-specific values.

    Args:
        product_name (str): Name of product. e.g. "lion"
        dataset_name (str): Name of dataset within a product. e.g. "pseudo-lots"
        path (Path): Path to parent directory or zip file containing shapefile, or geodatabase.
        layer (str): Shapefile or feature class name.
        zip_subdir (str | None): Internal path if shp is nested within a zip file.
            Must be None when path is a file geodatabase.
        org_md (Path | OrgMetadata | None): Metadata reference used to populate shapefile metadata.
    """
    if isinstance(org_md, Path) or not org_md:
        org_md = product_metadata.load(org_md_path_override=org_md)

    product_md = org_md.product(product_name).dataset(dataset_name)

    metadata = esri_metadata.generate_metadata()

    # Set dataset-level values
    # TODO: define DCP organizationally required metadata fields
    metadata.md_hr_lv_name = product_md.attributes.display_name
    metadata.data_id_info.id_abs = product_md.attributes.description
    metadata.data_id_info.other_keys.keyword = product_md.attributes.tags
    metadata.data_id_info.search_keys.keyword = product_md.attributes.tags

    metadata.eainfo.detailed.name = product_md.id
    metadata.eainfo.detailed.enttyp.enttypl.value = product_md.id
    metadata.eainfo.detailed.enttyp.enttypt.value = "Feature Class"

    # Build attribute metadata for each column
    metadata.eainfo.detailed.attr = [
        _create_attr_metadata(column) for column in product_md.columns
    ]

    if ".gdb" in path.suffixes:
        if zip_subdir is not None:
            raise ValueError(
                "Nested zipped GDBs are not supported. The GDB must be at the top level of the zip."
            )
        fgdb.write_metadata(gdb=path, layer=layer, metadata=metadata, overwrite=True)

    elif ".shp" in path.suffixes or layer.endswith(".shp"):
        shp = Shapefile(path=path, shp_name=layer, zip_subdir=zip_subdir)
        shp.write_metadata(metadata, overwrite=True)


def _create_attr_metadata(column: DatasetColumn) -> Attr:
    """Create an Attr metadata object from a column specification."""
    attr = Attr()

    attr.attrlabl.value = "FID" if column.id == "uid" else column.id
    attr.attalias.value = "FID" if column.name == "uid" else column.name
    attr.attrdef.value = column.description

    # TODO: define column-level defaults (e.g. attrdefs = 'Esri' if column.name == 'uid')
    # TODO: map DCP types to Esri types (e.g. attrtype = 'OID' if column.name == 'uid') Note DCP types != Esri types
    # attr.attrtype.value = column.data_type
    # attr.attwidth.value = None
    # attr.atprecis.value = None
    # attr.attscale.value = None
    # attr.attrdefs.value = ""

    # Handle domain values if present
    if hasattr(column, "values") and column.values:
        attr.attrdomv.edom = [_create_edom_metadata(value) for value in column.values]

    # TODO: handle 'attrdomv.udom', with other esri value defaults
    return attr


def _create_edom_metadata(column_value: ColumnValue) -> Edom:
    """Create an Edom metadata object from a column value specification."""
    edom = Edom()
    edom.edomv = column_value.value
    edom.edomvd = column_value.description

    return edom
