# TODO: Move this to a utils shapefile module.


import xml.etree.ElementTree as ET
from pathlib import Path

import typer

from dcpy.lifecycle import product_metadata
from dcpy.models.product.dataset.metadata import (
    COLUMN_TYPES,
    ColumnValue,
    DatasetAttributes,
    DatasetColumn,
    Metadata,
)

from dcpy.models.product.metadata import OrgMetadata
from dcpy.utils.geospatial import shapefile as shp_utils
from dcpy.utils.geospatial.shapefile import Shapefile
from dcpy.utils.logging import logger

_shapefile_to_dcpy_types: dict[str, COLUMN_TYPES] = {
    "OID": "integer",
    "Integer": "integer",
    "SmallInteger": "integer",
    "Double": "decimal",
    "Float": "decimal",
    "String": "text",
    "Date": "datetime",
    "Geometry": "geometry",
    "Boolean": "bool",
}


def _stripped_text(s) -> str:
    return s.text.strip() if (s is not None and s.text) else ""


def parse_shapefile_metadata(file_path: Path) -> Metadata:
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Extract relevant information from the XML
    title = _stripped_text(root.find(".//title"))
    abstract = _stripped_text(root.find(".//abstract"))
    purpose = _stripped_text(root.find(".//purpose"))
    update_frequency = _stripped_text(root.find(".//update"))

    # Extract keywords
    keywords = [_stripped_text(keyword) for keyword in root.findall(".//themekey")]

    columns = []
    for attr in root.findall(".//attr"):
        column_name = _stripped_text(attr.find("attrlabl"))
        if column_name == "FID":
            continue  # not an actual column

        column_display_name = _stripped_text(attr.find("attalias")) or "<FILL ME IN>"

        # TODO: test that this actually works
        if column_name == "Shape":
            column_name = "the_geom"
            column_display_name = "the_geom"

        column_desc = (
            _stripped_text(attr.find("attrdef"))
            if attr.find("attrdef") is not None
            else ""
        )
        column_type = (
            _stripped_text(attr.find("attrtype"))
            if attr.find("attrtype") is not None
            else "string"
        )

        # Parse standard values if they exist
        standard_values = []
        attrdomv = attr.find("attrdomv")
        if attrdomv is not None:
            for edom in attrdomv.findall("edom"):
                value = _stripped_text(edom.find("edomv"))
                description = (
                    _stripped_text(edom.find("edomvd"))
                    if edom.find("edomvd") is not None
                    else ""
                )
                standard_values.append(
                    ColumnValue(value=value, description=description)
                )

        column = DatasetColumn(
            id=column_name.lower().replace(" ", "_"),
            name=column_display_name,
            description=column_desc,
            values=standard_values,
            data_type=_shapefile_to_dcpy_types.get(column_type, "text"),
        )
        columns.append(column)

    attributes = DatasetAttributes(
        display_name=title,
        description=abstract,
        each_row_is_a="",
        publishing_purpose=purpose,
        publishing_frequency=update_frequency,
        tags=keywords,
    )

    metadata = Metadata(
        id=title.lower().replace(" ", "_"),
        attributes=attributes,
        columns=columns,
        files=[],
        destinations=[],
    )

    return metadata


app = typer.Typer()


@app.command("to_metadata")
def _write_metadata(
    shp_xml_path: Path,
    output_path: Path = typer.Option(
        None,
        "--output-path",
        "-o",
        help="Output Path. Defaults to ./data_dictionary.xlsx",
    ),
):
    out_path = output_path or Path("./metadata.yml")
    parse_shapefile_metadata(shp_xml_path).write_to_yaml(out_path)
    logger.info(f"Wrote metadata to {out_path}")


@app.command("write_metadata")
def _write_shapefile_xml_metadata(
    org_md: OrgMetadata,  # Should this be optional if the underlying arg is optional?
    product_name: str,
    dataset_name: str,
    path: Path,
    shp_name: str,
    zip_subdir: str | None = typer.Option(
        None,
        "--zip-subdir",
        help="Directory structure within zip file, if relevant",
    ),
):
    write_shapefile_xml_metadata(
        product_name=product_name,
        dataset_name=dataset_name,
        path=path,
        shp_name=shp_name,
        zip_subdir=zip_subdir,
        org_md=org_md,
    )
    logger.info(f"Wrote metadata to {shp_name} in {path}")


# TODO - decide whether to automatically detect zip files or not at this level
def write_shapefile_xml_metadata(
    product_name: str,
    dataset_name: str,
    path: Path,
    shp_name: str,
    zip_subdir: str | None,
    org_md: OrgMetadata | None,
):
    org_md = org_md or product_metadata.load()
    product_md = org_md.product(product_name).dataset(dataset_name)

    metadata = shp_utils.generate_metadata()

    metadata.md_hr_lv_name = product_md.attributes.display_name
    metadata.md_stan_name = "ArcGIS Metadata"
    metadata.md_stan_ver = 1.0
    metadata.data_id_info.id_abs = product_md.attributes.description
    metadata.data_id_info.other_keys.keyword = product_md.attributes.tags
    metadata.data_id_info.search_keys.keyword = product_md.attributes.tags

    shp = Shapefile(path=path, shp_name=shp_name, zip_subdir=zip_subdir)
    shp.write_metadata(metadata, overwrite=True)
