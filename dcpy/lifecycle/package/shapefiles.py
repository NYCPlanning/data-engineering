from pathlib import Path
import xml.etree.ElementTree as ET

from dcpy.models.product.dataset.metadata_v2 import (
    Metadata,
    DatasetAttributes,
    DatasetColumn,
    ColumnValue,
)

_shapefile_to_dcpy_types = {
    "OID": "integer",
    "Integer": "integer",
    "SmallInteger": "integer",
    "Double": "decimal",
    "Float": "decimal",
    "String": "text",
    "Date": "datetime",
    "Geometry": "geometry",
    "Boolean": "boolean",
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
