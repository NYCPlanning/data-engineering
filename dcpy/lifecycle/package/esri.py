import re
from pathlib import Path

import typer
import yaml

import dcpy.models.product.dataset.metadata as models
from dcpy.lifecycle import product_metadata
from dcpy.models.data.shapefile_metadata import Attr, Edom, Udom
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
    layer: str | None = typer.Argument(None),
    file_id: str = typer.Argument(...),
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
        file_id=file_id,
        zip_subdir=zip_subdir,
        org_md=org_md_path,
    )


def write_metadata(
    product_name: str,
    dataset_name: str,
    path: Path,
    layer: str | None,
    file_id: str,  # refers to product md
    zip_subdir: str | None,
    org_md: Path | OrgMetadata | None,  # Allow passing OrgMetadata for testing purposes
):
    """Write product metadata to an Esri metadata XML embedded in a shapefile or geodatabase.
    Generates a new XML with defaults and applies product-specific values.

    Args:
        product_name (str): Name of product. e.g. "lion"
        dataset_name (str): Name of dataset within a product. e.g. "pseudo-lots"
        path (Path): Path to parent directory or zip file containing shapefile, or geodatabase.
        layer (str | None): Shapefile or feature class name. For single-layer GDBs, may be
            omitted and will be inferred automatically.
        zip_subdir (str | None): Internal path if shp is nested within a zip file.
            Must be None when path is a file geodatabase.
        org_md (Path | OrgMetadata | None): Metadata reference used to populate the embedded XML.
    """
    if isinstance(org_md, Path) or not org_md:
        org_md = product_metadata.load(org_md_path_override=org_md)

    product_md = org_md.product(product_name).dataset(dataset_name)

    is_gdb = ".gdb" in path.suffixes
    is_shp = layer is not None and (".shp" in path.suffixes or layer.endswith(".shp"))

    if is_gdb:
        if zip_subdir is not None:
            raise ValueError(
                "Nested zipped GDBs are not supported. The GDB must be at the top level of the zip."
            )
        layer = fgdb.resolve_layer(path, layer)
        file_metadata = product_md.calculate_layer_dataset_metadata(
            file_id=file_id, layer=layer
        )
        custom_type_key = "fgdb_data_type"
    elif is_shp:
        file_metadata = product_md.calculate_file_dataset_metadata(file_id=file_id)
        custom_type_key = "shp_data_type"
    else:
        raise ValueError(
            f"Unsupported file type for metadata writing: path='{path}', layer='{layer}'. "
            "Expected a .gdb or .shp path."
        )

    assert layer is not None  # guaranteed: GDB branch resolved it, SHP branch required it

    logger.info(f"Wrote metadata to layer '{layer}' in {path}")

    esri_md = esri_metadata.generate_metadata()

    # Set dataset-level values
    # TODO: define DCP organizationally required metadata fields
    esri_md.md_hr_lv_name = "dataset"
    esri_md.data_id_info.id_citation.res_title = file_metadata.attributes.display_name
    esri_md.data_id_info.id_abs = file_metadata.attributes.description
    # TODO: map idPurp to a product-metadata field
    esri_md.data_id_info.id_credit = file_metadata.attributes.attribution
    esri_md.data_id_info.res_const.consts.use_limit = (
        file_metadata.attributes.disclaimer
    )
    esri_md.data_id_info.other_keys.keyword = file_metadata.attributes.tags
    esri_md.data_id_info.search_keys.keyword = file_metadata.attributes.tags

    if file_metadata.attributes.projection:
        authority, code = file_metadata.attributes.projection.split(":")
        ref_sys_id = esri_md.ref_sys_info.ref_system.ref_sys_id
        ref_sys_id.ident_code.code = int(code)
        ref_sys_id.id_code_space.value = authority
        # idVersion intentionally omitted: ArcGIS Synchronize Metadata overwrites it from its bundled EPSG dataset

    entity_name = layer.removesuffix(".shp")
    esri_md.eainfo.detailed.enttyp.enttypl.value = entity_name
    esri_md.eainfo.detailed.enttyp.enttypt.value = "Feature Class"
    esri_md.eainfo.detailed.name = entity_name

    # Build attribute metadata for each column
    esri_md.eainfo.detailed.attr = [
        _create_attr_metadata(column, custom_type_key=custom_type_key)
        for column in file_metadata.columns
    ]

    if is_gdb:
        fgdb.write_metadata(gdb=path, layer=layer, metadata=esri_md, overwrite=True)
    else:
        shp = Shapefile(path=path, shp_name=layer, zip_subdir=zip_subdir)
        shp.write_metadata(esri_md, overwrite=True)


def _create_attr_metadata(
    column: DatasetColumn, custom_type_key: str | None = None
) -> Attr:
    """Create an Attr metadata object from a column specification."""
    attr = Attr()

    is_uid = column.id == "uid"
    attr.attrlabl.value = column.name
    attr.attalias.value = column.name
    attr.attrdef.value = column.description
    attr.attrdefs.value = column.data_source
    effective_type = (
        column.custom.get(custom_type_key) if custom_type_key else None
    ) or column.data_type
    attr.attrtype.value = "OID" if is_uid else effective_type

    if column.values:
        attr.attrdomv.udom = None
        attr.attrdomv.edom = [_create_edom_metadata(value) for value in column.values]
    else:
        attr.attrdomv.udom = Udom(value=column.description)
        attr.attrdomv.edom = []
    return attr


def _create_edom_metadata(column_value: ColumnValue) -> Edom:
    """Create an Edom metadata object from a column value specification."""
    edom = Edom()
    edom.edomv = column_value.value
    edom.edomvd = column_value.description

    return edom
