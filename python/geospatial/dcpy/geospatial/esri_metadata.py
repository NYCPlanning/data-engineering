from datetime import datetime

from dcpy.geospatial.shapefile_metadata import (
    Attr,
    Edom,
    Esri,
    Mddatest,
    Metadata,
    Scalerange,
    Udom,
)
from dcpy.product_metadata.models.metadata.product import (
    ColumnValue,
    Dataset,
    DatasetColumn,
)


def generate_metadata() -> Metadata:
    """
    Generates a default Esri metadata object.
    Can be generated as an independent object without an existing spatial dataset.
    """
    esri_datestamp, esri_timestamp = _get_esri_timestamp()
    md_date_st = Mddatest(
        value=esri_datestamp,
    )
    scale_range = Scalerange()
    esri = Esri(
        crea_date=esri_datestamp,
        crea_time=esri_timestamp,
        scale_range=scale_range,
    )
    metadata = Metadata(
        esri=esri,
        md_date_st=md_date_st,
    )
    return metadata


def build_layer_metadata(
    dataset_md: Dataset, layer_name: str, custom_type_key: str | None = None
) -> Metadata:
    """Esri metadata for one layer (FGDB feature class or shapefile) from its
    product-metadata dataset, already resolved for the file/layer it's written into
    (e.g. ``DatasetMetadata.calculate_layer_dataset_metadata``).

    Args:
        dataset_md: The layer's dataset metadata (attributes and columns).
        layer_name: Feature class or shapefile name (a ``.shp`` suffix is dropped).
        custom_type_key: Column ``custom`` key holding the format's field type
            (e.g. ``fgdb_data_type``, ``shp_data_type``); falls back to ``data_type``.
    """
    esri_md = generate_metadata()
    attributes = dataset_md.attributes

    # TODO: define DCP organizationally required metadata fields
    esri_md.md_hr_lv_name = "dataset"
    esri_md.data_id_info.id_citation.res_title = attributes.display_name
    esri_md.data_id_info.id_abs = attributes.description
    # TODO: map idPurp to a product-metadata field
    esri_md.data_id_info.id_credit = attributes.attribution
    esri_md.data_id_info.res_const.consts.use_limit = attributes.disclaimer
    esri_md.data_id_info.other_keys.keyword = attributes.tags
    esri_md.data_id_info.search_keys.keyword = attributes.tags

    if attributes.projection:
        authority, code = attributes.projection.split(":")
        ref_sys_id = esri_md.ref_sys_info.ref_system.ref_sys_id
        ref_sys_id.ident_code.code = int(code)
        ref_sys_id.id_code_space.value = authority
        # idVersion omitted: ArcGIS Synchronize Metadata overwrites it from its EPSG data

    entity_name = layer_name.removesuffix(".shp")
    esri_md.eainfo.detailed.enttyp.enttypl.value = entity_name
    esri_md.eainfo.detailed.enttyp.enttypt.value = "Feature Class"
    esri_md.eainfo.detailed.name = entity_name

    esri_md.eainfo.detailed.attr = [
        _create_attr_metadata(column, custom_type_key=custom_type_key)
        for column in dataset_md.columns
    ]
    return esri_md


def metadata_xml(metadata: Metadata) -> str:
    """The metadata as an XML string (e.g. for the OpenFileGDB DOCUMENTATION option)."""
    xml = metadata.to_xml()
    return xml.decode("utf-8") if isinstance(xml, bytes) else xml


def _get_esri_timestamp(dt_obj=None):
    """
    Generate Esri-style CreaDate and CreaTime values.

    Args:
        dt_obj: datetime object (uses current time if None)

    Returns:
        tuple: (CreaDate, CreaTime) as strings
    """
    if dt_obj is None:
        dt_obj = datetime.now()

    # CreaDate: YYYYMMDD
    crea_date = dt_obj.strftime("%Y%m%d")

    # CreaTime: HHMMSSFF (hours, minutes, seconds, hundredths)
    hundredths = 0  # Esri appears to ignore the hundredths in practice
    crea_time = (
        f"{dt_obj.hour:02d}{dt_obj.minute:02d}{dt_obj.second:02d}{hundredths:02d}"
    )

    return crea_date, crea_time


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
