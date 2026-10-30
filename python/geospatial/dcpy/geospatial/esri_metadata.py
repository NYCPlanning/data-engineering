import html
import re
from datetime import datetime

from dcpy.geospatial.shapefile_metadata import (
    Attr,
    Attrdomv,
    Attrtype,
    Charsetcd,
    Datachar,
    Edom,
    Esri,
    Mdchar,
    Mddatest,
    Metadata,
    Scalerange,
    Udom,
)
from dcpy.product_metadata.models.metadata.org import EsriSystemField
from dcpy.product_metadata.models.metadata.product import (
    ColumnValue,
    Dataset,
    DatasetColumn,
)

# ISO MD_CharacterSetCode for utf8, as Pro writes it
UTF8_CHAR_SET_CODE = "004"


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


def generate_iso_metadata() -> Metadata:
    """generate_metadata() adjusted to the ArcGIS ISO 19115-3 structure Pro writes:
    ISO19115_3 profile, utf8 character sets, and no scaleRange or
    mdStanName/mdStanVer. Used by lifecycle.package.esri.write_metadata (PLUTO);
    CSCL's build_layer_metadata still uses generate_metadata()."""
    metadata = generate_metadata()
    metadata.esri.arc_gis_profile = "ISO19115_3"
    metadata.esri.scale_range = None
    metadata.md_stan_name = None
    metadata.md_stan_ver = None
    metadata.md_char = Mdchar(char_set_cd=Charsetcd(value=UTF8_CHAR_SET_CODE))
    metadata.data_id_info.data_char = Datachar(
        char_set_cd=Charsetcd(value=UTF8_CHAR_SET_CODE)
    )
    return metadata


def to_esri_rich_text(text: str | None) -> str | None:
    """Convert plain text to the HTML Pro stores for rich-text elements
    (e.g. Description/idAbs, Use Limitations/useLimit): one <p> per
    blank-line-separated paragraph, single line breaks joined with spaces."""
    if not text or not text.strip():
        return None
    paragraphs = re.split(r"\n\s*\n", text.strip())
    return "".join(
        f"<p>{html.escape(' '.join(p.split()), quote=False)}</p>" for p in paragraphs
    )


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
    # Attr's default factories fill these (they're optional only so the ISO
    # path can omit them); narrowed for the type checker
    assert attr.attalias is not None
    assert attr.attrtype is not None
    assert attr.attrdomv is not None

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


def _create_iso_attr_metadata(
    column: DatasetColumn,
    custom_type_key: str | None = None,
    include_type: bool = False,
) -> Attr:
    """Create an Attr metadata object from a column specification, for
    write_metadata's ISO 19115-3 output (PLUTO). Unlike _create_attr_metadata
    (CSCL's build_layer_metadata): no alias (attalias), no description copied
    into udom, attrdomv only for a value list, and the field type (attrtype)
    only when include_type is set."""
    attr = Attr()

    attr.attrlabl.value = column.name
    attr.attalias = None
    attr.attrdef.value = column.description
    attr.attrdefs.value = column.data_source
    if include_type:
        effective_type = (
            column.custom.get(custom_type_key) if custom_type_key else None
        ) or column.data_type
        attr.attrtype = Attrtype(value="OID" if column.id == "uid" else effective_type)
    else:
        attr.attrtype = None

    attr.attrdomv = (
        Attrdomv(
            udom=None, edom=[_create_edom_metadata(value) for value in column.values]
        )
        if column.values
        else None
    )
    return attr


def _create_system_attr_metadata(
    field: EsriSystemField, include_type: bool = False
) -> Attr:
    """Create an Attr metadata object for an Esri-managed system field. The
    field type (attrtype) is only written when include_type is set. No alias
    (attalias) is written, matching product columns."""
    attr = Attr()
    attr.attrlabl.value = field.name
    attr.attalias = None
    if include_type:
        attr.attrtype = Attrtype(value=field.data_type)
    else:
        attr.attrtype = None
    attr.attrdef.value = field.description
    attr.attrdefs.value = field.data_source
    attr.attrdomv = Attrdomv(udom=Udom(value=field.description_of_values), edom=[])
    return attr


def _create_edom_metadata(column_value: ColumnValue) -> Edom:
    """Create an Edom metadata object from a column value specification."""
    edom = Edom()
    edom.edomv = column_value.value
    edom.edomvd = column_value.description

    return edom
