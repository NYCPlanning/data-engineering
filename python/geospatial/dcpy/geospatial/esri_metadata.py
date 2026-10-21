from datetime import datetime

from dcpy.geospatial.shapefile_metadata import (
    Attr,
    Attrtype,
    Edom,
    Esri,
    Mddatest,
    Metadata,
    Scalerange,
    Udom,
)
from dcpy.product_metadata.models.metadata.org import EsriSystemField
from dcpy.product_metadata.models.metadata.product import ColumnValue, DatasetColumn


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
    column: DatasetColumn,
    custom_type_key: str | None = None,
    include_type: bool = False,
) -> Attr:
    """Create an Attr metadata object from a column specification. The field
    type (attrtype) is only written when include_type is set."""
    attr = Attr()

    attr.attrlabl.value = column.name
    attr.attalias.value = column.name
    attr.attrdef.value = column.description
    attr.attrdefs.value = column.data_source
    if include_type:
        effective_type = (
            column.custom.get(custom_type_key) if custom_type_key else None
        ) or column.data_type
        attr.attrtype = Attrtype(value="OID" if column.id == "uid" else effective_type)
    else:
        attr.attrtype = None

    if column.values:
        attr.attrdomv.udom = None
        attr.attrdomv.edom = [_create_edom_metadata(value) for value in column.values]
    else:
        attr.attrdomv.udom = Udom(value=column.description)
        attr.attrdomv.edom = []
    return attr


def _create_system_attr_metadata(
    field: EsriSystemField, include_type: bool = False
) -> Attr:
    """Create an Attr metadata object for an Esri-managed system field. The
    field type (attrtype) is only written when include_type is set."""
    attr = Attr()
    attr.attrlabl.value = field.name
    attr.attalias.value = field.alias
    if include_type:
        attr.attrtype = Attrtype(value=field.data_type)
    else:
        attr.attrtype = None
    attr.attrdef.value = field.description
    attr.attrdefs.value = field.data_source
    attr.attrdomv.udom = Udom(value=field.description_of_values)
    attr.attrdomv.edom = []
    return attr


def _create_edom_metadata(column_value: ColumnValue) -> Edom:
    """Create an Edom metadata object from a column value specification."""
    edom = Edom()
    edom.edomv = column_value.value
    edom.edomvd = column_value.description

    return edom
