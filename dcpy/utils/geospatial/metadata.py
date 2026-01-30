from datetime import datetime
from dcpy.models.data.shapefile_metadata import (
    Metadata,
    Esri,
    Mddatest,
    Scalerange,
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
