from datetime import datetime

from pytest import fixture

from dcpy.utils.geospatial.metadata import generate_metadata


@fixture
def today_datestamp() -> str:
    return datetime.now().strftime("%Y%m%d")


def test_generate_metadata(today_datestamp):
    md = generate_metadata()

    expected_date = today_datestamp

    assert hasattr(md, "esri")
    esri = md.esri
    assert isinstance(esri.crea_date, str)

    # CreaTime has leading zeros and must be preserved as string
    assert isinstance(esri.crea_time, str)
    assert esri.crea_date == expected_date

    # ArcGISFormat
    assert isinstance(esri.arc_gis_format, float)
    assert esri.arc_gis_format == 1.0

    # SyncOnce should be string
    assert isinstance(esri.sync_once, str)
    assert esri.sync_once == "TRUE"

    # mdHrLv.ScopeCd @value preserves leading zeros as string
    assert hasattr(md, "md_hr_lv")
    assert md.md_hr_lv.scope_cd.value == "005"
    assert isinstance(md.md_hr_lv.scope_cd.value, str)

    # mdDateSt should capture its text as an int and attribute Sync should be present
    assert hasattr(md, "md_date_st")
    assert isinstance(md.md_date_st.value, int)
    assert md.md_date_st.sync == "TRUE"
