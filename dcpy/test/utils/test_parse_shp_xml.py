from pathlib import Path

from dcpy.models.data.shp_xml_metadata import Metadata


def test_parse_minimum_shp_metadata():
    p = Path(__file__).parent / "resources" / "shp_metadata_minimum.xml"
    xml = p.read_text(encoding="utf-8")
    md = Metadata.from_xml(xml)
    assert md is not None
    # basic checks from provided sample
    assert md.esri is not None
    assert md.esri.creadate == "19700101"
    assert md.mdhrlv is not None
    assert md.mdhrlv.scopecd.value == "005"
    assert md.mddatest is not None
    assert md.mddatest.sync == "TRUE"
    assert md.mddatest.date == "19261122"
