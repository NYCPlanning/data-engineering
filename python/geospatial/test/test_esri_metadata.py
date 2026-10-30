from datetime import datetime

from pytest import fixture

from dcpy.geospatial.esri_metadata import (
    generate_iso_metadata,
    generate_metadata,
    to_esri_rich_text,
)


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


def _dataset():
    from dcpy.product_metadata.models.metadata.product import (
        ColumnValue,
        Dataset,
        DatasetAttributes,
        DatasetColumn,
    )

    return Dataset(
        attributes=DatasetAttributes(
            display_name="LION - Node",
            description="Nodes.",
            attribution="DCP",
            disclaimer="No warranty.",
            tags=["lion", "nodes"],
            projection="EPSG:2263",
            each_row_is_a="Node",
        ),
        columns=[
            DatasetColumn(
                id="nodeid",
                name="NODEID",
                data_type="integer",
                description="Node id.",
                data_source="CSCL",
                custom={"fgdb_data_type": "Integer"},
            ),
            DatasetColumn(
                id="vintersect",
                name="VIntersect",
                data_type="text",
                description="Virtual intersection flag.",
                values=[ColumnValue(value="VirtualIntersection", description="Yes")],
            ),
        ],
    )


def test_build_layer_metadata():
    from dcpy.geospatial.esri_metadata import build_layer_metadata, metadata_xml

    md = build_layer_metadata(_dataset(), "node", "fgdb_data_type")

    info = md.data_id_info
    assert info.id_citation.res_title == "LION - Node"
    assert info.id_abs == "Nodes."
    assert info.id_credit == "DCP"
    assert info.res_const.consts.use_limit == "No warranty."
    assert info.search_keys.keyword == ["lion", "nodes"]
    ref_sys_id = md.ref_sys_info.ref_system.ref_sys_id
    assert (ref_sys_id.id_code_space.value, ref_sys_id.ident_code.code) == (
        "EPSG",
        2263,
    )
    assert md.eainfo.detailed.name == "node"
    attrs = md.eainfo.detailed.attr
    assert [a.attrlabl.value for a in attrs] == ["NODEID", "VIntersect"]
    # the format's type wins over data_type; data_type is the fallback
    assert [a.attrtype.value for a in attrs] == ["Integer", "text"]
    assert [e.edomv for e in attrs[1].attrdomv.edom] == ["VirtualIntersection"]

    xml = metadata_xml(md)
    assert isinstance(xml, str)
    assert "<resTitle>LION - Node</resTitle>" in xml


def test_build_layer_metadata_drops_shp_suffix():
    from dcpy.geospatial.esri_metadata import build_layer_metadata

    md = build_layer_metadata(_dataset(), "nypuma2020.shp", "shp_data_type")
    assert md.eainfo.detailed.enttyp.enttypl.value == "nypuma2020"


def test_generate_iso_metadata_matches_iso_19115_3_structure():
    """Follows a Pro-synced ISO 19115-3 reference: utf8 char sets, no
    scaleRange or mdStanName/mdStanVer."""
    md = generate_iso_metadata()
    assert md.esri.arc_gis_profile == "ISO19115_3"
    assert md.esri.scale_range is None
    assert md.md_stan_name is None
    assert md.md_stan_ver is None
    assert md.md_char.char_set_cd.value == "004"
    assert md.data_id_info.data_char.char_set_cd.value == "004"

    xml = md.to_xml().decode()
    for absent in ("scaleRange", "mdStanName", "mdStanVer", "otherKeys"):
        assert absent not in xml


def test_to_esri_rich_text_paragraphs():
    text = "First paragraph,\nwrapped.\n\nSecond paragraph.\n\n\nThird."
    assert to_esri_rich_text(text) == (
        "<p>First paragraph, wrapped.</p><p>Second paragraph.</p><p>Third.</p>"
    )


def test_to_esri_rich_text_escapes_markup():
    assert to_esri_rich_text("Tax & lots <b>") == "<p>Tax &amp; lots &lt;b&gt;</p>"


def test_to_esri_rich_text_empty():
    assert to_esri_rich_text(None) is None
    assert to_esri_rich_text("  \n ") is None


def test_generate_metadata_defaults_unchanged():
    """generate_metadata() (CSCL's build_layer_metadata) keeps its original
    defaults; only generate_iso_metadata() applies the ISO 19115-3 structure."""
    md = generate_metadata()
    assert md.esri.arc_gis_profile == "ISO19139"
    assert md.esri.scale_range is not None
    assert md.md_stan_name == "ArcGIS Metadata"
    assert md.md_stan_ver == 1.0
