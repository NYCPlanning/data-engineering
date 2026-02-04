import importlib.util
import sys
from pathlib import Path
import pytest
import shutil
import logging
import difflib
from xml.etree.ElementTree import canonicalize
import uuid

from dcpy.utils.code_gen import pydantic_from_xml

PLUTO_METADATA_XML = "esri_metadata_pluto_export.xml"

# Test a minimal XML example
XML_TEMPLATE = """<?xml version="1.0"?>
<metadata xml:lang="en">
    <Esri>
        <CreaDate>{crea_date}</CreaDate>
        <CreaTime>{crea_time}</CreaTime>
        <ArcGISFormat>{arcgis_format}</ArcGISFormat>
        <SyncOnce>{sync_once}</SyncOnce>
        <scaleRange>
            <minScale>{min_scale}</minScale>
            <maxScale>{max_scale}</maxScale>
        </scaleRange>
        <ArcGISProfile>{arcgis_profile}</ArcGISProfile>
    </Esri>
    <mdHrLv>
        <ScopeCd value="{scope_value}" />
    </mdHrLv>
    <mdDateSt Sync="{md_date_st_sync}">{md_date_st}</mdDateSt>
</metadata>
"""


DEFAULT_VALUES = {
    "crea_date": "19261122",
    "crea_time": "00000000",
    "arcgis_format": "1.0",
    "sync_once": "TRUE",
    "item_name": "Dataset Title",
    "item_name_sync": "TRUE",
    "ims_content_type": "002",
    "ims_content_type_sync": "TRUE",
    "min_scale": "150000000",
    "max_scale": "5000",
    "arcgis_profile": "ISO19139",
    "res_title": "Dataset Title",
    "scope_value": "005",
    "md_date_st": "19261122",
    "md_date_st_sync": "TRUE",
}


@pytest.fixture
def temp_pluto_md_path(utils_resources_path, tmp_path):
    in_file = utils_resources_path / PLUTO_METADATA_XML
    out_file = tmp_path / PLUTO_METADATA_XML
    shutil.copy2(
        src=in_file,
        dst=out_file,
    )
    return out_file


@pytest.fixture
def temp_inline_md_as_string():
    xml_content = XML_TEMPLATE
    return xml_content


@pytest.fixture
def temp_pluto_md_as_string(temp_pluto_md_path):
    xml_content = temp_pluto_md_path.read_text()
    return xml_content


@pytest.fixture(params=["temp_inline_md_as_string", "temp_pluto_md_as_string"])
def generated_module(tmp_path: Path, request):
    """Generate the sample XML using DEFAULT_VALUES, run the generator, load
    the generated module and yield (module, xml_text, values). After the
    test completes remove the module from sys.modules to avoid pollution.
    """
    values = DEFAULT_VALUES

    xml_file = tmp_path / "sample.xml"
    template_text = request.getfixturevalue(request.param)
    xml_text = template_text.format(**values)
    xml_file.write_text(xml_text, encoding="utf-8")

    out_py = tmp_path / "generated.py"
    # generate
    pydantic_from_xml.generate_from_xml(xml_file, out_py)

    # use a randomized module name to avoid collisions between test runs
    name = f"generated_xml_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(name, str(out_py))
    assert spec is not None
    mod = importlib.util.module_from_spec(spec)
    assert mod is not None
    loader = spec.loader
    assert loader is not None
    # Ensure module is available in sys.modules before executing so
    # any import-time registration that relies on the module package
    # or on import-time lookups finds the module.
    if name in sys.modules:
        del sys.modules[name]
    sys.modules[name] = mod
    try:
        loader.exec_module(mod)
    except Exception:
        # remove partially-loaded module on failure to avoid poisoning sys.modules
        if name in sys.modules and sys.modules[name] is mod:
            del sys.modules[name]
        raise

    try:
        yield mod, xml_text, values
    finally:
        # cleanup module entry
        if name in sys.modules and sys.modules[name] is mod:
            del sys.modules[name]


def test_generate_and_parse_sample_xml(generated_module):
    # generated_module yields (mod, xml_text, values)
    mod, xml_text, values = generated_module

    # root class is likely named Metadata
    assert hasattr(mod, "Metadata")
    Metadata = getattr(mod, "Metadata")

    print(f"{Metadata=}")

    # parse
    root = Metadata.from_xml(xml_text)

    # CreaDate should be parsed as int
    assert hasattr(root, "esri")
    esri = root.esri
    assert esri.crea_date == int(values["crea_date"])
    assert isinstance(esri.crea_date, int)

    # CreaTime has leading zeros and must be preserved as string
    assert esri.crea_time == values["crea_time"]
    assert isinstance(esri.crea_time, str)

    # ArcGISFormat should be float
    assert isinstance(esri.arc_gis_format, float)
    assert esri.arc_gis_format == float(values["arcgis_format"])

    # SyncOnce should be string
    assert isinstance(esri.sync_once, str)
    assert esri.sync_once == values["sync_once"]

    ##----skipping, as these exist in the min example, but not the pluto resource----
    # scaleRange
    # assert isinstance(esri.scale_range.min_scale, str)
    # assert esri.scale_range.min_scale == values["min_scale"]
    # assert isinstance(esri.scale_range.max_scale, str)
    # assert esri.scale_range.max_scale == values["max_scale"]

    # resTitle
    # assert isinstance(esri.arc_gis_profile, str)
    # assert esri.arc_gis_profile == values["arcgis_profile"]
    ##--------------------------------end of skipping--------------------------------

    # mdHrLv.ScopeCd @value preserves leading zeros as string
    assert hasattr(root, "md_hr_lv")
    assert root.md_hr_lv.scope_cd.value == values["scope_value"]
    assert isinstance(root.md_hr_lv.scope_cd.value, str)

    # mdDateSt should capture its text as an int and attribute Sync should be present
    assert hasattr(root, "md_date_st")
    assert root.md_date_st.value == int(values["md_date_st"])
    assert isinstance(root.md_date_st.value, int)
    assert root.md_date_st.sync == values["md_date_st_sync"]


def test_roundtrip_serialization_matches_input(generated_module):
    """Generate classes, parse the sample XML, serialize back to XML and
    assert the canonicalized output matches the canonicalized input.

    On mismatch, log the full canonicalized strings and a unified diff to aid
    debugging.
    """
    mod, xml_text, values = generated_module

    assert hasattr(mod, "Metadata")
    Metadata = getattr(mod, "Metadata")

    # parse
    root = Metadata.from_xml(xml_text)

    # serialize back
    out_xml = root.to_xml()
    if isinstance(out_xml, (bytes, bytearray)):
        out_text = out_xml.decode()
    else:
        out_text = str(out_xml)

    # canonicalize both (strip insignificant whitespace)
    try:
        expected_can = canonicalize(xml_text, strip_text=True)
        actual_can = canonicalize(out_text, strip_text=True)
    except Exception:
        # If canonicalize is unavailable or fails, fall back to raw strings
        expected_can = xml_text
        actual_can = out_text

    if expected_can != actual_can:
        logger = logging.getLogger("test_roundtrip")
        logger.error("Roundtrip serialization mismatch")
        logger.error("--- expected canonical ---\n%s", expected_can)
        logger.error("--- actual canonical ---\n%s", actual_can)
        for line in difflib.unified_diff(
            expected_can.splitlines(),
            actual_can.splitlines(),
            fromfile="expected",
            tofile="actual",
            lineterm="",
        ):
            logger.error(line)

    assert expected_can == actual_can
