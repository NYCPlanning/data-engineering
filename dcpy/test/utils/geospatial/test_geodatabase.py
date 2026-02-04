import shutil
from pytest import fixture
import zipfile
from dcpy.models.data.shapefile_metadata import Metadata
from dcpy.utils.geospatial import geodatabase

GDB_ZIP = "geodatabase.gdb.zip"
FEATURE_CLASS = "pluto_one_row"
METADATA_XML = "esri_metadata.xml"


@fixture
def temp_gdb_zip_path(utils_resources_path, tmp_path):
    shutil.copy2(
        src=utils_resources_path / GDB_ZIP,
        dst=tmp_path / GDB_ZIP,
    )
    assert zipfile.is_zipfile(tmp_path / GDB_ZIP), (
        f"'{GDB_ZIP}' should be a valid zip file"
    )
    return tmp_path / GDB_ZIP


@fixture
def temp_gdb_nonzipped_path(temp_gdb_zip_path, tmp_path):
    shutil.unpack_archive(filename=temp_gdb_zip_path, extract_dir=tmp_path)
    gdb_path = tmp_path / temp_gdb_zip_path.stem
    assert gdb_path.is_dir(), "Expected a gdb directory, but found none"
    return gdb_path


@fixture
def temp_metadata_object(utils_resources_path):
    xml_file = utils_resources_path / METADATA_XML
    xml_content = xml_file.read_text()
    assert xml_content != "", (
        f"Non-empty string expected, got: '{xml_content}' instead."
    )
    md_object = Metadata.from_xml(xml_content)
    return md_object


# TODO - parametrize all tests
def test_read_metadata(temp_gdb_nonzipped_path):
    md = geodatabase.read_metadata(gdb=temp_gdb_nonzipped_path, layer=FEATURE_CLASS)

    element = "esri"
    assert hasattr(md, element), f"Expected element '{element}', but found none"

    assert md.esri.crea_date == "20260203"
    assert md.esri.crea_time == "10392600"


def test_write_metadata(temp_gdb_nonzipped_path, temp_metadata_object):
    geodatabase.write_metadata(
        gdb=temp_gdb_nonzipped_path,
        layer=FEATURE_CLASS,
        metadata=temp_metadata_object,
        overwrite=True,
    )

    md = geodatabase.read_metadata(temp_gdb_nonzipped_path, FEATURE_CLASS)
    element = "esri"
    assert hasattr(md, element), f"Expected element '{element}', but found none"

    assert md.esri.crea_date == "19611215"
    assert md.esri.crea_time == "00000000"


def test_metadata_exists(temp_gdb_nonzipped_path):
    originally_md_exists = geodatabase.metadata_exists(
        gdb=temp_gdb_nonzipped_path, layer=FEATURE_CLASS
    )
    # remove metadata
    geodatabase.remove_metadata(
        gdb=temp_gdb_nonzipped_path,
        layer=FEATURE_CLASS,
    )
    md_exists_after_removal = geodatabase.metadata_exists(
        gdb=temp_gdb_nonzipped_path, layer=FEATURE_CLASS
    )
    assert originally_md_exists is True, "Expected layer metadata but found none"
    assert md_exists_after_removal is False, (
        "Expected no layer metadata, but found some"
    )


def test_remove_metadata(temp_gdb_nonzipped_path):
    geodatabase.remove_metadata(
        gdb=temp_gdb_nonzipped_path,
        layer=FEATURE_CLASS,
    )

    md = geodatabase.read_metadata(temp_gdb_nonzipped_path, FEATURE_CLASS)
    assert md is None
