import shutil
import zipfile

import pytest
from pytest import fixture

from dcpy.models.data.shapefile_metadata import Metadata
from dcpy.utils.geospatial import fgdb

GDB_ZIP = "geodatabase.gdb.zip"
FEATURE_CLASS = "mappluto_one_row"
TABLE = "pluto_one_row"
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


@fixture
def path_fixture(request):
    return request.getfixturevalue(request.param)


gdb_paths = pytest.mark.parametrize(
    "path_fixture",
    [
        pytest.param(
            "temp_gdb_zip_path",
            id="run_tests_on_zipped_gdb",
        ),
        pytest.param(
            "temp_gdb_nonzipped_path",
            id="run_tests_on_nonzipped_gdb",
        ),
    ],
    indirect=True,
)


@gdb_paths
def test_get_layers(path_fixture):
    layers = fgdb.get_layers(path_fixture)
    assert layers == [FEATURE_CLASS, TABLE]


@gdb_paths
def test_read_metadata(path_fixture):
    md = fgdb.read_metadata(gdb=path_fixture, layer=FEATURE_CLASS)

    element = "esri"
    assert hasattr(md, element), f"Expected element '{element}', but found none"

    assert md.esri.crea_date == "20260203"
    assert md.esri.crea_time == "10392600"


@gdb_paths
def test_write_metadata(path_fixture, temp_metadata_object):
    layers_before_md_write = fgdb.get_layers(path_fixture)
    fgdb.write_metadata(
        gdb=path_fixture,
        layer=FEATURE_CLASS,
        metadata=temp_metadata_object,
        overwrite=True,
    )
    layers_after_md_write = fgdb.get_layers(path_fixture)

    md = fgdb.read_metadata(path_fixture, FEATURE_CLASS)
    element = "esri"
    assert hasattr(md, element), f"Expected element '{element}', but found none"

    assert md.esri.crea_date == "19611215"
    assert md.esri.crea_time == "00000000"
    # confirm that no gdb layers were lost during md writing operations
    assert sorted(layers_before_md_write) == sorted(layers_after_md_write)


@gdb_paths
def test_metadata_exists(path_fixture):
    originally_md_exists = fgdb.metadata_exists(gdb=path_fixture, layer=FEATURE_CLASS)
    # remove metadata
    fgdb.remove_metadata(
        gdb=path_fixture,
        layer=FEATURE_CLASS,
    )
    md_exists_after_removal = fgdb.metadata_exists(
        gdb=path_fixture, layer=FEATURE_CLASS
    )
    assert originally_md_exists is True, "Expected layer metadata but found none"
    assert md_exists_after_removal is False, (
        "Expected no layer metadata, but found some"
    )


@gdb_paths
def test_remove_metadata(path_fixture):
    layers_before_md_removal = fgdb.get_layers(path_fixture)
    fgdb.remove_metadata(
        gdb=path_fixture,
        layer=FEATURE_CLASS,
    )
    layers_after_md_removal = fgdb.get_layers(path_fixture)

    md = fgdb.read_metadata(path_fixture, FEATURE_CLASS)
    assert md is None
    # confirm that no gdb layers were lost during md removal
    assert sorted(layers_before_md_removal) == sorted(layers_after_md_removal)
