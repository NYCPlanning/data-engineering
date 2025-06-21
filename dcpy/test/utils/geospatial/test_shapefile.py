from dcpy.utils.geospatial import shapefile
from pytest import fixture
import shutil
import zipfile
from zipfile import ZipFile

SHP_ZIP = "shapefile_single_pluto_feature.shp.zip"
METADATA_XML = "shapefile_metadata.xml"


@fixture
def temp_shp_zip_path(utils_resources_path, tmp_path):
    shutil.copy2(
        src=utils_resources_path / SHP_ZIP,
        dst=tmp_path / SHP_ZIP,
    )
    assert zipfile.is_zipfile(tmp_path / SHP_ZIP), (
        f"'{SHP_ZIP}' should be a valid zip file"
    )
    return tmp_path / SHP_ZIP


@fixture
def temp_xml_string(utils_resources_path):
    xml_output = ""
    with open(utils_resources_path / METADATA_XML) as xml:
        xml_output = xml.read()
    assert xml_output != "", f"Non-empty string expected, got: '{xml_output}' instead."
    return xml_output


# def test_unzip_shapefile(temp_shp_zip_path, tmp_path):
#     shapefile._unpack_simple_shp(
#         zip_file_path=Path(temp_shp_zip_path), unzip_to=tmp_path
#     )
#     assert (tmp_path / temp_shp_zip_path.stem).is_dir(), (
#         f"'{temp_shp_zip_path.stem}' should be a directory"
#     )
#     assert (
#         tmp_path / temp_shp_zip_path.stem / f"{temp_shp_zip_path.stem}.shp"
#     ).is_file(), "A .shp file should be present"


# def test_shp_w_no_metadata(temp_shp_zip_path):
#     assert shapefile.read_metadata(temp_shp_zip_path) == {}, (
#         "No metadata should be present"
#     )


# TODO - just test "write metadata" function directly, not each constituent step
def test_add_metadata_to_shp(temp_shp_zip_path, temp_xml_string):
    assert shapefile.read_metadata(temp_shp_zip_path) == {}, (
        "No metadata should be present"
    )
    # TODO - add additional fields
    shapefile.write_metadata(
        path_to_shp=temp_shp_zip_path,
        metadata=temp_xml_string,
        shp_name=temp_shp_zip_path.stem,
        force=True,
    )
    # print(shapefile.get_contents(temp_shp_zip_path))
    items_in_zip = shapefile.get_contents(temp_shp_zip_path)
    print(items_in_zip)
    assert len(items_in_zip) >= 5, (
        f"The zip file should contain at least 5 files, but {len(items_in_zip)} were found."
    )

    # TODO - test actual output of xml file - valid xml, has a specific value, etc.
    assert shapefile.read_metadata(temp_shp_zip_path) == temp_xml_string, (
        "The correct metadata should be written"
    )
