from dcpy.utils.geospatial import shapefile
from pytest import fixture
import pytest
import shutil
import zipfile

SHP_ZIP = "shapefile_single_pluto_feature_no_metadata.shp.zip"
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


# TODO - test the shapefile input path parser
def test_parse_invalid_path_to_shp():
    invalid_path_to_parse = "zip:///Users/name/Downloads/gadm36_AFG_shp.zip!gadm36_AFG_1"  # valid would end with ".shp"
    with pytest.raises(Exception):
        shapefile._parse_path_to_shp(shp_filename=invalid_path_to_parse)


def test_parse_valid_path_to_shp():
    paths_to_parse = [
        "zip:///Users/name/Downloads/gadm36_AFG_shp.zip!data/gadm36_AFG_1.shp",  # valid path
        "/Users/name/Downloads/gadm36_AFG_shp/data/gadm36_AFG_1.shp",  # valid path
    ]
    dicts_returned = [
        {
            "dir_containing_shp": "data",
            "path_to_zip": "/Users/name/Downloads/gadm36_AFG_shp.zip",
            "shp_name": "gadm36_AFG_1.shp",
            "is_zip": True,
        },
        {
            "dir_containing_shp": "/Users/name/Downloads/gadm36_AFG_shp/data",
            "path_to_zip": "",
            "shp_name": "gadm36_AFG_1.shp",
            "is_zip": False,
        },
    ]
    for parsed_path, returned_dict in zip(paths_to_parse, dicts_returned):
        shp_info = shapefile._parse_path_to_shp(shp_filename=parsed_path)

        assert shp_info == returned_dict


def test_add_metadata_to_shp(temp_shp_zip_path, temp_xml_string):
    # TODO - ensure that no metadata is present before writing it
    #     # assert shapefile.read_metadata(temp_shp_zip_path) == {}, (
    #     #     "No metadata should be present"
    #     # )
    path_to_shp = (
        f"zip://{temp_shp_zip_path}!shapefile_single_pluto_feature_no_metadata.shp"
    )
    # print(path_to_shp)
    shapefile.write_metadata(
        path_to_shp=path_to_shp,
        metadata=temp_xml_string,
        overwrite=False,
    )
    items_in_zip = shapefile._list_files_in_shp_dir(path_to_shp)
    assert len(items_in_zip) >= 5, (
        f"The zip file should contain at least 5 files, but {len(items_in_zip)} were found."
    )
    metadata_xml = f"{shapefile._parse_path_to_shp(path_to_shp)['shp_name']}.xml"

    assert metadata_xml in items_in_zip, (
        f"Expected to find {metadata_xml}, but was not found"
    )


#     # TODO - test actual output of xml file - valid xml, has a specific value, etc.
#     assert shapefile.read_metadata(temp_shp_zip_path) == temp_xml_string, (
#         "The correct metadata should be written"
#     )
