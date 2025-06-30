from dcpy.utils.geospatial import shapefile
from pytest import fixture
import pytest
import shutil
import zipfile

SHP_ZIP_NO_MD = "shapefile_single_pluto_feature_no_metadata.shp.zip"
SHP_ZIP_WITH_MD = "shapefile_single_pluto_feature_with_metadata.shp.zip"
METADATA_XML = "shapefile_metadata.xml"


@fixture
def temp_shp_zip_no_md_path(utils_resources_path, tmp_path):
    shutil.copy2(
        src=utils_resources_path / SHP_ZIP_NO_MD,
        dst=tmp_path / SHP_ZIP_NO_MD,
    )
    assert zipfile.is_zipfile(tmp_path / SHP_ZIP_NO_MD), (
        f"'{SHP_ZIP_NO_MD}' should be a valid zip file"
    )
    return tmp_path / SHP_ZIP_NO_MD


@fixture
def temp_shp_zip_with_md_path(utils_resources_path, tmp_path):
    shutil.copy2(
        src=utils_resources_path / SHP_ZIP_WITH_MD,
        dst=tmp_path / SHP_ZIP_WITH_MD,
    )
    assert zipfile.is_zipfile(tmp_path / SHP_ZIP_WITH_MD), (
        f"'{SHP_ZIP_WITH_MD}' should be a valid zip file"
    )
    return tmp_path / SHP_ZIP_WITH_MD


@fixture
def temp_xml_string(utils_resources_path):
    xml_output = ""
    with open(utils_resources_path / METADATA_XML) as xml:
        xml_output = xml.read()
    assert xml_output != "", f"Non-empty string expected, got: '{xml_output}' instead."
    return xml_output


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


def test_add_metadata_to_shp_no_existing_metadata(
    temp_shp_zip_no_md_path, temp_xml_string
):
    # TODO - ensure that metadata is *not* present before writing it

    path_to_shp = f"zip://{temp_shp_zip_no_md_path}!{temp_shp_zip_no_md_path.stem}"
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


def test_add_metadata_to_shp_with_existing_metadata(
    temp_shp_zip_with_md_path, temp_xml_string
):
    # TODO - ensure that metadata *is* present before writing it

    path_to_shp = f"zip://{temp_shp_zip_with_md_path}!{temp_shp_zip_with_md_path.stem}"
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


def test_metadata_exists(temp_shp_zip_no_md_path, temp_shp_zip_with_md_path):
    path_to_shp_w_md = (
        f"zip://{temp_shp_zip_with_md_path}!shapefile_single_pluto_feature.shp"
    )
    path_to_shp_no_md = (
        f"zip://{temp_shp_zip_no_md_path}!shapefile_single_pluto_feature.shp"
    )
    assert shapefile.metadata_exists(path_to_shp=path_to_shp_no_md) is False, (
        "Expected no metadata, but found some"
    )
    assert shapefile.metadata_exists(path_to_shp=path_to_shp_w_md) is True, (
        "Expected metadata, but found none"
    )
