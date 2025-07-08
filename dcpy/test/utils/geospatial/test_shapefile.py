from dcpy.utils.geospatial import shapefile
from pytest import fixture
import pytest
import shutil
import zipfile
from pathlib import Path

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
def temp_unzipped_shp_no_md_path(temp_shp_zip_no_md_path, tmp_path):
    shutil.unpack_archive(filename=temp_shp_zip_no_md_path, extract_dir=tmp_path)
    shp_path = tmp_path / temp_shp_zip_no_md_path.stem
    assert shp_path.is_file(), "Expected a shapefile, but found none"
    assert not Path(f"{shp_path}.xml").is_file(), "Expected no file, but found one"
    return shp_path


@fixture
def temp_unzipped_shp_with_md_path(temp_shp_zip_with_md_path, tmp_path):
    shutil.unpack_archive(filename=temp_shp_zip_with_md_path, extract_dir=tmp_path)
    shp_path = tmp_path / temp_shp_zip_with_md_path.stem
    assert shp_path.is_file(), "Expected a shapefile, but found none"
    assert Path(f"{shp_path}.xml").is_file(), "Expected a file, but found none"
    return shp_path


@fixture
def temp_xml_string(utils_resources_path):
    xml_output = ""
    with open(utils_resources_path / METADATA_XML) as xml:
        xml_output = xml.read()
    assert xml_output != "", f"Non-empty string expected, got: '{xml_output}' instead."
    return xml_output


def test_parse_invalid_path_to_shp():
    invalid_paths_to_parse = [
        "zip:///Users/name/Downloads/gadm36_AFG_shp.zip!gadm36_AFG_1",  # valid would end with ".shp"
        "/Users/name/Downloads/gadm36_AFG_shp.zip!gadm36_AFG_1.shp",  # valid would begin with "zip://"
        "zip:///Users/name/Downloads/gadm36_AFG_shp.zip/gadm36_AFG_1.shp",  # valid would include "!" after .zip
        "/Users/name/Downloads/gadm36_AFG_shp/data",  # valid would end with ".shp"
    ]
    for invalid_path in invalid_paths_to_parse:
        with pytest.raises(Exception):
            print(invalid_path)
            shapefile._parse_path_to_shp(path_to_shp=invalid_path)


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
        shp_info = shapefile._parse_path_to_shp(path_to_shp=parsed_path)

        assert shp_info == returned_dict


def test_add_metadata_to_shp_no_existing_metadata(
    temp_shp_zip_no_md_path, temp_unzipped_shp_no_md_path, temp_xml_string
):
    paths_to_shp = [
        f"zip://{temp_shp_zip_no_md_path}!{temp_shp_zip_no_md_path.stem}",
        temp_unzipped_shp_no_md_path,
    ]

    for shp in paths_to_shp:
        print(shp)
        # ensure that metadata is *not* present before writing it
        assert shapefile.metadata_exists(shp) is False, (
            "Expected no metadata, but found some"
        )

        shapefile.write_metadata(
            path_to_shp=shp,
            metadata=temp_xml_string,
            overwrite=False,
        )
        assert shapefile.metadata_exists(shp) is True, (
            "Expected metadata, but found none"
        )

        items_in_zip = shapefile._list_files_in_shp_dir(shp)

        metadata_xml = f"{shapefile._parse_path_to_shp(shp)['shp_name']}.xml"

        assert metadata_xml in items_in_zip, (
            f"Expected to find {metadata_xml}, but was not found"
        )


def test_dont_overwrite_existing_shp_metadata(
    temp_shp_zip_with_md_path, temp_unzipped_shp_with_md_path, temp_xml_string
):
    paths_to_shp = [
        f"zip://{temp_shp_zip_with_md_path}!{temp_shp_zip_with_md_path.stem}",
        temp_unzipped_shp_with_md_path,
    ]
    for shp in paths_to_shp:
        # ensure that metadata *is* present before writing it
        assert shapefile.metadata_exists(shp) is True, (
            "Expected metadata, but found none"
        )

        with pytest.raises(
            FileExistsError,
            match="Metadata XML already exists, and overwrite is False. Nothing will be written",
        ):
            shapefile.write_metadata(
                path_to_shp=shp,
                metadata=temp_xml_string,
                overwrite=False,
            )


def test_metadata_exists(
    temp_shp_zip_no_md_path,
    temp_shp_zip_with_md_path,
    temp_unzipped_shp_no_md_path,
    temp_unzipped_shp_with_md_path,
):
    path_to_zip_shp_w_md = (
        f"zip://{temp_shp_zip_with_md_path}!{temp_shp_zip_with_md_path.stem}"
    )
    path_to_zip_shp_no_md = (
        f"zip://{temp_shp_zip_no_md_path}!{temp_shp_zip_no_md_path.stem}"
    )
    path_to_unzip_shp_w_md = temp_unzipped_shp_with_md_path
    path_to_unzip_shp_no_md = temp_unzipped_shp_no_md_path
    assert shapefile.metadata_exists(path_to_shp=path_to_zip_shp_w_md) is True, (
        "Expected metadata, but found none"
    )
    assert shapefile.metadata_exists(path_to_shp=path_to_zip_shp_no_md) is False, (
        "Expected no metadata, but found some"
    )
    assert shapefile.metadata_exists(path_to_shp=path_to_unzip_shp_w_md) is True, (
        "Expected metadata, but found none"
    )
    assert shapefile.metadata_exists(path_to_shp=path_to_unzip_shp_no_md) is False, (
        "Expected no metadata, but found some"
    )
    # test_cases = [
    #     {
    #         "path": f"zip://{temp_shp_zip_with_md_path}!{temp_shp_zip_with_md_path.stem}",
    #         "bool": True,
    #         "msg": "Expected metadata, but found none",
    #     },
    #     {
    #         "path": f"zip://{temp_shp_zip_no_md_path}!{temp_shp_zip_no_md_path.stem}",
    #         "bool": False,
    #         "msg": "Expected no metadata, but found some",
    #     },
    #     {
    #         "path": temp_unzipped_shp_with_md_path,
    #         "bool": True,
    #         "msg": "Expected metadata, but found none",
    #     },
    #     {
    #         "path": temp_unzipped_shp_no_md_path,
    #         "bool": True,
    #         "msg": "Expected no metadata, but found some",
    #     },
    # ]
    # for test_case in test_cases:
    #     assert (
    #         shapefile.metadata_exists(path_to_shp=test_case["path"])
    #         is test_case["bool"]
    #     ), test_case["msg"]
