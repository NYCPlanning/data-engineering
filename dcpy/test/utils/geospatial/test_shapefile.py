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
def temp_nonzipped_shp_no_md_path(temp_shp_zip_no_md_path, tmp_path):
    shutil.unpack_archive(filename=temp_shp_zip_no_md_path, extract_dir=tmp_path)
    shp_path = tmp_path / temp_shp_zip_no_md_path.stem
    assert shp_path.is_file(), "Expected a shapefile, but found none"
    assert not Path(f"{shp_path}.xml").is_file(), "Expected no file, but found one"
    return shp_path


@fixture
def temp_nonzipped_shp_with_md_path(temp_shp_zip_with_md_path, tmp_path):
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


def test_add_metadata_to_shp_no_existing_metadata(
    temp_shp_zip_no_md_path, temp_nonzipped_shp_no_md_path, temp_xml_string
):
    path = temp_shp_zip_no_md_path
    shp_name = temp_shp_zip_no_md_path.stem

    shp = shapefile.from_path(path, shp_name)
    assert not shp.metadata_exists(), "Expected no metadata, but found some"
    shp.write_metadata(
        metadata=temp_xml_string,
    )
    assert shp.metadata_exists(), "Expected metadata, but found none"


def test_overwrite_existing_shp_metadata(
    temp_shp_zip_with_md_path, temp_nonzipped_shp_with_md_path, temp_xml_string
):
    path = temp_shp_zip_with_md_path
    shp_name = temp_shp_zip_with_md_path.stem

    shp = shapefile.from_path(path, shp_name)
    assert shp.metadata_exists(), "Expected metadata, but found none"
    shp.write_metadata(metadata=temp_xml_string, overwrite=True)
    assert shp.metadata_exists(), "Expected metadata, but found none"


def test_dont_overwrite_existing_shp_metadata(
    temp_shp_zip_with_md_path, temp_nonzipped_shp_with_md_path, temp_xml_string
):
    path = temp_shp_zip_with_md_path
    shp_name = temp_shp_zip_with_md_path.stem

    with pytest.raises(
        FileExistsError,
        match="Metadata XML already exists, and overwrite is False. Nothing will be written",
    ):
        shp = shp = shapefile.from_path(path, shp_name)
        shp.write_metadata(
            metadata=temp_xml_string,
        )


def test_metadata_exists(
    temp_shp_zip_no_md_path,
    temp_shp_zip_with_md_path,
    temp_nonzipped_shp_no_md_path,
    temp_nonzipped_shp_with_md_path,
):
    path = temp_shp_zip_no_md_path
    shp_name = temp_shp_zip_no_md_path.stem
    print(f"{temp_shp_zip_no_md_path=}\n{path=}\n{shp_name=}\n")
    shp = shapefile.from_path(path, shp_name)
    assert not shp.metadata_exists(), "Expected no metadata, but found some"

    path = temp_shp_zip_with_md_path
    shp_name = temp_shp_zip_with_md_path.stem
    print(f"{temp_shp_zip_with_md_path=}\n{path=}\n{shp_name=}\n")
    shp = shapefile.from_path(path, shp_name)
    assert shp.metadata_exists(), "Expected metadata, but found none"

    path = temp_nonzipped_shp_no_md_path.parent
    shp_name = temp_nonzipped_shp_no_md_path.name
    print(f"{temp_nonzipped_shp_no_md_path=}\n{path=}\n{shp_name=}\n")
    shp = shapefile.from_path(path, shp_name)
    assert not shp.metadata_exists(), "Expected no metadata, but found some"

    path = temp_nonzipped_shp_with_md_path.parent
    shp_name = temp_nonzipped_shp_with_md_path.name
    print(f"{temp_nonzipped_shp_with_md_path=}\n{path=}\n{shp_name=}\n")
    shp = shapefile.from_path(path, shp_name)
    assert shp.metadata_exists(), "Expected metadata, but found none"

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
    #         "path": temp_nonzipped_shp_with_md_path,
    #         "bool": True,
    #         "msg": "Expected metadata, but found none",
    #     },
    #     {
    #         "path": temp_nonzipped_shp_no_md_path,
    #         "bool": True,
    #         "msg": "Expected no metadata, but found some",
    #     },
    # ]
    # for test_case in test_cases:
    #     assert (
    #         shapefile.metadata_exists(path_to_shp=test_case["path"])
    #         is test_case["bool"]
    #     ), test_case["msg"]
