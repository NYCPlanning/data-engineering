import pytest
from pytest import fixture
from dcpy.lifecycle.package import shapefiles
from dcpy.utils.geospatial import shapefile as shp_utils
from datetime import datetime
from dcpy.models.product.metadata import OrgMetadata
import shutil
import zipfile

SHP_ZIP_WITH_MD = "shapefile_single_pluto_feature_with_metadata.shp.zip"


def _get_info_from_file_fixture(
    request: pytest.FixtureRequest, fixture: str, file_type: str
) -> dict:
    """Calculate path and shp name for a given shapefile fixture.
    Calculation differs between zipped and non-zipped fixtures.

    Args:
        request (pytest.FixtureRequest):
        fixture (str): fixture name
        file_type (str): type of fixture - either "zip" or "nonzip"

    Returns:
        dict: path and shapefile name for given fixture
    """
    if file_type not in ["zip", "nonzip"]:
        raise Exception(f"Type: {file_type} is an ")
    elif file_type == "zip":
        path = request.getfixturevalue(fixture)  # Retrieve fixture by name
        shp_name = path.stem
    elif file_type == "nonzip":
        path_fixture = request.getfixturevalue(fixture)
        path = path_fixture.parent  # Retrieve fixture by name
        shp_name = path_fixture.name
    return {"path": path, "shp_name": shp_name}


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
def today_datestamp() -> str:
    return datetime.now().strftime("%Y%m%d")


@pytest.fixture
def org_metadata(package_and_dist_test_resources):
    return package_and_dist_test_resources.org_md


def test_write_shapefile_xml_metadata(
    org_metadata: OrgMetadata, request, path_fixture, file_type, subdir
):
    fixture_info = _get_info_from_file_fixture(
        request, fixture=path_fixture, file_type=file_type
    )
    # write metadata
    shapefiles.write_shapefile_xml_metadata(
        product_name="lion",
        dataset_name="pseudo_lots",
        shapefile_path=temp_shp_zip_with_md_path,
    )
    pseudo_lots_md = org_metadata.product("lion").dataset("pseudo_lots")
    # read it back
    shp = shp_utils.from_path(fixture_info["path"], fixture_info["shp_name"])
    written_md = shp.read_metadata()
    assert written_md.title == pseudo_lots_md.attributes.display_name


# TODO - test the ability to write values to metadata where that xpath/model doesn't already exist in the class instance
# e.g. if md doesn't have xml tag "x", but data model has structure for "x". Confirm writing value adds "x" to correct data model to md
# this might be a beter test for the test_pydantic_from_xml.py file?
