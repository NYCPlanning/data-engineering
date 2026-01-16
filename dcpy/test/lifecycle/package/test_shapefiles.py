import pytest
from pytest import fixture
from dcpy.lifecycle.package import shapefiles
from dcpy.utils.geospatial import shapefile as shp_utils
from datetime import datetime
from dcpy.models.product.metadata import OrgMetadata
from dcpy.models.data.shapefile_metadata import Metadata
import shutil
import zipfile
from pathlib import Path

SHP_ZIP_NO_MD = "shapefile_single_pluto_feature_no_metadata.shp.zip"
SHP_ZIP_WITH_MD = "shapefile_single_pluto_feature_with_metadata.shp.zip"


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
def today_datestamp() -> str:
    return datetime.now().strftime("%Y%m%d")


@pytest.fixture
def org_metadata(package_and_dist_test_resources):
    return package_and_dist_test_resources.org_md


@pytest.mark.parametrize(
    "path_fixture, file_type, subdir",
    [
        pytest.param(
            "temp_shp_zip_no_md_path",
            "zip",
            None,
            id="add_md_to_zip_shp_w_no_md",
        ),
        pytest.param(
            "temp_nonzipped_shp_no_md_path",
            "nonzip",
            None,
            id="add_md_to_nonzip_shp_w_no_md",
        ),
    ],
)
def test_write_shapefile_xml_metadata(
    request,
    path_fixture,
    file_type,
    subdir,
    org_metadata: OrgMetadata,
):
    fixture_info = _get_info_from_file_fixture(
        request, fixture=path_fixture, file_type=file_type
    )

    product_md = org_metadata.product("colp").dataset("colp")

    fields = Metadata.model_fields

    # write metadata
    shapefiles.write_shapefile_xml_metadata(
        product_name="colp",
        dataset_name="colp",
        path=fixture_info["path"],
        shp_name=fixture_info["shp_name"],
        zip_subdir=subdir,
        org_md=org_metadata,
    )

    # read it back
    shp = shp_utils.from_path(
        path=fixture_info["path"], shp_name=fixture_info["shp_name"], zip_subdir=subdir
    )
    metadata = shp.read_metadata()

    # Test default values
    assert metadata.md_stan_name == fields["md_stan_name"].default
    assert metadata.md_stan_ver == fields["md_stan_ver"].default
    # TODO - add helper code to access nested defaults (if this is the direction we end up pursuing)

    # Test product-specific values
    assert metadata.md_hr_lv_name == product_md.attributes.display_name
    assert metadata.data_id_info.id_abs == product_md.attributes.description
    assert metadata.data_id_info.other_keys.keyword == product_md.attributes.tags
    assert metadata.data_id_info.search_keys.keyword == product_md.attributes.tags

    assert metadata.eainfo.detailed.name == product_md.id
    assert metadata.eainfo.detailed.enttyp.enttypl.value == product_md.id
    assert metadata.eainfo.detailed.enttyp.enttypt.value == "Feature Class"

    assert product_md.columns[1].values is not None, "Column values must be defined"

    assert (
        metadata.eainfo.detailed.attr[1].attrdomv.edom[0].edomv
        == product_md.columns[1].values[0].value  # "1", when org_md product is colp
    )

    assert (
        metadata.eainfo.detailed.attr[1].attrdomv.edom[0].edomvd
        == product_md.columns[1]
        .values[0]
        .description  # "Manhattan", when org_md product is colp
    )
