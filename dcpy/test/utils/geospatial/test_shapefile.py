from dcpy.utils.geospatial import shapefile
from pytest import fixture
import pytest
import shutil
import zipfile
from pathlib import Path
from dcpy.models.data.shapefile_metadata import Metadata
from dcpy.utils.geospatial.shapefile import generate_metadata

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
def temp_metadata_object(utils_resources_path):
    xml_file = utils_resources_path / METADATA_XML
    xml_content = xml_file.read_text()
    assert xml_content != "", (
        f"Non-empty string expected, got: '{xml_content}' instead."
    )
    md_object = Metadata.from_xml(xml_content)
    return md_object


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
def test_add_metadata_to_shp_no_existing_metadata(
    request, path_fixture, file_type, subdir, temp_metadata_object
):
    fixture_info = _get_info_from_file_fixture(
        request, fixture=path_fixture, file_type=file_type
    )
    shp = shapefile.from_path(fixture_info["path"], fixture_info["shp_name"], subdir)
    assert not shp.metadata_exists(), "Expected no metadata, but found some"
    shp.write_metadata(
        metadata=temp_metadata_object,
    )
    assert shp.metadata_exists(), "Expected metadata, but found none"


@pytest.mark.parametrize(
    "path_fixture, file_type, subdir",
    [
        pytest.param(
            "temp_shp_zip_with_md_path",
            "zip",
            None,
            id="overwrite_existing_md_in_zip_shp",
        ),
        pytest.param(
            "temp_nonzipped_shp_with_md_path",
            "nonzip",
            None,
            id="overwrite_existing_md_in_nonzip_shp",
        ),
    ],
)
def test_overwrite_existing_shp_metadata(
    request, path_fixture, file_type, subdir, temp_metadata_object
):
    fixture_info = _get_info_from_file_fixture(
        request, fixture=path_fixture, file_type=file_type
    )
    shp = shapefile.from_path(fixture_info["path"], fixture_info["shp_name"], subdir)
    assert shp.metadata_exists(), "Expected metadata, but found none"
    shp.write_metadata(metadata=temp_metadata_object, overwrite=True)
    assert shp.metadata_exists(), "Expected metadata, but found none"


@pytest.mark.parametrize(
    "path_fixture, file_type, subdir",
    [
        pytest.param(
            "temp_shp_zip_with_md_path",
            "zip",
            None,
            id="dont_overwrite_existing_md_in_zip_shp",
        ),
        pytest.param(
            "temp_nonzipped_shp_with_md_path",
            "nonzip",
            None,
            id="dont_overwrite_existing_md_in_nonzip_shp",
        ),
    ],
)
def test_dont_overwrite_existing_shp_metadata(
    request, path_fixture, file_type, subdir, temp_metadata_object
):
    fixture_info = _get_info_from_file_fixture(
        request, fixture=path_fixture, file_type=file_type
    )

    with pytest.raises(
        FileExistsError,
        match="Metadata XML already exists, and overwrite is False. Nothing will be written",
    ):
        shp = shapefile.from_path(
            fixture_info["path"], fixture_info["shp_name"], subdir
        )
        shp.write_metadata(
            metadata=temp_metadata_object,
        )


@pytest.mark.parametrize(
    "path_fixture, file_type, subdir",
    [
        pytest.param(
            "temp_shp_zip_no_md_path",
            "zip",
            None,
            id="md_not_exists_in_zip_shp",
        ),
        pytest.param(
            "temp_nonzipped_shp_no_md_path",
            "nonzip",
            None,
            id="md_not_exists_in_nonzip_shp",
        ),
    ],
)
def test_metadata_not_exists(request, path_fixture, file_type, subdir):
    fixture_info = _get_info_from_file_fixture(
        request, fixture=path_fixture, file_type=file_type
    )
    shp = shapefile.from_path(fixture_info["path"], fixture_info["shp_name"], subdir)
    assert not shp.metadata_exists(), "Expected no metadata, but found some"


METADATA_EXISTS_PARAMS = [
    pytest.param(
        "temp_shp_zip_with_md_path",
        "zip",
        None,
    ),
    pytest.param(
        "temp_nonzipped_shp_with_md_path",
        "nonzip",
        None,
    ),
]


@pytest.mark.parametrize("path_fixture, file_type, subdir", METADATA_EXISTS_PARAMS)
def test_metadata_exists(request, path_fixture, file_type, subdir):
    fixture_info = _get_info_from_file_fixture(
        request, fixture=path_fixture, file_type=file_type
    )
    shp = shapefile.from_path(fixture_info["path"], fixture_info["shp_name"], subdir)
    assert shp.metadata_exists(), "Expected metadata, but found none"


@pytest.mark.parametrize("path_fixture, file_type, subdir", METADATA_EXISTS_PARAMS)
def test_read_metadata(request, path_fixture, file_type, subdir):
    fixture_info = _get_info_from_file_fixture(
        request, fixture=path_fixture, file_type=file_type
    )
    shp = shapefile.from_path(fixture_info["path"], fixture_info["shp_name"], subdir)
    md = shp.read_metadata()

    element = "esri"
    assert hasattr(md, element), f"Expected element '{element}', but found none"


def test_generate_metadata():
    md = generate_metadata()

    assert hasattr(md, "esri")
    esri = md.esri
    assert isinstance(esri.crea_date, str)

    # CreaTime has leading zeros and must be preserved as string
    assert isinstance(esri.crea_time, str)

    # ArcGISFormat
    assert isinstance(esri.arc_gis_format, str)
    assert esri.arc_gis_format == "1.0"

    # SyncOnce should be string
    assert isinstance(esri.sync_once, str)
    assert esri.sync_once == "TRUE"

    # mdHrLv.ScopeCd @value preserves leading zeros as string
    assert hasattr(md, "md_hr_lv")
    assert md.md_hr_lv.scope_cd.value == "005"
    assert isinstance(md.md_hr_lv.scope_cd.value, str)

    # mdDateSt should capture its text as an int and attribute Sync should be present
    assert hasattr(md, "md_date_st")
    assert isinstance(md.md_date_st.value, int)
    assert md.md_date_st.sync == "TRUE"
