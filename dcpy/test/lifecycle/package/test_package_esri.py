import shutil
import zipfile
from datetime import datetime
from pathlib import Path

import pytest
from pytest import fixture

from dcpy.lifecycle.package import esri
from dcpy.models.data.shapefile_metadata import Metadata
from dcpy.models.product.dataset.metadata import ColumnValue, DatasetColumn
from dcpy.models.product.metadata import OrgMetadata
from dcpy.utils.geospatial import fgdb
from dcpy.utils.geospatial import shapefile as shp_utils

SHP_ZIP_NO_MD = "shapefile_single_pluto_feature_no_metadata.shp.zip"
SHP_ZIP_WITH_MD = "shapefile_single_pluto_feature_with_metadata.shp.zip"

GDB_ZIP = "geodatabase.gdb.zip"
SPATIAL_LAYER = "mappluto_one_row"
SHP_SUBDIR = "subdir"


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
def temp_shp_zip_with_subdir_path(utils_resources_path, tmp_path):
    """Shapefile zip where the .shp files are nested inside a subdirectory."""
    extract_dir = tmp_path / "extracted"
    extract_dir.mkdir()
    shutil.unpack_archive(
        filename=utils_resources_path / SHP_ZIP_NO_MD, extract_dir=extract_dir
    )
    subdir_zip = tmp_path / SHP_ZIP_NO_MD
    with zipfile.ZipFile(subdir_zip, "w") as zf:
        for f in extract_dir.iterdir():
            zf.write(f, arcname=f"{SHP_SUBDIR}/{f.name}")
    return subdir_zip


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


def _get_info_from_file_fixture(
    request: pytest.FixtureRequest, fixture: str, file_type: str
) -> dict:
    """Calculate path and layer name for a given fixture.
    Calculation differs between zipped and non-zipped fixtures.
    Supports shapefiles and file geodatabases.

    Args:
        request (pytest.FixtureRequest):
        fixture (str): fixture name
        file_type (str): type of fixture - either "zip" or "nonzip"

    Returns:
        dict: path and layer name for given fixture
    """

    if file_type not in ["zip", "nonzip"]:
        raise Exception(f"Type: {file_type} is an ")
    elif file_type == "zip":
        path_fixture = request.getfixturevalue(fixture)  # Retrieve fixture by name
        if ".gdb" in path_fixture.suffixes:
            layer = SPATIAL_LAYER
        elif ".shp" in path_fixture.suffixes:
            layer = path_fixture.stem
        path = path_fixture
    elif file_type == "nonzip":
        path_fixture = request.getfixturevalue(fixture)
        if ".gdb" in path_fixture.suffixes:
            path = path_fixture  # GDB directory is the addressable path itself
            layer = SPATIAL_LAYER
        elif ".shp" in path_fixture.suffixes:
            path = path_fixture.parent
            layer = path_fixture.name
    return {"path": path, "layer": layer}


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
        pytest.param(
            "temp_shp_zip_with_md_path",
            "zip",
            None,
            id="add_md_to_zip_shp_with_md",
        ),
        pytest.param(
            "temp_nonzipped_shp_with_md_path",
            "nonzip",
            None,
            id="add_md_to_nonzip_shp_with_md",
        ),
        pytest.param(
            "temp_shp_zip_with_subdir_path",
            "zip",
            SHP_SUBDIR,
            id="add_md_to_zip_shp_with_subdir",
        ),
        pytest.param(
            "temp_gdb_zip_path",
            "zip",
            None,
            id="add_md_to_zip_gdb",
        ),
        pytest.param(
            "temp_gdb_nonzipped_path",
            "nonzip",
            None,
            id="add_md_to_nonzip_gdb",
        ),
    ],
)
def test_write_metadata(
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
    file_metadata = product_md.calculate_file_dataset_metadata(file_id="primary_shapefile")

    fields = Metadata.model_fields

    # write metadata
    esri.write_metadata(
        product_name="colp",
        dataset_name="colp",
        path=fixture_info["path"],
        layer=fixture_info["layer"],
        file_id="primary_shapefile",
        zip_subdir=subdir,
        org_md=org_metadata,
    )

    # read it back
    metadata = None

    if ".gdb" in fixture_info["path"].suffixes:
        metadata = fgdb.read_metadata(
            gdb=fixture_info["path"], layer=fixture_info["layer"]
        )
    if ".shp" in fixture_info["layer"]:
        shp = shp_utils.from_path(
            path=fixture_info["path"], shp_name=fixture_info["layer"], zip_subdir=subdir
        )
        metadata = shp.read_metadata()

    if metadata is None:
        pytest.fail("Expected metadata to exist")

    # Test default values
    assert metadata.md_stan_name == fields["md_stan_name"].default
    assert metadata.md_stan_ver == fields["md_stan_ver"].default
    # TODO - add helper code to access nested defaults (if this is the direction we end up pursuing)

    # Test product-specific values
    assert metadata.md_hr_lv_name == "dataset"
    assert metadata.data_id_info.id_citation.res_title == file_metadata.attributes.display_name
    assert metadata.data_id_info.id_abs == file_metadata.attributes.description
    assert metadata.data_id_info.id_credit == file_metadata.attributes.attribution
    assert metadata.data_id_info.res_const.consts.use_limit == file_metadata.attributes.disclaimer
    assert metadata.data_id_info.other_keys.keyword == file_metadata.attributes.tags
    assert metadata.data_id_info.search_keys.keyword == file_metadata.attributes.tags

    assert metadata.eainfo.detailed.name == fixture_info["layer"].removesuffix(".shp")
    assert metadata.eainfo.detailed.enttyp.enttypl.value == fixture_info["layer"].removesuffix(".shp")
    assert metadata.eainfo.detailed.enttyp.enttypt.value == "Feature Class"

    # column 0 has no domain values: attrlabl, attrtype, and udom should all round-trip
    col0 = file_metadata.columns[0]
    expected_label_0 = "FID" if col0.id == "uid" else col0.name
    assert metadata.eainfo.detailed.attr[0].attrlabl.value == expected_label_0
    expected_type_0 = "OID" if col0.id == "uid" else esri._dcp_type_to_esri(col0.data_type)
    assert metadata.eainfo.detailed.attr[0].attrtype.value == expected_type_0
    udom_0 = metadata.eainfo.detailed.attr[0].attrdomv.udom
    assert udom_0 is not None
    assert udom_0.value == col0.description

    assert file_metadata.columns[1].values is not None, "Column values must be defined"

    # column 1 is borough in the colp test fixture — has domain values
    assert metadata.eainfo.detailed.attr[1].attrlabl.value == file_metadata.columns[1].name
    assert metadata.eainfo.detailed.attr[1].attrtype.value == esri._dcp_type_to_esri(
        file_metadata.columns[1].data_type
    )
    udom_1 = metadata.eainfo.detailed.attr[1].attrdomv.udom
    assert udom_1 is None or udom_1.value is None
    assert (
        metadata.eainfo.detailed.attr[1].attrdomv.edom[0].edomv
        == file_metadata.columns[1].values[0].value  # "1", when org_md product is colp
    )
    assert (
        metadata.eainfo.detailed.attr[1].attrdomv.edom[0].edomvd
        == file_metadata.columns[1].values[0].description  # "Manhattan", when org_md product is colp
    )


def _make_column(**kwargs) -> DatasetColumn:
    defaults = dict(id="some_field", name="Some Field", data_type="text", description="A description")
    return DatasetColumn(**(defaults | kwargs))


def test_create_attr_metadata_basic():
    col = _make_column(name="MyField", description="My desc", data_source="Agency X")
    attr = esri._create_attr_metadata(col)
    assert attr.attrlabl.value == "MyField"
    assert attr.attalias.value == "MyField"
    assert attr.attrdef.value == "My desc"
    assert attr.attrdefs.value == "Agency X"


def test_create_attr_metadata_uid_becomes_fid():
    col = _make_column(id="uid", name="uid")
    attr = esri._create_attr_metadata(col)
    assert attr.attrlabl.value == "FID"
    assert attr.attalias.value == "FID"


def test_create_attr_metadata_no_data_source():
    col = _make_column()
    attr = esri._create_attr_metadata(col)
    assert attr.attrdefs.value is None


def test_create_attr_metadata_with_values():
    col = _make_column(
        values=[
            ColumnValue(value="A", description="Alpha"),
            ColumnValue(value="B", description="Beta"),
        ]
    )
    attr = esri._create_attr_metadata(col)
    assert len(attr.attrdomv.edom) == 2
    assert attr.attrdomv.edom[0].edomv == "A"
    assert attr.attrdomv.edom[0].edomvd == "Alpha"
    assert attr.attrdomv.edom[1].edomv == "B"
    assert attr.attrdomv.edom[1].edomvd == "Beta"


def test_create_attr_metadata_no_values():
    col = _make_column()
    attr = esri._create_attr_metadata(col)
    assert attr.attrdomv.edom == []


def test_create_attr_metadata_attrtype_mapped():
    col = _make_column(data_type="text")
    assert esri._create_attr_metadata(col).attrtype.value == "String"

    col = _make_column(data_type="integer")
    assert esri._create_attr_metadata(col).attrtype.value == "Integer"

    col = _make_column(data_type="decimal")
    assert esri._create_attr_metadata(col).attrtype.value == "Double"

    col = _make_column(data_type="bbl")
    assert esri._create_attr_metadata(col).attrtype.value == "Double"

    col = _make_column(data_type="geometry")
    assert esri._create_attr_metadata(col).attrtype.value == "Geometry"


def test_create_attr_metadata_uid_attrtype_is_oid():
    col = _make_column(id="uid", name="uid", data_type="text")
    assert esri._create_attr_metadata(col).attrtype.value == "OID"


def test_dcp_type_to_esri_unknown_returns_none():
    assert esri._dcp_type_to_esri("custom_type") is None
    assert esri._dcp_type_to_esri(None) is None


def test_create_attr_metadata_udom_set_when_no_values():
    col = _make_column(description="Free-form text field")
    attr = esri._create_attr_metadata(col)
    assert attr.attrdomv.udom is not None
    assert attr.attrdomv.udom.value == "Free-form text field"


def test_create_attr_metadata_udom_none_when_values_present():
    col = _make_column(values=[ColumnValue(value="A", description="Alpha")])
    attr = esri._create_attr_metadata(col)
    assert attr.attrdomv.udom is None


def test_write_metadata_gdb_pluto(temp_gdb_zip_path, org_metadata):
    """Verifies GDB-specific metadata writing using the pluto test fixture.

    Checks:
    - eainfo.detailed.name is product_md.id ("pluto"), not the layer name
    - date column maps to Esri type "Date"
    - full column names are used (no shapefile truncation)
    """
    esri.write_metadata(
        product_name="pluto",
        dataset_name="pluto",
        path=temp_gdb_zip_path,
        layer=SPATIAL_LAYER,
        file_id="primary_file_geodatabase",
        zip_subdir=None,
        org_md=org_metadata,
    )
    metadata = fgdb.read_metadata(gdb=temp_gdb_zip_path, layer=SPATIAL_LAYER)
    if metadata is None:
        pytest.fail("Expected metadata to exist after write")

    pluto_md = org_metadata.product("pluto").dataset("pluto")
    file_metadata = pluto_md.calculate_file_dataset_metadata(file_id="primary_file_geodatabase")

    assert metadata.eainfo.detailed.name == SPATIAL_LAYER
    assert metadata.eainfo.detailed.enttyp.enttypl.value == SPATIAL_LAYER

    # uid → FID / OID
    assert metadata.eainfo.detailed.attr[0].attrlabl.value == "FID"
    assert metadata.eainfo.detailed.attr[0].attrtype.value == "OID"

    # borough — full name, has domain values (edom), no truncation
    assert metadata.eainfo.detailed.attr[1].attrlabl.value == file_metadata.columns[1].name
    assert metadata.eainfo.detailed.attr[1].attrtype.value == "String"
    assert len(metadata.eainfo.detailed.attr[1].attrdomv.edom) == len(file_metadata.columns[1].values)

    # appdate — date type maps to Esri "Date"
    assert metadata.eainfo.detailed.attr[2].attrlabl.value == "APPDate"
    assert metadata.eainfo.detailed.attr[2].attrtype.value == "Date"

    # projection — EPSG:2263 maps to refSysInfo
    ref_sys_id = metadata.ref_sys_info.ref_system.ref_sys_id
    assert ref_sys_id.id_code_space.value == "EPSG"
    assert ref_sys_id.ident_code.code == 2263


def test_write_metadata_raises_on_nested_gdb_zip(tmp_path, org_metadata):
    gdb_path = tmp_path / "test.gdb"
    gdb_path.mkdir()
    with pytest.raises(ValueError, match="Nested zipped GDBs are not supported"):
        esri.write_metadata(
            product_name="colp",
            dataset_name="colp",
            path=gdb_path,
            layer="some_layer",
            file_id="primary_shapefile",
            zip_subdir="some_subdir",
            org_md=org_metadata,
        )


def test_write_metadata_raises_on_unsupported_file_type(tmp_path, org_metadata):
    bad_path = tmp_path / "file.csv"
    bad_path.touch()
    with pytest.raises(ValueError, match="Unsupported file type"):
        esri.write_metadata(
            product_name="colp",
            dataset_name="colp",
            path=bad_path,
            layer="some_layer",
            file_id="primary_shapefile",
            zip_subdir=None,
            org_md=org_metadata,
        )
