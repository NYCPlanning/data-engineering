from dcpy.utils.geospatial import shapefile
from pytest import fixture
import shutil
import subprocess
from pathlib import Path
import zipfile


@fixture
def temp_shp_zip_path(resources_path, tmp_path):
    shutil.copy2(
        src=resources_path / "shapefile_single_pluto_feature.zip",
        dst=tmp_path / "shapefile_single_pluto_feature.zip",
    )
    return tmp_path / "shapefile_single_pluto_feature.zip"


@fixture
def temp_xml_path(resources_path, tmp_path):
    shutil.copy2(
        src=resources_path / "shapefile_metadata.xml",
        dst=tmp_path / "shapefile_metadata.xml",
    )
    return tmp_path / "shapefile_metadata.xml"


def test_unzip_shapefile(temp_shp_zip_path, tmp_path):
    shapefile.unpack_simple_shp(
        zip_file_path=Path(temp_shp_zip_path), unzip_to=tmp_path
    )
    # print(subprocess.run(["tree", tmp_path]))  # print dir tree of output
    assert zipfile.is_zipfile(temp_shp_zip_path), (
        f"'{temp_shp_zip_path.name}' is not a valid zip file"
    )
    assert (tmp_path / temp_shp_zip_path.stem).is_dir(), (
        f"No extracted dir present named '{temp_shp_zip_path.stem}'"
    )
    assert (
        tmp_path / temp_shp_zip_path.stem / f"{temp_shp_zip_path.stem}.shp"
    ).is_file(), "No shapefile present"


def test_shp_w_no_metadata(temp_shp_zip_path):
    assert shapefile.read_metadata(temp_shp_zip_path) == {}, (
        "No metadata should be present"
    )


def test_add_metadata_to_shp(temp_shp_zip_path, temp_xml_path, tmp_path):
    shapefile.unpack_simple_shp(
        zip_file_path=Path(temp_shp_zip_path), unzip_to=tmp_path
    )
    shp_dir = tmp_path / temp_shp_zip_path.stem
    input_xml_file = temp_xml_path
    output_xml_file = shp_dir / f"{shp_dir.name}.xml"
    print(subprocess.run(["tree", tmp_path]))  # print dir tree of output
    shapefile.add_metadata_xml_to_shp_dir(
        shp_dir=shp_dir, metadata_xml_file=input_xml_file
    )
    assert (output_xml_file).is_file(), (
        f"'...{shp_dir.name}/{output_xml_file.name}' is not a valid file"
    )
    # TODO - test actual output of xml file - valid xml, has a specific value, etc.
    # metadata = {}
    # assert shapefile.read_metadata(temp_shp_zip_path) == metadata, (
    #     "The correct metadata should be written"
    # )
