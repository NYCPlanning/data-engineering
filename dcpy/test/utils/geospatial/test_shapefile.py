from dcpy.utils.geospatial import shapefile
from pytest import fixture
import shutil
import subprocess
from pathlib import Path


@fixture
def temp_shp_path(resources_path, tmp_path):
    shutil.copy2(
        src=resources_path / "shapefile_single_pluto_feature.zip",
        dst=tmp_path / "shapefile_single_pluto_feature.zip",
    )
    return tmp_path / "shapefile_single_pluto_feature.zip"


def test_unzip_shapefile(temp_shp_path, tmp_path):
    shapefile.unpack_simple_shp(zip_file_path=Path(temp_shp_path), unzip_to=tmp_path)
    print(subprocess.run(["tree", tmp_path]))  # print dir tree of output
    assert (tmp_path / temp_shp_path.stem).is_dir(), (
        f"No extracted dir present named '{temp_shp_path.stem}'"
    )
    assert (tmp_path / temp_shp_path.stem / f"{temp_shp_path.stem}.shp").is_file(), (
        "No shapefile present"
    )


def test_shp_w_no_metadata(temp_shp_path):
    assert shapefile.read_metadata(temp_shp_path) == {}, "No metadata should be present"


def test_add_metadata_to_shp(temp_shp_path):
    metadata = {"description": "This is a dataset with some information."}
    shapefile.write_metadata(temp_shp_path, metadata)
    assert shapefile.read_metadata(temp_shp_path) == metadata, (
        "The correct metadata should be written"
    )
