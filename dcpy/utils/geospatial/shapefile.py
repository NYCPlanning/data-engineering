import shutil
from pathlib import Path


def placeholder():
    return "Hello shapefiles!"


# TODO - explore using zipfile to add xml metadata within context manager
# TODO - move unpack_multilayer_shapefile() from lifecycle/assemble.py
def unpack_simple_shp(zip_file_path: Path, unzip_to: Path):
    """Unpacks a simple zipped shapefile into a directory of the same name as the shapefile.
    "Simple" in this context means that the zip file contains one shapefile at the top level
    of the zip file.

    Args:
        zip_file_path (Path): Path to zip file to be unpacked
        unzip_to (Path): Path to parent of directory containing unzipped files
    """
    new_shp_path = unzip_to / zip_file_path.stem
    new_shp_path.mkdir(exist_ok=True)
    shutil.unpack_archive(filename=zip_file_path, extract_dir=new_shp_path)


def add_simple_shp_to_archive(): ...


def read_metadata(path):
    return {}


def write_metadata(path, metadata):
    pass
