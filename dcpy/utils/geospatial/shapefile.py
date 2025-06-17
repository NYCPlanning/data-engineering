import shutil
from pathlib import Path
import os


def placeholder() -> str:
    return "Hello shapefiles!"


# TODO - explore using zipfile to add xml metadata within context manager
# TODO - move unpack_multilayer_shapefile() from lifecycle/assemble.py
def unpack_simple_shp(zip_file_path: Path, unzip_to: Path) -> None:
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


def add_metadata_xml_to_shp_dir(
    shp_dir: Path, metadata_xml_file: Path, shp_name: None | str = None
) -> None:
    """Copies an existing .xml file into a directory containing one or more shapefiles.
    If no shp (shapefile) name is provided, the resulting .shp.xml file is named after
    the directory.
    If a shp name is provided, the resulting .shp.xml is given that name.

    Args:
        shp_dir (Path): Path to the directory containing loose shp constituent files.
        metadata_xml_file (Path): Path the to source .xml file.
        shp_name (None | str, optional): Name of shp relevant to .xml. Should not
            include ".shp" in name. Defaults to None.
    """
    shutil.copy2(src=metadata_xml_file, dst=shp_dir)
    final_xml_name = shp_dir / f"{shp_dir.name}.xml"
    if shp_name is not None:
        final_xml_name = shp_dir / f"{shp_name}.shp.xml"
    os.rename(src=shp_dir / metadata_xml_file.name, dst=final_xml_name)
    # TODO - verify that the .xml filename matches a .shp filename present in dir


def zip_simple_shp(): ...


def read_metadata(path):
    return {}


def write_metadata(path, metadata):
    pass
