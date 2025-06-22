import os
import xml.etree.ElementTree as ET
from pathlib import Path
from zipfile import ZipFile

metadata_xml_template = """
<?xml version="1.0"?>
<metadata xml:lang="en">
    <Esri>
        <CreaDate></CreaDate>
        <CreaTime></CreaTime>
        <ArcGISFormat>1.0</ArcGISFormat>
        <SyncOnce>TRUE</SyncOnce>
        <DataProperties>
            <lineage>
                <Process></Process>
            </lineage>
        </DataProperties>
        <scaleRange>
            <minScale>150000000</minScale>
            <maxScale>5000</maxScale>
        </scaleRange>
        <ArcGISProfile></ArcGISProfile>
    </Esri>
    <dataIdInfo>
        <idCitation>
            <resTitle></resTitle>
        </idCitation>
    </dataIdInfo>
    <mdHrLv>
        <ScopeCd value="005"></ScopeCd>
    </mdHrLv>
    <mdDateSt Sync="TRUE"></mdDateSt>
</metadata>
"""


# TODO - move unpack_multilayer_shapefile() from lifecycle/assemble.py
# TODO - account for more complex nesting within zip
# TODO - add argument create new shp output, rather than overwriting in place
# DONE - explore using zipfile to add xml metadata within context manager
# DONE - swap out xml Path for str - adjust to read from stringified XML template
# DONE - call from within shapefile fxn
# DONE - add flag for xml overwrite vs edit
# OR - add flag to level above this fxn, and then either
#   call this one or an update xml fxn, depending on logical path


# TODO - complete function to read shapefile metadata from XML file
# def read_metadata(path_to_zip: Path, shp_name: str, return_as: str = "xml" | "str"):
#     # TODO - add logic to read xml from zipped shp
#     if return_as.lower == "xml":
#         return ET.parse(...)
#     elif return_as.lower == "str":
#         with open(...) as metadata:
#             return metadata.read()


# DONE - determine from input path string whether provided path
# is a zip file, whether it is a folder containing 1 shp or
# more than one shp. See geopandas.read_file() for syntax.
# TODO - verify with Alex, and fold into other functions if approved
def _parse_path_to_shp(shp_filename: str) -> dict:
    """
    Takes path to shapefile (shp) and returns relevant information, such as:
        - shp name
        - whether the shp is in a zip file
        - path to zip file (if it exists)
        - path to shp (starting at top level within zip, or complete path if no zip exists.)

    Args:
        shp_filename (str):
            - Path to shapefile, must end in ".shp"
            - If shp is in a zip file: arg must be prefixed with "zip://"
            - If shp is in a zip file: zip file to contents must be delimited by "!"
            - example with zip file: "zip:///Users/name/files.zip!data/gadm36_AFG_1.shp"
            - example without zip file: "/Users/name/files/data/gadm36_AFG_1.shp"

    Returns:
        dict: _description_
    """
    shp_filename = str(shp_filename)
    if not shp_filename.endswith(".shp"):
        raise Exception("Filename must be a full shapefile path, ending with '.shp'")

    # TODO - should this return path objects for the relevant values?
    output = {
        "dir_containing_shp": "",
        "path_to_zip": "",
        "shp_name": "",
        "zip": False,
    }
    # indicators / delimiters
    zip_indicator = "zip://"
    end_of_zip_indicator = ".zip!"

    if shp_filename.startswith(zip_indicator):  # syntax indicating a zip file
        start_path_idx = len(zip_indicator)

        # Get zip bool ------------------------
        output["zip"] = True

        # Get zip dir -------------------------
        if end_of_zip_indicator in shp_filename:
            end_of_zip_idx = shp_filename.find(end_of_zip_indicator) + (
                len(end_of_zip_indicator) - 1
            )

            output["path_to_zip"] = shp_filename[start_path_idx:end_of_zip_idx]

        # Get shp dir -------------------------
        path_in_zip_to_shp = Path(shp_filename[end_of_zip_idx + 1 :])
        if len(path_in_zip_to_shp.parts) > 1:
            output["dir_containing_shp"] = str(path_in_zip_to_shp.parent)

        # Get shp name ------------------------
        output["shp_name"] = path_in_zip_to_shp.name
    else:
        # Get shp dir -------------------------
        output["dir_containing_shp"] = str(Path(shp_filename).parent)

        # Get shp name ------------------------
        output["shp_name"] = Path(shp_filename).name

    return output


def _remove_item_from_zip_file(zip_file: Path, file_to_remove: str):
    temp_zip = zip_file.parent / (zip_file.name + ".tmp")

    with ZipFile(zip_file, "r") as old_zip:
        with ZipFile(temp_zip, "w") as new_zip:
            for item in old_zip.infolist():
                if item.filename != file_to_remove:
                    data = old_zip.read(item.filename)
                    new_zip.writestr(item, data)
    os.replace(temp_zip, zip_file)


def remove_metadata(): ...


def write_metadata(
    path_to_shp: Path, metadata: str, shp_name: str, force: bool = False
) -> None:
    # coerce provided shp name to include '.shp' suffix, if not already
    if not shp_name.endswith(".shp"):
        shp_name = f"{shp_name}.shp"

    xml_output = f"{shp_name}.xml"

    with ZipFile(path_to_shp, "r") as zf:
        items_in_zip = zf.namelist()

    if xml_output in items_in_zip and force is False:
        return print(
            "Metadata file already exists, and 'force' is set to False. Nothing will be overwritten."
        )

    if xml_output in items_in_zip and force is True:
        _remove_item_from_zip_file(zip_file=path_to_shp, file_to_remove=xml_output)

    with ZipFile(path_to_shp, "a") as shp:
        if shp_name in shp.namelist():
            shp.writestr(xml_output, metadata)
        else:
            print(f"\n'{shp_name}' not found in zip contents:")
            print(f"{shp.printdir()}")


def get_contents(zip_file: Path) -> list:
    with ZipFile(zip_file, "r") as archive:
        return archive.namelist()


##---------------------------------------------------------------
## Functions using shutil module - will likely get rid of these


# def _unpack_simple_shp(zip_file_path: Path, unzip_to: Path) -> Path:
#     """Unpacks a simple zipped shapefile into a directory of the same name as the shapefile.

#     "Simple" in this context means that the zip file contains one shapefile at the top level
#     of the zip file.

#     Args:
#         zip_file_path (Path): Path to zip file to be unpacked
#         unzip_to (Path): Path to parent of directory containing unzipped files
#     """
#     new_shp_path = unzip_to / zip_file_path.stem
#     new_shp_path.mkdir(exist_ok=True)
#     shutil.unpack_archive(filename=zip_file_path, extract_dir=new_shp_path)
#     return new_shp_path


# def _add_metadata_xml_to_shp_dir(
#     shp_dir: Path, metadata_xml_str: str, shp_name: str
# ) -> None:
#     """Writes a string to new .xml file within a directory containing one or more shapefiles.
#     If a shp name is provided, the resulting .shp.xml is given that name.

#     Args:
#         shp_dir (Path): Path to the directory containing loose shp constituent files.
#         metadata_xml_str (str): .xml content as string.
#         shp_name (str): Name of shp relevant to .xml. Should not
#             include ".shp" in name.
#     """
#     out_xml_name = shp_dir / f"{shp_name}.shp.xml"

#     with open(out_xml_name, "w") as xml_file:
#         xml_file.write(metadata_xml_str)


# def write_metadata(path: Path, metadata: str, overwrite: bool = False):
#     # get the metadata - defined at top of notebook
#     # I think these next two should be a different function altogether:
#         # xml.etree - read xml template (handle case where xml exists, and shouldn't be overwritten)
#         # set values - create date, etc.
#     with
#     # call shp unzip function
#     _unpack_simple_shp(zip_file_path=path, unzip_to=)
#     # call add metadata xml to shp internal function (must be edited to accept xml string, not Path)
#     # zip back up
#     pass
