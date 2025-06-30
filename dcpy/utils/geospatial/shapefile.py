import os
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
from typing import Optional

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
# TODO - add logic to create new shp output, rather than overwriting in place
# TODO - (possible) write `remove_metadata()` function


def _remove_item_from_zip_file(zip_file: str | Path, file_to_remove: str):
    zip_file = Path(zip_file)
    temp_zip = zip_file.parent / (zip_file.name + ".tmp")

    with ZipFile(zip_file, "r") as old_zip:
        with ZipFile(temp_zip, "w", compression=ZIP_DEFLATED) as new_zip:
            for item in old_zip.infolist():
                if item.filename != file_to_remove:
                    data = old_zip.read(item.filename)
                    new_zip.writestr(item, data)
    os.replace(temp_zip, zip_file)


def _parse_path_to_shp(shp_filename: str | Path) -> dict:
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
        raise ValueError("Filename must end with '.shp'")

    has_zip_prefix = shp_filename.startswith("zip://")
    has_zip_suffix = ".zip!" in shp_filename
    contains_zip = ".zip" in shp_filename

    # Check for valid zip format: must have both prefix and suffix
    if has_zip_prefix and not has_zip_suffix:
        raise ValueError("Zip path format incomplete: missing '.zip!' suffix")

    # Check for invalid zip format: has .zip but incorrect structure
    if contains_zip and not (has_zip_prefix and has_zip_suffix):
        raise ValueError(
            "Invalid zip path format. Expected format: 'zip://path/to/file.zip!shapefile.shp'"
        )

    # TODO - should this return path objects for the relevant values, or handle empty values more safely?
    output = {
        "dir_containing_shp": "",
        "path_to_zip": "",
        "shp_name": "",
        "is_zip": False,
    }
    # indicators / delimiters
    zip_indicator = "zip://"
    end_of_zip_delimiter = ".zip!"

    if shp_filename.startswith(zip_indicator):  # syntax indicating a zip file
        start_path_idx = len(zip_indicator)

        # Set zip bool ------------------------
        output["is_zip"] = True

        # Set zip dir -------------------------
        if end_of_zip_delimiter in shp_filename:
            end_of_zip_idx = shp_filename.find(end_of_zip_delimiter) + (
                len(end_of_zip_delimiter) - 1
            )

            output["path_to_zip"] = shp_filename[start_path_idx:end_of_zip_idx]

        # Set shp dir -------------------------
        path_in_zip_to_shp = Path(shp_filename[end_of_zip_idx + 1 :])
        if len(path_in_zip_to_shp.parts) > 1:
            output["dir_containing_shp"] = str(path_in_zip_to_shp.parent)

        # Set shp name ------------------------
        output["shp_name"] = path_in_zip_to_shp.name
    else:
        # Set shp dir -------------------------
        output["dir_containing_shp"] = str(Path(shp_filename).parent)

        # Set shp name ------------------------
        output["shp_name"] = Path(shp_filename).name

    return output


def write_metadata(
    path_to_shp: str | Path,
    metadata: str,
    overwrite: bool = False,
) -> None:
    shp_info: dict = _parse_path_to_shp(shp_filename=path_to_shp)

    xml_output = f"{shp_info['shp_name']}.xml"
    # TODO - incorporate metadata_exists() fn instead of custom code below
    # process zip files ------------------------
    if shp_info["is_zip"]:
        with ZipFile(shp_info["path_to_zip"], "r") as zf:
            items_in_zip = zf.namelist()
        # handle existing metadata
        if xml_output in items_in_zip:
            if overwrite:
                _remove_item_from_zip_file(
                    zip_file=shp_info["path_to_zip"], file_to_remove=xml_output
                )

                with ZipFile(
                    shp_info["path_to_zip"], "a", compression=ZIP_DEFLATED
                ) as shp:
                    shp.writestr(xml_output, metadata)

            if not overwrite:
                pass

        else:
            with ZipFile(shp_info["path_to_zip"], "a", compression=ZIP_DEFLATED) as shp:
                shp.writestr(xml_output, metadata)

    # process non zipped files -------------------
    elif not shp_info["is_zip"]:
        # handle existing metadata
        if (Path(shp_info["dir_containing_shp"] / xml_output)).is_file():
            if overwrite:
                with open(xml_output, "w") as xml_file:
                    xml_file.write(metadata)
            if not overwrite:
                pass


# TODO - write this function
def remove_metadata():
    return "This functionality has not been added yet"


def _get_metadata_xml_name(file_list: list[str], shp_name: str) -> Optional[str]:
    shp_stem = Path(shp_name).stem
    file_list_names_only = [Path(item).name for item in file_list]
    matched_files = [
        item
        for item in file_list_names_only
        if item.startswith(shp_stem) and item.endswith((".xml", ".shp.xml"))
    ]
    match len(matched_files):
        case 0:
            return None
        case 1:
            return matched_files[0]
        case _:
            raise ValueError(
                f"Expected a single xml with name '{shp_stem}', but found {len(matched_files)}"
            )


def read_metadata(path_to_shp: str | Path, encoding: str = "utf-8") -> str:
    shp_info: dict = _parse_path_to_shp(shp_filename=path_to_shp)
    items_present: list[str] = _list_files_in_shp_dir(path_to_shp)
    xml_filename: Optional[str] = _get_metadata_xml_name(
        file_list=items_present, shp_name=shp_info["shp_name"]
    )
    if xml_filename is None:
        raise ValueError("No xml file found with that name.")

    # access zip files ------------------------
    if shp_info["is_zip"]:
        with ZipFile(shp_info["path_to_zip"], "r") as zf:
            metadata = zf.read(xml_filename).decode(encoding=encoding)
            return metadata

    # access non zipped files -------------------
    else:
        path_to_xml = Path(shp_info["dir_containing_shp"]) / xml_filename
        with open(path_to_xml, "r") as f:
            return f.read()


def _list_files_in_shp_dir(path_to_shp: str | Path) -> list[str]:
    """Returns all files present at same level as specified {filename}.shp file

    Args:
        path_to_shp (Path):
            - Path to shapefile, must end in ".shp"
            - If shp is in a zip file: arg must be prefixed with "zip://"
            - If shp is in a zip file: zip file to contents must be delimited by "!"
            - example with zip file: "zip:///Users/name/files.zip!data/gadm36_AFG_1.shp"
            - example without zip file: "/Users/name/files/data/gadm36_AFG_1.shp"

    Returns:
        list[str]: List of files in same directory as specified shapefile
    """
    shp_info: dict = _parse_path_to_shp(shp_filename=path_to_shp)

    # process zip files ------------------------
    if shp_info["is_zip"]:
        with ZipFile(shp_info["path_to_zip"], "r") as archive:
            return archive.namelist()

    # process non zipped files -----------------
    elif not shp_info["is_zip"]:
        if not shp_info["dir_containing_shp"]:
            shp_dir = Path(os.getcwd())
        else:
            shp_dir = Path(shp_info["dir_containing_shp"])
        return [str(item) for item in shp_dir.iterdir()]
    else:
        raise Exception("More than one .xml files found that match the name provided")


def metadata_exists(path_to_shp: str | Path) -> bool:
    shp_info: dict = _parse_path_to_shp(shp_filename=path_to_shp)
    file_list: list[str] = _list_files_in_shp_dir(path_to_shp=path_to_shp)
    xml_filename: str | None = _get_metadata_xml_name(
        file_list=file_list, shp_name=shp_info["shp_name"]
    )
    if xml_filename is not None and xml_filename in file_list:
        return True
    else:
        return False
