import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from zipfile import ZIP_DEFLATED, ZipFile

# TODO - move unpack_multilayer_shapefile() from lifecycle/assemble.py
# TODO - add logic to create new shp output, rather than overwriting in place
# TODO - (possible) write `remove_metadata()` function


@dataclass
class Shapefile:
    shp_dir: Path
    shp_name: str


@dataclass
class ZippedShapefile:
    zip_path: Path
    subdir: str | None
    shp_name: str


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


def _validate_shp_input(shp_string: str | Path) -> None:
    """Ensure the input string conforms to the required format:
    Format if zip file: 'zip://path/to/file.zip!shapefile.shp'
    Format if not zip file: 'path/to/shapefile.shp'

    Args:
        shp_string (str | Path): Path to shapefile

    Raises:
        ValueError: Indicates when input does not conform to required format
    """
    shp_string = str(shp_string)
    if not shp_string.endswith(".shp"):
        raise ValueError("Filename must end with '.shp'")

    has_zip_prefix = shp_string.startswith("zip://")
    has_zip_suffix = ".zip!" in shp_string
    contains_zip = ".zip" in shp_string

    # Check for valid zip format: must have both prefix and suffix
    if has_zip_prefix and not has_zip_suffix:
        raise ValueError("Zip path format incomplete: missing '.zip!' suffix")

    # Check for invalid zip format: has .zip but incorrect structure
    if contains_zip and not (has_zip_prefix and has_zip_suffix):
        raise ValueError(
            "Invalid zip path format. Expected format: 'zip://path/to/file.zip!shapefile.shp'"
        )


def _parse_path_to_shp(path_to_shp: str | Path) -> Shapefile | ZippedShapefile:
    """
    Takes path to shapefile (shp) and returns relevant information, such as:
        - shp name
        - whether the shp is in a zip file
        - path to zip file (if it exists)
        - path to shp (starting at top level within zip, or complete path if no zip exists.)

    Args:
        path_to_shp (str):
            - Path to shapefile - must end in ".shp"
            - If shp is in a zip file: arg must be prefixed with "zip://"
            - If shp is in a zip file: zip file to contents must be delimited by "!"
            - example with zip file: "zip://path/to/file.zip!shapefile.shp"
            - example without zip file: "path/to/shapefile.shp"

    Returns:
        Shapefile | ZippedShapefile: Dataclasses with fields
        (see relevant class definitions for details.)
    """

    path_to_shp = str(path_to_shp)

    _validate_shp_input(path_to_shp)

    zip_indicator = "zip://"
    end_of_zip_delimiter = ".zip!"

    subdir = None

    if path_to_shp.startswith(zip_indicator):
        start_path_idx = len(zip_indicator)

        # Get zip path -------------------------
        if end_of_zip_delimiter in path_to_shp:
            end_of_zip_idx = path_to_shp.find(end_of_zip_delimiter) + (
                len(end_of_zip_delimiter) - 1
            )

            zip_path = path_to_shp[start_path_idx:end_of_zip_idx]

        # Get sub directory --------------------
        path_in_zip_to_shp = Path(path_to_shp[end_of_zip_idx + 1 :])
        if len(path_in_zip_to_shp.parts) > 1:
            subdir = str(path_in_zip_to_shp.parent)

        return ZippedShapefile(
            zip_path=Path(zip_path),
            subdir=subdir,
            shp_name=path_in_zip_to_shp.name,
        )
    else:
        return Shapefile(
            shp_dir=Path(path_to_shp).parent,
            shp_name=Path(path_to_shp).name,
        )


def _get_metadata_xml_name(file_list: list[str], shp_name: str) -> Optional[str]:
    """Extracts name of metadata .xml file matching specified .shp file from a
    list of filenames. Does not require an actual directory/zip location. Only
    parses a list of names as produced by other functions.
    Handles cases where metadata ends with either ".xml" or ".shp.xml"

    Args:
        file_list (list[str]): List of file names to parse
        shp_name (str): Shapefile name, not path, ending with ".shp"

    Raises:
        ValueError: Error if more than one .xml file matching the pattern are found.

    Returns:
        Optional[str]: Either name of metadata file, or None if no file is found
    """
    shp_stem = Path(shp_name).stem
    file_names = [Path(item).name for item in file_list]
    matched_names = [
        f for f in file_names if f in {f"{shp_stem}.shp.xml", f"{shp_stem}.xml"}
    ]
    match len(matched_names):
        case 0:
            return None
        case 1:
            return matched_names[0]
        case _:
            raise ValueError(
                f"Expected a single xml with name '{shp_stem}', but found {len(matched_names)}"
            )


def _list_files_in_shp_dir(path_to_shp: str | Path) -> list[str]:
    """Returns all files present at same level as specified {filename}.shp file

    Args:
        path_to_shp (str | Path):
            - Path to shapefile, must end in ".shp"
            - If shp is in a zip file: arg must be prefixed with "zip://"
            - If shp is in a zip file: zip file to contents must be delimited by "!"
            - example with zip file: "zip://path/to/file.zip!shapefile.shp"
            - example without zip file: "path/to/shapefile.shp"

    Returns:
        list[str]: List of files in same directory as specified shapefile
    """
    file = _parse_path_to_shp(path_to_shp=path_to_shp)

    match file:
        case ZippedShapefile():
            with ZipFile(file.zip_path, "r") as archive:
                return archive.namelist()
        case Shapefile():
            return [str(item.name) for item in file.shp_dir.iterdir()]
        case _:
            return []


def write_metadata(
    path_to_shp: str | Path,
    metadata: str,
    overwrite: bool = False,
) -> None:
    """Writes metadata to shapefile. Handles either zipped on unzipped shapefiles.
    If metadata already exists, this function can overwrite it or bypass it.

    Args:
        path_to_shp (str | Path):
            - Path to shapefile, must end in ".shp"
            - If shp is in a zip file: arg must be prefixed with "zip://"
            - If shp is in a zip file: zip file to contents must be delimited by "!"
            - example with zip file: "zip://path/to/file.zip!shapefile.shp"
            - example without zip file: "path/to/shapefile.shp"
        metadata (str): String containing metadata values in XML format.
        overwrite (bool, optional): If True, existing metadata will be overwritten.
            If False, function will not overwrite existing metadata. Defaults to False.
    """
    shp_info: ZippedShapefile | Shapefile = _parse_path_to_shp(path_to_shp=path_to_shp)
    xml_filename = f"{shp_info.shp_name}.xml"

    def _write_text_to_file(
        shp_info: ZippedShapefile | Shapefile,
        xml_filename: str,
        metadata: str,
    ) -> None:
        match shp_info:
            case ZippedShapefile():
                with ZipFile(shp_info.zip_path, "a", compression=ZIP_DEFLATED) as shp:
                    shp.writestr(xml_filename, metadata)
            case _:
                with open(Path(path_to_shp) / xml_filename, "w") as xml_file:
                    xml_file.write(metadata)

    if metadata_exists(path_to_shp) and overwrite:
        if overwrite:
            _remove_item_from_zip_file(
                zip_file=shp_info.zip_path, file_to_remove=xml_filename
            )
            _write_text_to_file(
                shp_info=shp_info,
                xml_filename=xml_filename,
                metadata=metadata,
            )
        else:
            raise FileExistsError(
                "Metadata XML already exists, and overwrite is False. Nothing will be written"
            )

    if not metadata_exists(path_to_shp):
        _write_text_to_file(
            shp_info=shp_info,
            xml_filename=xml_filename,
            metadata=metadata,
        )


# TODO - write this function
def remove_metadata():
    """Removes existing metadata from shapefile."""
    raise NotImplementedError("This function doesn't exist yet.")


def read_metadata(path_to_shp: str | Path, encoding: str = "utf-8") -> str:
    """_summary_

    Args:
        path_to_shp (str | Path):
            - Path to shapefile, must end in ".shp"
            - If shp is in a zip file: arg must be prefixed with "zip://"
            - If shp is in a zip file: zip file to contents must be delimited by "!"
            - example with zip file: "zip://path/to/file.zip!shapefile.shp"
            - example without zip file: "path/to/shapefile.shp"
        encoding (str, optional): Expected encoding of metadata file. Only relevant
            if metadata exists in a zip file. Defaults to "utf-8".

    Raises:
        ValueError: Error if no xml file exists with expected name.

    Returns:
        str: Metadata content as string.
    """
    shp_info: ZippedShapefile | Shapefile = _parse_path_to_shp(path_to_shp=path_to_shp)
    items_present: list[str] = _list_files_in_shp_dir(path_to_shp)
    xml_filename: Optional[str] = _get_metadata_xml_name(
        file_list=items_present, shp_name=shp_info.shp_name
    )
    if xml_filename is None:
        raise ValueError("Could not compute a metadata filename.")

    match shp_info:
        case ZippedShapefile():
            with ZipFile(shp_info.zip_path, "r") as zf:
                metadata = zf.read(xml_filename).decode(encoding=encoding)
                return metadata
        case Shapefile():
            path_to_xml = Path(shp_info.shp_dir) / xml_filename
            with open(path_to_xml, "r") as f:
                return f.read()


def metadata_exists(path_to_shp: str | Path) -> bool:
    """Detect whether shapefile has existing metadata.

    Args:
        path_to_shp (str | Path):
            - Path to shapefile, must end in ".shp"
            - If shp is in a zip file: arg must be prefixed with "zip://"
            - If shp is in a zip file: zip file to contents must be delimited by "!"
            - example with zip file: "zip://path/to/file.zip!shapefile.shp"
            - example without zip file: "path/to/shapefile.shp"

    Returns:
        bool: True if shapefile has metadata, False if no metadata is found.
    """
    shp_info: ZippedShapefile | Shapefile = _parse_path_to_shp(path_to_shp=path_to_shp)
    file_list: list[str] = _list_files_in_shp_dir(path_to_shp=path_to_shp)
    xml_filename: Optional[str] = _get_metadata_xml_name(
        file_list=file_list, shp_name=shp_info.shp_name
    )
    return xml_filename in file_list
