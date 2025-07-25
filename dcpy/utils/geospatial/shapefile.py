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
    shp_name: str

    def _get_metadata_xml_name(self, file_list: list[str]) -> Optional[str]:
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
        shp_stem = Path(self.shp_name).stem
        file_names = [Path(f).name for f in file_list]
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

    def _write_file(self, filename, contents):
        raise NotImplementedError("Not implemented on base class")

    def _remove_file(self, filename):
        raise NotImplementedError("Not implemented on base class")

    def _list_all_files(self):
        raise NotImplementedError("Not implemented on base class")

    def _read_file(self, filename) -> str:
        raise NotImplementedError("Not implemented on base class")


@dataclass
class ShapefileNonZipped(Shapefile):
    _shp_dir: Path

    def _write_file(self, filename, contents):
        with open(Path(self._shp_dir) / filename, "w") as xml_file:
            xml_file.write(contents)

    def _remove_file(self, filename):
        return

    def _list_all_files(self) -> list[str]:
        return [str(f.name) for f in self._shp_dir.iterdir()]

    def _read_file(self, filename) -> str:
        path_to_xml = Path(self._shp_dir) / filename
        with open(path_to_xml, "r") as f:
            return f.read()


@dataclass
class ShapefileZipped(Shapefile):
    _zip_path: Path
    _subdir: str | None

    def _write_file(self, filename, contents):
        with ZipFile(self._zip_path, "a", compression=ZIP_DEFLATED) as shp:
            shp.writestr(filename, contents)

    def _remove_file(self, filename):
        if self._subdir:
            filename = Path(self._subdir) / filename

        zip_file = Path(self._zip_path)
        temp_zip = zip_file.parent / (zip_file.name + ".tmp")
        with ZipFile(zip_file, "r") as old_zip:
            with ZipFile(temp_zip, "w", compression=ZIP_DEFLATED) as new_zip:
                for item in old_zip.infolist():
                    if item.filename != filename:
                        data = old_zip.read(item.filename)
                        new_zip.writestr(item, data)
        os.replace(temp_zip, zip_file)

    def _list_all_files(self) -> list[str]:
        with ZipFile(self._zip_path, "r") as archive:
            return archive.namelist()

    def _read_file(self, filename) -> str:
        with ZipFile(self._zip_path, "r") as zf:
            metadata = zf.read(filename).decode(encoding="utf-8")
            return metadata


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


def _parse_path_to_shp(path_to_shp: str | Path) -> Shapefile:
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
        Shapefile | ShapefileZipped: Dataclasses with relevant fields
        (see class definitions for details.)
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

        return ShapefileZipped(
            shp_name=path_in_zip_to_shp.name,
            _zip_path=Path(zip_path),
            _subdir=subdir,
        )
    else:
        return ShapefileNonZipped(
            shp_name=Path(path_to_shp).name,
            _shp_dir=Path(path_to_shp).parent,
        )


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
    shp_info: Shapefile = _parse_path_to_shp(path_to_shp=path_to_shp)
    # TODO - can the xml filename be moved to a class attr? If so - how to handle the possible file ext. combinations?
    xml_filename = f"{shp_info.shp_name}.xml"

    if metadata_exists(path_to_shp) and not overwrite:
        raise FileExistsError(
            "Metadata XML already exists, and overwrite is False. Nothing will be written"
        )
    if overwrite:
        # TODO - consider changing to .remove_metadata() 'external' fn, once it's been written
        shp_info._remove_file(xml_filename)

    shp_info._write_file(filename=xml_filename, contents=metadata)


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
    shp_info: Shapefile = _parse_path_to_shp(path_to_shp=path_to_shp)
    file_list: list[str] = shp_info._list_all_files()
    xml_filename: Optional[str] = shp_info._get_metadata_xml_name(file_list=file_list)
    if xml_filename is None:
        raise ValueError("Could not compute a metadata filename.")

    return shp_info._read_file(xml_filename)


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
    shp_info: Shapefile = _parse_path_to_shp(path_to_shp=path_to_shp)
    file_list: list[str] = shp_info._list_all_files()
    xml_filename: Optional[str] = shp_info._get_metadata_xml_name(file_list=file_list)
    return xml_filename in file_list
