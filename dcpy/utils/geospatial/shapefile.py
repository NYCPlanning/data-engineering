import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import zipfile

# TODO - move unpack_multilayer_shapefile() from lifecycle/assemble.py
# TODO - add logic to create new shp output, rather than overwriting in place
# TODO - (possible) write `remove_metadata()` function


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


def _parse_path_to_shp(path_to_shp: str | Path) -> dict:
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

    output = {
        "name": str,
        "is_zipped": bool,
        "shp_dir": Path | None,
        "zip_path": Path | None,
        "zip_subdir": str | None,
    }

    zip_indicator = "zip://"
    end_of_zip_delimiter = ".zip!"

    zip_subdir = None

    if path_to_shp.startswith(zip_indicator):
        start_path_idx = len(zip_indicator)

        # Get zip path -------------------------
        # TODO - remove conditional here - redundant when we're running validator func already
        if end_of_zip_delimiter in path_to_shp:
            end_of_zip_idx = path_to_shp.find(end_of_zip_delimiter) + (
                len(end_of_zip_delimiter) - 1
            )

            zip_path = path_to_shp[start_path_idx:end_of_zip_idx]

        # Get sub directory --------------------
        path_in_zip_to_shp = Path(path_to_shp[end_of_zip_idx + 1 :])
        if len(path_in_zip_to_shp.parts) > 1:
            zip_subdir = str(path_in_zip_to_shp.parent)

        output["name"] = path_in_zip_to_shp.name
        output["is_zipped"] = True
        output["shp_dir"] = None
        output["zip_path"] = zip_path
        output["zip_subdir"] = zip_subdir

        # return ShapefileZipped(
        #     shp_name=path_in_zip_to_shp.name,
        #     _zip_path=Path(zip_path),
        #     _subdir=zip_subdir,
        # )
    else:
        output["name"] = Path(path_to_shp).name
        output["is_zipped"] = False
        output["shp_dir"] = Path(path_to_shp).parent
        output["zip_path"] = None
        output["zip_subdir"] = None

        # return ShapefileNonZipped(
        #     shp_name=Path(path_to_shp).name,
        #     _shp_dir=Path(path_to_shp).parent,
        # )
    return output


class _FileManager:
    def __init__(self, attributes):
        self.attributes = attributes
        self.path = Path(attributes["shp_dir"])

    def read_file(self, filename):
        with open(self.path / filename, "r") as f:
            return f.read()

    def write_file(self, filename, contents: str):
        with open(self.path / filename, "w") as f:
            f.write(contents)

    def metadata_exists(self, filename):
        return (self.path / filename).is_file()

    def remove_file(self, filename):
        os.remove(self.path / filename)


class _FileManagerZipped:
    def __init__(self, attributes: dict):
        self.attributes = attributes
        self.zip_path = Path(attributes["zip_path"])  # ends in .zip
        self.zip_subdir = attributes["zip_subdir"]
        self.file_parent = (
            (zipfile.Path(self.zip_path) / self.zip_subdir)
            if self.zip_subdir
            else zipfile.Path(self.zip_path)
        )

    def read_file(self, filename):
        return (self.file_parent / filename).read_text()

    def write_file(self, filename, contents):
        with zipfile.ZipFile(
            self.zip_path, "a", compression=zipfile.ZIP_DEFLATED
        ) as zf:
            internal_path = (
                f"{self.zip_subdir}/{filename}" if self.zip_subdir else filename
            )
            zf.writestr(internal_path, contents)

    def metadata_exists(self, filename):
        return (self.file_parent / filename).is_file()

    def remove_file(self, filename):
        temp_zip = self.zip_path.parent / (self.zip_path.name + ".tmp")
        with zipfile.ZipFile(self.zip_path, "r") as old_zip:
            with zipfile.ZipFile(
                temp_zip, "w", compression=zipfile.ZIP_DEFLATED
            ) as new_zip:
                for item in old_zip.infolist():
                    if not item.filename.endswith(filename):
                        data = old_zip.read(item.filename)
                        new_zip.writestr(item, data)

        os.replace(temp_zip, self.zip_path)


class Shapefile:
    def __init__(self, input_string: str | Path):
        values: dict = _parse_path_to_shp(input_string)
        self.input_string = input_string
        self.name: str = values["name"]
        self.is_zipped = values["is_zipped"]
        self.shp_dir = values["shp_dir"]
        self.zip_path = values["zip_path"]
        self.zip_subdir = values["zip_subdir"]
        self.file_manager = (
            _FileManagerZipped(values) if self.is_zipped else _FileManager(values)
        )

    def read_metadata(self):
        return self.file_manager.read_file(f"{self.name}.xml")

    def write_metadata(self, contents: str):
        self.file_manager.write_file(f"{self.name}.xml", contents)

    def metadata_exists(self):
        return self.file_manager.metadata_exists(f"{self.name}.xml")

    def remove_metadata(self):
        self.file_manager.remove_file(f"{self.name}.xml")


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
    shp: Shapefile = Shapefile(path_to_shp)

    return shp.read_metadata()


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
    shp: Shapefile = Shapefile(path_to_shp)

    return shp.metadata_exists()


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
    shp: Shapefile = Shapefile(path_to_shp)

    if metadata_exists(path_to_shp) and not overwrite:
        raise FileExistsError(
            "Metadata XML already exists, and overwrite is False. Nothing will be written"
        )
    if overwrite:
        remove_metadata(path_to_shp)

    shp.write_metadata(contents=metadata)


# TODO - write this function
def remove_metadata(path_to_shp: str | Path):
    """Removes existing metadata file from shapefile."""
    shp: Shapefile = Shapefile(path_to_shp)
    shp.remove_metadata()
