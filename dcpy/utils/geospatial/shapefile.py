import os
from pathlib import Path
import zipfile
from datetime import datetime
from dcpy.models.data.shapefile_metadata import (
    Metadata,
    Esri,
    Mddatest,
    Mdhrlv,
    Scopecd,
    Scalerange,
)

# TODO - move unpack_multilayer_shapefile() from lifecycle/assemble.py


class _FileManager:
    def __init__(self, path: Path):
        self.path = path

    def read_file(self, filename: str) -> str:
        with open(self.path / filename, "r") as f:
            return f.read()

    def write_file(self, filename: str, contents: str):
        with open(self.path / filename, "w") as f:
            f.write(contents)

    def metadata_exists(self, filename: str) -> bool:
        return (self.path / filename).is_file()

    def remove_file(self, filename: str):
        os.remove(self.path / filename)


class _FileManagerZipped:
    def __init__(self, path: Path, zip_subdir: str | None):
        self.zip_path = path  # ends in .zip
        self.zip_subdir = zip_subdir

    def read_file(self, filename: str) -> str:
        path_to_file_in_zip = (
            (f"{self.zip_subdir}/{filename}") if self.zip_subdir else filename
        )
        with zipfile.ZipFile(self.zip_path, "r") as zf:
            metadata: str = zf.read(path_to_file_in_zip).decode(encoding="utf-8")
            return metadata

    def write_file(self, filename: str, contents: str):
        with zipfile.ZipFile(
            self.zip_path, "a", compression=zipfile.ZIP_DEFLATED
        ) as zf:
            internal_path = (
                f"{self.zip_subdir}/{filename}" if self.zip_subdir else filename
            )
            zf.writestr(internal_path, contents)

    def metadata_exists(self, filename: str) -> bool:
        self.file_parent = (
            (zipfile.Path(self.zip_path) / self.zip_subdir)
            if self.zip_subdir
            else zipfile.Path(self.zip_path)
        )
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
    def __init__(
        self,
        path: Path,
        shp_name: str,
        zip_subdir: str | None = None,
    ):
        self.name: str = shp_name
        self.is_zipped = True if zipfile.is_zipfile(path) else False
        self.path = path
        self.zip_subdir = zip_subdir
        self.has_metadata = None
        self.file_manager = (
            _FileManagerZipped(path, zip_subdir)
            if self.is_zipped
            else _FileManager(path)
        )
        self._post_init()

    def _post_init(self):
        self.has_metadata = self.metadata_exists()

    def read_metadata(self):
        """Read shapefile metadata from file.
        Works for both zipped and non-zipped shapefiles.

        Returns:
        str: Metadata content as string.
        """
        xml: str = self.file_manager.read_file(f"{self.name}.xml")
        metadata = Metadata.from_xml(xml)

        return metadata

    def write_metadata(
        self,
        metadata: Metadata,
        overwrite: bool = False,
    ) -> None:
        """Write shapefile metadata.
        Works for both zipped and non-zipped shapefiles.

        Args:
            metadata (str): Metadata content to write to file.
            overwrite (bool, optional): Whether to overwrite existing metadata. Defaults to False.

        Raises:
            FileExistsError: Raises when metadata already exists and overwrite is set to False.
        """
        if self.metadata_exists() and not overwrite:
            raise FileExistsError(
                "Metadata XML already exists, and overwrite is False. Nothing will be written"
            )
        if overwrite:
            self.remove_metadata()

        md = str(metadata.to_xml())

        self.file_manager.write_file(filename=f"{self.name}.xml", contents=md)

    def metadata_exists(self):
        """Detect whether shapefile has existing metadata.

        Returns:
            bool: True if shapefile has metadata, False if no metadata is found.
        """
        return self.file_manager.metadata_exists(f"{self.name}.xml")

    def remove_metadata(self):
        """Removes existing metadata file from shapefile."""
        self.file_manager.remove_file(f"{self.name}.xml")

    def generate_metadata(self) -> Metadata:
        """Generates a default Esri metadata object"""
        return generate_metadata()


def from_path(
    path: Path,
    shp_name: str,
    zip_subdir: str | None = None,
) -> Shapefile:
    """Instantiate Shapefile object.

    Args:
        path (Path): Path to directory or zip file containing shapefile.
        shp_name (str): Name of shapefile. Example: "filename.shp"
        zip_subdir (str | None): Path to shapefile within zip file, if relevant. Defaults to None.

    Returns:
        Shapefile: See Shapefile class definition.
    """
    return Shapefile(path=path, shp_name=shp_name, zip_subdir=zip_subdir)


def generate_metadata() -> Metadata:
    """Generates a default Esri metadata object"""

    def _get_esri_timestamp(dt_obj=None):
        """
        Generate Esri-style CreaDate and CreaTime values.

        Args:
            dt_obj: datetime object (uses current time if None)

        Returns:
            tuple: (CreaDate, CreaTime) as strings
        """
        if dt_obj is None:
            dt_obj = datetime.now()

        # CreaDate: YYYYMMDD
        crea_date = dt_obj.strftime("%Y%m%d")

        # CreaTime: HHMMSSFF (hours, minutes, seconds, hundredths)
        hundredths = 0  # Esri appears to ignore the hundredths in practice
        crea_time = (
            f"{dt_obj.hour:02d}{dt_obj.minute:02d}{dt_obj.second:02d}{hundredths:02d}"
        )

        return crea_date, crea_time

    datestamp, timestamp = _get_esri_timestamp()
    md_date_st = Mddatest(
        sync="TRUE",
        value=datestamp,
    )
    scale_range = Scalerange(
        min_scale="150000000",
        max_scale="5000",
    )
    esri = Esri(
        crea_date=datestamp,
        crea_time=timestamp,
        arc_gis_format="1.0",
        sync_once="TRUE",
        scale_range=scale_range,
        arc_gis_profile="ISO19139",
    )
    metadata = Metadata(
        lang="en",
        esri=esri,
        md_hr_lv=Mdhrlv(scope_cd=Scopecd(value="005")),
        md_date_st=md_date_st,
    )
    return metadata


# def _validate_shp_input(shp_string: str | Path) -> None:
#     """Ensure the input string conforms to the required format:
#     Format if zip file: 'zip://path/to/file.zip!shapefile.shp'
#     Format if not zip file: 'path/to/shapefile.shp'

#     Args:
#         shp_string (str | Path): Path to shapefile

#     Raises:
#         ValueError: Indicates when input does not conform to required format
#     """
#     shp_string = str(shp_string)
#     if not shp_string.endswith(".shp"):
#         raise ValueError("Filename must end with '.shp'")

#     has_zip_prefix = shp_string.startswith("zip://")
#     has_zip_suffix = ".zip!" in shp_string
#     contains_zip = ".zip" in shp_string

#     # Check for valid zip format: must have both prefix and suffix
#     if has_zip_prefix and not has_zip_suffix:
#         raise ValueError("Zip path format incomplete: missing '.zip!' suffix")

#     # Check for invalid zip format: has .zip but incorrect structure
#     if contains_zip and not (has_zip_prefix and has_zip_suffix):
#         raise ValueError(
#             "Invalid zip path format. Expected format: 'zip://path/to/file.zip!shapefile.shp'"
#         )


# def _parse_path_to_shp(path_to_shp: str | Path) -> dict:
#     """
#     Takes path to shapefile (shp) and returns relevant information, such as:
#         - shp name
#         - whether the shp is in a zip file
#         - path to zip file (if it exists)
#         - path to shp (starting at top level within zip, or complete path if no zip exists.)

#     Args:
#         path_to_shp (str):
#             - Path to shapefile - must end in ".shp"
#             - If shp is in a zip file: arg must be prefixed with "zip://"
#             - If shp is in a zip file: zip file to contents must be delimited by "!"
#             - example with zip file: "zip://path/to/file.zip!shapefile.shp"
#             - example without zip file: "path/to/shapefile.shp"

#     Returns:
#         Shapefile | ShapefileZipped: Dataclasses with relevant fields
#         (see class definitions for details.)
#     """

#     path_to_shp = str(path_to_shp)

#     _validate_shp_input(path_to_shp)

#     output = {
#         "name": str,
#         "is_zipped": bool,
#         "shp_dir": Path | None,
#         "zip_path": Path | None,
#         "zip_subdir": str | None,
#     }

#     zip_indicator = "zip://"
#     end_of_zip_delimiter = ".zip!"

#     zip_subdir = None

#     if path_to_shp.startswith(zip_indicator):
#         start_path_idx = len(zip_indicator)

#         # Get zip path -------------------------
#         # TODO - remove conditional here - redundant when we're running validator func already
#         if end_of_zip_delimiter in path_to_shp:
#             end_of_zip_idx = path_to_shp.find(end_of_zip_delimiter) + (
#                 len(end_of_zip_delimiter) - 1
#             )

#             zip_path = path_to_shp[start_path_idx:end_of_zip_idx]

#         # Get sub directory --------------------
#         path_in_zip_to_shp = Path(path_to_shp[end_of_zip_idx + 1 :])
#         if len(path_in_zip_to_shp.parts) > 1:
#             zip_subdir = str(path_in_zip_to_shp.parent)

#         output["name"] = path_in_zip_to_shp.name
#         output["is_zipped"] = True
#         output["shp_dir"] = None
#         output["zip_path"] = zip_path
#         output["zip_subdir"] = zip_subdir

#     else:
#         output["name"] = Path(path_to_shp).name
#         output["is_zipped"] = False
#         output["shp_dir"] = Path(path_to_shp).parent
#         output["zip_path"] = None
#         output["zip_subdir"] = None

#     return output
