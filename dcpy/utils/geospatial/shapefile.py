import os
import zipfile
from pathlib import Path

from dcpy.models.data.shapefile_metadata import Metadata
from dcpy.utils.geospatial.metadata import generate_metadata

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


# TODO - ensure that .write_metadata() includes "<?xml version="1.0"?>" at top of file
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

        xml_data = metadata.to_xml()
        md = xml_data.decode("utf-8") if isinstance(xml_data, bytes) else xml_data

        self.file_manager.write_file(filename=f"{self.name}.xml", contents=md)

    def metadata_exists(self):
        """Detect whether shapefile has existing metadata.

        Returns:
            bool: True if shapefile has metadata, False if no metadata is found.
        """
        return self.file_manager.metadata_exists(f"{self.name}.xml")

    def remove_metadata(self):
        """Removes existing metadata file from shapefile."""
        if self.metadata_exists():
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
