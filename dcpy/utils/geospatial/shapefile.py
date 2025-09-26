import os
from pathlib import Path
import zipfile
from dataclasses import dataclass, field
from lxml import objectify, etree
from typing import Optional, List
from datetime import datetime
import xml.etree.ElementTree as ET  # TODO remove me, once lxml is implemented
# from xml.etree.ElementTree import ParseError

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

    def write_file(self, filename: str, contents):
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
        return self.file_manager.read_file(f"{self.name}.xml")

    def write_metadata(
        self,
        metadata: str,
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

        self.file_manager.write_file(f"{self.name}.xml", metadata)

    def metadata_exists(self):
        """Detect whether shapefile has existing metadata.

        Returns:
            bool: True if shapefile has metadata, False if no metadata is found.
        """
        return self.file_manager.metadata_exists(f"{self.name}.xml")

    def remove_metadata(self):
        """Removes existing metadata file from shapefile."""
        self.file_manager.remove_file(f"{self.name}.xml")


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


## AI-generated classes below

"""
ArcGIS Metadata Dataclasses

This module defines dataclasses to represent ArcGIS metadata structure
based on XML xpath hierarchy and EPA compliance requirements.
"""


@dataclass
class ScaleRange:
    """Scale range information for the dataset."""

    min_scale: int
    max_scale: int


@dataclass
class GeographicBoundingBox:
    """Geographic extent as bounding box coordinates (Spatial Extent)"""

    west: Optional[float] = None  # westBL
    east: Optional[float] = None  # eastBL
    north: Optional[float] = None  # northBL
    south: Optional[float] = None  # southBL


@dataclass
class TemporalExtent:
    """Temporal range of dataset applicability (Temporal Extent)"""

    begin: Optional[datetime] = None  # tmBegin
    end: Optional[datetime] = None  # tmEnd


@dataclass
class ResponsibleParty:
    """Contact / publishing party info (Publishing Organization, Publisher, Metadata Responsible Party)"""

    organization_name: Optional[str] = None  # Publishing Organization / rpOrgName
    individual_name: Optional[str] = None  # Publisher / rpIndName
    email: Optional[str] = None  # Publisher Email / eMailAdd
    role_code: Optional[str] = None  # What is this?
    display_name: Optional[str] = None  # Should default to rpIndName value


@dataclass
class SpatialReference:
    """Spatial reference container (Spatial Reference)"""

    identification_code: Optional[int] = None  # Spatial Reference
    id_code_space: Optional[str] = None  # Spatial Reference


@dataclass
class SpatialRepresentation:
    """Spatial data representation (Spatial Data Representation)"""

    name: Optional[str] = None  # Spatial Data Representation
    geometric_object_type_code: Optional[str] = None  # Spatial Data Representation
    geometric_object_count: Optional[int] = None  # Spatial Data Representation
    topology_level_code: Optional[str] = None  # Spatial Data Representation


@dataclass
class Distribution:
    """Distribution information (Distribution URL and related)"""

    format_name: Optional[str] = None
    transfer_size: Optional[float] = None
    linkage: Optional[str] = None  # Distribution URL


@dataclass
class SecurityConstraints:
    """Security constraint information."""

    classification_code: Optional[str] = None  # Access Level
    user_note: Optional[str] = None  # Rights


@dataclass
class LegalConstraints:
    """Legal constraint information."""

    access_constraints_code: Optional[str] = None  # Unknown usage
    use_limit: Optional[str] = None  # System of Records
    other_constraints: Optional[str] = None  # Data License


@dataclass
class ItemProperties:
    """Properties of the dataset item."""

    item_name: str
    # Optional/Unknown fields marked with comments
    ims_content_type: Optional[str] = None  # What is this?
    item_size: Optional[float] = None  # What is this?


@dataclass
class CoordinateReference:
    """Coordinate reference system information."""

    # These fields marked as unknown - may be redundant with RefSystem
    type: Optional[str] = None  # Is it enough to have the refSysID fields?
    geogcsn: Optional[str] = None  # Is it enough to have the refSysID fields?
    cs_units: Optional[str] = None  # Is it enough to have the refSysID fields?
    projcsn: Optional[str] = None  # Is it enough to have the refSysID fields?
    pe_xml: Optional[str] = None  # What is this?


@dataclass
class DataProperties:
    """Data properties from Esri section."""

    item_props: ItemProperties
    coord_ref: Optional[CoordinateReference] = None  # Unknown if needed


@dataclass
class EsriMetadata:
    """Esri-specific metadata elements."""

    creation_date: datetime
    creation_time: datetime
    arcgis_format: float
    sync_once: str
    scale_range: ScaleRange
    arcgis_profile: str
    data_properties: DataProperties
    sync_date: datetime
    sync_time: datetime
    mod_date: datetime
    mod_time: datetime


@dataclass
class Language:
    """Language information."""

    language_code: str
    country_code: str


@dataclass
class ArcGISMetadata:
    """Root metadata container representing the complete ArcGIS metadata structure."""

    # Core identity
    title: Optional[str] = None  # Title (res_title)
    abstract: Optional[str] = None  # Description (HTML content)
    metadata_file_id: Optional[str] = (
        None  # Identifier - could be socrata four-four or BoBA dataset name
    )

    # Keywords and tags
    search_keys: List[str] = field(
        default_factory=list
    )  # Tags (General) - DCP overrode EPA path
    theme_keys: List[str] = field(default_factory=list)  # Tags (ISO)
    place_keys: List[str] = field(default_factory=list)  # Tags (Place)

    # Dates
    last_update: Optional[datetime] = None  # Last Update (reviseDate)
    metadata_date_stamp: Optional[datetime] = None  # Metadata Date Stamp
    maintenance_frequency_code: Optional[str] = None  # Update Frequency

    # Contact information
    point_of_contact: Optional[ResponsibleParty] = None
    metadata_contact: Optional[ResponsibleParty] = None

    # Spatial and temporal extents
    spatial_extent: Optional[GeographicBoundingBox] = None
    temporal_extent: Optional[TemporalExtent] = None

    # Technical details
    spatial_reference: Optional[SpatialReference] = None
    spatial_representation: Optional[SpatialRepresentation] = None
    distribution: Optional[Distribution] = None

    # Constraints and access
    security_constraints: Optional[SecurityConstraints] = None
    legal_constraints: Optional[LegalConstraints] = None
    general_use_limitation: Optional[str] = (
        None  # General Use Limitation (HTML content)
    )

    # Language and metadata
    data_language: Optional[Language] = None
    metadata_language: Optional[Language] = None

    # Esri-specific section (kept nested due to complexity)
    esri: Optional[EsriMetadata] = None

    # Optional/unknown fields
    environment_description: Optional[str] = None  # Unknown usage
    spatial_representation_type_code: Optional[str] = None  # What is this?
    data_character_set_code: Optional[str] = None  # What is this?
    topic_category_code: Optional[str] = None  # Required per Esri ISO
    metadata_hierarchy_level_name: Optional[str] = None
    metadata_hierarchy_level_code: Optional[str] = None  # What is this?
    metadata_character_set_code: Optional[str] = None  # Unknown usage
    pres_form_code: Optional[str] = None  # What is this?


# ----------------------------------------------------------------------------
## Temporarily retaining initial attempt at metadata class instantiation, so I can reference the methods
# ----------------------------------------------------------------------------

# @dataclass
# class Metadata:
#     path: Path
#     _root: ET.Element = field(init=False)
#     title: Optional[str] = field(init=False)
#     description: Optional[str] = field(init=False)
#     tags_general: Optional[list] = field(init=False)
#     tags_theme: Optional[list] = field(init=False)
#     tags_place: Optional[list] = field(init=False)
#     revised_date: Optional[str] = field(init=False)
#     publishing_organization: Optional[str] = field(init=False)
#     publisher: Optional[str] = field(init=False)
#     publisher_email: Optional[str] = field(init=False)
#     identifier: Optional[str] = field(init=False)
#     # access_level: Optional[str] = field(init=False)
#     # rights: Optional[str] = field(init=False)
#     data_license: Optional[str] = field(init=False)
#     system_of_records: Optional[str] = field(init=False)
#     general_use_limitation: Optional[str] = field(init=False)
#     spatial_extent: Optional[tuple] = field(init=False)
#     temporal_extent: Optional[tuple] = field(init=False)
#     distribution_url: Optional[str] = field(init=False)
#     metadata_date_stamp: Optional[str] = field(init=False)
#     update_frequency: Optional[str] = field(init=False)
#     metadata_responsible_party: Optional[dict] = field(init=False)
#     dataset_language: Optional[str] = field(init=False)
#     country: Optional[str] = field(init=False)
#     spatial_reference: Optional[dict] = field(init=False)
#     spatial_data_representation: Optional[dict] = field(init=False)

#     def __post_init__(self):
#         self._root = self._get_xml_root()

#         self.title = self._get_xml_text(self._root.dataIdInfo.idCitation.resTitle)
#         self.description = self._get_xml_text(self._root.dataIdInfo.idAbs)
#         self.revised_date = self._get_xml_text(
#             self._root.dataIdInfo.idCitation.date.reviseDate
#         )
#         self.publishing_organization = self._get_xml_text(
#             self._root.dataIdInfo.idPoC.rpOrgName
#         )
#         self.publisher = self._get_xml_text(self._root.dataIdInfo.idPoC.rpIndName)
#         self.publisher_email = self._get_xml_text(
#             self._root.dataIdInfo.idPoC.rpCntInfo.cntAddress.eMailAdd
#         )
#         # self.rights = self._get_xml_text(
#         #     self._root.dataIdInfo.resConst.SecConsts.userNote
#         # )  # only req'd if access level is not public
#         self.general_use_limitation = self._get_xml_text(
#             self._root.dataIdInfo.resConst.Consts.useLimit
#         )
#         self.temporal_extent = self._get_xml_text(
#             self._root.dataIdInfo.dataExt.tempEle.TempExtent.exTemp.TM_Period
#         )  # may not be necessary for most DCP datasets
#         self.distribution_url = self._get_xml_text(
#             self._root.distInfo.distTranOps.onLineSrc.linkage
#         )
#         self.metadata_date_stamp = self._get_xml_text(self._root.mdDateSt)

#         self.tags_theme = self._get_xml_tags(self._root.dataIdInfo.themeKeys)
#         self.tags_place = self._get_xml_tags(self._root.dataIdInfo.placeKeys)

#         # self.access_level = self._get_xml_value(self._root.dataIdInfo.resConst.SecConsts.class.ClasscationCd)
#         self.update_frequency = self._get_xml_value(
#             self._root.dataIdInfo.resMaint.maintFreq.MaintFreqCd
#         )
#         self.dataset_language = self._get_xml_value(
#             self._root.dataIdInfo.dataLang.languageCode
#         )
#         self.country = self._get_xml_value(self._root.mdLang.countryCode)

#         self.spatial_extent = self._get_bbox(
#             self._root.dataIdInfo.dataExt.geoEle.GeoBndBox
#         )

#         self.system_of_records = None  # exact path TBD (multiples of resConst tag)
#         self.data_license = None  # exact path TBD (multiples of resConst tag)
#         self.spatial_reference = None  # exact path TBD
#         self.spatial_data_representation = None  # exact path TBD
#         self.metadata_responsible_party = None  # exact path TBD
#         self.tags_general = None  # exact path TBD, and is it necessary for DCP
#         self.identifier = None  # need to identify what this means at DCP

#     def _get_xml_root(self) -> objectify.ObjectifiedElement:
#         with open(self.path) as f:
#             xml = f.read()

#         return objectify.fromstring(xml)
#         # tree = ET.parse(self.path)
#         # return tree.getroot()

#     def _get_xml_text(self, element_path: objectify.StringElement) -> str | None:
#         # element = self._get_xml_root().find(element_path)
#         if element_path is not None:
#             return element_path.text
#         return None

#     def _get_xml_value(self, element_path: objectify.StringElement) -> str | None:
#         # element = self._get_xml_root().find(element_path)
#         if element_path is not None:
#             return element_path.get("value")
#         return None

#     # def _get_contact(): ...
#     def _get_xml_tags(self, element_path: objectify.StringElement) -> list | None: ...

#     def _get_bbox(self, element_path: objectify.StringElement) -> tuple | None:
#         # element = self._get_xml_root().find(element_path)

#         # if element is not None:
#         #     bbox = tuple((item.text) for item in element)
#         #     return bbox
#         return None

#     def override_element_path(
#         self, attribute_with_path_to_override: str, new_path: objectify.StringElement
#     ) -> None: ...

# ----------------------------------------------------------------------------
## Temporarily retaining logic to process a formatted input string
# ----------------------------------------------------------------------------

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
