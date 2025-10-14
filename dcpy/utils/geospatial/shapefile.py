import os
from pathlib import Path
import zipfile
from dataclasses import dataclass, field
from lxml import etree

# TODO: - move unpack_multilayer_shapefile() from lifecycle/assemble.py


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
        metadata_str = self.file_manager.read_file(f"{self.name}.xml")
        parser = MetadataParser()
        metadata = parser.parse_from_string(metadata_str)

        return metadata

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


@dataclass
class ScaleRange:
    """Scale range information for the dataset."""

    min_scale: int
    max_scale: int


@dataclass
class GeographicBoundingBox:
    """Geographic extent as bounding box coordinates (Spatial Extent)"""

    west: float | None = None  # westBL
    east: float | None = None  # eastBL
    north: float | None = None  # northBL
    south: float | None = None  # southBL


@dataclass
class TemporalExtent:
    """Temporal range of dataset applicability (Temporal Extent)"""

    begin_date: str | None = None  # tmBegin
    end_date: str | None = None  # tmEnd


@dataclass
class ResponsibleParty:
    """Contact / publishing party info (Publishing Organization, Publisher, Metadata Responsible Party)"""

    organization_name: str | None = None  # Publishing Organization / rpOrgName
    individual_name: str | None = None  # Publisher / rpIndName
    email: str | None = None  # Publisher Email / eMailAdd
    role_code: str | None = None  # What is this?


@dataclass
class SpatialReference:
    """Spatial reference container (Spatial Reference)"""

    code: int | None = None = None  # e.g. "2263"
    authority: str | None = None  # e.g. "EPSG"


@dataclass
class SpatialRepresentation:
    """Spatial data representation (Spatial Data Representation)"""

    # TODO: must be amended to handle other data types besides vector data
    spatial_representation_type: str | None = None  # SpatRepTypCd
    geometric_object_name: str | None = (
        None  # appears to be same as title, but doesn't update when title or shp are renamed
    )
    geometric_object_type_code: str | None = None  # Spatial Data Representation
    geometric_object_count: int | None = None = None  # Spatial Data Representation
    topology_level_code: str | None = None  # Spatial Data Representation


@dataclass
class Constraints:
    """Access constraint information."""

    access_level: str | None = None  # ClasscationCd
    data_license: str | None = None  # othConst
    general_use_limitation: str | None = (
        None  # useLimit -- marked as optional, per EPA
    )
    # rights: str | None = None  # userNote -- req'd by EPA if item is not public
    # system_of_record: str | None = None  # useLimit -- may not be required?


@dataclass
class EsriMetadata:
    """Esri-specific metadata elements."""

    # NOTE: these were initially listed as non-optional, but I think all attrs should be optional
    #   and required values should be controlled via pydantic or other
    creation_date: str | None = None
    creation_time: str | None = None
    arcgis_format: float | None = None
    sync_once: str | None = None
    scale_range: ScaleRange | None = None
    arcgis_profile: str | None = None
    # data_properties: DataProperties   # see commented out classes
    # These next attrs will need to be calculated based on run time of
    # specific methods, sync etc.
    sync_date: str | None = None
    sync_time: str | None = None
    mod_date: str | None = None
    mod_time: str | None = None


@dataclass
class Language:
    """Language information."""

    # TODO: later, allow for a different metadata and data language
    language_code: str
    country_code: str


@dataclass
class ArcGISMetadata:
    """Root metadata container representing the complete ArcGIS metadata structure."""

    ## TODO: Requires categorization
    metadata_file_id: str | None = None  # UUID - four-four or BoBA dataset name?

    # Core identity
    title: str | None = None  # Title (res_title)
    description: str | None = None  # Description (HTML content)

    # Keywords and tags
    topic_category: str | None = None  # Required per Esri ISO
    general_tags: list[str] = field(
        default_factory=list
    )  # Tags (General) - DCP overrode EPA path
    place_tags: list[str] = field(default_factory=list)  # Tags (Place)

    # Dates
    last_update: str | None = None  # Last Update (reviseDate)
    update_frequency: str | None = None  # Update Frequency
    metadata_date_stamp: str | None = None

    # Contact information
    data_contact: ResponsibleParty | None = None
    # TODO: allow metadata contact to be different from dataset contact:
    #   - perhaps - define a MetadataResponsibleParty w/ same attrs, attrs default to data contact if not present in XML, or provided
    metadata_contact: ResponsibleParty | None = None

    # Spatial and temporal extents
    spatial_extent: GeographicBoundingBox | None = None
    temporal_extent: TemporalExtent | None = None

    # Technical details
    spatial_reference: SpatialReference | None = None
    metadata_hierarchy_level_code: str | None = (
        None  # numeric code indicating type of item: software, dataset, etc.
    )
    # distribution: Distribution | None = None   # may not be req'd

    # Constraints and access
    distribution_url: str | None = None  # Distribution URL
    constraints: Constraints | None = None

    # Language and metadata
    data_language: Language | None = None
    metadata_language: Language | None = None

    # Esri-specific section (kept nested due to complexity)
    esri: EsriMetadata | None = None

    # ------------------------------------------
    ## Optional/unknown/later development fields
    # ------------------------------------------
    # spatial_representation: SpatialRepresentation | None = (
    #     None  # TODO: must incl. other types
    # )
    # environment_description: str | None = None  # e.g. "Microsoft Windows 10 Version 10.0"...
    # spatial_representation_type: str | None = None  # e.g. vector, grid, tin, etc. -> may not be req'd
    # data_character_set_code: str | None = None  # numeric code, indicating character encoding of dataset
    # metadata_hierarchy_level_name: str | None = None # I think: mdHrLvName --> req'd if mdHrLv is not dataset
    # metadata_character_set_code: str | None = None  # numeric code, indicating character encoding of metadata
    # pres_form_code: str | None = None  # see presForm

    # System/process attributes
    missing_xpaths: list[str] = field(default_factory=list)


# TODO: add handling for when the xpath doesn't exist
class MetadataParser:
    def __init__(self):
        self.missing_xpaths = []  # Track missing XPaths during parsing

    def _get_xpath_results(
        self, tree: etree._ElementTree, xpath: str
    ) -> list[str] | None:
        result = tree.xpath(xpath)
        if len(result) == 0:
            self.missing_xpaths.append(xpath)  # Flag missing xpaths by adding to list
            return None

        for index, item in enumerate(result):
            # print(index, item)
            if hasattr(item, "text"):
                # print(f"replacing {item} with {item.text}")
                result[index] = item.text

        return result

    def parse_from_string(self, string) -> ArcGISMetadata:
        self.missing_xpaths = []  # Reset missing xpath collector for each parse
        tree = etree.ElementTree(etree.fromstring(string))

        # TODO: ResponsibleParty has to be written to default to a single set of contacts, but to
        #   also allow md and data contacts to be different, and to point to different xpaths
        responsible_party = ResponsibleParty()

        spatial_extent = GeographicBoundingBox(
            west=self._get_xpath_results(
                tree, ".//dataIdInfo/dataExt/geoEle/GeoBndBox/westBL"
            ),
            east=self._get_xpath_results(
                tree, ".//dataIdInfo/dataExt/geoEle/GeoBndBox/eastBL"
            ),
            north=self._get_xpath_results(
                tree, ".//dataIdInfo/dataExt/geoEle/GeoBndBox/northBL"
            ),
            south=self._get_xpath_results(
                tree, ".//dataIdInfo/dataExt/geoEle/GeoBndBox/southBL"
            ),
        )
        temporal_extent = TemporalExtent(
            begin_date=self._get_xpath_results(
                tree,
                ".//dataIdInfo/dataExt/tempEle/TempExtent/exTemp/TM_Period/tmBegin",
            ),
            end_date=self._get_xpath_results(
                tree,
                ".//dataIdInfo/dataExt/tempEle/TempExtent/exTemp/TM_Period/tmEnd",
            ),
        )
        spatial_reference = SpatialReference(
            code=self._get_xpath_results(
                tree, ".//refSysInfo/RefSystem/refSysID/identCode/@code"
            ),
            authority=self._get_xpath_results(
                tree, ".//refSysInfo/RefSystem/refSysID/idCodeSpace"
            ),
        )
        constraints = Constraints(
            access_level=self._get_xpath_results(
                tree, ".//dataIdInfo/resConst/SecConsts/class/ClasscationCd/@value"
            ),
            data_license=self._get_xpath_results(
                tree, ".//dataIdInfo/resConst/LegConsts/othConsts"
            ),
            general_use_limitation=self._get_xpath_results(
                tree, ".//dataIdInfo/resConst/Consts/useLimit"
            ),
        )
        language = Language(
            language_code=self._get_xpath_results(
                tree, ".//dataIdInfo/dataLang/languageCode/@value"
            ),
            country_code=self._get_xpath_results(tree, ".//mdLang/countryCode/@value"),
        )

        scale_range = ScaleRange(
            min_scale=self._get_xpath_results(tree, ".//Esri/scaleRange/minScale"),
            max_scale=self._get_xpath_results(tree, ".//Esri/scaleRange/maxScale"),
        )

        esri = EsriMetadata(
            creation_date=self._get_xpath_results(tree, ".//Esri/CreaDate"),
            creation_time=self._get_xpath_results(tree, ".//Esri/CreaTime"),
            arcgis_format=self._get_xpath_results(tree, ".//Esri/ArcGISFormat"),
            sync_once=self._get_xpath_results(tree, ".//Esri/SyncOnce"),
            scale_range=scale_range,
            arcgis_profile=self._get_xpath_results(tree, ".//Esri/ArcGISProfile"),
            sync_date=self._get_xpath_results(tree, ".//Esri/SyncDate"),
            sync_time=self._get_xpath_results(tree, ".//Esri/SyncTime"),
            mod_date=self._get_xpath_results(tree, ".//Esri/ModDate"),
            mod_time=self._get_xpath_results(tree, ".//Esri/ModTime"),
        )

        metadata = ArcGISMetadata(
            missing_xpaths=self.missing_xpaths.copy(),  # return missing xpaths
            metadata_date_stamp=self._get_xpath_results(tree=tree, xpath=".//mdDateSt"),
            esri=esri,
            metadata_file_id=self._get_xpath_results(tree, xpath=".//mdFileID"),
            title=self._get_xpath_results(
                tree, xpath=".//dataIdInfo/idCitation/resTitle"
            ),
            description=self._get_xpath_results(tree, xpath=".//dataIdInfo/idAbs"),
            topic_category=self._get_xpath_results(
                tree, xpath=".//dataIdInfo/tpCat/TopicCatCd/@value"
            ),
            general_tags=self._get_xpath_results(
                tree, xpath=".//dataIdInfo/themeKeys/keyword"
            ),
            place_tags=self._get_xpath_results(
                tree, xpath=".//dataIdInfo/placeKeys/keyword"
            ),
            last_update=self._get_xpath_results(
                tree, xpath=".//dataIdInfo/idCitation/date/reviseDate"
            ),
            update_frequency=self._get_xpath_results(
                tree, xpath=".//dataIdInfo/resMaint/maintFreq/MaintFreqCd/@value"
            ),
            data_contact=responsible_party,
            metadata_contact=responsible_party,
            spatial_extent=spatial_extent,
            temporal_extent=temporal_extent,
            spatial_reference=spatial_reference,
            metadata_hierarchy_level_code=self._get_xpath_results(
                tree, xpath=".//mdHrLv/ScopeCd/@value"
            ),
            distribution_url=self._get_xpath_results(
                tree, xpath=".//distInfo/distTranOps/onLineSrc/linkage"
            ),  # TODO: properly handle multiples of this xpath - may need to have specific
            # fn to handle instances where, say, a linkage (url) is listed for Bytes and
            # also Socrata, and where each needs to retain the other associated info.
            # Perhaps a dict that grabs everything under 'onLineSrc'?
            # Could be handled via instances of Distribution dataclass
            constraints=constraints,
            data_language=language.language_code,
            metadata_language=language.language_code,
        )

        return metadata


class MetadataWriter:
    def _sync_metadata(self):
        """Calculate synced value fields from dataset, e.g. row count"""
        ...

    def _write_metadata(self):
        """Write entire metadata xml file from a string"""
        # do some stuff
        self._sync_metadata()
        ...

    def _patch_metadata(self):
        """Write a single field value"""
        # do some stuff
        self._sync_metadata()
        ...

    def _generate_metadata(self):
        """Generate a minimally valid metadata xml file"""
        # do some stuff
        self._sync_metadata()
        ...


# --------------
## Unclear if the following attrs are required, will be reviewed
# --------------
# @dataclass
# class Distribution:
#     """Distribution information (Distribution URL and related)"""
#     # These don't seem to actually be req'd in our source refs
#     format_name: str | None = None   # e.g. "Shapefile"
#     transfer_size: float | None = None

# @dataclass
# class ItemProperties:
#     """Properties of the dataset item."""
#     item_name: str    # This updates to reflect *file name* when sync is run
#     ims_content_type: str | None = None  # What is this?
#     item_size: float | None = None  # I think this is the size of the file on disk?

# @dataclass
# class DataProperties:
#     """Data properties from Esri section."""
#     item_properties: ItemProperties
#     coord_ref: CoordinateReference | None = None  # Unknown if needed

# @dataclass
# class CoordinateReference:
#     """Coordinate reference system information."""

#     # These fields marked as unknown - may be redundant with RefSystem
#     type: str | None = None  # Is it enough to have the refSysID fields?
#     geogcsn: str | None = None  # Is it enough to have the refSysID fields?
#     cs_units: str | None = None  # Is it enough to have the refSysID fields?
#     projcsn: str | None = None  # Is it enough to have the refSysID fields?
#     pe_xml: str | None = None  # What is this?
