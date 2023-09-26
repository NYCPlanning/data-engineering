from typing import List, Literal, Optional
from pydantic import BaseModel, ValidationError, Extra

ValidAclValues = Literal["public-read", "private"]
ValidGeomTypes = Literal[
    "NONE",
    "GEOMETRY",
    "POINT",
    "LINESTRING",
    "POLYGON",
    "GEOMETRYCOLLECTION",
    "MULTIPOINT",
    "MULTIPOLYGON",
    "MULTILINESTRING",
    "CIRCULARSTRING",
    "COMPOUNDCURVE",
    "CURVEPOLYGON",
    "MULTICURVE",
    "MULTISURFACE",
]
ValidSocrataFormats = Literal["csv", "geojson", "shapefile"]


# Create schema
class GeometryType(BaseModel):
    SRS: Optional[str] = None
    type: ValidGeomTypes


class Url(BaseModel):
    path: str  # Specify field name and data type
    subpath: Optional[str] = None  # Set default value


class Socrata(BaseModel):
    uid: str
    format: ValidSocrataFormats


class SourceSection(BaseModel):
    url: Optional[Url] = None  # Pass another schema as a data type
    socrata: Optional[Socrata] = None
    script: Optional[str] = None
    geometry: GeometryType
    options: List[str] = []  # Use List[dtype] for a list field value


class DestinationSection(BaseModel):
    name: str
    geometry: GeometryType
    options: List[str] = []
    fields: List[str] = []
    sql: Optional[str] = None


class InfoSection(BaseModel):
    info: Optional[str] = None
    url: Optional[str] = None
    dependents: Optional[List[str]] = None

    class Config:
        extra = Extra.allow


class Dataset(BaseModel):
    name: str
    version: str
    acl: ValidAclValues
    source: SourceSection
    destination: DestinationSection
    info: Optional[InfoSection] = None


class Validator:
    """
    Validator takes as input the path of a configuration file
    and will run the necessary checks to determine wether the structure
    and values of the files are valid according to the requirements of
    the library.
    """

    def __init__(self, f):
        self.__parsed_file = f

    def __call__(self):
        assert self.tree_is_valid, "Some fields are not valid. Please review your file"
        assert (
            self.dataset_name_matches
        ), "Dataset name must match file and destination name"
        assert (
            self.has_only_one_source
        ), "Source can only have one property from either url, socrata or script"

    # Check that the tree structure fits the specified schema
    @property
    def tree_is_valid(self) -> bool:
        if self.__parsed_file["dataset"] == None:
            return False
        try:
            input_ds = Dataset(**self.__parsed_file["dataset"])
        except ValidationError as e:
            print(e.json())
            return False
        return True

    # Check that the dataset name matches with destination name
    @property
    def dataset_name_matches(self):
        dataset = self.__parsed_file["dataset"]
        return dataset["name"] == dataset["destination"]["name"]

    # Check that source has only one source from either url, socrata or script
    @property
    def has_only_one_source(self):
        dataset = self.__parsed_file["dataset"]
        source_fields = list(dataset["source"].keys())
        # In other words: if url is in source, socrata or script cannot be.
        # If url is NOT in source. Only one from socrata or url can be. (XOR operator ^)

        if "url" in source_fields:
            return ("socrata" not in source_fields) and ("script" not in source_fields)
        else:
            return ("socrata" in source_fields) ^ ("script" in source_fields)
