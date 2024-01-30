from pydantic import BaseModel, ValidationError
from dcpy.connectors.edm.recipes import DatasetDefinition, ValidAclValues


class Dataset(BaseModel):
    name: str
    version: str | None = None
    acl: ValidAclValues
    source: DatasetDefinition.SourceSection
    destination: DatasetDefinition.DestinationSection
    info: DatasetDefinition.InfoSection | None = None


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
