from dcpy.models.base import SortedSerializedBase, YamlWriter, TemplatedYamlReader


class DatasetAttributes(SortedSerializedBase):
    """Anything overrideable at the dataset level"""

    contains_address: bool | None = None
    date_made_public: str | None = None
    publishing_purpose: str | None = None
    potential_uses: str | None = None
    publishing_frequency: str | None = None  # TODO: picklist values
    publishing_frequency_details: str | None = None
    tags: list[str] | None = None


class Attributes(DatasetAttributes, extra="ignore"):
    display_name: str | None = None
    description: str | None = None

    def to_dataset_attributes(self):
        return DatasetAttributes(**self.model_dump())


class Metadata(SortedSerializedBase, YamlWriter, TemplatedYamlReader):
    id: str
    datasets: list[str]

    attributes: Attributes
