from .dataset.metadata_v2 import CustomizableBase
from dcpy.models.base import TemplatedYamlReader


class FieldDefinition(CustomizableBase):
    summary: str
    extra_description: str | None = None


class FieldSet(CustomizableBase):
    fields: dict[str, FieldDefinition] = {}


class DataDictionary(CustomizableBase, TemplatedYamlReader):
    org: dict[str, dict[str, FieldDefinition]] = {}
    product: dict[str, dict[str, FieldDefinition]] = {}
    dataset: dict[str, dict[str, FieldDefinition]] = {}
