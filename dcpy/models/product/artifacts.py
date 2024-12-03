from pathlib import Path

from .dataset.metadata_v2 import CustomizableBase
from dcpy.models.base import TemplatedYamlReader


class ComponentDefinition(CustomizableBase):
    id: str
    name: str
    type: str
    index: int
    title: str
    subtitle: str
    description: str | None = None
    extra_field_description_field: str | None = None
    image_path: Path | None = None
    rows: list[str] | None = None
    columns: list[str] | None = None
    column_widths: list[float] | None = None
    include_column_description_row: bool = True
    data_source: str | None = None


class Artifact(CustomizableBase, TemplatedYamlReader):
    name: str
    type: str
    components: list[ComponentDefinition]


class Artifacts(CustomizableBase, TemplatedYamlReader):
    artifacts: list[Artifact]
