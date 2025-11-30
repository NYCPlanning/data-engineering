from pathlib import Path

from dcpy.models.base import TemplatedYamlReader

from .dataset.metadata import CustomizableBase


class ExcelTableComponentDefinition(CustomizableBase):
    """Declaration for a table in an XLSX.

    A table should declare a data_source from which to pull data (and field metadata),
    and specify rows OR columns. (rows AND columns makes sense in theory, but isn't implemented)
    """

    id: str
    name: str
    type: str  # atm, either a `list_table` or `object_table`... but we should determine this from the object itself, so this attribute should go away.
    index: int
    data_source: str | None = None

    title: str
    subtitle: str
    description: str | None = None  # table description
    include_column_description_row: bool = (
        True  # header row underneath columns, with a description of the columns
    )

    extra_field_description_field: str | None = (
        None  # field from which to pull extra description paragraphs
    )
    image_path: Path | None = None
    rows: list[str] | None = None
    columns: list[str] | None = None
    column_widths: list[float] | None = (
        None  # TODO: generalize away from concrete numbers.
    )


class Artifact(CustomizableBase, TemplatedYamlReader):
    name: str
    type: str
    components: list[ExcelTableComponentDefinition]


class Artifacts(CustomizableBase, TemplatedYamlReader):
    artifacts: list[Artifact]
