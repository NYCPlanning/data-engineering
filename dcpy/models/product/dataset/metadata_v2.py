from __future__ import annotations

import jinja2
from pathlib import Path
from pydantic import BaseModel, field_validator, model_serializer
from pydantic.fields import PrivateAttr
from typing import Any, List, Literal, get_args
import typing
import yaml

from dcpy.utils.logging import logger


class YamlTopLevelSpacesDumper(yaml.SafeDumper):
    """YAML serializer that will insert lines between top-level entries,
    which is nice in longer files."""

    def write_line_break(self, data=None):
        super().write_line_break(data)

        if len(self.indents) == 1:
            super().write_line_break()


class SortedSerializedBase(BaseModel):
    """A Pydantic BaseModel that will allow for sensible (and overrideable) deserialization order.

    The serialization order is as follows:
    - model attributes defined in the head sort order
    - simple (and nullable simple) types: strings, ints, bools, literals
    - complext types
    - model attributes defined in the tail sort order

    Note: This is put in its own class because it might be useful for other
    classes unrelated to product metadata in the future.
    """

    _exclude_falsey_values: bool = True
    _head_sort_order: list[str] = PrivateAttr(default=["id"])
    _tail_sort_order: list[str] = PrivateAttr(default=["custom"])

    @model_serializer(mode="wrap")
    def _model_dump_ordered(self, handler):
        unordered = handler(self)

        ordered_items_head = []
        ordered_items_tail = []
        simple_type_items = []
        other_items = []

        for model_field in list(unordered.items()):
            if not model_field[1] and self._exclude_falsey_values:
                # If an object's values are all None, it will serialize as {}.
                # These aren't removed by model_dump(exclude_none=True), so we have to do it manually.
                continue
            field_type = self.model_fields[
                model_field[0]
            ].annotation  # Need to retrieve type from the class def, not the instance
            is_literal = type(field_type) is typing._LiteralGenericAlias  # type: ignore
            simple_types = {
                bool,
                bool | None,  # This is a little hacky, but does the job.
                str,
                str | None,
                int,
                int | None,
                float,
                float | None,
                type(None),
            }

            if model_field[0] in self._head_sort_order:
                ordered_items_head.append(model_field)
            elif model_field[0] in self._tail_sort_order:
                ordered_items_tail.append(model_field)
            elif field_type in simple_types or is_literal:
                simple_type_items.append(model_field)
            else:
                other_items.append(model_field)

        # As of python 3.7, dict keys are ordered, and so will serialize in the order below
        return dict(
            sorted(ordered_items_head, key=lambda x: self._head_sort_order.index(x[0]))
            + sorted(simple_type_items, key=lambda x: x[0])
            + sorted(other_items, key=lambda x: x[0])
            + sorted(
                ordered_items_tail, key=lambda x: self._tail_sort_order.index(x[0])
            )
        )


class CustomizableBase(SortedSerializedBase, extra="forbid"):
    custom: dict[str, Any] = {}


# TODO: move to share with ingest.validate
class Checks(CustomizableBase):
    is_primary_key: bool | None = None
    non_nullable: bool | None = None


# TODO: move to share with ingest.validate
COLUMN_TYPES = Literal[
    "text", "integer", "decimal", "geometry", "bool", "bbl", "datetime"
]


class ColumnValue(CustomizableBase):
    _head_sort_order = ["value", "description"]

    value: str
    description: str | None = None


class OverrideableColumnAttrs(CustomizableBase):
    _head_sort_order = ["id", "display_name", "data_type", "description"]
    _tail_sort_order = ["example", "values", "custom"]
    _validate_data_type = True  # used to generate md where we don't know the data_type

    # Note: id isn't intended to be overrideable, but is always required as a
    # pointer back to the original column, so it is required here.
    id: str
    display_name: str | None = None
    data_type: str | None = None
    data_source: str | None = None
    description: str | None = None
    example: str | None = None
    checks: Checks | None = None
    deprecated: bool | None = None
    values: list[ColumnValue] | None = None

    @field_validator("data_type")
    def _validate_colum_types(cls, v):
        if cls._validate_data_type:
            assert v in get_args(COLUMN_TYPES)
        return v


class DatasetColumn(OverrideableColumnAttrs):
    """Like OverrideableColumnAttrs, but with constraints for non-null fields"""

    @field_validator("display_name")
    def _validate_display_name(cls, dn):
        assert dn, "display_name may not be null"
        return dn

    @field_validator("data_type")
    def _validate_data_type(cls, dt):
        assert dt, "data_type may not be null"
        return dt


class PackageFile(CustomizableBase):
    id: str
    filename: str


class Package(CustomizableBase):
    """Container for lists of files. Used as assembly instructions."""

    id: str
    type: str
    filename: str
    contents: List[PackageFile]


class DatasetAttributes(CustomizableBase):
    display_name: str
    description: str
    each_row_is_a: str
    tags: List[str]


# All overrideable at the Destination/File level
# It would be nice if this shared ancestry with DatasetAttributes,
# but that's tricky.
class NullableDatasetAttributes(CustomizableBase):
    display_name: str | None = None
    description: str | None = None
    each_row_is_a: str | None = None
    tags: List[str] | None = None


class Destination(CustomizableBase):
    id: str
    type: str
    files: List[DestinationFile]

    dataset_overrides: NullableDatasetAttributes = NullableDatasetAttributes()


class FileOverrides(CustomizableBase):
    _head_sort_order = [
        "description",
        "display_name",
        "omitted_columns",
        "overridden_columns",
    ]

    filename: str | None = None
    overridden_columns: list[OverrideableColumnAttrs] = []
    omitted_columns: list[str] = []
    attributes: NullableDatasetAttributes = NullableDatasetAttributes()


class File(CustomizableBase):
    """Describes an actual dataset file, e.g. dataset files or attachments."""

    id: str
    filename: str
    type: str | None = None
    overrides: FileOverrides = FileOverrides()


class DestinationFile(CustomizableBase):
    """Pointer to an actual `File`, with specifiable overrides."""

    id: str
    overrides: FileOverrides = FileOverrides()


class Metadata(CustomizableBase):
    id: str
    attributes: DatasetAttributes
    assembly: List[Package] = []
    columns: List[DatasetColumn]
    files: List[File]
    destinations: List[Destination]

    _head_sort_order = [
        "id",
        "attributes",
    ]
    _tail_sort_order = ["columns"]
    _exclude_falsey_values = False  # We never want to prune top-level attrs

    def get_destination(self, id: str) -> Destination:
        dests = [d for d in self.destinations if d.id == id]
        if len(dests) != 1:
            raise Exception(f"There should exist one destination with id: {id}")
        return dests[0]

    @staticmethod
    def from_yaml(yaml_str: str, *, template_vars=None):
        if template_vars:
            logger.info(f"Templating metadata with vars: {template_vars}")
            templated = jinja2.Template(
                yaml_str, undefined=jinja2.StrictUndefined
            ).render(template_vars or {})
            return Metadata(**yaml.safe_load(templated))
        else:
            logger.info("No Template vars supplied. Skipping templating.")
        return Metadata(**yaml.safe_load(yaml_str))

    @classmethod
    def from_path(cls, path: Path, *, template_vars=None):
        with open(path, "r") as raw:
            return cls.from_yaml(raw.read(), template_vars=template_vars)

    def write_to_yaml(self, path: Path):
        with open(path, "w") as f:
            f.write(
                yaml.dump(
                    self.model_dump(exclude_none=True),
                    sort_keys=False,
                    default_flow_style=False,
                    Dumper=YamlTopLevelSpacesDumper,
                )
            )
