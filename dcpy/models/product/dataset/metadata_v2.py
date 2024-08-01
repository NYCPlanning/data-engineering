from __future__ import annotations

import jinja2
from pathlib import Path
from pydantic import BaseModel
from typing import Any, List, Literal
import yaml

from dcpy.utils.logging import logger


class CustomizableBase(BaseModel, extra="forbid"):
    custom: dict[str, Any] | None = None


# TODO: move to share with ingest.validate
class Checks(CustomizableBase):
    is_primary_key: bool | None
    nullable: bool | None


# TODO: move to share with ingest.validate
COLUMN_TYPES = Literal["text", "integer", "decimal", "geometry"]
COLUMN_TYPES = Literal[
    "text", "integer", "decimal", "geometry", "bool", "bbl", "datetime"
]


class Column(CustomizableBase):
    id: str
    display_name: str
    data_type: COLUMN_TYPES

    data_source: str | None = None
    description: str | None = None
    example: str | None = None
    checks: Checks | None = None


class PackageFile(CustomizableBase):
    id: str
    filename: str


class Package(CustomizableBase):
    id: str
    type: str
    filename: str
    contents: List[PackageFile]


class FileOverrides(CustomizableBase):
    columns_overridden: list[OverrideableColumnAttrs] = []
    columns_omitted: list[str] = []
    display_name: str | None = None
    description: str | None = None


class File(CustomizableBase):
    id: str
    filename: str
    type: str | None = None
    overrides: FileOverrides | None = None


class DestinationFile(CustomizableBase):
    id: str
    overrides: FileOverrides | None = None


class Destination(CustomizableBase):
    id: str
    type: str
    files: List[DestinationFile]


# All overrideable at the Destination/File level
class DatasetAttributes(CustomizableBase):
    display_name: str
    description: str
    each_row_is_a: str
    tags: List[str]


class Metadata(CustomizableBase):
    id: str
    attributes: DatasetAttributes
    assembly: List[Package]
    columns: List[DatasetColumn]
    files: List[File]
    destinations: List[Destination]

    @staticmethod
    def from_yaml(yaml_str: str, *, template_vars=None):
        logger.info(f"Templating metadata with vars: {template_vars}")
        templated = jinja2.Template(yaml_str, undefined=jinja2.StrictUndefined).render(
            template_vars or {}
        )
        return Metadata(**yaml.safe_load(templated))

    @classmethod
    def from_path(cls, path: Path, *, template_vars=None):
        with open(path, "r") as raw:
            return cls.from_yaml(raw.read(), template_vars=template_vars)
