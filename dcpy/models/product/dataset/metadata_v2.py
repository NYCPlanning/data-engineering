from __future__ import annotations

import jinja2

from pathlib import Path
from pydantic import field_validator
from typing import Any, List, Literal, get_args
import unicodedata
import yaml

from dcpy.utils.logging import logger
from dcpy.utils.collections import deep_merge_dict as merge
from dcpy.models.base import SortedSerializedBase, YamlWriter


# MISC UTILS
def normalize_text(s):
    """
    Normalize the text we may receive from the various metadata sources.
    Primarily useful for cleaning long-text like descriptions.
    """
    char_map = {
        "–": "-",  # en dash to hyphen
        "—": "-",  # em dash to hyphen
        "’": "'",  # curly apostrophe
        "“": '"',  # lcurly quote
        "”": '"',  # rcurly quote
    }
    translator = str.maketrans(char_map)
    # Normalize Unicode characters
    normalized = unicodedata.normalize("NFKD", s)
    # Apply the translation
    cleaned = normalized.translate(translator)
    return cleaned.strip()


class CustomizableBase(SortedSerializedBase, extra="forbid"):
    """A Base Pydantic class to allow extensibility of our models via a `custom`
    dictionary.

    Any additional attributes that aren't defined on our models should
    be added to `custom`. This is intended for domain-specific, non-generalized uses
    like data-dictionary generation (e.g. the `readme_data_type` field on the columns)

    It's also important that custom be preserved in the
    course of overriding, specifically with a deep merge of the dictionary elements.
    """

    custom: dict[str, Any] = {}


# COLUMNS
# TODO: move to share with ingest.validate
class Checks(CustomizableBase):
    is_primary_key: bool | None = None
    non_nullable: bool | None = None


# TODO: move to share with ingest.validate
COLUMN_TYPES = Literal[
    "text", "number", "integer", "decimal", "geometry", "bool", "bbl", "datetime"
]


class ColumnValue(CustomizableBase):
    _head_sort_order = ["value", "description"]

    value: str
    description: str | None = None


class DatasetColumnOverrides(CustomizableBase):
    _head_sort_order = ["id", "name", "data_type", "description"]
    _tail_sort_order = ["example", "values", "custom"]
    _validate_data_type = (
        True  # override, to generate md where we don't know the data_type
    )

    # Note: id isn't intended to be overrideable, but is always required as a
    # pointer back to the original column, so it is required here.
    id: str
    name: str | None = None
    data_type: str | None = None
    data_source: str | None = None
    description: str | None = None
    notes: str | None = None
    example: str | None = None
    checks: Checks | None = None
    deprecated: bool | None = None
    values: list[ColumnValue] | None = None

    @field_validator("data_type")
    def _validate_colum_types(cls, v):
        if cls._validate_data_type:
            assert v in get_args(COLUMN_TYPES)
        return v


class DatasetColumn(DatasetColumnOverrides):
    """Like DatasetColumnOverrides, but with constraints for non-null fields"""

    @field_validator("name")
    def _validate_name(cls, dn):
        assert dn, "name may not be null"
        return dn

    @field_validator("data_type")
    def _validate_data_type_field(cls, dt):
        assert dt, "data_type may not be null"
        return dt

    def override(self, overrides: DatasetColumnOverrides) -> DatasetColumn:
        return DatasetColumn(**merge(self.model_dump(), overrides.model_dump()))


# FILE
class FileOverrides(CustomizableBase):
    filename: str | None = None
    type: str | None = None


class File(CustomizableBase):
    """Describes an actual dataset file, e.g. dataset files or attachments."""

    id: str
    filename: str
    type: str | None = None
    is_metadata: bool | None = (
        None  # e.g. readmes, data_dictionaries, version_files, etc.
    )

    def override(self, overrides: FileOverrides) -> File:
        return File(
            **(self.model_dump() | overrides.model_dump()),
        )


# PACKAGE / ASSEMBLY
class PackageFile(CustomizableBase):
    """File found in a Package, e.g. a Zip. `filename` here refers to it's name
    in the package
    """

    id: str
    filename: str | None = None


class Package(CustomizableBase):
    """Container for lists of files. Used as assembly instructions."""

    id: str
    type: str = "zip"
    filename: str
    contents: List[PackageFile]


# DATASET
class DatasetAttributesOverride(CustomizableBase):
    display_name: str | None = None
    description: str | None = None
    each_row_is_a: str | None = None
    tags: List[str] | None = None


class DatasetAttributes(CustomizableBase):
    display_name: str
    description: str = ""
    each_row_is_a: str

    publishing_purpose: str | None = None
    potential_uses: str | None = None
    publishing_frequency: str | None = None  # TODO: picklist values
    publishing_frequency_details: str | None = None
    projection: str | None = None  # TODO: does projection belong here?

    tags: List[str] = []

    def override(
        self,
        overrides: DatasetAttributesOverride,
    ) -> DatasetAttributes:
        return DatasetAttributes(**merge(self.model_dump(), overrides.model_dump()))


class DatasetOverrides(CustomizableBase):
    overridden_columns: list[DatasetColumnOverrides] = []
    omitted_columns: list[str] = []
    attributes: DatasetAttributesOverride = DatasetAttributesOverride()


class Dataset(CustomizableBase):
    columns: list[DatasetColumn]
    attributes: DatasetAttributes

    def override(self, overrides: DatasetOverrides) -> Dataset:
        """Apply column updates and prune any columns specified as omitted"""
        overriden_cols_by_id = {c.id: c for c in overrides.overridden_columns}

        columns = [
            c.override(overriden_cols_by_id.get(c.id, DatasetColumnOverrides(id=c.id)))
            for c in self.columns
            if c.id not in overrides.omitted_columns
        ]

        return Dataset(
            columns=columns, attributes=self.attributes.override(overrides.attributes)
        )


# DESTINATION
class Destination(CustomizableBase):
    id: str
    type: str


class DestinationWithFiles(Destination):
    files: List[DestinationFile] = []


class FileAndOverrides(SortedSerializedBase):
    _head_sort_order = ["file"]

    dataset_overrides: DatasetOverrides = DatasetOverrides()
    file: File


class DestinationFile(CustomizableBase):
    """Pointer to an actual `File`, with specifiable overrides."""

    id: str
    dataset_overrides: DatasetOverrides = DatasetOverrides()
    file_overrides: FileOverrides = FileOverrides()


class DestinationMetadata(SortedSerializedBase):
    dataset: Dataset
    destination: Destination
    file: File


class Metadata(CustomizableBase, YamlWriter):
    id: str
    attributes: DatasetAttributes
    assembly: List[Package] = []
    columns: List[DatasetColumn]
    files: List[FileAndOverrides]
    destinations: List[DestinationWithFiles]

    _head_sort_order = [
        "id",
        "attributes",
    ]
    _tail_sort_order = ["columns"]
    _exclude_falsey_values = False  # We never want to prune top-level attrs

    @property
    def dataset(self):
        return Dataset(attributes=self.attributes, columns=self.columns)

    def get_package(self, id: str) -> Package:
        packages = [p for p in self.assembly if p.id == id]
        if len(packages) != 1:
            raise Exception(f"There should exist one package with id: {id}")
        return packages[0]

    def get_destination(self, id: str) -> DestinationWithFiles:
        dests = [d for d in self.destinations if d.id == id]
        if len(dests) != 1:
            raise Exception(f"There should exist one destination with id: {id}")
        return dests[0]

    def get_file_and_overrides(self, file_id: str) -> FileAndOverrides:
        files = [f for f in self.files if f.file.id == file_id]
        if len(files) != 1:
            raise Exception(f"There should exist one file with id: {file_id}")
        return files[0]

    def calculate_file_dataset_metadata(self, *, file_id: str) -> Dataset:
        return self.dataset.override(
            self.get_file_and_overrides(file_id).dataset_overrides
        )

    def calculate_destination_metadata(
        self, *, file_id: str, destination_id: str
    ) -> DestinationMetadata:
        dataset_file = self.get_file_and_overrides(file_id)

        destination = self.get_destination(destination_id)
        dest_files = [f for f in destination.files if f.id == file_id]
        if len(dest_files) != 1:
            raise Exception(
                f"Can't calculate overrides, because destination: {destination_id} doesn't reference file: {file_id}"
            )
        dest_file = dest_files[0]

        dataset_metadata = self.calculate_file_dataset_metadata(
            file_id=file_id
        ).override(dest_file.dataset_overrides)

        destination_file = (
            dataset_file.file
            if not dest_file
            else dataset_file.file.override(dest_file.file_overrides)
        )

        return DestinationMetadata(
            file=destination_file,
            dataset=dataset_metadata,
            destination=destination,
        )

    def validate_consistency(self):
        # validate file references
        errors = []

        column_ids = {c.id for c in self.columns}
        dataset_files_and_zip_ids = {f.id for f in self.assembly} | {
            f.file.id for f in self.files
        }

        # files
        for fo in self.files:
            for c in fo.dataset_overrides.overridden_columns:
                if c.id not in column_ids:
                    errors.append(
                        f"MISSING COLUMN: file {fo.file.id} references undefined column {c.id}"
                    )
            for c in fo.dataset_overrides.omitted_columns:
                if c not in column_ids:
                    errors.append(
                        f"MISSING COLUMN: file override for {fo.file.id} references undefined column {c}"
                    )

        # destinations
        for d in self.destinations:
            for df in d.files:
                if df.id not in dataset_files_and_zip_ids:
                    errors.append(
                        f"MISSING FILE: destination {d.id} references undefined file {df.id}"
                    )
                for c in df.dataset_overrides.omitted_columns:
                    if c not in column_ids:
                        errors.append(
                            f"MISSING COLUMN: file override for dest {d.id} references undefined omitted column {c}"
                        )
                for c in df.dataset_overrides.overridden_columns:
                    if c.id not in column_ids:
                        errors.append(
                            f"MISSING COLUMN: destination {d.id} references undefined column {c.id}"
                        )

        # assemblies
        for a in self.assembly:
            for df in a.contents:
                if df.id not in dataset_files_and_zip_ids:
                    errors.append(
                        f"MISSING FILE: zip {df.id} references undefined file {df.id}"
                    )

        return errors

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
        with open(path, "r", encoding="utf-8") as raw:
            return cls.from_yaml(raw.read(), template_vars=template_vars)
