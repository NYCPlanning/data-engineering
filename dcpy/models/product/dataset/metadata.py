from __future__ import annotations

from pydantic import BaseModel
from tabulate import tabulate  # type: ignore
from typing import Any, List
import unicodedata

from dcpy.utils.collections import deep_merge_dict as merge
from dcpy.models.base import SortedSerializedBase, YamlWriter, TemplatedYamlReader
from dcpy.models.dataset import Column, COLUMN_TYPES

ERROR_MISSING_COLUMN = "MISSING COLUMN"


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
class ColumnValue(CustomizableBase):
    _head_sort_order = ["value", "description"]

    value: str
    description: str | None = None


def make_value_table(values: list[ColumnValue]) -> str:
    return (
        tabulate(
            [
                [str(v.value) + " ", str(v.description or " ") + " "]  # bool issue
                for v in values
            ],
            headers=["Value", "Description"],
            tablefmt="presto",
            maxcolwidths=[10, 40],
        )
        if values
        else ""
    )


class DatasetColumn(CustomizableBase, Column):
    _head_sort_order = ["id", "name", "data_type", "description"]
    _tail_sort_order = ["example", "values", "custom"]
    _repr_functions = {"values": make_value_table}

    # Note: id isn't intended to be overrideable, but is always required as a
    # pointer back to the original column.
    name: str | None = None
    data_source: str | None = None
    description: str | None = None
    limitations: str | None = None
    notes: str | None = None
    example: str | None = None
    deprecated: bool | None = None
    values: list[ColumnValue] | None = None

    def override(self, overrides: DatasetColumn) -> DatasetColumn:
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


class DatasetOrgProductAttributesOverride(CustomizableBase):
    """Fields that might be set as a default at the Product/Org level."""

    agency: str | None = None
    agency_website_data_updated_automatically: bool | None = None
    attribution: str | None = None
    attribution_link: str | None = None
    can_be_automated: bool | None = None
    category: str | None = None
    contact_email: str | None = None
    contains_address: bool | None = (
        None  # `contains_address` refers specifically to addresses containing house, numbers + street names. (ie. not just streets, polys, etc.)
    )
    data_collection_method: str | None = None
    data_change_frequency: str | None = None
    date_made_public: str | None = None
    disclaimer: str | None = None
    geocoded: bool | None = None
    on_agency_website: bool | None = None
    potential_uses: str | None = None
    projection: str | None = None
    publishing_frequency: str | None = None  # TODO: picklist values
    publishing_frequency_details: str | None = None
    publishing_purpose: str | None = None
    rows_removed: bool | None = None
    tags: List[str] | None = []


class DatasetAttributesOverride(DatasetOrgProductAttributesOverride):
    description: str | None = None
    display_name: str | None = None
    each_row_is_a: str | None = None


class DatasetAttributes(DatasetOrgProductAttributesOverride):
    display_name: str
    description: str = ""
    each_row_is_a: str

    def override(self, overrides: DatasetAttributesOverride) -> DatasetAttributes:
        return DatasetAttributes(**merge(self.model_dump(), overrides.model_dump()))

    def apply_defaults(self, defaults: BaseModel) -> DatasetAttributes:
        return DatasetAttributes(**merge(defaults.model_dump(), self.model_dump()))


class DatasetOverrides(CustomizableBase):
    overridden_columns: list[DatasetColumn] = []
    omitted_columns: list[str] = []
    attributes: DatasetAttributesOverride = DatasetAttributesOverride()


class Revision(CustomizableBase):
    date: str
    summary: str
    notes: str


class Dataset(CustomizableBase):
    columns: list[DatasetColumn]
    attributes: DatasetAttributes
    revisions: list[Revision] = []

    def override(self, overrides: DatasetOverrides) -> Dataset:
        """Apply column updates and prune any columns specified as omitted"""
        overriden_cols_by_id = {c.id: c for c in overrides.overridden_columns}

        columns = [
            c.override(overriden_cols_by_id.get(c.id, DatasetColumn(id=c.id)))
            for c in self.columns
            if c.id not in overrides.omitted_columns
        ]

        return Dataset(
            columns=columns,
            attributes=self.attributes.override(overrides.attributes),
            revisions=self.revisions,
        )


# DESTINATION
class Destination(CustomizableBase):
    id: str
    type: str
    tags: list[str] = []


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


class Metadata(CustomizableBase, YamlWriter, TemplatedYamlReader):
    id: str
    attributes: DatasetAttributes
    assembly: List[Package] = []
    columns: List[DatasetColumn] = []
    files: List[FileAndOverrides] = []
    destinations: List[DestinationWithFiles] = []
    revisions: list[Revision] = []

    _head_sort_order = [
        "id",
        "attributes",
    ]
    _tail_sort_order = ["columns"]
    _exclude_falsey_values = False  # We never want to prune top-level attrs

    @property
    def dataset(self):
        return Dataset(
            attributes=self.attributes, columns=self.columns, revisions=self.revisions
        )

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

    def get_column(self, column_id: str):
        cols = [c for c in self.columns if c.id == column_id]
        if len(cols) != 1:
            raise Exception(
                f"There should exist one column with id: {column_id}. Found {len(cols)}"
            )
        return cols[0]

    def get_file_ids(self):
        return {f.file.id for f in self.files}

    def get_file_and_overrides(self, file_id: str) -> FileAndOverrides:
        files = [f for f in self.files if f.file.id == file_id]
        if len(files) != 1:
            raise Exception(f"There should exist one file with id: {file_id}")
        return files[0]

    def calculate_metadata(
        self, *, file_id: str, destination_id: str | None = None
    ) -> Dataset:
        if destination_id:
            return self.calculate_destination_metadata(
                file_id=file_id, destination_id=destination_id
            ).dataset
        else:
            return self.calculate_file_dataset_metadata(file_id=file_id)

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
                        f"{ERROR_MISSING_COLUMN}: file {fo.file.id} references undefined column {c.id}"
                    )
            for c in fo.dataset_overrides.omitted_columns:
                if c not in column_ids:
                    errors.append(
                        f"{ERROR_MISSING_COLUMN}: file override for {fo.file.id} references undefined column {c}"
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
                            f"{ERROR_MISSING_COLUMN}: file override for dest {d.id} references undefined omitted column {c}"
                        )
                for c in df.dataset_overrides.overridden_columns:
                    if c.id not in column_ids:
                        errors.append(
                            f"{ERROR_MISSING_COLUMN}: destination {d.id} references undefined column {c.id}"
                        )

        # assemblies
        for a in self.assembly:
            for df in a.contents:
                if df.id not in dataset_files_and_zip_ids:
                    errors.append(
                        f"MISSING FILE: zip {df.id} references undefined file {df.id}"
                    )

        for c in self.columns:
            if c.name is None:
                errors.append(f"MISSING COLUMN NAME: column id {c.id}")
            if c.data_type is None:
                errors.append(f"MISSING COLUMN DATA TYPE: column id {c.id}")

        return errors

    def apply_column_defaults(
        self, column_defaults: dict[tuple[str, COLUMN_TYPES], DatasetColumn]
    ) -> list[DatasetColumn]:
        return [
            c.override(column_defaults[c.id, c.data_type])
            if c.data_type and (c.id, c.data_type) in column_defaults
            else c
            for c in self.columns
        ]
