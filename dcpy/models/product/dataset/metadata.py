from __future__ import annotations
import jinja2
from pathlib import Path
from pydantic import BaseModel, conlist
from typing import Any, Literal
import yaml

from dcpy.utils.logging import logger
from . import metadata_v2 as md_v2

# Putting this here so we can have a constant value in all connectors
# and throw an exception when we attempt to deserialize files that contain it.
FILL_ME_IN_PLACEHOLDER = "<FILL ME IN!>"


class Column(BaseModel, extra="forbid"):
    name: str
    display_name: str
    description: str
    data_type: str

    # Optional
    data_source: str | None = None
    example: Any | None = None
    non_nullable: bool | None = None
    is_primary_key: bool | None = None
    readme_data_type: str | None = None
    deprecated: bool | None = None
    values: list[list] | None = None


class SocrataColumn(BaseModel, extra="forbid"):
    name: str | None = None
    api_name: str | None = None
    display_name: str | None = None
    description: str | None = None
    is_primary_key: bool = False


class DatasetOverrides(BaseModel, extra="forbid"):
    omit_columns: list[str] = []
    ignore_validation: list[str] = []
    columns: dict = {}
    display_name: str | None = None
    description: str | None = None
    tags: list[str] = []
    destination_file_name: str | None = None  # TODO filename


class RemoteFile(BaseModel, extra="forbid"):
    id: str
    url: str


class BytesDestination(BaseModel, extra="forbid"):
    type: Literal["bytes"]
    id: str
    files: list[RemoteFile]
    overrides: DatasetOverrides = DatasetOverrides()


DEFAULT_SOCRATA_CATEGORY = "city government"


class SocrataMetada(BaseModel, extra="forbid"):
    name: str
    description: str
    tags: list[str] = []
    metadata: dict[str, str] = {}
    # category: str = DEFAULT_SOCRATA_CATEGORY


class SocrataDestination(BaseModel, extra="forbid"):
    id: str
    type: Literal["socrata"] = "socrata"
    four_four: str
    attachments: list[str] = []
    datasets: conlist(item_type=str, max_length=1)  # type:ignore
    omit_columns: list[str] = []
    column_details: dict[str, SocrataColumn] = {}
    overrides: DatasetOverrides = DatasetOverrides()
    is_unparsed_dataset: bool = False

    def get_metadata(self, md: Metadata) -> SocrataMetada:
        dataset_file_overrides = md.package.get_dataset(self.datasets[0]).overrides

        # It would be nice to have a more comprehensive way to combine these overrides
        return SocrataMetada(
            name=self.overrides.display_name
            or dataset_file_overrides.display_name
            or md.display_name,
            description=self.overrides.description
            or dataset_file_overrides.description
            or md.description,
            tags=self.overrides.tags or dataset_file_overrides.tags or md.tags,
            metadata={"rowLabel": md.each_row_is_a},
        )

    def get_column_overrides(self, col_name):
        col = self.column_details.get(col_name, SocrataColumn())
        # Unfortunate reality of serializing with pydantic from a yaml dictionary.
        # columns overrides aren't deserialized with their name value,
        # since the name is a dictionary key in the metadata. So:
        col.name = col_name
        return col

    def destination_column_metadata(self, metadata: Metadata) -> list[SocrataColumn]:
        soc_cols = []
        dataset = metadata.package.get_dataset(self.datasets[0])
        for col in dataset.get_columns(metadata):
            if col.name in self.omit_columns:
                continue
            overrides = self.get_column_overrides(col.name)

            soc_cols.append(
                SocrataColumn(
                    name=col.name,
                    api_name=overrides.api_name or col.name,
                    display_name=overrides.display_name or col.display_name,
                    description=overrides.description or col.description,
                )
            )
        return soc_cols


class File(BaseModel, extra="forbid"):
    name: str  # TODO: migrate to `key` or `id`
    filename: str
    type: str | None = None

    @property
    def id(self):  # TODO: remove when we migrate
        return self.name


class DatasetFile(File, extra="forbid"):
    overrides: DatasetOverrides = DatasetOverrides()

    def get_columns(self, metadata: Metadata) -> list[Column]:
        cols = []
        for col in metadata.columns:
            if col.name in self.overrides.omit_columns:
                continue
            overrides = self.overrides.columns.get(col.name, {})
            new_col = col.model_dump()
            new_col.update(overrides)
            cols.append(Column(**new_col))
        return cols


class ZipFile(File, extra="forbid"):
    type: Literal["Zip"] = "Zip"
    contains: list[File]


class Package(BaseModel, extra="forbid"):
    # TODO: should these just be combined into one flat list of files? Why have three distinctions
    dataset_files: list[DatasetFile]
    attachments: list[File]
    zip_files: list[ZipFile] = []

    _ASSET_TYPES = Literal["dataset_files", "attachments", "zip_files"]

    def __init__(self, **data):
        file_attachments = []
        for a in data["attachments"]:
            if type(a) is str:
                logger.warning(
                    f"Found string attachment type: {a}. Migrate to File type."
                )
                file_attachments.append(File(name=a, filename=a).model_dump())
            else:
                file_attachments.append(a)
        data["attachments"] = file_attachments
        super().__init__(**data)

    def asset_types_by_file_id(self) -> dict[str, _ASSET_TYPES]:
        return (
            {dsf.id: "dataset_files" for dsf in self.dataset_files}
            | {a.id: "attachments" for a in self.attachments}
            | {z.id: "zip_files" for z in self.zip_files}
        )  # type: ignore

    def get_files(self) -> list[File]:
        return self.dataset_files + self.zip_files + self.attachments

    def get_dataset(self, ds_id: str) -> DatasetFile:
        ds = [d for d in self.dataset_files if d.id == ds_id]
        if len(ds) == 1:
            return ds[0]
        raise Exception(f"No dataset named {ds_id}")

    def files_by_id(self) -> dict[str, File]:
        return {f.id: f for f in self.get_files()}


class Metadata(BaseModel, extra="forbid"):
    name: str
    display_name: str
    summary: str  # TODO: potentially remove this field
    description: str
    tags: list[str]
    each_row_is_a: str

    destinations: list[
        BytesDestination | SocrataDestination
    ]  # TODO: Destination superclass
    package: Package
    columns: list[Column]

    # Hold onto this for serialization, to avoid Pydantics reformatting of the metadata.yml
    _templated_source_metadata: str

    def __init__(self, templated_source_metadata: str = "", **data):
        super().__init__(**data)
        self._templated_source_metadata = templated_source_metadata

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

    def write_source_metadata_to_file(self, path: Path):
        with open(path, "w") as f:
            f.write(self._templated_source_metadata)

    def get_destination(self, dest_id) -> BytesDestination | SocrataDestination:
        ds = [d for d in self.destinations if d.id == dest_id]
        if len(ds) == 1:
            return ds[0]
        raise Exception(f"No destination named {dest_id}")

    def validate(self):
        col_names = [c.name for c in self.columns]
        assert len(col_names) == len(
            set(col_names)
        ), "There should be no duplicate column names"
        # TODO: all the rest

    def upgrade_to_v2(self):
        def _remove_falsey_from_dict(d):
            return {k: v for k, v in d.items() if v}

        def _translate_types(s: str):
            old_to_new_types = {
                "boolean": "bool",
                "double": "decimal",
                "float": "decimal",
                "geom_point": "geometry",
                "geom_poly": "geometry",
                "wkb": "geometry",
            }
            return old_to_new_types.get(s) or s

        def _construct_with_custom(cls, **kwargs):
            kwargs_with_custom = {"custom": {}}
            for kwarg in kwargs.items():
                if kwarg[0] in cls.__fields__:
                    kwargs_with_custom[kwarg[0]] = kwarg[1]
                else:
                    kwargs_with_custom["custom"][kwarg[0]] = kwarg[1]
            return cls(**kwargs_with_custom)

        def _v1_overrides_to_v2(v1: DatasetOverrides):
            overridden_columns = []
            for k, col_overrides in v1.columns.items():
                if "data_type" in col_overrides:
                    col_overrides["data_type"] = _translate_types(
                        col_overrides["data_type"]
                    )
                custom = _construct_with_custom(
                    md_v2.DatasetColumnOverrides, id=k, **col_overrides
                )
                overridden_columns.append(custom)

            return md_v2.DatasetOverrides(
                overridden_columns=overridden_columns,
                omitted_columns=v1.omit_columns,
                attributes=md_v2.DatasetAttributesOverride(
                    display_name=v1.display_name,
                    description=md_v2.normalize_text(v1.description or ""),
                ),
            )

        return md_v2.Metadata(
            id=self.name,
            files=[
                md_v2.FileAndOverrides(
                    file=md_v2.File(
                        id=dsf.name,
                        filename=dsf.filename,
                        type=str(dsf.type),
                        custom=_remove_falsey_from_dict(
                            {"ignore_validation": dsf.overrides.ignore_validation}
                        ),
                    ),
                    dataset_overrides=_v1_overrides_to_v2(dsf.overrides),
                )
                for dsf in self.package.dataset_files
            ]
            + [
                md_v2.FileAndOverrides(
                    file=md_v2.File(
                        id=att.name,
                        filename=att.filename,
                        type=str(att.type),
                        is_metadata=True,
                    )
                )
                for att in self.package.attachments
            ],
            assembly=[
                md_v2.Package(
                    id=zf.name,
                    type=zf.type,
                    filename=zf.filename,
                    contents=[
                        md_v2.PackageFile(id=pf.name, filename=pf.filename)
                        for pf in zf.contains
                    ],
                )
                for zf in self.package.zip_files
            ],
            destinations=[  # Socrata Dests
                md_v2.DestinationWithFiles(
                    id=dest.id,
                    type="socrata",
                    files=[
                        md_v2.DestinationFile(
                            id=att,
                            custom={"destination_use": "attachment"},
                        )
                        for att in dest.attachments
                    ]
                    + [
                        md_v2.DestinationFile(
                            id=ds,
                            custom={"destination_use": "dataset_file"},
                            dataset_overrides=md_v2.DatasetOverrides(
                                overridden_columns=[
                                    md_v2.DatasetColumnOverrides(
                                        id=k,
                                        name=v.display_name,
                                        description=md_v2.normalize_text(
                                            v.description or ""
                                        ),
                                        custom=_remove_falsey_from_dict(
                                            {"api_name": v.api_name}
                                        ),
                                    )
                                    for k, v in dest.column_details.items()
                                ]
                            ),
                        )
                        for ds in dest.datasets
                    ],
                    custom=_remove_falsey_from_dict(
                        {
                            "four_four": dest.four_four,
                            "is_unparsed_dataset": dest.is_unparsed_dataset,
                        }
                    ),
                )
                for dest in self.destinations
                if type(dest) == SocrataDestination
            ]
            + [
                md_v2.DestinationWithFiles(
                    id=dest.id,
                    type="bytes",
                    files=[
                        md_v2.DestinationFile(id=f.id, custom={"url": f.url})
                        for f in dest.files
                    ],
                )
                for dest in self.destinations
                if type(dest) == BytesDestination
            ],  # md_v2,
            attributes=md_v2.DatasetAttributes(
                display_name=self.display_name,
                description=md_v2.normalize_text(self.description or ""),
                each_row_is_a=self.each_row_is_a,
                tags=self.tags,
            ),
            columns=[
                md_v2.DatasetColumn(
                    data_type=_translate_types(v1_col.data_type),
                    name=v1_col.display_name,
                    data_source=md_v2.normalize_text(v1_col.data_source or ""),
                    description=md_v2.normalize_text(v1_col.description or ""),
                    example=str(v1_col.example),
                    deprecated=v1_col.deprecated,
                    values=[
                        md_v2.ColumnValue(
                            value=str(cv[0]),
                            description=md_v2.normalize_text(
                                str(cv[1]) if len(cv) > 1 else ""
                            ),
                            custom=_remove_falsey_from_dict(
                                {"other_details": str(cv[2:]) if len(cv) > 2 else None}
                            ),
                        )
                        for cv in v1_col.values or []
                    ],
                    id=v1_col.name,
                    checks=md_v2.Checks(
                        is_primary_key=v1_col.is_primary_key,
                        non_nullable=v1_col.non_nullable,
                    ),
                    custom=_remove_falsey_from_dict(
                        {
                            "readme_data_type": md_v2.normalize_text(
                                v1_col.readme_data_type or ""
                            )
                        }
                    ),
                )
                for v1_col in self.columns
            ],
        )
