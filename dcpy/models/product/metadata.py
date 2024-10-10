from __future__ import annotations
from collections import defaultdict
from pathlib import Path
from pydantic import BaseModel, Field, TypeAdapter
import yaml

from dcpy.models.base import SortedSerializedBase, YamlWriter, TemplatedYamlReader
from dcpy.models.product.dataset.metadata_v2 import (
    Metadata as DatasetMetadata,
    DatasetColumn,
    DatasetOrgProductAttributesOverride,
)
from dcpy.utils.collections import deep_merge_dict as merge


ERROR_PRODUCT_DATASET_METADATA_INSTANTIATION = (
    "Error instantiating dataset metadata for"
)
ERROR_PRODUCT_METADATA_INSTANTIATION = "Error instantiating product metadata"


class ProductAttributes(SortedSerializedBase, extra="forbid"):
    display_name: str | None = None
    description: str | None = None


class ProductMetadataFile(
    SortedSerializedBase, YamlWriter, TemplatedYamlReader, extra="forbid"
):
    id: str
    attributes: ProductAttributes = Field(default_factory=ProductAttributes)
    dataset_defaults: DatasetOrgProductAttributesOverride = Field(
        default_factory=DatasetOrgProductAttributesOverride
    )
    datasets: list[str] = []


class ProductMetadata(SortedSerializedBase, extra="forbid"):
    root_path: Path
    metadata: ProductMetadataFile
    template_vars: dict = {}
    column_defaults: dict[tuple[str, str], DatasetColumn] = {}
    org_attributes: DatasetOrgProductAttributesOverride

    @classmethod
    def from_path(
        cls,
        root_path: Path,
        template_vars: dict = {},
        column_defaults: dict[tuple[str, str], DatasetColumn] = {},
        org_attributes: DatasetOrgProductAttributesOverride = DatasetOrgProductAttributesOverride(),
    ) -> ProductMetadata:
        return ProductMetadata(
            root_path=root_path,
            metadata=ProductMetadataFile.from_path(
                root_path / "metadata.yml", template_vars=template_vars
            ),
            template_vars=template_vars,
            column_defaults=column_defaults,
            org_attributes=org_attributes,
        )

    def _dataset_folders(self):
        return [p.parent.name for p in self.root_path.glob("*/*.yml")]

    def dataset(self, dataset_id: str) -> DatasetMetadata:
        ds_md = DatasetMetadata.from_path(
            self.root_path / dataset_id / "metadata.yml",
            template_vars=self.template_vars,
        )

        ds_md.attributes = ds_md.attributes.apply_defaults(
            self.metadata.dataset_defaults
        ).apply_defaults(self.org_attributes)

        ds_md.columns = ds_md.apply_column_defaults(self.column_defaults)

        return ds_md

    def get_datasets_by_id(self) -> dict[str, DatasetMetadata]:
        dataset_mds = [self.dataset(ds_id) for ds_id in self.metadata.datasets]
        return {m.id: m for m in dataset_mds}

    def get_tagged_destinations(self, tag) -> dict[str, dict[str, DatasetMetadata]]:
        datasets = self.get_datasets_by_id()
        found_tagged_dests: dict[str, dict[str, DatasetMetadata]] = defaultdict(dict)
        for ds in datasets.values():
            for dest in ds.destinations:
                if tag in dest.tags:
                    found_tagged_dests[ds.id][dest.id] = ds
        return found_tagged_dests

    def validate_dataset_metadata(self) -> dict[str, list[str]]:
        product_errors = {}

        for ds_id in self.metadata.datasets:
            errors = []
            try:
                errors = self.dataset(ds_id).validate_consistency()
            except Exception as e:
                errors = [
                    f"Error instantiating dataset metadata for {self.metadata.id}: {ds_id}: {e}"
                ]
            if errors:
                product_errors[ds_id] = errors
        return product_errors


class ProductDatasetDestinationKey(BaseModel):
    product: str
    dataset: str
    destination: str


class OrgMetadataFile(TemplatedYamlReader, SortedSerializedBase, extra="forbid"):
    products: list[str]
    attributes: DatasetOrgProductAttributesOverride


class OrgMetadata(SortedSerializedBase, extra="forbid"):
    root_path: Path
    template_vars: dict = Field(default_factory=dict)
    metadata: OrgMetadataFile
    column_defaults: dict[tuple[str, str], DatasetColumn]

    @classmethod
    def get_string_snippets(cls, path: Path) -> dict:
        s_path = path / "snippets" / "strings.yml"
        if not s_path.exists():
            return {}

        with open(s_path, "r", encoding="utf-8") as raw:
            yml = yaml.safe_load(raw) or {}
        if not isinstance(yml, dict):
            raise ValueError("snippets must be valid yml dict, not array")
        return yml

    @classmethod
    def get_column_defaults(cls, path: Path) -> dict[tuple[str, str], DatasetColumn]:
        c_path = path / "snippets" / "column_defaults.yml"
        if not c_path.exists():
            return {}
        with open(c_path, "r", encoding="utf-8") as raw:
            yml = yaml.safe_load(raw) or []
        columns = TypeAdapter(list[DatasetColumn]).validate_python(yml)
        return {(c.id, c.data_type): c for c in columns if c.data_type}

    @classmethod
    def from_path(cls, path: Path, template_vars: dict | None = None):
        template_vars = merge(cls.get_string_snippets(path), template_vars or {}) or {}
        return OrgMetadata(
            root_path=path,
            metadata=OrgMetadataFile.from_path(
                path / "metadata.yml", template_vars=template_vars
            ),
            template_vars=template_vars,
            column_defaults=cls.get_column_defaults(path),
        )

    def product(self, name: str) -> ProductMetadata:
        return ProductMetadata.from_path(
            root_path=self.root_path / "products" / name,
            template_vars=self.template_vars,
            column_defaults=self.column_defaults,
            org_attributes=self.metadata.attributes,
        )

    def validate_metadata(self) -> dict[str, dict[str, list[str]]]:
        product_errors = {}
        for p in self.metadata.products:
            try:
                errors = self.product(p).validate_dataset_metadata()
                if errors:
                    product_errors[p] = errors
            except Exception as e:
                product_errors[p] = {
                    "product-level-metadata": [
                        f"{ERROR_PRODUCT_METADATA_INSTANTIATION}: {e}"
                    ]
                }
        return product_errors

    def query_dataset_destinations(
        self, tag: str
    ) -> list[ProductDatasetDestinationKey]:
        keys = []
        for p_name in self.metadata.products:
            for ds_id, ds_md in self.product(p_name).get_datasets_by_id().items():
                keys += [
                    ProductDatasetDestinationKey(
                        product=p_name, dataset=ds_id, destination=dest.id
                    )
                    for dest in ds_md.destinations
                    if tag in dest.tags
                ]
        return keys
