from __future__ import annotations

from pathlib import Path
from typing import ClassVar

import pandas as pd
import yaml
from pydantic import BaseModel, Field, TypeAdapter

from dcpy.models.base import SortedSerializedBase, TemplatedYamlReader, YamlWriter
from dcpy.models.product.artifacts import Artifact, Artifacts
from dcpy.models.product.data_dictionary import DataDictionary
from dcpy.models.product.dataset.metadata import (
    COLUMN_TYPES,
    DatasetColumn,
    DatasetOrgProductAttributesOverride,
)
from dcpy.models.product.dataset.metadata import (
    Metadata as DatasetMetadata,
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
    DATASET_NOT_LISTED_ERROR: ClassVar[str] = "Dataset not listed in metadata"

    root_path: Path
    metadata: ProductMetadataFile
    template_vars: dict = {}
    column_defaults: dict[tuple[str, COLUMN_TYPES], DatasetColumn] = {}
    org_attributes: DatasetOrgProductAttributesOverride

    @classmethod
    def from_path(
        cls,
        root_path: Path,
        template_vars: dict = {},
        column_defaults: dict[tuple[str, COLUMN_TYPES], DatasetColumn] = {},
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
        if dataset_id not in self.metadata.datasets:
            raise Exception(f"{self.DATASET_NOT_LISTED_ERROR}: {dataset_id}")

        ds_md = DatasetMetadata.from_path(
            self.root_path / dataset_id / "metadata.yml",
            template_vars=self.template_vars,
        )
        if ds_md.id != dataset_id:
            raise Exception(
                (
                    "There is a mismatch between the dataset id listed at the"
                    f" dataset level ({ds_md.id}) vs the product-level ({dataset_id})"
                )
            )

        ds_md.attributes = ds_md.attributes.apply_defaults(
            self.metadata.dataset_defaults
        ).apply_defaults(self.org_attributes)

        ds_md.columns = ds_md.apply_column_defaults(self.column_defaults)

        return ds_md

    def get_datasets_by_id(self) -> dict[str, DatasetMetadata]:
        dataset_mds = [self.dataset(ds_id) for ds_id in self.metadata.datasets]
        return {m.id: m for m in dataset_mds}

    def all_destinations(self) -> list[dict]:
        """Get all destinations for a product"""
        found_dests = []
        for ds in self.get_datasets_by_id().values():
            for dest in ds.destinations:
                found_dests.append(
                    {
                        "product": self.metadata.id,
                        "dataset_id": ds.id,
                        "destination_id": dest.id,
                        "destination_type": dest.type,
                        # "remote_id": (dest.custom or {}).get("four_four"),
                        "tags": set(dest.tags or []),
                        "custom": dest.custom,
                        "destination_path": f"{self.metadata.id}.{ds.id}.{dest.id}",
                    }
                )
        return found_dests

    def all_destinations_df(self, grouped: bool = False) -> pd.DataFrame:
        """Helper to display all destinations for a product.
        Using the `grouped` flag wil group the output to make things visually a little easier.
        """
        df = pd.DataFrame(self.all_destinations())
        return (
            df.set_index(["product", "dataset_id", "destination_id"]).sort_index()
            if grouped
            else df
        )

    def query_dataset_destinations(
        self,
        *,
        dataset_ids: set[str] | None = None,
        destination_filter: dict | None = None,
    ) -> list[str]:
        """Retrieve a list of destination paths for given filters.
        .e.g. [lion.atomic_polygons.socrata, ...]
        """
        dataset_ids = (
            dataset_ids & set(self.metadata.datasets)
            if dataset_ids
            else set(self.metadata.datasets)
        )
        dest_paths = []
        for d in dataset_ids:
            dest_paths += [
                f"{self.metadata.id}.{d}.{dest_id}"
                for dest_id in self.dataset(d).query_destinations(
                    **(destination_filter or {})
                )
            ]
        return sorted(dest_paths)

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
    PRODUCT_NOT_LISTED_ERROR: ClassVar[str] = "Product not listed in metadata"

    root_path: Path
    template_vars: dict = Field(default_factory=dict)
    metadata: OrgMetadataFile
    column_defaults: dict[tuple[str, COLUMN_TYPES], DatasetColumn]
    data_dictionary: DataDictionary = DataDictionary()

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
    def get_column_defaults(
        cls, path: Path
    ) -> dict[tuple[str, COLUMN_TYPES], DatasetColumn]:
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
        dd_default_path = path / "data_dictionary.yml"
        return OrgMetadata(
            root_path=path,
            metadata=OrgMetadataFile.from_path(
                path / "metadata.yml", template_vars=template_vars
            ),
            template_vars=template_vars,
            column_defaults=cls.get_column_defaults(path),
            data_dictionary=DataDictionary.from_path(dd_default_path)
            if dd_default_path.exists()
            else DataDictionary(),
        )

    def product(self, name: str) -> ProductMetadata:
        if name not in self.metadata.products:
            raise Exception(f"{self.PRODUCT_NOT_LISTED_ERROR}: {name}")

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

    def get_packaging_artifacts(self) -> list[Artifact]:
        return Artifacts.from_path(
            self.root_path / "packaging" / "artifacts.yml"
        ).artifacts

    def get_full_resource_path(self, file: str | Path):
        return self.root_path / "packaging" / "resources" / file

    def query_product_dataset_destinations(
        self,
        *,
        product_ids: set[str] | None = None,
        dataset_ids: set[str] | None = None,
        destination_filter: dict | None = None,
    ) -> list[str]:
        """Query for all destinations matching filters.
        Returns a list of destination paths in the format product.dataset.destination_id
        """
        all_dests = []
        for p_name in (
            (product_ids & set(self.metadata.products))
            if product_ids
            else set(self.metadata.products)
        ):
            all_dests += self.product(p_name).query_dataset_destinations(
                dataset_ids=dataset_ids, destination_filter=destination_filter or {}
            )
        return sorted(all_dests)

    def get_product_dataset_destinations(self, destination_path: str):
        prod, ds, dest_id = destination_path.split(".")
        return self.product(prod).dataset(ds).get_destination(dest_id)
