from __future__ import annotations
from collections import defaultdict
import pandas as pd
from pathlib import Path
from pydantic import BaseModel, Field, TypeAdapter
from typing import ClassVar
import yaml

from dcpy.models.base import SortedSerializedBase, YamlWriter, TemplatedYamlReader
from dcpy.models.product.artifacts import Artifacts, Artifact
from dcpy.models.product.data_dictionary import DataDictionary
from dcpy.models.product.dataset.metadata import (
    Metadata as DatasetMetadata,
    DatasetColumn,
    DatasetOrgProductAttributesOverride,
    COLUMN_TYPES,
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

    def destinations(self) -> pd.DataFrame:
        """Helper to display all destinations under a product"""
        filtered_datasets = self.get_datasets_by_id()
        found_dests = []

        for ds in filtered_datasets.values():
            for dest in ds.destinations:
                found_dests.append(
                    {
                        "product": self.metadata.id,
                        "dataset": ds.id,
                        "destination": dest.id,
                        "destination_type": dest.type,
                        "remote_id": (dest.custom or {}).get("four_four"),
                        "tags": dest.tags or [],
                    }
                )
        df = pd.DataFrame(found_dests)
        df["destination_path"] = df.apply(
            lambda r: f"{r['product']}.{r.dataset}.{r.destination}", axis=1
        )
        return df.set_index(["product", "dataset", "destination"]).sort_index()

    def query_destinations(
        self,
        *,
        datasets: frozenset[str] | None = None,
        destination_id: str | None = None,
        destination_type: str | None = None,
        destination_tag: str | None = None,
    ) -> dict[str, dict[str, DatasetMetadata]]:
        """Retrieve a map[map] of dataset->destination->DatasetMetadata filtered by
           - destination type. (e.g. Socrata)
           - dataset name
           - tags

        e.g. for LION: {"2020_census_blocks": {"socrata_water_included": [Fully rendered metadata for this destination]}}
        """
        filtered_datasets = self.get_datasets_by_id()
        found_dests: dict[str, dict[str, DatasetMetadata]] = defaultdict(dict)

        for ds in filtered_datasets.values():
            for dest in ds.destinations:
                if (
                    (not destination_type or dest.type == destination_type)
                    and (not destination_id or dest.id == destination_id)
                    and (not datasets or ds.id in datasets)
                    and (not destination_tag or destination_tag in dest.tags)
                ):
                    found_dests[ds.id][dest.id] = ds
        return found_dests

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
