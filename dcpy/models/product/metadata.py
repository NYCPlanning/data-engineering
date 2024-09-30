from collections import defaultdict
from pathlib import Path
from pydantic import Field

from dcpy.models.base import SortedSerializedBase, YamlWriter, TemplatedYamlReader
from dcpy.models.product.dataset.metadata_v2 import Metadata as DatasetMetadata


ERROR_PRODUCT_DATASET_METADATA_INSTANTIATION = (
    "Error instantiating dataset metadata for"
)
ERROR_PRODUCT_METADATA_INSTANTIATION = "Error instantiating product metadata"


class DefaultDatasetAttributes(SortedSerializedBase):
    """Anything overrideable at the dataset level"""

    contains_address: bool | None = None
    date_made_public: str | None = None
    publishing_purpose: str | None = None
    potential_uses: str | None = None
    publishing_frequency: str | None = None  # TODO: picklist values
    publishing_frequency_details: str | None = None
    tags: list[str] | None = None


class Attributes(DefaultDatasetAttributes, extra="ignore"):
    display_name: str | None = None
    description: str | None = None

    def to_dataset_attributes(self):
        return DefaultDatasetAttributes(**self.model_dump())


class ProductMetadataFile(
    SortedSerializedBase, YamlWriter, TemplatedYamlReader, extra="forbid"
):
    id: str
    datasets: list[str] = []
    attributes: Attributes = Field(default_factory=Attributes)


class OrgMetadataFile(TemplatedYamlReader, SortedSerializedBase, extra="forbid"):
    products: list[str]


class ProductFolder(SortedSerializedBase, extra="forbid"):
    root_path: Path
    template_vars: dict = {}

    def _dataset_folders(self):
        return [p.parent.name for p in self.root_path.glob("*/*.yml")]

    def get_product_metadata(self) -> ProductMetadataFile:
        return ProductMetadataFile.from_path(
            self.root_path / "metadata.yml",
            template_vars=self.template_vars,
        )

    def get_product_dataset(
        self,
        dataset_id,
        product_metadata: ProductMetadataFile,
    ) -> DatasetMetadata:
        ds_md = DatasetMetadata.from_path(
            self.root_path / dataset_id / "metadata.yml",
            template_vars=self.template_vars,
        )

        ds_md.attributes = ds_md.attributes.apply_defaults(
            product_metadata.attributes.to_dataset_attributes()
        )
        return ds_md

    def get_datasets_by_id(self) -> dict[str, DatasetMetadata]:
        md = self.get_product_metadata()
        dataset_mds = [self.get_product_dataset(ds_id, md) for ds_id in md.datasets]
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
        md = self.get_product_metadata()
        product_errors = {}

        for ds_id in md.datasets:
            errors = []
            try:
                errors = self.get_product_dataset(ds_id, md).validate_consistency()
            except Exception as e:
                errors = [
                    f"Error instantiating dataset metadata for {md.id}: {ds_id}: {e}"
                ]
            if errors:
                product_errors[ds_id] = errors
        return product_errors


class MetadataRepoFolder(SortedSerializedBase, extra="forbid"):
    root_path: Path
    template_vars: dict = Field(default_factory=dict)
    metadata: OrgMetadataFile

    @classmethod
    def from_path(cls, path: Path):
        return MetadataRepoFolder(
            root_path=path, metadata=OrgMetadataFile.from_path(path / "metadata.yml")
        )

    def product(self, name) -> ProductFolder:
        return ProductFolder(
            root_path=self.root_path / "products" / name,
            template_vars=self.template_vars,
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
