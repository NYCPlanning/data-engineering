from pathlib import Path

from dcpy.models.base import SortedSerializedBase, YamlWriter, TemplatedYamlReader
from dcpy.models.product.dataset.metadata_v2 import Metadata as DatasetMetadata
# from dcpy.models.product.dataset.metadata_v2 import DatasetAttributes


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


class Metadata(SortedSerializedBase, YamlWriter, TemplatedYamlReader, extra="forbid"):
    id: str
    datasets: list[str] = []
    dataset_collections: dict[str, list[str]] = {}

    attributes: Attributes


class ProductFolder(SortedSerializedBase, extra="forbid"):
    root_path: Path
    template_vars: dict = {}

    def get_product_metadata(self) -> Metadata:
        return Metadata.from_path(
            self.root_path / "metadata.yml",
            template_vars=self.template_vars,
        )

    def get_product_dataset(
        self,
        dataset_id,
        product_metadata: Metadata,
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
