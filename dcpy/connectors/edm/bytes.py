from itertools import groupby
from pathlib import Path
from pydantic import BaseModel, TypeAdapter
import requests

from dcpy.connectors.registry import VersionedConnector


class _RawBytesCatalogueYear(BaseModel):
    link: str
    text: str  # Note: this is actually the version string


class _RawBytesCatalogueVersion(BaseModel):
    dataset: str
    year: str
    releases: list[_RawBytesCatalogueYear]


class BytesPackageConnector(VersionedConnector):
    conn_type: str = "bytes"

    BYTES_CATALOGUE_URL_PREFIX: str = (
        "https://www.nyc.gov/assets/planning/json/content/resources/dataset-archives"
    )
    BYTES_CATALOGUE_CONFIG: dict[str, dict] = {
        "pluto": {
            "change_file": "mappluto-pluto-change.json",
            "pluto": "mappluto-pluto-change.json",
        },
        "lion": {
            "atomic_polygons": "atomic-polygons.json",
            "2010_census_blocks": "census-blocks.json",
            "2020_census_blocks": "census-blocks.json",
        },
        "facilities": {"facilities": "facilities.json"},
        # ETC...
    }

    def _catalogue_url(self, product, dataset):
        return f"{self.BYTES_CATALOGUE_URL_PREFIX}/{self.BYTES_CATALOGUE_CONFIG[product][dataset]}"

    def _fetch_product_versions_by_dataset(self, product, dataset=""):
        dataset = (
            dataset or product
        )  # e.g. takes care of cases like facilities.facilities
        resp_type = TypeAdapter(list[_RawBytesCatalogueVersion])

        catalogue_url = self._catalogue_url(product, dataset)
        response = requests.get(catalogue_url)
        product_versions = resp_type.validate_python(response.json())

        versions = []
        for pv in product_versions:
            for year_version in pv.releases:
                versions.append(
                    {
                        "dataset": pv.dataset,
                        "version": year_version.text,
                        "url": year_version.link,
                    }
                )
        return {
            ds: list(versions)
            for ds, versions in groupby(versions, lambda v: v["version"])
        }

    def pull_versioned(
        self, key: str, version: str, destination_path: Path, **kwargs
    ) -> dict:
        raise NotImplementedError()

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        raise NotImplementedError()

    def list_versions(self, key: str, *, sort_desc: bool = True, **kwargs) -> list[str]:
        split = key.split(".")
        assert len(split) <= 2, (
            f"Expected a key in the format of `product`, or a `product.dataset`. Got {key}"
        )
        product = split[0]
        dataset = split[1] if len(split) == 2 else product  # Eg. pluto.pluto
        dataset_versions = list(
            self._fetch_product_versions_by_dataset(product, dataset).keys()
        )
        if sort_desc:
            dataset_versions.sort(reverse=True)
        return dataset_versions

    def get_latest_version(self, key: str, **kwargs) -> str:
        raise NotImplementedError()

    def version_exists(self, key: str, version: str, **kwargs) -> bool:
        raise NotImplementedError()

    def data_local_sub_path(
        self, key: str, *, version: str, revision: str, **kwargs
    ) -> Path:
        raise NotImplementedError()
