from bs4 import BeautifulSoup
from itertools import groupby
import json
from pathlib import Path
from pydantic import BaseModel, TypeAdapter
import requests

from dcpy.connectors.registry import VersionedConnector
from dcpy.utils.logging import logger


class _RawBytesCatalogueYear(BaseModel):
    link: str
    text: str  # Note: this is actually the version string


class _RawBytesCatalogueVersion(BaseModel):
    dataset: str
    year: str
    releases: list[_RawBytesCatalogueYear]


class BytesConnector(VersionedConnector):
    conn_type: str = "bytes"

    BYTES_CATALOGUE_URL_PREFIX: str = (
        "https://www.nyc.gov/assets/planning/json/content/resources/dataset-archives"
    )
    BYTES_API_PREFIX: str = (
        "https://apps.nyc.gov/content-api/v1/content/planning/resources/datasets"
    )

    # Overrides for BYTES resources. These are only needed where dataset names don't match up with bytes url info.
    BYTES_URL_RESOURCE_OVERRIDES: dict[str, dict] = {
        "pluto": {
            "change_file": "mappluto-pluto-change",
            "pluto": "mappluto-pluto-change",
        },
        "lion": {
            "2010_census_blocks": "census-blocks",
            "2020_census_blocks": "census-blocks",
        },
        # TODO: Add other datasets here. Or move this to product metadata, where it probably belongs
    }

    def _get_product_dataset_bytes_resource(self, product, dataset) -> str | None:
        """The Bytes convention for datasets seems to be that for each, there's a resource name like 'atomic-polygons' below:
                https://www.nyc.gov/content/planning/pages/resources/datasets/atomic-polygons
                https://apps.nyc.gov/content-api/v1/content/planning/resources/datasets/atomic-polygons
                https://www.nyc.gov/assets/planning/json/content/resources/dataset-archives/atomic-polygons.json
        For a dataset like lion.atomic_polygons, their dataset name matches up with ours (after twiddling the dash and underscore)
        but for a dataset like pluto, we're not so lucky so we need to do some overriding.
        """
        return self.BYTES_URL_RESOURCE_OVERRIDES.get(product, {}).get(
            dataset
        ) or dataset.replace("_", "-")

    def _fetch_archived_product_versions_by_dataset(self, product, dataset):
        """Fetch the json config for the archived dataset versions.
        This includes all versions except for the current one."""
        resource_name = self._get_product_dataset_bytes_resource(product, dataset)
        dataset = dataset or product
        catalogue_url_file = f"{self.BYTES_CATALOGUE_URL_PREFIX}/{resource_name}.json"

        logger.debug(f"Grabbing version catalogue from {catalogue_url_file}")
        response = requests.get(catalogue_url_file)
        product_versions = TypeAdapter(list[_RawBytesCatalogueVersion]).validate_python(
            response.json()
        )

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

    def _parse_most_recent_version_from_html(self, product, dataset) -> str:
        """Unfortunately the most recent version isn't recorded in a JSON file, so we need to parse it from the HTML."""
        resource_name = self._get_product_dataset_bytes_resource(product, dataset)
        url = f"{self.BYTES_API_PREFIX}/{resource_name}"
        logger.debug(f"Grabbing latest version from {url}")
        content = json.loads(requests.get(url).content)
        assert "description" in content, (
            f"The JSON response should have a `description` field. If not, it indicates the url is probably wrong. Response: {content}"
        )

        soup = BeautifulSoup(content["description"], features="html.parser")
        latest_release_text = soup.find(
            "span", string=lambda text: "Latest Release:" in text
        )
        assert latest_release_text, (
            "No `Latest Release:` text was found! Can't parse version."
        )

        return latest_release_text.text.split(": ")[1]

    def _key_to_product_dataset(self, key: str) -> tuple[str, str]:
        """e.g.
        - lion.atomic_polygons -> (lion, atomic_polygons)
        - lion -> (lion, lion)
        """
        split = key.split(".")
        assert len(split) <= 2, (
            f"Expected a key in the format of `product`, or a `product.dataset`. Got {key}"
        )
        product = split[0]
        dataset = (
            split[1] if len(split) == 2 else product
        )  # Eg. pluto is the same as pluto.pluto
        return product, dataset

    def pull_versioned(
        self, key: str, version: str, destination_path: Path, **kwargs
    ) -> dict:
        raise NotImplementedError()

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        raise NotImplementedError()

    def list_versions(self, key: str, *, sort_desc: bool = True, **_) -> list[str]:
        product, dataset = self._key_to_product_dataset(key)
        latest_version = self._parse_most_recent_version_from_html(product, dataset)

        # Unfortunately, the versions on BYTES often don't match our convention.
        # E.g. Facilities has versions like "March". Let's filter them out until we can fix or map them.
        dataset_versions = [
            v
            for v in self._fetch_archived_product_versions_by_dataset(
                product, dataset
            ).keys()
            if v[0].isdigit()
        ] + [latest_version]
        dataset_versions.sort(reverse=sort_desc)
        return dataset_versions

    def get_latest_version(self, key: str, **_) -> str:
        return self._parse_most_recent_version_from_html(
            *self._key_to_product_dataset(key)
        )
