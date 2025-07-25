from bs4 import BeautifulSoup
from itertools import groupby
import json
from pathlib import Path
from pydantic import BaseModel, TypeAdapter
import requests

from dcpy.connectors.registry import VersionedConnector
from dcpy.connectors import web
from dcpy.utils.logging import logger


class _RawBytesCatalogYear(BaseModel):
    link: str
    text: str  # Note: this is actually the version string


class _RawBytesCatalogVersion(BaseModel):
    dataset: str
    year: str
    releases: list[_RawBytesCatalogYear]


class BytesConnector(VersionedConnector):
    conn_type: str = "bytes"

    S_MEDIA_ZIP_PREFIX: str = (
        "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes"
    )

    BYTES_CATALOG_URL_PREFIX: str = (
        "https://www.nyc.gov/assets/planning/json/content/resources/dataset-archives"
    )
    BYTES_API_PREFIX: str = (
        "https://apps.nyc.gov/content-api/v1/content/planning/resources/datasets"
    )

    # Overrides for BYTES resources. These are only needed where dataset names don't match up with bytes url info.
    BYTES_PAGE_CONFIGS: dict[str, dict] = {
        "pluto": {
            "change_file": {"bytes_resource_name": "mappluto-pluto-change"},
            "pluto": {"bytes_resource_name": "mappluto-pluto-change"},
        },
        "lion": {
            "2010_census_blocks": {
                "bytes_resource_name": "census-blocks",
                "files": {
                    "shapefile_water_not_included": {
                        "filename_template": lambda v: f"nycb2010_{v}.zip",
                    }
                },
            },
            "2020_census_blocks": {"bytes_resource_name": "census-blocks"},
            "borough_boundaries": {
                "files": {
                    "shapefile_wi": {
                        "filename_template": lambda v: f"nybb_{v}.zip",
                    }
                }
            },
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
        return self.BYTES_PAGE_CONFIGS.get(product, {}).get(dataset, {}).get(
            "bytes_resource_name"
        ) or dataset.replace("_", "-")

    def _fetch_archived_product_versions_by_dataset(self, product, dataset):
        """Fetch the json config for the archived dataset versions.
        This includes all versions except for the current one. It also includes
        all datasets on the given page, so e.g. for PLUTO this fetch returns
        PLUTO, MapPLUTO, and the Change File.

        # MAYBE TODO: we're not cleanly mapping or filtering the returned files on Bytes per dataset.
        E.g. the fetch for `pluto.change_file` might return something like:
        [{'dataset': 'MapPLUTO™ - Shapefile', 'version': '25v1'...} ...
         {'dataset': 'PLUTO™', 'version': '25v1'...}
        This then acts as though 25v1 is a version of the changefile as well,
        even if the change_file is not returned in the response. It might be a safe assumption.

        TLDR: for BYTES pages with multiple datasets, we act as though each dataset will have every version.
        """
        resource_name = self._get_product_dataset_bytes_resource(product, dataset)
        dataset = dataset or product
        catalog_url_file = f"{self.BYTES_CATALOG_URL_PREFIX}/{resource_name}.json"

        logger.info(f"Grabbing version catalog from {catalog_url_file}")
        response = requests.get(catalog_url_file)
        product_versions = TypeAdapter(list[_RawBytesCatalogVersion]).validate_python(
            response.json()
        )

        versions = []
        for pv in product_versions:
            for year_version in pv.releases:
                versions.append(
                    {
                        "dataset": pv.dataset,
                        # Somewhat frustratingly, return versions are up-cased, e.g. 25A,
                        # whereas the versions in filenames are downcased, e.g. myshapefile_25a.zip
                        "version": year_version.text.lower(),
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

        return latest_release_text.text.split(": ")[1].lower()

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
        product, dataset = self._key_to_product_dataset(key)
        files = self.BYTES_PAGE_CONFIGS[product][dataset]["files"]
        resource_name = self._get_product_dataset_bytes_resource(product, dataset)

        file_id = (
            kwargs.get("file_id") or list(files.keys())[0]
        )  # default to the first file if none are spec'd

        assert file_id in files, (
            f"{file_id} was specified, but the Bytes connector does not have this file mapped."
        )
        bytes_filename = files[file_id]["filename_template"](version)
        file_url = f"{self.S_MEDIA_ZIP_PREFIX}/{resource_name}/{bytes_filename}"

        destination_path.mkdir(exist_ok=True, parents=True)

        web.download_file(file_url, path=destination_path / bytes_filename)
        return {"path": destination_path}

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
