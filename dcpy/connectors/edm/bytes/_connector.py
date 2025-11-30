import json
from itertools import groupby
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, TypeAdapter

from dcpy.connectors import web
from dcpy.connectors.registry import VersionedConnector
from dcpy.utils.logging import logger

from . import _sitemap


class _RawBytesCatalogYear(BaseModel):
    link: str
    text: str  # Note: this is actually the version string


class _RawBytesCatalogVersion(BaseModel):
    dataset: str
    year: str
    releases: list[_RawBytesCatalogYear]


class BytesConnector(VersionedConnector):
    conn_type: str = "bytes"

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
        catalog_url_file = _sitemap.get_dataset_catalog_json_url(product, dataset)
        if not catalog_url_file:
            return {}

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
        url = _sitemap.get_most_recent_version_url(product, dataset)
        logger.info(f"Grabbing latest version from {url}")
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
        file_id = kwargs["file_id"]
        file_url = _sitemap.get_file_url(product, dataset, file_id, version=version)

        if (
            not destination_path.suffix
        ):  # If it lacks a suffix (e.g. .txt), we'll guess that it's a dir
            filename = _sitemap.get_filename(product, dataset, file_id, version=version)
            out_path = destination_path / filename
            destination_path.mkdir(exist_ok=True, parents=True)
        else:
            destination_path.parent.mkdir(exist_ok=True, parents=True)
            out_path = destination_path

        web.download_file(file_url, path=out_path)
        return {"path": out_path}

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        raise NotImplementedError()

    def list_versions(self, key: str, *, sort_desc: bool = True, **_) -> list[str]:
        latest_version = self.get_latest_version(key)

        # Unfortunately, the versions on BYTES often don't match our convention.
        # E.g. Facilities has versions like "March". Let's filter them out until we can fix or map them.
        product, dataset = self._key_to_product_dataset(key)
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

    def fetch_all_latest_versions(self) -> list[tuple[str, str, str]]:
        """Fetch latest versions for all datasets.

        Returns a tuple of (product.dataset, version, maybe_fetch_error)
        """
        versions = []
        for prod, ds in _sitemap.all_product_datasets():
            key = f"{prod}.{ds}"
            logger.info(f"Fetching latest version for {key}")
            try:
                versions.append((key, self.get_latest_version(key), ""))
            except Exception as e:
                versions.append((key, "", str(e)))
        return versions

    def fetch_all_latest_versions_df(self):
        """Dataframe version of `fetch_all_latest_versions` for ease of use."""
        df = pd.DataFrame(self.fetch_all_latest_versions())
        df.columns = ["key", "version", "version_fetch_error"]

        def _split_ds_key(row):
            prod, ds = row["key"].split(".")
            row["product"] = prod
            row["dataset"] = ds
            return row

        return (
            df.apply(_split_ds_key, axis="columns")
            .set_index(["product", "dataset"])
            .sort_index()[["version", "version_fetch_error"]]
        )

    def validate_product_file_urls(self, product, version):
        """Validate all product dataset files on BYTES.

        Returns a list of [(product, dataset, file_id), http_status_code]
        """
        product_files = [
            [file_key_tuple, url]
            for file_key_tuple, url in _sitemap.all_urls(version).items()
            if file_key_tuple[0] == product
        ]
        return [
            [file_key_tuple, requests.head(url).status_code, url]
            for file_key_tuple, url in product_files
        ]
