from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, TypeAdapter

S_MEDIA_FILES_URL_BASE = "https://s-media.nyc.gov/agencies/dcp/assets/files/"
PLANNING_ASSETS_URL_BASE = "https://www1.nyc.gov/assets/planning/download/"

FILE_TYPE_SUFFIXES_TO_URL_PART = {
    "xlsx": "excel/data-tools/bytes/",
    "pdf": "pdf/data-tools/bytes/",
    "zip": "zip/data-tools/bytes/",
}

BYTES_CATALOG_URL_PREFIX = (
    "https://www.nyc.gov/assets/planning/json/content/resources/dataset-archives"
)
BYTES_API_PREFIX = (
    "https://apps.nyc.gov/content-api/v1/content/planning/resources/datasets"
)


class _FileUrlInfo(BaseModel, extra="forbid"):
    """File Url component parts"""

    # sample url: 'https://www1.nyc.gov/assets/planning/download/pdf/data-tools/bytes/meta_nhood.pdf'

    # e.g. https://www1.nyc.gov/assets/planning/download/
    url_base_override: str | None = None

    # e.g. pdf/data-maps/
    url_middle: str | None = None

    # e.g. meta_nhood.pdf
    filename: str

    def get_filename(self, *, version):
        return self.filename.format(v=version)


class _DatasetConfig(BaseModel, extra="forbid"):
    files: dict[str, _FileUrlInfo]

    # Some pages don't have archived versions
    no_archived_versions: bool | None = None

    # The closest thing to an actual dataset key
    # in certain cases (e.g. COLP) we get lucky and all resources
    # e.g. the JSON versions file, the page url etc, make use of this identifier
    bytes_dataset_key: str | None = None

    # For when the JSON catalog doesn't match the bytes_dataset_key
    catalog_file_override: str | None = None
    # For when the file_path doesn't match the bytes_dataset_key
    file_resource_override: str | None = None


_site_map_path = Path(__file__).parent / "site_map.json"
_SiteMap = TypeAdapter(dict[str, dict[str, _DatasetConfig]])

# TODO: everything below should just be a class, and the site_map read into
# the _connector on init


def get_site_map():
    with open(_site_map_path, "r") as site_map_file:
        return _SiteMap.validate_python(json.load(site_map_file))


SITE_MAP = get_site_map()


def reload_site_map():
    global SITE_MAP
    SITE_MAP = get_site_map()


def get_product_dataset_bytes_resource(product, dataset) -> str:
    """The Bytes convention for datasets seems to be that for each, there's a resource name like 'atomic-polygons' below:
            https://www.nyc.gov/content/planning/pages/resources/datasets/atomic-polygons
            https://apps.nyc.gov/content-api/v1/content/planning/resources/datasets/atomic-polygons
            https://www.nyc.gov/assets/planning/json/content/resources/dataset-archives/atomic-polygons.json
    For a dataset like lion.atomic_polygons, their dataset name matches up with ours (after twiddling the dash and underscore)
    but for a dataset like pluto, we're not so lucky so we need to do some overriding.
    """
    return SITE_MAP[product][dataset].bytes_dataset_key or dataset.replace("_", "-")


def get_most_recent_version_url(product, dataset) -> str:
    return f"{BYTES_API_PREFIX}/{get_product_dataset_bytes_resource(product, dataset)}"


def get_dataset_catalog_json_url(product, dataset) -> str | None:
    conf = SITE_MAP[product][dataset]
    if conf.no_archived_versions:
        return None

    json_file_start = conf.catalog_file_override or get_product_dataset_bytes_resource(
        product, dataset
    )
    return f"{BYTES_CATALOG_URL_PREFIX}/{json_file_start}.json"


def all_product_dataset_files() -> list[tuple[str, str, str]]:
    """Get a list of all (product, dataset, files)"""
    files = []
    for prod, ds in SITE_MAP.items():
        for ds, ds_conf in ds.items():  # type: ignore
            for f_id in ds_conf.files:
                files.append((prod, ds, f_id))
    return files  # type: ignore


def all_product_datasets() -> list[tuple[str, str]]:
    """Get a list of all (product, dataset)"""
    pds = []
    for prod, ds in SITE_MAP.items():
        for ds, ds_conf in ds.items():  # type: ignore
            pds.append((prod, ds))
    return pds  # type: ignore


def get_filename(product, dataset, file_id, *, version="") -> str:
    """Returns a function to resolve a (potentially templated) filename."""
    return SITE_MAP[product][dataset].files[file_id].get_filename(version=version)


def get_file_url(product, dataset, file_id, *, version="") -> str:
    ds_conf = SITE_MAP[product][dataset]

    file_res_name: str = (
        ds_conf.file_resource_override
        or get_product_dataset_bytes_resource(product, dataset)
    )

    assert file_id in ds_conf.files, (
        f"No file_id `{file_id}` is defined in the site_map for {product}.{dataset}"
    )
    f = ds_conf.files[file_id]
    base_url = f.url_base_override or S_MEDIA_FILES_URL_BASE

    filename = get_filename(product, dataset, file_id, version=version)
    suffix = filename.rsplit(".", 1)[-1]

    # Calculate the middle part of the url, e.g. zip/data-tools/bytes/lion-differences-file
    if suffix == "zip":
        url_mid_part = FILE_TYPE_SUFFIXES_TO_URL_PART[suffix] + (
            file_res_name + "/" if file_res_name else ""
        )
    else:
        url_mid_part = FILE_TYPE_SUFFIXES_TO_URL_PART[suffix]

    return f"{base_url}{url_mid_part}{filename}"


def all_urls(version) -> dict:
    return {
        pdf: get_file_url(*pdf, version=version) for pdf in all_product_dataset_files()
    }
