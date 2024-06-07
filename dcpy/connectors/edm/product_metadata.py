import requests
import yaml

from dcpy.utils.logging import logger
import dcpy.models.product.dataset.metadata as md

METADATA_REPO_RAW_URL = (
    "https://raw.githubusercontent.com/NYCPlanning/product-metadata/main/products"
)


def _github_url(product: str, *, dataset: str | None = None) -> str:
    return f"{METADATA_REPO_RAW_URL}/{product}/{dataset or product}/metadata.yml"


def fetch_raw(product: str, *, dataset: str | None = None) -> str:
    url = _github_url(product, dataset=dataset)
    logger.info(f"Pulling metadata at {url}")
    return requests.get(url).text


def fetch(product: str, *, dataset: str | None = None) -> md.Metadata:
    return md.Metadata(**yaml.safe_load(fetch_raw(product, dataset=dataset)))
