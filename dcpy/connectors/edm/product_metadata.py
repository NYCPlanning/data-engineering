from pathlib import Path

import requests
import yaml

import dcpy.models.product.dataset.metadata as md
from dcpy.utils.logging import logger

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


def download(product: str, output_path: Path, *, dataset: str | None = None) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write(fetch_raw(product, dataset=dataset))
    return output_path
