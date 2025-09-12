from datetime import datetime
from pathlib import Path

from dcpy.connectors.socrata.configuration import Org, ValidFormat, server_url
from .utils import _socrata_request


def _get_base_url(org: Org) -> str:
    return f"https://{server_url(org)}/api"


def get_metadata_url(org: Org, uid: str) -> str:
    base_url = _get_base_url(org)
    return f"{base_url}/views/{uid}.json"


def get_download_url(org: Org, uid: str, format: ValidFormat) -> str:
    """For a given dataset (org and four-four), get the url to download
    it in a specific format."""
    base_url = _get_base_url(org)
    match format:
        case "csv":
            url = f"{base_url}/views/{uid}/rows.csv"
        case "geojson":
            url = f"{base_url}/geospatial/{uid}?method=export&format=GeoJSON"
        case "shapefile":
            url = f"{base_url}/geospatial/{uid}?method=export&format=Shapefile"
        case format:
            raise Exception(f"Unsupported socrata format: '{format}'")
    return url


def get_version(org: Org, uid: str) -> str:
    """For given socrata dataset, get date last updated formatted as a string.
    This is used as a proxy for a 'version' of the dataset."""
    url = get_metadata_url(org, uid)
    resp = _socrata_request(url, "GET")
    return datetime.fromtimestamp(resp.json()["rowsUpdatedAt"]).strftime("%Y%m%d")


def download(org: Org, uid: str, format: ValidFormat, path: Path) -> None:
    path.parent.mkdir(exist_ok=True, parents=True)
    response = _socrata_request(get_download_url(org, uid, format), "GET")
    with open(path, "wb") as f:
        f.write(response.content)
