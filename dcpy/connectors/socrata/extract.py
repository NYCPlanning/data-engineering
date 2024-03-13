from datetime import datetime
import requests

from dcpy.models.connectors.socrata import Source


def get_base_url(source: Source):
    return f"https://{source.org.server}/api"


def get_metadata_url(source: Source):
    base_url = get_base_url(source)
    return f"{base_url}/views/{source.uid}.json"


def get_download_url(source: Source):
    """For a given dataset (org and four-four), get the url to download
    it in a specific format."""
    base_url = get_base_url(source)
    match source.format:
        case "csv":
            url = f"{base_url}/views/{source.uid}/rows.csv"
        case "geojson":
            url = f"{base_url}/geospatial/{source.uid}?method=export&format=GeoJSON"
        case "shapefile":
            url = f"{base_url}/geospatial/{source.uid}?method=export&format=Shapefile"
        case format:
            raise Exception(f"Unsupported socrata format: '{format}'")
    return url


def _get_version_from_resp(resp: dict) -> str:
    return datetime.fromtimestamp(resp["rowsUpdatedAt"]).strftime("%Y%m%d")


def get_version(source: Source):
    """For given socrata dataset, get date last updated formatted as a string.
    This is used as a proxy for a 'version' of the dataset."""
    url = get_metadata_url(source)
    print(url)
    resp = requests.get(url)
    resp.raise_for_status()
    return _get_version_from_resp(resp.json())
