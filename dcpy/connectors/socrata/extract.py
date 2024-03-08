from datetime import datetime
import requests

## These types will eventually not be in recipes
## Socrata connector should not depend on recipes connector
from dcpy.connectors.edm.recipes import ExtractConfig


def get_base_url(source: ExtractConfig.Source.Socrata):
    return f"https://{source.org.server}/api"


def get_metadata_url(source: ExtractConfig.Source.Socrata):
    base_url = get_base_url(source)
    return f"{base_url}/views/{source.uid}.json"


def get_download_url(source: ExtractConfig.Source.Socrata):
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


def get_version(source: ExtractConfig.Source.Socrata):
    """For given socrata dataset, get date last updated formatted as a string.
    This is used as a proxy for a 'version' of the dataset."""
    url = get_metadata_url(source)
    print(url)
    resp = requests.get(url)
    resp.raise_for_status()
    return _get_version_from_resp(resp.json())
