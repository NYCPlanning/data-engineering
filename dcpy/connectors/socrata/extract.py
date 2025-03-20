from datetime import datetime
from pathlib import Path

# todo -> this should not depend on ingest models
from dcpy.models.lifecycle.ingest import SocrataSource as Source
from .utils import _socrata_request


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


def get_version(source: Source):
    """For given socrata dataset, get date last updated formatted as a string.
    This is used as a proxy for a 'version' of the dataset."""
    url = get_metadata_url(source)
    resp = _socrata_request(url, "GET")
    return datetime.fromtimestamp(resp.json()["rowsUpdatedAt"]).strftime("%Y%m%d")


def download(source: Source, path: Path):
    path.parent.mkdir(exist_ok=True, parents=True)
    response = _socrata_request(get_download_url(source), "GET")
    with open(path, "wb") as f:
        f.write(response.content)
