from datetime import datetime
from enum import StrEnum
from pathlib import Path
import requests
from typing import Literal

from dcpy.models.lifecycle.ingest import Source as IngestSource
from dcpy.connectors import web


class Org(StrEnum):
    nyc = "nyc"
    nys = "nys"
    nys_health = "nys_health"

    @property
    def server(self) -> str:
        mapping = {
            "nyc": "data.cityofnewyork.us",
            "nys": "data.ny.gov",
            "nys_health": "health.data.ny.gov",
        }
        if self in mapping:
            return mapping[self]
        else:
            raise Exception(f"Unknown Socrata org {self}")


class Source(IngestSource):
    # This type should eventually be aligned more with logic in connectors.socrata
    # and live outside of recipes
    type: Literal["socrata"]
    org: Org
    uid: str
    format: Literal["csv", "geojson", "shapefile"]

    @property
    def extension(self) -> str:
        if self.format == "shapefile":
            return "zip"
        else:
            return self.format

    @property
    def base_url(self) -> str:
        return f"https://{self.org.server}/api"

    @property
    def download_url(self) -> str:
        """For a given dataset (org and four-four), get the url to download
        it in a specific format."""
        match self.format:
            case "csv":
                url = f"{self.base_url}/views/{self.uid}/rows.csv"
            case "geojson":
                url = f"{self.base_url}/geospatial/{self.uid}?method=export&format=GeoJSON"
            case "shapefile":
                url = f"{self.base_url}/geospatial/{self.uid}?method=export&format=Shapefile"
            case format:
                raise Exception(f"Unsupported socrata format: '{format}'")
        return url

    @property
    def metadata_url(self):
        return f"{self.base_url}/views/{self.uid}.json"

    def version(self, timestamp):
        """For given socrata dataset, get date last updated formatted as a string.
        This is used as a proxy for a 'version' of the dataset."""
        resp = requests.get(self.metadata_url)
        resp.raise_for_status()
        field = resp.json()["rowsUpdatedAt"]
        return datetime.fromtimestamp(field).strftime("%Y%m%d")

    def download(self, path: Path):
        web.download_file(self.download_url, path)
