from __future__ import annotations
from enum import Enum
from pydantic import BaseModel, field_serializer
from typing import Literal


ValidSourceFormats = Literal["csv", "geojson", "shapefile"]


class Org(str, Enum):
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


class Source(BaseModel, extra="forbid"):
    # This type should eventually be aligned more with logic in connectors.socrata
    # and live outside of recipes
    type: Literal["socrata"]
    org: Org
    uid: str
    format: ValidSourceFormats

    @property
    def extension(self) -> str:
        if self.format == "shapefile":
            return "zip"
        else:
            return self.format

    @field_serializer("org")
    def _serialize_org(self, org: Org, _info) -> str:
        return org.value
