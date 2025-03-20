from __future__ import annotations
from enum import StrEnum
from typing import Literal


ValidSourceFormats = Literal["csv", "geojson", "shapefile"]


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
