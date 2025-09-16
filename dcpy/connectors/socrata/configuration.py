from __future__ import annotations
from enum import StrEnum
from typing import Literal


ValidFormat = Literal["csv", "geojson", "shapefile"]


class Org(StrEnum):
    nyc = "nyc"
    nys = "nys"
    nys_health = "nys_health"


def server_url(org: Org | str) -> str:
    mapping = {
        "nyc": "data.cityofnewyork.us",
        "nys": "data.ny.gov",
        "nys_health": "health.data.ny.gov",
    }
    if org in mapping:
        return mapping[org]
    else:
        raise Exception(f"Unknown Socrata org {org}")
