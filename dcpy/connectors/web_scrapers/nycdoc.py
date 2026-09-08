import re
from pathlib import Path

import pandas as pd
import requests
import usaddress
from bs4 import BeautifulSoup

from dcpy.connectors.registry import Pull
from dcpy.utils.logging import logger

SOURCE_URL = "https://www.nyc.gov/site/doc/about/facilities-locations.page"

# The page mixes prose into the same <p> structure as the facility entries (bus
# directions, for one), so entries are matched by name rather than by position.
# Names must match the page exactly; FACILITIES_MISSING_FROM_PAGE below is the
# guard against this list silently going stale.
FACILITIES = [
    "Anna M. Kross Center (AMKC)",
    "Eric M. Taylor Center (EMTC) Formerly known as CIFM",
    "George R. Vierno Center (GRVC)",
    "North Infirmary Command (NIC)",
    "Otis Bantum Correctional Center (OBCC)",
    "Robert N. Davoren Complex (RNDC)",
    "Rose M. Singer Center (RMSC)",
    "West Facility (WF)",
    "Bellevue Hospital Prison Ward (BHPW)",
    "Elmhurst Hospital Prison Ward (EHPW)",
    "Correction Academy",
    "Bulova Building- DOC Headquarters",
]


def _address_part(address: str, tag_pattern: str) -> str:
    return " ".join(
        part for part, tag in usaddress.parse(address) if re.search(tag_pattern, tag)
    )


def _scrape() -> pd.DataFrame:
    response = requests.get(SOURCE_URL, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    rows = []
    for paragraph in soup.find_all("p"):
        fields = paragraph.get_text("|").split("|")
        if fields[0] not in FACILITIES:
            continue
        rows.append(
            {
                "name": fields[0],
                "address1": fields[1],
                "address2": fields[2],
                "house_number": _address_part(fields[1], "Address"),
                "street_name": _address_part(fields[1], "Street"),
                "zipcode": _address_part(fields[2], "ZipCode"),
            }
        )

    found = {row["name"] for row in rows}
    missing = [name for name in FACILITIES if name not in found]
    if missing:
        # A facility that closes or gets renamed just stops matching. Without this
        # the dataset quietly shrinks and facdb loses records with no failure.
        raise ValueError(
            f"{len(missing)} of {len(FACILITIES)} facilities are no longer on "
            f"{SOURCE_URL}: {missing}. Confirm each one and update FACILITIES."
        )
    return pd.DataFrame.from_records(rows)


class NYCDOCConnector(Pull):
    conn_type: str = "nycdoc"
    filename: str = "nycdoc_corrections.csv"

    def pull(self, key: str, destination_path: Path, **kwargs) -> dict:
        df = _scrape()
        filepath = destination_path / self.filename
        logger.info(f"Saving NYC DOC facility location data to {filepath}")
        df.to_csv(filepath, index=False)
        return {"path": filepath}
