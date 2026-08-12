import json
import os
import time
from pathlib import Path
from typing import Any

import requests

from dcpy.connectors.registry import Pull
from dcpy.utils.logging import logger

URL = "https://tools.usps.com/locations/getLocations"
REQUEST_DELAY_SECONDS = 3

STATIC_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/json;charset=utf-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://tools.usps.com",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:153.0) "
        "Gecko/20100101 Firefox/153.0"
    ),
}

# TODO expand to cover the rest of NYC - one query only returns locations within
# `maxDistance` of the given zip, so we'll need zips spread across the boroughs.
QUERY_ZIP_CODES = [
    "10012",  # Lower Manhattan
    "10027",  # Upper Manhattan / Harlem
    "10455",  # South Bronx
    "11377",  # Queens
    "11210",  # Brooklyn
]


def _extract_locations(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("locations", "Locations", "results"):
            if key in payload:
                return payload[key]
    raise ValueError(
        "Unrecognized USPS locations response shape: "
        f"{list(payload) if isinstance(payload, dict) else type(payload)}"
    )


class USPSLocationsConnector(Pull):
    """tools.usps.com/locations sits behind Akamai Bot Manager: requests need a Cookie
    header plus several `X-jFuguZWB-*` sensor headers that Akamai's JS generates per
    browser session and validates server-side - there's no way to derive them
    statically. Capture them from a real browser (devtools -> Copy as cURL on the
    getLocations request) and set them as a JSON object of {header_name: value} in the
    env var below. They expire with the browser session, so this needs refreshing
    regularly.
    """

    conn_type: str = "usps_locations"
    filename: str = "usps_locations.json"
    session_headers_env_var: str = "USPS_LOCATIONS_SESSION_HEADERS"

    def _session_headers(self) -> dict:
        return json.loads(os.environ[self.session_headers_env_var])

    def pull(
        self,
        key: str,
        destination_path: Path,
        **kwargs,
    ) -> dict:
        headers = {**STATIC_HEADERS, **self._session_headers()}
        locations_by_id = {}
        for i, zip_code in enumerate(QUERY_ZIP_CODES):
            if i > 0:
                time.sleep(REQUEST_DELAY_SECONDS)
            logger.info(f"Querying USPS locations for zip {zip_code}")
            body = {
                "requestZipCode": zip_code,
                "requestType": "PO",
                "maxDistance": "10",
                "requestServices": "",
                "requestHours": "",
            }
            response = requests.post(URL, headers=headers, json=body)
            response.raise_for_status()
            locations = _extract_locations(response.json())
            logger.info(f"Got {len(locations)} locations for zip {zip_code}")
            for location in locations:
                locations_by_id[location["locationID"]] = location

        all_locations = list(locations_by_id.values())
        filepath = destination_path / self.filename
        logger.info(f"Saving {len(all_locations)} unique USPS locations to {filepath}")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(all_locations, f, indent=2, ensure_ascii=False)
        return {"path": filepath}
