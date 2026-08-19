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


ONE_PASSWORD_ITEM = "Data Engineering -> USPS_LOCATIONS -> session_headers"

# Sessions last ~an hour, so a stored value is stale far more often than not, and the fix
# is a capture-and-run loop that has to happen in one sitting - spell it out rather than
# making whoever hits a failed run rediscover it.
REFRESH_INSTRUCTIONS = f"""\
Capture a fresh session, in one sitting:
  1. Open https://tools.usps.com/locations/ in a browser and search any ZIP code.
  2. Devtools -> Network -> right-click the `getLocations` request -> Copy as cURL.
  3. Pull out the `Cookie` and `X-jFuguZWB-*` headers as a JSON object of
     {{"header_name": "value"}}.
  4. Paste that into 1Password ({ONE_PASSWORD_ITEM}).
  5. Re-run the ingest right away - the session expires ~1 hour after capture."""

EXPIRED_SESSION = (
    "This almost always means the stored session headers expired (they last ~1 hour)."
)


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
    statically. Capture them from a real browser and set them as a JSON object of
    {header_name: value} in the env var below - see REFRESH_INSTRUCTIONS above for the
    full loop. They expire after ~1 hour, so every run needs a freshly captured session.
    """

    conn_type: str = "usps_locations"
    filename: str = "usps_locations.json"
    session_headers_env_var: str = "USPS_LOCATIONS_SESSION_HEADERS"

    def _session_headers(self) -> dict:
        raw = os.environ.get(self.session_headers_env_var)
        if not raw:
            raise EnvironmentError(
                f"{self.session_headers_env_var} is unset. In CI it comes from 1Password "
                f"({ONE_PASSWORD_ITEM}); locally, set it yourself.\n{REFRESH_INSTRUCTIONS}"
            )
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"{self.session_headers_env_var} is not valid JSON ({e}). Expected an "
                f"object of {{'header_name': 'value'}}.\n{REFRESH_INSTRUCTIONS}"
            ) from e

    def _parse_response(self, response: requests.Response, zip_code: str) -> list[dict]:
        if not response.ok:
            raise RuntimeError(
                f"USPS returned HTTP {response.status_code} for zip {zip_code}. "
                f"{EXPIRED_SESSION}\n{REFRESH_INSTRUCTIONS}"
            )
        try:
            payload = response.json()
        except requests.exceptions.JSONDecodeError as e:
            # Akamai serves its bot challenge as a 200 HTML page, so a non-JSON body is
            # a rejected session rather than a USPS outage.
            raise RuntimeError(
                f"USPS returned a non-JSON response for zip {zip_code}, which usually "
                f"means an Akamai bot challenge.\n{REFRESH_INSTRUCTIONS}"
            ) from e
        return _extract_locations(payload)

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
            locations = self._parse_response(response, zip_code)
            logger.info(f"Got {len(locations)} locations for zip {zip_code}")
            for location in locations:
                locations_by_id[location["locationID"]] = location

        all_locations = list(locations_by_id.values())
        filepath = destination_path / self.filename
        logger.info(f"Saving {len(all_locations)} unique USPS locations to {filepath}")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(all_locations, f, indent=2, ensure_ascii=False)
        return {"path": filepath}
