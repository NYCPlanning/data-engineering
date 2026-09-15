"""Scrape post office locations from USPS's location finder.

**This does not run unattended, in CI or anywhere else.** Two things prevent it, and
neither is a bug to be fixed here:

- The session headers can only come from a real browser (see `REFRESH_INSTRUCTIONS`),
  so every run needs a person.
- USPS blocks the client partway through regardless of pacing. Delays from 1s to 40s
  have all ended in a block, and no session has ever completed more than 8 of the 18
  queries. Waiting recovers roughly one query per 8-14 minutes of idle.

So a `Ingest Single Dataset` dispatch will fail partway through, and because its
checkpoint lives in the job's workspace, a re-dispatch starts over rather than resuming.

To collect the full dataset, run the ingest locally and repeat it. The checkpoint is
written after every zip into the staging directory, so each run resumes where the last
was blocked; a run whose session has expired simply fails on its first query, costing
nothing. Capturing a fresh session between attempts helps - replaying the browser's
complete header set doubled the per-session budget from 4 queries to 8 - and so does
waiting longer between them.

Collecting all 18 zips on 2026-08-18 took 5 runs and 2 captures over about 2.5 hours,
yielding 367 locations, 277 of them inside NYC.
"""

import json
import os
import shlex
import sys
import time
from pathlib import Path
from typing import Any

import requests

from dcpy.connectors.registry import Pull
from dcpy.utils.logging import logger

URL = "https://tools.usps.com/locations/getLocations"

# Do not assume slower is safer here - the evidence points the other way. 1s got 7
# queries through before a block; 3s completed a 5-query run; a 20-40s gap was blocked
# on the *second* query. That last run also changed two other things at once, so the
# cause is unproven - but until someone isolates it, 3s is the only pacing that has
# ever finished a run.
REQUEST_DELAY_SECONDS = 3
# Checkpoint after every zip. A blocked session can't be retried (the block sticks), so
# a run is worth resuming rather than repeating - and 18 small writes cost nothing.
CHECKPOINT_EVERY_N_ZIPS = 1

# Fallback only, for a capture that predates full-header replay - anything the capture
# provides overrides these. A fresh capture supplies its own User-Agent.
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

# USPS caps a single query at ~199 results, so the old maxDistance=10 silently
# truncated: every NYC query came back with exactly 199. It also blocks a client
# that makes too many requests too quickly - a 246-zip sweep got shut out after 7.
# So: query the fewest points that still cover the city. These 18 zip centroids
# cover every quarter-mile of NYC land area within 4 miles (greedy set cover over
# dcp_cscl_zipcode 26b); querying at 5 leaves a mile of slack for USPS geocoding a
# zip differently than CSCL. Each returns ~110 results, well under the cap.
MAX_DISTANCE_MILES = "5"
RESULT_CAP = 199

# Enough of an Akamai response to tell a challenge page from a block page.
BODY_SNIPPET_CHARS = 300

QUERY_ZIP_CODES = [
    "10002",
    "10304",
    "10306",
    "10309",
    "10314",
    "10455",
    "10460",
    "10466",
    "10475",
    "11004",
    "11096",
    "11109",
    "11230",
    "11357",
    "11385",
    "11423",
    "11693",
    "11697",
]


ONE_PASSWORD_ITEM = "Data Engineering -> USPS_LOCATIONS -> session_headers"

# Sessions last ~an hour, so a stored value is stale far more often than not, and the fix
# is a capture-and-run loop that has to happen in one sitting - spell it out rather than
# making whoever hits a failed run rediscover it.
REFRESH_INSTRUCTIONS = f"""\
Capture a fresh session, in one sitting:
  1. Open https://tools.usps.com/locations/ in a browser and search any ZIP code.
  2. Devtools -> Network -> right-click the `getLocations` request -> Copy as cURL.
  3. Convert it to JSON (reads the cURL from stdin, writes the headers to stdout):
       pbpaste | python -m dcpy.connectors.web_scrapers.usps | pbcopy
  4. Paste that into 1Password ({ONE_PASSWORD_ITEM}).
  5. Re-run the ingest right away - the session expires ~1 hour after capture."""

EXPIRED_SESSION = (
    "Nothing succeeded, so the session headers are probably expired or wrong (they last "
    "~1 hour)."
)

RATE_LIMITED = (
    "Earlier queries succeeded, so this is Akamai blocking the client rather than a bad "
    "session. The block sticks for a while and a fresh capture from the same network may "
    "be blocked too - wait before retrying, and raise REQUEST_DELAY_SECONDS if it "
    "recurs. Completed zips are checkpointed, so a rerun resumes rather than repeats."
)


SENSOR_HEADER_PREFIX = "x-jfuguzwb-"

# requests and urllib3 derive these per-request; replaying captured values would either
# be ignored or, for Content-Length, actively wrong once the body changes.
TRANSPORT_MANAGED_HEADERS = {"content-length", "host", "connection", "accept-encoding"}


def session_headers_from_curl(curl_command: str) -> dict[str, str]:
    """Extract the session headers from a devtools 'Copy as cURL' command.

    Keeps every header the browser sent, minus the ones the HTTP layer owns. An earlier
    version kept only Cookie and the sensors, on the evidence that a single request
    succeeds without the rest - but a single request was the whole test, and runs still
    get blocked after a handful of queries at any pacing. Replaying the browser's exact
    headers is the premise of session replay; a partial replay contradicts itself, most
    visibly by pairing Android sensors and cookies with a hardcoded macOS Firefox
    User-Agent and no sec-ch-ua headers at all.
    """
    headers: dict[str, str] = {}
    tokens = shlex.split(curl_command)
    for i, token in enumerate(tokens[:-1]):
        value = tokens[i + 1]
        if token in ("-H", "--header"):
            name, _, header_value = value.partition(":")
            name = name.strip()
            if name.lower() not in TRANSPORT_MANAGED_HEADERS:
                headers[name] = header_value.strip()
        elif token in ("-b", "--cookie"):
            # Chrome emits the cookie jar as -b rather than a Cookie header.
            headers["Cookie"] = value

    if not any(h.lower() == "cookie" for h in headers):
        raise ValueError(
            "No Cookie found in the cURL command. Copy the `getLocations` POST request, "
            "not another one."
        )
    sensors = [h for h in headers if h.lower().startswith(SENSOR_HEADER_PREFIX)]
    if not sensors:
        raise ValueError(
            f"No {SENSOR_HEADER_PREFIX}* sensor headers found - without them USPS serves "
            "an Akamai bot challenge instead of JSON. Copy the `getLocations` POST "
            "request, not another one."
        )
    return headers


def _merge_headers(
    defaults: dict[str, str], captured: dict[str, str]
) -> dict[str, str]:
    """Captured headers win, matched case-insensitively.

    A capture writes `user-agent` while STATIC_HEADERS writes `User-Agent`; a plain dict
    merge keeps both and leaves which one is sent to insertion order.
    """
    captured_names = {name.lower() for name in captured}
    kept = {k: v for k, v in defaults.items() if k.lower() not in captured_names}
    return {**kept, **captured}


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
    full loop, and `session_headers_from_curl` for turning a capture into that JSON.
    They expire after ~1 hour, so every run needs a freshly captured session.
    """

    conn_type: str = "usps_locations"
    filename: str = "usps_locations.json"
    checkpoint_filename: str = "usps_locations.checkpoint.json"
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

    def _parse_response(
        self, response: requests.Response, zip_code: str, succeeded_so_far: int = 0
    ) -> list[dict]:
        # Both failures look identical in the response, but the cause differs entirely
        # depending on whether anything worked first - and so does the fix.
        cause = RATE_LIMITED if succeeded_so_far else EXPIRED_SESSION
        if not response.ok:
            raise RuntimeError(
                f"USPS returned HTTP {response.status_code} for zip {zip_code} after "
                f"{succeeded_so_far} successful queries. {cause}\n{REFRESH_INSTRUCTIONS}"
            )
        try:
            payload = response.json()
        except requests.exceptions.JSONDecodeError as e:
            # Akamai serves both its bot challenge and its block page as HTML with a 200,
            # so a non-JSON body is a rejected client rather than a USPS outage.
            # Akamai's challenge and its outright block are different states with
            # different prospects for a retry, and the body is the only thing that
            # tells them apart - a run costs a browser capture, so don't discard it.
            raise RuntimeError(
                f"USPS returned a non-JSON response for zip {zip_code} after "
                f"{succeeded_so_far} successful queries. {cause}\n"
                f"First {BODY_SNIPPET_CHARS} chars of the body: "
                f"{response.text[:BODY_SNIPPET_CHARS]!r}\n{REFRESH_INSTRUCTIONS}"
            ) from e
        return _extract_locations(payload)

    def _read_checkpoint(self, path: Path) -> tuple[list[str], dict[str, dict]]:
        if not path.exists():
            return [], {}
        data = json.loads(path.read_text(encoding="utf-8"))
        locations = {loc["locationID"]: loc for loc in data["locations"]}
        logger.info(
            f"Resuming from {path}: {len(data['queried_zips'])} zips already queried, "
            f"{len(locations)} locations kept"
        )
        return data["queried_zips"], locations

    def _write_checkpoint(
        self, path: Path, queried_zips: list[str], locations_by_id: dict[str, dict]
    ) -> None:
        # Write-and-rename so a process killed mid-write leaves the previous checkpoint
        # intact rather than a truncated file that fails to parse on resume.
        temp_path = path.with_suffix(".tmp")
        temp_path.write_text(
            json.dumps(
                {
                    "queried_zips": queried_zips,
                    "locations": list(locations_by_id.values()),
                }
            ),
            encoding="utf-8",
        )
        temp_path.replace(path)

    def pull(
        self,
        key: str,
        destination_path: Path,
        **kwargs,
    ) -> dict:
        headers = _merge_headers(STATIC_HEADERS, self._session_headers())
        # A blocked session can't be salvaged mid-run, so the only thing that makes a
        # partial run worth anything is keeping what it got. Checkpointing is per
        # workspace: it helps local runs and reruns in the same CI job, not a fresh
        # dispatch.
        checkpoint_path = destination_path / self.checkpoint_filename
        queried_zips, locations_by_id = self._read_checkpoint(checkpoint_path)
        remaining = [z for z in QUERY_ZIP_CODES if z not in set(queried_zips)]
        saturated = []
        for i, zip_code in enumerate(remaining):
            if i > 0:
                time.sleep(REQUEST_DELAY_SECONDS)
            logger.info(
                f"Querying USPS locations for zip {zip_code} "
                f"({len(queried_zips) + 1}/{len(QUERY_ZIP_CODES)})"
            )
            body = {
                "requestZipCode": zip_code,
                "requestType": "PO",
                "maxDistance": MAX_DISTANCE_MILES,
                "requestServices": "",
                "requestHours": "",
            }
            response = requests.post(URL, headers=headers, json=body)
            locations = self._parse_response(response, zip_code, len(queried_zips))
            logger.info(f"Got {len(locations)} locations for zip {zip_code}")
            if len(locations) >= RESULT_CAP:
                saturated.append(zip_code)
            for location in locations:
                locations_by_id[location["locationID"]] = location
            queried_zips.append(zip_code)
            if len(queried_zips) % CHECKPOINT_EVERY_N_ZIPS == 0:
                self._write_checkpoint(checkpoint_path, queried_zips, locations_by_id)

        if saturated:
            # Silent truncation is how the original 5-zip version looked healthy while
            # missing most of the city - say so rather than archiving a short dataset.
            logger.warning(
                f"{len(saturated)} zip(s) returned {RESULT_CAP}+ results and were likely "
                f"truncated by USPS: {saturated}. Coverage is incomplete - lower "
                "MAX_DISTANCE_MILES."
            )

        all_locations = list(locations_by_id.values())
        filepath = destination_path / self.filename
        logger.info(f"Saving {len(all_locations)} unique USPS locations to {filepath}")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(all_locations, f, indent=2, ensure_ascii=False)
        checkpoint_path.unlink(missing_ok=True)
        return {"path": filepath}


if __name__ == "__main__":
    # Converting a capture by hand is fiddly enough to get wrong quietly, so ship it as
    # a filter: `pbpaste | python -m dcpy.connectors.web_scrapers.usps | pbcopy`.
    # Piped into pbcopy, stdout is invisible - so failures must go to stderr and exit
    # non-zero, or a bad run looks identical to a good one.
    curl_command = sys.stdin.read()
    if not curl_command.strip():
        sys.exit(
            "Nothing on stdin. Copy the `getLocations` request from devtools first "
            f"(Copy as cURL).\n{REFRESH_INSTRUCTIONS}"
        )
    try:
        print(json.dumps(session_headers_from_curl(curl_command)))
    except ValueError as e:
        sys.exit(str(e))
