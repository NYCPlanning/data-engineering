"""Checkbook NYC API (https://www.checkbooknyc.com/api).

Requests and responses are XML, and a single call returns at most 20,000 records. A pull is
split into monthly ranges of a date field so that a month rarely needs more than one call.
"""

import calendar
import os
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests

from dcpy.connectors.registry import Pull
from dcpy.utils.logging import logger

API_URL = "https://www.checkbooknyc.com/api"
MAX_RECORDS = 20_000
DATE_FORMAT = "%Y-%m-%d"

# Checkbook sits behind Incapsula, which can block requests from IPs it doesn't recognize.
# If it does, copy the Cookie and User-Agent of a browser request to the API into these.
COOKIE_ENV_VAR = "NYCOC_CHECKBOOK_VISID_COOKIE_TOKEN"
USER_AGENT_ENV_VAR = "NYCOC_CHECKBOOK_USER_AGENT"


# search criteria are {"name", "type": "value", "value"} or {"name", "type": "range", "start", "end"}
Criteria = dict[str, str]


def monthly_ranges(start: date, end: date) -> list[tuple[date, date]]:
    """Split [start, end] into calendar-month ranges, trimmed to start and end."""
    ranges = []
    month_start = start
    while month_start <= end:
        last_day = calendar.monthrange(month_start.year, month_start.month)[1]
        month_end = min(month_start.replace(day=last_day), end)
        ranges.append((month_start, month_end))
        month_start = date.fromordinal(month_end.toordinal() + 1)
    return ranges


def request_xml(
    type_of_data: str, criteria: list[Criteria], records_from: int, max_records: int
) -> str:
    request = ET.Element("request")
    ET.SubElement(request, "type_of_data").text = type_of_data
    ET.SubElement(request, "records_from").text = str(records_from)
    ET.SubElement(request, "max_records").text = str(max_records)
    search_criteria = ET.SubElement(request, "search_criteria")
    for criterion in criteria:
        element = ET.SubElement(search_criteria, "criteria")
        for field, value in criterion.items():
            ET.SubElement(element, field).text = value
    return ET.tostring(request, encoding="unicode")


def parse_response(xml_text: str) -> tuple[int, list[dict]]:
    """Return the total record count for the request and this page's records."""
    root = ET.fromstring(xml_text)
    if root.findtext("./status/result") != "success":
        messages = [m.text.strip() for m in root.iter("description") if m.text]
        raise RuntimeError(f"Checkbook NYC request failed: {messages}")
    total = int(root.findtext("./result_records/record_count") or 0)
    records = [
        {field.tag: field.text for field in transaction}
        # the wrapper's name doesn't always follow type_of_data: Contracts records come in
        # contract_transactions
        for transaction in root.findall("./result_records/*/transaction")
    ]
    return total, records


def _send(body: str) -> str:
    headers = {"Content-Type": "application/xml"}
    if cookie := os.environ.get(COOKIE_ENV_VAR):
        headers["Cookie"] = cookie
    if user_agent := os.environ.get(USER_AGENT_ENV_VAR):
        headers["User-Agent"] = user_agent
    response = requests.post(API_URL, data=body, headers=headers, timeout=300)
    response.raise_for_status()
    return response.text


def _post(body: str, retries: int, pause_seconds: int) -> str:
    for attempt in range(1, retries):
        try:
            return _send(body)
        except requests.RequestException as e:
            logger.warning(f"Checkbook NYC request failed ({attempt}/{retries}): {e}")
            time.sleep(pause_seconds)
    return _send(body)


def fetch(
    type_of_data: str,
    criteria: list[Criteria],
    *,
    retries: int = 3,
    pause_seconds: int = 10,
) -> list[dict]:
    """Fetch every record matching the criteria, a page of MAX_RECORDS at a time."""
    records: list[dict] = []
    while True:
        body = request_xml(type_of_data, criteria, len(records) + 1, MAX_RECORDS)
        total, page = parse_response(_post(body, retries, pause_seconds))
        records += page
        if not page or len(records) >= total:
            return records
        time.sleep(pause_seconds)


class CheckbookNYCConnector(Pull):
    conn_type: str = "checkbook_nyc"

    def get_latest_version(self, key: str, **kwargs) -> str:
        # the API has no versions, so a pull is versioned by the day it runs
        return date.today().strftime("%Y%m%d")

    def pull(self, key: str, destination_path: Path, **kwargs) -> dict:
        return self._pull(key, destination_path, **kwargs)

    def _pull(
        self,
        key: str,
        destination_path: Path,
        *,
        date_field: str,
        start_date: str,
        search_criteria: list[Criteria] | None = None,
        version: str | None = None,
        pause_seconds: int = 10,
        **kwargs,
    ) -> dict:
        """Pull `key` (the API's type_of_data, like Contracts or Spending) month by month
        of `date_field`, from `start_date` through the version date."""
        start = datetime.strptime(start_date, DATE_FORMAT).date()
        end = datetime.strptime(version, "%Y%m%d").date() if version else date.today()
        records: list[dict] = []
        for month_start, month_end in monthly_ranges(start, end):
            month_criteria: Criteria = {
                "name": date_field,
                "type": "range",
                "start": month_start.strftime(DATE_FORMAT),
                "end": month_end.strftime(DATE_FORMAT),
            }
            page = fetch(
                key,
                [*(search_criteria or []), month_criteria],
                pause_seconds=pause_seconds,
            )
            logger.info(f"{len(page)} {key} records for {month_start:%Y-%m}")
            records += page
            time.sleep(pause_seconds)

        path = destination_path / f"{key.lower()}.csv"
        pd.DataFrame(records).to_csv(path, index=False)
        return {"path": path}
