import calendar
import datetime as dt
import os
import random
import shutil
import sys
import time
import xml.etree.ElementTree as ET
from io import StringIO
from pathlib import Path
from typing import Any, Literal, TypedDict

import pandas as pd
import pytz
import requests

from dcpy.utils.logging import logger

from . import df_to_tempfile
from .scriptor import ScriptorInterface

api_endpoint = "https://www.checkbooknyc.com/api"

TYPE_OF_DATA = Literal[
    "Contracts",
    "Budget",
    "Revenue",
    "Payroll",
    "Spending",
    "Spending_OGE",
    "Contracts_OGE",
    "Spending_NYCHA",
    "Contracts_NYCHA",
    "Payroll_NYCHA",
]

DEFAULT_RECORDS_FROM = "1"
DEFAULT_MAX_RECORDS = "20000"

DEFAULT_DATE_FORMAT = "%Y-%m-%d"
DEFAULT_START_PERIOD = dt.date(2010, 1, 1).strftime(DEFAULT_DATE_FORMAT)

DEFAULT_DATA_PATH = Path(__file__).resolve().parent / "capital_spending_data"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
]

# NOTE for when you're running locally:
# Checkbook appears to use incapsula to prevent automated scraping from non-whitelisted IPs
# So you may need to set the env vars below.
#
# To obtain, visit the api url in your browser, open the network panel, verify that you're not a robot,
# then inspect the get request for the `Cookie` value and User-Agent.
# It's unclear if the cookie and User-Agent are linked.
VISID_COOKIE_TOKEN_ENV_VAR = "NYCOC_CHECKBOOK_VISID_COOKIE_TOKEN"
MAYBE_VISID_TOKEN: str | None = os.environ.get(VISID_COOKIE_TOKEN_ENV_VAR)

if not MAYBE_VISID_TOKEN:
    logger.warn(
        "Running nycoc ingestion without an incapsula token. This could cause problems if your IP is not whitelisted"
    )
NYCOC_USER_AGENT_ENV_VAR = "NYCOC_CHECKBOOK_USER_AGENT"
MAYBE_NYCOC_USER_AGENT: str | None = os.environ.get(NYCOC_USER_AGENT_ENV_VAR)


class CriteriaValue(TypedDict):
    name: str
    type: Literal["value"]
    value: str


class CriteriaRange(TypedDict):
    name: str
    type: Literal["range"]
    start: str
    end: str


class Scriptor(ScriptorInterface):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def ingest(
        self,
        type_of_data: TYPE_OF_DATA = "Spending",
        records_from: str = DEFAULT_RECORDS_FROM,
        max_records: str = DEFAULT_MAX_RECORDS,
        response_columns: list[str] | None = None,
        search_criteria: list[CriteriaValue | CriteriaRange] | None = None,
        num_retries: int = 3,
        data_dir: Path = DEFAULT_DATA_PATH,
        start_period: str = DEFAULT_START_PERIOD,
        end_period: str | None = None,
    ) -> pd.DataFrame:
        """Fetches data from Checkbook NYC API based on input parameters, saves it
        locally in parquet files organized by months, and returns data in a pandas DataFrame.
        """
        create_data_dir(data_dir)
        if not end_period:
            end_date = dt.datetime.strptime(self.version, "%Y%m%d")
            end_period = end_date.strftime(DEFAULT_DATE_FORMAT)

        search_criteria = validate_search_criteria(search_criteria)

        month_range = generate_monthly_ranges(start_period, end_period)

        for month in month_range:
            start, end = month
            month_criteria: CriteriaRange = {
                "name": "issue_date",
                "type": "range",
                "start": start,
                "end": end,
            }
            search_criteria_modified = search_criteria + [month_criteria]
            df = get_data(
                type_of_data=type_of_data,
                records_from=records_from,
                max_records=max_records,
                response_columns=response_columns,
                search_criteria=search_criteria_modified,
                num_retries=num_retries,
            )
            print(f"There are {len(df)} records for period {start}-{end}.")

            filename = f"{start}-{end}_checkbook.parquet"
            filepath = str(data_dir / filename)
            df.to_parquet(filepath)

            # pausing before next request: we don't want to overload API
            time.sleep(random.randint(10, 15))

        try:
            data = read_parquet_files_to_df(data_dir)
            print("Successfully ingested data.")
        except ValueError as e:
            print(
                f"No data to ingest for period {start_period}-{end_period}. Error: {e}"
            )
            sys.exit(1)

        return data

    def runner(self) -> str:
        df = self.ingest(
            search_criteria=[
                {"name": "spending_category", "type": "value", "value": "cc"}
            ],
        )
        local_path = df_to_tempfile(df)
        return local_path


def get_data(
    type_of_data: TYPE_OF_DATA,
    records_from: str,
    max_records: str,
    response_columns: list[str] | None,
    search_criteria: list[CriteriaValue | CriteriaRange],
    num_retries: int,
) -> pd.DataFrame:
    """
    Gets data from API and returns it in pandas dataframe. Makes multiple API calls
    if total number of records is greater than max_records.
    """
    all_records = False
    record_count = 0
    df_list = []
    while not all_records:
        response = get_response(
            type_of_data=type_of_data,
            records_from=records_from,
            max_records=max_records,
            response_columns=response_columns,
            search_criteria=search_criteria,
            num_retries=num_retries,
        )
        xml_response = response.text

        # get total record count from response string
        root = ET.fromstring(xml_response)
        records_tag = root.find("./result_records/record_count")
        if records_tag is None or records_tag.text is None:
            raise Exception("Unable to extract total record count")
        total_record_count = int(records_tag.text)
        df = pd.read_xml(
            StringIO(xml_response),
            xpath=f"./result_records/{type_of_data.lower()}_transactions/*",
        )
        df_list.append(df)
        record_count += len(df)

        # if all expected records are received, update all_records. Otherwise, continue making calls
        if (record_count + int(records_from) - 1) == total_record_count:
            all_records = True
        else:
            records_from = str(int(records_from) + record_count)
            time.sleep(random.randint(10, 15))

    result_df = pd.concat(df_list)

    return result_df


def get_response(
    type_of_data: TYPE_OF_DATA,
    records_from: str,
    max_records: str,
    response_columns: list[str] | None,
    search_criteria: list[CriteriaValue | CriteriaRange],
    num_retries: int,
) -> requests.Response:
    """
    Makes a request call to api and returns Response object.
    """
    request_dict = create_request_dict(
        type_of_data, records_from, max_records, response_columns, search_criteria
    )
    xml = dict_to_xml_obj(request_dict)
    xml_string = xml_obj_to_string(xml)
    headers = {"Content-Type": "application/xml"}

    if MAYBE_VISID_TOKEN:
        headers["Cookie"] = MAYBE_VISID_TOKEN
    if MAYBE_NYCOC_USER_AGENT:
        headers["User-Agent"] = MAYBE_NYCOC_USER_AGENT

    for n in range(num_retries):
        try:
            response = requests.post(url=api_endpoint, data=xml_string, headers=headers)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            # user_agent = random.choice(USER_AGENTS)   # TODO: uncomment or remove completely
            timestamp = (
                dt.datetime.utcnow()
                .astimezone(pytz.timezone("US/Eastern"))
                .strftime("%Y-%m-%d %H:%M")
            )
            print(
                f"Failed to get a response from api (try # {n + 1}/{num_retries}) at {timestamp}."
                f"\nRequest body: \n{xml_string}"
                f"\nException: {e}"
            )
            # pausing before next request: we don't want to overload API
            time.sleep(random.randint(15, 20))
    raise Exception(f"Exceeded maximum tries to retrieve data from {records_from}")


def create_request_dict(
    type_of_data: TYPE_OF_DATA,
    records_from: str,
    max_records: str,
    response_columns: list[str] | None,
    search_criteria: list[CriteriaValue | CriteriaRange],
) -> dict:
    """
    Creates a request body in dictionary format.

    Sample output dict: {
        'request': {
            'type_of_data':'Spending',
            'records_from': '1',
            'response_columns': [
                {'column': 'agency'},
                {'column': 'expense_category'}
            ]
        }
    }
    """
    request_dict: dict[str, Any] = {
        "request": {
            "type_of_data": type_of_data,
            "records_from": records_from,
            "max_records": max_records,
        }
    }
    if response_columns:
        request_dict["request"]["response_columns"] = [
            {"column": col} for col in response_columns
        ]
    if search_criteria:
        request_dict["request"]["search_criteria"] = [
            {"criteria": criteria_item} for criteria_item in search_criteria
        ]

    return request_dict


def dict_to_xml_obj(request_dict: dict) -> ET.Element:
    """
    Converts a dictionary into an xml.etree.ElementTree with a tree-like structure.
    Input dict is expected to have a single root key.

    Returns:
        xml.etree.ElementTree.Element: The root element of the generated XML tree.

    Example:
    ```python
    >>> sample_dict = {
        'request': {
            'type_of_data': 'Spending',
            'records_from': '1',
            'response_columns': [
                {'column': 'agency'},
                {'column': 'expense_category'}
            ],
            'search_criteria': [
                {'criteria': {
                    'name': 'issue_date',
                    'type': 'range',
                    'start': '2023-01-01',
                    'end': '2024-01-01'
                    }
                }
            ]
        }
    }
    >>> xml_tree = dict_to_xml(Sample_dict)
    >>> xml_string = ET.tostring(xml_tree, encoding='unicode', method='xml')
    >>> print(xml_string)

    <request>
        <type_of_data>Spending</type_of_data>
        <records_from>1</records_from>
        <response_columns>
            <column>agency</column>
        </response_columns>
        <response_columns>
            <column>expense_category</column>
        </response_columns>
        <search_criteria>
            <criteria>
                <name>issue_date</name>
                <type>range</type>
                <start>2023-01-01</start>
                <end>2024-01-01</end>
            </criteria>
        </search_criteria>
    </request>
    ```python
    """
    assert len(request_dict) == 1, "Request_dict must have a root."
    parent_key = next(iter(request_dict))
    parent_val = request_dict[parent_key]
    parent_node = ET.Element(parent_key)

    if isinstance(parent_val, str):
        parent_node.text = parent_val
        return parent_node
    elif isinstance(parent_val, list):
        for list_item in parent_val:
            if len(list_item) != 0:
                parent_node.append(dict_to_xml_obj(list_item))
        return parent_node
    else:
        for child_key, child_val in parent_val.items():
            if len(child_val) != 0:
                parent_node.append(dict_to_xml_obj({child_key: child_val}))
        return parent_node


def xml_obj_to_string(xml_obj: ET.Element) -> str:
    """Returns a XML string representation of python xml object."""
    return ET.tostring(xml_obj, encoding="utf-8").decode("utf-8")


def generate_monthly_ranges(
    start_period: str, end_period: str, date_format: str = DEFAULT_DATE_FORMAT
) -> list[tuple[str, str]]:
    """
    Generate a list of tuples representing monthly date ranges within the specified time period.

    Example:
    ```python
    >>> result = generate_monthly_ranges("2023-07-08", "2023-08-27")
    >>> print(result)
    [("2023-07-08", "2023-07-30"), ("2023-08-01", "2023-08-27")]
    ```python
    """

    # TODO: AR: fix bug that generated the following: <start>2024-11-01</start><end>2024-10-31</end>.
    # Skipping for now, to get CPDB out.
    start_period_dt = dt.datetime.strptime(start_period, date_format)
    end_period_dt = dt.datetime.strptime(end_period, date_format)

    # if start date is later than end date, flip them
    if start_period_dt > end_period_dt:
        start_period_dt, end_period_dt = end_period_dt, start_period_dt

    start_month_date = start_period_dt
    _, num_days = calendar.monthrange(start_month_date.year, start_month_date.month)
    end_month_date = start_month_date.replace(day=num_days)

    monthly_ranges = []

    while end_month_date <= end_period_dt:
        monthly_ranges.append(
            (
                start_month_date.strftime(date_format),
                end_month_date.strftime(date_format),
            )
        )
        start_month_date = end_month_date + dt.timedelta(days=1)
        _, num_days = calendar.monthrange(start_month_date.year, start_month_date.month)
        end_month_date = start_month_date.replace(day=num_days)

    if end_month_date > end_period_dt:
        monthly_ranges.append(
            (
                start_month_date.strftime(date_format),
                end_period_dt.strftime(date_format),
            )
        )

    return monthly_ranges


def create_data_dir(data_path: Path = DEFAULT_DATA_PATH) -> None:
    """Creates a local data directory."""
    if data_path.exists():
        shutil.rmtree(data_path)
    try:
        data_path.mkdir(parents=False, exist_ok=False)
    except Exception as err:
        print("❌ Unable to create data directory.")
        raise err


def read_parquet_files_to_df(data_dir: Path) -> pd.DataFrame:
    """
    Reads Parquet files from a given data directory and concatenates
    them into a single pandas DataFrame.
    """
    df = pd.concat(
        pd.read_parquet(parquet_file) for parquet_file in data_dir.glob("*.parquet")
    )

    return df


def validate_search_criteria(
    search_criteria: list[CriteriaValue | CriteriaRange] | None,
) -> list[CriteriaValue | CriteriaRange]:
    """Validate search criteria. If it is None, return search_criteria = []."""

    if not search_criteria:
        search_criteria = []
    else:
        for criteria_dict in search_criteria:
            if (
                criteria_dict["type"] == "range"
                and criteria_dict["name"] == "issue_date"
            ):
                raise Exception(
                    "issue_date criteria was found in search_criteria. Use start_period and end_period instead."
                )
    return search_criteria
