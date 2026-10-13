import xml.etree.ElementTree as ET
from datetime import date
from unittest.mock import Mock

import pandas as pd
import pytest

from dcpy.connectors import checkbook_nyc


def _response(total: int, contract_ids: list[str]) -> str:
    transactions = "".join(
        f"<transaction><prime_contract_id>{c}</prime_contract_id></transaction>"
        for c in contract_ids
    )
    return (
        "<response><status><result>success</result></status><result_records>"
        f"<record_count>{total}</record_count>"
        f"<contract_transactions>{transactions}</contract_transactions>"
        "</result_records></response>"
    )


def _records_from(request_body: str) -> int:
    return int(ET.fromstring(request_body).findtext("records_from") or 0)


def test_monthly_ranges_trims_first_and_last_month():
    assert checkbook_nyc.monthly_ranges(date(2024, 1, 15), date(2024, 3, 10)) == [
        (date(2024, 1, 15), date(2024, 1, 31)),
        (date(2024, 2, 1), date(2024, 2, 29)),
        (date(2024, 3, 1), date(2024, 3, 10)),
    ]


def test_monthly_ranges_within_one_month():
    assert checkbook_nyc.monthly_ranges(date(2024, 5, 2), date(2024, 5, 9)) == [
        (date(2024, 5, 2), date(2024, 5, 9))
    ]


def test_request_xml_includes_criteria():
    body = checkbook_nyc.request_xml(
        "Contracts",
        [
            {"name": "status", "type": "value", "value": "registered"},
            {
                "name": "registration_date",
                "type": "range",
                "start": "2024-01-01",
                "end": "2024-01-31",
            },
        ],
        records_from=1,
        max_records=20_000,
    )
    criteria = ET.fromstring(body).findall("./search_criteria/criteria")
    assert [c.findtext("name") for c in criteria] == ["status", "registration_date"]
    assert criteria[1].findtext("end") == "2024-01-31"


def test_parse_response_raises_on_failure():
    failure = (
        "<response><status><result>failure</result><messages><message>"
        "<description>Invalid value 'active' is provided for 'status'.</description>"
        "</message></messages></status></response>"
    )
    with pytest.raises(RuntimeError, match="Invalid value 'active'"):
        checkbook_nyc.parse_response(failure)


def test_fetch_pages_until_total(monkeypatch):
    monkeypatch.setattr(checkbook_nyc, "MAX_RECORDS", 2)
    pages = {1: ["a", "b"], 3: ["c"]}
    send = Mock(side_effect=lambda body: _response(3, pages[_records_from(body)]))
    monkeypatch.setattr(checkbook_nyc, "_send", send)

    records = checkbook_nyc.fetch("Contracts", [], pause_seconds=0)

    assert [r["prime_contract_id"] for r in records] == ["a", "b", "c"]
    assert send.call_count == 2


def test_pull_requests_each_month_through_version_date(monkeypatch, tmp_path):
    months = []

    def send(body):
        criteria = ET.fromstring(body).findall("./search_criteria/criteria")
        months.append(criteria[-1].findtext("start"))
        return _response(1, [f"contract-{len(months)}"])

    monkeypatch.setattr(checkbook_nyc, "_send", send)

    result = checkbook_nyc.CheckbookNYCConnector().pull(
        "Contracts",
        tmp_path,
        date_field="registration_date",
        start_date="2024-01-01",
        search_criteria=[{"name": "status", "type": "value", "value": "registered"}],
        version="20240315",
        pause_seconds=0,
    )

    assert months == ["2024-01-01", "2024-02-01", "2024-03-01"]
    pulled = pd.read_csv(result["path"])
    assert list(pulled["prime_contract_id"]) == [
        "contract-1",
        "contract-2",
        "contract-3",
    ]
