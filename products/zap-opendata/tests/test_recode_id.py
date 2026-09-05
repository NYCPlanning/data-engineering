import json
import logging
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from src import recode_id as recode_id_module
from src.recode_id import ReuseTracker, convert_to_human_readable, recode_id

_logger = logging.getLogger("test_recode_id")


def test_convert_to_human_readable_recodes_id():
    """Happy path: a present id is mapped to its human-readable name."""
    expanded = {"dcp_leadagencyforenvreview": {"name": "DCP", "accountid": "acct-1"}}
    row = pd.Series({"crm_project_id": "p1", "ceqr_leadagency": "acct-1"})
    result = convert_to_human_readable(
        expanded=expanded,
        row=row,
        local_fieldname="ceqr_leadagency",
        recode_tracker=ReuseTracker(),
        logger=_logger,
    )
    assert result == "DCP"


def test_convert_to_human_readable_passes_through_missing_id():
    """Regression: a missing id (NaN) with no expand metadata must pass through,
    not raise. pandas 3.0 reads a SQL NULL in a str column as NaN rather than
    None, so the old `id_val is not None` guard raised here; `pd.isna` fixes it."""
    expanded = {"dcp_leadagencyforenvreview": None}
    row = pd.Series({"crm_project_id": "p1", "ceqr_leadagency": np.nan})
    result = convert_to_human_readable(
        expanded=expanded,
        row=row,
        local_fieldname="ceqr_leadagency",
        recode_tracker=ReuseTracker(),
        logger=_logger,
    )
    assert pd.isna(result)


def test_convert_to_human_readable_raises_on_present_id_without_metadata():
    """The guard's real purpose is preserved: a present id with no expand
    metadata is a genuine inconsistency and must still raise."""
    expanded = {"dcp_leadagencyforenvreview": None}
    row = pd.Series({"crm_project_id": "p1", "ceqr_leadagency": "acct-1"})
    with pytest.raises(Exception, match="ceqr_leadagency"):
        convert_to_human_readable(
            expanded=expanded,
            row=row,
            local_fieldname="ceqr_leadagency",
            recode_tracker=ReuseTracker(),
            logger=_logger,
        )


class StubResponse:
    def __init__(self, payload: dict, status_code: int = 200):
        self.status_code = status_code
        self.payload = payload
        self.text = json.dumps(payload)

    def json(self) -> dict:
        return self.payload


@pytest.fixture
def stub_crm(monkeypatch, tmp_path):
    """Stub CRM auth and HTTP for recode_id, and keep its .logs/ inside tmp_path."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        recode_id_module, "Client", lambda **kwargs: SimpleNamespace(request_header={})
    )

    def install(*responses: StubResponse) -> list[str]:
        requested: list[str] = []

        def fake_get(url, headers=None):
            requested.append(url)
            return responses[min(len(requested), len(responses)) - 1]

        monkeypatch.setattr(recode_id_module.requests, "get", fake_get)
        return requested

    return install


class TestRecodeId:
    """The top-level recode loop, with the CRM stubbed.

    Covers what convert_to_human_readable's own tests can't reach: the reuse cache,
    the refresh-and-retry on a non-200, and the give-up path.
    """

    # Shaped like the $expand payload expand_url() asks for. The null contact matters:
    # convert_to_human_readable pops metadata field names off the end, so primary
    # applicant resolves through contact first and falls back to account.
    expanded = {
        "dcp_applicant_customer_account": {
            "name": "624 Morris B, LLC",
            "accountid": "acct-applicant",
        },
        "dcp_applicant_customer_contact": None,
        "dcp_leadagencyforenvreview": {"name": "DCP", "accountid": "acct-agency"},
        "dcp_CurrentMilestone": {
            "dcp_name": "EAS - Project Completed",
            "dcp_projectmilestoneid": "ms-current",
        },
        "dcp_currentenvironmentmilestone": {
            "dcp_name": "EAS - Review Filed EAS",
            "dcp_projectmilestoneid": "ms-env",
        },
    }

    recoded_names = [
        "624 Morris B, LLC",
        "DCP",
        "EAS - Project Completed",
        "EAS - Review Filed EAS",
    ]

    def frame(self, *project_ids: str) -> pd.DataFrame:
        return pd.DataFrame.from_records(
            [
                {
                    "crm_project_id": project_id,
                    "primary_applicant": "acct-applicant",
                    "ceqr_leadagency": "acct-agency",
                    "current_milestone": "ms-current",
                    "current_envmilestone": "ms-env",
                }
                for project_id in project_ids
            ]
        )

    def names(self, recoded: pd.DataFrame, row: int = 0) -> list:
        return [recoded.loc[row, field] for field in recode_id_module.RECODE_ID_FIELDS]

    def test_replaces_ids_with_names(self, stub_crm):
        requested = stub_crm(StubResponse(self.expanded))

        recoded = recode_id(self.frame("proj-1"))

        assert self.names(recoded) == self.recoded_names
        assert "proj-1" in requested[0]

    def test_reuses_a_recode_across_rows(self, stub_crm):
        """Second row hits the cache, so the CRM is queried once, not twice."""
        requested = stub_crm(StubResponse(self.expanded))

        recoded = recode_id(self.frame("proj-1", "proj-2"))

        assert len(requested) == 1
        assert self.names(recoded, row=0) == self.recoded_names
        assert self.names(recoded, row=1) == self.recoded_names

    def test_refreshes_auth_and_retries_after_non_200(self, stub_crm):
        requested = stub_crm(
            StubResponse({"error": "expired"}, status_code=401),
            StubResponse(self.expanded),
        )

        recoded = recode_id(self.frame("proj-1"))

        assert len(requested) == 2
        assert self.names(recoded) == self.recoded_names

    def test_gives_up_after_a_second_non_200(self, stub_crm):
        """Row passes through with its raw ids rather than raising."""
        requested = stub_crm(StubResponse({"error": "expired"}, status_code=401))

        recoded = recode_id(self.frame("proj-1"))

        assert len(requested) == 2
        assert self.names(recoded) == [
            "acct-applicant",
            "acct-agency",
            "ms-current",
            "ms-env",
        ]

    def test_skips_rows_with_no_ids(self, stub_crm):
        requested = stub_crm(StubResponse(self.expanded))
        empty = self.frame("proj-1")
        for field in recode_id_module.RECODE_ID_FIELDS:
            empty[field] = np.nan

        recoded = recode_id(empty)

        assert requested == []
        assert recoded.loc[0, "crm_project_id"] == "proj-1"
