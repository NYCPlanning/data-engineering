import logging

import numpy as np
import pandas as pd
import pytest
from src.recode_id import ReuseTracker, convert_to_human_readable

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
