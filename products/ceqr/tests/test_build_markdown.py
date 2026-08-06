import pandas as pd
import pytest
from build_scripts.build_markdown import _add_link

# pandas 3's str dtype surfaces a SQL NULL as float nan rather than None, and nan is
# truthy — built the way it actually reaches _add_link so this stays honest if the
# NA sentinel changes again.
STR_DTYPE_NULL = pd.Series([None], dtype="str")[0]


@pytest.mark.parametrize(
    "url", [STR_DTYPE_NULL, None, ""], ids=["str_dtype_null", "none", "empty"]
)
def test_add_link_returns_na_for_missing_url(url):
    assert _add_link(url) == "N/A"


def test_add_link_builds_anchor():
    assert (
        _add_link("https://example.gov/data")
        == '<a href="https://example.gov/data" target="_blank">webpage</a>'
    )
