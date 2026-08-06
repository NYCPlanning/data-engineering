import pandas as pd
import pytest
from build_scripts import build_markdown
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


def test_chapter_tables_render_missing_use_as_blank(tmp_path, monkeypatch):
    chapters = pd.DataFrame(
        {"chapter_id": ["transportation"], "chapter_name": ["Transportation"]}
    )

    class _FakeClient:
        def read_table_df(self, table_name):
            return chapters

    monkeypatch.setattr(build_markdown.postgres, "PostgresClient", _FakeClient)
    monkeypatch.setattr(build_markdown, "DATA_DIR", tmp_path)

    chapter_datasets = pd.DataFrame(
        {
            "chapter_id": ["transportation"],
            "use_category": pd.Series(["Resources"], dtype="str"),
            "dataset_name": pd.Series(["Various DOT Data Feeds"], dtype="str"),
            "use_details": pd.Series([None], dtype="str"),
        }
    )
    datasets = pd.DataFrame(
        {
            "Dataset Name": pd.Series(["Various DOT Data Feeds"], dtype="str"),
            "Download Link": pd.Series(["N/A"], dtype="str"),
            "Source Link": pd.Series(["N/A"], dtype="str"),
        }
    )

    build_markdown.build_chapter_tables_markdown(chapter_datasets, datasets)

    written = (tmp_path / "chapters_tables.md").read_text()
    assert "nan" not in written
    # a missing Use is blank, while missing links keep the N/A the link builders emit
    assert written.count("N/A") == 2
