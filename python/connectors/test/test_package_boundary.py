import pytest


@pytest.mark.xfail(
    reason="known coupling: dcpy.connectors imports from dcpy.lifecycle in "
    "dcpy/connectors/ingest_datastore.py, dcpy/connectors/edm/open_data_nyc.py, and "
    "dcpy/connectors/edm/publishing.py. Remove this xfail once that's fixed "
    "(xfail_strict will fail the suite if it starts passing without this marker "
    "being removed).",
)
def test_cannot_import_lifecycle():
    """dcpy-lifecycle sits at the top of the dependency hierarchy (it depends on every
    other dcpy-* package) - nothing below it should import from it. Only meaningful
    when run against an isolated sync (uv sync --package dcpy-connectors), which is
    the only context where dcpy.lifecycle would otherwise be absent - see
    bash/run_isolated_tests.sh. dcpy-connectors is synced together with dcpy-lifecycle
    there, matching the known coupling, so this currently, correctly, fails.
    """
    with pytest.raises(ImportError):
        import dcpy.lifecycle  # noqa: F401
