import pytest


def test_cannot_import_lifecycle():
    """dcpy-lifecycle sits at the top of the dependency hierarchy (it depends on every
    other dcpy-* package) - nothing below it should import from it. Only meaningful
    when run against an isolated sync (uv sync --package dcpy-utils), which is the
    only context where dcpy.lifecycle is genuinely absent - see
    bash/run_isolated_tests.sh.
    """
    with pytest.raises(ImportError):
        import dcpy.lifecycle  # noqa: F401
