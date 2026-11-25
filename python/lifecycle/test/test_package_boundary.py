import pytest


def test_cannot_import_services():
    """dcpy-services sits above dcpy-lifecycle (it depends on lifecycle, like the apps do),
    so lifecycle must never import from it. Only meaningful when run against an isolated
    sync (uv sync --package dcpy-lifecycle), the only context where dcpy.services is
    genuinely absent - see bash/run_isolated_tests.sh.
    """
    with pytest.raises(ImportError):
        import dcpy.services  # noqa: F401
