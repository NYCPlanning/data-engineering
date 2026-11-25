import pytest


def test_cannot_import_lifecycle_geo_extra():
    """dcpy-services depends on dcpy-lifecycle without its "geo" extra: the catalog needs
    lifecycle's orchestration (config, connectors, planning), not GDAL. Only meaningful when
    run against an isolated sync (uv sync --package dcpy-services), the only context where
    dcpy.geosupport is genuinely absent - see bash/run_isolated_tests.sh.
    """
    with pytest.raises(ImportError):
        import dcpy.geosupport  # noqa: F401
