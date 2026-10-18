from dcpy.lifecycle.builds import metadata


def test_schema_name_ignores_build_engine_schema(monkeypatch):
    monkeypatch.setenv("BUILD_ENGINE_SCHEMA", "main")
    assert metadata.schema_name("dm-some-Branch") == "dm_some_branch"
    assert metadata.tests_schema_name("dm_some_branch") == "dm_some_branch__tests"


def test_build_name_prefers_build_engine_schema(monkeypatch):
    monkeypatch.setenv("BUILD_ENGINE_SCHEMA", "main")
    assert metadata.build_name("dm-some-branch") == "main"

    monkeypatch.delenv("BUILD_ENGINE_SCHEMA")
    assert metadata.build_name("dm-some-branch") == "dm_some_branch"
