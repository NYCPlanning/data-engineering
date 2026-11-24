import pytest

from dcpy.lifecycle import config, ops


def test_purge_data_dir_removes_string_configured_dir(tmp_path, monkeypatch):
    data_dir = tmp_path / "lifecycle"
    (data_dir / "ingest" / "staging").mkdir(parents=True)
    (data_dir / "ingest" / "staging" / "file.parquet").write_text("x")
    monkeypatch.setitem(config.CONF, "local_data_path", str(data_dir))

    ops.purge_data_dir()

    assert not data_dir.exists()


def test_purge_data_dir_missing_dir_is_fine(tmp_path, monkeypatch):
    monkeypatch.setitem(config.CONF, "local_data_path", str(tmp_path / "missing"))
    ops.purge_data_dir()


def test_purge_data_dir_refuses_repo_root(tmp_path, monkeypatch):
    root = tmp_path / "data-engineering"
    root.mkdir()
    monkeypatch.setitem(config.CONF, "local_data_path", str(root))
    with pytest.raises(AssertionError):
        ops.purge_data_dir()
    assert root.exists()
